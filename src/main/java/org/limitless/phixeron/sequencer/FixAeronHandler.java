package org.limitless.phixeron.sequencer;

import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableDirectByteBuffer;
import org.agrona.MutableDirectBuffer;

import java.util.LinkedHashMap;

import static org.limitless.phixeron.sequencer.FixSessionStateMachine.DISC_SESSION_CONNECT;
import static org.limitless.phixeron.sequencer.FixSessionStateMachine.DISC_SESSION_DISCONNECT;

/**
 * ClusteredService implementation for the FIX Sequencer.
 *
 * Owns all Aeron Cluster lifecycle state and coordinates between the Raft log
 * and the per-session FIX state machines. All callbacks run on the single Aeron
 * Cluster conductor thread in commit order, guaranteeing deterministic execution.
 *
 * Responsibilities:
 *   - Decode the 9-byte SBE envelope (discriminator + fixSessionId) from each inbound message.
 *   - Delegate to the appropriate FixSessionStateMachine.
 *   - Handle internal commands (ScheduleTimerCommand, StartArchiveReplayCommand,
 *     CancelPendingResendCommand) and publish the rest on stream 2.
 *   - Maintain MemoryStorage and outboundArchiveIndex per session.
 *   - Snapshot and restore all session state via Aeron Archive.
 *   - Maintain the cluster-level clOrdIdRoutingTable (snapshotted).
 */
public final class FixAeronHandler implements ClusteredService
{
    // ── Configuration ─────────────────────────────────────────────────────────

    /** Aeron IPC channel for stream 2 (cluster → AppWorker). */
    static final String STREAM_2_CHANNEL = "aeron:ipc";
    static final int    STREAM_2_ID      = 2;

    /** Aeron IPC channel for Archive replay of the Raft log. */
    static final String REPLAY_CHANNEL   = "aeron:ipc";
    static final int    REPLAY_STREAM_ID = 99;  // private; not shared with other components

    /** Batch size for polling the Archive replay image per duty cycle. */
    static final int REPLAY_BATCH = 10;

    /**
     * Envelope header: 1-byte discriminator + 8-byte fixSessionId.
     * FIX messages carry their MsgType ASCII code as the discriminator.
     * Lifecycle events (SESSION_CONNECT, SESSION_DISCONNECT) use 0x01 / 0x02.
     */
    static final int ENVELOPE_HEADER_LEN = 9;
    static final int ENVELOPE_DISC_OFFSET = 0;
    static final int ENVELOPE_SESSION_ID_OFFSET = 1;

    /**
     * Maximum back-pressure retry cycles before alerting operations.
     * At ~10 ns/cycle this is ~100 ms before the first alert.
     * The spin continues (re-alerting each period) rather than dropping the message,
     * because dropping would cause a sequence gap on the AppWorker side.
     */
    static final int STREAM2_MAX_IDLE_CYCLES = 1_000_000;

    // ── Cluster-level state ───────────────────────────────────────────────────

    /**
     * Per-session state keyed by fixSessionId.
     * LinkedHashMap preserves insertion order → deterministic snapshot byte sequence
     * across all cluster nodes (§6.1.11 determinism constraint).
     */
    private final LinkedHashMap<Long, FixSessionStateMachine> sessions =
        new LinkedHashMap<>();

    /**
     * Cluster-level routing table: ClOrdId → buy-side sessionId.
     * Populated when a FORWARD_APP is processed. Entries removed on final ExecutionReport.
     * Snapshotted so the table survives failover without rebuild.
     */
    private final LinkedHashMap<String, Long> clOrdIdRoutingTable = new LinkedHashMap<>();

    // ── Aeron runtime state (not snapshotted) ────────────────────────────────

    private Cluster      cluster;
    private Publication  stream2;
    private AeronArchive aeronArchive;

    /** True only while this node is the Raft leader. Suppressors stream 2 output on followers. */
    private boolean isLeader = false;

    /** Non-null only during an active Archive replay (slow-path ResendRequest). */
    private PendingResend pendingResend = null;

    /** Recording ID of the Aeron Archive recording that holds the Raft log. Set in onStart. */
    private long clusterLogRecordingId = -1;

    /** Shared encoding buffer for stream 2 command serialisation. */
    private final MutableDirectBuffer encodingBuffer = new ExpandableDirectByteBuffer(4096);

    // ── ClusteredService lifecycle ────────────────────────────────────────────

    @Override
    public void onStart(final Cluster cluster, final Image snapshotImage)
    {
        this.cluster = cluster;
        this.stream2 = cluster.context().aeron()
            .addPublication(STREAM_2_CHANNEL, STREAM_2_ID);

        this.aeronArchive = AeronArchive.connect(new AeronArchive.Context()
            .aeron(cluster.context().aeron()));

        // Locate the cluster log recording so Archive replay positions are valid.
        clusterLogRecordingId = findClusterLogRecordingId();

        if (snapshotImage != null)
        {
            loadSnapshot(snapshotImage);
        }
    }

    @Override
    public void onSessionOpen(final ClientSession session, final long timestamp)
    {
        // Aeron Cluster client sessions (ingress publishers) open and close at the
        // Aeron level; actual FIX session lifecycle is tracked via SESSION_CONNECT /
        // SESSION_DISCONNECT events on stream 1.
    }

    @Override
    public void onSessionClose(
        final ClientSession session,
        final long timestamp,
        final io.aeron.cluster.codecs.CloseReason closeReason)
    {
    }

    // ── Committed log entries ─────────────────────────────────────────────────

    @Override
    public void onSessionMessage(
        final ClientSession aeronSession,
        final long          timestamp,
        final DirectBuffer  buffer,
        final int           offset,
        final int           length,
        final Header        header)
    {
        // Make progress on any in-flight Archive replay before processing new commits.
        pollPendingResend();

        final int  discriminator = buffer.getByte(offset + ENVELOPE_DISC_OFFSET) & 0xFF;
        final long fixSessionId  = buffer.getLong(offset + ENVELOPE_SESSION_ID_OFFSET);

        final FixSessionStateMachine fsm = sessions.computeIfAbsent(
            fixSessionId, id -> new FixSessionStateMachine(id, SessionRole.ACCEPTOR, 2500));

        final Command[] commands = fsm.onEvent(
            discriminator,
            buffer,
            offset + ENVELOPE_HEADER_LEN,
            length - ENVELOPE_HEADER_LEN,
            cluster.time());

        emitCommands(fsm, commands, header.position());
    }

    @Override
    public void onTimerEvent(final long correlationId, final long timestamp)
    {
        pollPendingResend();

        // Iterate in insertion order; each FSM checks whether the correlationId belongs
        // to it and returns commands only on a match. Stop after the first match because
        // correlationIds are unique across all sessions.
        for (final FixSessionStateMachine fsm : sessions.values())
        {
            final Command[] commands = fsm.onTimer(correlationId, cluster.time());
            if (commands.length > 0)
            {
                emitCommands(fsm, commands, 0L);
                return;
            }
        }
    }

    // ── Snapshot ──────────────────────────────────────────────────────────────

    @Override
    public void onTakeSnapshot(final ExclusivePublication snapshotPublication)
    {
        for (final FixSessionStateMachine fsm : sessions.values())
        {
            final int len = fsm.encodeTo(encodingBuffer, 0);
            offerSnapshot(snapshotPublication, encodingBuffer, len);
        }

        // Snapshot the cluster-level clOrdIdRoutingTable after all session entries.
        int pos = 0;
        encodingBuffer.putInt(pos, clOrdIdRoutingTable.size());  pos += Integer.BYTES;
        for (final var entry : clOrdIdRoutingTable.entrySet())
        {
            final byte[] clOrdIdBytes = entry.getKey().getBytes();
            encodingBuffer.putShort(pos, (short) clOrdIdBytes.length);  pos += Short.BYTES;
            encodingBuffer.putBytes(pos, clOrdIdBytes);                  pos += clOrdIdBytes.length;
            encodingBuffer.putLong(pos, entry.getValue());               pos += Long.BYTES;
        }
        offerSnapshot(snapshotPublication, encodingBuffer, pos);
    }

    private void offerSnapshot(
        final ExclusivePublication pub,
        final MutableDirectBuffer  buf,
        final int                  len)
    {
        while (pub.offer(buf, 0, len) < 0)
        {
            cluster.idleStrategy().idle();
        }
    }

    private void loadSnapshot(final Image snapshotImage)
    {
        // Phase 1: restore per-session FSMs.
        // Each fragment in the snapshot image corresponds to one FixSessionStateMachine,
        // followed by a final fragment containing the clOrdIdRoutingTable.
        // We rely on a sentinel in the data (sessionId == 0) to detect the routing table
        // fragment; all real sessions have sessionId > 0.
        final FragmentHandler decoder = (buf, off, len, hdr) ->
        {
            final long firstLong = buf.getLong(off);

            if (firstLong == 0L && buf.getInt(off + Long.BYTES) < 0)
            {
                // Routing table fragment (sessionId == 0 is impossible for a real session).
                loadClOrdIdRoutingTable(buf, off + Long.BYTES);
                return;
            }

            // Check if this looks like a routing-table-only record (marker = -1 after sessionId).
            // Simpler: attempt to decode as FSM; if sessionId == 0 treat as routing table.
            if (firstLong == 0L)
            {
                loadClOrdIdRoutingTable(buf, off);
                return;
            }

            final FixSessionStateMachine fsm = FixSessionStateMachine.decodeFrom(buf, off);
            sessions.put(fsm.sessionId(), fsm);

            // Re-register active timers using snapshotted deadlines so they fire correctly
            // after log replay resumes from the snapshot position (§6.1.10 timer rationale).
            if (fsm.heartbeatTimer() != 0)
            {
                cluster.scheduleTimer(fsm.heartbeatTimer(), fsm.heartbeatTimerDeadline());
            }
            if (fsm.testReqTimer() != 0)
            {
                cluster.scheduleTimer(fsm.testReqTimer(), fsm.testReqTimerDeadline());
            }

            // If a TestRequest probe was in flight at snapshot time, the client's response
            // will carry the old TestReqID which no longer matches. Flag the FSM to clear
            // the stale probe on the first committed inbound message (§4.5.1).
            if (fsm.pendingTestReqId() != null)
            {
                fsm.postFailoverPendingTestReqReset = true;
            }
        };

        while (!snapshotImage.isClosed())
        {
            cluster.idleStrategy().idle(snapshotImage.poll(decoder, REPLAY_BATCH));
        }
    }

    private void loadClOrdIdRoutingTable(final DirectBuffer buf, int offset)
    {
        final int count = buf.getInt(offset);  offset += Integer.BYTES;
        for (int i = 0; i < count; i++)
        {
            final int    keyLen    = buf.getShort(offset) & 0xFFFF;  offset += Short.BYTES;
            final byte[] keyBytes  = new byte[keyLen];
            buf.getBytes(offset, keyBytes);                           offset += keyLen;
            final long   sessionId = buf.getLong(offset);             offset += Long.BYTES;
            clOrdIdRoutingTable.put(new String(keyBytes), sessionId);
        }
    }

    // ── Leadership ────────────────────────────────────────────────────────────

    @Override
    public void onNewLeadershipTermEvent(
        final long logPosition,
        final long leadershipTermId,
        final long timestamp,
        final long termBaseLogPosition,
        final int  leaderMemberId,
        final int  logSessionId,
        final java.util.concurrent.TimeUnit timeUnit,
        final int  appVersion)
    {
        isLeader = (leaderMemberId == cluster.memberId());
    }

    @Override
    public void onRoleChange(final Cluster.Role newRole)
    {
        isLeader = (newRole == Cluster.Role.LEADER);
        if (!isLeader)
        {
            // Discard any in-progress Archive replay; the new leader will restart it
            // by re-processing the committed ResendRequest log entry.
            pendingResend = null;
        }
    }

    @Override
    public void onTerminate(final Cluster cluster)
    {
    }

    // ── Archive replay (slow-path ResendRequest) ──────────────────────────────

    private void startArchiveReplay(
        final FixSessionStateMachine fsm,
        final long                   recordingId,
        final long                   startPosition,
        final long                   endSeqNo)
    {
        if (recordingId < 0)
        {
            // Archive recording not yet located; fall back to MemoryStorage tail only.
            alertArchiveUnavailable();
            return;
        }

        final long length = ArchivePosition.REPLAY_TO_END;  // replay to recording end

        final long replaySessionId = aeronArchive.startReplay(
            recordingId, startPosition, length,
            REPLAY_CHANNEL, REPLAY_STREAM_ID);

        // Wait for the image to become available (brief busy-spin is acceptable here
        // because this code path is rare and does not block the conductor thread for
        // more than a few microseconds on a local IPC channel).
        io.aeron.Subscription sub = cluster.context().aeron()
            .addSubscription(REPLAY_CHANNEL, REPLAY_STREAM_ID);
        Image image = null;
        for (int i = 0; i < 10_000 && image == null; i++)
        {
            image = sub.imageBySessionId((int) replaySessionId);
            Thread.onSpinWait();
        }

        if (image == null)
        {
            alertArchiveReplayImageMissing(recordingId, startPosition);
            return;
        }

        pendingResend = new PendingResend(fsm, sub, image, startPosition, endSeqNo);
    }

    private void pollPendingResend()
    {
        if (pendingResend == null || !isLeader) return;

        final int fragments = pendingResend.image().poll(
            (buf, off, len, hdr) ->
            {
                final Command[] commands = pendingResend.fsm()
                    .onResendFragment(buf, off, len, cluster.time());
                emitCommands(pendingResend.fsm(), commands, 0L);
                pendingResend = pendingResend.withEmitPosition(hdr.position());
            },
            REPLAY_BATCH);

        cluster.idleStrategy().idle(fragments);

        if (pendingResend.image().isClosed())
        {
            // Replay complete: flush any tail entries that fall within MemoryStorage.
            final Command[] tail = pendingResend.fsm()
                .flushMemoryStorageTail(pendingResend.emitPosition(), pendingResend.endSeqNo());
            emitCommands(pendingResend.fsm(), tail, 0L);

            pendingResend.subscription().close();
            pendingResend = null;
        }
    }

    // ── Command emission ──────────────────────────────────────────────────────

    private void emitCommands(
        final FixSessionStateMachine fsm,
        final Command[]              commands,
        final long                   clusterPosition)
    {
        if (!isLeader) return;  // suppressors output on follower during log replay

        for (final Command cmd : commands)
        {
            switch (cmd)
            {
                case Command.ScheduleTimerCommand t ->
                    cluster.scheduleTimer(t.correlationId(), t.deadlineMs());

                case Command.StartArchiveReplayCommand a ->
                    startArchiveReplay(fsm, a.recordingId(), a.startPosition(), a.endSeqNo());

                case Command.CancelPendingResendCommand c ->
                {
                    if (pendingResend != null && pendingResend.fsm().sessionId() == c.sessionId())
                    {
                        pendingResend.subscription().close();
                        pendingResend = null;
                    }
                }

                case Command.SendCommand send ->
                {
                    // Update MemoryStorage; record eviction in outboundArchiveIndex.
                    final MemoryStorage.EvictedEntry evicted = fsm.memoryStorage.store(
                        send.seqNum(), clusterPosition, send.templateId(),
                        send.payload(), send.length());
                    if (evicted != null)
                    {
                        fsm.outboundArchiveIndex.put(
                            evicted.seqNum(),
                            new ArchivePosition(
                                clusterLogRecordingId,
                                evicted.clusterPosition(),
                                ArchivePosition.REPLAY_TO_END));
                    }
                    publishOnStream2(cmd, clusterPosition);
                }

                default -> publishOnStream2(cmd, clusterPosition);
            }
        }
    }

    private void publishOnStream2(final Command cmd, final long clusterPosition)
    {
        cmd.encodeInto(encodingBuffer, 0, clusterPosition);
        int idleCycles = 0;
        while (stream2.offer(encodingBuffer, 0, cmd.encodedLength()) < 0)
        {
            if (++idleCycles >= STREAM2_MAX_IDLE_CYCLES)
            {
                alertStream2BackPressureExceeded(clusterPosition);
                idleCycles = 0;
            }
            // Must use cluster idle strategy so consensus housekeeping continues
            // while we spin. Blocking here without yielding control would prevent
            // Raft heartbeat processing and trigger a spurious leader election.
            cluster.idleStrategy().idle();
        }
    }

    // ── Archive discovery ─────────────────────────────────────────────────────

    private long findClusterLogRecordingId()
    {
        // The Aeron Cluster records the consensus log on an internal channel/stream.
        // We list recordings matching the cluster log stream to obtain the recording ID.
        // In production the recording ID is stable across restarts (same channel URI).
        final long[] found = {-1L};
        aeronArchive.listRecordingsForUri(
            0, 1,
            "aeron:ipc",                                   // cluster log IPC channel
            io.aeron.cluster.ConsensusModule.Configuration.logStreamId(),
            (controlSessionId, correlationId, recordingId,
             startTimestamp, stopTimestamp, startPosition, stopPosition,
             initialTermId, segmentFileLength, termBufferLength, mtuLength,
             sessionId, streamId, strippedChannel, originalChannel, sourceIdentity) ->
                found[0] = recordingId);

        return found[0];
    }

    // ── Diagnostics / alerting ────────────────────────────────────────────────

    private void alertStream2BackPressureExceeded(final long clusterPosition)
    {
        System.err.printf("[FixAeronHandler] ALERT: stream 2 back-pressure at clusterPosition=%d%n",
            clusterPosition);
    }

    private void alertArchiveUnavailable()
    {
        System.err.println("[FixAeronHandler] ALERT: cluster log recording not yet located; " +
            "slow-path ResendRequest will be serviced from MemoryStorage tail only");
    }

    private void alertArchiveReplayImageMissing(final long recordingId, final long startPosition)
    {
        System.err.printf(
            "[FixAeronHandler] ALERT: Archive replay image never appeared " +
            "(recordingId=%d startPosition=%d)%n", recordingId, startPosition);
    }

    // ── PendingResend ─────────────────────────────────────────────────────────

    /**
     * Tracks state of an in-progress async Archive replay for a slow-path ResendRequest.
     * Not snapshotted; re-derived on failover by re-processing the committed ResendRequest.
     */
    record PendingResend(
        FixSessionStateMachine   fsm,
        io.aeron.Subscription    subscription,
        Image                    image,
        long                     emitPosition,
        long                     endSeqNo)
    {
        PendingResend withEmitPosition(final long newPos)
        {
            return new PendingResend(fsm, subscription, image, newPos, endSeqNo);
        }
    }
}