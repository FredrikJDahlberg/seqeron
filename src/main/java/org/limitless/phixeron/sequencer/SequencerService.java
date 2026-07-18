package org.limitless.phixeron.sequencer;

import io.aeron.ExclusivePublication;
import io.aeron.FragmentAssembler;
import io.aeron.Image;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.archive.status.RecordingPos;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.logbuffer.Header;
import java.util.concurrent.TimeUnit;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableDirectByteBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.NoOpLock;
import org.agrona.concurrent.status.CountersReader;
import org.limitless.phixeron.replayer.ReplayerService;
import org.limitless.phixeron.sbe.sequenced.ClientConnectedEncoder;
import org.limitless.phixeron.sbe.sequenced.ClientDisconnectedEncoder;
import org.limitless.phixeron.sbe.sequenced.HeaderEncoder;
import org.limitless.phixeron.sbe.sequenced.LeadershipChangedEncoder;
import org.limitless.phixeron.sbe.sequenced.MessageHeaderEncoder;
import org.limitless.phixeron.sbe.sequenced.TickEncoder;
import org.limitless.phixeron.sbe.unsequenced.HeaderDecoder;
import org.limitless.phixeron.sbe.unsequenced.MessageHeaderDecoder;

/**
 * Aeron Cluster service that imposes a total order on messages arriving from multiple clients.
 *
 * <p>For every committed {@link #onSessionMessage} the service assigns a
 * <b>globalSeqNo</b> — a cluster-wide monotone counter shared across all sources and
 * lifecycle events (connect / disconnect / leadership change) — and stamps it, together with the cluster
 * consensus timestamp, into the message's {@code header} composite before republishing it.
 *
 * <p>Ingress messages arrive already SBE-encoded as {@code sbe-unsequenced.xml} (schema
 * ID 200) — the FIX gateway encodes every admin and application FIX message that way and
 * offers it directly to the cluster, with {@code header.sourceId} identifying the submitting
 * gateway <em>process</em> (a fixed constant, stable across restarts and unique across every
 * gateway instance sharing this cluster), {@code header.connectionId} identifying the specific
 * TCP connection at that gateway, and {@code header.sessionId} the Aeron Cluster session. This
 * service does not need to know about individual FIX message types to re-stamp them: {@code
 * sbe-sequenced.xml} (schema ID 202) is deliberately kept byte-identical to {@code
 * sbe-unsequenced.xml} past the {@code header} composite (same field order/types/ids, same
 * var-data layout), so {@link #onSessionMessage} decodes only the outer {@code
 * MessageHeader} and the {@code header} composite (both always at a fixed offset,
 * regardless of {@code templateId}), then copies every remaining byte — the rest of the
 * fixed block plus all var-data — verbatim into a new {@code sbe-sequenced.xml} message
 * whose {@code header} composite carries the original {@code sourceId}/{@code connectionId}/
 * {@code sessionId} plus the new {@code globalSeqNo}/{@code timestamp}.
 *
 * <p>The decorated message is published on the node-local <em>tap</em>
 * ({@link #REPLAYER_CHANNEL} / {@link #REPLAYER_STREAM_ID}), an {@code aeron:ipc} stream that this node's
 * co-located Aeron Archive records. Co-located app replicas follow it live directly, and the
 * co-located {@link ReplayerService} serves history/gap replay of this
 * recording to those apps on startup.
 *
 * <p><b>Every node records its own tap (no leader/follower asymmetry on the stream path):</b> all
 * cluster nodes maintain identical sequencing state (updated on every callback) and each one
 * publishes and records its own tap. Because every node processes the same committed log in the
 * same order, the taps are byte-identical across nodes, so every node's local archive independently
 * holds a complete copy of the sequenced history — no cross-node replication is needed, and any node
 * a client is co-located with can serve full history/gap replay. The tap {@link
 * ExclusivePublication} and its recording are created once in {@link #onStart} and live for the whole
 * process, continuous across leadership changes (an {@code aeron:ipc} publication has no fixed port to
 * collide on across a failover, unlike the retired UDP global stream), so a given node's recording is a
 * single continuous run spanning every leader tenure rather than one recording per tenure.
 *
 * <p><b>Durability:</b> {@link #emit} is <em>reliable</em> (it spins until the offer lands), because
 * the tap recording is the authoritative history — a dropped frame would be an unrecoverable gap. This
 * cannot wedge structurally the way the retired UDP global stream did (audit.md S4, where {@code
 * MaxMulticastFlowControl} never advanced the sender limit with zero network subscribers): the only
 * tethered subscriber of the tap is the co-located archive recording, so {@link #emit} blocks only on
 * real local-archive write back-pressure, which clears as the archive drains to disk. The app replicas'
 * own tap subscriptions are untethered, so a slow app is dropped (and heals via the ReplayerService replay
 * protocol) rather than back-pressuring the recording.
 *
 * <p><b>Snapshot format</b> (little-endian binary, single fragment):
 * <pre>
 *   int64  globalSeqNo
 * </pre>
 */
public final class SequencerService implements ClusteredService {
    /**
     * Node-local IPC channel and stream the sequenced stream is tapped onto. Every node — leader
     * <em>and</em> follower — republishes each sequenced frame here in {@code globalSeqNo} order (the
     * taps are byte-identical across nodes, since every node processes the same committed log in the
     * same order) and records it into its own co-located archive. The co-located app replicas follow it
     * directly as their live feed, and the co-located {@link ReplayerService}
     * serves history/gap replay of this recording — the same node-local archive serves both.
     *
     * <p>Created and recorded once in {@link #onStart} and continuous per node across leadership changes
     * ({@code aeron:ipc} has no fixed port to collide on across a failover, unlike the retired UDP global
     * stream), so a node's recording is one continuous run spanning every leader tenure that consumers
     * never re-resolve. Reliable, not lossy ({@link #emit} spins until the offer lands): the recording
     * is the authoritative history, so a dropped frame would be an unrecoverable gap.
     */
    public static final String REPLAYER_CHANNEL = "aeron:ipc";
    public static final int REPLAYER_STREAM_ID = 205;

    /**
     * How long {@link #awaitReplayerRecordingActive} waits for the co-located archive's recording of the tap
     * to become active before failing start-up. Bounded so a wedged/absent local archive fails fast at
     * onStart rather than hanging the node.
     */
    private static final long REPLAYER_RECORDING_START_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(5);

    /**
     * Maximum consecutive back-pressure spins in {@link #emit} before printing an alert.
     * At ~10 ns/spin this is ~10 ms per alert period.
     */
    private static final int MAX_BACK_PRESSURE_SPINS = 1_000_000;

    private static final int SNAPSHOT_POLL_BATCH = 10;

    /**
     * Period of the internal cluster clock ({@link TickEncoder}): the leader fires this timer once per
     * second and every node emits a header-only {@code Tick} carrying the consensus timestamp. It exists
     * so every consumer has a cluster-driven clock that keeps advancing even while an individual FIX
     * session is silent — which is exactly when the gateway's keepalive watchdog must probe/disconnect
     * (the sequenced-header timestamp is the only clock the watchdog is allowed to trust, since only the
     * leader assigns real time). 1 Hz gives ±1 s resolution, ample for the watchdog's tens-of-seconds
     * thresholds. Trade-off: every tick appends a timer event + a tick frame to the replicated
     * log/recording, so full-log-replay recovery grows with uptime; this constant is the single knob to
     * trade watchdog resolution against that cost. (A tighter win — gating clock emission on active FIX
     * sessions — is noted in doc/gap.md; 1 Hz is the low-risk interim.)
     */
    private static final long TICK_INTERVAL_MS = 1000;

    /**
     * Correlation id of the single repeating tick timer. There is only one service timer, so a fixed
     * constant is safe; rescheduling with the same id simply moves the one timer's deadline.
     */
    private static final long TICK_TIMER_CORRELATION_ID = 0x7100_0000_0000_0001L;

    /**
     * header.sourceId/connectionId for lifecycle events synthesized by this service (ClientConnected /
     * ClientDisconnected): an Aeron Cluster session opening/closing has no gateway-process or TCP-level
     * connection id to carry, unlike the ingress messages it forwards.
     */
    private static final int NO_SOURCE_ID = -1;

    // ── SBE codecs — single conductor thread; no synchronisation needed ───────

    // Ingress decode (schema 200, sbe-unsequenced.xml). Only the outer framing
    // header and the generic `header` composite are ever decoded — body
    // fields are copied through as opaque bytes, see onSessionMessage.
    private final MessageHeaderDecoder ingressMsgHeaderDecoder = new MessageHeaderDecoder();
    private final HeaderDecoder ingressHeaderDecoder = new HeaderDecoder();

    // Egress encode (schema 202, sbe-sequenced.xml).
    private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
    private final HeaderEncoder egressHeaderEncoder = new HeaderEncoder();
    private final ClientConnectedEncoder clientConnEncoder = new ClientConnectedEncoder();
    private final ClientDisconnectedEncoder clientDiscEncoder = new ClientDisconnectedEncoder();
    private final LeadershipChangedEncoder leadershipChangedEncoder = new LeadershipChangedEncoder();
    private final TickEncoder tickEncoder = new TickEncoder();
    private final MutableDirectBuffer encodeBuffer = new ExpandableDirectByteBuffer(4096);

    // ── Sequencing state (snapshotted; updated on every node for determinism) ─

    /** Cluster-wide sequence counter; incremented for messages and lifecycle events. */
    private long globalSeqNo = 0;

    // ── Aeron runtime (not snapshotted) ──────────────────────────────────────

    private Cluster cluster;
    private ExclusivePublication replayerPub;
    private AeronArchive aeronArchive;

    /**
     * memberId of whichever node last reported itself the leader; -1 until the first
     * {@link #onNewLeadershipTermEvent}. Kept only to de-duplicate leadership-change events and to
     * stamp {@code newLeaderMemberId} onto the synthesized {@code LeadershipChanged}.
     */
    private int currentLeaderMemberId = -1;

    // ── ClusteredService lifecycle ────────────────────────────────────────────

    @Override
    public void onStart(final Cluster cluster, final Image snapshotImage) {
        this.cluster = cluster;

        // Connect to the co-located archive via IPC — NoOpLock is safe on the single conductor thread.
        aeronArchive = AeronArchive.connect(new AeronArchive.Context()
                                                .aeron(cluster.context().aeron())
                                                .controlRequestChannel("aeron:ipc")
                                                .controlRequestStreamId(100)
                                                .controlResponseChannel("aeron:ipc")
                                                .controlResponseStreamId(101)
                                                .lock(NoOpLock.INSTANCE));

        // Node-local live tap of the sequenced stream, created and recorded on every node (leader and
        // follower alike). Every node re-publishes each sequenced frame here and records it into its own
        // co-located archive, so every node independently holds a complete copy of the sequenced history
        // — no cross-node replication needed. Co-located app replicas follow this live directly; the
        // co-located ReplayerService serves history/gap replay of this recording. See REPLAYER_CHANNEL. Recording
        // must be active before the first
        // frame is published, so await it here (onStart runs before any onSessionMessage, so this waits on
        // start-up alone, never on live traffic).
        replayerPub = cluster.context().aeron().addExclusivePublication(REPLAYER_CHANNEL, REPLAYER_STREAM_ID);
        aeronArchive.startRecording(REPLAYER_CHANNEL, REPLAYER_STREAM_ID, SourceLocation.LOCAL);
        awaitReplayerRecordingActive();

        if (snapshotImage != null) {
            loadSnapshot(snapshotImage);
        }
        // NB: the internal clock timer is armed in onNewLeadershipTermEvent, not here — Aeron forbids
        // scheduling timers (or sending messages) from onStart.
    }

    // Blocks until the co-located archive's recording subscription has attached to the tap publication,
    // so no frame is published before the recording begins (which would leave an unrecoverable hole in
    // the authoritative history). Bounded by REPLAYER_RECORDING_START_TIMEOUT_NS so an absent/wedged local
    // archive fails start-up fast rather than hanging.
    private void awaitReplayerRecordingActive() {
        final CountersReader counters = cluster.context().aeron().countersReader();
        final long archiveId = aeronArchive.archiveId();
        final long deadlineNs = System.nanoTime() + REPLAYER_RECORDING_START_TIMEOUT_NS;
        while (RecordingPos.findCounterIdBySession(counters, replayerPub.sessionId(), archiveId)
               == CountersReader.NULL_COUNTER_ID) {
            if (System.nanoTime() >= deadlineNs) {
                throw new IllegalStateException("[SequencerService] replayer recording did not start within timeout");
            }
            cluster.idleStrategy().idle();
        }
    }

    @Override
    public void onSessionOpen(final ClientSession session, final long timestamp) {
        final long globalSeq = ++globalSeqNo;
        clientConnEncoder.wrapAndApplyHeader(encodeBuffer, 0, headerEncoder);
        clientConnEncoder.header()
            .sourceId(NO_SOURCE_ID)
            .connectionId(NO_SOURCE_ID)
            .sessionId(session.id())
            .globalSeqNo(globalSeq)
            .timestamp(timestamp);
        emit(MessageHeaderEncoder.ENCODED_LENGTH + clientConnEncoder.encodedLength());
    }

    @Override
    public void onSessionClose(final ClientSession session, final long timestamp, final CloseReason closeReason) {
        final long globalSeq = ++globalSeqNo;
        clientDiscEncoder.wrapAndApplyHeader(encodeBuffer, 0, headerEncoder);
        clientDiscEncoder.header()
            .sourceId(NO_SOURCE_ID)
            .connectionId(NO_SOURCE_ID)
            .sessionId(session.id())
            .globalSeqNo(globalSeq)
            .timestamp(timestamp);
        emit(MessageHeaderEncoder.ENCODED_LENGTH + clientDiscEncoder.encodedLength());
    }

    @Override
    public void onSessionMessage(final ClientSession session, final long timestamp, final DirectBuffer buffer,
                                 final int offset, final int length, final Header header) {
        final long sourceSessionId = session.id();
        final long globalSeq = ++globalSeqNo;

        // Decode just enough of the ingress (schema 200) message to re-stamp
        // it: the outer framing header (for templateId/blockLength) and the
        // `header` composite (for sourceId/connectionId) — both at fixed
        // offsets, independent of message type.
        ingressMsgHeaderDecoder.wrap(buffer, offset);
        final int templateId = ingressMsgHeaderDecoder.templateId();
        final int ingressBlockLen = ingressMsgHeaderDecoder.blockLength();

        final int ingressBodyOffset = offset + MessageHeaderDecoder.ENCODED_LENGTH;
        ingressHeaderDecoder.wrap(buffer, ingressBodyOffset);
        final int sourceId = ingressHeaderDecoder.sourceId();
        final int connectionId = ingressHeaderDecoder.connectionId();

        // sbe-sequenced.xml's header composite is sbe-unsequenced.xml's plus
        // two int64 fields (globalSeqNo, timestamp); every other field is
        // byte-identical, so the egress blockLength is simply the ingress
        // blockLength with the header composite's growth added on.
        final int egressBlockLen = HeaderEncoder.ENCODED_LENGTH + (ingressBlockLen - HeaderDecoder.ENCODED_LENGTH);

        headerEncoder.wrap(encodeBuffer, 0)
            .blockLength(egressBlockLen)
            .templateId(templateId)
            .schemaId(MessageHeaderEncoder.SCHEMA_ID)
            .version(MessageHeaderEncoder.SCHEMA_VERSION);

        final int egressBodyOffset = MessageHeaderEncoder.ENCODED_LENGTH;
        egressHeaderEncoder.wrap(encodeBuffer, egressBodyOffset)
            .sourceId(sourceId)
            .connectionId(connectionId)
            .sessionId(sourceSessionId)
            .globalSeqNo(globalSeq)
            .timestamp(timestamp);

        // Copy every byte after the ingress header composite — the rest of
        // the fixed block plus all var-data — verbatim; see class Javadoc.
        final int copyFromOffset = ingressBodyOffset + HeaderDecoder.ENCODED_LENGTH;
        final int copyLength = length - MessageHeaderDecoder.ENCODED_LENGTH - HeaderDecoder.ENCODED_LENGTH;
        encodeBuffer.putBytes(egressBodyOffset + HeaderEncoder.ENCODED_LENGTH, buffer, copyFromOffset, copyLength);

        emit(egressBodyOffset + HeaderEncoder.ENCODED_LENGTH + copyLength);
    }

    @Override
    public void onTimerEvent(final long correlationId, final long timestamp) {
        if (correlationId == TICK_TIMER_CORRELATION_ID) {
            emitTick(timestamp);
            scheduleTick();
        }
    }

    // Emits one internal clock frame carrying the consensus timestamp. Fires on every node (onTimerEvent
    // is a committed log event delivered identically to all), so like every other emit here it advances
    // each node's byte-identical tap and consumes a globalSeqNo on every node in the same order. Consumers
    // (the FIX gateway watchdog above all) read header.timestamp off it to keep their session clock moving
    // while a counterparty is silent. See TICK_INTERVAL_MS for the trade-off.
    private void emitTick(final long timestamp) {
        final long globalSeq = ++globalSeqNo;
        tickEncoder.wrapAndApplyHeader(encodeBuffer, 0, headerEncoder);
        tickEncoder.header()
            .sourceId(NO_SOURCE_ID)
            .connectionId(NO_SOURCE_ID)
            .sessionId(NO_SOURCE_ID)
            .globalSeqNo(globalSeq)
            .timestamp(timestamp);
        emit(MessageHeaderEncoder.ENCODED_LENGTH + tickEncoder.encodedLength());
    }

    // Arms the single repeating tick timer for one TICK_INTERVAL_MS ahead of current cluster time. Spins
    // until the schedule lands, mirroring emit()'s reliable-offer discipline: a dropped reschedule would
    // stop the cluster clock. Deadlines are in the cluster's time unit (milliseconds — the Aeron default,
    // not overridden in SequencerNode), matching cluster.time().
    private void scheduleTick() {
        final long deadline = cluster.time() + TICK_INTERVAL_MS;
        while (!cluster.scheduleTimer(TICK_TIMER_CORRELATION_ID, deadline)) {
            cluster.idleStrategy().idle();
        }
    }

    // ── Snapshot ──────────────────────────────────────────────────────────────

    @Override
    public void onTakeSnapshot(final ExclusivePublication snapshotPublication) {
        encodeBuffer.putLong(0, globalSeqNo);
        long offerResult;
        do {
            offerResult = snapshotPublication.offer(encodeBuffer, 0, Long.BYTES);
            if (offerResult == ExclusivePublication.CLOSED
                || offerResult == ExclusivePublication.MAX_POSITION_EXCEEDED) {
                throw new IllegalStateException("[SequencerService] Snapshot publication failed: " + offerResult);
            }
            if (offerResult < 0) {
                cluster.idleStrategy().idle();
            }
        } while (offerResult < 0);
    }

    private void loadSnapshot(final Image snapshotImage) {
        final FragmentAssembler handler = new FragmentAssembler((buf, off, len, hdr) -> globalSeqNo = buf.getLong(off));
        while (!snapshotImage.isClosed()) {
            cluster.idleStrategy().idle(snapshotImage.poll(handler, SNAPSHOT_POLL_BATCH));
        }
    }

    // ── Leadership ────────────────────────────────────────────────────────────

    @Override
    public void onNewLeadershipTermEvent(final long logPosition, final long leadershipTermId, final long timestamp,
                                         final long termBaseLogPosition, final int leaderMemberId,
                                         final int logSessionId, final TimeUnit timeUnit, final int appVersion) {
        applyLeadership(leaderMemberId, timestamp);
        // Arm (or re-arm) the internal cluster clock here rather than in onStart, where Aeron forbids
        // scheduling timers. Fires on every node when a term begins (cold start and every failover);
        // scheduleTick is idempotent by correlation id, and the timer then re-arms itself in
        // onTimerEvent, so the clock runs continuously across leadership changes.
        scheduleTick();
    }

    @Override
    public void onRoleChange(final Cluster.Role newRole) {
        // Deliberately a no-op: onNewLeadershipTermEvent already fires on every node (leader and
        // followers alike) with an explicit leaderMemberId, which is what applyLeadership needs to stamp
        // newLeaderMemberId onto the LeadershipChanged event. Keying everything off one authoritative
        // event (rather than also reacting here) removes a class of double-firing/idempotency bugs.
    }

    // Called on every node whenever a new leadership term begins (including this node's own promotion).
    // With every node recording its own tap there is no leader-only publication or standby-follow to
    // manage here any more (both retired in the tap-recording change) — the only per-leadership work is
    // synthesizing a LeadershipChanged event (ReplayerService design §3). Every node consumes
    // onNewLeadershipTermEvent in the same log order, so ++globalSeqNo here (on every node, exactly like
    // onSessionOpen/onSessionMessage) keeps the counter identical across nodes, and each node stamps that
    // same globalSeqNo onto a LeadershipChanged it emits onto its own tap — the per-node replicas use it
    // to track the current leader at one exact point in the ordered stream.
    private void applyLeadership(final int leaderMemberId, final long timestamp) {
        if (leaderMemberId == currentLeaderMemberId) {
            return;
        }
        currentLeaderMemberId = leaderMemberId;
        final boolean leader = leaderMemberId == cluster.memberId();
        final long globalSeq = ++globalSeqNo;
        System.out.printf("[SequencerService/%d] leadership change: new leader is memberId=%d (isLeader=%b)%n",
                          cluster.memberId(), leaderMemberId, leader);

        // Encoded and emitted on every node, so each node's replayer (and its recording) carries this
        // globalSeqNo gap-free.
        leadershipChangedEncoder.wrapAndApplyHeader(encodeBuffer, 0, headerEncoder);
        leadershipChangedEncoder.header()
            .sourceId(NO_SOURCE_ID)
            .connectionId(NO_SOURCE_ID)
            .sessionId(NO_SOURCE_ID)
            .globalSeqNo(globalSeq)
            .timestamp(timestamp);
        leadershipChangedEncoder.newLeaderMemberId(leaderMemberId);
        emit(MessageHeaderEncoder.ENCODED_LENGTH + leadershipChangedEncoder.encodedLength());
    }

    @Override
    public void onTerminate(final Cluster cluster) {
        if (aeronArchive != null) {
            aeronArchive.close();
        }
        if (replayerPub != null) {
            // Closing the publication ends the recording's source image, so the archive stops the tap
            // recording (sets its stopPosition) without an explicit stopRecording call.
            replayerPub.close();
        }
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    // Publishes the frame in encodeBuffer[0, length) onto the node-local tap, which every node records
    // into its own local archive as the authoritative sequenced history. Reliable: spins until the offer
    // lands, because a dropped frame would be an unrecoverable hole in the recording. Unlike the retired
    // UDP global stream this cannot wedge structurally — the only tethered subscriber of the tap is the
    // co-located archive recording (the app replicas' tap subscriptions are untethered, so a slow app is
    // dropped, not back-pressuring), so this blocks only on real local-archive write back-pressure, which
    // clears as the archive drains to disk.
    private void emit(final int length) {
        int idleSpins = 0;
        long result;
        while ((result = replayerPub.offer(encodeBuffer, 0, length)) < 0) {
            if (result == ExclusivePublication.CLOSED || result == ExclusivePublication.MAX_POSITION_EXCEEDED) {
                throw new IllegalStateException("[SequencerService] replayer publication failed: " + result);
            }
            if (++idleSpins >= MAX_BACK_PRESSURE_SPINS) {
                System.err.printf("[SequencerService] ALERT: replayer back-pressure at globalSeqNo=%d%n", globalSeqNo);
                idleSpins = 0;
            }
            cluster.idleStrategy().idle();
        }
    }

}
