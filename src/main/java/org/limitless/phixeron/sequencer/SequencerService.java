package org.limitless.phixeron.sequencer;

import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.FragmentAssembler;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableDirectByteBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.collections.LongLongConsumer;
import io.aeron.archive.codecs.SourceLocation;
import org.agrona.concurrent.NoOpLock;
import org.limitless.phixeron.sbe.sequencer.MessageHeaderEncoder;
import org.limitless.phixeron.sbe.sequencer.SequencedMessageEncoder;
import org.limitless.phixeron.sbe.sequencer.SourceConnectedEncoder;
import org.limitless.phixeron.sbe.sequencer.SourceDisconnectedEncoder;

import java.util.concurrent.TimeUnit;

/**
 * Aeron Cluster service that imposes a total order on messages arriving from multiple clients.
 *
 * <p>For every committed {@link #onSessionMessage} the service assigns:
 * <ul>
 *   <li><b>globalSeqNo</b>  — cluster-wide monotone counter shared across all sources and
 *       lifecycle events (connect / disconnect).</li>
 *   <li><b>appSeqNo</b>     — per-{@code sourceSessionId} monotone counter; allows each
 *       client to detect loss of its own messages independently.</li>
 * </ul>
 *
 * <p>The decorated message is published on the <em>global stream</em>
 * ({@link #GLOBAL_STREAM_CHANNEL} / {@link #GLOBAL_STREAM_ID}).
 * That channel is simultaneously recorded by the co-located Aeron Archive so C++ clients can
 * replay the full history on startup.
 *
 * <p><b>Leader-only publishing:</b> all cluster nodes maintain identical sequencing state
 * (updated on every callback), but only the leader writes to the global stream.  On failover
 * the new leader resumes from the snapshotted {@code globalSeqNo} and continues publishing.
 *
 * <p>Messages are SBE-encoded using the {@code sequencer.xml} schema (schema ID 201).
 *
 * <p><b>Snapshot format</b> (little-endian binary, single fragment):
 * <pre>
 *   int64  globalSeqNo
 *   int32  sourceCount
 *   [sourceCount × (int64 sourceSessionId + int64 appSeqNo)]
 * </pre>
 */
public final class SequencerService implements ClusteredService {

    /** UDP multicast channel for the global sequenced stream.  All clients subscribe here. */
    public static final String GLOBAL_STREAM_CHANNEL = "aeron:udp?endpoint=224.0.1.1:9200|interface=localhost";
    public static final int    GLOBAL_STREAM_ID      = 1;

    /**
     * Maximum consecutive back-pressure spins on the global stream before printing an alert.
     * At ~10 ns/spin this is ~10 ms per alert period.
     */
    private static final int MAX_BACK_PRESSURE_SPINS = 1_000_000;

    private static final int SNAPSHOT_POLL_BATCH = 10;

    // ── SBE encoders — single conductor thread; no synchronisation needed ─────

    private final MessageHeaderEncoder      headerEncoder  = new MessageHeaderEncoder();
    private final SequencedMessageEncoder   seqMsgEncoder  = new SequencedMessageEncoder();
    private final SourceConnectedEncoder    srcConnEncoder = new SourceConnectedEncoder();
    private final SourceDisconnectedEncoder srcDiscEncoder = new SourceDisconnectedEncoder();
    private final MutableDirectBuffer       encodeBuffer   = new ExpandableDirectByteBuffer(4096);

    // ── Sequencing state (snapshotted; updated on every node for determinism) ─

    /** Cluster-wide sequence counter; incremented for messages and lifecycle events. */
    private long globalSeqNo = 0;

    /**
     * Per-source application sequence counters keyed by {@link ClientSession#id()}.
     * Missing value is 0; the first message from a source gets appSeqNo 1.
     */
    private final Long2LongHashMap sourceAppSeqNos = new Long2LongHashMap(0L);

    // ── Aeron runtime (not snapshotted) ──────────────────────────────────────

    private Cluster              cluster;
    private boolean              isLeader;
    private ExclusivePublication globalStreamPub;
    private AeronArchive         aeronArchive;

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

        // Start recording the global stream (idempotent: re-calling after a restart is safe).
        aeronArchive.startRecording(
            GLOBAL_STREAM_CHANNEL, GLOBAL_STREAM_ID, SourceLocation.LOCAL);

        // Publication created on all nodes; only the leader calls offer().
        globalStreamPub = cluster.context().aeron()
            .addExclusivePublication(GLOBAL_STREAM_CHANNEL, GLOBAL_STREAM_ID);

        if (snapshotImage != null) {
            loadSnapshot(snapshotImage);
        }
    }

    @Override
    public void onSessionOpen(final ClientSession session, final long timestamp) {
        final long globalSeq = ++globalSeqNo;
        if (!isLeader) {
            return;
        }
        srcConnEncoder.wrapAndApplyHeader(encodeBuffer, 0, headerEncoder)
            .globalSeqNo(globalSeq)
            .sourceSessionId(session.id())
            .clusterTimestamp(timestamp);
        offerToGlobalStream(MessageHeaderEncoder.ENCODED_LENGTH + SourceConnectedEncoder.BLOCK_LENGTH);
    }

    @Override
    public void onSessionClose(final ClientSession session,
                               final long timestamp,
                               final CloseReason closeReason) {
        sourceAppSeqNos.remove(session.id());
        final long globalSeq = ++globalSeqNo;
        if (!isLeader) {
            return;
        }
        srcDiscEncoder.wrapAndApplyHeader(encodeBuffer, 0, headerEncoder)
            .globalSeqNo(globalSeq)
            .sourceSessionId(session.id())
            .clusterTimestamp(timestamp);
        offerToGlobalStream(MessageHeaderEncoder.ENCODED_LENGTH + SourceDisconnectedEncoder.BLOCK_LENGTH);
    }

    @Override
    public void onSessionMessage(final ClientSession session,
                                 final long          timestamp,
                                 final DirectBuffer  buffer,
                                 final int           offset,
                                 final int           length,
                                 final Header        header) {
        final long sourceId  = session.id();
        final long globalSeq = ++globalSeqNo;
        final long appSeq    = nextAppSeq(sourceId);
        if (!isLeader) {
            return;
        }
        seqMsgEncoder.wrapAndApplyHeader(encodeBuffer, 0, headerEncoder)
            .globalSeqNo(globalSeq)
            .sourceSessionId(sourceId)
            .appSeqNo(appSeq)
            .clusterTimestamp(timestamp)
            .putPayload(buffer, offset, length);
        offerToGlobalStream(MessageHeaderEncoder.ENCODED_LENGTH + seqMsgEncoder.encodedLength());
    }

    @Override
    public void onTimerEvent(final long correlationId, final long timestamp) {
    }

    // ── Snapshot ──────────────────────────────────────────────────────────────

    @Override
    public void onTakeSnapshot(final ExclusivePublication snapshotPublication) {
        int pos = 0;
        encodeBuffer.putLong(pos, globalSeqNo);
        pos += Long.BYTES;
        encodeBuffer.putInt(pos, sourceAppSeqNos.size());
        pos += Integer.BYTES;
        final int[] posRef = {pos};
        sourceAppSeqNos.longForEach((long srcId, long appSeq) -> {
            encodeBuffer.putLong(posRef[0], srcId);
            posRef[0] += Long.BYTES;
            encodeBuffer.putLong(posRef[0], appSeq);
            posRef[0] += Long.BYTES;
        });
        pos = posRef[0];
        long offerResult;
        do {
            offerResult = snapshotPublication.offer(encodeBuffer, 0, pos);
            if (offerResult == ExclusivePublication.CLOSED
                || offerResult == ExclusivePublication.MAX_POSITION_EXCEEDED) {
                throw new IllegalStateException(
                    "[SequencerService] Snapshot publication failed: " + offerResult);
            }
            if (offerResult < 0) {
                cluster.idleStrategy().idle();
            }
        } while (offerResult < 0);
    }

    private void loadSnapshot(final Image snapshotImage) {
        final FragmentAssembler handler = new FragmentAssembler((buf, off, len, hdr) -> {
            int pos = off;
            globalSeqNo = buf.getLong(pos);
            pos += Long.BYTES;
            final int count = buf.getInt(pos);
            pos += Integer.BYTES;
            for (int i = 0; i < count; i++) {
                final long srcId  = buf.getLong(pos); pos += Long.BYTES;
                final long appSeq = buf.getLong(pos); pos += Long.BYTES;
                sourceAppSeqNos.put(srcId, appSeq);
            }
        });
        while (!snapshotImage.isClosed()) {
            cluster.idleStrategy().idle(snapshotImage.poll(handler, SNAPSHOT_POLL_BATCH));
        }
    }

    // ── Leadership ────────────────────────────────────────────────────────────

    @Override
    public void onNewLeadershipTermEvent(final long logPosition,
                                          final long leadershipTermId,
                                          final long timestamp,
                                          final long termBaseLogPosition,
                                          final int  leaderMemberId,
                                          final int  logSessionId,
                                          final TimeUnit timeUnit,
                                          final int  appVersion) {
        isLeader = (leaderMemberId == cluster.memberId());
    }

    @Override
    public void onRoleChange(final Cluster.Role newRole) {
        isLeader = (newRole == Cluster.Role.LEADER);
    }

    @Override
    public void onTerminate(final Cluster cluster) {
        if (aeronArchive  != null) { aeronArchive.close(); }
        if (globalStreamPub != null) { globalStreamPub.close(); }
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    private long nextAppSeq(final long sourceId) {
        final long next = sourceAppSeqNos.get(sourceId) + 1L;
        sourceAppSeqNos.put(sourceId, next);
        return next;
    }

    private void offerToGlobalStream(final int length) {
        int idleSpins = 0;
        long result;
        while ((result = globalStreamPub.offer(encodeBuffer, 0, length)) < 0) {
            if (result == ExclusivePublication.CLOSED
                || result == ExclusivePublication.MAX_POSITION_EXCEEDED) {
                throw new IllegalStateException(
                    "[SequencerService] Global stream publication failed: " + result);
            }
            if (++idleSpins >= MAX_BACK_PRESSURE_SPINS) {
                System.err.printf(
                    "[SequencerService] ALERT: global stream back-pressure at globalSeqNo=%d%n",
                    globalSeqNo);
                idleSpins = 0;
            }
            cluster.idleStrategy().idle();
        }
    }
}
