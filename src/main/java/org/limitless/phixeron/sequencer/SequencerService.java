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
import io.aeron.archive.codecs.SourceLocation;
import org.agrona.concurrent.NoOpLock;
import org.limitless.phixeron.sbe.unsequenced.MessageHeaderDecoder;
import org.limitless.phixeron.sbe.unsequenced.HeaderDecoder;
import org.limitless.phixeron.sbe.sequenced.MessageHeaderEncoder;
import org.limitless.phixeron.sbe.sequenced.HeaderEncoder;
import org.limitless.phixeron.sbe.sequenced.ClientConnectedEncoder;
import org.limitless.phixeron.sbe.sequenced.ClientDisconnectedEncoder;

import java.util.concurrent.TimeUnit;

/**
 * Aeron Cluster service that imposes a total order on messages arriving from multiple clients.
 *
 * <p>For every committed {@link #onSessionMessage} the service assigns a
 * <b>globalSeqNo</b> — a cluster-wide monotone counter shared across all sources and
 * lifecycle events (connect / disconnect) — and stamps it, together with the cluster
 * consensus timestamp, into the message's {@code header} composite before republishing it.
 *
 * <p>Ingress messages arrive already SBE-encoded as {@code sbe-unsequenced.xml} (schema
 * ID 200) — the FIX gateway encodes every admin and application FIX message that way and
 * offers it directly to the cluster, with {@code header.sourceId}/{@code header.sessionId}
 * identifying the submitting TCP connection and Aeron Cluster session. This service does
 * not need to know about individual FIX message types to re-stamp them: {@code
 * sbe-sequenced.xml} (schema ID 202) is deliberately kept byte-identical to {@code
 * sbe-unsequenced.xml} past the {@code header} composite (same field order/types/ids, same
 * var-data layout), so {@link #onSessionMessage} decodes only the outer {@code
 * MessageHeader} and the {@code header} composite (both always at a fixed offset,
 * regardless of {@code templateId}), then copies every remaining byte — the rest of the
 * fixed block plus all var-data — verbatim into a new {@code sbe-sequenced.xml} message
 * whose {@code header} composite carries the original {@code sourceId}/{@code sessionId}
 * plus the new {@code globalSeqNo}/{@code timestamp}.
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
 * <p><b>Snapshot format</b> (little-endian binary, single fragment):
 * <pre>
 *   int64  globalSeqNo
 * </pre>
 */
public final class SequencerService implements ClusteredService {

    /**
     * Multi-destination-cast (dynamic control mode) channel for the global sequenced stream.
     * This is the publisher/archive-recording channel: the leader's {@link ExclusivePublication}
     * and {@code startRecording} both use it. Subscribers connect with {@link
     * #GLOBAL_STREAM_SUBSCRIBER_CHANNEL} instead, which points at the same control address but
     * carries its own (ephemeral) data endpoint.
     */
    public static final String GLOBAL_STREAM_CHANNEL = "aeron:udp?control-mode=dynamic|control=localhost:9200";

    /**
     * Subscriber-side channel for the global stream's MDC dynamic control mode: same control
     * address as {@link #GLOBAL_STREAM_CHANNEL}, plus an ephemeral local data endpoint that the
     * publisher discovers and adds as a destination automatically.
     */
    public static final String GLOBAL_STREAM_SUBSCRIBER_CHANNEL =
        "aeron:udp?control-mode=dynamic|control=localhost:9200|endpoint=localhost:0";

    public static final int GLOBAL_STREAM_ID = 1;

    /**
     * Maximum consecutive back-pressure spins on the global stream before printing an alert.
     * At ~10 ns/spin this is ~10 ms per alert period.
     */
    private static final int MAX_BACK_PRESSURE_SPINS = 1_000_000;

    private static final int SNAPSHOT_POLL_BATCH = 10;

    /**
     * header.sourceId for lifecycle events synthesized by this service (ClientConnected /
     * ClientDisconnected): an Aeron Cluster session opening/closing has no TCP-level
     * connection id to carry, unlike the ingress messages it forwards.
     */
    private static final int NO_SOURCE_ID = -1;

    // ── SBE codecs — single conductor thread; no synchronisation needed ───────

    // Ingress decode (schema 200, sbe-unsequenced.xml). Only the outer framing
    // header and the generic `header` composite are ever decoded — body
    // fields are copied through as opaque bytes, see onSessionMessage.
    private final MessageHeaderDecoder ingressMsgHeaderDecoder = new MessageHeaderDecoder();
    private final HeaderDecoder        ingressHeaderDecoder    = new HeaderDecoder();

    // Egress encode (schema 202, sbe-sequenced.xml).
    private final MessageHeaderEncoder      headerEncoder     = new MessageHeaderEncoder();
    private final HeaderEncoder             egressHeaderEncoder = new HeaderEncoder();
    private final ClientConnectedEncoder    clientConnEncoder = new ClientConnectedEncoder();
    private final ClientDisconnectedEncoder clientDiscEncoder = new ClientDisconnectedEncoder();
    private final MutableDirectBuffer       encodeBuffer      = new ExpandableDirectByteBuffer(4096);

    // ── Sequencing state (snapshotted; updated on every node for determinism) ─

    /** Cluster-wide sequence counter; incremented for messages and lifecycle events. */
    private long globalSeqNo = 0;

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
        clientConnEncoder.wrapAndApplyHeader(encodeBuffer, 0, headerEncoder);
        clientConnEncoder.header()
            .sourceId(NO_SOURCE_ID)
            .sessionId(session.id())
            .globalSeqNo(globalSeq)
            .timestamp(timestamp);
        offerToGlobalStream(MessageHeaderEncoder.ENCODED_LENGTH + clientConnEncoder.encodedLength());
    }

    @Override
    public void onSessionClose(final ClientSession session,
                               final long timestamp,
                               final CloseReason closeReason) {
        final long globalSeq = ++globalSeqNo;
        if (!isLeader) {
            return;
        }
        clientDiscEncoder.wrapAndApplyHeader(encodeBuffer, 0, headerEncoder);
        clientDiscEncoder.header()
            .sourceId(NO_SOURCE_ID)
            .sessionId(session.id())
            .globalSeqNo(globalSeq)
            .timestamp(timestamp);
        offerToGlobalStream(MessageHeaderEncoder.ENCODED_LENGTH + clientDiscEncoder.encodedLength());
    }

    @Override
    public void onSessionMessage(final ClientSession session,
                                 final long          timestamp,
                                 final DirectBuffer  buffer,
                                 final int           offset,
                                 final int           length,
                                 final Header        header) {
        final long sourceSessionId = session.id();
        final long globalSeq       = ++globalSeqNo;
        if (!isLeader) {
            return;
        }

        // Decode just enough of the ingress (schema 200) message to re-stamp
        // it: the outer framing header (for templateId/blockLength) and the
        // `header` composite (for sourceId) — both at fixed offsets,
        // independent of message type.
        ingressMsgHeaderDecoder.wrap(buffer, offset);
        final int templateId      = ingressMsgHeaderDecoder.templateId();
        final int ingressBlockLen = ingressMsgHeaderDecoder.blockLength();

        final int ingressBodyOffset = offset + MessageHeaderDecoder.ENCODED_LENGTH;
        ingressHeaderDecoder.wrap(buffer, ingressBodyOffset);
        final int sourceId = ingressHeaderDecoder.sourceId();

        // sbe-sequenced.xml's header composite is sbe-unsequenced.xml's plus
        // two int64 fields (globalSeqNo, timestamp); every other field is
        // byte-identical, so the egress blockLength is simply the ingress
        // blockLength with the header composite's growth added on.
        final int egressBlockLen =
            HeaderEncoder.ENCODED_LENGTH + (ingressBlockLen - HeaderDecoder.ENCODED_LENGTH);

        headerEncoder.wrap(encodeBuffer, 0)
            .blockLength(egressBlockLen)
            .templateId(templateId)
            .schemaId(MessageHeaderEncoder.SCHEMA_ID)
            .version(MessageHeaderEncoder.SCHEMA_VERSION);

        final int egressBodyOffset = MessageHeaderEncoder.ENCODED_LENGTH;
        egressHeaderEncoder.wrap(encodeBuffer, egressBodyOffset)
            .sourceId(sourceId)
            .sessionId(sourceSessionId)
            .globalSeqNo(globalSeq)
            .timestamp(timestamp);

        // Copy every byte after the ingress header composite — the rest of
        // the fixed block plus all var-data — verbatim; see class Javadoc.
        final int copyFromOffset = ingressBodyOffset + HeaderDecoder.ENCODED_LENGTH;
        final int copyLength     = length - MessageHeaderDecoder.ENCODED_LENGTH - HeaderDecoder.ENCODED_LENGTH;
        encodeBuffer.putBytes(egressBodyOffset + HeaderEncoder.ENCODED_LENGTH, buffer, copyFromOffset, copyLength);

        offerToGlobalStream(egressBodyOffset + HeaderEncoder.ENCODED_LENGTH + copyLength);
    }

    @Override
    public void onTimerEvent(final long correlationId, final long timestamp) {
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
                throw new IllegalStateException(
                    "[SequencerService] Snapshot publication failed: " + offerResult);
            }
            if (offerResult < 0) {
                cluster.idleStrategy().idle();
            }
        } while (offerResult < 0);
    }

    private void loadSnapshot(final Image snapshotImage) {
        final FragmentAssembler handler = new FragmentAssembler(
            (buf, off, len, hdr) -> globalSeqNo = buf.getLong(off));
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
