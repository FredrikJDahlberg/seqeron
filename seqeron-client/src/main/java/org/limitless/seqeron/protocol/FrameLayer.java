package org.limitless.seqeron.protocol;

import java.util.concurrent.TimeUnit;
import org.limitless.seqeron.sbe.frame.MessageHeaderDecoder;
import org.limitless.seqeron.sbe.frame.UnsequencedDecoder;
import org.limitless.seqeron.sbe.frame.UnsequencedHeaderDecoder;

/**
 * The frame layer's constants: the queue's identity, the cluster clock, and the size limits of spec §12.
 * Java's compiled-in mirror of the spec (C++'s is {@code SequencedFrame.hpp}); a change is a wire change
 * (<b>V-3</b>) and lands in both. Here because producer, sequencer and consumer all answer to them. Never
 * read from a transport: <b>S-3</b> forbids checking a node's own MTU, which would fork {@code clusterSeqNo}.
 */
public final class FrameLayer {
    /**
     * Largest payload any frame may carry (<b>T-2</b>): an 8960-byte IPC MTU, one jumbo frame, less the 86
     * bytes of Aeron data header and sequenced envelope, so a queue whose MTU is that large never fragments.
     */
    public static final int MAX_PAYLOAD_LENGTH = 8874;

    /**
     * Smallest ingress frame of either family, 30 bytes: framing header, 20-byte header composite and the
     * payload's 2-byte length prefix. An empty payload is legal.
     */
    public static final int MIN_INGRESS_LENGTH = MessageHeaderDecoder.ENCODED_LENGTH +
                                                 UnsequencedHeaderDecoder.ENCODED_LENGTH +
                                                 UnsequencedDecoder.payloadHeaderLength();

    /** Largest ingress frame: a full payload behind that framing — 8904 bytes. §9.2 condition 1's ceiling. */
    public static final int MAX_INGRESS_LENGTH = MIN_INGRESS_LENGTH + MAX_PAYLOAD_LENGTH;

    /**
     * The queue: the node-local IPC stream every node republishes each sequenced frame on, in {@code
     * clusterSeqNo} order, and records into its own archive. Co-located apps follow it live. Each group's queue
     * is this channel on its own stream, {@link #queueStreamId}.
     */
    public static final String QUEUE_CHANNEL = "aeron:ipc?alias=seqeron-queue";

    /** The width of {@code groupMask}: {@code groupId}s run 1 to 16. */
    public static final int MAX_GROUPS = 16;

    /** Group {@code groupId}'s queue is stream {@code QUEUE_STREAM_ID_BASE + groupId}. */
    public static final int QUEUE_STREAM_ID_BASE = 2000;

    /**
     * @param groupId 1 to {@link #MAX_GROUPS}
     * @return the stream id of that group's queue
     */
    public static int queueStreamId(final int groupId) {
        return QUEUE_STREAM_ID_BASE + groupId;
    }

    /**
     * Period of the cluster clock: every node emits a {@code ClusterHeartbeat} carrying the consensus
     * timestamp, so consumers have a clock that advances while producers are silent. The sequencer's own
     * deadlines are quantised to it, and a consumer's queue watchdog must span whole periods. Each beat grows
     * the log, so this trades clock resolution against recovery time.
     */
    public static final long CLUSTER_HEARTBEAT_INTERVAL_MS = 1000;

    /** {@link #CLUSTER_HEARTBEAT_INTERVAL_MS} in consensus time, which is epoch nanoseconds. */
    public static final long CLUSTER_HEARTBEAT_INTERVAL_NS =
        TimeUnit.MILLISECONDS.toNanos(CLUSTER_HEARTBEAT_INTERVAL_MS);

    private FrameLayer() {
    }
}
