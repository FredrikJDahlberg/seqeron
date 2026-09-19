package org.limitless.seqeron.sequencer;

import java.util.concurrent.TimeUnit;
import org.limitless.seqeron.sbe.frame.MessageHeaderDecoder;
import org.limitless.seqeron.sbe.frame.UnsequencedDecoder;
import org.limitless.seqeron.sbe.frame.UnsequencedHeaderDecoder;

/**
 * The frame layer's constants: the tap's identity, the cluster clock, and the size limits of spec §12.
 * Java's compiled-in mirror of the spec (C++'s is {@code SequencedFrame.hpp}); a change is a wire change
 * (<b>V-3</b>) and lands in both. Here because producer, sequencer and consumer all answer to them. Never
 * read from a transport: <b>S-3</b> forbids checking a node's own MTU, which would fork {@code globalSeqNo}.
 */
public final class FrameLayer {
    /**
     * Largest body or payload any frame may carry (<b>T-2</b>): the pinned 1408-byte MTU less the
     * 92-byte ingress header stack.
     */
    public static final int MAX_PAYLOAD_LENGTH = 1316;

    /**
     * Smallest ingress frame of either family, 28 bytes: framing header, 18-byte header composite and the
     * body's 2-byte length prefix. An empty body is legal.
     */
    public static final int MIN_INGRESS_LENGTH = MessageHeaderDecoder.ENCODED_LENGTH +
                                                 UnsequencedHeaderDecoder.ENCODED_LENGTH +
                                                 UnsequencedDecoder.payloadHeaderLength();

    /** Largest ingress frame: a full payload behind that framing — 1344 bytes. §9.2 condition 1's ceiling. */
    public static final int MAX_INGRESS_LENGTH = MIN_INGRESS_LENGTH + MAX_PAYLOAD_LENGTH;

    /**
     * The tap: the node-local IPC stream every node republishes each sequenced frame on, in {@code
     * globalSeqNo} order, and records into its own archive. Co-located apps follow it live.
     */
    public static final String FEEDER_CHANNEL = "aeron:ipc";

    /** Stream id of the tap. See {@link #FEEDER_CHANNEL}. */
    public static final int FEEDER_STREAM_ID = 205;

    /**
     * Period of the cluster clock: every node emits a {@code ClusterHeartbeat} carrying the consensus
     * timestamp, so consumers have a clock that advances while producers are silent. The sequencer's own
     * deadlines are quantised to it, and a consumer's tap watchdog must span whole periods. Each beat grows
     * the log, so this trades clock resolution against recovery time.
     */
    public static final long CLUSTER_HEARTBEAT_INTERVAL_MS = 1000;

    /** {@link #CLUSTER_HEARTBEAT_INTERVAL_MS} in consensus time, which is epoch nanoseconds. */
    public static final long CLUSTER_HEARTBEAT_INTERVAL_NS =
        TimeUnit.MILLISECONDS.toNanos(CLUSTER_HEARTBEAT_INTERVAL_MS);

    private FrameLayer() {
    }
}
