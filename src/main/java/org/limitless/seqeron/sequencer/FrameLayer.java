package org.limitless.seqeron.sequencer;

import org.limitless.seqeron.sbe.frame.MessageHeaderDecoder;
import org.limitless.seqeron.sbe.frame.UnsequencedDecoder;
import org.limitless.seqeron.sbe.frame.UnsequencedHeaderDecoder;

/**
 * The frame layer's constants: the sequenced stream's identity, its clock, and the size limits of
 * doc/seqeron-protocol-spec.md §12.
 *
 * <p><b>These belong to the protocol, and the spec is where they reside.</b> §12's table is the
 * definition; this class is Java's compiled-in mirror of it, and {@code sequencer/SequencedFrame.hpp}'s
 * {@code Limits} block is C++'s. A change starts in the spec and lands in both, and is a wire change
 * (<b>V-3</b>) whichever way round it is made.
 *
 * <p>They are no one participant's: the producer's encode methods enforce {@link #MAX_PAYLOAD_LENGTH}
 * (<b>T-3</b>), the sequencer's §9.2 condition 1 is the backstop behind it, and a consumer sizes its
 * buffers from the same numbers. So they are here rather than in {@link SystemFrame} (only the ingress
 * encoder) or {@link Sequencer} (only the replicated state machine) — either would make one
 * participant's class the authority for something all three answer to.
 *
 * <p><b>Compiled in, never read from a transport.</b> <b>S-3</b> forbids checking against a node's own
 * MTU: a node provisioned smaller than its peers would reject frames its peers admit and fork
 * {@code globalSeqNo}. The per-node question — whether this node's MTUs can carry the constant — is a
 * start-up check (<b>T-2</b>), never a per-frame one.
 */
public final class FrameLayer {
    /**
     * Largest body or payload any frame may carry (<b>T-2</b>): the pinned 1408-byte MTU less the
     * 92-byte ingress header stack.
     */
    public static final int MAX_PAYLOAD_LENGTH = 1316;

    /**
     * Smallest ingress frame of either family — 28 bytes: the framing header, the 18-byte header
     * composite and the body's own 2-byte length prefix. A frame this size carries an empty body, which
     * is legal. One constant for both templates, because both header composites are 18 bytes
     * (<b>F-3</b>).
     */
    public static final int MIN_INGRESS_LENGTH = MessageHeaderDecoder.ENCODED_LENGTH +
                                                 UnsequencedHeaderDecoder.ENCODED_LENGTH +
                                                 UnsequencedDecoder.payloadHeaderLength();

    /** Largest ingress frame: a full payload behind that framing — 1344 bytes. §9.2 condition 1's ceiling. */
    public static final int MAX_INGRESS_LENGTH = MIN_INGRESS_LENGTH + MAX_PAYLOAD_LENGTH;

    /**
     * Node-local IPC channel and stream the sequenced stream is tapped onto. Every node — leader
     * <em>and</em> follower — republishes each sequenced frame here in {@code globalSeqNo} order (the
     * taps are byte-identical across nodes, since every node processes the same committed log in the
     * same order) and records it into its own co-located archive. A co-located app follows it directly
     * as its live feed, and that node's {@code ReplayerService} serves history and gap replay off the
     * same recording.
     *
     * <p>The tap's identity, not the sequencer's: a consumer addresses this stream without knowing
     * anything about the node publishing it, which is why it sits beside the frame limits rather than
     * in {@link SequencerService}. The C++ side has it the same way round —
     * {@code sequencer/SequencedFrame.hpp} holds {@code FEEDER_STREAM_ID}.
     */
    public static final String FEEDER_CHANNEL = "aeron:ipc";

    /** Stream id of the tap. See {@link #FEEDER_CHANNEL}. */
    public static final int FEEDER_STREAM_ID = 205;

    /**
     * Period of the cluster clock ({@code Sequencer.clusterHeartbeat}): the leader fires this timer once per
     * second and every node emits a header-only {@code ClusterHeartbeat} carrying the consensus timestamp. It
     * exists so every consumer has a cluster-driven clock that keeps advancing even while an individual FIX
     * session is silent — which is exactly when the gateway's keepalive watchdog must probe/disconnect
     * (the sequenced-header timestamp is the only clock the watchdog is allowed to trust, since only the
     * leader assigns real time). 1 Hz gives ±1 s resolution, ample for the watchdog's tens-of-seconds
     * thresholds. Trade-off: every heartbeat appends a timer event + a heartbeat frame to the replicated
     * log/recording, so full-log-replay recovery grows with uptime; this constant is the single knob to
     * trade watchdog resolution against that cost. (A tighter win — gating clock emission on active FIX
     * sessions — is noted in doc/gap.md; 1 Hz is the low-risk interim.)
     *
     * <p>Here rather than in {@link Sequencer} for the reason the limits above are: every participant
     * answers to it. The state machine's own deadlines are evaluated in cluster time, on heartbeat
     * timestamps, so this is the resolution each of them is quantised to; and every consumer sizes its tap
     * watchdog in whole periods of it, since a watchdog tighter than the clock it watches fires on a
     * healthy stream. The C++ edge duplicates it as {@code FixGateway::CLUSTER_HEARTBEAT_INTERVAL_MS} for
     * want of a way to share it.
     */
    public static final long CLUSTER_HEARTBEAT_INTERVAL_MS = 1000;

    private FrameLayer() {
    }
}
