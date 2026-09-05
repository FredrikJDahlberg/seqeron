package org.limitless.phixeron.sequencer;

import org.limitless.phixeron.sbe.frame.MessageHeaderDecoder;
import org.limitless.phixeron.sbe.frame.UnsequencedDecoder;
import org.limitless.phixeron.sbe.frame.UnsequencedHeaderDecoder;

/**
 * The frame layer's limits — doc/seqeron-protocol-spec.md §12.
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

    private FrameLayer() {
    }
}
