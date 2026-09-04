package org.limitless.phixeron.sequencer;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.limitless.phixeron.sbe.frame.MessageHeaderEncoder;
import org.limitless.phixeron.sbe.frame.UnsequencedEncoder;

/**
 * The ingress side of the frame layer: wraps an already-encoded core payload in an {@code Unsequenced}
 * envelope. The Java twin of {@code publishCore} in {@code sequencer/IngressPublisher.hpp}.
 *
 * <p>Encoding only — every producer owns its own buffer and its own way of offering, so this returns a
 * length and leaves the offer to the caller.
 */
public final class CoreFrame {
    /**
     * The one {@code payloadId} the cluster tier owns and decodes: seqeron's own core payloads
     * (doc/seqeron-protocol-spec.md §6.1). One definition, referenced by producer, sequencer and consumer.
     */
    public static final int PAYLOAD_ID = 1;

    /** {@code payloadId} of a bare schema-202 message — not an envelope at all, and invalid on the wire. */
    public static final int NO_PAYLOAD_ID = 0;

    private CoreFrame() {
    }

    /**
     * Encodes one {@code Unsequenced} frame around {@code payload}.
     *
     * @param frame         where the frame is written, from offset 0
     * @param sourceId      the producing gateway's {@code gatewaySourceId}; -1 is reserved for the cluster
     * @param connectionId  the connection this frame belongs to, or -1 for a gateway-scoped one
     * @param sessionId     this process's cluster session; advisory, the sequencer overwrites it
     * @param payload       the core payload, its own 8-byte {@code MessageHeader} included
     * @param payloadLength bytes of {@code payload} to carry
     * @return the frame's length in bytes
     */
    public static int wrap(final MutableDirectBuffer frame, final int sourceId, final int connectionId,
                           final long sessionId, final DirectBuffer payload, final int payloadLength) {
        final UnsequencedEncoder encoder = new UnsequencedEncoder();
        encoder.wrapAndApplyHeader(frame, 0, new MessageHeaderEncoder());
        encoder.header().sourceId(sourceId).connectionId(connectionId).sessionId(sessionId).payloadId(PAYLOAD_ID);
        encoder.putPayload(payload, 0, payloadLength);
        return MessageHeaderEncoder.ENCODED_LENGTH + encoder.encodedLength();
    }
}
