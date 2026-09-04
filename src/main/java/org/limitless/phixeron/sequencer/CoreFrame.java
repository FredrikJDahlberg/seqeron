package org.limitless.phixeron.sequencer;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.limitless.phixeron.sbe.frame.MessageHeaderEncoder;
import org.limitless.phixeron.sbe.frame.UnsequencedEncoder;

/**
 * The ingress side of the frame layer: wraps an already-encoded payload in an {@code Unsequenced}
 * envelope. The Java twin of {@code publishPayload} in {@code sequencer/IngressPublisher.hpp}.
 *
 * <p>Encoding only — every producer owns its own buffer and its own way of offering, so this returns a
 * length and leaves the offer to the caller. The envelope is the cluster tier's and is the same for every
 * protocol; what is inside it is the application's, and seqeron never opens anything but core.
 */
public final class CoreFrame {
    /**
     * The one {@code payloadId} the cluster tier owns and decodes: seqeron's own core payloads
     * (doc/seqeron-protocol-spec.md §6.1). One definition, referenced by producer, sequencer and consumer.
     */
    public static final int PAYLOAD_ID = 1;

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
        return wrapPayload(frame, sourceId, connectionId, sessionId, PAYLOAD_ID, payload, payloadLength);
    }

    /**
     * The same, for an application's own payload rather than core's.
     *
     * @param frame         where the frame is written, from offset 0
     * @param sourceId      the producing gateway's {@code gatewaySourceId}; -1 is reserved for the cluster
     * @param connectionId  the connection this frame belongs to, or -1 for a gateway-scoped one
     * @param sessionId     this process's cluster session; advisory, the sequencer overwrites it
     * @param payloadId     names the payload's decoder namespace and encoding; 0 is invalid on the wire
     * @param payload       the payload, its own 8-byte {@code MessageHeader} included
     * @param payloadLength bytes of {@code payload} to carry
     * @return the frame's length in bytes
     */
    public static int wrapPayload(final MutableDirectBuffer frame, final int sourceId, final int connectionId,
                                  final long sessionId, final int payloadId, final DirectBuffer payload,
                                  final int payloadLength) {
        final UnsequencedEncoder encoder = new UnsequencedEncoder();
        encoder.wrapAndApplyHeader(frame, 0, new MessageHeaderEncoder());
        encoder.header().sourceId(sourceId).connectionId(connectionId).sessionId(sessionId).payloadId(payloadId);
        encoder.putPayload(payload, 0, payloadLength);
        return MessageHeaderEncoder.ENCODED_LENGTH + encoder.encodedLength();
    }
}
