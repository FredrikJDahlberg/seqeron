package org.limitless.seqeron.sequencer;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.limitless.seqeron.sbe.frame.ApplicationRegisteredEncoder;
import org.limitless.seqeron.sbe.frame.ClusterStartedEncoder;
import org.limitless.seqeron.sbe.frame.ClusterStoppedEncoder;
import org.limitless.seqeron.sbe.frame.ConnectionClosedEncoder;
import org.limitless.seqeron.sbe.frame.ConnectionOpenedEncoder;
import org.limitless.seqeron.sbe.frame.GatewayActivationRequestedEncoder;
import org.limitless.seqeron.sbe.frame.GatewayRegisteredEncoder;
import org.limitless.seqeron.sbe.frame.GatewayStartedEncoder;
import org.limitless.seqeron.sbe.frame.MessageHeaderEncoder;
import org.limitless.seqeron.sbe.frame.PayloadIdRegisteredEncoder;
import org.limitless.seqeron.sbe.frame.UnsequencedEncoder;
import org.limitless.seqeron.sbe.frame.UnsequencedSystemEncoder;

/**
 * The ingress side of the frame layer: wraps an already-encoded body in the envelope of its family.
 * The Java twin of {@code publishSystem}/{@code publishPayload} in {@code sequencer/IngressPublisher.hpp}.
 *
 * <p>Encoding only — every producer owns its own buffer and its own way of offering, so this returns a
 * length and leaves the offer to the caller.
 *
 * <p><b>Two families, and the choice is the caller's</b> (doc/seqeron-protocol-spec.md §4). A system
 * frame carries seqeron's own vocabulary, named by {@code header.systemEventType} and decoded by the
 * cluster tier; an application frame carries one opaque payload named by {@code header.payloadId},
 * which seqeron never opens. The two headers are byte-for-byte identical apart from that field's name.
 *
 * <p><b>A system body carries no {@code MessageHeader}.</b> {@code systemEventType} is what names the
 * event, so an encoder {@code wrap}s rather than {@code wrapAndApplyHeader}s and a decoder supplies
 * {@code BLOCK_LENGTH} and {@code SCHEMA_VERSION} from its own compiled constants (§7, <b>V-3</b>).
 */
public final class SystemFrame {
    /**
     * {@link #wrap} and {@link #wrapPayload} return this instead of a length when the body is above
     * {@link FrameLayer#MAX_PAYLOAD_LENGTH}. Enforcing it here rather than leaving §9.2 condition 1 to
     * catch it on the far side of a transport is <b>T-3</b>; the constant itself is the protocol's
     * ({@link FrameLayer}), not this class's.
     *
     * <p><b>T-3.</b> The refusal is local and permanent, and is not back-pressure: nothing was encoded and
     * nothing may be offered, and a caller that retries is retrying something that can never succeed. What
     * it does instead is its own business (<b>P-0</b>) -- chunk, drop, or fail the session -- but it must
     * not be to try again. Distinguishable from every length a successful encode can return, which is why
     * this is a sentinel rather than a zero.
     */
    public static final int REFUSED = -1;

    /**
     * The {@code systemEventType} table (§7). The nine submitted events are their own body codec's
     * template id — the numbers they have always held, so a recording made by an older build can never
     * read as one of these. The three the sequencer synthesizes have no body codec: a top-level template
     * names each of them, and these are the values they nonetheless stamp at offset 16 so that field
     * discriminates every frame on the tap.
     */
    public static final int CONNECTION_OPENED = ConnectionOpenedEncoder.TEMPLATE_ID;

    public static final int CONNECTION_CLOSED = ConnectionClosedEncoder.TEMPLATE_ID;

    /** Synthesis-only; {@code LeadershipChangedEncoder.TEMPLATE_ID} is the frame's, not this. */
    public static final int LEADERSHIP_CHANGED = 5;

    public static final int CLUSTER_STARTED = ClusterStartedEncoder.TEMPLATE_ID;

    public static final int CLUSTER_STOPPED = ClusterStoppedEncoder.TEMPLATE_ID;

    /** Synthesis-only. */
    public static final int CLUSTER_HEARTBEAT = 16;

    public static final int GATEWAY_REGISTERED = GatewayRegisteredEncoder.TEMPLATE_ID;

    /** Synthesis-only. An operator asks for one with {@link #GATEWAY_ACTIVATION_REQUESTED}. */
    public static final int GATEWAY_ACTIVE = 18;

    public static final int GATEWAY_STARTED = GatewayStartedEncoder.TEMPLATE_ID;

    public static final int PAYLOAD_ID_REGISTERED = PayloadIdRegisteredEncoder.TEMPLATE_ID;

    public static final int GATEWAY_ACTIVATION_REQUESTED = GatewayActivationRequestedEncoder.TEMPLATE_ID;

    public static final int APPLICATION_REGISTERED = ApplicationRegisteredEncoder.TEMPLATE_ID;

    private SystemFrame() {
    }

    /**
     * Encodes one {@code UnsequencedSystem} frame around {@code body}.
     *
     * @param frame           where the frame is written, from offset 0
     * @param sourceId        the producing process's {@code gatewaySourceId}; -1 is reserved for the cluster
     * @param connectionId    the connection this frame belongs to, or -1 for a producer-scoped one
     * @param sessionId       this process's cluster session; advisory, the sequencer overwrites it
     * @param systemEventType which of §7's submitted events {@code body} holds
     * @param body            the event's SBE block, with no {@code MessageHeader} of its own
     * @param bodyLength      bytes of {@code body} to carry
     * @return the frame's length in bytes, or {@link #REFUSED} if {@code bodyLength} is above
     *     {@link FrameLayer#MAX_PAYLOAD_LENGTH}
     */
    public static int wrap(final MutableDirectBuffer frame, final int sourceId, final int connectionId,
                           final long sessionId, final int systemEventType, final DirectBuffer body,
                           final int bodyLength) {
        if (bodyLength > FrameLayer.MAX_PAYLOAD_LENGTH) {
            return REFUSED;
        }
        final UnsequencedSystemEncoder encoder = new UnsequencedSystemEncoder();
        encoder.wrapAndApplyHeader(frame, 0, new MessageHeaderEncoder());
        encoder.header().sourceId(sourceId).connectionId(connectionId).sessionId(sessionId)
            .systemEventType(systemEventType);
        encoder.putBody(body, 0, bodyLength);
        return MessageHeaderEncoder.ENCODED_LENGTH + encoder.encodedLength();
    }

    /**
     * The same, for an application's own payload rather than a system event's body.
     *
     * @param frame         where the frame is written, from offset 0
     * @param sourceId      the producing process's {@code gatewaySourceId}; -1 is reserved for the cluster
     * @param connectionId  the connection this frame belongs to, or -1 for a producer-scoped one
     * @param sessionId     this process's cluster session; advisory, the sequencer overwrites it
     * @param payloadId     names the payload's decoder namespace and encoding; 0 and 1 are invalid on the wire
     * @param payload       the payload, its own 8-byte {@code MessageHeader} included
     * @param payloadLength bytes of {@code payload} to carry
     * @return the frame's length in bytes, or {@link #REFUSED} if {@code payloadLength} is above
     *     {@link FrameLayer#MAX_PAYLOAD_LENGTH}
     */
    public static int wrapPayload(final MutableDirectBuffer frame, final int sourceId, final int connectionId,
                                  final long sessionId, final int payloadId, final DirectBuffer payload,
                                  final int payloadLength) {
        if (payloadLength > FrameLayer.MAX_PAYLOAD_LENGTH) {
            return REFUSED;
        }
        final UnsequencedEncoder encoder = new UnsequencedEncoder();
        encoder.wrapAndApplyHeader(frame, 0, new MessageHeaderEncoder());
        encoder.header().sourceId(sourceId).connectionId(connectionId).sessionId(sessionId).payloadId(payloadId);
        encoder.putPayload(payload, 0, payloadLength);
        return MessageHeaderEncoder.ENCODED_LENGTH + encoder.encodedLength();
    }
}
