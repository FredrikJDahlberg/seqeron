package org.limitless.seqeron.protocol;

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
 * The ingress side of the frame layer: wraps an already-encoded body in its family's envelope and returns
 * the length; the offer is the caller's. The Java twin of {@code publishSystem}/{@code publishPayload} in
 * {@code sequencer/client/IngressPublisher.hpp}. A system body carries no {@code MessageHeader}: it is
 * {@code wrap}ped, and decoded with its codec's compiled constants (§7, <b>V-3</b>).
 *
 * <p>Not thread-safe: one instance per producing thread, reusing its encoders.
 */
public final class SystemFrame {
    /**
     * Returned instead of a length when the body is above {@link FrameLayer#MAX_PAYLOAD_LENGTH}, or the frame
     * breaks §9.2 conditions 6 to 9 — the checks a producer can make without the sequencer's state, so a
     * frame the sequencer would drop is never sent (<b>T-3</b>). Local and permanent: nothing was encoded,
     * and retrying cannot succeed.
     */
    public static final int REFUSED = -1;

    /** {@link #ingressBlockLength}'s answer for a {@code systemEventType} that may not be submitted. */
    public static final int NOT_INGRESS_LEGAL = -1;

    /** {@code sourceId} -1 is the cluster's own (<b>F-4</b>); §9.2 condition 6 refuses it on ingress. */
    private static final int CLUSTER_SOURCE_ID = -1;

    /** §9.2 condition 7: 0 names no protocol, and 1 is core's retired id. */
    private static final int RETIRED_CORE_PAYLOAD_ID = 1;

    /**
     * The {@code systemEventType} table (§7). A submitted event's value is its body codec's template id; the
     * three synthesized events have templates of their own and stamp these at offset 16 so that field
     * discriminates every frame on the tap.
     */
    public static final int CONNECTION_OPENED = ConnectionOpenedEncoder.TEMPLATE_ID;

    /** A connection closed at a producer; {@code header.connectionId} names it. */
    public static final int CONNECTION_CLOSED = ConnectionClosedEncoder.TEMPLATE_ID;

    /** Synthesis-only; {@code LeadershipChangedEncoder.TEMPLATE_ID} is the frame's, not this. */
    public static final int LEADERSHIP_CHANGED = 5;

    /** Operator marker: the cluster is up and open for the day. */
    public static final int CLUSTER_STARTED = ClusterStartedEncoder.TEMPLATE_ID;

    /** Operator marker: an orderly shutdown, the last business event in the log. */
    public static final int CLUSTER_STOPPED = ClusterStoppedEncoder.TEMPLATE_ID;

    /** Synthesis-only. */
    public static final int CLUSTER_HEARTBEAT = 16;

    /** One row of the topology list, and its {@code remaining} is the completeness edge. */
    public static final int GATEWAY_REGISTERED = GatewayRegisteredEncoder.TEMPLATE_ID;

    /** Synthesis-only. An operator asks for one with {@link #GATEWAY_ACTIVATION_REQUESTED}. */
    public static final int GATEWAY_ACTIVE = 18;

    /** A gateway instance announcing itself, which binds its session to its {@code gatewayId}. */
    public static final int GATEWAY_STARTED = GatewayStartedEncoder.TEMPLATE_ID;

    /** One {@code payloadId}'s name in this deployment; labelling only. */
    public static final int PAYLOAD_ID_REGISTERED = PayloadIdRegisteredEncoder.TEMPLATE_ID;

    /** An operator asking for one instance to be made active. */
    public static final int GATEWAY_ACTIVATION_REQUESTED = GatewayActivationRequestedEncoder.TEMPLATE_ID;

    /** One co-located application's row; labelling only, and no election behind it. */
    public static final int APPLICATION_REGISTERED = ApplicationRegisteredEncoder.TEMPLATE_ID;

    /**
     * The compiled {@code BLOCK_LENGTH} of the event {@code systemEventType} names, or {@link
     * #NOT_INGRESS_LEGAL} if it is unallocated or synthesis-only: §9.2 conditions 8 and 9 in one lookup.
     */
    public static int ingressBlockLength(final int systemEventType) {
        return switch (systemEventType) {
            case SystemFrame.CONNECTION_OPENED -> ConnectionOpenedEncoder.BLOCK_LENGTH;
            case SystemFrame.CONNECTION_CLOSED -> ConnectionClosedEncoder.BLOCK_LENGTH;
            case SystemFrame.CLUSTER_STARTED -> ClusterStartedEncoder.BLOCK_LENGTH;
            case SystemFrame.CLUSTER_STOPPED -> ClusterStoppedEncoder.BLOCK_LENGTH;
            case SystemFrame.GATEWAY_REGISTERED -> GatewayRegisteredEncoder.BLOCK_LENGTH;
            case SystemFrame.GATEWAY_STARTED -> GatewayStartedEncoder.BLOCK_LENGTH;
            case SystemFrame.PAYLOAD_ID_REGISTERED -> PayloadIdRegisteredEncoder.BLOCK_LENGTH;
            case SystemFrame.GATEWAY_ACTIVATION_REQUESTED -> GatewayActivationRequestedEncoder.BLOCK_LENGTH;
            case SystemFrame.APPLICATION_REGISTERED -> ApplicationRegisteredEncoder.BLOCK_LENGTH;
            default -> NOT_INGRESS_LEGAL;
        };
    }

    private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
    private final UnsequencedSystemEncoder systemEncoder = new UnsequencedSystemEncoder();
    private final UnsequencedEncoder payloadEncoder = new UnsequencedEncoder();

    /** One per producing thread. */
    public SystemFrame() {
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
     *     {@link FrameLayer#MAX_PAYLOAD_LENGTH} or the frame breaks §9.2 condition 6, 8 or 9
     */
    public int wrap(final MutableDirectBuffer frame, final int sourceId, final int connectionId,
                    final long sessionId, final int systemEventType, final DirectBuffer body,
                    final int bodyLength) {
        // Ahead of every wrap, so a refusal leaves the encoders exactly as it found them (T-3).
        final int blockLength = ingressBlockLength(systemEventType);
        if (bodyLength > FrameLayer.MAX_PAYLOAD_LENGTH || sourceId == CLUSTER_SOURCE_ID ||
            blockLength == NOT_INGRESS_LEGAL || bodyLength < blockLength) {
            return REFUSED;
        }
        systemEncoder.wrapAndApplyHeader(frame, 0, headerEncoder);
        systemEncoder.header().sourceId(sourceId).connectionId(connectionId).sessionId(sessionId)
            .systemEventType(systemEventType);
        systemEncoder.putBody(body, 0, bodyLength);
        return MessageHeaderEncoder.ENCODED_LENGTH + systemEncoder.encodedLength();
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
     *     {@link FrameLayer#MAX_PAYLOAD_LENGTH} or the frame breaks §9.2 condition 6 or 7
     */
    public int wrapPayload(final MutableDirectBuffer frame, final int sourceId, final int connectionId,
                           final long sessionId, final int payloadId, final DirectBuffer payload,
                           final int payloadLength) {
        if (payloadLength > FrameLayer.MAX_PAYLOAD_LENGTH || sourceId == CLUSTER_SOURCE_ID || payloadId == 0 ||
            payloadId == RETIRED_CORE_PAYLOAD_ID) {
            return REFUSED;
        }
        payloadEncoder.wrapAndApplyHeader(frame, 0, headerEncoder);
        payloadEncoder.header().sourceId(sourceId).connectionId(connectionId).sessionId(sessionId)
            .payloadId(payloadId);
        payloadEncoder.putPayload(payload, 0, payloadLength);
        return MessageHeaderEncoder.ENCODED_LENGTH + payloadEncoder.encodedLength();
    }
}
