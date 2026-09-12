package org.limitless.seqeron.sequencer;

import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;

/**
 * Encode-and-offer for cluster ingress: the one preamble every producer writes before its own fields.
 * The Java twin of {@code sequencer/IngressPublisher.hpp} — same two operations, same three-valued
 * outcome.
 *
 * <p>Two differences from the C++ file, both forced by the language. It is an instance rather than a pair
 * of free functions, because the C++ ones encode into a stack array and Java's equivalent of that is a
 * buffer owned once rather than allocated per call. And the body arrives already encoded rather than
 * through a {@code Fill} callback over an encoder, because {@code sbe.java.generate.interfaces} is off:
 * Java's generated codecs share no type to be generic over.
 *
 * <p>Not thread-safe: one publisher per producing thread, like the buffer it holds.
 */
public final class IngressPublisher {
    /**
     * What a publish did.
     *
     * <p>Three-valued rather than a boolean for the reason the C++ twin gives: {@code Refused} is local
     * and permanent — the body is above {@link FrameLayer#MAX_PAYLOAD_LENGTH}, nothing was encoded and
     * nothing was offered, and a caller that retries is retrying something that can never succeed.
     * {@code Declined} is the transport's answer — back-pressure past the send's own spin, or a session
     * that is gone — and is the one a caller may retry.
     */
    public enum Publish {
        Published, Refused, Declined
    }

    private final ExpandableArrayBuffer frame = new ExpandableArrayBuffer(FrameLayer.MAX_INGRESS_LENGTH);
    private final SystemFrame envelope = new SystemFrame();

    /**
     * Wraps one application payload in an {@code Unsequenced} frame and offers it.
     *
     * <p>Two header fields are not parameters: the frame's template, and the cluster session, which is the
     * sender's. What varies is {@code sourceId} — for a reply, the requester's rather than this process's
     * own — {@code connectionId}, {@code payloadId}, and the payload.
     *
     * @param payload the payload, its own 8-byte {@code MessageHeader} included
     */
    public Publish publishPayload(final IngressSender sender, final int sourceId, final int connectionId,
                                  final int payloadId, final DirectBuffer payload, final int payloadLength) {
        final int length = envelope.wrapPayload(frame, sourceId, connectionId, sender.clusterSessionId(),
                                                payloadId, payload, payloadLength);
        return offer(sender, length);
    }

    /**
     * The same for one of seqeron's own events (doc/seqeron-protocol-spec.md §7), in an
     * {@code UnsequencedSystem} frame.
     *
     * @param body the event's SBE block, with no {@code MessageHeader} of its own — {@code systemEventType}
     *     is what names it
     */
    public Publish publishSystem(final IngressSender sender, final int sourceId, final int connectionId,
                                 final int systemEventType, final DirectBuffer body, final int bodyLength) {
        final int length = envelope.wrap(frame, sourceId, connectionId, sender.clusterSessionId(),
                                         systemEventType, body, bodyLength);
        return offer(sender, length);
    }

    private Publish offer(final IngressSender sender, final int length) {
        if (length == SystemFrame.REFUSED) {
            return Publish.Refused;
        }
        return sender.send(frame, length) ? Publish.Published : Publish.Declined;
    }
}
