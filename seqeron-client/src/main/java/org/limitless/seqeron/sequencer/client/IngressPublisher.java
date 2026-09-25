package org.limitless.seqeron.sequencer.client;

import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.limitless.seqeron.protocol.FrameLayer;
import org.limitless.seqeron.protocol.Publish;
import org.limitless.seqeron.protocol.SystemFrame;

/**
 * Encode-and-offer for cluster ingress — the Java twin of {@code sequencer/client/IngressPublisher.hpp}. An
 * instance owning its buffer rather than free functions over a stack array, and the payload arrives
 * pre-encoded rather than through a {@code Fill}, since Java's SBE codecs share no interface.
 *
 * <p>Given an {@link IngressTracker} (spec §16 A-4, A-5), each published frame is tracked under the sender's
 * session and term, and nothing is sent while the tracker holds or is full.
 *
 * <p>Not thread-safe: one publisher per producing thread.
 */
public final class IngressPublisher {
    private final ExpandableArrayBuffer frame = new ExpandableArrayBuffer(FrameLayer.MAX_INGRESS_LENGTH);
    private final SystemFrame envelope = new SystemFrame();
    private final IngressTracker tracker;

    /** A publisher that tracks nothing. */
    public IngressPublisher() {
        this(null);
    }

    /** A publisher that tracks every frame it places with {@code tracker}, and sends nothing while it holds. */
    public IngressPublisher(final IngressTracker tracker) {
        this.tracker = tracker;
    }

    /**
     * Wraps one application payload in an {@code Unsequenced} frame and offers it.
     *
     * <p>Two header fields are not parameters: the frame's template, and the cluster session, which is the
     * sender's. What varies is {@code sourceId} — for a reply, the requester's rather than this process's
     * own — {@code connectionId}, {@code payloadId}, and the payload.
     *
     * @param payload the payload's bytes, whatever encoding they carry: the tier copies them through
     *     unopened, so an SBE payload includes its own 8-byte {@code MessageHeader} and a raw one no framing
     *     at all
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
        if (tracker != null && (tracker.isHolding() || tracker.isFull())) {
            return Publish.Declined;
        }
        if (!sender.send(frame, length)) {
            return Publish.Declined;
        }
        if (tracker != null) {
            tracker.track(frame, length, sender.clusterSessionId(), sender.leadershipTermId());
        }
        return Publish.Published;
    }
}
