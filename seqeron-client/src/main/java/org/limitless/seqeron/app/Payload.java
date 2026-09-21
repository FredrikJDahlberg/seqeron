package org.limitless.seqeron.app;

import org.agrona.DirectBuffer;
import org.limitless.seqeron.replayer.client.SequencedEvent;
import org.limitless.seqeron.sbe.frame.MessageHeaderDecoder;

/**
 * One application payload delivered in order, with the frame layer off it: the envelope is stripped, and
 * {@link #bodyOffset()} strips the payload's own {@code MessageHeader} too, which is where an SBE decoder
 * wraps. A payload that carries no header (spec §13.2) is addressed by {@link #payloadOffset()} instead.
 *
 * <p>Nothing of seqeron's own vocabulary reaches here — the system family is the façade's business, and a
 * frame of it never arrives as a payload. What is left is the payload's identity in the total order
 * ({@link #globalSeqNo()}), who sent it and on whose behalf ({@link #sourceId()},
 * {@link #connectionId()}), the consensus clock it was stamped with, and the bytes.
 *
 * <p>Dispatch on {@code (payloadId, templateId)}, never {@code templateId} alone: template ids are unique
 * per schema, so two applications' templates can collide.
 *
 * <p>A flyweight: valid only during the handler call; copy anything that must outlive it.
 */
public final class Payload {
    private SequencedEvent event;

    Payload() {
    }

    void wrap(final SequencedEvent event) {
        this.event = event;
    }

    /** Cluster-wide monotone sequence number; increments by exactly one per frame. */
    public long globalSeqNo() {
        return event.globalSeqNo();
    }

    /** The producer that submitted this frame (spec §5). */
    public int sourceId() {
        return event.sourceId();
    }

    /** The producer's connection this frame belongs to, or -1 for a producer-scoped one. */
    public int connectionId() {
        return event.connectionId();
    }

    /** The cluster session it was submitted on; a gateway pair shares its {@code sourceId} but not this. */
    public long sourceSessionId() {
        return event.sourceSessionId();
    }

    /** The Raft consensus timestamp, identical on every node. */
    public long clusterTimestampNs() {
        return event.clusterTimestampNs();
    }

    /** When this process read it — a delivery stamp, not the frame's. */
    public long receiveTimeNs() {
        return event.receiveTimeNs();
    }

    /** Which protocol the body speaks (spec §13). */
    public int payloadId() {
        return event.payloadId();
    }

    /** The body's own template within that protocol. */
    public int templateId() {
        return event.templateId();
    }

    /** For the decoder's {@code wrap}, from the payload's own {@code MessageHeader}. */
    public int blockLength() {
        return event.blockLength();
    }

    /** For the decoder's {@code wrap}, from the payload's own {@code MessageHeader}. */
    public int version() {
        return event.version();
    }

    /** The buffer the body sits in. */
    public DirectBuffer buffer() {
        return event.buffer();
    }

    /** Where the payload starts: past the envelope, its own {@code MessageHeader} included if it has one. */
    public int payloadOffset() {
        return event.payloadOffset();
    }

    /** The payload's length from {@link #payloadOffset()}. */
    public int payloadLength() {
        return event.payloadLength();
    }

    /** Where the body starts: past the envelope and past the payload's own {@code MessageHeader}. */
    public int bodyOffset() {
        return event.payloadOffset() + MessageHeaderDecoder.ENCODED_LENGTH;
    }

    /** The body's length from {@link #bodyOffset()}. */
    public int bodyLength() {
        return event.payloadLength() - MessageHeaderDecoder.ENCODED_LENGTH;
    }
}
