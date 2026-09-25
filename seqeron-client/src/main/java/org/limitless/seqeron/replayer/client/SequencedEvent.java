package org.limitless.seqeron.replayer.client;

import org.agrona.DirectBuffer;
import org.limitless.seqeron.protocol.SequencedFrameDecoder;

/**
 * One frame delivered in order off the sequenced stream, live or replayed — the Java twin of the C++
 * {@code SequencedEvent} in {@code protocol/SequencedFrame.hpp}. The envelope is stripped: split on
 * {@link #isSystem()}, then dispatch on {@code (payloadId, templateId)} or {@link #systemEventType()}. Every
 * system frame but {@code LeadershipChanged}, which has its own callback, arrives here.
 *
 * <p>A view over the {@link SequencedFrameDecoder} the receiver wraps over each frame, plus the two stamps
 * that belong to the delivery rather than to the frame — {@link #receiveTimeNs()} and {@link #position()}.
 *
 * <p>A flyweight: the buffer and every field are valid only during the handler call; copy to keep.
 */
public final class SequencedEvent {
    private final SequencedFrameDecoder frame;

    private long receiveTimeNs;
    private long position;

    /** Reads whichever frame {@code frame} is wrapped over; {@link #set} stamps the delivery. */
    SequencedEvent(final SequencedFrameDecoder frame) {
        this.frame = frame;
    }

    /** Cluster-wide monotone sequence number; increments by exactly one per frame. */
    public long globalSeqNo() {
        return frame.globalSeqNo();
    }

    /** Publishing gateway process ({@code header.sourceId}). */
    public int sourceId() {
        return frame.sourceId();
    }

    /** Connection at that gateway ({@code header.connectionId}); routes the reply. */
    public int connectionId() {
        return frame.connectionId();
    }

    /** Aeron Cluster client session the frame was submitted on ({@code header.sessionId}). */
    public long sourceSessionId() {
        return frame.sourceSessionId();
    }

    /** Cluster consensus time (epoch ns) at which the frame was committed. */
    public long clusterTimestampNs() {
        return frame.clusterTimestampNs();
    }

    /** Wall-clock ns at receipt by this client. */
    public long receiveTimeNs() {
        return receiveTimeNs;
    }

    /** Which protocol {@link #templateId()} belongs to; template ids are unique only within one. 0 on a system frame. */
    public int payloadId() {
        return frame.payloadId();
    }

    /** True if this frame is one of §7's system messages rather than an application payload. */
    public boolean isSystem() {
        return frame.isSystem();
    }

    /** Which of §7's twelve events this frame carries; 0 on an application frame. */
    public int systemEventType() {
        return frame.systemEventType();
    }

    /** The message's {@code messageHeader} templateId; picks the specific decode. */
    public int templateId() {
        return frame.templateId();
    }

    /**
     * What to wrap a decoder over {@link #payloadOffset()} with: the payload's own on an application frame, the
     * frame's own on a synthesized system frame, and 0 on a submitted one — whose decoder's compiled
     * {@code BLOCK_LENGTH} and {@code SCHEMA_VERSION} are the only ones there are.
     */
    public int blockLength() {
        return frame.blockLength();
    }

    /** See {@link #blockLength()}. */
    public int version() {
        return frame.version();
    }

    /** Buffer holding the message; valid only during the handler call. */
    public DirectBuffer buffer() {
        return frame.buffer();
    }

    /**
     * Offset within {@link #buffer()} of what a consumer decodes: the payload, its own 8-byte
     * {@code MessageHeader} included, on an application frame; the payload on a submitted system frame; the
     * frame's own block on one of the synthesized three.
     */
    public int payloadOffset() {
        return frame.payloadOffset();
    }

    /** Length in bytes of what {@link #payloadOffset()} addresses; the envelope is not in it. */
    public int payloadLength() {
        return frame.payloadLength();
    }

    /** Recording/stream position of this frame's first byte — what a resumed replay is anchored on. */
    public long position() {
        return position;
    }

    void set(final long receiveTimeNs, final long position) {
        this.receiveTimeNs = receiveTimeNs;
        this.position = position;
    }
}
