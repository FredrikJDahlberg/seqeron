package org.limitless.seqeron.replayer.client;

import org.limitless.seqeron.protocol.SequencedFrameDecoder;

/**
 * One frame delivered in order off the sequenced stream, live or replayed — the Java twin of the C++
 * {@code SequencedEvent} in {@code protocol/SequencedFrame.hpp}. The envelope is stripped: split on
 * {@link #isSystem()}, then dispatch on {@code (payloadId, templateId)} or {@link #systemEventType()}. Every
 * system frame but {@code LeadershipChanged}, which has its own callback, arrives here.
 *
 * <p>The {@link SequencedFrameDecoder} the receiver wraps over each frame — only the receiver wraps it — plus the
 * two stamps that belong to the delivery rather than to the frame: {@link #receiveTimeNs()} and {@link #position()}.
 *
 * <p>A flyweight: the buffer and every field are valid only during the handler call; copy to keep.
 */
public final class SequencedEvent extends SequencedFrameDecoder {
    private long receiveTimeNs;
    private long position;

    SequencedEvent() {
    }

    /** Wall-clock ns at receipt by this client. */
    public long receiveTimeNs() {
        return receiveTimeNs;
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
