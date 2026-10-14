package org.limitless.seqeron.sequencer;

import java.util.ArrayList;
import java.util.List;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

/** A {@link Sequencer.FrameSink} that keeps a copy of every frame, in the order the sequencer published them. */
final class FrameRecorder implements Sequencer.FrameSink {
    private final List<byte[]> frames = new ArrayList<>();

    @Override
    public void publish(final DirectBuffer buffer, final int length) {
        final byte[] frame = new byte[length];
        buffer.getBytes(0, frame);
        frames.add(frame);
    }

    /** How many frames have been published. */
    int count() {
        return frames.size();
    }

    /** The frame at {@code index}, in publication order. */
    MutableDirectBuffer frame(final int index) {
        return new UnsafeBuffer(frames.get(index));
    }

    /** The length of the frame at {@code index}. */
    int length(final int index) {
        return frames.get(index).length;
    }

    /** The frame published last. */
    MutableDirectBuffer last() {
        return frame(frames.size() - 1);
    }

    /** The length of the frame published last. */
    int lastLength() {
        return length(frames.size() - 1);
    }

    /** Every frame, in publication order. */
    List<byte[]> all() {
        return frames;
    }
}
