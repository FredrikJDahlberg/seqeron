package org.limitless.seqeron.sequencer;

import java.util.ArrayList;
import java.util.List;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.IntArrayList;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * A {@link Sequencer.FrameSink} that keeps a copy of every frame, in the order the sequencer published them, with
 * the queue each went to and the groups it activated.
 */
final class FrameRecorder implements Sequencer.FrameSink {
    private final List<byte[]> frames = new ArrayList<>();
    private final IntArrayList groupIds = new IntArrayList();
    private int[] activated;

    @Override
    public void activate(final int[] groupIds) {
        activated = groupIds.clone();
    }

    @Override
    public void publish(final int groupId, final DirectBuffer buffer, final int length) {
        final byte[] frame = new byte[length];
        buffer.getBytes(0, frame);
        frames.add(frame);
        groupIds.addInt(groupId);
    }

    /** The groups {@link #activate} was called with, or null if it was not. */
    int[] activated() {
        return activated;
    }

    /** Forgets every frame published so far. */
    void clear() {
        frames.clear();
        groupIds.clear();
    }

    /** The queue the frame at {@code index} went to. */
    int groupId(final int index) {
        return groupIds.getInt(index);
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
