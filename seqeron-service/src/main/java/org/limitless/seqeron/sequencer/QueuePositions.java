package org.limitless.seqeron.sequencer;

import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.BitUtil;

/**
 * Where a frame lands on a queue, by the arithmetic of Aeron's {@code ExclusivePublication.offer}: a queue position is
 * a function of the lengths of the frames before it, at a given term length and MTU, so one who derives the frames can
 * derive their positions (doc/disk-load-plan.md, step 2a).
 */
public final class QueuePositions {
    private QueuePositions() {
    }

    /**
     * How far a frame advances its queue: its length under one data header, aligned, or under one per fragment when it
     * exceeds the MTU.
     *
     * @param length of the frame
     * @param mtu    of the queue
     */
    public static int framedLength(final int length, final int mtu) {
        final int maxPayload = mtu - DataHeaderFlyweight.HEADER_LENGTH;
        final int remainder = length % maxPayload;
        final int last = remainder > 0 || length == 0
            ? BitUtil.align(remainder + DataHeaderFlyweight.HEADER_LENGTH, FrameDescriptor.FRAME_ALIGNMENT)
            : 0;
        return (length / maxPayload) * mtu + last;
    }

    /**
     * Where a frame offered at {@code position} starts: there, or at the next term when it does not fit the rest of
     * this one, which is then padded.
     *
     * @param position   the queue's position before the offer
     * @param length     of the frame
     * @param termLength of the queue
     * @param mtu        of the queue
     */
    public static long frameStart(final long position, final int length, final int termLength, final int mtu) {
        final long termOffset = position & (termLength - 1);
        return termOffset + framedLength(length, mtu) > termLength ? position - termOffset + termLength : position;
    }
}
