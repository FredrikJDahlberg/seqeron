package org.limitless.seqeron.replayer.server;

import java.util.ArrayDeque;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.limitless.seqeron.sequencer.QueuePositions;
import org.limitless.seqeron.sequencer.Sequencer;

/**
 * Puts a derivation's frames on the queues without waiting (doc/plans/queue-off-duty-cycle-plan.md, step 2): a frame a
 * queue does not take is kept, with every frame after it, until {@link #flush} lands it, and a frame that lands must
 * end where {@link QueuePositions} says. {@link ShadowQueueAgent} is the adapter.
 *
 * <p>Free of Aeron types. Single-threaded.
 */
final class QueueEmitter implements Sequencer.FrameSink {
    /** The queues, which the adapter implements against Aeron. */
    interface Queues {
        /**
         * Offers the frame at offset 0 of {@code buffer} onto group {@code groupId}'s queue.
         *
         * @return the raw Aeron offer result
         */
        long offer(int groupId, DirectBuffer buffer, int length);

        /** Whether no amount of retrying can clear an offer result. */
        boolean isUnrecoverable(long offerResult);
    }

    private record Kept(int groupId, byte[] frame) { }

    private final Queues queues;
    private final long[] positions;
    private final int termLength;
    private final int mtu;
    private final ArrayDeque<Kept> kept = new ArrayDeque<>();
    private final UnsafeBuffer keptBuffer = new UnsafeBuffer();
    private String failure;

    /**
     * @param queueStarts where each queue starts, slot {@code groupId - 1}
     * @param termLength  the queues'
     * @param mtu         the queues'
     */
    QueueEmitter(final Queues queues, final long[] queueStarts, final int termLength, final int mtu) {
        this.queues = queues;
        this.positions = queueStarts.clone();
        this.termLength = termLength;
        this.mtu = mtu;
    }

    /** Whether a frame is kept: no log entry may be applied until {@link #flush} has landed it. */
    boolean isHolding() {
        return !kept.isEmpty();
    }

    /** Why no frame can be put on a queue any more, or null. */
    String failure() {
        return failure;
    }

    /** Where group {@code groupId}'s queue ends once every frame published has landed. */
    long position(final int groupId) {
        return positions[groupId - 1];
    }

    /**
     * Offers the kept frames in order, up to the first a queue does not take.
     *
     * @return how many landed
     */
    int flush() {
        int landed = 0;
        while (failure == null && !kept.isEmpty()) {
            final Kept next = kept.peek();
            keptBuffer.wrap(next.frame());
            if (!offer(next.groupId(), keptBuffer, next.frame().length)) {
                break;
            }
            kept.poll();
            landed++;
        }
        return landed;
    }

    @Override
    public void activate(final int[] groupIds) {
    }

    @Override
    public void publish(final int groupId, final DirectBuffer buffer, final int length) {
        if (failure != null) {
            return;
        }
        if (!kept.isEmpty() || !offer(groupId, buffer, length)) {
            final byte[] frame = new byte[length];
            buffer.getBytes(0, frame);
            kept.add(new Kept(groupId, frame));
        }
    }

    /** @return whether the queue took the frame */
    private boolean offer(final int groupId, final DirectBuffer buffer, final int length) {
        final long result = queues.offer(groupId, buffer, length);
        if (result > 0) {
            final long start = QueuePositions.frameStart(positions[groupId - 1], length, termLength, mtu);
            positions[groupId - 1] = start + QueuePositions.framedLength(length, mtu);
            if (result != positions[groupId - 1]) {
                failure = "group " + groupId + "'s queue took a frame to " + result + ", where the log puts it at " +
                    positions[groupId - 1];
            }
            return true;
        }
        if (queues.isUnrecoverable(result)) {
            failure = "group " + groupId + "'s queue refused a frame for good (" + result + ")";
        }
        return false;
    }
}
