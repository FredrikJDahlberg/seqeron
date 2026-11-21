package org.limitless.seqeron.replayer.server;

import java.util.ArrayDeque;
import java.util.concurrent.TimeUnit;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.limitless.seqeron.sequencer.QueuePositions;
import org.limitless.seqeron.sequencer.Sequencer;
import org.limitless.seqeron.util.Logger;

/**
 * Puts a derivation's frames on the queues without waiting (doc/plans/queue-off-duty-cycle-plan.md, step 2): a frame a
 * queue does not take is kept, with every frame after it, until {@link #flush} lands it, and a frame that lands must
 * end where {@link QueuePositions} says. A kept frame that no flush lands for the stall timeout fails it. {@link
 * QueueAgent} is the adapter.
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

        /** The {@code seqeron.sequencer.queueBackPressureAlerts} counter. */
        void backPressureAlert();

        /** The {@code seqeron.sequencer.queueStalled} gauge. */
        void stalled(boolean stalled);
    }

    /** How often, while a frame is kept, back-pressure is alerted on. */
    static final long BACK_PRESSURE_ALERT_INTERVAL_NS = TimeUnit.MILLISECONDS.toNanos(10);

    /** How long a frame may stay kept before the stall gauge is raised. */
    static final long SUSTAINED_BACKPRESSURE_THRESHOLD_NS = TimeUnit.MILLISECONDS.toNanos(200);

    private record Kept(int groupId, byte[] frame) { }

    private final Queues queues;
    private final long[] positions;
    private final int termLength;
    private final int mtu;
    private final long stallTimeoutNs;
    private final int memberId;
    private final ArrayDeque<Kept> kept = new ArrayDeque<>();
    private final UnsafeBuffer keptBuffer = new UnsafeBuffer();
    private String failure;
    /** Whether a kept frame is timed from {@link #heldSinceNs}: the first flush to find it, or the last landing. */
    private boolean timing;
    private long heldSinceNs;
    private long nextAlertNs;
    private boolean stalled;

    /**
     * @param queueStarts    where each queue starts, slot {@code groupId - 1}
     * @param termLength     the queues'
     * @param mtu            the queues'
     * @param stallTimeoutNs how long a kept frame may go unlanded before the emitter fails
     * @param memberId       for the log lines
     */
    QueueEmitter(final Queues queues, final long[] queueStarts, final int termLength, final int mtu,
                 final long stallTimeoutNs, final int memberId) {
        this.queues = queues;
        this.positions = queueStarts.clone();
        this.termLength = termLength;
        this.mtu = mtu;
        this.stallTimeoutNs = stallTimeoutNs;
        this.memberId = memberId;
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
     * Offers the kept frames in order, up to the first a queue does not take, then times the one still kept: the
     * first call that finds it only starts the clock.
     *
     * @param nowNs a monotonic clock reading
     * @return how many landed
     */
    int flush(final long nowNs) {
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
        if (failure != null) {
            return landed;
        }
        if (landed > 0 && stalled) {
            stalled = false;
            queues.stalled(false);
            Logger.info(Logger.CoreComponent.ReplayerServer, memberId, "RECOVERED: queue back-pressure cleared");
        }
        if (kept.isEmpty()) {
            timing = false;
        } else if (landed > 0 || !timing) {
            timing = true;
            heldSinceNs = nowNs;
            nextAlertNs = nowNs + BACK_PRESSURE_ALERT_INTERVAL_NS;
        } else {
            onHeld(kept.peek().groupId(), nowNs);
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

    /** A frame has stayed kept since {@link #heldSinceNs}: alert each interval, raise the gauge, then fail. */
    private void onHeld(final int groupId, final long nowNs) {
        if (nowNs - nextAlertNs >= 0) {
            nextAlertNs = nowNs + BACK_PRESSURE_ALERT_INTERVAL_NS;
            Logger.error(Logger.CoreComponent.ReplayerServer, Logger.CoreEventCode.QueueBackpressure, memberId,
                         "ALERT: group %d's queue back-pressure", groupId);
            queues.backPressureAlert();
        }
        final long heldNs = nowNs - heldSinceNs;
        if (heldNs >= stallTimeoutNs) {
            failure = "group " + groupId + "'s queue made no progress for " +
                TimeUnit.NANOSECONDS.toMillis(stallTimeoutNs) +
                "ms of continuous back-pressure, held by a subscriber the media driver has not dropped";
        } else if (heldNs >= SUSTAINED_BACKPRESSURE_THRESHOLD_NS && !stalled) {
            stalled = true;
            queues.stalled(true);
            Logger.error(Logger.CoreComponent.ReplayerServer, Logger.CoreEventCode.QueueBackpressure, memberId,
                         "STALLED: group %d's queue back-pressure sustained beyond %dms", groupId,
                         TimeUnit.NANOSECONDS.toMillis(SUSTAINED_BACKPRESSURE_THRESHOLD_NS));
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
