package org.limitless.seqeron.tools;

import io.aeron.cluster.NanosecondClusterClock;
import io.aeron.cluster.service.ClusterClock;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/** A member's consensus clock, which a test steps as a time daemon steps its host's. */
final class SteppableClusterClock implements ClusterClock {
    private final NanosecondClusterClock clock = new NanosecondClusterClock();
    private final AtomicLong offsetNs = new AtomicLong();

    @Override
    public TimeUnit timeUnit() {
        return TimeUnit.NANOSECONDS;
    }

    @Override
    public long time() {
        return clock.time() + offsetNs.get();
    }

    /** Moves the clock by {@code millis}, backward when negative, from the next reading on. */
    void step(final long millis) {
        offsetNs.addAndGet(TimeUnit.MILLISECONDS.toNanos(millis));
    }
}
