package org.limitless.seqeron.util;

/** The monotonic clock every duty-cycle deadline in this tree measures against. */
public final class Clocks {
    private static final long NANOS_PER_MILLI = 1_000_000L;

    private Clocks() {
    }

    /**
     * Elapsed milliseconds from an arbitrary origin, for measuring durations. {@code System.nanoTime} is
     * the JDK's only monotonic clock — {@code currentTimeMillis} steps under NTP — so this divides it
     * rather than reading a wall clock, which keeps nanoTime's resolution and loses only its unit.
     *
     * @return a monotonic reading in milliseconds; meaningful only as a difference against another
     */
    public static long monotonicMs() {
        return System.nanoTime() / NANOS_PER_MILLI;
    }
}
