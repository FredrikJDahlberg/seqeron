package org.limitless.seqeron.tools;

import io.aeron.archive.checksum.Checksum;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * A recording checksum that holds the archive's recorder while it is shut, so a recording trails its stream as a slow
 * disk would make it. An archive launched with {@code aeron.archive.record.checksum} naming this class takes it.
 * Public, with a public constructor: the archive makes one by reflection.
 */
public final class GatedChecksum implements Checksum {
    /** How long one frame may be held: past it the recorder runs on, so a failed test cannot hang its archive. */
    private static final long MAX_HOLD_NS = TimeUnit.SECONDS.toNanos(30);

    private static volatile boolean shut;

    /** Holds every recorder using this checksum at its next frame, or lets them go. */
    static void shut(final boolean isShut) {
        shut = isShut;
    }

    @Override
    public int compute(final long address, final int offset, final int length) {
        final long deadlineNs = System.nanoTime() + MAX_HOLD_NS;
        while (shut && System.nanoTime() < deadlineNs) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
        }
        return 0;
    }
}
