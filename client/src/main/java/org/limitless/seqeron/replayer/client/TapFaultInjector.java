package org.limitless.seqeron.replayer.client;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test-only: drops live tap frames before a {@link ReplayerStreamReceiver} sees them, so the next frame reads
 * as a gap (gap-recovery-test.sh). The caller owns it and hands it to the receiver's constructor.
 */
public final class TapFaultInjector {
    private final AtomicInteger pending = new AtomicInteger();

    /** Drops the next {@code n} live tap frames. Callable from any thread. */
    public void arm(final int n) {
        pending.addAndGet(n);
    }

    /** Poll thread only, the one decrementer. */
    boolean dropNext() {
        if (pending.get() > 0) {
            pending.decrementAndGet();
            return true;
        }
        return false;
    }
}
