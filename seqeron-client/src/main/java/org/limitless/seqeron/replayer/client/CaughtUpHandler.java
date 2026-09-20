package org.limitless.seqeron.replayer.client;

/** Fires on every transition to caught-up, including re-convergence after a gap. */
@FunctionalInterface
public interface CaughtUpHandler {
    /** Called when the receiver reaches the live tap, and again after each gap it heals. */
    void onCaughtUp();
}
