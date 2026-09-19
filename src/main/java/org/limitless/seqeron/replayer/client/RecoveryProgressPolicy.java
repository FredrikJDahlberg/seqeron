package org.limitless.seqeron.replayer.client;

/**
 * The convergence alarm behind {@link ReplayerStreamReceiver}: recovery that never converges is safely held
 * (nothing dispatched, every gate shut) but silent, so this reports it. Measured by progress — the
 * dispatched globalSeqNo — not elapsed time, since a cold start has no time bound. An alarm, not a fence.
 * The C++ twin is {@code replayer/RecoveryProgressPolicy.hpp}; keep the two in step.
 */
public final class RecoveryProgressPolicy {
    private final long stallMs;

    /** When the current no-progress episode started being timed; 0 = no episode currently timed. */
    private long sinceMs;
    private boolean reported;

    /**
     * @param stallMs how long recovery may dispatch nothing before it is called unconvergent; must clear the
     *                longest legitimate wait, for a Replayer slot behind other apps' walks
     */
    public RecoveryProgressPolicy(final long stallMs) {
        this.stallMs = stallMs;
    }

    /**
     * A frame was dispatched in order: clears the clock and re-arms the report.
     * @return true only when this ends a reported episode, so the caller's gauge clears once
     */
    public boolean onProgress() {
        final boolean wasReported = reported;
        sinceMs = 0;
        reported = false;
        return wasReported;
    }

    /**
     * Evaluates one observation made while not caught up; the first of an episode only anchors the clock.
     * @param nowMs wall-clock reading
     * @return true once per episode, on the observation that crosses {@code stallMs}
     */
    public boolean onNoProgress(final long nowMs) {
        if (sinceMs == 0) {
            sinceMs = nowMs;
            return false;
        }
        if (reported || (nowMs - sinceMs) < stallMs) {
            return false;
        }
        reported = true;
        return true;
    }
}
