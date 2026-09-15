package org.limitless.seqeron.app;

/**
 * A gateway's recovery-stall fence: recovery dispatching nothing for a deadline, once this instance has been
 * caught up at least once. It covers the state a tap-silence watchdog cannot, because that watchdog is gated
 * on {@code isCaughtUp()}: a recovery that never converges would otherwise keep an active gateway serving
 * behind a view of the log frozen at a hole it cannot close.
 *
 * <p><b>Progress, not elapsed recovery.</b> A converging re-walk always advances the {@code globalSeqNo}
 * it has dispatched and a non-converging one never does, so timing elapsed recovery would fence the very
 * path a recovery takes. It never arms before the first catch-up: a cold start replays the whole log (no
 * snapshots) and has no useful time bound.
 *
 * <p>A fence, where {@link org.limitless.seqeron.replayer.client.RecoveryProgressPolicy} is the alarm on the
 * same predicate; set this deadline longer. The C++ twin is {@code app/RecoveryStallFence.hpp}; keep the two
 * in step.
 */
public final class RecoveryStallFence {
    private final long deadlineMs;

    private boolean everCaughtUp;

    /** When the current no-progress episode started being timed; 0 = no episode currently timed. */
    private long recoveryStartMs;

    /** The dispatched frontier at the last observation; only meaningful while an episode is timed. */
    private long lastGlobalSeqNo;

    /**
     * @param deadlineMs how long recovery may run without dispatching a single frame, once this instance has
     *                   been caught up before, before it is declared unconvergent. Deliberately generous
     *                   relative to a normal gap-recovery re-walk (seconds) so that is never mistaken for the
     *                   pathological case this exists to catch.
     */
    public RecoveryStallFence(final long deadlineMs) {
        this.deadlineMs = deadlineMs;
    }

    /**
     * Caught up: not, or no longer, recovering. Latches {@code everCaughtUp} forever and clears the recovery
     * clock, so re-convergence after a legitimate re-walk re-arms cleanly for the next one.
     */
    public void onCaughtUp() {
        everCaughtUp = true;
        recoveryStartMs = 0;
    }

    /**
     * Evaluates one not-caught-up observation. The first of an episode, and every one that finds the frontier
     * advanced, only re-anchors the clock — the deadline is always measured from the last sign of progress.
     * @param nowMs        monotonic clock reading
     * @param globalSeqNo  the highest globalSeqNo dispatched so far
     * @return true once recovery has dispatched nothing for at least the deadline, on an instance that has
     *         been caught up before; always false for a cold start
     */
    public boolean onNotCaughtUp(final long nowMs, final long globalSeqNo) {
        if (!everCaughtUp) {
            return false;
        }
        if (recoveryStartMs == 0 || globalSeqNo != lastGlobalSeqNo) {
            lastGlobalSeqNo = globalSeqNo;
            recoveryStartMs = nowMs;
            return false;
        }
        return (nowMs - recoveryStartMs) >= deadlineMs;
    }
}
