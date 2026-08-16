package org.limitless.phixeron.replayer.client;

/**
 * Pure decision logic behind {@link ReplayerStreamReceiver}'s convergence alarm. Recovery that never
 * converges is correctly CONTAINED — nothing is dispatched from a baseline this node cannot establish, so
 * every consumer gate stays shut — but it is also silent: a hole this node's recording chain cannot cover,
 * a Replayer that never answers, and a {@code ReplayUnavailable} refusal all hold {@code isCaughtUp()}
 * false indefinitely while the process looks busy.
 *
 * <p>PROGRESS, not elapsed time. A legitimate cold start has no time bound — it replays the whole log, and
 * the log grows through the trading day — so an elapsed deadline either fires on a healthy slow start or is
 * too loose to catch anything. A converging recovery always advances globalSeqNo; a non-converging one
 * never does. The re-walk loop is the clearest case: it re-delivers the history it already holds and
 * de-dupes all of it, so replays keep starting and finishing with the last dispatched globalSeqNo frozen.
 *
 * <p>ALARM, not fence — it reports and returns. Holding is the right response to a baseline that cannot be
 * established; the only thing missing was someone saying so. The C++ twin is
 * {@code replayer/RecoveryProgressPolicy.hpp}; keep the two in step.
 */
public final class RecoveryProgressPolicy {
    private final long stallMs;

    /** When the current no-progress episode started being timed; 0 = no episode currently timed. */
    private long sinceMs;
    private boolean reported;

    /**
     * @param stallMs how long recovery may run without dispatching a single frame before it is called
     *                unconvergent. It has to clear the longest legitimate pause with nothing dispatched —
     *                waiting on a Replayer slot behind other co-located apps' walks, not any step of this
     *                client's own.
     */
    public RecoveryProgressPolicy(final long stallMs) {
        this.stallMs = stallMs;
    }

    /**
     * Recovery is converging: a frame was dispatched in order. Clears the clock and re-arms the report, so
     * a later episode is reported again rather than swallowed.
     * @return true only when this ends an episode that HAD been reported — the falling edge, so the
     *         caller's gauge clears exactly once rather than on every frame of a healthy stream
     */
    public boolean onProgress() {
        final boolean wasReported = reported;
        sinceMs = 0;
        reported = false;
        return wasReported;
    }

    /**
     * Evaluates one observation made while not caught up. The first of an episode only anchors the clock —
     * how long recovery had already been running before this call is unknown — so the deadline is measured
     * from here.
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
