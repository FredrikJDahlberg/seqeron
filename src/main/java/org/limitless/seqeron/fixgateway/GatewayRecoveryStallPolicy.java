package org.limitless.seqeron.fixgateway;

/**
 * Pure decision logic behind an Artio gateway's recovery fence — the symmetric case to its tap-silence
 * fence. Shared by both legs: {@code ExchangeGateway} toward a venue and {@code OrderGateway} toward
 * clients. Silence is bounded by "no {@code ClusterHeartbeat} for a while"; this bounds the opposite state,
 * a tap that never goes contiguous again. A {@code Replayer} that never answers, a {@code ReplayUnavailable}
 * refusal, and a gap this node's recording chain cannot cover all hold {@code isCaughtUp()} false
 * indefinitely without ever being fatal on their own.
 *
 * <p>For a gateway that has been serving, that state is silently wrong. It keeps the counterparty socket, and Artio
 * keeps deciding what to send on it, but nothing decided reaches the wire — an emission happens only when
 * the frame comes back off the tap, and the tap is stuck. Meanwhile the keep-alive still runs every duty
 * cycle, so the cluster session stays open and the sequencer never promotes the standby. The counterparty is
 * left with a session going quiet behind a gateway nothing will replace.
 *
 * <p>A cold-starting gateway must NOT be fenced by this: it legitimately holds no socket however long the
 * initial walk takes, and with no snapshots that walk is the whole log. The deadline arms only once
 * {@link #onCaughtUp} has fired at least once.
 *
 * <p>PROGRESS, not elapsed recovery. A converging recovery always advances the globalSeqNo it has
 * dispatched; a non-converging one never does, because the re-walk loop re-delivers history it already holds
 * and de-dupes all of it. Measuring elapsed recovery instead would fence a re-walk that is working, which is
 * the very path a recovery takes. The same predicate {@code RecoveryProgressPolicy} alarms on, at a longer
 * deadline, because this one fences.
 *
 * <p>The C++ twin is {@code fix/GatewayRecoveryStallPolicy.hpp}, driving the same fence on the client-facing
 * edge; keep the two in step, and both {@code GatewayRecoveryStallPolicyTest}s with them.
 */
public final class GatewayRecoveryStallPolicy {
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
    public GatewayRecoveryStallPolicy(final long deadlineMs) {
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
