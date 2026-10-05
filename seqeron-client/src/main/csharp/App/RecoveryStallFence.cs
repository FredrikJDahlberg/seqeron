namespace Org.Limitless.Seqeron.App;

/// <summary>
/// A gateway's recovery-stall fence: recovery dispatching nothing for a deadline, once this instance has been
/// caught up at least once. It covers the state a tap-silence watchdog cannot, because that watchdog is gated on
/// <c>IsCaughtUp</c>: a recovery that never converges would otherwise keep an active gateway serving behind a view
/// of the log frozen at a hole it cannot close.
/// <para><b>Progress, not elapsed recovery.</b> A converging re-walk always advances the <c>globalSeqNo</c> it has
/// dispatched and a non-converging one never does, so timing elapsed recovery would fence the very path a recovery
/// takes. It never arms before the first catch-up: a cold start replays the whole log (no snapshots) and has no
/// useful time bound.</para>
/// <para>A fence, where <c>ReplayerRecovery.CheckRecoveryProgress</c> is the alarm on the same predicate; set this
/// deadline longer. <c>RecoveryStallFence.java</c> and <c>app/detail/RecoveryStallFence.hpp</c> are its twins;
/// keep the three in step.</para>
/// </summary>
internal sealed class RecoveryStallFence
{
    private readonly long _deadlineMs;

    private bool _everCaughtUp;

    // When the current no-progress episode started being timed; 0 = no episode currently timed.
    private long _recoveryStartMs;

    // The dispatched frontier at the last observation; only meaningful while an episode is timed.
    private long _lastGlobalSeqNo;

    /// <summary>A fence with no episode timed yet.</summary>
    /// <param name="deadlineMs">how long recovery may dispatch nothing, once caught up before, before it is
    /// declared unconvergent; generous against a normal re-walk's seconds</param>
    public RecoveryStallFence(long deadlineMs)
    {
        _deadlineMs = deadlineMs;
    }

    /// <summary>Caught up: latches <c>everCaughtUp</c> and clears the clock, so the next re-walk is timed
    /// afresh.</summary>
    public void OnCaughtUp()
    {
        _everCaughtUp = true;
        _recoveryStartMs = 0;
    }

    /// <summary>Recovery starts over as a cold start, a passive gateway instance's activation: disarmed until caught
    /// up.</summary>
    public void OnRestart()
    {
        _everCaughtUp = false;
        _recoveryStartMs = 0;
    }

    /// <summary>Evaluates one not-caught-up observation. The first of an episode, and every one that finds the
    /// frontier advanced, only re-anchors the clock — the deadline is always measured from the last sign of
    /// progress.</summary>
    /// <param name="nowMs">monotonic clock reading</param>
    /// <param name="globalSeqNo">the highest globalSeqNo dispatched so far</param>
    /// <returns>true once recovery has dispatched nothing for at least the deadline, on an instance that has been
    /// caught up before; always false for a cold start</returns>
    public bool OnNotCaughtUp(long nowMs, long globalSeqNo)
    {
        if (!_everCaughtUp)
        {
            return false;
        }
        if (_recoveryStartMs == 0 || globalSeqNo != _lastGlobalSeqNo)
        {
            _lastGlobalSeqNo = globalSeqNo;
            _recoveryStartMs = nowMs;
            return false;
        }
        return (nowMs - _recoveryStartMs) >= _deadlineMs;
    }
}
