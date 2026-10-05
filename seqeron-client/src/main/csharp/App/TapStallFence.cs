namespace Org.Limitless.Seqeron.App;

/// <summary>
/// A producer's tap-liveness fence: no <c>ClusterHeartbeat</c> for a deadline, on an instance that has caught up.
/// The heartbeat is the one frame that keeps arriving while every producer is silent, so its absence — and nothing
/// else — separates a quiet deployment from a tap this process has stopped seeing.
/// <para><b>Armed by catching up, and re-armed by each re-convergence.</b> A cold start replays history, from
/// <c>globalSeqNo</c> 1 or a snapshot's cut, and a re-walk dispatches history rather than live frames; neither has
/// a heartbeat cadence to measure, so timing either would fence the very path recovery takes.
/// <see cref="RecoveryStallFence"/> is the fence covering that side.</para>
/// <para>Single-threaded, like every block here. <c>TapStallFence.java</c> and <c>app/detail/TapStallFence.hpp</c>
/// are its twins; keep the three in step.</para>
/// </summary>
internal sealed class TapStallFence
{
    private readonly long _deadlineMs;

    private bool _armed;
    private long _lastProgressMs;

    /// <summary>A fence that is not armed until the first catch-up.</summary>
    /// <param name="deadlineMs">how long the tap may be silent before this instance can no longer be trusted to be
    /// seeing it; generous against the 1 Hz heartbeat</param>
    public TapStallFence(long deadlineMs)
    {
        _deadlineMs = deadlineMs;
    }

    /// <summary>Caught up: arms the fence and anchors the deadline, so a healed gap is never timed against
    /// it.</summary>
    /// <param name="nowMs">monotonic clock reading</param>
    public void OnCaughtUp(long nowMs)
    {
        _armed = true;
        _lastProgressMs = nowMs;
    }

    /// <summary>A <c>ClusterHeartbeat</c> off the tap.</summary>
    /// <param name="nowMs">monotonic clock reading</param>
    public void OnClusterHeartbeat(long nowMs)
    {
        _lastProgressMs = nowMs;
    }

    /// <summary>Whether the tap has been silent for the deadline.</summary>
    /// <param name="nowMs">monotonic clock reading</param>
    /// <returns>true once it has; always false before the first catch-up</returns>
    public bool IsStalled(long nowMs)
    {
        return _armed && (nowMs - _lastProgressMs) >= _deadlineMs;
    }
}
