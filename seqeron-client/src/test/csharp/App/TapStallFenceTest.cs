using Xunit;

namespace Org.Limitless.Seqeron.App;

/// <summary>
/// Unit tests for the tap-silence verdict a producer applies once caught up. No Aeron runtime: the catch-up
/// transition and the <c>ClusterHeartbeat</c> arrivals are exactly the two signals <c>App.Session</c> feeds in off
/// <c>ReplayerStreamReceiver</c>. Case for case with <c>TapStallFenceTest.java</c>.
/// </summary>
public class TapStallFenceTest
{
    private const long DeadlineMs = 20 * 1000;

    // Arbitrary clock origin: the fence is only ever read as a difference.
    private const long T0 = 7 * 3_600_000L;

    private readonly TapStallFence _fence = new TapStallFence(DeadlineMs);

    private static long Seconds(long s)
    {
        return T0 + s * 1000;
    }

    [Fact(DisplayName = "an instance still replaying is never fenced, however long the replay takes")]
    public void ReplayNeverTrips()
    {
        for (long s = 0; s < 3600; s += 30)
        {
            Assert.False(_fence.IsStalled(Seconds(s)), $"second {s}: the fence arms on the first catch-up");
        }
    }

    [Fact(DisplayName = "a heartbeat each second holds the fence open indefinitely")]
    public void HeartbeatsHoldItOpen()
    {
        _fence.OnCaughtUp(Seconds(0));
        for (long s = 1; s < 600; s++)
        {
            _fence.OnClusterHeartbeat(Seconds(s));
            Assert.False(_fence.IsStalled(Seconds(s)), $"second {s}");
        }
    }

    [Fact(DisplayName = "silence for the deadline after the last heartbeat trips it")]
    public void SilenceTrips()
    {
        _fence.OnCaughtUp(Seconds(0));
        _fence.OnClusterHeartbeat(Seconds(5));
        Assert.False(_fence.IsStalled(Seconds(24)), "one second short of the deadline");
        Assert.True(_fence.IsStalled(Seconds(25)), "the deadline measured from the last heartbeat");
    }

    [Fact(DisplayName = "catching up re-anchors the deadline, so a healed gap is not fenced for the time it took")]
    public void CatchUpReAnchors()
    {
        _fence.OnCaughtUp(Seconds(0));
        Assert.True(_fence.IsStalled(Seconds(30)), "silent through the deadline");
        _fence.OnCaughtUp(Seconds(30));
        Assert.False(_fence.IsStalled(Seconds(30)), "re-converged: the deadline starts again here");
        Assert.True(_fence.IsStalled(Seconds(50)), "and runs from there");
    }
}
