using Xunit;

namespace Org.Limitless.Seqeron.App;

/// <summary>
/// Unit tests for the recovery-deadline verdict a gateway applies while not caught up. No Aeron runtime:
/// <c>IsCaughtUp</c>/<c>OnCaughtUp</c> and the dispatched globalSeqNo are exactly the three signals a gateway feeds
/// in off <c>ReplayerStreamReceiver</c>. Case for case with <c>RecoveryStallFenceTest.java</c> and
/// <c>RecoveryStallFenceTest.cpp</c>.
/// </summary>
public class RecoveryStallFenceTest
{
    private const long DeadlineMs = 60_000;

    // Arbitrary non-zero clock origin — 0 is the policy's "no recovery episode timed" sentinel.
    private const long T0 = 3 * 3_600_000L;

    // A frontier that never advances: the non-converging case every deadline test below drives.
    private const long Stuck = 4242;

    private readonly RecoveryStallFence _policy = new RecoveryStallFence(DeadlineMs);

    private static long Seconds(long s)
    {
        return T0 + s * 1000;
    }

    [Fact(DisplayName = "a cold start is never fenced, however long it takes")]
    public void ColdStartNeverTrips()
    {
        for (long s = 0; s < 3600; s += 30)
        {
            Assert.False(_policy.OnNotCaughtUp(Seconds(s), Stuck),
                         $"second {s}: an instance that has never caught up must not be fenced");
        }
    }

    [Fact(DisplayName = "recovery that converges within the deadline never trips, and re-arms cleanly")]
    public void ConvergentRecoveryNeverTrips()
    {
        _policy.OnCaughtUp();

        Assert.False(_policy.OnNotCaughtUp(Seconds(0), Stuck)); // anchors the recovery clock
        Assert.False(_policy.OnNotCaughtUp(Seconds(10), Stuck));
        Assert.False(_policy.OnNotCaughtUp(Seconds(30), Stuck));
        _policy.OnCaughtUp(); // a normal re-walk converges well inside the deadline

        Assert.False(_policy.OnNotCaughtUp(Seconds(31), Stuck), "a fresh episode, not the old clock carried forward");
        Assert.False(_policy.OnNotCaughtUp(Seconds(50), Stuck));
    }

    [Fact(DisplayName = "unconvergent recovery on an instance that has served trips at the deadline")]
    public void UnconvergentRecoveryTrips()
    {
        _policy.OnCaughtUp(); // this instance has been live before — the deadline is armed

        Assert.False(_policy.OnNotCaughtUp(Seconds(0), Stuck)); // anchors the recovery clock
        Assert.False(_policy.OnNotCaughtUp(Seconds(30), Stuck));
        Assert.False(_policy.OnNotCaughtUp(Seconds(59), Stuck));
        Assert.True(_policy.OnNotCaughtUp(Seconds(60), Stuck), "recovery dispatched nothing for the whole deadline");
        Assert.True(_policy.OnNotCaughtUp(Seconds(61), Stuck), "stays tripped on every later observation");
    }

    [Fact(DisplayName = "re-converging after a trip resets the clock for the next episode")]
    public void ReconvergingAfterATripResetsTheClock()
    {
        _policy.OnCaughtUp();
        _policy.OnNotCaughtUp(Seconds(0), Stuck);
        Assert.True(_policy.OnNotCaughtUp(Seconds(60), Stuck));

        _policy.OnCaughtUp(); // recovers before the process is actually fenced (a slow but real re-walk)

        Assert.False(_policy.OnNotCaughtUp(Seconds(61), Stuck), "anchors a fresh episode rather than inheriting");
        Assert.False(_policy.OnNotCaughtUp(Seconds(90), Stuck));
        Assert.True(_policy.OnNotCaughtUp(Seconds(121), Stuck));
    }

    [Fact(DisplayName = "recovery that keeps dispatching is never fenced, however long it runs")]
    public void DispatchingRecoveryIsNeverFenced()
    {
        // The defect this measure exists for: a re-walk of the whole chain is recovery WORKING, and an elapsed-time
        // deadline fenced it regardless of the history it was delivering.
        _policy.OnCaughtUp();

        for (long s = 0; s < 3600; ++s)
        {
            Assert.False(_policy.OnNotCaughtUp(Seconds(s), 1000 + s),
                         $"second {s}: a recovery advancing its globalSeqNo is converging");
        }
    }

    [Fact(DisplayName = "progress part way through an episode restarts the deadline")]
    public void ProgressRestartsTheDeadline()
    {
        _policy.OnCaughtUp();

        Assert.False(_policy.OnNotCaughtUp(Seconds(0), 1000));
        Assert.False(_policy.OnNotCaughtUp(Seconds(59), 1000));
        Assert.False(_policy.OnNotCaughtUp(Seconds(59), 1001), "one frame dispatched — the deadline restarts here");
        Assert.False(_policy.OnNotCaughtUp(Seconds(118), 1001));
        Assert.True(_policy.OnNotCaughtUp(Seconds(119), 1001), "60s with the frontier frozen at the new value");
    }

    [Fact(DisplayName = "a restart disarms the fence until the instance catches up again")]
    public void RestartDisarmsUntilCaughtUp()
    {
        _policy.OnCaughtUp();
        _policy.OnNotCaughtUp(Seconds(0), Stuck);

        _policy.OnRestart(); // a passive instance's activation: its restore pass dispatches nothing

        for (long s = 1; s < 3600; s += 30)
        {
            Assert.False(_policy.OnNotCaughtUp(Seconds(s), 0), $"second {s}: a restart is a cold start");
        }
        _policy.OnCaughtUp();
        Assert.False(_policy.OnNotCaughtUp(Seconds(3600), Stuck));
        Assert.True(_policy.OnNotCaughtUp(Seconds(3660), Stuck), "armed again once caught up");
    }
}
