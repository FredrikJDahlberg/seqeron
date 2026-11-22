using Adaptive.Aeron;
using Xunit;
using Action = Org.Limitless.Seqeron.Sequencer.Client.IngressStallPolicy.Action;

namespace Org.Limitless.Seqeron.Sequencer.Client;

/// <summary>
/// Unit tests for the verdict <c>ClusterStreamSender.Send</c> reaches on a failed offer. No Aeron runtime: the offer
/// result and the two session flags are exactly what the sender reads, and the clock is the caller's. Case for case
/// with <c>IngressStallPolicyTest.java</c>.
/// </summary>
public class IngressStallPolicyTest
{
    private const long StallMs = 10_000;
    private const long AlertMs = 1_000;

    // Arbitrary non-zero clock origin — 0 is the policy's "no block in progress" sentinel.
    private const long T0 = 5 * 3_600_000L;

    private readonly IngressStallPolicy _policy = new IngressStallPolicy(StallMs, AlertMs);

    private static long Seconds(long s)
    {
        return T0 + s * 1000;
    }

    // A caller retrying back-pressured offers once an alert interval, from fromMs to before toMs.
    private void FailEveryAlertInterval(long fromMs, long toMs)
    {
        for (long now = fromMs; now < toMs; now += AlertMs)
        {
            _policy.OnOfferFailed(now, Publication.BACK_PRESSURED, false, false);
        }
    }

    [Fact(DisplayName = "CLOSED is an election in progress, not a dead session: it is retried")]
    public void ClosedIsRetriedForTheLengthOfAnElection()
    {
        // The regression this class exists for. A leader that dies has AeronCluster close the ingress publication
        // while it awaits a NewLeader event, so every offer returns CLOSED meanwhile — on a session the cluster
        // still holds. Reading it as terminal fails every submit made during one.
        Assert.Equal(Action.Retry, _policy.OnOfferFailed(Seconds(0), Publication.CLOSED, false, false));
        for (long s = 1; s < 5; s++)
        {
            Action action = _policy.OnOfferFailed(Seconds(s), Publication.CLOSED, false, false);
            Assert.True(action == Action.Alert, $"second {s} should alert and be retried");
        }
        // …and the frame lands once the new leader's publication is installed.
        _policy.OnOffered();
        Assert.Equal(0, _policy.BlockedMs(Seconds(5)));
    }

    [Fact(DisplayName = "ordinary back-pressure retries silently until the alert interval elapses")]
    public void BackPressureRetriesQuietlyThenAlerts()
    {
        Assert.Equal(Action.Retry, _policy.OnOfferFailed(T0, Publication.BACK_PRESSURED, false, false));
        Assert.Equal(Action.Retry, _policy.OnOfferFailed(T0 + AlertMs / 2, Publication.BACK_PRESSURED, false, false));
        Assert.Equal(Action.Alert, _policy.OnOfferFailed(T0 + AlertMs, Publication.BACK_PRESSURED, false, false));
        Assert.Equal(Action.Retry, _policy.OnOfferFailed(T0 + AlertMs + 1, Publication.BACK_PRESSURED, false, false));
    }

    [Fact(DisplayName = "NOT_CONNECTED is retried: the publication is being (re)established")]
    public void NotConnectedIsRetried()
    {
        Assert.Equal(Action.Retry, _policy.OnOfferFailed(T0, Publication.NOT_CONNECTED, false, false));
    }

    [Fact(DisplayName = "MAX_POSITION_EXCEEDED is unrecoverable, and is so from the first observation")]
    public void MaxPositionExceededIsFatalImmediately()
    {
        Assert.Equal(Action.Fatal, _policy.OnOfferFailed(T0, Publication.MAX_POSITION_EXCEEDED, false, false));
    }

    [Fact(DisplayName = "a session the cluster closed is gone — there is nothing left to retry on")]
    public void SessionLostIsGone()
    {
        Assert.Equal(Action.Retry, _policy.OnOfferFailed(T0, Publication.CLOSED, false, false));
        Assert.Equal(Action.SessionGone, _policy.OnOfferFailed(Seconds(1), Publication.CLOSED, true, false));
    }

    [Fact(DisplayName = "a client that closed itself ends it too")]
    public void ClosedClientIsGone()
    {
        Assert.Equal(Action.SessionGone, _policy.OnOfferFailed(T0, Publication.CLOSED, false, true));
    }

    [Fact(DisplayName = "nothing accepted for the fatal timeout: the session is called what it has become")]
    public void SustainedBlockStalls()
    {
        FailEveryAlertInterval(T0, T0 + StallMs);
        Assert.Equal(Action.Stalled, _policy.OnOfferFailed(T0 + StallMs, Publication.BACK_PRESSURED, false, false));
    }

    [Fact(DisplayName = "a failure after a quiet gap starts a new block: one refused offer then silence is no stall")]
    public void AQuietGapStartsANewBlock()
    {
        Assert.Equal(Action.Retry, _policy.OnOfferFailed(T0, Publication.BACK_PRESSURED, false, false));
        Assert.Equal(Action.Retry, _policy.OnOfferFailed(T0 + StallMs, Publication.BACK_PRESSURED, false, false));
        Assert.Equal(0, _policy.BlockedMs(T0 + StallMs));
        Assert.Equal(Action.Retry,
                     _policy.OnOfferFailed(T0 + StallMs + AlertMs + 1, Publication.BACK_PRESSURED, false, false));
        Assert.Equal(0, _policy.BlockedMs(T0 + StallMs + AlertMs + 1));
    }

    [Fact(DisplayName =
              "the fatal clock is per block: a landed offer resets it, so two short blocks are not one long one")]
    public void ALandedOfferResetsTheClock()
    {
        FailEveryAlertInterval(T0, T0 + StallMs - AlertMs);
        Assert.Equal(Action.Alert,
                     _policy.OnOfferFailed(T0 + StallMs - AlertMs, Publication.BACK_PRESSURED, false, false));
        _policy.OnOffered();
        // A new block starting just before the old clock would have fired must anchor its own.
        Assert.Equal(Action.Retry, _policy.OnOfferFailed(T0 + StallMs, Publication.BACK_PRESSURED, false, false));
        Assert.Equal(Action.Retry, _policy.OnOfferFailed(T0 + StallMs + 1, Publication.BACK_PRESSURED, false, false));
    }

    [Fact(DisplayName = "the first failure only anchors the clock: how long it had been failing is unknown")]
    public void FirstFailureAnchorsRatherThanJudges()
    {
        Assert.Equal(Action.Retry, _policy.OnOfferFailed(Seconds(100), Publication.BACK_PRESSURED, false, false));
        Assert.Equal(0, _policy.BlockedMs(Seconds(100)));
        Assert.Equal(1000, _policy.BlockedMs(Seconds(101)));
    }
}
