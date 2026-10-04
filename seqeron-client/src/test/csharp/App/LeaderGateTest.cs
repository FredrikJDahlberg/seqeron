using Xunit;
using Transition = Org.Limitless.Seqeron.App.LeaderGate.Transition;

namespace Org.Limitless.Seqeron.App;

/// <summary>
/// Unit tests for the leader gate's edges. <c>(IsCaughtUp, CurrentLeaderMemberId)</c> is exactly what a replica
/// reads off its <c>ReplayerStreamReceiver</c> each duty cycle. Case for case with <c>LeaderGateTest.java</c> and
/// <c>LeaderGateTest.cpp</c>.
/// </summary>
public class LeaderGateTest
{
    private const int Self = 1;
    private const int Other = 2;
    private const int NoLeader = -1;

    private readonly LeaderGate _gate = new LeaderGate(Self);

    [Fact(DisplayName = "starts closed")]
    public void StartsClosed()
    {
        Assert.False(_gate.IsOpen);
        Assert.Equal(Transition.None, _gate.Update(false, NoLeader));
    }

    [Fact(DisplayName = "opens once when caught up and leader")]
    public void OpensOnce()
    {
        Assert.Equal(Transition.Opened, _gate.Update(true, Self));
        Assert.True(_gate.IsOpen);
        Assert.Equal(Transition.None, _gate.Update(true, Self)); // an edge, not a level
    }

    [Fact(DisplayName = "stays closed unless both hold")]
    public void StaysClosedUnlessBothHold()
    {
        Assert.Equal(Transition.None, _gate.Update(false, Self)); // leader, but still recovering
        Assert.Equal(Transition.None, _gate.Update(true, Other));
        Assert.Equal(Transition.None, _gate.Update(true, NoLeader));
        Assert.False(_gate.IsOpen);
    }

    [Fact(DisplayName = "closes when leadership moves away")]
    public void ClosesWhenLeadershipMovesAway()
    {
        _gate.Update(true, Self);

        Assert.Equal(Transition.Closed, _gate.Update(true, Other));
        Assert.False(_gate.IsOpen);
        Assert.Equal(Transition.None, _gate.Update(true, Other));
    }

    [Fact(DisplayName = "closes when a leader falls behind, and reopens when it catches up")]
    public void ClosesWhenFallingBehind()
    {
        _gate.Update(true, Self);

        Assert.Equal(Transition.Closed, _gate.Update(false, Self));
        Assert.Equal(Transition.Opened, _gate.Update(true, Self));
    }

    [Fact(DisplayName = "a leadership flip away and back between two updates closes the gate for one cycle")]
    public void FlipBetweenUpdatesClosesForOneCycle()
    {
        _gate.Update(true, Self);

        _gate.OnLeadershipChanged(); // Self -> Other
        _gate.OnLeadershipChanged(); // Other -> Self, both applied within one duty cycle

        Assert.Equal(Transition.Closed, _gate.Update(true, Self)); // a reply sent during that election may be lost
        Assert.Equal(Transition.Opened, _gate.Update(true, Self));
        Assert.Equal(Transition.None, _gate.Update(true, Self));
    }

    [Fact(DisplayName = "a leadership change while closed costs nothing")]
    public void LeadershipChangeWhileClosed()
    {
        _gate.OnLeadershipChanged();

        Assert.Equal(Transition.Opened, _gate.Update(true, Self)); // nothing was dispatched, so no cycle is lost
    }

    [Fact(DisplayName = "off the cluster, opens when caught up whoever leads, and only then")]
    public void OffClusterOpensWhoeverLeads()
    {
        var offCluster = new LeaderGate(Self, true);

        Assert.Equal(Transition.None, offCluster.Update(false, Other)); // still recovering
        Assert.Equal(Transition.Opened, offCluster.Update(true, Other));
        Assert.Equal(Transition.None, offCluster.Update(true, NoLeader));
        Assert.Equal(Transition.Closed, offCluster.Update(false, Other));
    }

    [Fact(DisplayName = "off the cluster, a leadership change still closes the gate for one cycle")]
    public void OffClusterLeadershipChangeClosesForOneCycle()
    {
        var offCluster = new LeaderGate(Self, true);
        offCluster.Update(true, Other);

        offCluster.OnLeadershipChanged(); // a new term

        // a reply sent during that election may be lost
        Assert.Equal(Transition.Closed, offCluster.Update(true, Other));
        Assert.Equal(Transition.Opened, offCluster.Update(true, Other));
    }
}
