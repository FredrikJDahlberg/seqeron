using System.Collections.Generic;
using System.Linq;
using Org.Limitless.Seqeron.Protocol;
using Xunit;
using GatewayRow = Org.Limitless.Seqeron.Protocol.SnapshotHeader.GatewayRow;
using GatewayState = Org.Limitless.Seqeron.Protocol.SnapshotHeader.GatewayState;
using State = Org.Limitless.Seqeron.App.GatewayLifecycle.State;

namespace Org.Limitless.Seqeron.App;

/// <summary>
/// Drives <see cref="GatewayLifecycle"/> directly; the races it covers are too narrow for an end-to-end script.
/// Topology: <c>GW-A</c> (id 5) and <c>GW-B</c> (id 6) under <c>gatewaySourceId</c> 6, beside another pair (ids 1
/// and 2) under 0.
/// <para>Case for case with <c>GatewayLifecycleTest.java</c> and <c>GatewayLifecycleTest.cpp</c>.</para>
/// </summary>
public class GatewayLifecycleTest
{
    private const string Me = "GW-A";
    private const int MyId = 5;
    private const int SiblingId = 6;
    private const int MySourceId = 6;
    private const int OtherPairId = 1;
    private const int OtherPairSourceId = 0;

    private sealed class RecordingActions : GatewayLifecycle.IActions
    {
        public readonly List<string> Calls = new List<string>();
        public bool GatewayStartedLands = true;
        public bool GateOpens = true;

        public bool PublishGatewayStarted(int gatewayId)
        {
            Calls.Add($"GatewayStarted({gatewayId})");
            return GatewayStartedLands;
        }

        public bool OpenGate()
        {
            Calls.Add("open");
            return GateOpens;
        }

        public void CloseGate()
        {
            Calls.Add("close");
        }

        public int Count(string call)
        {
            return Calls.Count(each => each == call);
        }
    }

    private readonly RecordingActions _actions = new RecordingActions();
    private readonly GatewayLifecycle _lifecycle;

    public GatewayLifecycleTest()
    {
        _lifecycle = new GatewayLifecycle(Me, _actions);
    }

    private void LoadTopology()
    {
        _lifecycle.OnGatewayRegistered(OtherPairId, OtherPairSourceId, "OTHER-A", 0);
        _lifecycle.OnGatewayRegistered(2, OtherPairSourceId, "OTHER-B", 1);
        _lifecycle.OnGatewayRegistered(MyId, MySourceId, Me, 0);
        _lifecycle.OnGatewayRegistered(SiblingId, MySourceId, "GW-B", 1);
    }

    private void BecomeServing()
    {
        LoadTopology();
        _lifecycle.OnGatewayActive(MyId);
        _lifecycle.OnCaughtUp();
        _lifecycle.Advance();
    }

    [Fact]
    public void ResolvesItsIdentityFromTheRowNamingIt()
    {
        LoadTopology();
        Assert.Equal(MyId, _lifecycle.GatewayId);
        Assert.Equal(MySourceId, _lifecycle.GatewaySourceId);
        Assert.Empty(_actions.Calls);
    }

    [Fact]
    public void IgnoresAnActivationBeforeAnyRowNamesThisInstance()
    {
        _lifecycle.OnGatewayActive(MyId);
        LoadTopology();
        _lifecycle.OnCaughtUp();
        Assert.False(_lifecycle.IsActivated);
        Assert.Equal(0, _lifecycle.Advance());
    }

    [Fact]
    public void AnActivationSeenWhileReplayingOpensOnlyOnceCaughtUp()
    {
        LoadTopology();
        _lifecycle.OnGatewayActive(MyId);
        Assert.Equal(State.Replaying, _lifecycle.CurrentState);
        Assert.Equal(0, _lifecycle.Advance());
        Assert.Equal(0, _actions.Count("open"));

        Assert.True(_lifecycle.OnCaughtUp());
        Assert.False(_lifecycle.OnCaughtUp());
        Assert.Equal(1, _lifecycle.Advance());
        Assert.Equal(State.Serving, _lifecycle.CurrentState);
    }

    [Fact]
    public void StaysShutWhenCaughtUpButNotActivated()
    {
        LoadTopology();
        _lifecycle.OnCaughtUp();
        Assert.Equal(State.Passive, _lifecycle.CurrentState);
        Assert.Equal(0, _lifecycle.Advance());
        Assert.False(_lifecycle.IsServing);
        Assert.Equal(0, _actions.Count("open"));
    }

    [Fact]
    public void PublishesGatewayStartedBeforeOpeningTheGate()
    {
        BecomeServing();
        Assert.True(_lifecycle.IsServing);
        Assert.Equal(new List<string> { $"GatewayStarted({MyId})", "open" }, _actions.Calls);
    }

    [Fact]
    public void DoesNotOpenTheGateWhileGatewayStartedIsBackPressured()
    {
        _actions.GatewayStartedLands = false;
        BecomeServing();
        Assert.Equal(State.Passive, _lifecycle.CurrentState);
        Assert.Equal(0, _actions.Count("open"));

        _actions.GatewayStartedLands = true;
        Assert.Equal(1, _lifecycle.Advance());
        Assert.Equal(State.Serving, _lifecycle.CurrentState);
    }

    [Fact]
    public void RetriesAGateThatDidNotOpenWithoutRegisteringAgain()
    {
        _actions.GateOpens = false;
        BecomeServing();
        Assert.Equal(State.Passive, _lifecycle.CurrentState);

        _actions.GateOpens = true;
        Assert.Equal(1, _lifecycle.Advance());
        Assert.Equal(State.Serving, _lifecycle.CurrentState);
        Assert.Equal(1, _actions.Count($"GatewayStarted({MyId})")); // once per activation, not per attempt
        Assert.Equal(2, _actions.Count("open"));
    }

    [Fact]
    public void AGateThatClosesByItselfReopensWithoutRegisteringAgain()
    {
        BecomeServing();
        _lifecycle.OnGateClosed();
        Assert.Equal(State.Passive, _lifecycle.CurrentState);
        Assert.Equal(0, _actions.Count("close")); // the gate is already closed

        Assert.Equal(1, _lifecycle.Advance());
        Assert.Equal(1, _actions.Count($"GatewayStarted({MyId})"));
        Assert.Equal(2, _actions.Count("open"));
    }

    [Fact]
    public void StandsDownWhenAGatewayActiveNamesTheSibling()
    {
        BecomeServing();
        _lifecycle.OnGatewayActive(SiblingId);
        Assert.Equal(State.Passive, _lifecycle.CurrentState);
        Assert.False(_lifecycle.IsActivated);
        Assert.Equal(1, _actions.Count("close"));
        Assert.Equal(0, _lifecycle.Advance());
    }

    [Fact]
    public void ReopensOnASecondDesignationAsANewEpoch()
    {
        BecomeServing();
        _lifecycle.OnGatewayActive(SiblingId);
        _lifecycle.OnGatewayActive(MyId);
        Assert.Equal(1, _lifecycle.Advance());
        Assert.Equal(State.Serving, _lifecycle.CurrentState);
        Assert.Equal(2, _actions.Count($"GatewayStarted({MyId})"));
    }

    [Fact]
    public void IgnoresAnotherLogicalGatewaysElection()
    {
        BecomeServing();
        _lifecycle.OnGatewayActive(OtherPairId);
        Assert.Equal(State.Serving, _lifecycle.CurrentState);
        Assert.True(_lifecycle.IsActivated);
        Assert.Equal(0, _actions.Count("close"));
    }

    [Fact]
    public void IgnoresAnActivationForAnUnknownGateway()
    {
        BecomeServing();
        _lifecycle.OnGatewayActive(99);
        Assert.Equal(State.Serving, _lifecycle.CurrentState);
        Assert.True(_lifecycle.IsActivated);
    }

    [Fact]
    public void ASupersededActivationReplayedOnRestartLeavesItPassive()
    {
        LoadTopology();
        _lifecycle.OnGatewayActive(MyId);      // history: this instance once served
        _lifecycle.OnGatewayActive(SiblingId); // and was replaced
        _lifecycle.OnCaughtUp();
        Assert.Equal(0, _lifecycle.Advance());
        Assert.Equal(State.Passive, _lifecycle.CurrentState);
        Assert.Equal(0, _actions.Count("close")); // never opened, so nothing to close
        Assert.Equal(0, _actions.Count($"GatewayStarted({MyId})"));
    }

    [Fact]
    public void RefusesASessionAcquiredWhileTheGateIsShut()
    {
        LoadTopology();
        _lifecycle.OnCaughtUp();
        Assert.False(_lifecycle.OnSessionAcquired());

        _lifecycle.OnGatewayActive(MyId);
        _lifecycle.Advance();
        Assert.True(_lifecycle.OnSessionAcquired());

        _lifecycle.OnGatewayActive(SiblingId); // the gate closed mid-logon
        Assert.False(_lifecycle.OnSessionAcquired());
    }

    [Fact]
    public void IsAnnouncedOnceThisActivationsGatewayStartedIsPlaced()
    {
        LoadTopology();
        _lifecycle.OnGatewayActive(MyId);
        Assert.False(_lifecycle.IsAnnounced, "activated while replaying");

        _lifecycle.OnCaughtUp();
        _actions.GateOpens = false;
        _lifecycle.Advance();
        Assert.True(_lifecycle.IsAnnounced, "placed, though the gate did not open");

        _lifecycle.OnGatewayActive(SiblingId);
        Assert.False(_lifecycle.IsAnnounced);
    }

    [Fact]
    public void ReportsItsPairsRowsInListOrderAndTheInstanceLastActivated()
    {
        LoadTopology();
        Assert.Equal(SnapshotHeader.NoGateway, _lifecycle.ActiveGatewayId);

        _lifecycle.OnGatewayRegistered(MyId, MySourceId, Me, 2); // re-published: replaced in place
        _lifecycle.OnGatewayActive(SiblingId);
        _lifecycle.OnGatewayActive(OtherPairId);

        Assert.Equal(SiblingId, _lifecycle.ActiveGatewayId);
        Assert.Equal(new List<GatewayRow> { new GatewayRow(MyId, 2, Me), new GatewayRow(SiblingId, 1, "GW-B") },
                     _lifecycle.PairRows());
    }

    [Fact]
    public void RestoresItsIdentityAndActivationFromASnapshotHeader()
    {
        var rows = new List<GatewayRow> { new GatewayRow(MyId, 0, Me), new GatewayRow(SiblingId, 1, "GW-B") };
        _lifecycle.OnSnapshotHeader(new GatewayState(MySourceId, MyId, 41, rows));

        Assert.Equal(MyId, _lifecycle.GatewayId);
        Assert.Equal(MySourceId, _lifecycle.GatewaySourceId);
        Assert.True(_lifecycle.IsActivated);
        Assert.Equal(MyId, _lifecycle.ActiveGatewayId);
        Assert.Equal(rows, _lifecycle.PairRows());
        Assert.Equal(State.Replaying, _lifecycle.CurrentState);

        _lifecycle.OnCaughtUp();
        Assert.Equal(1, _lifecycle.Advance());
        Assert.Equal(new List<string> { $"GatewayStarted({MyId})", "open" }, _actions.Calls);
    }

    [Fact]
    public void ASnapshotHeaderReplacesTheRowsAndTheActivationItFinds()
    {
        LoadTopology();
        _lifecycle.OnGatewayActive(MyId);
        var rows = new List<GatewayRow> { new GatewayRow(SiblingId, 0, "GW-B"), new GatewayRow(MyId, 1, Me) };

        _lifecycle.OnSnapshotHeader(new GatewayState(MySourceId, SiblingId, -1, rows));

        Assert.False(_lifecycle.IsActivated);
        Assert.Equal(SiblingId, _lifecycle.ActiveGatewayId);
        Assert.Equal(rows, _lifecycle.PairRows());
        _lifecycle.OnCaughtUp();
        Assert.Equal(0, _lifecycle.Advance());
        Assert.Equal(0, _actions.Count("close"));
    }
}
