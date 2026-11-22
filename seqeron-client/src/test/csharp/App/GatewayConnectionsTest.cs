using System.Collections.Generic;
using Org.Limitless.Seqeron.Protocol;
using Xunit;

namespace Org.Limitless.Seqeron.App;

/// <summary>
/// Drives <see cref="GatewayConnections"/> over a real <see cref="GatewayLifecycle"/>: <c>GW-A</c> (id 5) and
/// <c>GW-B</c> (id 6) under <c>gatewaySourceId</c> 6.
/// <para>Case for case with <c>GatewayConnectionsTest.java</c>.</para>
/// </summary>
public class GatewayConnectionsTest
{
    private const int MyId = 5;
    private const int SiblingId = 6;
    private const int SourceId = 6;
    private const int FirstId = 10;

    private sealed class Actions : GatewayLifecycle.IActions
    {
        public GatewayConnections Connections;

        public bool PublishGatewayStarted(int gatewayId)
        {
            Connections.ResumeAt(FirstId);
            return true;
        }

        public bool OpenGate()
        {
            return true;
        }

        public void CloseGate()
        {
            Connections.OnStandby();
        }
    }

    // Records each frame placed, refusing every one once Lands is false.
    private sealed class Log : GatewayConnections.IPlacer
    {
        public readonly List<string> Placed = new List<string>();
        public bool Lands = true;

        public bool Place(int connectionId, int systemEventType, byte[] connectionData)
        {
            if (!Lands)
            {
                return false;
            }
            Placed.Add((systemEventType == SystemFrame.ConnectionOpened ? "opened " : "closed ") + connectionId);
            return true;
        }
    }

    private readonly GatewayLifecycle _lifecycle;
    private readonly GatewayConnections _connections;
    private readonly Log _log = new Log();

    public GatewayConnectionsTest()
    {
        var actions = new Actions();
        _lifecycle = new GatewayLifecycle("GW-A", actions);
        _connections = new GatewayConnections(_lifecycle);
        actions.Connections = _connections;
        _lifecycle.OnGatewayRegistered(MyId, SourceId, "GW-A", 0);
        _lifecycle.OnGatewayRegistered(SiblingId, SourceId, "GW-B", 1);
        _lifecycle.OnCaughtUp();
    }

    private void Serve()
    {
        _lifecycle.OnGatewayActive(MyId);
        Assert.Equal(1, _lifecycle.Advance());
    }

    [Fact]
    public void AStandbyPublishesNothingOnAConnectionOrOffOne()
    {
        Assert.False(_connections.MayPublish(Gateway.NoConnection));
        Assert.Equal(Gateway.NoConnection, _connections.Open(new byte[0]));

        _lifecycle.OnGatewayActive(SiblingId);
        Assert.False(_connections.MayPublish(Gateway.NoConnection));

        Serve();
        Assert.True(_connections.MayPublish(Gateway.NoConnection));

        _lifecycle.OnGatewayActive(SiblingId);
        Assert.False(_connections.MayPublish(Gateway.NoConnection));
    }

    [Fact]
    public void APayloadWaitsForItsConnectionsConnectionOpened()
    {
        Serve();
        int id = _connections.Open(new byte[0]);
        Assert.Equal(FirstId, id);
        Assert.False(_connections.MayPublish(id));
        Assert.Equal(1, _connections.Drain(_log));
        Assert.True(_connections.MayPublish(id));
        Assert.Equal(new[] { "opened 10" }, _log.Placed);
    }

    [Fact]
    public void AConnectionClosedBeforeItsConnectionOpenedLandedIsNeverAnnounced()
    {
        Serve();
        int id = _connections.Open(new byte[0]);
        _connections.Close(id);
        Assert.Equal(0, _connections.Drain(_log));
        Assert.Equal(0, _connections.Count);
    }

    [Fact]
    public void TheDrainStopsAtTheFirstFrameThatDoesNotLandAndResumesInOrder()
    {
        Serve();
        int first = _connections.Open(new byte[0]);
        _connections.Open(new byte[0]);
        _log.Lands = false;
        Assert.Equal(0, _connections.Drain(_log));
        _log.Lands = true;
        _connections.Close(first);
        Assert.Equal(1, _connections.Drain(_log)); // the unplaced open of the first is dropped with it
        Assert.Equal(new[] { "opened 11" }, _log.Placed);
    }

    [Fact]
    public void TheEdgeClosingClosesEveryConnectionTheLogSawAndAnnouncesNoneItHasNot()
    {
        Serve();
        int seen = _connections.Open(new byte[0]);
        int closed = _connections.Open(new byte[0]);
        _connections.Drain(_log);
        _connections.Close(closed);
        int unseen = _connections.Open(new byte[0]);

        _lifecycle.OnGateClosed();
        _connections.OnEdgeClosed();
        _connections.Close(seen); // after the edge has gone: nothing more to say
        _connections.Drain(_log);

        Assert.Equal(new[] { $"opened {seen}", $"opened {closed}", $"closed {closed}", $"closed {seen}" }, _log.Placed);
        Assert.False(_connections.MayPublish(seen));
        Assert.False(_connections.MayPublish(unseen));
        Assert.False(_connections.MayPublish(Gateway.NoConnection));
        Assert.Equal(0, _connections.Count);
    }

    [Fact]
    public void AReopenedEdgeAllocatesPastTheIdsTheClosedOneTook()
    {
        Serve();
        _connections.Open(new byte[0]);
        _lifecycle.OnGateClosed();
        _connections.OnEdgeClosed();

        Assert.Equal(1, _lifecycle.Advance()); // the designation stands: the gate reopens without a GatewayStarted
        Assert.Equal(FirstId + 1, _connections.Open(new byte[0]));
    }

    [Fact]
    public void StandingDownDropsWhatIsOwed()
    {
        Serve();
        _connections.Open(new byte[0]);
        _lifecycle.OnGatewayActive(SiblingId);
        Assert.Equal(0, _connections.Drain(_log));
        Assert.Equal(0, _connections.Count);
    }
}
