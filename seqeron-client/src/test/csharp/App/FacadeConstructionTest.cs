using System;
using Adaptive.Agrona;
using Org.Limitless.Seqeron.Protocol;
using Xunit;

namespace Org.Limitless.Seqeron.App;

/// <summary>
/// The façades' construction rules and what a replica that has not connected answers. No Aeron is started. Case for
/// case with <c>ApplicationBuilderTest.java</c> and <c>GatewayBuilderTest.java</c>.
/// </summary>
public class FacadeConstructionTest
{
    private const string Egress = "aeron:udp?endpoint=localhost:0";

    private sealed class StubApplicationListener : IApplicationListener
    {
        public void OnGroupsResolved(GroupDirectory directory) { }
        public void OnLeadershipChanged(bool leading) { }
        public void OnSequenced(Payload payload) { }
        public void OnCaughtUp(long clusterSeqNo) { }
        public void OnClusterHeartbeat(long clusterTimeNs, long receiveTimeNs) { }
        public void OnFenced(ClusterError fence, string detail) { }
    }

    private sealed class StubGatewayListener : IGatewayListener
    {
        public void OnGroupsResolved(GroupDirectory directory) { }
        public bool OnActivated(int firstConnectionId) => false;
        public void OnStandby() { }
        public void OnSequenced(Payload payload) { }
        public void OnConnectionOpened(int connectionId, IDirectBuffer connectionData, int offset, int length) { }
        public void OnConnectionClosed(int connectionId) { }
        public void OnCaughtUp(long clusterSeqNo) { }
        public void OnClusterHeartbeat(long clusterTimeNs, long receiveTimeNs) { }
        public void OnFenced(ClusterError fence, string detail) { }
    }

    private sealed class StubSnapshotListener : ISnapshotListener
    {
        public int FormatVersion => 1;
        public int OnSnapshot(IMutableDirectBuffer buffer, int recordIndex) => 0;
        public void OnRestore(IDirectBuffer buffer, int length, int recordIndex, int formatVersion) { }
    }

    private static ApplicationOptions ApplicationWith(string ingress = "0=localhost:1", string directory = null,
                                                  ISnapshotListener snapshots = null, IApplicationListener listener = null)
    {
        return new ApplicationOptions
        {
            SourceId = 7, EgressChannel = Egress, IngressEndpoints = ingress,
            Listener = listener ?? new StubApplicationListener(), SnapshotListener = snapshots,
            SnapshotDirectory = directory, MemberId = 1,
        };
    }

    private static GatewayOptions GatewayWith(string name = "GW-A", string ingress = "0=localhost:1",
                                              int sourceId = Gateway.Unresolved, string directory = null,
                                              ISnapshotListener snapshots = null)
    {
        return new GatewayOptions
        {
            GatewayName = name, EgressChannel = Egress, IngressEndpoints = ingress, Listener = new StubGatewayListener(),
            SnapshotListener = snapshots, SnapshotDirectory = directory, SourceId = sourceId,
        };
    }

    [Fact(DisplayName = "an application needs its ingress endpoints and a listener")]
    public void ApplicationRequiresIngressAndListener()
    {
        Assert.Equal("IngressEndpoints", Assert.Throws<ArgumentException>(() => new Application(ApplicationWith(null))).ParamName);
        var noListener = new ApplicationOptions { EgressChannel = Egress, Listener = null };
        Assert.Equal("Listener", Assert.Throws<ArgumentException>(() => new Application(noListener)).ParamName);
        Assert.Throws<ArgumentNullException>(() => new Application(null));
    }

    [Fact(DisplayName = "an application's snapshot listener needs a directory to keep its files in")]
    public void ApplicationSnapshotListenerNeedsDirectory()
    {
        var error = Assert.Throws<ArgumentException>(() => new Application(ApplicationWith(snapshots: new StubSnapshotListener())));
        Assert.Contains("SnapshotDirectory is required with a SnapshotListener", error.Message);
    }

    [Fact(DisplayName = "before it has caught up a replica is not leading, has seen nothing, and declines every publish")]
    public void ApplicationDeclinesWhileTheGateIsShut()
    {
        var application = new Application(ApplicationWith());
        var payload = new byte[16];
        Assert.Equal(7, application.SourceId);
        Assert.False(application.IsLeading);
        Assert.False(application.CanPublish);
        Assert.False(application.IsCaughtUp);
        Assert.Equal(Publish.Declined, application.Publish(5, payload));
        Assert.Equal(Publish.Declined, application.Reply(2, 9, 1, 5, payload));
    }

    [Fact(DisplayName = "a gateway needs a name, its ingress endpoints and a listener")]
    public void GatewayRequiresNameIngressAndListener()
    {
        Assert.Equal("GatewayName", Assert.Throws<ArgumentException>(() => new Gateway(GatewayWith(name: null))).ParamName);
        Assert.Equal("IngressEndpoints", Assert.Throws<ArgumentException>(() => new Gateway(GatewayWith(ingress: null))).ParamName);
        var noListener = new GatewayOptions { GatewayName = "GW-A", EgressChannel = Egress, Listener = null };
        Assert.Equal("Listener", Assert.Throws<ArgumentException>(() => new Gateway(noListener)).ParamName);
    }

    [Fact(DisplayName = "a gateway's snapshot listener needs a source id, then a directory")]
    public void GatewaySnapshotListenerNeedsSourceIdAndDirectory()
    {
        var noSourceId = Assert.Throws<ArgumentException>(
            () => new Gateway(GatewayWith(directory: "unused", snapshots: new StubSnapshotListener())));
        Assert.Contains("SourceId is required with a SnapshotListener", noSourceId.Message);
        var noDirectory = Assert.Throws<ArgumentException>(
            () => new Gateway(GatewayWith(sourceId: 4, snapshots: new StubSnapshotListener())));
        Assert.Contains("SnapshotDirectory is required with a SnapshotListener", noDirectory.Message);
    }

    [Fact(DisplayName = "an instance that is not serving accepts no connection and has nothing to close")]
    public void GatewayTakesNoConnectionUntilServing()
    {
        var gateway = new Gateway(GatewayWith());
        Assert.Equal(Gateway.Unresolved, gateway.SourceId);
        Assert.Equal(Gateway.Unresolved, gateway.GatewayId);
        Assert.False(gateway.CanAccept);
        Assert.False(gateway.IsActivated);
        Assert.False(gateway.IsServing);
        Assert.False(gateway.IsCaughtUp);
        Assert.Equal(Gateway.NoConnection, gateway.OpenConnection());
        Assert.Equal(Gateway.NoConnection, gateway.OpenConnection(new byte[8]));
        gateway.CloseConnection(0);
        gateway.GateClosed();
        Assert.False(gateway.IsServing);
    }
}
