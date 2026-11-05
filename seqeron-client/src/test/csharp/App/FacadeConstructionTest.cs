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

    private sealed class MinimalApplicationListener : IApplicationListener
    {
        public void OnLeadershipChanged(bool leading) { }
        public void OnSequenced(Payload payload) { }
        public void OnFenced(ClusterError fence, string detail) { }
    }

    private sealed class MinimalGatewayListener : IGatewayListener
    {
        public bool OnActivated(int firstConnectionId) => false;
        public void OnStandby() { }
        public void OnSequenced(Payload payload) { }
        public void OnFenced(ClusterError fence, string detail) { }
    }

    private sealed class GateRecorder : IApplicationListener
    {
        public readonly System.Collections.Generic.List<string> Seen = new System.Collections.Generic.List<string>();
        public Func<bool> Discharged = () => false;
        public void OnLeadershipChanged(bool leading) => Seen.Add(leading + ":" + Discharged());
        public void OnSequenced(Payload payload) { }
        public void OnFenced(ClusterError fence, string detail) { }
    }

    [Fact(DisplayName = "a closing gate runs OnNotLeader before the listener hears false; an opening one does not")]
    public void ClosingTheGateRunsTheHookFirst()
    {
        var recorder = new GateRecorder();
        bool discharged = false;
        recorder.Discharged = () => discharged;
        var options = new ApplicationOptions
        {
            SourceId = 7, ClientId = 1, EgressChannel = Egress, IngressEndpoints = "0=localhost:1", Listener = recorder,
            MemberId = 1, OnNotLeader = () => discharged = true,
        };
        var application = new Application(options);

        application.GateEdge(true);
        Assert.False(discharged);
        application.GateEdge(false);
        Assert.Equal(new[] { "True:False", "False:True" }, recorder.Seen);
    }

    [Fact(DisplayName = "off the cluster there is no IPC ingress to time, so the two options are rejected together")]
    public void OffClusterRejectsAnIpcTimeout()
    {
        var offCluster = new Application(new ApplicationOptions
        {
            SourceId = 7, ClientId = 3, MemberId = 9, Listener = new MinimalApplicationListener(), OffCluster = true,
            Tuning = new Tuning { PendingCapacity = 16 },
        });
        Assert.False(offCluster.IsLeading);
        var error = Assert.Throws<ArgumentException>(() => new Application(new ApplicationOptions
        {
            SourceId = 7, ClientId = 3, Listener = new MinimalApplicationListener(), OffCluster = true,
            IpcConnectTimeoutMs = 4000,
        }));
        Assert.Contains("IpcConnectTimeoutMs has no meaning with OffCluster", error.Message);
    }

    [Fact(DisplayName = "a consumer needs a group and a listener, and is idle before it starts")]
    public void ConsumerConstructs()
    {
        var consumer = new Consumer(new ConsumerOptions
        {
            ClientId = 4, MemberId = 2, GroupId = 1, Listener = new MinimalConsumerListener(),
        });
        Assert.False(consumer.IsCaughtUp);
        Assert.Equal(0, consumer.LastClusterSeqNo);
        Assert.Throws<ArgumentException>(() => new Consumer(new ConsumerOptions { ClientId = 4, GroupId = 1, Listener = null }));
    }

    private sealed class MinimalConsumerListener : IConsumerListener
    {
        public void OnSequenced(Payload payload) { }
        public void OnFenced(ClusterError fence, string detail) { }
    }

    [Fact(DisplayName = "a listener implementing only the required callbacks is accepted, and the rest do nothing")]
    public void ListenersNeedOnlyTheRequiredCallbacks()
    {
        IApplicationListener application = new MinimalApplicationListener();
        application.OnGroupsResolved(null);
        application.OnCaughtUp(1);
        application.OnClusterHeartbeat(1, 1);
        IGatewayListener gateway = new MinimalGatewayListener();
        gateway.OnGroupsResolved(null);
        gateway.OnConnectionOpened(1, null, 0, 0);
        gateway.OnConnectionClosed(1);
        gateway.OnCaughtUp(1);
        gateway.OnClusterHeartbeat(1, 1);
        var options = new ApplicationOptions
        {
            SourceId = 7, ClientId = 1, EgressChannel = Egress, IngressEndpoints = "0=localhost:1", Listener = application,
            MemberId = 1, OnNotLeader = () => { },
        };
        Assert.NotNull(new Application(options));
    }

    private static SnapshotOptions SnapshotsWith(ISnapshotListener snapshots, string directory)
    {
        return snapshots == null ? null : new SnapshotOptions { Listener = snapshots, Directory = directory };
    }

    private static ApplicationOptions ApplicationWith(string ingress = "0=localhost:1", string directory = null,
                                                  ISnapshotListener snapshots = null, IApplicationListener listener = null)
    {
        return new ApplicationOptions
        {
            SourceId = 7, ClientId = 1, EgressChannel = Egress, IngressEndpoints = ingress,
            Listener = listener ?? new StubApplicationListener(), Snapshots = SnapshotsWith(snapshots, directory),
            MemberId = 1,
        };
    }

    private static GatewayOptions GatewayWith(string name = "GW-A", string ingress = "0=localhost:1",
                                              int sourceId = Gateway.Unresolved, string directory = null,
                                              ISnapshotListener snapshots = null)
    {
        return new GatewayOptions
        {
            GatewayName = name, ClientId = 1, EgressChannel = Egress, IngressEndpoints = ingress, Listener = new StubGatewayListener(),
            Snapshots = SnapshotsWith(snapshots, directory), SourceId = sourceId,
        };
    }

    [Fact(DisplayName = "an application needs its ingress endpoints and a listener")]
    public void ApplicationRequiresIngressAndListener()
    {
        Assert.Equal("IngressEndpoints", Assert.Throws<ArgumentException>(() => new Application(ApplicationWith(null))).ParamName);
        var noListener = new ApplicationOptions { ClientId = 1, EgressChannel = Egress, Listener = null };
        Assert.Equal("Listener", Assert.Throws<ArgumentException>(() => new Application(noListener)).ParamName);
        Assert.Throws<ArgumentNullException>(() => new Application(null));
    }

    [Fact(DisplayName = "an application's snapshot listener needs a directory to keep its files in")]
    public void ApplicationSnapshotListenerNeedsDirectory()
    {
        var error = Assert.Throws<ArgumentException>(() => new Application(ApplicationWith(snapshots: new StubSnapshotListener())));
        Assert.Contains("Snapshots.Directory is required", error.Message);
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
        Assert.Equal(Publish.Declined, application.Reply(new ReplyAddress(2, 9, 1), 5, payload));
    }

    [Fact(DisplayName = "a gateway needs a name, its ingress endpoints and a listener")]
    public void GatewayRequiresNameIngressAndListener()
    {
        Assert.Equal("GatewayName", Assert.Throws<ArgumentException>(() => new Gateway(GatewayWith(name: null))).ParamName);
        Assert.Equal("IngressEndpoints", Assert.Throws<ArgumentException>(() => new Gateway(GatewayWith(ingress: null))).ParamName);
        var noListener = new GatewayOptions { GatewayName = "GW-A", ClientId = 1, EgressChannel = Egress, Listener = null };
        Assert.Equal("Listener", Assert.Throws<ArgumentException>(() => new Gateway(noListener)).ParamName);
    }

    [Fact(DisplayName = "a gateway's snapshot listener needs a source id, then a directory")]
    public void GatewaySnapshotListenerNeedsSourceIdAndDirectory()
    {
        var noSourceId = Assert.Throws<ArgumentException>(
            () => new Gateway(GatewayWith(directory: "unused", snapshots: new StubSnapshotListener())));
        Assert.Contains("SourceId is required with Snapshots", noSourceId.Message);
        var noDirectory = Assert.Throws<ArgumentException>(
            () => new Gateway(GatewayWith(sourceId: 4, snapshots: new StubSnapshotListener())));
        Assert.Contains("Snapshots.Directory is required", noDirectory.Message);
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
