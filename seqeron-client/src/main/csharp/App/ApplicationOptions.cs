using System;
using System.Collections.Generic;
using Org.Limitless.Seqeron.Protocol;

namespace Org.Limitless.Seqeron.App;

/// <summary>Everything one <see cref="Application"/> replica needs to join its deployment; the Java builder's
/// setters, as properties.</summary>
public sealed class ApplicationOptions
{
    /// <summary>An ephemeral port: one cluster session, and no port of its own to allocate.</summary>
    public const string DefaultEgressChannel = "aeron:udp?endpoint=localhost:0";

    /// <summary>This application's <c>sourceId</c>, the one its <c>&lt;applications&gt;</c> row declares —
    /// required, because spec §5 is one id space, and never −1, which ingress refuses.</summary>
    public int SourceId { get; init; }

    /// <summary>The names of the groups this application publishes to besides its own; one the log's group table
    /// lacks fences it with <see cref="ClusterError.GroupUnknown"/>.</summary>
    public IReadOnlyList<string> AddressedGroups { get; init; } = Array.Empty<string>();

    /// <summary>This replica's client id, its counters' label — required, and unique among the clients on its
    /// node: two sharing one share their counters' series.</summary>
    public required int ClientId { get; init; }

    /// <summary>The node this replica runs on: whose queue it follows, and whose leadership opens its gate.</summary>
    public int MemberId { get; init; }

    /// <summary>Whether this node is a gateway host, running no member, rather than a member: the gate then opens
    /// whoever leads, and ingress is UDP alone. Run one instance, as nothing elects between two. Not with an
    /// <see cref="IpcConnectTimeoutMs"/>, which has no IPC ingress to time.</summary>
    public bool OffCluster { get; init; }

    /// <summary>This client's own egress endpoint; two media drivers on one host cannot both bind a port. An
    /// ephemeral port on localhost, <see cref="DefaultEgressChannel"/>, by default.</summary>
    public string EgressChannel { get; init; } = DefaultEgressChannel;

    /// <summary>The members to reach when this node is not leading,
    /// <see cref="PortLayout.IngressEndpoints()"/> by default.</summary>
    public string IngressEndpoints { get; init; } = PortLayout.IngressEndpoints();

    public required IApplicationListener Listener { get; init; }

    /// <summary>Takes this replica into snapshot rounds; without it it takes part in none, whatever its topology row
    /// says, and recovers from <c>clusterSeqNo</c> 1.</summary>
    public SnapshotOptions Snapshots { get; init; }

    /// <summary>Called whenever the gate closes, before <see cref="IApplicationListener.OnLeadershipChanged"/>(false):
    /// the place for <c>OutstandingWork.OnNotLeader</c>. Optional; without it the listener does so.</summary>
    public Action OnNotLeader { get; init; }

    /// <summary>The stall and capacity policy.</summary>
    public Tuning Tuning { get; init; } = new Tuning();

    /// <summary>How long to wait for IPC ingress to the local member while it leads before using UDP,
    /// <see cref="Application.DefaultIpcConnectTimeoutMs"/> by default. Not with <see cref="OffCluster"/>.</summary>
    public long? IpcConnectTimeoutMs { get; init; }
}
