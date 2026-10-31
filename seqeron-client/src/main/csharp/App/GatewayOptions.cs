using System;
using System.Collections.Generic;
using Org.Limitless.Seqeron.Protocol;

namespace Org.Limitless.Seqeron.App;

/// <summary>Everything one <see cref="Gateway"/> instance needs to join its pair; the Java builder's setters, as
/// properties.</summary>
public sealed class GatewayOptions
{
    /// <summary>The <c>GatewayRegistered</c> row name this instance joins on.</summary>
    public required string GatewayName { get; init; }

    /// <summary>The names of the groups this gateway publishes to besides its own; one the log's group table lacks
    /// fences it with <see cref="ClusterError.GroupUnknown"/>.</summary>
    public IReadOnlyList<string> AddressedGroups { get; init; } = Array.Empty<string>();

    /// <summary>The logical gateway's <c>sourceId</c>, the one its rows declare. Required with
    /// <see cref="Snapshots"/>, whose snapshot counter it labels.</summary>
    public int SourceId { get; init; } = Gateway.Unresolved;

    /// <summary>Takes this instance into snapshot rounds; without it it takes part in none, whatever its rows say,
    /// and recovers from <c>clusterSeqNo</c> 1. Requires a <see cref="SourceId"/>.</summary>
    public SnapshotOptions Snapshots { get; init; }

    /// <summary>This instance's client id, its counters' label — required, and unique among the clients on its
    /// node: two sharing one share their counters' series.</summary>
    public required int ClientId { get; init; }

    /// <summary>The node whose queue this instance follows.</summary>
    public int MemberId { get; init; }

    /// <summary>This client's own egress endpoint; two media drivers on one host cannot both bind a port. An
    /// ephemeral port on localhost, <see cref="ApplicationOptions.DefaultEgressChannel"/>, by default.</summary>
    public string EgressChannel { get; init; } = ApplicationOptions.DefaultEgressChannel;

    /// <summary>The members to reach, <see cref="PortLayout.IngressEndpoints()"/> by default.</summary>
    public string IngressEndpoints { get; init; } = PortLayout.IngressEndpoints();

    public required IGatewayListener Listener { get; init; }

    /// <summary>The stall and capacity policy.</summary>
    public Tuning Tuning { get; init; } = new Tuning();
}
