using Org.Limitless.Seqeron.Protocol;

namespace Org.Limitless.Seqeron.App;

/// <summary>Everything one <see cref="Gateway"/> instance needs to join its pair; the Java builder's setters, as
/// properties.</summary>
public sealed class GatewayOptions
{
    /// <summary>The <c>GatewayRegistered</c> row name this instance joins on.</summary>
    public required string GatewayName { get; init; }

    /// <summary>The logical gateway's <c>sourceId</c>, the one its rows declare. Required with a
    /// <see cref="SnapshotListener"/>, whose snapshot counter it labels.</summary>
    public int SourceId { get; init; } = Gateway.Unresolved;

    /// <summary>What serializes this instance's state for snapshot rounds and restores it on start; without one it
    /// takes part in none, whatever its rows say, and recovers from <c>globalSeqNo</c> 1.</summary>
    public ISnapshotListener SnapshotListener { get; init; }

    /// <summary>Where this instance keeps its snapshots, one file per round; required with a
    /// <see cref="SnapshotListener"/>. Its own: no other instance, its pair's included, may write it. A restart
    /// restores from what it holds, so it must outlive the process.</summary>
    public string SnapshotDirectory { get; init; }

    /// <summary>This replica's client id, its counters' label; unique among the co-located apps on its node.</summary>
    public int ClientId { get; init; }

    /// <summary>The node whose tap this instance follows.</summary>
    public int MemberId { get; init; }

    /// <summary>This client's own egress endpoint; two media drivers on one host cannot both bind a port.</summary>
    public required string EgressChannel { get; init; }

    /// <summary>The members to reach, <see cref="PortLayout.IngressEndpoints()"/> by default.</summary>
    public string IngressEndpoints { get; init; } = PortLayout.IngressEndpoints();

    public required IGatewayListener Listener { get; init; }

    public int PendingCapacity { get; init; } = Gateway.DefaultPendingCapacity;

    public long TapStallTimeoutMs { get; init; } = Gateway.DefaultTapStallTimeoutMs;

    public long RecoveryStallTimeoutMs { get; init; } = Gateway.DefaultRecoveryStallTimeoutMs;
}
