using Org.Limitless.Seqeron.Protocol;

namespace Org.Limitless.Seqeron.App;

/// <summary>Everything one <see cref="Application"/> replica needs to join its deployment; the Java builder's
/// setters, as properties.</summary>
public sealed class ApplicationOptions
{
    /// <summary>This application's <c>sourceId</c>, the one its <c>&lt;applications&gt;</c> row declares —
    /// required, because spec §5 is one id space, and never −1, which ingress refuses.</summary>
    public int SourceId { get; init; }

    /// <summary>This replica's Replayer client id, unique among the co-located apps on its node.</summary>
    public int ClientId { get; init; }

    /// <summary>The node this replica runs on: whose tap it follows, and whose leadership opens its gate.</summary>
    public int MemberId { get; init; }

    /// <summary>Whether this node is a gateway host, running no member, rather than a member: the gate then opens
    /// whoever leads, and ingress is UDP alone. Run one instance, as nothing elects between two.</summary>
    public bool OffCluster { get; init; }

    /// <summary>This client's own egress endpoint; two media drivers on one host cannot both bind a port.</summary>
    public required string EgressChannel { get; init; }

    /// <summary>The members to reach when this node is not leading,
    /// <see cref="PortLayout.IngressEndpoints()"/> by default.</summary>
    public string IngressEndpoints { get; init; } = PortLayout.IngressEndpoints();

    public required IApplicationListener Listener { get; init; }

    /// <summary>What serializes this application's state for snapshot rounds and restores it on start; without one
    /// it takes part in none, whatever its topology row says, and recovers from <c>globalSeqNo</c> 1.</summary>
    public ISnapshotListener SnapshotListener { get; init; }

    /// <summary>Where this replica keeps its snapshots, one file per round; required with a
    /// <see cref="SnapshotListener"/>. Its own: no other instance may write it. A restart restores from what it
    /// holds, so it must outlive the process.</summary>
    public string SnapshotDirectory { get; init; }

    public int PendingCapacity { get; init; } = Application.DefaultPendingCapacity;

    public long TapStallTimeoutMs { get; init; } = Application.DefaultTapStallTimeoutMs;

    public long RecoveryStallTimeoutMs { get; init; } = Application.DefaultRecoveryStallTimeoutMs;

    public long IpcConnectTimeoutMs { get; init; } = Application.DefaultIpcConnectTimeoutMs;
}
