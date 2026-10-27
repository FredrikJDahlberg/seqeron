namespace Org.Limitless.Seqeron.App;

/// <summary>The policy knobs a producer rarely changes, kept off its options so that holds only identity and
/// wiring.</summary>
public sealed class Tuning
{
    /// <summary>Messages tracked by confirmed ingress; beyond it <c>Publish</c> returns <c>Declined</c>. A
    /// <see cref="Consumer"/> sends nothing and ignores it.</summary>
    public int PendingCapacity { get; init; } = Session.DefaultPendingCapacity;

    /// <summary>No <c>ClusterHeartbeat</c> for this long fences an instance that acts with
    /// <see cref="ClusterError.QueueStalled"/>; any other logs it.</summary>
    public long QueueStallTimeoutMs { get; init; } = Session.DefaultQueueStallTimeoutMs;

    /// <summary>Recovery dispatching nothing for this long fences, as above, with
    /// <see cref="ClusterError.RecoveryStalled"/>.</summary>
    public long RecoveryStallTimeoutMs { get; init; } = Session.DefaultRecoveryStallTimeoutMs;
}
