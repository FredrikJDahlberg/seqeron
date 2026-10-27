namespace Org.Limitless.Seqeron.App;

/// <summary>Everything one <see cref="Consumer"/> needs to follow its queue; the Java builder's setters, as
/// properties.</summary>
public sealed class ConsumerOptions
{
    /// <summary>This consumer's client id, its counters' label — required, and unique among the clients on its node:
    /// two sharing one share their counters' series.</summary>
    public required int ClientId { get; init; }

    /// <summary>The node whose queue this consumer follows, or a gateway host's node id.</summary>
    public int MemberId { get; init; }

    /// <summary>The group whose queue this consumer reads — required; the first topology's is 1.</summary>
    public required int GroupId { get; init; }

    public required IConsumerListener Listener { get; init; }

    /// <summary>The stall policy; <see cref="Tuning.PendingCapacity"/> is unused, as a consumer sends
    /// nothing.</summary>
    public Tuning Tuning { get; init; } = new Tuning();
}
