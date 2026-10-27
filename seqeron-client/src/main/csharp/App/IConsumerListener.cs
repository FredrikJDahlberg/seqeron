namespace Org.Limitless.Seqeron.App;

/// <summary>What a consumer does that <see cref="Consumer"/> cannot do for it. Only <see cref="OnSequenced"/> and
/// <see cref="OnFenced"/> must be implemented; the rest do nothing by default.</summary>
public interface IConsumerListener
{
    /// <summary>One application payload off the queue, in <c>clusterSeqNo</c> order.</summary>
    void OnSequenced(Payload payload);

    /// <summary>Every transition to caught-up, the first included.</summary>
    void OnCaughtUp(long clusterSeqNo)
    {
    }

    /// <summary>The cluster clock's tick (spec §7), once a second. It is identical on every node, so a timer driven
    /// by it decides the same thing everywhere.</summary>
    void OnClusterHeartbeat(long clusterTimeNs, long receiveTimeNs)
    {
    }

    /// <summary>Once, latched: this consumer may no longer be trusted to see the queue. Exiting is the usual
    /// way.</summary>
    void OnFenced(ClusterError fence, string detail);
}
