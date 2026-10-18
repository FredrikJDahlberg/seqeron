namespace Org.Limitless.Seqeron.App;

/// <summary>What an application does that <see cref="Application"/> cannot do for it.</summary>
public interface IApplicationListener
{
    /// <summary>The log's group table, once, before the gate first opens: what
    /// <see cref="Application.Publish(ushort, int, Adaptive.Agrona.IDirectBuffer, int)"/> takes its masks
    /// from.</summary>
    void OnGroupsResolved(GroupDirectory directory);

    /// <summary>
    /// The leader gate crossed an edge: true when this replica may do leader-only work — caught up, and its own node
    /// leads, or off the cluster caught up alone — false when it may not. <b>Every leadership change closes an open
    /// gate</b>, so false is where <see cref="OutstandingWork{TKey,TWork}.OnNotLeader"/> belongs: a reply this node
    /// submitted during the election may have gone with it, and the next opening dispatches it again. False also
    /// comes before <see cref="OnFenced"/> when this replica fences with its gate open.
    /// </summary>
    void OnLeadershipChanged(bool leading);

    /// <summary>One application payload off this node's queue, in <c>clusterSeqNo</c> order.</summary>
    void OnSequenced(Payload payload);

    /// <summary>Every transition to caught-up, the first included.</summary>
    void OnCaughtUp(long clusterSeqNo);

    /// <summary>
    /// The cluster clock's tick (spec §7), once a second. It is the one time source that keeps advancing while every
    /// producer is silent, which is exactly when a watchdog must still fire, and it is identical on every node — so a
    /// timer driven by it decides the same thing everywhere.
    /// </summary>
    void OnClusterHeartbeat(long clusterTimeNs, long receiveTimeNs);

    /// <summary>Once, latched: this replica may no longer act. Exiting is the usual way — its restart
    /// re-walks.</summary>
    void OnFenced(ClusterError fence, string detail);
}
