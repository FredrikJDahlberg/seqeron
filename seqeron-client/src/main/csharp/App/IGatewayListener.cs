using Adaptive.Agrona;

namespace Org.Limitless.Seqeron.App;

/// <summary>What a gateway does that <see cref="Gateway"/> cannot do for it.</summary>
public interface IGatewayListener
{
    /// <summary>This instance has been designated, has announced itself and may now serve: open the edge.</summary>
    /// <param name="firstConnectionId">the id <see cref="Gateway.OpenConnection()"/> will allocate first, resumed
    /// past whatever a predecessor issued — a gateway that numbers its own sessions starts here</param>
    /// <returns>whether the edge opened; false is retried on the next <see cref="Gateway.DoWork"/></returns>
    bool OnActivated(int firstConnectionId);

    /// <summary>Stand down: close the edge and drop every connection it let in. Publishes nothing.</summary>
    void OnStandby();

    /// <summary>One application payload off this node's tap, in <c>globalSeqNo</c> order.</summary>
    void OnSequenced(Payload payload);

    /// <summary>This logical gateway took a connection, whichever instance issued it. A consumer that keeps
    /// per-connection state rebuilds it from these while it replays. The buffer is valid only during the
    /// call.</summary>
    void OnConnectionOpened(int connectionId, IDirectBuffer connectionData, int offset, int length);

    /// <summary>That connection has gone. A client that drops its socket without logging out produces no payload at
    /// all, so this is the only notice of it.</summary>
    void OnConnectionClosed(int connectionId);

    /// <summary>Every transition to caught-up, the first included.</summary>
    void OnCaughtUp(long globalSeqNo);

    /// <summary>
    /// The cluster clock's tick (spec §7), once a second. It is the one time source that keeps advancing while every
    /// producer is silent, which is exactly when a watchdog must still fire, and it is identical on every node — so a
    /// timer driven by it decides the same thing everywhere.
    /// </summary>
    void OnClusterHeartbeat(long clusterTimeNs, long receiveTimeNs);

    /// <summary>Once, latched: release the cluster session — exiting is the usual way — so a standby takes
    /// over.</summary>
    void OnFenced(ClusterError fence, string detail);
}
