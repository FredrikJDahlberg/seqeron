using System;
using Adaptive.Aeron;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona;
using Adaptive.Cluster.Client;
using Adaptive.Cluster.Codecs;
using Org.Limitless.Seqeron.Protocol;
using Org.Limitless.Seqeron.Replayer.Client;
using Org.Limitless.Seqeron.Sequencer.Client;
using Org.Limitless.Seqeron.Util;

namespace Org.Limitless.Seqeron.App;

/// <summary>
/// What every seqeron client does the same way: the cluster session it submits on, the co-located tap it follows,
/// confirmed ingress across a failover, and the fences that say when it may no longer act — wired together into one
/// <see cref="DoWork"/> whose ordering is not the caller's to get right.
/// <para>Not API. <see cref="Gateway"/> and <see cref="Application"/> are the façades over it.
/// <c>Session.java</c> and <c>app/detail/Session.hpp</c> are its twins; keep the three in step.</para>
/// </summary>
internal sealed class Session : IDisposable
{
    /// <summary>The deployment policy every producer had been copying: the tap may be silent for 20 heartbeat
    /// periods.</summary>
    internal const long DefaultTapStallTimeoutMs = 20 * FrameLayer.ClusterHeartbeatIntervalMs;

    /// <summary>Longer than the tap's, since a re-walk is slower than the live stream it is catching up to.</summary>
    internal const long DefaultRecoveryStallTimeoutMs = 3 * DefaultTapStallTimeoutMs;

    /// <summary>Frames in flight between a publish and the tap; far above what one round trip holds.</summary>
    internal const int DefaultPendingCapacity = 1024;

    /// <summary>How often a lost session is replaced while the façade allows it.</summary>
    internal const long ReconnectIntervalMs = 1_000;

    /// <summary>What the façade above does with what comes off the tap, and with a fence.</summary>
    internal interface IDispatch
    {
        /// <summary>A system frame this core does not consume itself. <c>LeadershipChanged</c> never arrives
        /// here.</summary>
        void OnSystem(SequencedEvent sequencedEvent);

        /// <summary>A <c>LeadershipChanged</c> was applied; confirmed ingress has already taken the new
        /// term.</summary>
        void OnLeadershipChanged();

        void OnPayload(Payload payload);

        /// <summary>Every transition to caught-up, the first included.</summary>
        void OnCaughtUp(long globalSeqNo);

        /// <summary>The cluster clock's tick, once a second.</summary>
        void OnClusterHeartbeat(long clusterTimeNs, long receiveTimeNs);

        /// <summary>Once, latched: this producer may no longer act.</summary>
        void OnFenced(ClusterError fence, string detail);

        /// <summary>Whether a session the cluster closed may be replaced rather than fenced: the instance holds
        /// nothing the loss of that session changed. Asked on every cycle without one.</summary>
        bool MayReconnect();

        /// <summary>Whether this instance acts for its producer right now: a stall fences only one that
        /// does.</summary>
        bool IsActing();
    }

    private readonly IDispatch _dispatch;
    private readonly PendingSends _pending;
    private readonly IngressPublisher _publisher;
    private readonly ClusterStreamSender _sender = new ClusterStreamSender();
    private readonly ReplayerStreamReceiver _receiver;
    private readonly RecoveryStallFence _recoveryStall;
    private readonly TapStallFence _tapStall;
    private readonly Payload _payload = new Payload();

    private readonly long _recoveryStallTimeoutMs;
    private readonly long _tapStallTimeoutMs;

    // The cluster's own account of why this session ended, kept for the fence's detail.
    private string _sessionFault;

    // The stall an instance that does not act has been alarmed for, or null.
    private ClusterError? _stallAlarm;

    private bool _caughtUp;
    private bool _fenced;

    // Monotonic ms when the session was found lost; -1 while one is open.
    private long _sessionLostSinceMs = -1;
    private long _nextReconnectMs;

    // The term of the last LeadershipChanged applied; -1 before the first.
    private long _leadershipTermId = -1;

    internal Session(int clientId, int pendingCapacity, long tapStallTimeoutMs, long recoveryStallTimeoutMs,
                     IDispatch dispatch)
    {
        _dispatch = dispatch;
        _tapStallTimeoutMs = tapStallTimeoutMs;
        _recoveryStallTimeoutMs = recoveryStallTimeoutMs;
        _pending = new PendingSends(pendingCapacity);
        _publisher = new IngressPublisher(_pending);
        _recoveryStall = new RecoveryStallFence(recoveryStallTimeoutMs);
        _tapStall = new TapStallFence(tapStallTimeoutMs);
        _receiver = new ReplayerStreamReceiver(clientId, OnSequenced, OnLeadershipChanged, null);
        _sender.SetIngressHold(_pending);
    }

    /// <summary>Restores <paramref name="sourceId"/>'s newest confirmed snapshot in <paramref name="store"/> first;
    /// call before starting.</summary>
    internal void RestoreFrom(int sourceId, SnapshotStore store, ISnapshotRestoreHandler handler)
    {
        _receiver.RestoreFrom(sourceId, store, handler);
    }

    /// <summary>Follows the tap from its start again, restoring the snapshot given to <see cref="RestoreFrom"/>
    /// first.</summary>
    internal void Restart()
    {
        _recoveryStall.OnRestart(); // the restore pass dispatches nothing, and is no stall
        _receiver.Restart();
    }

    /// <summary>Opens the cluster session over UDP and starts following this node's tap.</summary>
    internal void Start(Aeron aeron, int memberId, string egressChannel, string ingressEndpoints)
    {
        _sender.Connect(aeron, egressChannel, ingressEndpoints, new SessionListener(this));
        _receiver.Start(aeron, memberId);
    }

    /// <summary>The same for a client sharing this member's media driver: ingress over its own <c>aeron:ipc</c>
    /// while that member leads, the UDP endpoint set when it does not.</summary>
    internal void StartColocated(Aeron aeron, int memberId, long ipcConnectTimeoutMs, string egressChannel,
                                 string ingressEndpoints)
    {
        _sender.ConnectColocated(aeron, memberId, ipcConnectTimeoutMs, egressChannel, ingressEndpoints,
                                 new SessionListener(this));
        _receiver.Start(aeron, memberId);
    }

    /// <summary>One duty cycle, in the order the parts require: the tap first, so a fence is judged on what this
    /// cycle saw; the resend last, so nothing new goes out ahead of it.</summary>
    /// <returns>units of work done</returns>
    internal int DoWork()
    {
        if (_fenced)
        {
            return 0;
        }
        int work = _receiver.Poll();
        CheckCaughtUp();
        CheckFences();
        if (_fenced)
        {
            return work;
        }
        work += _sender.PollEgress();
        _sender.KeepAlive();
        work += _pending.ResendMissing(_sender);
        return work;
    }

    internal Publish PublishPayload(int sourceId, int connectionId, int payloadId, IDirectBuffer body, int bodyLength)
    {
        return _fenced ? Publish.Declined
                       : _publisher.PublishPayload(_sender, sourceId, connectionId, payloadId, body, bodyLength);
    }

    internal Publish PublishSystem(int sourceId, int connectionId, int systemEventType, IDirectBuffer body,
                                   int bodyLength)
    {
        return _fenced ? Publish.Declined
                       : _publisher.PublishSystem(_sender, sourceId, connectionId, systemEventType, body, bodyLength);
    }

    /// <summary>Tells the cluster this client is alive, for a façade whose consumer spins inside a callback and
    /// would otherwise starve the one in <see cref="DoWork"/>. Self-throttling.</summary>
    internal void KeepAlive()
    {
        if (!_fenced)
        {
            _sender.KeepAlive();
        }
    }

    /// <summary>Send nothing new while this holds: an older term's frames are still unseen or unresent.</summary>
    internal bool IsHolding => _pending.IsHolding;

    internal bool IsCaughtUp => _receiver.IsCaughtUp;

    internal long LastGlobalSeqNo => _receiver.LastGlobalSeqNo;

    internal int CurrentLeaderMemberId => _receiver.CurrentLeaderMemberId;

    internal long LeadershipTermId => _leadershipTermId;

    /// <summary>Whether a cluster session is open. The recorded fault and <c>IsConnected</c> as well as the sender's
    /// latch: <c>AeronCluster</c> also closes itself, with no event at all, when a new leader does not arrive before
    /// its timeout.</summary>
    internal bool HasSession => _sessionFault == null && !_sender.IsSessionLost && _sender.IsConnected;

    public void Dispose()
    {
        _receiver.Dispose();
        _sender.Dispose();
    }

    // Polled rather than taken off the receiver's callback: neither fence may wait on the next frame to land.
    private void CheckCaughtUp()
    {
        bool now = _receiver.IsCaughtUp;
        if (now == _caughtUp)
        {
            return;
        }
        _caughtUp = now;
        if (!now)
        {
            return;
        }
        long nowMs = Clocks.MonotonicMs();
        _recoveryStall.OnCaughtUp();
        _tapStall.OnCaughtUp(nowMs);
        _dispatch.OnCaughtUp(_receiver.LastGlobalSeqNo);
    }

    private void CheckFences()
    {
        if (!HasSession)
        {
            string why = _sessionFault ?? "closed";
            if (!_dispatch.MayReconnect())
            {
                Fence(ClusterError.ClusterSessionLost, why);
                return;
            }
            ReplaceSession(why);
            if (_fenced)
            {
                return;
            }
        }
        if (_pending.IsFaulted)
        {
            Fence(ClusterError.IngressConfirmFaulted, "an own frame came back differing from the oldest pending one, " +
                                                          "so what reached the log can no longer be counted");
            return;
        }
        if (_receiver.RestoreFailure != null)
        {
            Fence(ClusterError.SnapshotUnrestorable, _receiver.RestoreFailure);
            return;
        }
        long nowMs = Clocks.MonotonicMs();
        if (!_receiver.IsCaughtUp)
        {
            if (_recoveryStall.OnNotCaughtUp(nowMs, _receiver.LastGlobalSeqNo))
            {
                Stalled(ClusterError.RecoveryStalled, Logger.CoreEventCode.RecoveryStalled,
                        "recovery has dispatched nothing for >" + _recoveryStallTimeoutMs +
                            "ms (globalSeqNo stuck at " + _receiver.LastGlobalSeqNo + ")");
                return;
            }
        }
        else if (_tapStall.IsStalled(nowMs))
        {
            Stalled(ClusterError.TapStalled, Logger.CoreEventCode.TapStalled,
                    "no ClusterHeartbeat for >" + _tapStallTimeoutMs + "ms");
            return;
        }
        if (_stallAlarm != null)
        {
            Logger.Info(Logger.CoreComponent.Cluster, null, "{0} cleared", _stallAlarm);
            _stallAlarm = null;
        }
    }

    // Fences an instance that acts; one that does not raises an alarm once and keeps following the tap.
    private void Stalled(ClusterError stall, Logger.CoreEventCode code, string detail)
    {
        if (_dispatch.IsActing())
        {
            Fence(stall, detail);
            return;
        }
        if (_stallAlarm != stall)
        {
            _stallAlarm = stall;
            Logger.Error(Logger.CoreComponent.Cluster, code, null,
                         "{0}, not fenced while this instance does not act: {1}", stall, detail);
        }
    }

    // Opens a session in place of the lost one, once a second, until one opens or the tap could have gone silent for
    // as long: past that the cluster is not coming back for this process, and the session's loss is a fence.
    private void ReplaceSession(string why)
    {
        long nowMs = Clocks.MonotonicMs();
        if (_sessionLostSinceMs < 0)
        {
            _sessionLostSinceMs = nowMs;
            _nextReconnectMs = nowMs;
        }
        if (nowMs - _sessionLostSinceMs > _tapStallTimeoutMs)
        {
            Fence(ClusterError.ClusterSessionLost,
                  why + "; no session replaced it within " + _tapStallTimeoutMs + "ms");
            return;
        }
        if (nowMs < _nextReconnectMs)
        {
            return;
        }
        _nextReconnectMs = nowMs + ReconnectIntervalMs;
        if (!_sender.Reconnect())
        {
            return;
        }
        _sessionFault = null;
        _sessionLostSinceMs = -1;
        DiscardUnconfirmed("the session they went out on was lost (" + why + ")");
    }

    /// <summary>Forgets every frame not yet seen on the tap, for a façade that knows none of them will be
    /// sequenced.</summary>
    internal void DiscardUnconfirmed(string why)
    {
        int dropped = _pending.DiscardUnconfirmed();
        if (dropped > 0)
        {
            Logger.Info(Logger.CoreComponent.Cluster, null, "{0} unconfirmed frames are not resent: {1}", dropped, why);
        }
    }

    /// <summary>Latches the first fence; a façade raises its own, such as a diverged snapshot, through this
    /// too.</summary>
    internal void Fence(ClusterError reason, string detail)
    {
        if (_fenced)
        {
            return;
        }
        _fenced = true;
        _dispatch.OnFenced(reason, detail);
    }

    // Confirmed ingress takes the term first: nothing new may go out before the hold it may place is on.
    private void OnLeadershipChanged(int leaderMemberId, long leadershipTermId, long globalSeqNo)
    {
        _pending.OnLeadershipChanged(leadershipTermId);
        _leadershipTermId = leadershipTermId;
        _dispatch.OnLeadershipChanged();
    }

    private void OnSequenced(SequencedEvent sequencedEvent)
    {
        _pending.OnSequenced(sequencedEvent);
        if (sequencedEvent.IsSystem)
        {
            if (sequencedEvent.SystemEventType == SystemFrame.ClusterHeartbeat)
            {
                _tapStall.OnClusterHeartbeat(Clocks.MonotonicMs());
                _dispatch.OnClusterHeartbeat(sequencedEvent.ClusterTimestampNs, sequencedEvent.ReceiveTimeNs);
            }
            _dispatch.OnSystem(sequencedEvent);
            return;
        }
        _payload.Wrap(sequencedEvent);
        _dispatch.OnPayload(_payload);
    }

    // Records why the cluster ended this session; CheckFences is where it becomes a fence.
    private sealed class SessionListener : IEgressListener
    {
        private readonly Session _session;

        public SessionListener(Session session)
        {
            _session = session;
        }

        public void OnMessage(long clusterSessionId, long timestamp, IDirectBuffer buffer, int offset, int length,
                              Header header)
        {
            // Nothing is addressed to a producer on egress: everything it publishes comes back on the tap.
        }

        public void OnSessionEvent(long correlationId, long clusterSessionId, long leadershipTermId, int leaderMemberId,
                                   EventCode code, string detail)
        {
            if (code == EventCode.ERROR || code == EventCode.CLOSED)
            {
                _session._sessionFault = code + ": " + detail;
            }
        }

        public void OnNewLeader(long clusterSessionId, long leadershipTermId, int leaderMemberId,
                                string ingressEndpoints)
        {
        }

        public void OnAdminResponse(long clusterSessionId, long correlationId, AdminRequestType requestType,
                                    AdminResponseCode responseCode, string message, IDirectBuffer payload,
                                    int payloadOffset, int payloadLength)
        {
        }
    }
}
