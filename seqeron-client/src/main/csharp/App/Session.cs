using System;
using System.Collections.Generic;
using Adaptive.Aeron;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Adaptive.Cluster.Client;
using Adaptive.Cluster.Codecs;
using Org.Limitless.Seqeron.Protocol;
using Org.Limitless.Seqeron.Replayer.Client;
using Org.Limitless.Seqeron.Sequencer.Client;
using Org.Limitless.Seqeron.Util;
using Replay = Org.Limitless.Seqeron.Sbe.Replay;
using SbeBuffer = Org.SbeTool.Sbe.Dll.DirectBuffer;

namespace Org.Limitless.Seqeron.App;

/// <summary>
/// What every seqeron client does the same way: the cluster session it submits on, the co-located queue it follows,
/// confirmed ingress across a failover, and the fences that say when it may no longer act — wired together into one
/// <see cref="DoWork"/> whose ordering is not the caller's to get right.
/// <para>Not API. <see cref="Gateway"/> and <see cref="Application"/> are the façades over it.
/// <c>Session.java</c> and <c>app/detail/Session.hpp</c> are its twins; keep the three in step.</para>
/// </summary>
internal sealed class Session : IDisposable
{
    /// <summary>The deployment policy every producer had been copying: the queue may be silent for 20 heartbeat
    /// periods.</summary>
    internal const long DefaultQueueStallTimeoutMs = 20 * FrameLayer.ClusterHeartbeatIntervalMs;

    /// <summary>Longer than the queue's, since a re-walk is slower than the live stream it is catching up to.</summary>
    internal const long DefaultRecoveryStallTimeoutMs = 3 * DefaultQueueStallTimeoutMs;

    /// <summary>Frames in flight between a publish and the queue; far above what one round trip holds.</summary>
    internal const int DefaultPendingCapacity = 1024;

    /// <summary>How often an attempt to replace a lost session begins while the façade allows it.</summary>
    internal const long ReconnectIntervalMs = 1_000;

    // Untethered, as the receiver's is: a reply this instance does not read must not hold up the Replayer.
    private const string ControlChannel = ReplayProtocol.IpcChannel + "|tether=false";

    private const int ControlFragmentLimit = 16;

    private const int QueueRequestLength = Replay.MessageHeader.Size + Replay.QueueRequest.BlockLength;

    /// <summary>What the façade above does with what comes off the queue, and with a fence.</summary>
    internal interface IDispatch
    {
        /// <summary>A system frame this core does not consume itself. <c>LeadershipChanged</c> never arrives
        /// here.</summary>
        void OnSystem(SequencedEvent sequencedEvent);

        /// <summary>A <c>LeadershipChanged</c> was applied; confirmed ingress has already taken the new
        /// term.</summary>
        void OnLeadershipChanged();

        /// <summary>The Replayer cut a snapshot round on this node; a gateway has nothing to say to it.</summary>
        void OnSnapshotRound(long round);

        void OnPayload(Payload payload);

        /// <summary>Every transition to caught-up, the first included.</summary>
        void OnCaughtUp(long clusterSeqNo);

        /// <summary>The cluster clock's tick, once a second.</summary>
        void OnClusterHeartbeat(long clusterTimeNs, long receiveTimeNs);

        /// <summary>The group table, once, at the first catch-up: before anything may be published.</summary>
        void OnGroupsResolved(GroupDirectory directory);

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
    private readonly GroupResolution _groups;
    private readonly RecoveryStallFence _recoveryStall;
    private readonly QueueStallFence _queueStall;
    private readonly Payload _payload = new Payload();

    private readonly long _recoveryStallTimeoutMs;
    private readonly long _queueStallTimeoutMs;

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

    // What the QueueRequest names: the instance's topology row (spec R-6).
    private readonly int _queueSourceId;
    private readonly string _queueGatewayName;

    // The queue's resolution, until the receiver starts on it.
    private readonly QueueResolution _queueResolution;
    private readonly Replay.MessageHeader _queueRequestHeader = new Replay.MessageHeader();
    private readonly Replay.QueueRequest _queueRequest = new Replay.QueueRequest();
    private readonly UnsafeBuffer _queueRequestBuffer =
        new UnsafeBuffer(GC.AllocateArray<byte>(QueueRequestLength, true));
    private readonly SbeBuffer _queueRequestView = new SbeBuffer();
    private readonly FragmentHandler _queueControlHandler;
    private Aeron _aeron;
    private int _memberId;
    private long _queueClientId = -1;
    private Publication _queueRequestPublication;
    private Subscription _queueControlSubscription;

    /// <summary>A session that has not started.</summary>
    /// <param name="clientId">this instance's id on the node, its counters' <c>client</c> label</param>
    /// <param name="pendingCapacity">how many frames may be in flight between a publish and the queue</param>
    /// <param name="queueStallTimeoutMs">how long the queue may be silent once caught up</param>
    /// <param name="recoveryStallTimeoutMs">how long recovery may dispatch nothing once caught up before</param>
    /// <param name="sourceId">the application's <c>sourceId</c> its row names, or
    /// <see cref="ReplayProtocol.NoSourceId"/> for a gateway instance</param>
    /// <param name="gatewayName">the gateway instance's row name, or null for an application</param>
    /// <param name="addressedGroups">the names of the other groups the instance publishes to</param>
    /// <param name="dispatch">what the façade does with what comes off the queue</param>
    internal Session(int clientId, int pendingCapacity, long queueStallTimeoutMs, long recoveryStallTimeoutMs,
                     int sourceId, string gatewayName, IReadOnlyList<string> addressedGroups, IDispatch dispatch)
    {
        _dispatch = dispatch;
        _groups = new GroupResolution(addressedGroups);
        _queueSourceId = sourceId;
        _queueGatewayName = gatewayName ?? "";
        _queueResolution = new QueueResolution(new QueueResolutionActions(this));
        _queueControlHandler = (buffer, offset, length, header) => _queueResolution.OnControl(buffer, offset, length);
        SbeBuffers.Wrap(_queueRequestView, _queueRequestBuffer, 0, QueueRequestLength);
        _queueStallTimeoutMs = queueStallTimeoutMs;
        _recoveryStallTimeoutMs = recoveryStallTimeoutMs;
        _pending = new PendingSends(pendingCapacity);
        _publisher = new IngressPublisher(_pending);
        _recoveryStall = new RecoveryStallFence(recoveryStallTimeoutMs);
        _queueStall = new QueueStallFence(queueStallTimeoutMs);
        _receiver = new ReplayerStreamReceiver(clientId, OnSequenced, OnLeadershipChanged, null);
        _receiver.OnSnapshotRound((round, clusterSeqNo) => _dispatch.OnSnapshotRound(round));
        _sender.SetIngressHold(_pending);
    }

    /// <summary>Tells this node's Replayer the application <paramref name="sourceId"/> takes part in
    /// <paramref name="round"/>; whether the offer landed.</summary>
    internal bool SendSnapshotJoined(int sourceId, long round)
    {
        return _receiver.SendSnapshotJoined(sourceId, round);
    }

    /// <summary>Tells this node's Replayer the application's file for <paramref name="round"/> is durable; whether the
    /// offer landed.</summary>
    internal bool SendSnapshotConfirmed(int sourceId, long round)
    {
        return _receiver.SendSnapshotConfirmed(sourceId, round);
    }

    /// <summary>Restores the newest snapshot in <paramref name="store"/> first; call before starting.</summary>
    internal void RestoreFrom(SnapshotStore store, ISnapshotRestoreHandler handler)
    {
        _receiver.RestoreFrom(store, new GroupsRestore(_groups, handler));
    }

    /// <summary>Opens the cluster session over UDP and asks this node's Replayer which queue to follow.</summary>
    internal void Start(Aeron aeron, int memberId, string egressChannel, string ingressEndpoints)
    {
        _sender.Connect(aeron, egressChannel, ingressEndpoints, new SessionListener(this));
        ResolveQueue(aeron, memberId);
    }

    /// <summary>The same for a client sharing this member's media driver: ingress over its own <c>aeron:ipc</c>
    /// while that member leads, the UDP endpoint set when it does not.</summary>
    internal void StartColocated(Aeron aeron, int memberId, long ipcConnectTimeoutMs, string egressChannel,
                                 string ingressEndpoints)
    {
        _sender.ConnectColocated(aeron, memberId, ipcConnectTimeoutMs, egressChannel, ingressEndpoints,
                                 new SessionListener(this));
        ResolveQueue(aeron, memberId);
    }

    /// <summary>The receiver starts once the Replayer has said which queue this instance reads (spec R-6).</summary>
    private void ResolveQueue(Aeron aeron, int memberId)
    {
        _aeron = aeron;
        _memberId = memberId;
        _queueClientId = aeron.NextCorrelationId();
        _queueControlSubscription = aeron.AddSubscription(ControlChannel, ReplayProtocol.ControlStreamId);
        _queueRequestPublication = aeron.AddPublication(ReplayProtocol.IpcChannel, ReplayProtocol.RequestStreamId);
        _queueResolution.Start();
    }

    /// <summary>Waits for the Replayer's answer, unfenced, then starts the receiver on that queue.</summary>
    private int PollQueueResolution()
    {
        if (_queueControlSubscription == null)
        {
            return 0;
        }
        int work = _queueControlSubscription.Poll(_queueControlHandler, ControlFragmentLimit);
        _queueResolution.DoTimers(!_queueRequestPublication.IsConnected);
        int groupId = _queueResolution.GroupId;
        if (groupId != QueueResolution.Unresolved)
        {
            _receiver.Start(_aeron, _memberId, groupId);
            CloseQueueResolution();
            ++work;
        }
        return work;
    }

    private void CloseQueueResolution()
    {
        _queueControlSubscription?.Dispose();
        _queueControlSubscription = null;
        _queueRequestPublication?.Dispose();
        _queueRequestPublication = null;
    }

    /// <summary>One duty cycle, in the order the parts require: the queue first, so a fence is judged on what this
    /// cycle saw; the resend last, so nothing new goes out ahead of it.</summary>
    /// <returns>units of work done</returns>
    internal int DoWork()
    {
        if (_fenced)
        {
            return 0;
        }
        int work = _queueResolution.GroupId == QueueResolution.Unresolved ? PollQueueResolution() : _receiver.Poll();
        CheckCaughtUp();
        if (!_fenced)
        {
            CheckFences();
        }
        if (_fenced)
        {
            // Here rather than at the fence, which may be raised from inside that poll. Nothing polls the receiver
            // again, and a replay left open would hold one of the member's Replayer slots.
            _receiver.Dispose();
            CloseQueueResolution();
            return work;
        }
        work += _sender.PollEgress();
        _sender.KeepAlive();
        work += _pending.ResendMissing(_sender);
        return work;
    }

    /// <summary>Declined until the groups are resolved, since nothing may be published before (A-9).</summary>
    /// <param name="sourceId">the frame's <c>sourceId</c></param>
    /// <param name="connectionId">the frame's <c>connectionId</c></param>
    /// <param name="groupMask">the groups addressed besides the own one, 0 for none</param>
    /// <param name="payloadId">which protocol the payload speaks</param>
    /// <param name="body">the payload's bytes from offset 0</param>
    /// <param name="bodyLength">bytes of <paramref name="body"/></param>
    /// <returns><c>Refused</c> for a mask naming a group the table lacks</returns>
    internal Publish PublishPayload(int sourceId, int connectionId, ushort groupMask, int payloadId, IDirectBuffer body,
                                    int bodyLength)
    {
        int frameMask = _fenced ? GroupResolution.Unresolved : _groups.FrameMask(groupMask);
        if (frameMask == GroupResolution.Unresolved)
        {
            return Publish.Declined;
        }
        return frameMask == GroupResolution.NotAddressable
                   ? Publish.Refused
                   : _publisher.PublishPayload(_sender, sourceId, connectionId, payloadId, frameMask, body, bodyLength);
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

    internal bool IsFenced => _fenced;

    internal long LastClusterSeqNo => _receiver.LastClusterSeqNo;

    internal int CurrentLeaderMemberId => _receiver.CurrentLeaderMemberId;

    internal long LeadershipTermId => _leadershipTermId;

    /// <summary>The group table so far, for a snapshot header.</summary>
    internal IReadOnlyList<SnapshotHeader.Group> GroupTable => _groups.Table;

    /// <summary>Whether a cluster session is open. The recorded fault and <c>IsConnected</c> as well as the sender's
    /// latch: <c>AeronCluster</c> also closes itself, with no event at all, when a new leader does not arrive before
    /// its timeout. And the queue, whose <c>SessionClosed</c> is the cluster's own word.</summary>
    internal bool HasSession => _sessionFault == null && !_sender.IsSessionLost && _sender.IsConnected &&
                                !_pending.IsClosed(_sender.ClusterSessionId);

    public void Dispose()
    {
        _receiver.Dispose();
        CloseQueueResolution();
        _sender.Dispose();
    }

    // Takes a restore's group table before the façade's handler takes the header.
    private sealed class GroupsRestore : ISnapshotRestoreHandler
    {
        private readonly GroupResolution _groups;
        private readonly ISnapshotRestoreHandler _handler;

        public GroupsRestore(GroupResolution groups, ISnapshotRestoreHandler handler)
        {
            _groups = groups;
            _handler = handler;
        }

        public bool SupportsFormatVersion(long formatVersion)
        {
            return _handler.SupportsFormatVersion(formatVersion);
        }

        public void OnSnapshotHeader(SnapshotHeader header)
        {
            _groups.OnSnapshotHeader(header.Groups);
            _handler.OnSnapshotHeader(header);
        }

        public void OnSnapshotRecord(IDirectBuffer record, int length, int recordIndex, long formatVersion)
        {
            _handler.OnSnapshotRecord(record, length, recordIndex, formatVersion);
        }
    }

    // The Aeron half of QueueResolution.
    private sealed class QueueResolutionActions : QueueResolution.IActions
    {
        private readonly Session _session;

        public QueueResolutionActions(Session session)
        {
            _session = session;
        }

        public void SendQueueRequest(long requestId)
        {
            Publication publication = _session._queueRequestPublication;
            if (publication == null || !publication.IsConnected)
            {
                return; // Replayer not up yet; the resend timer retries
            }
            Replay.QueueRequest request = _session._queueRequest;
            request.WrapForEncodeAndApplyHeader(_session._queueRequestView, 0, _session._queueRequestHeader);
            request.ClientId = _session._queueClientId;
            request.RequestId = requestId;
            request.SourceId = _session._queueSourceId;
            request.SetGatewayName(_session._queueGatewayName);
            publication.Offer(_session._queueRequestBuffer, 0, Replay.MessageHeader.Size + request.Size);
        }

        public long ClientId => _session._queueClientId;

        public long NowMs()
        {
            return Clocks.MonotonicMs();
        }
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
        _queueStall.OnCaughtUp(nowMs);
        GroupResolution.Outcome resolution = _groups.OnCaughtUp(_queueResolution.GroupId);
        if (resolution == GroupResolution.Outcome.GroupUnknown)
        {
            Fence(ClusterError.GroupUnknown, "group " + _groups.UnknownGroup + " is not in the log's group table");
            return;
        }
        if (resolution == GroupResolution.Outcome.Resolved)
        {
            _dispatch.OnGroupsResolved(_groups.Directory);
        }
        _dispatch.OnCaughtUp(_receiver.LastClusterSeqNo);
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
            if (_recoveryStall.OnNotCaughtUp(nowMs, _receiver.LastClusterSeqNo))
            {
                Stalled(ClusterError.RecoveryStalled, Logger.CoreEventCode.RecoveryStalled,
                        "recovery has dispatched nothing for >" + _recoveryStallTimeoutMs +
                            "ms (clusterSeqNo stuck at " + _receiver.LastClusterSeqNo + ")");
                return;
            }
        }
        else if (_queueStall.IsStalled(nowMs))
        {
            Stalled(ClusterError.QueueStalled, Logger.CoreEventCode.QueueStalled,
                    "no ClusterHeartbeat for >" + _queueStallTimeoutMs + "ms");
            return;
        }
        if (_stallAlarm != null)
        {
            Logger.Info(Logger.CoreComponent.Cluster, null, "{0} cleared", _stallAlarm);
            _stallAlarm = null;
        }
    }

    // Fences an instance that acts; one that does not raises an alarm once and keeps following the queue.
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

    // Opens a session in place of the lost one, an attempt at most once a second and each advanced a step per cycle,
    // until one opens or the queue could have gone silent for as long: past that the cluster is not coming back for
    // this process, and the session's loss is a fence. An attempt under way then is let finish, since one whose
    // request an election swallowed spends its whole timeout.
    private void ReplaceSession(string why)
    {
        long nowMs = Clocks.MonotonicMs();
        if (_sessionLostSinceMs < 0)
        {
            _sessionLostSinceMs = nowMs;
            _nextReconnectMs = nowMs;
        }
        if (nowMs - _sessionLostSinceMs > _queueStallTimeoutMs && !_sender.IsReconnecting)
        {
            Fence(ClusterError.ClusterSessionLost,
                  why + "; no session replaced it within " + _queueStallTimeoutMs + "ms");
            return;
        }
        if (!_sender.IsReconnecting)
        {
            if (nowMs < _nextReconnectMs)
            {
                return;
            }
            _nextReconnectMs = nowMs + ReconnectIntervalMs;
        }
        if (!_sender.Reconnect())
        {
            return;
        }
        _sessionFault = null;
        _sessionLostSinceMs = -1;
        if (_pending.Count > 0)
        {
            Logger.Info(Logger.CoreComponent.Cluster, null,
                        "{0} unconfirmed frames hold new ones until the queue settles them: the session they went " +
                            "out on was lost ({1})",
                        _pending.Count, why);
        }
    }

    /// <summary>Forgets every frame not yet seen on the queue, for a façade that knows none of them will be
    /// sequenced.</summary>
    internal void DiscardUnconfirmed(string why)
    {
        int dropped = _pending.DiscardUnconfirmed();
        if (dropped > 0)
        {
            Logger.Info(Logger.CoreComponent.Cluster, null, "{0} unconfirmed frames are not resent: {1}", dropped, why);
        }
    }

    /// <summary>Latches the first fence and releases the cluster session, so a designated instance's sibling is
    /// promoted now rather than when the application exits; a façade raises its own, such as a diverged snapshot,
    /// through this too.</summary>
    internal void Fence(ClusterError reason, string detail)
    {
        if (_fenced)
        {
            return;
        }
        _fenced = true;
        _sender.Dispose();
        _dispatch.OnFenced(reason, detail);
    }

    // Confirmed ingress takes the term first: nothing new may go out before the hold it may place is on.
    private void OnLeadershipChanged(int leaderMemberId, long leadershipTermId, long clusterSeqNo)
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
            if (sequencedEvent.SystemEventType == SystemFrame.GroupsRegistered)
            {
                _groups.OnGroupsRegistered(sequencedEvent.Buffer, sequencedEvent.PayloadOffset,
                                           sequencedEvent.PayloadLength);
            }
            else if (sequencedEvent.SystemEventType == SystemFrame.ClusterHeartbeat)
            {
                _queueStall.OnClusterHeartbeat(Clocks.MonotonicMs());
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
            // Nothing is addressed to a producer on egress: everything it publishes comes back on the queue.
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
