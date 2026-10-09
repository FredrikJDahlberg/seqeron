using System;
using Adaptive.Aeron;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Org.Limitless.Seqeron.Protocol;
using Org.Limitless.Seqeron.Replayer.Client;
using Org.Limitless.Seqeron.Util;
using Frame = Org.Limitless.Seqeron.Sbe.Frame;
using SbeBuffer = Org.SbeTool.Sbe.Dll.DirectBuffer;

namespace Org.Limitless.Seqeron.App;

/// <summary>
/// One replica of a co-located application — the kind of producer nothing elects. One runs per node, the topology's
/// <c>&lt;applications&gt;</c> section names it, and <c>LeadershipChanged</c> already picks the replica that
/// submits: this one publishes only while its own node leads. What that takes is behind it — the cluster session
/// over the node's own <c>aeron:ipc</c>, the tap it follows, the leader gate, confirmed ingress across a failover,
/// and the fences that say it may no longer act.
/// <para><b>Off the cluster</b> (<see cref="ApplicationOptions.OffCluster"/>), on a gateway host, it is the one
/// instance rather than one of three: its gate opens once caught up, whoever leads, and it submits over UDP. Nothing
/// stops a second instance publishing too; an application that needs a standby off the cluster is a
/// <see cref="Gateway"/>.</para>
/// <para><b>What is left to the consumer is its own work</b>: the payloads it reads, the state it keeps, the
/// payloads it submits. Every replica reads the same ordered stream and so holds the same state, which is what makes
/// <see cref="OutstandingWork{TKey,TWork}"/> — fed that same stream — the way work survives the gate closing under
/// it.</para>
/// <para><b>Snapshots</b> (doc/snapshot.md §4): given an <see cref="ISnapshotListener"/> and a topology row with
/// <c>snapshot="true"</c>, every replica serializes its state at each round's cut into its own directory, and the
/// one whose gate is open then submits the round's <c>SnapshotEnd</c>. A replica whose snapshot differs from the one
/// sequenced is fenced with <see cref="ClusterError.SnapshotDiverged"/>. On start, a replica given a listener
/// restores the newest snapshot of its own that the log confirms and resumes after its cut, or replays from
/// <c>globalSeqNo</c> 1 without one; one it cannot restore is fenced with
/// <see cref="ClusterError.SnapshotUnrestorable"/>.</para>
/// <para>Single-threaded: every member belongs to the caller's one duty-cycle thread, which calls
/// <see cref="DoWork"/> each iteration. <c>Application.java</c> and <c>app/Application.hpp</c> are its twins; keep
/// the three in step.</para>
/// </summary>
public sealed class Application : IDisposable
{
    /// <summary>How long the tap may be silent before this replica is fenced — 20 heartbeat periods.</summary>
    public const long DefaultTapStallTimeoutMs = Session.DefaultTapStallTimeoutMs;

    /// <summary>How long recovery may dispatch nothing, once caught up before; longer, as a re-walk is
    /// slower.</summary>
    public const long DefaultRecoveryStallTimeoutMs = Session.DefaultRecoveryStallTimeoutMs;

    /// <summary>Frames in flight between a publish and the tap; far above what one round trip holds.</summary>
    public const int DefaultPendingCapacity = Session.DefaultPendingCapacity;

    /// <summary>How long ingress is tried on this member's own <c>aeron:ipc</c>: short, as a follower never
    /// answers.</summary>
    public const long DefaultIpcConnectTimeoutMs = 500;

    // A payload of this application's own belongs to no connection.
    private const int NoConnection = -1;

    private readonly Session _session;
    private readonly LeaderGate _gate;
    private readonly IApplicationListener _listener;
    private readonly SnapshotTaker _snapshots;
    private readonly SnapshotFrames _snapshotFrames;
    private readonly int _sourceId;
    private readonly int _memberId;
    private readonly bool _offCluster;
    private readonly long _ipcConnectTimeoutMs;
    private readonly string _egressChannel;
    private readonly string _ingressEndpoints;
    private readonly byte[] _staging = GC.AllocateArray<byte>(FrameLayer.MaxPayloadLength, true);
    private readonly UnsafeBuffer _stagingBuffer;

    /// <summary>A replica configured by <paramref name="options"/>; nothing is connected until
    /// <see cref="Start"/>.</summary>
    /// <exception cref="ArgumentException">if a required option is missing</exception>
    public Application(ApplicationOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        Require(options.EgressChannel, nameof(options.EgressChannel));
        Require(options.IngressEndpoints, nameof(options.IngressEndpoints));
        Require(options.Listener, nameof(options.Listener));
        if (options.SnapshotListener != null && options.SnapshotDirectory == null)
        {
            throw new ArgumentException("SnapshotDirectory is required with a SnapshotListener", nameof(options));
        }
        _listener = options.Listener;
        _sourceId = options.SourceId;
        _memberId = options.MemberId;
        _offCluster = options.OffCluster;
        _ipcConnectTimeoutMs = options.IpcConnectTimeoutMs;
        _egressChannel = options.EgressChannel;
        _ingressEndpoints = options.IngressEndpoints;
        _stagingBuffer = new UnsafeBuffer(_staging);
        _gate = new LeaderGate(options.MemberId, options.OffCluster);
        SnapshotStore store = options.SnapshotListener == null ? null : new SnapshotStore(options.SnapshotDirectory);
        _snapshots = new SnapshotTaker(options.SnapshotListener, store, false, KeepAlive);
        _session = new Session(options.ClientId, options.PendingCapacity, options.TapStallTimeoutMs,
                               options.RecoveryStallTimeoutMs, new SessionDispatch(this));
        _snapshotFrames = new SnapshotFrames(_session, () => _sourceId);
        if (store != null)
        {
            _session.RestoreFrom(_sourceId, store, _snapshots);
        }
    }

    /// <summary>Opens the cluster session on this node and starts following its tap; the gate stays shut until
    /// caught up.</summary>
    public void Start(Aeron aeron)
    {
        if (_offCluster)
        {
            _session.Start(aeron, _memberId, _egressChannel, _ingressEndpoints);
        }
        else
        {
            _session.StartColocated(aeron, _memberId, _ipcConnectTimeoutMs, _egressChannel, _ingressEndpoints);
        }
    }

    /// <summary>One duty-cycle iteration: the cluster session and the tap, then the gate over what they
    /// left.</summary>
    /// <returns>units of work done, for the caller's idle strategy</returns>
    public int DoWork()
    {
        int work = _session.DoWork();
        // Shut while a lost session is replaced: a reply sent meanwhile may not land, so the next opening redispatches.
        LeaderGate.Transition transition =
            _gate.Update(_session.IsCaughtUp && _session.HasSession, _session.CurrentLeaderMemberId);
        if (transition != LeaderGate.Transition.None)
        {
            _listener.OnLeadershipChanged(transition == LeaderGate.Transition.Opened);
            work++;
        }
        if (_gate.IsOpen)
        {
            work += _snapshots.Submit(_snapshotFrames);
        }
        return work;
    }

    /// <summary>Whether leader-only work may reach ingress right now: the gate is open, and ingress is not
    /// held.</summary>
    public bool CanPublish => _gate.IsOpen && !_session.IsHolding;

    /// <summary>Whether this replica may do leader-only work at all; <see cref="CanPublish"/> is what a publish
    /// needs.</summary>
    public bool IsLeading => _gate.IsOpen;

    /// <summary>Submits one payload of this application's own, stamped with its <c>sourceId</c> and belonging to no
    /// connection.</summary>
    /// <param name="payloadId">which protocol the payload speaks</param>
    /// <param name="payload">the payload's bytes from offset 0, its own <c>MessageHeader</c> included</param>
    /// <param name="length">bytes of <paramref name="payload"/></param>
    /// <returns><c>Declined</c> while the gate is shut, ingress is held or the transport is back-pressured — retry
    /// it; <c>Refused</c> is permanent</returns>
    public Publish Publish(int payloadId, IDirectBuffer payload, int length)
    {
        return Submit(_sourceId, NoConnection, payloadId, payload, length);
    }

    /// <summary>The same from a span, copied once into a buffer this replica owns.</summary>
    public Publish Publish(int payloadId, ReadOnlySpan<byte> payload)
    {
        return Submit(_sourceId, NoConnection, payloadId, Stage(payload), payload.Length);
    }

    /// <summary>
    /// The same on behalf of the producer that asked for it: a reply carries the <b>requester's</b> <c>sourceId</c>
    /// and <c>connectionId</c>, which is how the gateway that took the request routes the answer back out of it. Keep
    /// those two off the request rather than the request itself — a <see cref="Payload"/> is valid only during its
    /// callback, and a reply is usually dispatched later.
    /// </summary>
    public Publish Reply(int requesterSourceId, int connectionId, int payloadId, IDirectBuffer payload, int length)
    {
        return Submit(requesterSourceId, connectionId, payloadId, payload, length);
    }

    /// <summary>The same from a span, copied once into a buffer this replica owns.</summary>
    public Publish Reply(int requesterSourceId, int connectionId, int payloadId, ReadOnlySpan<byte> payload)
    {
        return Submit(requesterSourceId, connectionId, payloadId, Stage(payload), payload.Length);
    }

    /// <summary>This replica's own <c>sourceId</c>, the one its topology row gives it.</summary>
    public int SourceId => _sourceId;

    public bool IsCaughtUp => _session.IsCaughtUp;

    public long LastGlobalSeqNo => _session.LastGlobalSeqNo;

    /// <summary>Closes the cluster session and the tap. The Aeron client is the caller's and is left open.</summary>
    public void Dispose()
    {
        _session.Dispose();
    }

    internal static void Require(object value, string name)
    {
        if (value == null)
        {
            throw new ArgumentException(name + " is required", name);
        }
    }

    // A payload too long to stage is left out: ingress refuses it on its length before reading a byte.
    private UnsafeBuffer Stage(ReadOnlySpan<byte> payload)
    {
        if (payload.Length <= _staging.Length)
        {
            payload.CopyTo(_staging);
        }
        return _stagingBuffer;
    }

    // The session's keep-alive, for a round serialized inside a dispatch.
    private void KeepAlive()
    {
        _session.KeepAlive();
    }

    // A shut gate declines rather than submits: only the leading replica's copy of the work is the one sent.
    private Publish Submit(int frameSourceId, int connectionId, int payloadId, IDirectBuffer payload, int length)
    {
        if (!_gate.IsOpen)
        {
            return Protocol.Publish.Declined;
        }
        return _session.PublishPayload(frameSourceId, connectionId, payloadId, payload, length);
    }

    // What comes off the tap, and the one frame the gate is driven by.
    private sealed class SessionDispatch : Session.IDispatch
    {
        private readonly Application _app;
        private readonly SbeBuffer _view = new SbeBuffer();
        private readonly Frame.ApplicationRegistered _row = new Frame.ApplicationRegistered();
        private readonly Frame.SnapshotStarted _started = new Frame.SnapshotStarted();
        private readonly Frame.SnapshotEnd _end = new Frame.SnapshotEnd();

        public SessionDispatch(Application app)
        {
            _app = app;
        }

        // Of seqeron's own vocabulary, this replica reads its topology row and the snapshot rounds; the leadership
        // the gate turns on arrives below rather than here. A submitted payload carries no MessageHeader, so each
        // decode takes this build's constants (spec §7, V-3).
        public void OnSystem(SequencedEvent sequencedEvent)
        {
            switch (sequencedEvent.SystemEventType)
            {
                case SystemFrame.ApplicationRegistered:
                    Wrap(sequencedEvent);
                    _row.WrapForDecode(_view, 0, Frame.ApplicationRegistered.BlockLength,
                                       Frame.MessageHeader.SbeSchemaVersion);
                    if (_row.ApplicationSourceId == _app._sourceId)
                    {
                        _app._snapshots.SetParticipating(_row.Snapshot == 1);
                    }
                    break;
                case SystemFrame.SnapshotStarted:
                    Wrap(sequencedEvent);
                    _started.WrapForDecode(_view, 0, Frame.SnapshotStarted.BlockLength,
                                           Frame.SnapshotStarted.SchemaVersion);
                    _app._snapshots.OnSnapshotStarted(
                        _started.Round,
                        new SnapshotHeader(_app._session.LeadershipTermId, _app._session.CurrentLeaderMemberId, null),
                        _app._gate.IsOpen);
                    break;
                case SystemFrame.SnapshotEnd:
                    if (sequencedEvent.SourceId != _app._sourceId)
                    {
                        return;
                    }
                    Wrap(sequencedEvent);
                    _end.WrapForDecode(_view, 0, Frame.SnapshotEnd.BlockLength, Frame.MessageHeader.SbeSchemaVersion);
                    if (!_app._snapshots.OnSnapshotEnd(_end.Round, _end.RecordCount, _end.Length, _end.Crc32c))
                    {
                        _app._session.Fence(ClusterError.SnapshotDiverged,
                                            "round " + _end.Round + "'s sequenced snapshot at globalSeqNo " +
                                                sequencedEvent.GlobalSeqNo + " differs from this replica's");
                    }
                    break;
                default:
                    // Nothing else says anything to a producer nothing elects.
                    break;
            }
        }

        // A term won by another member ends this replica's part in the round it is publishing.
        public void OnLeadershipChanged()
        {
            _app._gate.OnLeadershipChanged();
            if (!_app._offCluster && _app._session.CurrentLeaderMemberId != _app._memberId)
            {
                _app._snapshots.StopPublishing();
            }
        }

        public void OnPayload(Payload payload)
        {
            _app._listener.OnSequenced(payload);
        }

        public void OnCaughtUp(long globalSeqNo)
        {
            _app._listener.OnCaughtUp(globalSeqNo);
        }

        public void OnClusterHeartbeat(long clusterTimeNs, long receiveTimeNs)
        {
            _app._listener.OnClusterHeartbeat(clusterTimeNs, receiveTimeNs);
        }

        public void OnFenced(ClusterError fence, string detail)
        {
            _app._listener.OnFenced(fence, detail);
        }

        // Nothing elects a replica, and its work stays outstanding on the log until replied to.
        public bool MayReconnect()
        {
            return true;
        }

        // The leader's replica, caught up or not: no other holds the leader-only work.
        public bool IsActing()
        {
            return _app._offCluster || _app._session.CurrentLeaderMemberId == _app._memberId;
        }

        private void Wrap(SequencedEvent sequencedEvent)
        {
            SbeBuffers.Wrap(_view, sequencedEvent.Buffer, sequencedEvent.PayloadOffset, sequencedEvent.PayloadLength);
        }
    }
}
