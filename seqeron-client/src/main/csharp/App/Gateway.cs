using System;
using System.Collections.Generic;
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
/// One instance of an elected active/standby producer pair. Everything this tier defines about being a gateway is
/// behind it: the list row that names the instance, the designation that makes it serve, the <c>GatewayStarted</c>
/// that binds its session, the connection id space it resumes from its predecessor, the connection lifecycle frames,
/// confirmed ingress across a failover, and the fences that stand it down.
/// <para><b>What is left to the consumer is its edge</b> — a socket, a dialler, a codec. It opens that edge in
/// <see cref="IGatewayListener.OnActivated"/>, closes it in <see cref="IGatewayListener.OnStandby"/>, and otherwise
/// exchanges payloads: nothing of the frame layer or of seqeron's system vocabulary appears in its code.</para>
/// <para><b>Snapshots</b> (doc/snapshot.md §4): given an <see cref="ISnapshotListener"/> and topology rows with
/// <c>snapshot="true"</c>, every instance serializes its state at each round's cut into its own directory, under a
/// header carrying the pair's election state, and the active one then submits the round's <c>SnapshotEnd</c>. An
/// instance whose snapshot differs from the one sequenced is fenced with <see cref="ClusterError.SnapshotDiverged"/>.
/// On start, an instance given a listener restores the newest snapshot of its own that the log confirms and resumes
/// after its cut, or replays from <c>globalSeqNo</c> 1 without one; one it cannot restore is fenced with
/// <see cref="ClusterError.SnapshotUnrestorable"/>. A <see cref="GatewayOptions.Passive"/> instance holds no state
/// until it is activated: it follows the tap for the election alone, then restores and catches up before it
/// serves.</para>
/// <para>Single-threaded: every member belongs to the caller's one duty-cycle thread, which calls
/// <see cref="DoWork"/> each iteration. <c>Gateway.java</c> and <c>app/Gateway.hpp</c> are its twins; keep the three
/// in step.</para>
/// </summary>
public sealed class Gateway : IDisposable
{
    /// <summary>How long the tap may be silent before this instance is fenced — 20 heartbeat periods. The deployment
    /// policy every gateway had been copying.</summary>
    public const long DefaultTapStallTimeoutMs = Session.DefaultTapStallTimeoutMs;

    /// <summary>How long recovery may dispatch nothing, once caught up before; longer than the tap's, as a re-walk is
    /// slower.</summary>
    public const long DefaultRecoveryStallTimeoutMs = Session.DefaultRecoveryStallTimeoutMs;

    /// <summary>Frames in flight between a publish and the tap; far above what one round trip holds.</summary>
    public const int DefaultPendingCapacity = Session.DefaultPendingCapacity;

    /// <summary>A frame that belongs to the gateway rather than to one of its connections, and what
    /// <see cref="OpenConnection()"/> answers when there is none to give.</summary>
    public const int NoConnection = -1;

    /// <summary>What <see cref="SourceId"/> and <see cref="GatewayId"/> read until a <c>GatewayRegistered</c> row
    /// names this instance. Nothing may be published under an unresolved identity, so a consumer that stamps its own
    /// records with <see cref="SourceId"/> checks for this first.</summary>
    public const int Unresolved = GatewayLifecycle.Unresolved;

    private static readonly byte[] NoConnectionData = Array.Empty<byte>();

    private readonly Session _session;
    private readonly GatewayLifecycle _lifecycle;
    private readonly IGatewayListener _listener;
    private readonly SnapshotTaker _snapshots;
    private readonly SnapshotFrames _snapshotFrames;
    private readonly int _memberId;
    private readonly string _egressChannel;
    private readonly string _ingressEndpoints;

    // Room for the longest connectionData SBE can carry, so a frame too long for ingress is refused rather than
    // overrun here.
    private readonly UnsafeBuffer _body = new UnsafeBuffer(
        GC.AllocateArray<byte>(ushort.MaxValue + Frame.ConnectionOpened.ConnectionDataHeaderSize, true));
    private readonly SbeBuffer _bodyView = new SbeBuffer();
    private readonly Frame.GatewayStarted _gatewayStarted = new Frame.GatewayStarted();
    private readonly Frame.ConnectionOpened _connectionOpened = new Frame.ConnectionOpened();
    private readonly Frame.ConnectionClosed _connectionClosed = new Frame.ConnectionClosed();
    private readonly byte[] _staging = GC.AllocateArray<byte>(FrameLayer.MaxPayloadLength, true);
    private readonly UnsafeBuffer _stagingBuffer;

    // Connection lifecycle frames still to be placed, in the order they were asked for.
    private readonly List<Lifecycle> _lifecycleQueue = new List<Lifecycle>();

    // Connections whose ConnectionOpened has not landed: nothing may be published on one yet.
    private readonly HashSet<int> _unopened = new HashSet<int>();

    // The highest connectionId this logical gateway's history holds; the resume point for §7's row.
    private int _highestConnectionId = NoConnection;
    private int _nextConnectionId;

    /// <summary>An instance configured by <paramref name="options"/>; nothing is connected until
    /// <see cref="Start"/>.</summary>
    /// <exception cref="ArgumentException">if a required option is missing</exception>
    public Gateway(GatewayOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        Application.Require(options.GatewayName, nameof(options.GatewayName));
        Application.Require(options.EgressChannel, nameof(options.EgressChannel));
        Application.Require(options.IngressEndpoints, nameof(options.IngressEndpoints));
        Application.Require(options.Listener, nameof(options.Listener));
        if (options.SnapshotListener != null && options.SourceId == Unresolved)
        {
            throw new ArgumentException("SourceId is required with a SnapshotListener", nameof(options));
        }
        if (options.SnapshotListener != null && options.SnapshotDirectory == null)
        {
            throw new ArgumentException("SnapshotDirectory is required with a SnapshotListener", nameof(options));
        }
        _listener = options.Listener;
        _memberId = options.MemberId;
        _egressChannel = options.EgressChannel;
        _ingressEndpoints = options.IngressEndpoints;
        _stagingBuffer = new UnsafeBuffer(_staging);
        SbeBuffers.Wrap(_bodyView, _body, 0, _body.Capacity);
        _lifecycle = new GatewayLifecycle(options.GatewayName, new LifecycleActions(this));
        SnapshotStore store = options.SnapshotListener == null ? null : new SnapshotStore(options.SnapshotDirectory);
        _snapshots = new SnapshotTaker(options.SnapshotListener, store, options.Passive, KeepAlive);
        _session = new Session(options.ClientId, options.PendingCapacity, options.TapStallTimeoutMs,
                               options.RecoveryStallTimeoutMs, new SessionDispatch(this));
        _snapshotFrames = new SnapshotFrames(_session, () => _lifecycle.GatewaySourceId);
        if (store != null)
        {
            _session.RestoreFrom(options.SourceId, store, new Restore(this));
        }
    }

    /// <summary>Opens the cluster session and starts following this node's tap; the gate stays shut until
    /// designated.</summary>
    public void Start(Aeron aeron)
    {
        _session.Start(aeron, _memberId, _egressChannel, _ingressEndpoints);
    }

    /// <summary>One duty-cycle iteration: the cluster session and the tap, then whatever the connection lifecycle,
    /// the election and a snapshot round still owe. A passive instance that has been activated starts over
    /// here.</summary>
    /// <returns>units of work done, for the caller's idle strategy</returns>
    public int DoWork()
    {
        int work = _session.DoWork();
        if (_snapshots.Activate(_lifecycle.IsActivated, _session.IsCaughtUp))
        {
            _session.Restart();
            return work + 1;
        }
        work += DrainLifecycle();
        work += _session.IsCaughtUp ? _lifecycle.Advance() : 0;
        work += _snapshots.Submit(_snapshotFrames);
        return work;
    }

    /// <summary>
    /// The edge closed without being asked to — a dial that failed, a counterparty that hung up. The designation
    /// stands, so <see cref="DoWork"/> opens it again through <see cref="IGatewayListener.OnActivated"/>, and without
    /// a second <c>GatewayStarted</c>: this instance is still the one the cluster designated.
    /// <para>An acceptor whose listen socket stays bound never calls this. An initiator does: its edge is one dial,
    /// and a dial that fails or a session that drops is the gate closing under it.</para>
    /// </summary>
    public void GateClosed()
    {
        _lifecycle.OnGateClosed();
    }

    /// <summary>
    /// Tells the cluster this instance is alive. <see cref="DoWork"/> already does it once a cycle; call this as well
    /// from inside an <see cref="IGatewayListener"/> callback that spins — writing to an edge that is back-pressured,
    /// say — since <c>DoWork</c> cannot run again until that callback returns, and the cluster drops a session that
    /// goes quiet for <c>sequencer.sessionTimeoutMs</c>. Self-throttling, so a call per spin costs nothing.
    /// </summary>
    public void KeepAlive()
    {
        _session.KeepAlive();
    }

    /// <summary>Whether a connection may be taken right now: this instance is serving, and ingress is not held
    /// behind a failover's resend. Check it before accepting or dialling — a connection taken while ingress is held
    /// could not have its <c>ConnectionOpened</c> placed.</summary>
    public bool CanAccept => _lifecycle.IsServing && !_session.IsHolding;

    /// <summary>Takes one connection into the cluster's view of this gateway: allocates its id from the resume point
    /// and queues its <c>ConnectionOpened</c>, which <see cref="DoWork"/> places and retries.</summary>
    /// <param name="connectionData">opaque bytes the frame carries, from offset 0</param>
    /// <param name="length">bytes of <paramref name="connectionData"/></param>
    /// <returns>the connection's id, or <see cref="NoConnection"/> if this instance is not serving</returns>
    public int OpenConnection(IDirectBuffer connectionData, int length)
    {
        var data = new byte[length];
        connectionData.GetBytes(0, data, 0, length);
        return Open(data);
    }

    /// <summary>The same from a span.</summary>
    public int OpenConnection(ReadOnlySpan<byte> connectionData)
    {
        return Open(connectionData.ToArray());
    }

    /// <summary>The same for a connection whose only identity is its id.</summary>
    public int OpenConnection()
    {
        return Open(NoConnectionData);
    }

    /// <summary>The same for a connection that has gone. One the cluster never heard of is dropped rather than
    /// announced.</summary>
    public void CloseConnection(int connectionId)
    {
        if (!_lifecycle.IsServing)
        {
            // Stood down: the connections went with the edge, and a successor's GatewayStarted releases them.
            return;
        }
        if (_unopened.Remove(connectionId))
        {
            _lifecycleQueue.RemoveAll(queued => queued.ConnectionId == connectionId);
            return;
        }
        _lifecycleQueue.Add(new Lifecycle(connectionId, SystemFrame.ConnectionClosed, NoConnectionData));
    }

    /// <summary>Submits one application payload on a connection, stamped with this gateway's
    /// <c>sourceId</c>.</summary>
    /// <param name="connectionId">the connection it belongs to</param>
    /// <param name="payloadId">which protocol the payload speaks</param>
    /// <param name="payload">the payload's bytes from offset 0, its own <c>MessageHeader</c> included</param>
    /// <param name="length">bytes of <paramref name="payload"/></param>
    /// <returns><c>Declined</c> while ingress is held, back-pressured, or the connection's <c>ConnectionOpened</c>
    /// has not landed yet — retry it; <c>Refused</c> is permanent</returns>
    public Publish Publish(int connectionId, int payloadId, IDirectBuffer payload, int length)
    {
        if (_unopened.Contains(connectionId))
        {
            return Protocol.Publish.Declined;
        }
        return _session.PublishPayload(_lifecycle.GatewaySourceId, connectionId, payloadId, payload, length);
    }

    /// <summary>The same from a span, copied once into a buffer this instance owns.</summary>
    public Publish Publish(int connectionId, int payloadId, ReadOnlySpan<byte> payload)
    {
        // A payload too long to stage is left out: ingress refuses it on its length before reading a byte.
        if (payload.Length <= _staging.Length)
        {
            payload.CopyTo(_staging);
        }
        return Publish(connectionId, payloadId, _stagingBuffer, payload.Length);
    }

    /// <summary>Whether the last <c>GatewayActive</c> for this pair named this instance.</summary>
    public bool IsActivated => _lifecycle.IsActivated;

    /// <summary>Whether the edge is open — <see cref="IGatewayListener.OnActivated"/> has returned true and nothing
    /// has stood it down.</summary>
    public bool IsServing => _lifecycle.IsServing;

    /// <summary>Whether this instance still holds no state: passive and not yet activated.</summary>
    public bool IsPassive => !_snapshots.HoldsState;

    /// <summary>This logical gateway's <c>sourceId</c>, shared with its standby; <see cref="Unresolved"/> until a
    /// <c>GatewayRegistered</c> row names it.</summary>
    public int SourceId => _lifecycle.GatewaySourceId;

    /// <summary>This instance's list row, or <see cref="Unresolved"/> until a row names it.</summary>
    public int GatewayId => _lifecycle.GatewayId;

    public bool IsCaughtUp => _session.IsCaughtUp;

    public long LastGlobalSeqNo => _session.LastGlobalSeqNo;

    /// <summary>Closes the cluster session and the tap. The Aeron client is the caller's and is left open.</summary>
    public void Dispose()
    {
        _session.Dispose();
    }

    private int Open(byte[] connectionData)
    {
        if (!_lifecycle.OnSessionAcquired())
        {
            return NoConnection;
        }
        int connectionId = _nextConnectionId++;
        _lifecycleQueue.Add(new Lifecycle(connectionId, SystemFrame.ConnectionOpened, connectionData));
        _unopened.Add(connectionId);
        return connectionId;
    }

    // Places what the connection lifecycle owes, oldest first, stopping at the first frame that is declined.
    private int DrainLifecycle()
    {
        int work = 0;
        while (_lifecycleQueue.Count > 0)
        {
            Lifecycle next = _lifecycleQueue[0];
            if (!Published(_session.PublishSystem(_lifecycle.GatewaySourceId, next.ConnectionId, next.SystemEventType,
                                                  _body, Encode(next))))
            {
                break;
            }
            _lifecycleQueue.RemoveAt(0);
            _unopened.Remove(next.ConnectionId);
            work++;
        }
        return work;
    }

    private int Encode(Lifecycle queued)
    {
        if (queued.SystemEventType == SystemFrame.ConnectionOpened)
        {
            _connectionOpened.WrapForEncode(_bodyView, 0);
            _connectionOpened.SetConnectionData(queued.ConnectionData);
            return _connectionOpened.Size;
        }
        _connectionClosed.WrapForEncode(_bodyView, 0);
        return _connectionClosed.Size;
    }

    // The resume point, read off every frame this logical gateway's history holds, whichever instance issued it.
    private void ObserveConnectionId(int sourceId, int connectionId)
    {
        if (_lifecycle.GatewaySourceId != GatewayLifecycle.Unresolved && sourceId == _lifecycle.GatewaySourceId &&
            connectionId > _highestConnectionId)
        {
            _highestConnectionId = connectionId;
        }
    }

    // A refused frame is this class's own bug, never a condition to wait out.
    private static bool Published(Publish result)
    {
        if (result == Protocol.Publish.Refused)
        {
            throw new InvalidOperationException(
                "a frame the sequencer would reject (doc/seqeron-protocol-spec.md §9.2)");
        }
        return result == Protocol.Publish.Published;
    }

    // What this instance derives from the frames before a cut, for the round it starts (§6).
    private SnapshotHeader CurrentSnapshotHeader()
    {
        return new SnapshotHeader(_session.LeadershipTermId, _session.CurrentLeaderMemberId,
                                  new SnapshotHeader.GatewayState(_lifecycle.GatewaySourceId,
                                                                  _lifecycle.ActiveGatewayId, _highestConnectionId,
                                                                  _lifecycle.PairRows()));
    }

    // One connection lifecycle frame waiting to be placed.
    private sealed record Lifecycle(int ConnectionId, int SystemEventType, byte[] ConnectionData);

    // The election's side effects.
    private sealed class LifecycleActions : GatewayLifecycle.IActions
    {
        private readonly Gateway _gateway;

        public LifecycleActions(Gateway gateway)
        {
            _gateway = gateway;
        }

        public bool PublishGatewayStarted(int gatewayId)
        {
            Gateway g = _gateway;
            g._nextConnectionId = g._highestConnectionId + 1;
            g._gatewayStarted.WrapForEncode(g._bodyView, 0);
            g._gatewayStarted.GatewayId = gatewayId;
            g._gatewayStarted.FirstConnectionId = g._nextConnectionId;
            return Published(g._session.PublishSystem(g._lifecycle.GatewaySourceId, NoConnection,
                                                      SystemFrame.GatewayStarted, g._body, g._gatewayStarted.Size));
        }

        public bool OpenGate()
        {
            return _gateway._listener.OnActivated(_gateway._nextConnectionId);
        }

        public void CloseGate()
        {
            // The connections go with the edge, and a successor's GatewayStarted is what releases them.
            _gateway._lifecycleQueue.Clear();
            _gateway._unopened.Clear();
            _gateway._listener.OnStandby();
        }
    }

    // What a restore hands over in place of the frames before the cut: the header's election state and connection id
    // resume point, then, unless passive, the application's records.
    private sealed class Restore : ISnapshotRestoreHandler
    {
        private readonly Gateway _gateway;

        public Restore(Gateway gateway)
        {
            _gateway = gateway;
        }

        public bool SupportsFormatVersion(long formatVersion)
        {
            return _gateway._snapshots.SupportsFormatVersion(formatVersion);
        }

        public void OnSnapshotHeader(SnapshotHeader header)
        {
            if (header.Gateway != null)
            {
                _gateway._lifecycle.OnSnapshotHeader(header.Gateway);
                _gateway._highestConnectionId = header.Gateway.HighestConnectionId;
            }
            _gateway._snapshots.OnSnapshotHeader(header);
        }

        public void OnSnapshotRecord(IDirectBuffer record, int length, int recordIndex)
        {
            _gateway._snapshots.OnSnapshotRecord(record, length, recordIndex);
        }
    }

    // What comes off the tap, split into what the election reads and what the consumer does.
    private sealed class SessionDispatch : Session.IDispatch
    {
        private readonly Gateway _gateway;
        private readonly SbeBuffer _view = new SbeBuffer();
        private readonly Frame.GatewayRegistered _gatewayRow = new Frame.GatewayRegistered();
        private readonly Frame.GatewayActive _gatewayActive = new Frame.GatewayActive();
        private readonly Frame.ConnectionOpened _connectionOpenedIn = new Frame.ConnectionOpened();
        private readonly Frame.SnapshotStarted _snapshotStarted = new Frame.SnapshotStarted();
        private readonly Frame.SnapshotEnd _snapshotEnd = new Frame.SnapshotEnd();

        public SessionDispatch(Gateway gateway)
        {
            _gateway = gateway;
        }

        // A submitted system payload carries no MessageHeader, so each decode takes this build's own block length and
        // version (doc/seqeron-protocol-spec.md §7, V-3). A passive instance reads the election alone.
        public void OnSystem(SequencedEvent sequencedEvent)
        {
            Gateway g = _gateway;
            g.ObserveConnectionId(sequencedEvent.SourceId, sequencedEvent.ConnectionId);
            switch (sequencedEvent.SystemEventType)
            {
                case SystemFrame.GatewayRegistered:
                    Wrap(sequencedEvent);
                    _gatewayRow.WrapForDecode(_view, 0, Frame.GatewayRegistered.BlockLength,
                                              Frame.MessageHeader.SbeSchemaVersion);
                    g._lifecycle.OnGatewayRegistered(_gatewayRow.GatewayId, _gatewayRow.GatewaySourceId,
                                                     _gatewayRow.GetGatewayName(), _gatewayRow.PreferenceRank);
                    if (_gatewayRow.GatewayId == g._lifecycle.GatewayId)
                    {
                        g._snapshots.SetParticipating(_gatewayRow.Snapshot == 1);
                    }
                    break;
                case SystemFrame.ConnectionOpened:
                    if (g._snapshots.HoldsState && sequencedEvent.SourceId == g._lifecycle.GatewaySourceId)
                    {
                        DispatchConnectionOpened(sequencedEvent);
                    }
                    break;
                case SystemFrame.ConnectionClosed:
                    if (g._snapshots.HoldsState && sequencedEvent.SourceId == g._lifecycle.GatewaySourceId)
                    {
                        g._listener.OnConnectionClosed(sequencedEvent.ConnectionId);
                    }
                    break;
                case SystemFrame.GatewayActive:
                    Wrap(sequencedEvent);
                    _gatewayActive.WrapForDecode(_view, 0, Frame.GatewayActive.BlockLength,
                                                 Frame.GatewayActive.SchemaVersion);
                    g._lifecycle.OnGatewayActive(_gatewayActive.GatewayId);
                    if (!g._lifecycle.IsActivated)
                    {
                        g._snapshots.StopPublishing();
                        // S-6 refuses whatever this instance's session sends from here on, resends included.
                        g._session.DiscardUnconfirmed("gateway instance " + _gatewayActive.GatewayId +
                                                      " is designated");
                    }
                    break;
                case SystemFrame.SnapshotStarted:
                    Wrap(sequencedEvent);
                    _snapshotStarted.WrapForDecode(_view, 0, Frame.SnapshotStarted.BlockLength,
                                                   Frame.SnapshotStarted.SchemaVersion);
                    g._snapshots.OnSnapshotStarted(_snapshotStarted.Round, g.CurrentSnapshotHeader(),
                                                   g._lifecycle.IsAnnounced);
                    break;
                case SystemFrame.SnapshotEnd:
                    if (sequencedEvent.SourceId != g._lifecycle.GatewaySourceId)
                    {
                        break;
                    }
                    Wrap(sequencedEvent);
                    _snapshotEnd.WrapForDecode(_view, 0, Frame.SnapshotEnd.BlockLength,
                                               Frame.MessageHeader.SbeSchemaVersion);
                    if (!g._snapshots.OnSnapshotEnd(_snapshotEnd.Round, _snapshotEnd.RecordCount, _snapshotEnd.Length,
                                                    _snapshotEnd.Crc32c))
                    {
                        g._session.Fence(ClusterError.SnapshotDiverged,
                                         "round " + _snapshotEnd.Round + "'s sequenced snapshot at globalSeqNo " +
                                             sequencedEvent.GlobalSeqNo + " differs from this instance's");
                    }
                    break;
                default:
                    break;
            }
        }

        public void OnLeadershipChanged()
        {
            // A gateway is elected by the cluster rather than gated on its node leading: nothing to do.
        }

        public void OnPayload(Payload payload)
        {
            _gateway.ObserveConnectionId(payload.SourceId, payload.ConnectionId);
            if (_gateway._snapshots.HoldsState)
            {
                _gateway._listener.OnSequenced(payload);
            }
        }

        public void OnCaughtUp(long globalSeqNo)
        {
            _gateway._lifecycle.OnCaughtUp();
            _gateway._listener.OnCaughtUp(globalSeqNo);
        }

        public void OnClusterHeartbeat(long clusterTimeNs, long receiveTimeNs)
        {
            _gateway._listener.OnClusterHeartbeat(clusterTimeNs, receiveTimeNs);
        }

        public void OnFenced(ClusterError fence, string detail)
        {
            _gateway._listener.OnFenced(fence, detail);
        }

        // Not while designated: closing an active instance's session promotes its sibling, and no frame on the tap
        // says whether that promotion is still to come, so a replacement could announce itself on a stale view.
        public bool MayReconnect()
        {
            return !_gateway._lifecycle.IsActivated;
        }

        // The designated instance: a standby serves no one.
        public bool IsActing()
        {
            return _gateway._lifecycle.IsActivated;
        }

        // A ConnectionOpened's opaque tail, or nothing. §7.1 lets connectionData be absent, and a producer that takes
        // the option encodes no var-data header at all — so a payload too short to hold one is that case, not a short
        // read.
        private void DispatchConnectionOpened(SequencedEvent sequencedEvent)
        {
            if (sequencedEvent.PayloadLength < Frame.ConnectionOpened.ConnectionDataHeaderSize)
            {
                _gateway._listener.OnConnectionOpened(sequencedEvent.ConnectionId, sequencedEvent.Buffer,
                                                      sequencedEvent.PayloadOffset, 0);
                return;
            }
            Wrap(sequencedEvent);
            _connectionOpenedIn.WrapForDecode(_view, 0, Frame.ConnectionOpened.BlockLength,
                                              Frame.MessageHeader.SbeSchemaVersion);
            int length = _connectionOpenedIn.ConnectionDataLength();
            _gateway._listener.OnConnectionOpened(sequencedEvent.ConnectionId, sequencedEvent.Buffer,
                                                  sequencedEvent.PayloadOffset + _connectionOpenedIn.Limit +
                                                      Frame.ConnectionOpened.ConnectionDataHeaderSize,
                                                  length);
        }

        private void Wrap(SequencedEvent sequencedEvent)
        {
            SbeBuffers.Wrap(_view, sequencedEvent.Buffer, sequencedEvent.PayloadOffset, sequencedEvent.PayloadLength);
        }
    }
}
