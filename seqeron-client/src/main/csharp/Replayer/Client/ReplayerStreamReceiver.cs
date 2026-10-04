using System;
using Adaptive.Aeron;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Org.Limitless.Seqeron.Protocol;
using Org.Limitless.Seqeron.Util;
using Replay = Org.Limitless.Seqeron.Sbe.Replay;
using SbeBuffer = Org.SbeTool.Sbe.Dll.DirectBuffer;

namespace Org.Limitless.Seqeron.Replayer.Client;

/// <summary>
/// App-replica side of the per-node <c>ReplayerService</c>: reads the co-located tap live and asks the Replayer for
/// history and gaps. The Aeron adapter only: every decision lives in <c>ReplayerRecovery</c>.
/// <c>ReplayerStreamReceiver.java</c> and <c>replayer/client/ReplayerStreamReceiver.hpp</c> are its twins.
/// <para>Single-threaded: every method runs on the one duty-cycle thread.</para>
/// </summary>
public sealed class ReplayerStreamReceiver : IDisposable
{
    /// <summary>The tap as a consumer addresses it: untethered, so a slow app is dropped and heals via
    /// replay.</summary>
    public const string FeederConsumerChannel = FrameLayer.FeederChannel + "?tether=false";

    // Untethered like the tap: the Replayer answers every app from one thread, so an app that stops polling must
    // not back-pressure the others' replies. A dropped reply costs one resend interval.
    private const string ControlChannel = ReplayProtocol.IpcChannel + "?tether=false";

    private const int FragmentLimit = 16;

    // Scratch for the four control messages this client sends; each is well under 64 bytes.
    private const int RequestBufferLength = 64;

    private readonly int _clientId;
    private readonly ReplayerRecovery _recovery;
    private readonly Func<bool> _tapFaults;

    private readonly Replay.MessageHeader _requestHeader = new Replay.MessageHeader();
    private readonly Replay.ReplayRequest _replayRequest = new Replay.ReplayRequest();
    private readonly Replay.ReplayComplete _replayComplete = new Replay.ReplayComplete();
    private readonly Replay.ReplayHeartbeat _replayHeartbeat = new Replay.ReplayHeartbeat();
    private readonly Replay.SnapshotQuery _snapshotQuery = new Replay.SnapshotQuery();
    private readonly UnsafeBuffer _requestBuffer = new UnsafeBuffer(GC.AllocateArray<byte>(RequestBufferLength, true));
    private readonly SbeBuffer _requestView = new SbeBuffer();

    private readonly FragmentAssembler _tapHandler;
    private readonly FragmentAssembler _replayHandler;
    private readonly FragmentAssembler _controlHandler;

    private Aeron _aeron;
    private int? _memberId;
    private Subscription _tapSubscription;
    private Subscription _replaySubscription;
    private Subscription _controlSubscription;
    private Publication _requestPublication;
    private Image _replayImage;
    private Counter _recoveryStalledCounter;

    /// <summary>A receiver, dropping a live tap frame whenever <paramref name="tapFaults"/> answers true.</summary>
    /// <param name="clientId">this replica's stable id, unique among the Replayer's co-located apps
    /// (<c>SEQERON_REPLAYER_CLIENT_ID</c>); two apps sharing one supersede each other's replays and neither ever
    /// catches up</param>
    /// <param name="onSequenced">receives every in-order frame</param>
    /// <param name="onLeadershipChanged">receives each leadership change, or null to have it reach
    /// <paramref name="onSequenced"/></param>
    /// <param name="onCaughtUp">fires on every transition to caught-up, or null</param>
    /// <param name="tapFaults">test harnesses only: polled once per live tap frame, on the poll thread; null drops
    /// nothing</param>
    /// <exception cref="ArgumentNullException">if <paramref name="onSequenced"/> is null</exception>
    public ReplayerStreamReceiver(int clientId, SequencedHandler onSequenced, LeadershipHandler onLeadershipChanged,
                                  CaughtUpHandler onCaughtUp, Func<bool> tapFaults = null)
    {
        _clientId = clientId;
        _tapFaults = tapFaults;
        _recovery = new ReplayerRecovery(clientId, new Actions(this), onSequenced, onLeadershipChanged, onCaughtUp);
        _tapHandler = new FragmentAssembler(OnTapFragment);
        _replayHandler = new FragmentAssembler(OnReplayFragment);
        _controlHandler = new FragmentAssembler(OnControlFragment);
        SbeBuffers.Wrap(_requestView, _requestBuffer, 0, RequestBufferLength);
    }

    /// <summary>Restores <paramref name="sourceId"/>'s newest snapshot in <paramref name="store"/> that the log
    /// confirms on <see cref="Start"/>, before anything is dispatched (doc/snapshot.md §7). Call before
    /// <see cref="Start"/>; <see cref="RestoreFailure"/> reports a snapshot that cannot be restored.</summary>
    /// <param name="sourceId">the source whose snapshot to restore</param>
    /// <param name="store">this instance's snapshot files</param>
    /// <param name="handler">takes the restored records</param>
    public void RestoreFrom(int sourceId, SnapshotStore store, ISnapshotRestoreHandler handler)
    {
        _recovery.RestoreFrom(sourceId, store, handler);
    }

    /// <summary>Subscribes the tap and control streams, opens the request publication and the convergence counter,
    /// and requests the cold-start replay from the recording's start, or the snapshot to restore first.</summary>
    /// <param name="aeron">client sharing the co-located node's media driver</param>
    /// <param name="memberId">this app's node, to label the counter</param>
    public void Start(Aeron aeron, int memberId)
    {
        _aeron = aeron;
        _memberId = memberId;
        _recoveryStalledCounter = SeqeronCounters.AddAppCounter(
            aeron, SeqeronCounters.AppRecoveryStalledTypeId,
            $"seqeron.app.recoveryStalled member={memberId} client={_clientId}", memberId, _clientId);
        _tapSubscription = aeron.AddSubscription(FeederConsumerChannel, FrameLayer.FeederStreamId);
        _controlSubscription = aeron.AddSubscription(ControlChannel, ReplayProtocol.ControlStreamId);
        _requestPublication = aeron.AddPublication(ReplayProtocol.IpcChannel, ReplayProtocol.RequestStreamId);
        _recovery.Start();
    }

    /// <summary>One duty-cycle iteration: drain control, ride an attached replay image, and always drain and
    /// dispatch the tap — <c>ReplayerRecovery</c>'s contiguity check, not the poll routing, decides what a tap
    /// frame is worth mid-walk.</summary>
    /// <returns>fragments consumed</returns>
    /// <exception cref="InvalidOperationException">once this node's Replayer reports another process using this
    /// clientId</exception>
    public int Poll()
    {
        int work = 0;
        if (_controlSubscription != null)
        {
            work += _controlSubscription.Poll(_controlHandler, FragmentLimit);
        }

        if (_recovery.IsClientIdInUse)
        {
            throw new InvalidOperationException(
                $"[ReplayerStreamReceiver] clientId {_clientId} is in use by another process on this node");
        }

        bool requestPubPending = _requestPublication != null && !_requestPublication.IsConnected;
        _recovery.DoTimers(requestPubPending);

        if (_recovery.ReplaySessionId >= 0)
        {
            if (_replayImage == null && _replaySubscription != null)
            {
                _replayImage = _replaySubscription.ImageBySessionId((int)_recovery.ReplaySessionId);
            }
            if (_replayImage != null)
            {
                Image image = _replayImage;
                if (!image.Closed)
                {
                    work += image.Poll(_replayHandler, FragmentLimit);
                    if (_replayImage == image)
                    {
                        _recovery.OnReplayPosition(image.Position);
                    }
                }
                else
                {
                    _recovery.OnReplayImageClosed(image.Position);
                }
            }
        }
        if (_tapSubscription != null)
        {
            work += _tapSubscription.Poll(_tapHandler, FragmentLimit);
        }

        _recovery.CheckRecoveryProgress();
        return work;
    }

    /// <summary>Whether this client is following the live tail; revoked on a tap gap, re-established at the
    /// seam.</summary>
    public bool IsCaughtUp => _recovery.IsCaughtUp;

    /// <summary>Highest globalSeqNo dispatched in order, 0 before the first. The frontier a consumer measures its
    /// own recovery progress by.</summary>
    public long LastGlobalSeqNo => _recovery.LastGlobalSeqNo;

    /// <summary>memberId of the current leader per the last <c>LeadershipChanged</c> processed, or -1 until one is
    /// seen. A replica emits iff its own node is this leader.</summary>
    public int CurrentLeaderMemberId => _recovery.CurrentLeaderMemberId;

    /// <summary>Starts recovery over as a cold start, restoring the snapshot given to <see cref="RestoreFrom"/>
    /// again: a passive gateway instance's activation. Every frame after the restored cut is dispatched once
    /// more.</summary>
    public void Restart()
    {
        _recovery.Restart();
    }

    /// <summary>Why the snapshot given to <see cref="RestoreFrom"/> cannot be restored, or null. Latched: recovery
    /// has stopped.</summary>
    public string RestoreFailure => _recovery.RestoreFailure;

    /// <summary>Closes every subscription, the publication and the counter. The Aeron client is the
    /// caller's.</summary>
    public void Dispose()
    {
        CloseReplay();
        _tapSubscription?.Dispose();
        _tapSubscription = null;
        _controlSubscription?.Dispose();
        _controlSubscription = null;
        _requestPublication?.Dispose();
        _requestPublication = null;
        _recoveryStalledCounter?.Dispose();
        _recoveryStalledCounter = null;
    }

    private void OnTapFragment(IDirectBuffer buffer, int offset, int length, Header header)
    {
        if (_tapFaults != null && _tapFaults())
        {
            return;
        }
        _recovery.OnFrame(buffer, offset, length, FrameStartPosition(header), Clocks.EpochNanos(), false);
    }

    // Drops the rest of a poll's batch once a frame in it has superseded the replay, which closes the image.
    private void OnReplayFragment(IDirectBuffer buffer, int offset, int length, Header header)
    {
        if (_replayImage != null)
        {
            _recovery.OnFrame(buffer, offset, length, FrameStartPosition(header), Clocks.EpochNanos(), true);
        }
    }

    private void OnControlFragment(IDirectBuffer buffer, int offset, int length, Header header)
    {
        _recovery.OnControl(buffer, offset, length);
    }

    // Stream position of the first byte of the frame header describes. Not header.Position - frameLength: that is
    // the next frame's, aligned to 32 bytes, and a replay position must sit on a frame boundary.
    private static long FrameStartPosition(Header header)
    {
        return LogBufferDescriptor.ComputePosition(header.TermId, header.TermOffset, header.PositionBitsToShift,
                                                   header.InitialTermId);
    }

    private void CloseReplay()
    {
        _replayImage = null;
        _replaySubscription?.Dispose();
        _replaySubscription = null;
    }

    private bool RequestPublicationConnected => _requestPublication != null && _requestPublication.IsConnected;

    // The recovery state machine's transport. Private, so a consumer cannot drive the replay protocol.
    private sealed class Actions : IReplayerRecoveryActions
    {
        private readonly ReplayerStreamReceiver _receiver;

        public Actions(ReplayerStreamReceiver receiver)
        {
            _receiver = receiver;
        }

        public void SendReplayRequest(long requestId, long fromPosition)
        {
            if (!_receiver.RequestPublicationConnected)
            {
                return; // Replayer not up yet; the resend timer retries
            }
            Replay.ReplayRequest request = _receiver._replayRequest;
            request.WrapForEncodeAndApplyHeader(_receiver._requestView, 0, _receiver._requestHeader);
            request.ClientId = _receiver._clientId;
            request.RequestId = requestId;
            request.FromPosition = fromPosition;
            Offer(Replay.MessageHeader.Size + request.Size);
        }

        public void SendSnapshotQuery(long requestId, int sourceId, long round)
        {
            if (!_receiver.RequestPublicationConnected)
            {
                return;
            }
            Replay.SnapshotQuery query = _receiver._snapshotQuery;
            query.WrapForEncodeAndApplyHeader(_receiver._requestView, 0, _receiver._requestHeader);
            query.ClientId = _receiver._clientId;
            query.RequestId = requestId;
            query.SourceId = sourceId;
            query.Round = round;
            Offer(Replay.MessageHeader.Size + query.Size);
        }

        public bool SendReplayComplete()
        {
            if (!_receiver.RequestPublicationConnected)
            {
                return false;
            }
            Replay.ReplayComplete complete = _receiver._replayComplete;
            complete.WrapForEncodeAndApplyHeader(_receiver._requestView, 0, _receiver._requestHeader);
            complete.ClientId = _receiver._clientId;
            return Offer(Replay.MessageHeader.Size + complete.Size) >= 0;
        }

        public bool SendReplayHeartbeat()
        {
            if (!_receiver.RequestPublicationConnected)
            {
                return false;
            }
            Replay.ReplayHeartbeat heartbeat = _receiver._replayHeartbeat;
            heartbeat.WrapForEncodeAndApplyHeader(_receiver._requestView, 0, _receiver._requestHeader);
            heartbeat.ClientId = _receiver._clientId;
            return Offer(Replay.MessageHeader.Size + heartbeat.Size) >= 0;
        }

        // Subscribes to exactly one replay, this one, and to nothing on the replay stream otherwise. A standing
        // subscription would make every idle app a tethered, never-polled subscriber of every other app's replay
        // on the shared stream, and wedge the archive's replay one window in.
        public void OpenReplay(long replaySessionId)
        {
            _receiver.CloseReplay();
            if (_receiver._aeron == null)
            {
                return;
            }
            string channel = ReplayProtocol.IpcChannel + "?session-id=" + (int)replaySessionId;
            _receiver._replaySubscription = _receiver._aeron.AddSubscription(channel, ReplayProtocol.ReplayStreamId);
        }

        public void CloseReplay()
        {
            _receiver.CloseReplay();
        }

        public void RecoveryStalled(bool stalled)
        {
            _receiver._recoveryStalledCounter?.Set(stalled ? 1 : 0);
        }

        public int? MemberId => _receiver._memberId;

        public long NowMs()
        {
            return DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        }

        private long Offer(int length)
        {
            return _receiver._requestPublication.Offer(_receiver._requestBuffer, 0, length);
        }
    }
}
