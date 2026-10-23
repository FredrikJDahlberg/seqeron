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
/// App-replica side of the per-node <c>ReplayerService</c>: reads the co-located queue live and asks the Replayer for
/// history and gaps. The Aeron adapter only: every decision lives in <c>ReplayerRecovery</c>.
/// <c>ReplayerStreamReceiver.java</c> and <c>replayer/client/ReplayerStreamReceiver.hpp</c> are its twins.
/// <para>Single-threaded: every method runs on the one duty-cycle thread.</para>
/// </summary>
public sealed class ReplayerStreamReceiver : IDisposable
{
    /// <summary>The queue as a consumer addresses it: untethered, so a slow app is dropped and heals via
    /// replay.</summary>
    public const string QueueConsumerChannel = FrameLayer.QueueChannel + "|tether=false";

    // Untethered like the queue: the Replayer answers every app from one thread, so an app that stops polling must
    // not back-pressure the others' replies. A dropped reply costs one resend interval.
    private const string ControlChannel = ReplayProtocol.IpcChannel + "|tether=false";

    private const int FragmentLimit = 16;

    // Scratch for the two control messages this client sends; each is well under 64 bytes.
    private const int RequestBufferLength = 64;

    private readonly int _clientId;
    private readonly ReplayerRecovery _recovery;
    private readonly Func<bool> _queueFaults;

    private readonly Replay.MessageHeader _requestHeader = new Replay.MessageHeader();
    private readonly Replay.ReplayRequest _replayRequest = new Replay.ReplayRequest();
    private readonly Replay.SnapshotJoined _snapshotJoined = new Replay.SnapshotJoined();
    private readonly Replay.SnapshotConfirmed _snapshotConfirmed = new Replay.SnapshotConfirmed();
    private readonly Replay.MessageHeader _controlHeader = new Replay.MessageHeader();
    private readonly Replay.SnapshotRound _snapshotRound = new Replay.SnapshotRound();
    private readonly SbeBuffer _controlView = new SbeBuffer();
    private readonly UnsafeBuffer _requestBuffer = new UnsafeBuffer(GC.AllocateArray<byte>(RequestBufferLength, true));
    private readonly SbeBuffer _requestView = new SbeBuffer();

    private readonly FragmentAssembler _queueHandler;
    private readonly FragmentAssembler _replayHandler;
    private readonly FragmentAssembler _controlHandler;

    private Action<long, long> _onSnapshotRound;

    private Aeron _aeron;
    private int? _memberId;
    private int _groupId;

    // The id the Replayer addresses this receiver's replies to, drawn from the driver at start; -1 before.
    private long _replyId = -1;
    private Subscription _queueSubscription;
    private Subscription _replaySubscription;
    private Subscription _controlSubscription;
    private Publication _requestPublication;
    private Image _replayImage;
    private Counter _recoveryStalledCounter;

    /// <summary>A receiver, dropping a live queue frame whenever <paramref name="queueFaults"/> answers true.</summary>
    /// <param name="clientId">this replica's id on the node, its counters' <c>client</c> label; replicas sharing one
    /// share their counters' series</param>
    /// <param name="onSequenced">receives every in-order frame</param>
    /// <param name="onLeadershipChanged">receives each leadership change, or null to have it reach
    /// <paramref name="onSequenced"/></param>
    /// <param name="onCaughtUp">fires on every transition to caught-up, or null</param>
    /// <param name="queueFaults">test harnesses only: polled once per live queue frame, on the poll thread; null drops
    /// nothing</param>
    /// <exception cref="ArgumentNullException">if <paramref name="onSequenced"/> is null</exception>
    public ReplayerStreamReceiver(int clientId, SequencedHandler onSequenced, LeadershipHandler onLeadershipChanged,
                                  CaughtUpHandler onCaughtUp, Func<bool> queueFaults = null)
    {
        _clientId = clientId;
        _queueFaults = queueFaults;
        _recovery = new ReplayerRecovery(new Actions(this), onSequenced, onLeadershipChanged, onCaughtUp);
        _queueHandler = new FragmentAssembler(OnQueueFragment);
        _replayHandler = new FragmentAssembler(OnReplayFragment);
        _controlHandler = new FragmentAssembler(OnControlFragment);
        SbeBuffers.Wrap(_requestView, _requestBuffer, 0, RequestBufferLength);
    }

    /// <summary>Hands each <c>SnapshotRound</c> the Replayer sends to <paramref name="handler"/>, with the round and
    /// its cut; none is delivered without one (doc/snapshot.md §10.8).</summary>
    /// <param name="handler">receives the round's number and its cut's <c>clusterSeqNo</c></param>
    public void OnSnapshotRound(Action<long, long> handler)
    {
        _onSnapshotRound = handler;
    }

    /// <summary>Tells the Replayer this replica takes part in <paramref name="round"/>. Best-effort: the offer may not
    /// land.</summary>
    /// <param name="sourceId">the application's <c>sourceId</c></param>
    /// <param name="round">the round's number</param>
    /// <returns>whether it was placed</returns>
    public bool SendSnapshotJoined(int sourceId, long round)
    {
        if (!RequestPublicationConnected)
        {
            return false;
        }
        _snapshotJoined.WrapForEncodeAndApplyHeader(_requestView, 0, _requestHeader);
        _snapshotJoined.ClientId = _replyId;
        _snapshotJoined.SourceId = sourceId;
        _snapshotJoined.Round = round;
        return _requestPublication.Offer(_requestBuffer, 0, Replay.MessageHeader.Size + _snapshotJoined.Size) > 0;
    }

    /// <summary>Tells the Replayer this replica's file for <paramref name="round"/> is durable under its name.
    /// Best-effort: the offer may not land.</summary>
    /// <param name="sourceId">the application's <c>sourceId</c></param>
    /// <param name="round">the round's number</param>
    /// <returns>whether it was placed</returns>
    public bool SendSnapshotConfirmed(int sourceId, long round)
    {
        if (!RequestPublicationConnected)
        {
            return false;
        }
        _snapshotConfirmed.WrapForEncodeAndApplyHeader(_requestView, 0, _requestHeader);
        _snapshotConfirmed.ClientId = _replyId;
        _snapshotConfirmed.SourceId = sourceId;
        _snapshotConfirmed.Round = round;
        return _requestPublication.Offer(_requestBuffer, 0, Replay.MessageHeader.Size + _snapshotConfirmed.Size) > 0;
    }

    /// <summary>Restores the newest snapshot in <paramref name="store"/> on <see cref="Start"/>, before anything is
    /// dispatched, once a replay shows the log holds its cut (doc/snapshot.md §7). Call before <see cref="Start"/>;
    /// <see cref="RestoreFailure"/> reports a snapshot that cannot be restored.</summary>
    /// <param name="store">this instance's snapshot files</param>
    /// <param name="handler">takes the restored records</param>
    public void RestoreFrom(SnapshotStore store, ISnapshotRestoreHandler handler)
    {
        _recovery.RestoreFrom(store, handler);
    }

    /// <summary>Subscribes the queue of a group and the control stream, opens the request publication and the
    /// convergence counter, and requests the cold-start replay from the recording's start, or the snapshot to restore
    /// first.</summary>
    /// <param name="aeron">client sharing the co-located node's media driver</param>
    /// <param name="memberId">this app's node, to label the counter</param>
    /// <param name="groupId">the group whose queue to read: a consumer names its own, a façade passes the one the
    /// Replayer resolved (spec R-6)</param>
    public void Start(Aeron aeron, int memberId, int groupId)
    {
        _aeron = aeron;
        _memberId = memberId;
        _groupId = groupId;
        _replyId = aeron.NextCorrelationId();
        Logger.Info(Logger.CoreComponent.ReplayerStreamReceiver, memberId,
                    "clientId {0} reads group {1}'s queue and requests replays as client {2}", _clientId, groupId,
                    _replyId);
        _recoveryStalledCounter = SeqeronCounters.AddAppCounter(
            aeron, SeqeronCounters.AppRecoveryStalledTypeId,
            $"seqeron.app.recoveryStalled member={memberId} client={_clientId}", memberId, _clientId);
        _queueSubscription = aeron.AddSubscription(QueueConsumerChannel, FrameLayer.QueueStreamIdOf(groupId));
        _controlSubscription = aeron.AddSubscription(ControlChannel, ReplayProtocol.ControlStreamId);
        _requestPublication = aeron.AddPublication(ReplayProtocol.IpcChannel, ReplayProtocol.RequestStreamId);
        _recovery.Start();
    }

    /// <summary>One duty-cycle iteration: drain control, ride an attached replay image, and always drain and
    /// dispatch the queue — <c>ReplayerRecovery</c>'s contiguity check, not the poll routing, decides what a queue
    /// frame is worth mid-walk.</summary>
    /// <returns>fragments consumed and snapshot records restored</returns>
    /// <exception cref="InvalidOperationException">once this node's recording does not hold what recovery asked of
    /// it</exception>
    public int Poll()
    {
        int work = 0;
        if (_controlSubscription != null)
        {
            work += _controlSubscription.Poll(_controlHandler, FragmentLimit);
        }

        if (_recovery.Fault != null)
        {
            throw new InvalidOperationException("[ReplayerStreamReceiver] " + _recovery.Fault);
        }

        bool requestPubPending = _requestPublication != null && !_requestPublication.IsConnected;
        work += _recovery.DoTimers(requestPubPending);

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
        if (_queueSubscription != null)
        {
            work += _queueSubscription.Poll(_queueHandler, FragmentLimit);
        }

        _recovery.CheckRecoveryProgress();
        return work;
    }

    /// <summary>Whether this client is following the live tail; revoked on a queue gap, re-established at the
    /// seam.</summary>
    public bool IsCaughtUp => _recovery.IsCaughtUp;

    /// <summary>Highest clusterSeqNo dispatched in order, 0 before the first. The frontier a consumer measures its
    /// own recovery progress by.</summary>
    public long LastClusterSeqNo => _recovery.LastClusterSeqNo;

    /// <summary>memberId of the current leader per the last <c>LeadershipChanged</c> processed, or -1 until one is
    /// seen. A replica emits iff its own node is this leader.</summary>
    public int CurrentLeaderMemberId => _recovery.CurrentLeaderMemberId;

    /// <summary>Why the snapshot given to <see cref="RestoreFrom"/> cannot be restored, or null. Latched: recovery
    /// has stopped.</summary>
    public string RestoreFailure => _recovery.RestoreFailure;

    /// <summary>Closes every subscription, the publication and the counter. The Aeron client is the
    /// caller's.</summary>
    public void Dispose()
    {
        CloseReplay();
        _recovery.Close();
        _queueSubscription?.Dispose();
        _queueSubscription = null;
        _controlSubscription?.Dispose();
        _controlSubscription = null;
        _requestPublication?.Dispose();
        _requestPublication = null;
        _recoveryStalledCounter?.Dispose();
        _recoveryStalledCounter = null;
    }

    private void OnQueueFragment(IDirectBuffer buffer, int offset, int length, Header header)
    {
        if (_queueFaults != null && _queueFaults())
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

    // A SnapshotRound is no one's reply, so it never reaches ReplayerRecovery.
    private void OnControlFragment(IDirectBuffer buffer, int offset, int length, Header header)
    {
        if (length >= Replay.MessageHeader.Size)
        {
            SbeBuffers.Wrap(_controlView, buffer, offset, length);
            _controlHeader.Wrap(_controlView, 0, Replay.MessageHeader.SbeSchemaVersion);
            if (_controlHeader.TemplateId == Replay.SnapshotRound.TemplateId)
            {
                if (_onSnapshotRound != null && Replay.MessageHeader.Size + _controlHeader.BlockLength <= length)
                {
                    _snapshotRound.WrapForDecode(_controlView, Replay.MessageHeader.Size, _controlHeader.BlockLength,
                                                 _controlHeader.Version);
                    _onSnapshotRound(_snapshotRound.Round, _snapshotRound.ClusterSeqNo);
                }
                return;
            }
        }
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

        public void SendReplayRequest(long requestId, long fromPosition, long cutClusterSeqNo)
        {
            if (!_receiver.RequestPublicationConnected)
            {
                return; // Replayer not up yet; the resend timer retries
            }
            Replay.ReplayRequest request = _receiver._replayRequest;
            request.WrapForEncodeAndApplyHeader(_receiver._requestView, 0, _receiver._requestHeader);
            request.ClientId = _receiver._replyId;
            request.RequestId = requestId;
            request.FromPosition = fromPosition;
            request.QueueId = _receiver._groupId;
            request.CutClusterSeqNo = cutClusterSeqNo;
            Offer(Replay.MessageHeader.Size + request.Size);
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
            string channel = ReplayProtocol.IpcChannel + "|session-id=" + (int)replaySessionId;
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

        public long ClientId => _receiver._replyId;

        public long NowMs()
        {
            return Clocks.MonotonicMs();
        }

        private void Offer(int length)
        {
            _receiver._requestPublication.Offer(_receiver._requestBuffer, 0, length);
        }
    }
}
