using System;
using System.Collections.Generic;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Org.Limitless.Seqeron.Protocol;
using Org.Limitless.Seqeron.Util;
using Frame = Org.Limitless.Seqeron.Sbe.Frame;
using Replay = Org.Limitless.Seqeron.Sbe.Replay;
using SbeBuffer = Org.SbeTool.Sbe.Dll.DirectBuffer;

namespace Org.Limitless.Seqeron.Replayer.Client;

/// <summary>
/// The state machine of <c>ReplayerStreamReceiver</c>. A cold start walks the node's active recording from its
/// start, which holds the whole log; catch-up is detected by position (<c>Replaying.catchUpPosition</c>); a gap by
/// globalSeqNo, repaired by resuming the active recording at the last dispatched frame.
/// <para>The replay-to-live seam is closed by the tap itself: the untethered tap is polled every duty cycle, so
/// frames beyond the current hole are retained and drained once contiguous. While <see cref="IsRecovering"/> a
/// non-contiguous tap frame is expected rather than a new gap, and it may not set the globalSeqNo baseline before
/// the walk has: a cold start would otherwise adopt a mid-stream baseline.</para>
/// <para><see cref="IsCaughtUp"/> is cleared on a live-tap gap and re-established once contiguous: consumers gate
/// real decisions on it. Single-threaded: every method runs on the one duty-cycle thread.</para>
/// <para>Given a snapshot store (<see cref="RestoreFrom"/>), a cold start first restores the newest snapshot it holds
/// (doc/snapshot.md §7): it replays the recording from the snapshot's cut, and if the frame there is the round's
/// <c>SnapshotStarted</c> the file recorded, reads the file's records into the handler, dispatching nothing, then
/// resumes at the cut with it as the anchor. A file whose cut the recording does not hold gives way to the next older
/// one, and the last to a walk.</para>
/// <para><c>ReplayerRecovery.java</c> and <c>ReplayerRecovery.hpp</c> are its twins; keep the three in step.</para>
/// </summary>
internal sealed class ReplayerRecovery
{
    private const long ResendIntervalMs = 500;

    // How long an established replay may deliver nothing before it is re-requested. The archive reads local disk,
    // so a replay with anything left is never quiet this long; a spurious fire re-requests the replay.
    private const long ReplayStallTimeoutMs = 5_000;

    // How long recovery may dispatch nothing before it is reported unconvergent: a re-request loop that keeps
    // finishing healthy replays yet dispatches nothing, which ReplayStallTimeoutMs cannot see.
    private const long RecoveryProgressTimeoutMs = 30_000;

    // Caps on frames retained ahead of a hole; past either, recovery resumes at the last dispatched frame.
    private const int MaxRetainedFrames = 65536;
    private const long MaxRetainedBytes = 16L * 1024 * 1024;

    /// <summary>Records one duty cycle restores at most, so a large snapshot does not stall the session.</summary>
    internal const int MaxRestoreRecordsPerCycle = 1024;

    private readonly IReplayerRecoveryActions _actions;
    private readonly SequencedHandler _onSequenced;
    private readonly LeadershipHandler _onLeadershipChanged;
    private readonly CaughtUpHandler _onCaughtUp;

    private readonly SequencedEvent _event = new SequencedEvent();
    private readonly SbeBuffer _view = new SbeBuffer();
    private readonly Frame.LeadershipChanged _leadershipChanged = new Frame.LeadershipChanged();
    private readonly Replay.MessageHeader _controlHeader = new Replay.MessageHeader();
    private readonly Replay.Replaying _replaying = new Replay.Replaying();
    private readonly Replay.ReplayPending _replayPending = new Replay.ReplayPending();
    private readonly UnsafeBuffer _restoreRecord = new UnsafeBuffer();

    // When the current no-progress episode started; 0 = none timed.
    private long _noProgressSinceMs;
    private bool _recoveryStallReported;

    private bool _awaitingReplay;
    private long _replaySessionId = -1;

    // Position the bounded replay ends at; the replay is done once the image reaches it.
    private long _catchUpPosition;

    // fromPosition of the current request, for an idempotent resend; FromStart for a walk.
    private long _requestFromPosition = ReplayProtocol.FromStart;

    private long _lastRequestMs;

    // Advances per send; replies not carrying it are stale (see OnControl).
    private long _requestId;

    private long _lastReplayPosition = -1;
    private long _lastReplayProgressMs;

    private long _lastGlobalSeqNo;

    // Where the last dispatched frame starts in the recording; RequestResume's anchor.
    private long _lastFramePosition;

    // globalSeqNo a resume replay must open at, or 0 if not resuming.
    private long _resumeAnchorGlobalSeqNo;

    // Report a hole in replayed history once per episode, not per frame.
    private bool _replayGapLogged;

    private bool _caughtUp;
    private int _currentLeaderMemberId = -1;

    // Live tap frames from beyond the current hole, retained in arrival order.
    private readonly Queue<RetainBlock> _retained = new Queue<RetainBlock>();
    private readonly Stack<RetainBlock> _retainPool = new Stack<RetainBlock>();
    private RetainBlock _retainTail;
    private int _retainReadOffset;
    private long _retainTailGlobalSeqNo;
    private int _retainFrameCount;
    private long _retainBytes;

    // Frames were dropped ahead of the hole because the FIFO was full, so this client's frontier is short: it must
    // resume rather than declare itself caught up. Cleared by EndOverflowEpisode.
    private bool _retainOverflowed;
    private bool _retainOverflowLogged;

    private ISnapshotRestoreHandler _restoreHandler;

    // Where the snapshot to restore is, or null for none.
    private SnapshotStore _store;

    // Replaying from the chosen file's cut, to see that the recording holds it.
    private bool _checkingCut;

    // Reading the chosen file's records; nothing is dispatched until its last.
    private bool _restoring;
    private SnapshotStore.Reader _reader;
    private int _restoreRecordIndex;

    // The chosen snapshot's round.
    private long _snapshotRound;

    // The header of the restore in progress, null before its record 0.
    private SnapshotHeader _restoredHeader;

    /// <summary>Creates the state machine; nothing is sent before <see cref="Start"/>.</summary>
    /// <param name="actions">performs the sends and the replay subscription this class decides on, and supplies the
    /// clock and the id replies are addressed to</param>
    /// <param name="onSequenced">receives every in-order frame</param>
    /// <param name="onLeadershipChanged">receives each leadership change, or null to have it reach
    /// <paramref name="onSequenced"/></param>
    /// <param name="onCaughtUp">fires on every transition to caught-up, or null</param>
    /// <exception cref="ArgumentNullException">if <paramref name="onSequenced"/> is null</exception>
    public ReplayerRecovery(IReplayerRecoveryActions actions, SequencedHandler onSequenced,
                            LeadershipHandler onLeadershipChanged, CaughtUpHandler onCaughtUp)
    {
        ArgumentNullException.ThrowIfNull(onSequenced);
        _actions = actions;
        _onSequenced = onSequenced;
        _onLeadershipChanged = onLeadershipChanged;
        _onCaughtUp = onCaughtUp;
    }

    /// <summary>Restores the newest snapshot in <paramref name="store"/> on <see cref="Start"/>, before anything is
    /// dispatched. Call before <see cref="Start"/>.</summary>
    /// <param name="store">this instance's snapshot files</param>
    /// <param name="handler">takes the restored records</param>
    public void RestoreFrom(SnapshotStore store, ISnapshotRestoreHandler handler)
    {
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(handler);
        _store = store;
        _restoreHandler = handler;
    }

    /// <summary>Cold start: restore the newest local snapshot if restoring one, else walk the recording from its
    /// start.</summary>
    public void Start()
    {
        if (_store != null)
        {
            ChooseBelow(long.MaxValue);
        }
        else
        {
            RequestReplay(ReplayProtocol.FromStart);
        }
    }

    /// <summary>Releases the snapshot file a restore in progress holds open. Nothing may be called after.</summary>
    public void Close()
    {
        CloseReader();
    }

    /// <summary>Decodes one sequenced frame and applies the contiguity rules.</summary>
    /// <param name="buffer">holding the frame</param>
    /// <param name="offset">of its first byte</param>
    /// <param name="length">of the frame</param>
    /// <param name="framePosition">where this frame starts in the recording: the tap, a replay image and the
    /// recording itself all count positions in the same space</param>
    /// <param name="receiveNs">when the frame arrived, so a consumer's delivery-latency stats measure the tap rather
    /// than the drain of the retained FIFO</param>
    /// <param name="fromReplay">whether the frame arrived on a replay image rather than the live tap</param>
    /// <exception cref="InvalidOperationException">if the first frame observed is not globalSeqNo 1</exception>
    public void OnFrame(IDirectBuffer buffer, int offset, int length, long framePosition, long receiveNs,
                        bool fromReplay)
    {
        if (RestoreFailure != null || Fault != null || !_event.Wrap(buffer, offset, length))
        {
            return;
        }
        long globalSeqNo = _event.GlobalSeqNo;

        // First frame off a resume replay: it must be the frame whose position requested. Positions follow from the
        // frames, and the recording changes only with the node's driver, so a mismatch is a fault.
        if (fromReplay && _resumeAnchorGlobalSeqNo != 0)
        {
            long anchor = _resumeAnchorGlobalSeqNo;
            _resumeAnchorGlobalSeqNo = 0;
            if (_checkingCut)
            {
                OnCutReplayed(globalSeqNo == anchor && _event.ClusterTimestampNs == _reader.CutTimestampNs);
                return;
            }
            if (globalSeqNo != anchor)
            {
                Fail($"resume replay at position {_requestFromPosition} opened at globalSeqNo={globalSeqNo}, " +
                     $"expected {anchor}");
                return;
            }
        }
        if (_lastGlobalSeqNo != 0)
        {
            if (globalSeqNo <= _lastGlobalSeqNo)
            {
                return;
            }
            if (globalSeqNo > _lastGlobalSeqNo + 1)
            {
                if (!fromReplay && !IsRecovering)
                {
                    Logger.Log(Logger.CoreComponent.ReplayerStreamReceiver, Logger.Severity.Warn,
                               Logger.CoreEventCode.TapGap, _actions.MemberId,
                               "tap gap: expected globalSeqNo={0} got {1} — resuming the recording at globalSeqNo={2}",
                               _lastGlobalSeqNo + 1, globalSeqNo, _lastGlobalSeqNo);
                    // No longer following live: consumers gate real decisions on IsCaughtUp, and it must not hold
                    // again until the stream goes contiguous.
                    _caughtUp = false;
                    RequestResume();
                }
                if (!fromReplay)
                {
                    RetainFrame(globalSeqNo, buffer, offset, length, framePosition, receiveNs);
                }
                else if (!_replayGapLogged)
                {
                    _replayGapLogged = true;
                    Logger.Log(Logger.CoreComponent.ReplayerStreamReceiver, Logger.Severity.Warn,
                               Logger.CoreEventCode.TapGap, _actions.MemberId,
                               "gap in REPLAYED history: expected globalSeqNo={0} got {1} — this node's recording " +
                                   "does not cover the hole; recovery cannot converge until it does",
                               _lastGlobalSeqNo + 1, globalSeqNo);
                }
                return;
            }
        }
        else if (globalSeqNo != 1)
        {
            if (!fromReplay && IsRecovering)
            {
                RetainFrame(globalSeqNo, buffer, offset, length, framePosition, receiveNs);
                return;
            }
            Logger.Fault(Logger.CoreComponent.ReplayerStreamReceiver, Logger.CoreEventCode.FirstFrameNotOne,
                         _actions.MemberId,
                         "FATAL: first frame observed has globalSeqNo={0}, expected 1 — this node's recording does " +
                             "not reach the start of the log",
                         globalSeqNo);
            throw new InvalidOperationException(
                $"first frame observed has globalSeqNo={globalSeqNo}, expected 1 — this node's recording does not " +
                "reach the start of the log");
        }
        DispatchFrame(buffer, offset, length, globalSeqNo, framePosition, receiveNs, fromReplay);
        DrainRetained();
    }

    /// <summary>Decodes one Replayer control message (<c>Replaying</c>/<c>ReplayPending</c>).</summary>
    /// <param name="buffer">holding the message</param>
    /// <param name="offset">of its first byte</param>
    /// <param name="length">of the message</param>
    public void OnControl(IDirectBuffer buffer, int offset, int length)
    {
        if (length < Replay.MessageHeader.Size)
        {
            return;
        }
        SbeBuffers.Wrap(_view, buffer, offset, length);
        _controlHeader.Wrap(_view, 0, Replay.MessageHeader.SbeSchemaVersion);
        int bodyOffset = Replay.MessageHeader.Size;
        int blockLength = _controlHeader.BlockLength;
        int version = _controlHeader.Version;
        // SBE throws on a block past the view, and Image.Poll would swallow it.
        if (bodyOffset + blockLength > length)
        {
            return;
        }

        int templateId = _controlHeader.TemplateId;
        long clientId = _actions.ClientId;
        if (templateId == Replay.Replaying.TemplateId)
        {
            _replaying.WrapForDecode(_view, bodyOffset, blockLength, version);
            if (_replaying.ClientId != clientId)
            {
                return; // another replica's reply on the shared control stream
            }
            if (_replaying.RequestId != _requestId)
            {
                return;
            }
            OnReplaying(_replaying.ReplaySessionId, _replaying.CatchUpPosition);
        }
        else if (templateId == Replay.ReplayPending.TemplateId)
        {
            _replayPending.WrapForDecode(_view, bodyOffset, blockLength, version);
            if (_replayPending.ClientId == clientId && _replayPending.RequestId == _requestId)
            {
                _lastRequestMs = _actions.NowMs();
            }
        }
    }

    /// <summary>
    /// Why recovery cannot go on, or null. Latched: the receiver raises it from its duty cycle, since a throw from
    /// inside a fragment handler is swallowed.
    /// </summary>
    public string Fault { get; private set; }

    /// <summary>
    /// Where the attached replay image has reached. Completion is by position, not by the image closing: a bounded
    /// replay of an ACTIVE (still-recording) recording does NOT close its image at the bound.
    /// </summary>
    /// <param name="position">the image's position</param>
    public void OnReplayPosition(long position)
    {
        if (position >= _catchUpPosition)
        {
            OnReplayReachedBound();
            return;
        }
        if (position != _lastReplayPosition)
        {
            _lastReplayPosition = position;
            _lastReplayProgressMs = _actions.NowMs();
        }
    }

    /// <summary>
    /// Decides what a closed replay image means. A replay that closes at its bound is complete; any close short of
    /// it (superseded, faulted) is not, and treating it as complete would leave a silent hole.
    /// </summary>
    /// <param name="finalPosition">the image's position when it closed</param>
    public void OnReplayImageClosed(long finalPosition)
    {
        if (finalPosition >= _catchUpPosition)
        {
            OnReplayReachedBound();
            return;
        }
        Logger.Log(Logger.CoreComponent.ReplayerStreamReceiver, Logger.Severity.Warn, Logger.CoreEventCode.TapGap,
                   _actions.MemberId,
                   "replay image closed at position {0}, short of catchUpPosition {1} — the replay was stopped " +
                       "under us; re-requesting it",
                   finalPosition, _catchUpPosition);
        ReRequestCurrent();
    }

    /// <summary>
    /// Timer-driven work, once per duty cycle: the next records of a restore, the request resend, and the replay
    /// stall watchdog.
    /// </summary>
    /// <param name="requestPublicationPending">the request publication has not connected yet: retried every cycle
    /// rather than eating a full resend interval for a race much shorter than that</param>
    public void DoTimers(bool requestPublicationPending)
    {
        if (_restoring)
        {
            RestoreRecords();
        }
        long nowMs = _actions.NowMs();
        if (_awaitingReplay && (requestPublicationPending || (nowMs - _lastRequestMs) > ResendIntervalMs))
        {
            RequestReplay(_requestFromPosition); // re-send the same request verbatim
        }
        if (_replaySessionId >= 0 && (nowMs - _lastReplayProgressMs) > ReplayStallTimeoutMs)
        {
            OnReplayStalled();
        }
    }

    /// <summary>Recovery has run without dispatching a frame for <c>RecoveryProgressTimeoutMs</c>. Evaluated every
    /// duty cycle.</summary>
    /// <returns>whether it reported</returns>
    public bool CheckRecoveryProgress()
    {
        if (_caughtUp)
        {
            return false;
        }
        long nowMs = _actions.NowMs();
        if (_noProgressSinceMs == 0)
        {
            _noProgressSinceMs = nowMs; // the first observation only anchors the clock
            return false;
        }
        if (_recoveryStallReported || nowMs - _noProgressSinceMs < RecoveryProgressTimeoutMs)
        {
            return false;
        }
        _recoveryStallReported = true;
        Logger.Fault(Logger.CoreComponent.ReplayerStreamReceiver, Logger.CoreEventCode.RecoveryStalled,
                     _actions.MemberId,
                     "recovery has dispatched nothing for >{0}ms: lastGlobalSeqNo={1} fromPosition={2} " +
                         "awaitingReplay={3} replaySession={4} checkingCut={5} restoring={6} — holding; check this " +
                         "node's Replayer and its recording",
                     RecoveryProgressTimeoutMs, _lastGlobalSeqNo, _requestFromPosition, Text(_awaitingReplay),
                     _replaySessionId, Text(_checkingCut), Text(_restoring));
        _actions.RecoveryStalled(true);
        return true;
    }

    /// <summary>Whether this client is following the live tail; revoked on a tap gap, re-established at the
    /// seam.</summary>
    public bool IsCaughtUp => _caughtUp;

    /// <summary>Highest globalSeqNo dispatched in order, 0 before the first. The frontier a consumer measures its own
    /// recovery progress by.</summary>
    public long LastGlobalSeqNo => _lastGlobalSeqNo;

    /// <summary>memberId of the current leader per the last <c>LeadershipChanged</c> processed, or -1 until one is
    /// seen. A replica emits iff its own node is this leader.</summary>
    public int CurrentLeaderMemberId => _currentLeaderMemberId;

    /// <summary>A request is out and the Replayer has not answered it yet.</summary>
    public bool IsAwaitingReplay => _awaitingReplay;

    /// <summary>
    /// Mid-replay (cold-start walk or gap resume) or awaiting the Replayer's answer: a non-contiguous live-tap frame is
    /// expected while this holds (the tap runs ahead of the replay), so <see cref="OnFrame"/> drops it without
    /// treating it as a new gap.
    /// </summary>
    public bool IsRecovering => _replaySessionId >= 0 || _awaitingReplay || _restoring;

    /// <summary>Reading a snapshot's records, before anything after its cut is dispatched.</summary>
    public bool IsRestoring => _restoring;

    /// <summary>Why the snapshot cannot be restored, or null: this instance can no longer recover. Latched.</summary>
    public string RestoreFailure { get; private set; }

    /// <summary>The replay currently being ridden, or -1. The receiver attaches its image by this id.</summary>
    public long ReplaySessionId => _replaySessionId;

    /// <summary>The position the current replay is bounded to; the replay is done once the image reaches it.</summary>
    public long CatchUpPosition => _catchUpPosition;

    /// <summary>The id the next reply must carry to be acted on: see the correlation check in
    /// <see cref="OnControl"/>.</summary>
    public long RequestId => _requestId;

    /// <summary>The fromPosition of the current request: a gap's resume position, or
    /// <see cref="ReplayProtocol.FromStart"/>.</summary>
    public long RequestFromPosition => _requestFromPosition;

    /// <summary>Frames held ahead of the current hole.</summary>
    public int RetainedFrameCount => _retainFrameCount;

    /// <summary>
    /// Sends <c>ReplayRequest</c> and marks us awaiting the reply. The Replayer supersedes any in-flight replay for
    /// this clientId, so a resend is safe. The request id advances on every send, resends included: only a
    /// per-send id tells a stale reply from the live one on the shared control stream.
    /// </summary>
    private void RequestReplay(long fromPosition)
    {
        if (fromPosition == ReplayProtocol.FromStart)
        {
            _resumeAnchorGlobalSeqNo = 0; // a walk supersedes any resume in flight
        }
        _requestFromPosition = fromPosition;
        _awaitingReplay = true;
        _replaySessionId = -1;
        _actions.CloseReplay();
        _lastRequestMs = _actions.NowMs();
        ++_requestId;
        _actions.SendReplayRequest(_requestId, fromPosition);
    }

    /// <summary>
    /// Steady-state gap recovery: resume the active recording at the last dispatched frame instead of walking it, so
    /// a one-frame drop costs a one-frame replay. The resumed replay's first frame is checked against the
    /// anchor.
    /// </summary>
    private void RequestResume()
    {
        RequestReplay(_lastFramePosition);
        _resumeAnchorGlobalSeqNo = _lastGlobalSeqNo;
    }

    /// <summary>Opens the newest snapshot file below <paramref name="belowRound"/> and replays the recording from its
    /// cut, or walks the recording when there is none. A format this build does not read stops recovery.</summary>
    /// <param name="belowRound"><see cref="long.MaxValue"/> for the newest of all</param>
    private void ChooseBelow(long belowRound)
    {
        CloseReader();
        for (long round = _store.LatestRound(belowRound); round >= 0; round = _store.LatestRound(round))
        {
            _reader = _store.Open(round);
            if (_reader == null)
            {
                Logger.Log(Logger.CoreComponent.ReplayerStreamReceiver, Logger.Severity.Warn,
                           Logger.CoreEventCode.SnapshotStoreFailed, _actions.MemberId,
                           "round {0}'s snapshot file is cut short; trying an older snapshot", round);
                continue;
            }
            _snapshotRound = round;
            long formatVersion = (uint)_reader.FormatVersion;
            if (!_restoreHandler.SupportsFormatVersion(formatVersion))
            {
                FailRestore($"round {round} has formatVersion {formatVersion}, which this build does not read");
                return;
            }
            RequestCut();
            return;
        }
        RequestReplay(ReplayProtocol.FromStart);
    }

    /// <summary>Replays the recording from the chosen snapshot's cut; its first frame decides the restore.</summary>
    private void RequestCut()
    {
        _checkingCut = true;
        RequestReplay(_reader.CutPosition);
        _resumeAnchorGlobalSeqNo = _reader.CutGlobalSeqNo;
    }

    /// <summary>The replay from the cut opened: on the round's <c>SnapshotStarted</c>, the file's records are read;
    /// on anything else, or nothing, this recording is not the log the file was cut from, and an older file is
    /// tried.</summary>
    private void OnCutReplayed(bool matches)
    {
        _checkingCut = false;
        _replaySessionId = -1;
        _actions.CloseReplay();
        if (!matches)
        {
            Logger.Log(Logger.CoreComponent.ReplayerStreamReceiver, Logger.Severity.Warn,
                       Logger.CoreEventCode.SnapshotStoreFailed, _actions.MemberId,
                       "round {0}'s cut, globalSeqNo={1} at position {2}, is not in this node's recording; trying an " +
                           "older snapshot",
                       _snapshotRound, _reader.CutGlobalSeqNo, _reader.CutPosition);
            ChooseBelow(_snapshotRound);
            return;
        }
        _restoring = true;
        _restoreRecordIndex = 0;
        _restoredHeader = null;
        Logger.Info(Logger.CoreComponent.ReplayerStreamReceiver, _actions.MemberId,
                    "restoring round {0}, cut at globalSeqNo={1} position={2}", _snapshotRound, _reader.CutGlobalSeqNo,
                    _reader.CutPosition);
    }

    /// <summary>The next records of the restore, at most <see cref="MaxRestoreRecordsPerCycle"/>. They match the end
    /// the log holds by the file's trailer, so records that fail it are a damaged file.</summary>
    private void RestoreRecords()
    {
        _noProgressSinceMs = 0;
        for (int i = 0; i < MaxRestoreRecordsPerCycle; i++)
        {
            int length = _reader.Next(_restoreRecord);
            if (length == SnapshotStore.Reader.End)
            {
                CompleteRestore();
                return;
            }
            if (length == SnapshotStore.Reader.Damaged)
            {
                FailRestore($"round {_snapshotRound}'s snapshot file is damaged: its records do not match its " +
                            "trailer");
                return;
            }
            if (_restoreRecordIndex == 0)
            {
                _restoredHeader = SnapshotHeader.Decode(_restoreRecord, 0, length);
                if (_restoredHeader == null)
                {
                    FailRestore($"round {_snapshotRound} has a header of version " +
                                $"{SnapshotHeader.Version(_restoreRecord, 0, length)}, which this build does not read");
                    return;
                }
                _restoreHandler.OnSnapshotHeader(_restoredHeader);
            }
            else
            {
                _restoreHandler.OnSnapshotRecord(_restoreRecord, length, _restoreRecordIndex - 1);
            }
            _restoreRecordIndex++;
        }
    }

    private void CloseReader()
    {
        if (_reader != null)
        {
            _reader.Dispose();
            _reader = null;
        }
    }

    /// <summary>The snapshot is restored: the state is that after the cut, and so is the leadership its header
    /// carries. Resumes at the cut, which is dropped as already dispatched.</summary>
    private void CompleteRestore()
    {
        _restoring = false;
        _lastGlobalSeqNo = _reader.CutGlobalSeqNo;
        _lastFramePosition = _reader.CutPosition;
        CloseReader();
        _currentLeaderMemberId = _restoredHeader.LeaderMemberId;
        Logger.Info(Logger.CoreComponent.ReplayerStreamReceiver, _actions.MemberId,
                    "restored round {0}; resuming after globalSeqNo={1}", _snapshotRound, _lastGlobalSeqNo);
        if (_onLeadershipChanged != null && _restoredHeader.LeadershipTermId >= 0)
        {
            _onLeadershipChanged(_currentLeaderMemberId, _restoredHeader.LeadershipTermId, _lastGlobalSeqNo);
        }
        RequestResume();
    }

    /// <summary>Stops recovering for good: what is left cannot be restored, and nothing else may be dispatched in its
    /// place.</summary>
    private void FailRestore(string reason)
    {
        RestoreFailure = reason;
        _checkingCut = false;
        _restoring = false;
        CloseReader();
        _awaitingReplay = false;
        _replaySessionId = -1;
        _actions.CloseReplay();
        Logger.Fault(Logger.CoreComponent.ReplayerStreamReceiver, Logger.CoreEventCode.SnapshotRestoreFailed,
                     _actions.MemberId, "cannot restore a snapshot: {0}", reason);
    }

    /// <summary>Re-asks for whatever is in flight; a resume goes back through <see cref="RequestResume"/> for a fresh
    /// anchor.</summary>
    private void ReRequestCurrent()
    {
        if (_checkingCut)
        {
            RequestCut();
        }
        else if (_requestFromPosition == ReplayProtocol.FromStart)
        {
            RequestReplay(ReplayProtocol.FromStart); // same request verbatim, new request id
        }
        else
        {
            RequestResume();
        }
    }

    private void OnReplaying(long session, long replayCatchUpPosition)
    {
        _awaitingReplay = false;
        if (session == ReplayProtocol.NoReplayNeeded)
        {
            if (_checkingCut)
            {
                OnCutReplayed(false); // the cut is not a frame of this recording
                return;
            }
            if (_requestFromPosition != ReplayProtocol.FromStart)
            {
                Fail($"the Replayer refused a resume at position {_requestFromPosition} above " +
                     $"globalSeqNo={_lastGlobalSeqNo}");
                return;
            }
            _replaySessionId = -1; // the recording holds nothing yet: follow the live tap
            if (ReachedTip())
            {
                NotifyCaughtUp();
            }
            return;
        }
        _replaySessionId = session;
        _catchUpPosition = replayCatchUpPosition;
        _actions.OpenReplay(session);
        _lastReplayPosition = -1;
        _lastReplayProgressMs = _actions.NowMs();
    }

    /// <summary>Stops recovering: the recording does not hold what the request expected of it.</summary>
    private void Fail(string reason)
    {
        Fault = reason;
        _awaitingReplay = false;
        _replaySessionId = -1;
        _actions.CloseReplay();
        Logger.Fault(Logger.CoreComponent.ReplayerStreamReceiver, Logger.CoreEventCode.TapGap, _actions.MemberId,
                     "FATAL: {0} — this node's recording does not match the positions this client holds", reason);
    }

    /// <summary>An attached (or expected) replay stopped delivering: see the watchdog in
    /// <see cref="DoTimers"/>.</summary>
    private void OnReplayStalled()
    {
        Logger.Log(Logger.CoreComponent.ReplayerStreamReceiver, Logger.Severity.Warn, Logger.CoreEventCode.TapGap,
                   _actions.MemberId,
                   "replay session {0} made no progress for {1}ms at position {2} of catchUpPosition {3} — " +
                       "re-requesting it",
                   _replaySessionId, ReplayStallTimeoutMs, _lastReplayPosition, _catchUpPosition);
        ReRequestCurrent();
    }

    /// <summary>The replay reached its bound, or its image closed there. Closing the image frees the Replayer's
    /// slot.</summary>
    private void OnReplayReachedBound()
    {
        _actions.CloseReplay();
        _replaySessionId = -1;
        if (ReachedTip())
        {
            NotifyCaughtUp();
        }
    }

    /// <summary>Everything past the contiguity check; also the path a drained retained frame takes.</summary>
    private void DispatchFrame(IDirectBuffer buffer, int offset, int length, long globalSeqNo, long framePosition,
                               long receiveNs, bool fromReplay)
    {
        _noProgressSinceMs = 0;
        if (_recoveryStallReported)
        {
            _recoveryStallReported = false;
            _actions.RecoveryStalled(false);
        }
        _event.Wrap(buffer, offset, length);

        _lastGlobalSeqNo = globalSeqNo;
        _replayGapLogged = false;
        _lastFramePosition = framePosition;
        if (!fromReplay && !_caughtUp && !_retainOverflowed)
        {
            NotifyCaughtUp();
        }
        if (_event.IsSystem && _event.SystemEventType == SystemFrame.LeadershipChanged)
        {
            SbeBuffers.Wrap(_view, buffer, _event.PayloadOffset, Frame.LeadershipChanged.BlockLength);
            _leadershipChanged.WrapForDecode(_view, 0, Frame.LeadershipChanged.BlockLength,
                                             Frame.LeadershipChanged.SchemaVersion);
            _currentLeaderMemberId = _leadershipChanged.NewLeaderMemberId;
            if (_onLeadershipChanged != null)
            {
                _onLeadershipChanged(_currentLeaderMemberId, _leadershipChanged.LeadershipTermId, globalSeqNo);
                return;
            }
        }
        _event.Set(receiveNs, framePosition);
        _onSequenced(_event);
    }

    /// <summary>
    /// Keeps a live tap frame from beyond the current hole; one not kept is gone. Bounded and lossy past the bound
    /// (overflow falls back to a resume). A plain FIFO: one image delivers globalSeqNo in order, so only an exact
    /// redelivery needs checking.
    /// </summary>
    private void RetainFrame(long globalSeqNo, IDirectBuffer buffer, int offset, int length, long framePosition,
                             long receiveNs)
    {
        int recordSize = RetainBlock.RecordHeaderLength + length;
        if (_retainFrameCount >= MaxRetainedFrames || (_retainBytes + length) > MaxRetainedBytes ||
            recordSize > RetainBlock.Size)
        {
            _retainOverflowed = true;
            if (!_retainOverflowLogged)
            {
                _retainOverflowLogged = true;
                Logger.Log(Logger.CoreComponent.ReplayerStreamReceiver, Logger.Severity.Warn,
                           Logger.CoreEventCode.TapGap, _actions.MemberId,
                           "retained-frame buffer full at globalSeqNo={0} ({1} frames, {2} bytes) — dropping " +
                               "ahead-of-hole frames; recovery resumes at the last dispatched frame",
                           globalSeqNo, _retainFrameCount, _retainBytes);
            }
            return;
        }
        if (_retainFrameCount > 0 && globalSeqNo <= _retainTailGlobalSeqNo)
        {
            return; // already retained (the tap redelivered it): keep the first copy
        }
        if (_retained.Count == 0 || _retainTail.Used + recordSize > RetainBlock.Size)
        {
            _retainTail = AcquireBlock();
            _retained.Enqueue(_retainTail);
        }
        RetainBlock tail = _retainTail;
        tail.Buffer.PutLong(tail.Used, globalSeqNo);
        tail.Buffer.PutLong(tail.Used + sizeof(long), framePosition);
        tail.Buffer.PutLong(tail.Used + 2 * sizeof(long), receiveNs);
        tail.Buffer.PutInt(tail.Used + 3 * sizeof(long), length);
        tail.Buffer.PutBytes(tail.Used + RetainBlock.RecordHeaderLength, buffer, offset, length);
        tail.Used += recordSize;
        ++_retainFrameCount;
        _retainBytes += length;
        _retainTailGlobalSeqNo = globalSeqNo;
    }

    /// <summary>Dispatches every retained frame now contiguous, dropping any the replay covered, and recycles
    /// blocks.</summary>
    private void DrainRetained()
    {
        // Exits when the FIFO runs dry, or on a hole below the oldest retained frame.
        RetainBlock front = FrontRetained();
        while (front != null && front.Buffer.GetLong(_retainReadOffset) <= _lastGlobalSeqNo + 1)
        {
            long globalSeqNo = front.Buffer.GetLong(_retainReadOffset);
            long position = front.Buffer.GetLong(_retainReadOffset + sizeof(long));
            long receiveNs = front.Buffer.GetLong(_retainReadOffset + 2 * sizeof(long));
            int length = front.Buffer.GetInt(_retainReadOffset + 3 * sizeof(long));
            int payloadOffset = _retainReadOffset + RetainBlock.RecordHeaderLength;
            _retainReadOffset += RetainBlock.RecordHeaderLength + length;
            --_retainFrameCount;
            _retainBytes -= length;
            if (globalSeqNo > _lastGlobalSeqNo) // otherwise the replay already covered it
            {
                DispatchFrame(front.Buffer, payloadOffset, length, globalSeqNo, position, receiveNs, false);
            }
            front = FrontRetained();
        }
    }

    /// <summary>Block holding the oldest retained record, recycling any fully-read blocks first. Null when
    /// none.</summary>
    private RetainBlock FrontRetained()
    {
        while (_retained.TryPeek(out RetainBlock front))
        {
            if (_retainReadOffset < front.Used)
            {
                return front;
            }
            _retained.Dequeue();
            _retainPool.Push(front);
            _retainReadOffset = 0;
        }
        return null;
    }

    /// <summary>
    /// The replay side says we are at the tip. Whether we are is the retained FIFO's call: a retained frame may sit
    /// behind a hole the replay never reached, or an overflow dropped frames. Either way this resumes at the last
    /// dispatched frame now rather than waiting for a later tap frame to rediscover the hole.
    /// </summary>
    /// <returns>true only once nothing is left waiting and no overflow is latched</returns>
    private bool ReachedTip()
    {
        DrainRetained();
        if (_retained.Count == 0 && !_retainOverflowed)
        {
            return true;
        }
        EndOverflowEpisode();
        RequestResume();
        return false;
    }

    /// <summary>Ends an overflow episode where the covering resume is requested, not in
    /// <see cref="DrainRetained"/>, where the dispatch that cleared it would declare us caught up over dropped
    /// frames.</summary>
    private void EndOverflowEpisode()
    {
        _retainOverflowed = false;
        _retainOverflowLogged = false;
    }

    private void NotifyCaughtUp()
    {
        if (_caughtUp)
        {
            return; // idempotent: reached from the replay-tip, no-replay, and first-live-frame paths
        }
        _caughtUp = true;
        _onCaughtUp?.Invoke();
    }

    private RetainBlock AcquireBlock()
    {
        if (!_retainPool.TryPop(out RetainBlock block))
        {
            return new RetainBlock();
        }
        block.Used = 0;
        return block;
    }

    // Java's %b, which the stall report's readers grep for.
    private static string Text(bool value)
    {
        return value ? "true" : "false";
    }

    /// <summary>Fixed-size block of the retained FIFO; a record never spans two blocks, and the largest frame fits
    /// one.</summary>
    private sealed class RetainBlock
    {
        public const int Size = 16 * 1024;

        // globalSeqNo, position, receiveNs, length: the per-record framing within a block.
        public const int RecordHeaderLength = 3 * sizeof(long) + sizeof(int);

        // Pinned for as long as it is pooled, so on the pinned heap rather than fragmenting the young one.
        public readonly UnsafeBuffer Buffer = new UnsafeBuffer(GC.AllocateArray<byte>(Size, true));
        public int Used;
    }
}
