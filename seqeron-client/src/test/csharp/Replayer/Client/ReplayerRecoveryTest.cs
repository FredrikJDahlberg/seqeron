using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Org.Limitless.Seqeron.Helpers;
using Org.Limitless.Seqeron.Protocol;
using Org.Limitless.Seqeron.Util;
using Xunit;

namespace Org.Limitless.Seqeron.Replayer.Client;

/// <summary>
/// Unit tests for the walk / gap-recovery state machine. <see cref="ReplayerRecovery"/> holds no Aeron runtime and
/// no clock of its own, so the suite drives it directly: its transport is recorded through
/// <see cref="IReplayerRecoveryActions"/> and its clock is owned by the test.
/// <para>Case for case, in the same order, with <c>ReplayerRecoveryTest.java</c> and
/// <c>ReplayerRecoveryTest.cpp</c>, so the three files walk side by side: the ports are hand-kept and nothing but
/// this correspondence enforces it.</para>
/// </summary>
[Collection(LoggerCollection.Name)]
public class ReplayerRecoveryTest : IDisposable
{
    private const long ClientId = 4;
    private const long NoReplayNeeded = ReplayProtocol.NoReplayNeeded;
    private const long FromStart = ReplayProtocol.FromStart;

    // Arrival stamp; carried through to SequencedEvent, asserted on by no test here.
    private const long ReceiveNs = 0;

    // Stands in for the wall clock. The absolute value is arbitrary; only applied deltas matter.
    private const long ClockMs = 3 * 60 * 60 * 1000L;

    // Past ResendIntervalMs (500) and ReplayStallTimeoutMs (5000) alike, so one duty cycle fires whichever of the
    // two the state under test has armed.
    private const long PastEveryTimerMs = 6_000;

    // Past RecoveryProgressTimeoutMs, the convergence alarm's deadline.
    private const long PastDeadlineMs = ClockMs + 30_000;

    // Bigger than one RetainBlock, which is what makes RetainFrame refuse outright.
    private const int OversizedFrameLength = 32 * 1024;

    // The largest frame the queue carries (§12): a full payload behind the 44-byte sequenced envelope.
    private const int LargestFrameLength = 44 + FrameLayer.MaxPayloadLength;

    // The snapshot's cut; frame n starts at n * 1024, as everywhere here.
    private const long Cut = 10;

    // The snapshot every round here holds: the header, term 4 and leader 2, then one record.
    private static readonly byte[] SnapshotHeaderRecord = RestoreFrames.Header(4, 2);
    private static readonly byte[] SnapshotRecord = RestoreFrames.Record(7);

    private readonly TempDirectory _directory = new TempDirectory();
    private readonly UnsafeBuffer _wire = new UnsafeBuffer();
    private int _stores;
    private SnapshotStore _store;

    private readonly List<long> _dispatched = new List<long>();
    private readonly List<string> _restored = new List<string>();
    private readonly List<long> _caughtUpAt = new List<long>();
    private readonly List<Logger.LoggerEvent> _logged = new List<Logger.LoggerEvent>();
    private RecordingActions _actions;
    private ReplayerRecovery _receiver;

    // The transport recorded rather than performed: no publication, so nothing ever reaches the wire.
    private sealed class RecordingActions : IReplayerRecoveryActions
    {
        public long ClockMs = ReplayerRecoveryTest.ClockMs;
        public int RequestsSent;
        public readonly List<bool> StalledGauge = new List<bool>();

        public void SendReplayRequest(long requestId, long fromPosition)
        {
            ++RequestsSent;
        }

        public void OpenReplay(long replaySessionId)
        {
        }

        public void CloseReplay()
        {
        }

        public void RecoveryStalled(bool stalled)
        {
            StalledGauge.Add(stalled);
        }

        public int? MemberId => 0;

        long IReplayerRecoveryActions.ClientId => ClientId;

        public long NowMs()
        {
            return ClockMs;
        }
    }

    private sealed class RecordingRestoreHandler : ISnapshotRestoreHandler
    {
        private readonly List<string> _restored;

        public RecordingRestoreHandler(List<string> restored)
        {
            _restored = restored;
        }

        public bool SupportsFormatVersion(long formatVersion)
        {
            return formatVersion == 1;
        }

        public void OnSnapshotHeader(SnapshotHeader header)
        {
            _restored.Add($"header {header.LeadershipTermId} {header.LeaderMemberId}");
        }

        public void OnSnapshotRecord(IDirectBuffer record, int length, int recordIndex, long formatVersion)
        {
            Assert.Equal(1, formatVersion);
            _restored.Add($"{recordIndex} {record.GetLong(0)}");
        }
    }

    // Deliberately does NOT call Start(): several cases assert on the pre-request state (IsRecovering false, nothing
    // awaited). The ones that need the cold-start request ask for it themselves.
    public ReplayerRecoveryTest()
    {
        SetUp();
    }

    private void SetUp()
    {
        _dispatched.Clear();
        _restored.Clear();
        _caughtUpAt.Clear();
        _logged.Clear();
        _actions = new RecordingActions();
        _store = new SnapshotStore(Path.Combine(_directory.Path, "instance-" + _stores++));
        _receiver = new ReplayerRecovery(_actions, e => _dispatched.Add(e.ClusterSeqNo), null,
                                         () => _caughtUpAt.Add(_dispatched.Count));
    }

    public void Dispose()
    {
        Logger.Reset();
        _receiver.Close();
        _wire.Dispose();
        _directory.Dispose();
    }

    // ── baseline ──────────────────────────────────────────────────────────────────────────────────

    [Fact(DisplayName = "cold start walks the recording from its start")]
    public void ColdStartWalksFromTheStart()
    {
        _receiver.Start();

        Assert.True(_receiver.IsAwaitingReplay);
        Assert.Equal(FromStart, _receiver.RequestFromPosition);
    }

    [Fact(DisplayName = "a first frame at clusterSeqNo 1 is the baseline and puts the client live")]
    public void FirstFrameAtOneIsAccepted()
    {
        DeliverQueue(1);

        Assert.Equal(new List<long> { 1 }, _dispatched);
        Assert.True(_receiver.IsCaughtUp);
    }

    [Fact(DisplayName = "a first frame that is not clusterSeqNo 1 is fatal")]
    public void FirstFrameMustBeOne()
    {
        AttachReplay(11, 4096);

        Assert.Throws<InvalidOperationException>(() => DeliverReplay(7));
    }

    // Once the queue is dispatched mid-walk, a cold start races a live queue already carrying mid-stream clusterSeqNos
    // against a replay that has not delivered 1 yet. That queue frame is neither a valid baseline nor a fault: only
    // the walk may establish the baseline.
    [Fact(DisplayName = "the live queue may not establish the baseline while the cold-start walk is in flight")]
    public void LiveQueueMayNotEstablishTheBaselineMidWalk()
    {
        AnswerReplaying(7, 500);
        Assert.True(_receiver.IsRecovering);

        DeliverQueue(15); // the live tip, far ahead of a replay that has not started delivering

        Assert.True(_dispatched.Count == 0, "must not adopt the live queue's mid-stream baseline");
        Assert.False(_receiver.IsCaughtUp);

        DeliverReplay(1); // the walk supplies the real baseline

        Assert.Equal(new List<long> { 1 }, _dispatched);
    }

    // ── a queue's continuity: groupSeqNo, across jumps in clusterSeqNo (spec §14 row 12) ──────────────────────

    [Fact(DisplayName = "a first frame at groupSeqNo 1 is the baseline, whatever its clusterSeqNo")]
    public void FirstFrameAtGroupSeqNoOneIsAcceptedWhateverItsClusterSeqNo()
    {
        DeliverOnQueue(40, 1, false);

        Assert.Equal(new List<long> { 40 }, _dispatched);
        Assert.True(_receiver.IsCaughtUp);
    }

    [Fact(DisplayName = "a first frame that is not groupSeqNo 1 is fatal, even at clusterSeqNo 1")]
    public void FirstFrameMustBeGroupSeqNoOne()
    {
        AttachReplay(11, 4096);

        Assert.Throws<InvalidOperationException>(() => DeliverOnQueue(1, 2, true));
    }

    [Fact(DisplayName = "continuity follows groupSeqNo, so a jump in clusterSeqNo is no gap")]
    public void ContinuityFollowsGroupSeqNoAcrossJumpsInClusterSeqNo()
    {
        DeliverOnQueue(1, 1, false);
        DeliverOnQueue(5, 2, false);
        DeliverOnQueue(9, 3, false);

        Assert.Equal(new List<long> { 1, 5, 9 }, _dispatched);
        Assert.True(_receiver.IsCaughtUp);
        Assert.True(_actions.RequestsSent == 0, "nothing to recover");
    }

    [Fact(DisplayName = "a missing groupSeqNo is a gap, resumed at the last dispatched frame and drained in order")]
    public void AMissingGroupSeqNoIsAGap()
    {
        DeliverOnQueue(1, 1, false);
        DeliverOnQueue(9, 3, false);

        Assert.False(_receiver.IsCaughtUp);
        Assert.True(_receiver.RequestFromPosition == 1024, "resumes at frame 1, where it starts");
        Assert.Equal(1, _receiver.RetainedFrameCount);

        AttachReplay(21, 3 * 1024);
        DeliverOnQueue(1, 1, true); // the anchor, already dispatched
        DeliverOnQueue(5, 2, true);

        Assert.Equal(new List<long> { 1, 5, 9 }, _dispatched);
    }

    [Fact(DisplayName = "a frame whose clusterSeqNo is already passed is dropped")]
    public void AFrameWhoseClusterSeqNoIsPassedIsDropped()
    {
        DeliverOnQueue(1, 1, false);
        DeliverOnQueue(5, 2, false);
        DeliverOnQueue(5, 2, false);

        Assert.Equal(new List<long> { 1, 5 }, _dispatched);
    }

    // ── gap detection and the replay-to-live seam ─────────────────────────────────────────────────

    [Fact(DisplayName = "a mid-stream gap withholds the out-of-order frame and requests a replay")]
    public void MidStreamGapWithholdsTheOutOfOrderFrame()
    {
        DeliverQueue(1);
        DeliverQueue(2);
        Assert.Equal(2, _dispatched.Count);
        Assert.False(_receiver.IsAwaitingReplay);

        DeliverQueue(5); // skips 3, 4

        Assert.True(_dispatched.Count == 2, "the out-of-order frame itself must not be dispatched");
        Assert.True(_receiver.IsAwaitingReplay, "a gap must re-request a replay");
        Assert.True(_receiver.ReplaySessionId == -1, "no session yet, only a request, until the Replayer answers");
    }

    [Fact(DisplayName = "a queue gap reports a structured diagnostic event")]
    public void QueueGapReportsAStructuredDiagnosticEvent()
    {
        CaptureLogs();

        DeliverQueue(1);
        DeliverQueue(2);
        DeliverQueue(5); // skips 3, 4

        Logger.LoggerEvent loggerEvent = Assert.Single(_logged); // the one gap above, nothing from setup
        Assert.Equal(Logger.CoreComponent.ReplayerStreamReceiver, loggerEvent.Component);
        Assert.Equal(Logger.Severity.Warn, loggerEvent.Severity);
        Assert.Equal(Logger.CoreEventCode.QueueGap, loggerEvent.Code);
        Assert.Equal("queue gap: expected groupSeqNo=3 got 5 — resuming the recording at clusterSeqNo=2",
                     loggerEvent.Message);
    }

    [Fact(DisplayName = "a live queue frame ahead of an in-flight walk is retained, not treated as a gap")]
    public void QueueFrameMidWalkIsRetainedNotAGap()
    {
        AttachReplay(11, 4096);
        long requestIdBefore = _receiver.RequestId;

        DeliverQueue(9); // the queue runs ahead of the replay

        Assert.True(_dispatched.Count == 0, "no baseline yet: only the walk may establish one");
        Assert.Equal(1, _receiver.RetainedFrameCount);
        Assert.True(requestIdBefore == _receiver.RequestId, "an in-flight walk must not be superseded");
    }

    [Fact(DisplayName = "a queue gap while the walk is still in flight does not supersede it")]
    public void QueueGapMidWalkDoesNotSupersedeTheWalk()
    {
        AttachReplay(11, 4096);
        DeliverReplay(1); // baseline established, by the walk rather than by the queue
        long requestIdBefore = _receiver.RequestId;

        DeliverQueue(5); // 2..4 are not lost: they are frames this walk has not reached yet

        Assert.True(requestIdBefore == _receiver.RequestId, "an in-flight walk must run to completion");
        Assert.True(_receiver.RequestFromPosition == FromStart, "a mid-walk queue gap is expected, not a new gap");
        Assert.Equal(1, _receiver.RetainedFrameCount);
    }

    // The same rule one state later: not merely awaiting an answer but riding an assigned session. Superseding the
    // walk on each queue frame would restart it from the start forever under any sustained publish rate.
    [Fact(DisplayName = "a queue frame arriving during an assigned replay session does not supersede it")]
    public void QueueFrameAheadOfAnAssignedReplayDoesNotSupersedeIt()
    {
        CaptureLogs();
        DeliverQueue(1);
        DeliverQueue(5); // the genuine steady-state gap
        Assert.True(_receiver.IsAwaitingReplay);
        Assert.Single(_logged);

        AnswerReplaying(99, 500);
        Assert.Equal(99, _receiver.ReplaySessionId);
        Assert.False(_receiver.IsAwaitingReplay);
        Assert.True(_receiver.IsRecovering);

        DeliverQueue(12); // the queue running ahead, not a hole this walk will not cover

        Assert.True(_receiver.ReplaySessionId == 99, "the in-flight walk must run to completion");
        Assert.False(_receiver.IsAwaitingReplay);
        Assert.True(_receiver.RequestFromPosition != FromStart, "still the resume the gap asked for, not restarted");
        Assert.True(_dispatched.Count == 1, "the out-of-order frame itself is still withheld");
        Assert.True(_logged.Count == 1, "and it is not reported as a second gap");
    }

    // The replay->live seam: the queue is dispatched throughout a walk, so the frame at gseq == last + 1 lands the
    // instant the replay reaches it. Discarding queue frames while recovering ends every walk one guaranteed gap short
    // of live.
    [Fact(DisplayName = "the queue frame at the seam is dispatched while the walk is still in flight")]
    public void LiveQueueFrameAtTheSeamIsDispatchedMidWalk()
    {
        AnswerReplaying(7, 500);
        DeliverReplay(1);
        DeliverReplay(2);
        Assert.Equal(2, _dispatched.Count);
        Assert.False(_receiver.IsCaughtUp, "still riding replayed history");
        Assert.True(_receiver.IsRecovering);

        DeliverQueue(3); // the seam: first queue frame contiguous with what the replay delivered

        Assert.True(_dispatched.Count == 3, "the seam frame must be dispatched, not discarded as mid-walk noise");
        Assert.True(_receiver.IsCaughtUp, "a contiguous frame off the live queue means we are following live");
    }

    [Fact(DisplayName = "a steady-state queue gap clears caught-up and resumes at the last dispatched frame")]
    public void SteadyStateGapResumesAtLastFrame()
    {
        GoLive();
        DeliverQueueAt(1, 1024);
        DeliverQueueAt(2, 2048);
        Assert.True(_receiver.IsCaughtUp);

        DeliverQueueAt(5, 8192); // 3 and 4 dropped

        Assert.False(_receiver.IsCaughtUp, "consumers gate real decisions on this: it must not stay latched");
        Assert.True(_receiver.IsAwaitingReplay);
        Assert.True(_receiver.RequestFromPosition != FromStart,
                    "a gap resumes; it does not re-walk from a stale index");
        Assert.True(_receiver.RequestFromPosition == 2048, "resume at the frame last dispatched, not at 0");
        Assert.True(_receiver.RetainedFrameCount == 1, "the ahead-of-hole frame is kept, not dropped");
    }

    [Fact(DisplayName = "replayed frames already seen off the live queue are de-duped")]
    public void DuplicateAndStaleReplayFramesAreDropped()
    {
        DeliverQueue(1);
        DeliverQueue(2);
        Assert.Equal(2, _dispatched.Count);

        DeliverReplay(1);
        DeliverReplay(2);
        Assert.True(_dispatched.Count == 2, "replayed frames already delivered off the live queue must be de-duped");

        DeliverReplay(3);
        Assert.Equal(3, _dispatched.Count);
    }

    [Fact(DisplayName = "replayed history dispatches in order and de-dupes a redelivered frame")]
    public void DispatchesInOrderAndDeDupes()
    {
        AttachReplay(11, 4096);

        DeliverReplay(1);
        DeliverReplay(2);
        DeliverReplay(2); // redelivered
        DeliverReplay(3);

        Assert.Equal(new List<long> { 1, 2, 3 }, _dispatched);
        Assert.Equal(3, _receiver.LastClusterSeqNo);
    }

    // ── the Replayer's control-stream replies, and request/reply correlation ──────────────────────

    [Fact(DisplayName = "nothing to replay from the start marks the client caught up")]
    public void NothingToReplayFromTheStartMarksCaughtUp()
    {
        AnswerReplaying(NoReplayNeeded, 0);

        Assert.True(_receiver.IsCaughtUp);
        Assert.False(_receiver.IsAwaitingReplay);
    }

    [Fact(DisplayName = "a Replaying carrying a session arms the replay without marking the client caught up")]
    public void ReplayingWithSessionArmsReplayWithoutMarkingCaughtUp()
    {
        AnswerReplaying(42, 1000);

        Assert.False(_receiver.IsCaughtUp, "a session id means there IS history to replay first");
        Assert.False(_receiver.IsAwaitingReplay, "OnControl always clears awaiting once answered");
        Assert.Equal(42, _receiver.ReplaySessionId);
    }

    [Fact(DisplayName = "a reply addressed to another replica on the shared control stream is ignored")]
    public void ReplayingForAnotherClientIdIsIgnored()
    {
        Control(ControlMessages.Replaying(ClientId + 1, _receiver.RequestId, 42, 1000));

        Assert.True(_receiver.ReplaySessionId == -1, "a reply for a different replica must not be applied");
        Assert.False(_receiver.IsCaughtUp);
    }

    [Fact(DisplayName = "a walk reaching its bound catches up")]
    public void AWalkReachingItsBoundCatchesUp()
    {
        AnswerReplaying(7, 500);
        Assert.Equal(7, _receiver.ReplaySessionId);

        CompleteReplay();

        Assert.True(_receiver.IsCaughtUp, "the recording held the whole log when the request was served");
        Assert.False(_receiver.IsAwaitingReplay, "there is nothing further to ask for");
        Assert.Equal(-1, _receiver.ReplaySessionId);
    }

    // clientId alone cannot identify WHICH request a reply answers. A resend makes the Replayer stop the in-flight
    // session and start a new one, so both replies sit in order on the one shared control publication and the stale
    // one is always processed first.
    [Fact(DisplayName = "a reply carrying a superseded requestId is ignored")]
    public void StaleReplyIsIgnored()
    {
        _receiver.Start();
        long staleRequestId = _receiver.RequestId;

        Control(ControlMessages.Replaying(ClientId, staleRequestId - 1, 11, 4096));

        Assert.True(_receiver.IsAwaitingReplay, "a stale reply must not stop the resend timer");
        Assert.Equal(-1, _receiver.ReplaySessionId);
        Assert.NotEqual(11, _receiver.ReplaySessionId);
    }

    [Fact(DisplayName = "the reply to the current request is still accepted after a stale one")]
    public void CurrentReplyIsStillAcceptedAfterAStaleOne()
    {
        long stale = _receiver.RequestId;
        _receiver.Start(); // a request, and with it a new requestId

        Control(ControlMessages.Replaying(ClientId, stale, 42, 900));
        AnswerReplaying(43, 1000);

        Assert.True(_receiver.ReplaySessionId == 43, "the reply to the current request must be applied");
        Assert.False(_receiver.IsAwaitingReplay);
    }

    // A stale ReplayPending must not reset the request clock either: that is what paces the resend, so honouring a
    // superseded request's "wait" defers the retry the client is relying on.
    [Fact(DisplayName = "a ReplayPending for a superseded request does not push out the current resend deadline")]
    public void ReplayPendingForASupersededRequestIsIgnored()
    {
        long stale = _receiver.RequestId;
        _receiver.Start();
        int sends = _actions.RequestsSent;

        _actions.ClockMs += 300; // still inside the current request's resend interval
        DeliverPending(stale);
        _actions.ClockMs += 300; // ... which has now elapsed, unless the stale "wait" pushed it out
        _receiver.DoTimers(false);

        Assert.True(sends + 1 == _actions.RequestsSent,
                    "a superseded request's ReplayPending must not defer the current request's resend");
    }

    // ── a closed image, a completed replay, and the stall watchdog ────────────────────────────────

    [Fact(DisplayName = "a replay image closing at its bound completes the replay; short of it re-requests")]
    public void ClosedImageIsCompletionOnlyAtTheBound()
    {
        AnswerReplaying(11, 4096);
        _receiver.OnReplayImageClosed(4000);
        Assert.True(_receiver.IsAwaitingReplay, "short of the bound: the replay is re-requested");
        Assert.Equal(FromStart, _receiver.RequestFromPosition);

        AnswerReplaying(12, 4096);
        _receiver.OnReplayImageClosed(4096);
        Assert.True(_receiver.IsCaughtUp, "at the bound: the replay is done");
        Assert.False(_receiver.IsAwaitingReplay);
    }

    // Once Replaying arrives the resend timer is disarmed, and a bounded replay of an active recording never closes
    // its image, so an image that simply stops advancing leaves the client waiting on it forever with nothing
    // retrying.
    [Fact(DisplayName = "a replay that stops advancing re-requests it")]
    public void ReplayThatStopsAdvancingReRequestsIt()
    {
        CaptureLogs();
        AnswerReplaying(7, 500);
        long requestId = _receiver.RequestId;
        Assert.False(_receiver.IsAwaitingReplay, "a session was assigned, so nothing else is retrying");

        AdvancePastTimers();

        Assert.True(_receiver.RequestFromPosition == FromStart, "the walk is asked for again, from the start");
        Assert.True(_receiver.IsAwaitingReplay, "a stalled replay is not taken for a complete one");
        Assert.Equal(-1, _receiver.ReplaySessionId);
        Assert.True(requestId != _receiver.RequestId,
                    "re-requesting is a new request, so the reply to the stalled one is recognisably stale");
        Assert.True(_logged.Count == 1, "a stalled replay is reported, not silently absorbed");
    }

    // Session id 0 is an ordinary archive replaySessionId; -1 is the only value that means "no replay". Testing >= 1
    // skips the stall watchdog for it once it stops delivering.
    [Fact(DisplayName = "replay session 0 is watchdogged like any other")]
    public void ReplaySessionZeroIsWatchdoggedLikeAnyOther()
    {
        AnswerReplaying(0, 500);
        Assert.True(_receiver.ReplaySessionId == 0,
                    "session 0 is a replay this client is riding, not the absence of one");

        AdvancePastTimers();

        Assert.True(_receiver.IsAwaitingReplay, "its stall watchdog must re-request it");
        Assert.Equal(-1, _receiver.ReplaySessionId);
    }

    // ── ReplayPending ─────────────────────────────────────────────────────────────────────────────

    [Fact(DisplayName = "ReplayPending holds at the gap without assigning a session")]
    public void ReplayPendingHoldsAtTheGapWithoutAssigningASession()
    {
        DeliverQueue(1);
        DeliverQueue(5); // gap -> awaiting a replay
        Assert.True(_receiver.IsAwaitingReplay);

        DeliverPending(_receiver.RequestId);

        Assert.True(_receiver.IsAwaitingReplay, "no free Replayer slot -> keep holding at the gap");
        Assert.Equal(-1, _receiver.ReplaySessionId);
        Assert.True(_dispatched.Count == 1, "must not advance past the hole while pending");
    }

    // ── the resend timer ──────────────────────────────────────────────────────────────────────────

    [Fact(DisplayName = "a request the Replayer never answered is re-sent once the interval elapses")]
    public void StuckAwaitingReplayResendsAfterTheIntervalElapses()
    {
        DeliverQueue(1);
        DeliverQueue(5); // gap -> arms the resend timer
        Assert.True(_receiver.IsAwaitingReplay);
        int sends = _actions.RequestsSent;

        _actions.ClockMs += 300; // inside ResendIntervalMs (500)
        _receiver.DoTimers(false);
        Assert.True(sends == _actions.RequestsSent, "must not resend before ResendIntervalMs elapses");
        Assert.True(_receiver.IsAwaitingReplay);

        _actions.ClockMs += 300; // 600ms since the request: past it
        _receiver.DoTimers(false);

        Assert.True(sends + 1 == _actions.RequestsSent, "the duty cycle must re-send rather than give up");
        Assert.True(_receiver.IsAwaitingReplay, "still stuck at the gap: a resend, not a new state");
        Assert.Equal(-1, _receiver.ReplaySessionId);
        Assert.True(_receiver.RequestFromPosition != FromStart, "a resend repeats the same request verbatim");
    }

    // Every send advances the request id and OnControl acts only on a reply carrying the CURRENT id, so a per-poll
    // retry runs the counter away and every reply that arrives is discarded as stale, leaving the client awaiting a
    // replay that can never correlate, hence never caught up.
    [Fact(DisplayName = "a request that never went out is not re-sent once per poll")]
    public void AnUnsentRequestIsNotReSentOncePerPoll()
    {
        DeliverQueue(1);
        DeliverQueue(5); // gap -> a request that never goes out, there being no publication here
        long before = _receiver.RequestId;

        for (int i = 0; i < 50; i++)
        {
            _receiver.DoTimers(false); // duty cycles, no clock advance
        }

        Assert.True(_receiver.IsAwaitingReplay, "still waiting: nothing answered it");
        Assert.True(_receiver.RequestId - before <= 1,
                    "the resend timer paces this; one id per poll outruns every reply in flight");
    }

    // ── IsRecovering and IsCaughtUp ───────────────────────────────────────────────────────────────

    // IsRecovering is what separates "the queue is ahead of my replay" from "there is a hole": narrowing it means a
    // walk supersedes itself on its own in-flight frames, widening it means a real gap goes unreported. This walks
    // every transition it must track.
    [Fact(DisplayName = "the recovering flag tracks the walk and awaiting-replay states")]
    public void RecoveringFlagTracksWalkAndAwaitingReplayState()
    {
        Assert.False(_receiver.IsRecovering, "nothing in flight yet");

        DeliverQueue(1);
        DeliverQueue(5); // skips 2..4 -> gap, re-requests a replay
        Assert.True(_receiver.IsRecovering, "awaiting the Replayer's answer is already mid-recovery");

        AnswerReplaying(7, 500);
        Assert.True(_receiver.IsRecovering, "a session id means history remains to replay");

        DeliverReplay(1); // opens on the anchored frame -> anchor consumed, frame deduped
        DeliverReplay(2);
        DeliverReplay(3);
        DeliverReplay(4); // closes the hole, draining the retained frame 5
        CompleteReplay();
        Assert.False(_receiver.IsRecovering, "a resume ends at its bound");

        // A resume whose bound leaves a retained frame stranded behind a hole resumes again.
        DeliverQueue(9); // another gap -> another resume, anchored on frame 5
        AnswerReplaying(8, 500);
        DeliverReplay(5);
        DeliverReplay(6);
        CompleteReplay(); // 7 and 8 not covered: 9 is still stranded
        Assert.True(_receiver.IsRecovering, "awaiting the next resume");

        AnswerReplaying(9, 500);
        DeliverReplay(6); // anchored on frame 6, the last dispatched
        DeliverReplay(7);
        DeliverReplay(8); // dispatching 8 drains the retained 9 straight over
        Assert.True(_receiver.IsRecovering, "mid-resume until the bound");
        CompleteReplay();
        Assert.False(_receiver.IsRecovering, "that resume ends at its bound too");
    }

    // IsCaughtUp is a state, not a latch: consumers gate real decisions on it, and a gateway's queue-stall watchdog
    // measures silence in DISPATCHED frames. A re-walk dispatches nothing until the replay passes the hole, so
    // leaving it latched makes a gateway diagnose its own recovery as a stalled sequencer.
    [Fact(DisplayName = "a queue gap revokes caught-up until the stream goes contiguous again")]
    public void QueueGapRevokesCaughtUpUntilTheStreamGoesContiguousAgain()
    {
        DeliverQueue(1);
        DeliverQueue(2);
        Assert.True(_receiver.IsCaughtUp);
        Assert.Single(_caughtUpAt);

        DeliverQueue(5); // skips 3, 4 -> gap (and 5 is retained, not dropped)
        Assert.False(_receiver.IsCaughtUp, "a hole means we are demonstrably not following live");

        DeliverReplay(2); // a resume opens on the frame it anchored at: already delivered, deduped
        DeliverReplay(3);
        Assert.False(_receiver.IsCaughtUp, "4 is still missing: the hole is not closed yet");
        Assert.Single(_caughtUpAt);

        DeliverReplay(4); // closes the hole; the retained queue frame 5 then hands straight over

        Assert.True(_receiver.IsCaughtUp, "the retained frame closes the seam with no further round trip");
        Assert.True(_caughtUpAt.Count == 2, "consumers must be able to re-arm on re-convergence");
    }

    // ── the retained-ahead FIFO ───────────────────────────────────────────────────────────────────

    [Fact(DisplayName = "retained queue frames hand straight over when the replay reaches them")]
    public void RetainedFramesCloseTheSeam()
    {
        AttachReplay(11, 4096);
        DeliverQueue(3);
        DeliverQueue(4);

        DeliverReplay(1);
        DeliverReplay(2);

        Assert.Equal(new List<long> { 1, 2, 3, 4 }, _dispatched); // the seam closes with no residual
        Assert.Equal(0, _receiver.RetainedFrameCount);
    }

    [Fact(DisplayName = "frames beyond the hole are retained and delivered exactly once when it closes")]
    public void FramesBeyondTheHoleAreRetainedAndDeliveredOnceItCloses()
    {
        DeliverQueue(1);
        DeliverQueue(4); // gap -> retained
        DeliverQueue(5); // still ahead of the hole -> retained
        DeliverQueue(6);
        Assert.Equal(new List<long> { 1 }, _dispatched); // nothing past the hole may be dispatched early

        DeliverReplay(1); // a resume opens on the frame it anchored at: already delivered, deduped
        DeliverReplay(2);
        Assert.Equal(new List<long> { 1, 2 }, _dispatched); // 3 is still missing

        DeliverReplay(3); // closes the hole -> 4, 5, 6 drain behind it

        Assert.Equal(new List<long> { 1, 2, 3, 4, 5, 6 }, _dispatched); // in order, exactly once
        Assert.True(_receiver.IsCaughtUp);
    }

    [Fact(DisplayName = "a frame of the largest payload ahead of the hole is retained, not an overflow")]
    public void LargestFrameAheadOfTheHoleIsRetained()
    {
        DeliverQueue(1);
        DeliverQueuePadded(3, LargestFrameLength); // gap -> retained
        Assert.Equal(1, _receiver.RetainedFrameCount);

        DeliverReplay(1); // a resume opens on the frame it anchored at: already delivered, deduped
        DeliverReplay(2); // closes the hole -> 3 drains behind it

        Assert.Equal(new List<long> { 1, 2, 3 }, _dispatched);
        Assert.True(_receiver.IsCaughtUp, "an overflow would have forced a re-walk");
    }

    // The ranges legitimately overlap, since a re-walk restarts from the start while the queue keeps arriving.
    [Fact(DisplayName = "retained frames the replay has meanwhile covered are not redelivered")]
    public void RetainedFramesAlreadyCoveredByTheReplayAreNotRedelivered()
    {
        DeliverQueue(1);
        DeliverQueue(3); // gap -> retained
        DeliverQueue(4); // retained

        DeliverReplay(1); // a resume opens on the frame it anchored at: already delivered, deduped
        DeliverReplay(2);
        DeliverReplay(3); // the replay covers a frame the queue already retained
        DeliverReplay(4);

        Assert.Equal(new List<long> { 1, 2, 3, 4 }, _dispatched); // each clusterSeqNo dispatched exactly once
    }

    // A hole in REPLAYED history is the one invariant violation that produced no log and no counter: the frame is
    // dropped, the walk rides on, and recovery quietly never converges. Nothing unsafe follows (contiguity still
    // holds), so this reports rather than aborts.
    [Fact(DisplayName = "a gap in replayed history is reported once per episode")]
    public void GapInReplayedHistoryIsReportedOncePerEpisode()
    {
        DeliverReplay(1);
        Assert.Single(_dispatched);

        CaptureLogs();    // installed after the baseline, so it captures only the hole below
        DeliverReplay(7); // the recording does not cover 2..6

        Assert.True(_dispatched.Count == 1, "the frame past the hole must not be dispatched");
        Assert.Single(_logged);
        Assert.Equal(Logger.Severity.Warn, _logged[0].Severity);
        Assert.Equal(Logger.CoreEventCode.QueueGap, _logged[0].Code);

        DeliverReplay(8);
        Assert.True(_logged.Count == 1, "report the episode, not every frame of a walk retrying the same recording");

        // A later, distinct episode must be reported again rather than swallowed by the latch.
        DeliverReplay(2);
        Assert.Equal(2, _dispatched.Count);
        DeliverReplay(9);
        Assert.True(_logged.Count == 2, "the latch clears once history goes contiguous again");
    }

    [Fact(DisplayName = "dropped queue frames are reported once per episode")]
    public void DroppedQueueFramesAreReportedOncePerEpisode()
    {
        DeliverQueue(1);
        DeliverQueue(3); // gap -> resume anchored on frame 1; 3 is retained

        CaptureLogs(); // installed after the setup gap, so it captures only the overflow lines
        DeliverQueueTooBigToRetain(4);
        DeliverQueueTooBigToRetain(5);
        Assert.True(OverflowReports() == 1, "one episode is one line, not one per dropped frame");

        // The resume is what covers the dropped frames, so the episode ends when it is requested.
        AnswerReplaying(7, 500);
        DeliverReplay(1);
        DeliverReplay(2);
        CompleteReplay();
        Assert.True(_receiver.RequestFromPosition == 3072, "at the retained frame 3, drained behind 2");

        DeliverQueueTooBigToRetain(9);

        Assert.True(OverflowReports() == 2, "a later overflow is a distinct episode and names itself");
    }

    // ── the resume anchor ─────────────────────────────────────────────────────────────────────────

    // Positions follow from the frames and the recording changes only with the node's driver, so a resume opening
    // anywhere but its anchor means the recording is not what this client's positions say.
    [Fact(DisplayName = "a resume replay that opens at the wrong frame is fatal")]
    public void ResumeAnchorMismatchIsFatal()
    {
        GoLive();
        DeliverQueueAt(1, 1024);
        DeliverQueueAt(2, 2048);
        DeliverQueueAt(5, 8192); // gap -> resume anchored on clusterSeqNo 2
        AttachReplay(12, 16384);

        DeliverReplay(4); // not the anchor

        Assert.Contains("expected 2", _receiver.Fault);
        Assert.False(_receiver.IsAwaitingReplay, "nothing more is asked for");
        Assert.Equal(2, _dispatched.Count);
    }

    // The anchor is consumed by the first frame of ONE replay episode: a retry of that same logical resume starts a
    // NEW episode and must re-arm the check, or the retried replay's first frame rides with nothing to validate
    // against and a wrong frame is dispatched merely because it looks contiguous.
    [Fact(DisplayName = "a resume whose image closes short of its bound re-arms the anchor check")]
    public void ReplayImageClosingShortOfTheBoundOnAResumeReArmsTheAnchorCheck()
    {
        DeliverQueue(1);
        DeliverQueue(5); // gap -> resume anchored on frame 1
        AnswerReplaying(7, 500);
        DeliverReplay(1); // opens on the anchored frame -> anchor consumed, frame deduped
        Assert.Single(_dispatched);

        _receiver.OnReplayImageClosed(300); // stopped under us before frame 2 arrived
        Assert.True(_receiver.RequestFromPosition != FromStart, "still a resume retry, not a walk step");

        AnswerReplaying(8, 500);
        // The retried resume's first frame is NOT the frame it was anchored on, but it looks contiguous.
        DeliverReplay(2);

        Assert.True(_dispatched.Count == 1, "must not accept an unanchored frame merely because it looks contiguous");
        Assert.True(_receiver.Fault != null, "the mismatch is fatal");
        Assert.Equal(-1, _receiver.ReplaySessionId);
        Assert.False(_receiver.IsCaughtUp);
    }

    [Fact(DisplayName = "a resume that stalls re-arms the anchor check")]
    public void ReplayStallOnAResumeReArmsTheAnchorCheck()
    {
        DeliverQueue(1);
        DeliverQueue(5); // gap -> resume anchored on frame 1
        AnswerReplaying(7, 500);
        DeliverReplay(1);
        Assert.Single(_dispatched);

        AdvancePastTimers(); // stopped advancing before frame 2 arrived
        Assert.True(_receiver.RequestFromPosition != FromStart, "still a resume retry, not a walk step");

        AnswerReplaying(8, 500);
        DeliverReplay(2); // looks contiguous but is not the anchored frame

        Assert.True(_dispatched.Count == 1, "must not accept an unanchored frame merely because it looks contiguous");
        Assert.True(_receiver.Fault != null, "the mismatch is fatal");
        Assert.Equal(-1, _receiver.ReplaySessionId);
        Assert.False(_receiver.IsCaughtUp);
    }

    // For a walk "nothing to replay" means caught up. For a resume the Replayer answers it only when it refuses the
    // position; a recording that has not reached it yet is answered ReplayPending instead.
    [Fact(DisplayName = "NoReplayNeeded for a resume is fatal")]
    public void NoReplayNeededForAResumeIsFatal()
    {
        DeliverQueue(1);
        DeliverQueue(2);
        Assert.True(_receiver.IsCaughtUp);
        DeliverQueue(6); // gap -> resume
        Assert.NotEqual(FromStart, _receiver.RequestFromPosition);

        AnswerReplaying(NoReplayNeeded, 0);

        Assert.Contains("refused a resume", _receiver.Fault);
        Assert.False(_receiver.IsAwaitingReplay);
        Assert.False(_receiver.IsCaughtUp, "the hole above clusterSeqNo=2 is still open");
    }

    // A resume ends at the bound it was given, so it must declare itself caught up and close its image, which frees
    // the Replayer's slot.
    [Fact(DisplayName = "a resume reaching its bound catches up and requests nothing more")]
    public void ResumeReachingItsBoundCatchesUpAndRequestsNothingMore()
    {
        DeliverQueue(1);
        Assert.Single(_caughtUpAt);
        DeliverQueue(5); // gap -> resume
        Assert.False(_receiver.IsCaughtUp);
        AnswerReplaying(7, 500);

        DeliverReplay(1); // opens on the anchored frame -> anchor consumed, frame deduped
        DeliverReplay(2);
        DeliverReplay(3);
        DeliverReplay(4); // closes the hole, draining the retained frame 5
        CompleteReplay();

        Assert.False(_receiver.IsAwaitingReplay, "no follow-up request");
        Assert.Equal(-1, _receiver.ReplaySessionId);
        Assert.True(_receiver.IsCaughtUp, "we hold everything the recording had when the request was served");
        Assert.True(_caughtUpAt.Count == 2, "re-fired, so consumers re-arm on re-convergence");
    }

    // A resume can reach its bound while a retained-ahead frame still sits behind a hole the replay never covered.
    // Declaring caught up on the bound alone is silently wrong: consumers gate real decisions on it and nothing would
    // rediscover the hole until some later, unrelated queue frame exposed it.
    [Fact(DisplayName = "a resume reaching its bound with a retained hole still open resumes again")]
    public void ResumeReachingItsBoundWithARetainedHoleStillOpenResumesAgain()
    {
        DeliverQueue(1);
        Assert.Single(_caughtUpAt);
        DeliverQueue(5); // gap -> resume anchored on frame 1; 5 is retained ahead of the hole
        AnswerReplaying(7, 500);

        DeliverReplay(1); // opens on the anchored frame -> anchor consumed, frame deduped
        DeliverReplay(2); // the bound is reached having covered only up to 2: 3, 4 unfilled

        CompleteReplay();

        Assert.False(_receiver.IsCaughtUp, "must not declare caught up with a retained frame behind a hole");
        Assert.True(_caughtUpAt.Count == 1, "no false re-convergence notification");
        Assert.True(_receiver.RequestFromPosition == 2048, "resumes at the last dispatched frame, not the start");
        Assert.True(_receiver.IsAwaitingReplay);
        Assert.Equal(-1, _receiver.ReplaySessionId);
    }

    // The overflow half of the same guard. When the FIFO had to DROP queue frames the frontier is short by frames that
    // are gone from the queue for good, and every retained frame draining cleanly says nothing about them.
    [Fact(DisplayName = "a resume reaching its bound after dropped queue frames resumes again")]
    public void ResumeReachingItsBoundAfterDroppedQueueFramesResumesAgain()
    {
        DeliverQueue(1);
        Assert.Single(_caughtUpAt);
        DeliverQueue(3);               // gap -> resume anchored on frame 1; 3 is retained
        DeliverQueueTooBigToRetain(4); // ahead of the hole too, but dropped rather than retained
        AnswerReplaying(7, 500);

        DeliverReplay(1); // opens on the anchored frame -> anchor consumed, frame deduped
        DeliverReplay(2); // closes the hole the resume was asked to cover, draining retained frame 3

        CompleteReplay();

        Assert.False(_receiver.IsCaughtUp, "frame 4 was dropped: the drained frontier is not the real one");
        Assert.True(_caughtUpAt.Count == 1, "no false re-convergence notification");
        Assert.True(_receiver.RequestFromPosition == 3072,
                    "resumes now rather than leaving the hole for a later queue gap");
        Assert.True(_receiver.IsAwaitingReplay);
    }

    // ── the end of a walk ─────────────────────────────────────────────────────────────────────────
    // A walk ends at its bound, and reaching it is not by itself permission to declare caught up.

    [Fact(DisplayName = "frames still retained when a walk reaches its bound force a resume rather than caught-up")]
    public void RetainedResidualForcesAResume()
    {
        AttachReplay(11, 4096);
        DeliverQueue(5); // a hole below it that this walk never reached
        DeliverReplay(1);
        Assert.Equal(1, _receiver.RetainedFrameCount);

        CompleteReplay();

        Assert.False(_receiver.IsCaughtUp, "declaring caught up here would close the hole by fiat");
        Assert.True(_receiver.RequestFromPosition == 1024, "resumes at the last dispatched frame");
        Assert.True(_receiver.IsAwaitingReplay);
    }

    // The negative control for the guard above: a walk whose retained frames all drained must finish.
    [Fact(DisplayName = "a walk reaching its bound with every retained frame drained catches up")]
    public void AWalkWithEveryRetainedFrameDrainedCatchesUp()
    {
        AnswerReplaying(7, 500);
        DeliverReplay(1);
        DeliverQueue(3);  // ahead of the hole at 2 -> retained
        DeliverReplay(2); // closes it, so 3 drains straight over

        CompleteReplay();

        Assert.True(_receiver.IsCaughtUp, "nothing is outstanding: the guard must not fire here");
        Assert.False(_receiver.IsAwaitingReplay, "not a spurious resume");
    }

    // The same guard driven through to convergence: the resume must both happen AND be able to finish. Clearing the
    // overflow on drain forgets the drop and catches up over the hole; never clearing it leaves a client that can
    // never declare itself caught up again.
    [Fact(DisplayName = "a walk reaching its bound after dropped queue frames resumes, then catches up")]
    public void AWalkAfterDroppedQueueFramesResumesThenCatchesUp()
    {
        AnswerReplaying(7, 500);
        DeliverReplay(1);
        DeliverReplay(2);
        DeliverQueueTooBigToRetain(4); // the queue runs ahead of the walk, and this one is dropped

        CompleteReplay();

        Assert.False(_receiver.IsCaughtUp, "the walk reached its bound, but a queue frame above it was dropped");
        Assert.True(_caughtUpAt.Count == 0, "no false convergence notification: this opens the accept gate");
        Assert.True(_receiver.RequestFromPosition == 2048, "resumes rather than trusting the bound");

        // The resume replays what the drop lost, and this time nothing is dropped.
        AnswerReplaying(8, 900);
        DeliverReplay(2); // the anchor
        DeliverReplay(3);
        DeliverReplay(4);
        CompleteReplay();

        Assert.True(_receiver.IsCaughtUp, "a resume that dropped nothing must be able to finish");
        Assert.Single(_caughtUpAt);
    }

    // ── the convergence alarm ─────────────────────────────────────────────────────────────────────
    // Measured by progress, not elapsed time: a cold start has no time bound. Locked here: an in-order dispatch
    // counts as progress, catching up without dispatching does too, the report carries the state telling the causes
    // apart, and each episode reports and clears exactly once.

    [Fact(DisplayName = "recovery delivering nothing is reported once per episode")]
    public void RecoveryDeliveringNothingIsReportedOncePerEpisode()
    {
        CaptureLogs();

        Assert.False(CheckProgressAt(ClockMs), "the first observation only anchors the clock");
        Assert.True(CheckProgressAt(PastDeadlineMs));

        Assert.Single(_logged);
        Assert.Equal(Logger.CoreComponent.ReplayerStreamReceiver, _logged[0].Component);
        Assert.Equal(Logger.Severity.Fault, _logged[0].Severity);
        Assert.Equal(Logger.CoreEventCode.RecoveryStalled, _logged[0].Code);

        // The condition persists for as long as the archive is broken; the caller logs, so it must not repeat every
        // duty cycle.
        Assert.False(CheckProgressAt(PastDeadlineMs + 60_000));
        Assert.Single(_logged);
    }

    [Fact(DisplayName = "a walk that is still delivering is never reported, however long it runs")]
    public void AWalkThatIsStillDeliveringIsNeverReported()
    {
        CaptureLogs();

        // A cold start replaying a long log: slow, but converging one frame at a time. Measuring elapsed time
        // instead of progress is exactly what would fence this.
        for (long clusterSeqNo = 1; clusterSeqNo <= 20; clusterSeqNo++)
        {
            DeliverReplay(clusterSeqNo);
            Assert.False(CheckProgressAt(ClockMs + clusterSeqNo * 60_000), $"at clusterSeqNo {clusterSeqNo}");
        }
    }

    [Fact(DisplayName = "a gap after a healthy run starts its own episode rather than inheriting one")]
    public void AGapAfterAHealthyRunStartsItsOwnEpisode()
    {
        CaptureLogs();
        Assert.False(CheckProgressAt(ClockMs)); // an episode anchored during cold start
        DeliverQueue(1);                        // ... which then converges
        Assert.True(_receiver.IsCaughtUp);

        DeliverQueue(5); // much later, a queue gap: not caught up again
        Assert.False(_receiver.IsCaughtUp);

        // Inheriting the cold-start clock would report this the instant the gap opened, before the re-walk it
        // triggers had any chance to converge.
        Assert.False(CheckProgressAt(PastDeadlineMs));
        Assert.True(_logged.Count == 1, "the queue-gap warn only: no convergence report");
        Assert.Equal(Logger.CoreEventCode.QueueGap, _logged[0].Code);
    }

    [Fact(DisplayName = "a caught-up client that simply goes quiet is not reported")]
    public void ACaughtUpClientThatGoesQuietIsNotReported()
    {
        CaptureLogs();
        DeliverQueue(1);
        Assert.True(_receiver.IsCaughtUp);

        // A queue that goes silent is a gateway's queue-stall watchdog's business, and it is not a convergence failure
        // at all: diagnosing it as one puts a second, wrong explanation on the same event.
        Assert.False(CheckProgressAt(ClockMs));
        Assert.False(CheckProgressAt(PastDeadlineMs));
        Assert.Empty(_logged);
    }

    [Fact(DisplayName = "the report names the state that tells the causes apart")]
    public void TheReportNamesTheStateThatTellsTheCausesApart()
    {
        DeliverQueue(1);
        DeliverQueue(5); // gap -> awaiting a replay

        CaptureLogs(); // installed after the gap, so it captures only the report below
        Assert.False(CheckProgressAt(ClockMs));
        Assert.True(CheckProgressAt(PastDeadlineMs));

        Assert.Single(_logged);
        string text = _logged[0].Message;
        // Without these an operator cannot tell an unanswering node from one whose recording cannot cover the hole.
        Assert.Contains("lastClusterSeqNo=1", text);
        Assert.Contains("awaitingReplay=true", text);
    }

    [Fact(DisplayName = "the stall gauge clears once, on the dispatch that ends a reported episode")]
    public void TheStallGaugeClearsOnceOnTheDispatchThatEndsAReportedEpisode()
    {
        CaptureLogs();
        DeliverReplay(1);
        Assert.True(_actions.StalledGauge.Count == 0, "progress with nothing reported has no edge to fall from");

        Assert.False(CheckProgressAt(ClockMs));
        Assert.True(CheckProgressAt(PastDeadlineMs));
        DeliverReplay(2); // the reported episode ends here
        DeliverReplay(3); // ... and only here

        Assert.Equal(new List<bool> { true, false }, _actions.StalledGauge);
    }

    [Fact(DisplayName = "a later episode is reported again rather than swallowed by the first")]
    public void ALaterEpisodeIsReportedAgain()
    {
        CaptureLogs();
        Assert.False(CheckProgressAt(ClockMs));
        Assert.True(CheckProgressAt(PastDeadlineMs));
        DeliverReplay(1); // one frame got through: the walk is slow, not stuck

        Assert.False(CheckProgressAt(PastDeadlineMs + 1_000), "anchors a fresh episode");
        Assert.False(CheckProgressAt(PastDeadlineMs + 30_000), "rather than inheriting the old clock");
        Assert.True(CheckProgressAt(PastDeadlineMs + 31_000), "a second, distinct episode reports too");
        Assert.Equal(2, _logged.Count);
    }

    // ── Delivery ───────────────────────────────────────────────────────────────────────────────────

    [Fact(DisplayName = "a leadership change reaches onSequenced when no leadership handler is given")]
    public void ALeadershipChangeReachesOnSequencedWithoutAHandler()
    {
        DeliverLeadershipChanged(1);

        Assert.Equal(new List<long> { 1 }, _dispatched); // dropped rather than delivered
    }

    [Fact(DisplayName = "a leadership change with a handler goes to it alone")]
    public void ALeadershipChangeWithAHandlerGoesToItAlone()
    {
        var changes = new List<long>();
        _receiver =
            new ReplayerRecovery(_actions, e => _dispatched.Add(e.ClusterSeqNo),
                                 (leaderMemberId, leadershipTermId, clusterSeqNo) => changes.Add(clusterSeqNo), null);
        DeliverLeadershipChanged(1);

        Assert.Equal(new List<long> { 1 }, changes);
        Assert.True(_dispatched.Count == 0, "delivered twice");
    }

    [Fact(DisplayName = "onSequenced is required")]
    public void OnSequencedIsRequired()
    {
        Assert.Throws<ArgumentNullException>(() => new ReplayerRecovery(_actions, null, null, null));
    }

    // ── restoring a snapshot ──────────────────────────────────────────────────────────────────────

    [Fact(DisplayName = "a restoring cold start replays from its newest snapshot's cut first, holding the live queue")]
    public void RestoringColdStartReplaysItsSnapshotsCutFirst()
    {
        RestoreFrames.Write(_store, 1, Cut - 5, (Cut - 5) * 1024, 1, SnapshotHeaderRecord, SnapshotRecord);
        WriteSnapshot(2);
        StartRestoring();

        Assert.Equal(1, _actions.RequestsSent);
        Assert.True(_receiver.IsAwaitingReplay);
        Assert.Equal(Cut * 1024, _receiver.RequestFromPosition); // the newest round's cut
        Assert.False(_receiver.IsRestoring, "no record before the cut is seen");
        Assert.True(_receiver.IsRecovering);

        DeliverQueue(40); // mid-stream, and no baseline: held, not fatal
        Assert.Empty(_dispatched);
    }

    [Fact(DisplayName = "an unanswered replay of the cut is resent; a pending answer holds it")]
    public void UnansweredCutReplayIsResent()
    {
        WriteSnapshot(2);
        StartRestoring();
        AdvancePastTimers();
        Assert.Equal(2, _actions.RequestsSent);
        Assert.Equal(Cut * 1024, _receiver.RequestFromPosition);

        DeliverPending(_receiver.RequestId);
        _actions.ClockMs += 100;
        _receiver.DoTimers(false);
        Assert.Equal(2, _actions.RequestsSent);
    }

    [Fact(DisplayName = "with no local snapshot, the cold start walks the recording from its start")]
    public void NoSnapshotWalksFromTheStart()
    {
        StartRestoring();

        Assert.Equal(1, _actions.RequestsSent);
        Assert.False(_receiver.IsRestoring);
        Assert.True(_receiver.IsAwaitingReplay);
        Assert.Equal(FromStart, _receiver.RequestFromPosition);
    }

    [Fact(DisplayName =
              "a snapshot whose cut the recording holds is restored from its file, then resumes anchored at its cut")]
    public void SnapshotIsRestoredThenResumesAtItsCut()
    {
        WriteSnapshot(2);
        StartRestoring();
        AttachReplay(21, 64 * 1024);
        DeliverReplayFrame(RestoreFrames.Started(Cut, 2), Cut);
        Assert.True(_receiver.IsRestoring);
        Assert.True(_receiver.ReplaySessionId == -1, "the records come from the file, not the replay");
        Assert.True(_dispatched.Count == 0, "the cut is not dispatched");

        _receiver.DoTimers(false);

        Assert.Equal(new List<string> { "header 4 2", "0 7" }, _restored);
        Assert.Empty(_dispatched);
        Assert.False(_receiver.IsRestoring);
        Assert.Equal(Cut, _receiver.LastClusterSeqNo);
        Assert.True(_receiver.CurrentLeaderMemberId == 2, "the header's leader");
        Assert.True(_receiver.IsAwaitingReplay);
        Assert.Equal(Cut * 1024, _receiver.RequestFromPosition);

        AttachReplay(22, 64 * 1024);
        DeliverReplayFrame(RestoreFrames.Started(Cut, 2), Cut); // the anchor, already dispatched
        DeliverReplay(11);
        DeliverReplay(12);
        Assert.Equal(new List<long> { 11, 12 }, _dispatched);
    }

    [Fact(DisplayName = "after a restore, the queue's continuity resumes at the cut frame's groupSeqNo")]
    public void AfterARestoreContinuityResumesAtTheCutsGroupSeqNo()
    {
        WriteSnapshot(2);
        StartRestoring();
        AttachReplay(21, 64 * 1024);
        DeliverReplayFrame(Frames.OnQueue(RestoreFrames.Started(Cut, 2), 4), 4); // the queue's fourth frame
        _receiver.DoTimers(false);

        AttachReplay(22, 64 * 1024);
        DeliverReplayFrame(Frames.OnQueue(RestoreFrames.Started(Cut, 2), 4), 4);
        DeliverOnQueue(Cut + 3, 5, true);
        Assert.Equal(new List<long> { Cut + 3 }, _dispatched); // groupSeqNo 5 follows the cut's 4
    }

    [Fact(DisplayName = "a replay of the cut that stops advancing is requested again at the cut")]
    public void StalledCutReplayIsRequestedAgain()
    {
        WriteSnapshot(2);
        StartRestoring();
        AttachReplay(21, 64 * 1024);
        AdvancePastTimers();

        Assert.True(_receiver.IsAwaitingReplay);
        Assert.Equal(Cut * 1024, _receiver.RequestFromPosition);
        AttachReplay(22, 64 * 1024);
        DeliverReplayFrame(RestoreFrames.Started(Cut, 2), Cut);
        Assert.True(_receiver.IsRestoring);
    }

    [Fact(DisplayName = "a restore reads a bounded number of records per duty cycle, holding the queue until its last")]
    public void RestoreIsPacedAcrossDutyCycles()
    {
        var records = new byte[ReplayerRecovery.MaxRestoreRecordsPerCycle + 1][];
        records[0] = SnapshotHeaderRecord;
        for (int i = 1; i < records.Length; i++)
        {
            records[i] = RestoreFrames.Record(i);
        }
        RestoreFrames.Write(_store, 2, Cut, Cut * 1024, 1, records);
        StartRestoring();
        ReplayTheCut();

        _receiver.DoTimers(false);
        Assert.True(_receiver.IsRestoring);
        Assert.Equal(ReplayerRecovery.MaxRestoreRecordsPerCycle, _restored.Count);
        DeliverQueue(40);
        Assert.Empty(_dispatched);

        _receiver.DoTimers(false);
        Assert.False(_receiver.IsRestoring);
        Assert.Equal(records.Length, _restored.Count);
        Assert.True(_receiver.IsAwaitingReplay);
    }

    [Fact(
        DisplayName =
            "a snapshot whose cut the recording does not hold gives way to the next older one, and the last to a walk")]
    public void SnapshotOfAnotherLogGivesWayToAnOlderOne()
    {
        for (long round = 3; round >= 1; round--)
        {
            WriteSnapshot(round); // newest first: a confirmation deletes the older files
        }
        File.WriteAllBytes(StoreFile(4), File.ReadAllBytes(StoreFile(3))[..40]);
        StartRestoring();

        Assert.True(_actions.RequestsSent == 1, "round 4 is cut short: no replay for it");
        AttachReplay(21, 64 * 1024);
        DeliverReplayFrame(RestoreFrames.Started(Cut, 3, 1), Cut); // another log's frame at round 3's cut
        Assert.False(_receiver.IsRestoring);
        Assert.True(_actions.RequestsSent == 2, "round 2's cut, next");
        AttachReplay(22, 64 * 1024);
        DeliverReplay(Cut + 1); // round 2's cut is not where this recording has it
        Assert.False(_receiver.IsRestoring);
        Assert.Equal(3, _actions.RequestsSent);
        AnswerReplaying(NoReplayNeeded, 0); // round 1's cut is no frame of this recording
        Assert.Null(_receiver.Fault);
        Assert.False(_receiver.IsRestoring);
        Assert.True(_receiver.IsAwaitingReplay);
        Assert.Equal(FromStart, _receiver.RequestFromPosition);
        Assert.True(_restored.Count == 0, "nothing restored from a file the recording does not hold");
    }

    [Fact(DisplayName = "after a restore, a resume that opens anywhere but the cut is fatal")]
    public void AfterARestoreAResumeOpeningAnywhereButTheCutIsFatal()
    {
        WriteSnapshot(2);
        StartRestoring();
        ReplayTheCut();
        _receiver.DoTimers(false);
        AttachReplay(22, 64 * 1024);

        DeliverReplay(12); // not the anchor

        Assert.Contains("expected " + Cut, _receiver.Fault);
        Assert.True(_dispatched.Count == 0, "nothing after a wrong cut may be dispatched");
    }

    [Fact(DisplayName = "a snapshot that cannot be restored stops recovery: its format, its header, or its records")]
    public void UnrestorableSnapshotStopsRecovery()
    {
        RestoreFrames.Write(_store, 2, Cut, Cut * 1024, 9, SnapshotHeaderRecord, SnapshotRecord);
        StartRestoring();
        Assert.Contains("formatVersion 9", _receiver.RestoreFailure);
        Assert.False(_receiver.IsRecovering);
        Assert.Equal(0, _actions.RequestsSent);
        DeliverQueue(40); // no baseline, and no longer an error either
        Assert.Empty(_dispatched);

        SetUp();
        byte[] header = (byte[])SnapshotHeaderRecord.Clone();
        header[0] = 9;
        RestoreFrames.Write(_store, 2, Cut, Cut * 1024, 1, header, SnapshotRecord);
        StartRestoring();
        ReplayTheCut();
        _receiver.DoTimers(false);
        Assert.Contains("header of version 9", _receiver.RestoreFailure);

        SetUp();
        WriteSnapshot(2);
        string file = StoreFile(2);
        byte[] damaged = File.ReadAllBytes(file);
        damaged[2 + SnapshotHeaderRecord.Length + 2] ^= 1; // the record's first byte; the trailer still matches
        File.WriteAllBytes(file, damaged);
        StartRestoring();
        ReplayTheCut();
        _receiver.DoTimers(false);
        Assert.Contains("damaged", _receiver.RestoreFailure);
        Assert.False(_receiver.IsRecovering);
        Assert.Null(new ReplayerRecovery(_actions, Ignore, null, null).RestoreFailure);
    }

    // ── helpers ───────────────────────────────────────────────────────────────────────────────────

    private static void Ignore(SequencedEvent sequencedEvent)
    {
    }

    private void StartRestoring()
    {
        _receiver.RestoreFrom(_store, new RecordingRestoreHandler(_restored));
        _receiver.Start();
    }

    // This instance's confirmed file of `round`, cut at Cut: the header and one record.
    private void WriteSnapshot(long round)
    {
        RestoreFrames.Write(_store, round, Cut, Cut * 1024, 1, SnapshotHeaderRecord, SnapshotRecord);
    }

    // The current store's file of `round`.
    private string StoreFile(long round)
    {
        return Path.Combine(_directory.Path, "instance-" + (_stores - 1), round + ".snapshot");
    }

    // The replay of the cut opens on the round's SnapshotStarted, as the file recorded it.
    private void ReplayTheCut()
    {
        AttachReplay(21, 64 * 1024);
        DeliverReplayFrame(RestoreFrames.Started(Cut, 2), Cut);
    }

    private void DeliverReplayFrame(byte[] frame, long clusterSeqNo)
    {
        _wire.Wrap(frame);
        _receiver.OnFrame(_wire, 0, frame.Length, clusterSeqNo * 1024, ReceiveNs, true);
    }

    // Starts capturing log events here rather than in the constructor: several cases must see only what follows
    // their own setup, and the twins install their sinks at the same points for the same reason.
    private void CaptureLogs()
    {
        _logged.Clear();
        Logger.Install(new CallbackLoggerSink(_logged.Add));
    }

    // Retained-buffer overflows only: every gap/re-walk warn shares QueueGap, so only the text separates them.
    private int OverflowReports()
    {
        return _logged.Count(e => e.Message.Contains("retained-frame buffer full"));
    }

    // Drives the receiver to the caught-up, steady state a live consumer runs in.
    private void GoLive()
    {
        AnswerReplaying(NoReplayNeeded, 0);
    }

    private void AttachReplay(long replaySessionId, long catchUpPosition)
    {
        AnswerReplaying(replaySessionId, catchUpPosition);
    }

    private void AnswerReplaying(long replaySessionId, long catchUpPosition)
    {
        Control(ControlMessages.Replaying(ClientId, _receiver.RequestId, replaySessionId, catchUpPosition));
    }

    private void DeliverPending(long requestId)
    {
        Control(ControlMessages.ReplayPending(ClientId, requestId));
    }

    private void Control(byte[] message)
    {
        _wire.Wrap(message);
        _receiver.OnControl(_wire, 0, message.Length);
    }

    private void DeliverLeadershipChanged(long clusterSeqNo)
    {
        byte[] frame = Frames.LeadershipChanged(clusterSeqNo, 2, 3);
        _wire.Wrap(frame);
        _receiver.OnFrame(_wire, 0, frame.Length, clusterSeqNo * 1024, ReceiveNs, false);
    }

    // The replay image reaching the bound the Replayer gave it: how a replay completes.
    private void CompleteReplay()
    {
        _receiver.OnReplayPosition(_receiver.CatchUpPosition);
    }

    // One duty cycle with the clock past every timer in it: an unanswered request is re-sent, and an established
    // replay that has delivered nothing is declared stalled.
    private void AdvancePastTimers()
    {
        _actions.ClockMs += PastEveryTimerMs;
        _receiver.DoTimers(false);
    }

    private bool CheckProgressAt(long nowMs)
    {
        _actions.ClockMs = nowMs;
        return _receiver.CheckRecoveryProgress();
    }

    private void DeliverReplay(long clusterSeqNo)
    {
        byte[] frame = Frames.ClusterHeartbeat(clusterSeqNo);
        _wire.Wrap(frame);
        _receiver.OnFrame(_wire, 0, frame.Length, clusterSeqNo * 1024, ReceiveNs, true);
    }

    private void DeliverQueue(long clusterSeqNo)
    {
        DeliverQueueAt(clusterSeqNo, clusterSeqNo * 1024);
    }

    // A frame at groupSeqNo on a queue that does not carry every frame, starting at its 1024s.
    private void DeliverOnQueue(long clusterSeqNo, long groupSeqNo, bool fromReplay)
    {
        byte[] frame = Frames.OnQueue(Frames.ClusterHeartbeat(clusterSeqNo), groupSeqNo);
        _wire.Wrap(frame);
        _receiver.OnFrame(_wire, 0, frame.Length, groupSeqNo * 1024, ReceiveNs, fromReplay);
    }

    private void DeliverQueueAt(long clusterSeqNo, long position)
    {
        byte[] frame = Frames.ClusterHeartbeat(clusterSeqNo);
        _wire.Wrap(frame);
        _receiver.OnFrame(_wire, 0, frame.Length, position, ReceiveNs, false);
    }

    // A live queue frame too big for the retained-ahead FIFO to hold: a heartbeat zero-padded past one RetainBlock,
    // which RetainFrame refuses outright. The cheapest of its three overflow triggers to drive; the other two need
    // 65536 frames or 16 MiB.
    private void DeliverQueueTooBigToRetain(long clusterSeqNo)
    {
        DeliverQueuePadded(clusterSeqNo, OversizedFrameLength);
    }

    // A live queue frame of `length` bytes: a heartbeat, decoded from the front, zero-padded to it.
    private void DeliverQueuePadded(long clusterSeqNo, int length)
    {
        var frame = new byte[length];
        Frames.ClusterHeartbeat(clusterSeqNo).CopyTo(frame, 0);
        _wire.Wrap(frame);
        _receiver.OnFrame(_wire, 0, length, clusterSeqNo * 1024, ReceiveNs, false);
    }
}
