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
    private const int ClientId = 4;
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

    // The largest frame the tap carries (§12): a full payload behind the 44-byte sequenced envelope.
    private const int LargestFrameLength = 44 + FrameLayer.MaxPayloadLength;

    // The source restored, its snapshot's cut, and where frame n starts: n * 1024, as everywhere here.
    private const int Source = 3;
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
        public int QueriesSent;
        public int QuerySourceId = -1;
        public long QueryRound = -1;
        public int HeartbeatsSent;
        public readonly List<bool> StalledGauge = new List<bool>();

        public void SendReplayRequest(long requestId, long fromPosition)
        {
            ++RequestsSent;
        }

        public void SendSnapshotQuery(long requestId, int sourceId, long round)
        {
            ++QueriesSent;
            QuerySourceId = sourceId;
            QueryRound = round;
        }

        // False like a receiver with no publication: every send here is one that never went out, which is the
        // state the ReplayComplete retry tests need.
        public bool SendReplayComplete()
        {
            return false;
        }

        public bool SendReplayHeartbeat()
        {
            ++HeartbeatsSent;
            return false;
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

        public void OnSnapshotRecord(IDirectBuffer record, int length, int recordIndex)
        {
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
        _receiver = new ReplayerRecovery(ClientId, _actions, e => _dispatched.Add(e.GlobalSeqNo), null,
                                         () => _caughtUpAt.Add(_dispatched.Count));
    }

    public void Dispose()
    {
        Logger.Reset();
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

    [Fact(DisplayName = "a first frame at globalSeqNo 1 is the baseline and puts the client live")]
    public void FirstFrameAtOneIsAccepted()
    {
        DeliverTap(1);

        Assert.Equal(new List<long> { 1 }, _dispatched);
        Assert.True(_receiver.IsCaughtUp);
    }

    [Fact(DisplayName = "a first frame that is not globalSeqNo 1 is fatal")]
    public void FirstFrameMustBeOne()
    {
        AttachReplay(11, 4096);

        Assert.Throws<InvalidOperationException>(() => DeliverReplay(7));
    }

    // Once the tap is dispatched mid-walk, a cold start races a live tap already carrying mid-stream globalSeqNos
    // against a replay that has not delivered 1 yet. That tap frame is neither a valid baseline nor a fault: only
    // the walk may establish the baseline.
    [Fact(DisplayName = "the live tap may not establish the baseline while the cold-start walk is in flight")]
    public void LiveTapMayNotEstablishTheBaselineMidWalk()
    {
        AnswerReplaying(7, 500);
        Assert.True(_receiver.IsRecovering);

        DeliverTap(15); // the live tip, far ahead of a replay that has not started delivering

        Assert.True(_dispatched.Count == 0, "must not adopt the live tap's mid-stream baseline");
        Assert.False(_receiver.IsCaughtUp);

        DeliverReplay(1); // the walk supplies the real baseline

        Assert.Equal(new List<long> { 1 }, _dispatched);
    }

    // ── gap detection and the replay-to-live seam ─────────────────────────────────────────────────

    [Fact(DisplayName = "a mid-stream gap withholds the out-of-order frame and requests a replay")]
    public void MidStreamGapWithholdsTheOutOfOrderFrame()
    {
        DeliverTap(1);
        DeliverTap(2);
        Assert.Equal(2, _dispatched.Count);
        Assert.False(_receiver.IsAwaitingReplay);

        DeliverTap(5); // skips 3, 4

        Assert.True(_dispatched.Count == 2, "the out-of-order frame itself must not be dispatched");
        Assert.True(_receiver.IsAwaitingReplay, "a gap must re-request a replay");
        Assert.True(_receiver.ReplaySessionId == -1, "no session yet, only a request, until the Replayer answers");
    }

    [Fact(DisplayName = "a tap gap reports a structured diagnostic event")]
    public void TapGapReportsAStructuredDiagnosticEvent()
    {
        CaptureLogs();

        DeliverTap(1);
        DeliverTap(2);
        DeliverTap(5); // skips 3, 4

        Logger.LoggerEvent loggerEvent = Assert.Single(_logged); // the one gap above, nothing from setup
        Assert.Equal(Logger.CoreComponent.ReplayerStreamReceiver, loggerEvent.Component);
        Assert.Equal(Logger.Severity.Warn, loggerEvent.Severity);
        Assert.Equal(Logger.CoreEventCode.TapGap, loggerEvent.Code);
        Assert.Equal("tap gap: expected globalSeqNo=3 got 5 — resuming the recording at globalSeqNo=2",
                     loggerEvent.Message);
    }

    [Fact(DisplayName = "a live tap frame ahead of an in-flight walk is retained, not treated as a gap")]
    public void TapFrameMidWalkIsRetainedNotAGap()
    {
        AttachReplay(11, 4096);
        long requestIdBefore = _receiver.RequestId;

        DeliverTap(9); // the tap runs ahead of the replay

        Assert.True(_dispatched.Count == 0, "no baseline yet: only the walk may establish one");
        Assert.Equal(1, _receiver.RetainedFrameCount);
        Assert.True(requestIdBefore == _receiver.RequestId, "an in-flight walk must not be superseded");
    }

    [Fact(DisplayName = "a tap gap while the walk is still in flight does not supersede it")]
    public void TapGapMidWalkDoesNotSupersedeTheWalk()
    {
        AttachReplay(11, 4096);
        DeliverReplay(1); // baseline established, by the walk rather than by the tap
        long requestIdBefore = _receiver.RequestId;

        DeliverTap(5); // 2..4 are not lost: they are frames this walk has not reached yet

        Assert.True(requestIdBefore == _receiver.RequestId, "an in-flight walk must run to completion");
        Assert.True(_receiver.RequestFromPosition == FromStart, "a mid-walk tap gap is expected, not a new gap");
        Assert.Equal(1, _receiver.RetainedFrameCount);
    }

    // The same rule one state later: not merely awaiting an answer but riding an assigned session. Superseding the
    // walk on each tap frame would restart it from the start forever under any sustained publish rate.
    [Fact(DisplayName = "a tap frame arriving during an assigned replay session does not supersede it")]
    public void TapFrameAheadOfAnAssignedReplayDoesNotSupersedeIt()
    {
        CaptureLogs();
        DeliverTap(1);
        DeliverTap(5); // the genuine steady-state gap
        Assert.True(_receiver.IsAwaitingReplay);
        Assert.Single(_logged);

        AnswerReplaying(99, 500);
        Assert.Equal(99, _receiver.ReplaySessionId);
        Assert.False(_receiver.IsAwaitingReplay);
        Assert.True(_receiver.IsRecovering);

        DeliverTap(12); // the tap running ahead, not a hole this walk will not cover

        Assert.True(_receiver.ReplaySessionId == 99, "the in-flight walk must run to completion");
        Assert.False(_receiver.IsAwaitingReplay);
        Assert.True(_receiver.RequestFromPosition != FromStart, "still the resume the gap asked for, not restarted");
        Assert.True(_dispatched.Count == 1, "the out-of-order frame itself is still withheld");
        Assert.True(_logged.Count == 1, "and it is not reported as a second gap");
    }

    // The replay->live seam: the tap is dispatched throughout a walk, so the frame at gseq == last + 1 lands the
    // instant the replay reaches it. Discarding tap frames while recovering ends every walk one guaranteed gap short
    // of live.
    [Fact(DisplayName = "the tap frame at the seam is dispatched while the walk is still in flight")]
    public void LiveTapFrameAtTheSeamIsDispatchedMidWalk()
    {
        AnswerReplaying(7, 500);
        DeliverReplay(1);
        DeliverReplay(2);
        Assert.Equal(2, _dispatched.Count);
        Assert.False(_receiver.IsCaughtUp, "still riding replayed history");
        Assert.True(_receiver.IsRecovering);

        DeliverTap(3); // the seam: first tap frame contiguous with what the replay delivered

        Assert.True(_dispatched.Count == 3, "the seam frame must be dispatched, not discarded as mid-walk noise");
        Assert.True(_receiver.IsCaughtUp, "a contiguous frame off the live tap means we are following live");
    }

    [Fact(DisplayName = "a steady-state tap gap clears caught-up and resumes at the last dispatched frame")]
    public void SteadyStateGapResumesAtLastFrame()
    {
        GoLive();
        DeliverTapAt(1, 1024);
        DeliverTapAt(2, 2048);
        Assert.True(_receiver.IsCaughtUp);

        DeliverTapAt(5, 8192); // 3 and 4 dropped

        Assert.False(_receiver.IsCaughtUp, "consumers gate real decisions on this: it must not stay latched");
        Assert.True(_receiver.IsAwaitingReplay);
        Assert.True(_receiver.RequestFromPosition != FromStart,
                    "a gap resumes; it does not re-walk from a stale index");
        Assert.True(_receiver.RequestFromPosition == 2048, "resume at the frame last dispatched, not at 0");
        Assert.True(_receiver.RetainedFrameCount == 1, "the ahead-of-hole frame is kept, not dropped");
    }

    [Fact(DisplayName = "replayed frames already seen off the live tap are de-duped")]
    public void DuplicateAndStaleReplayFramesAreDropped()
    {
        DeliverTap(1);
        DeliverTap(2);
        Assert.Equal(2, _dispatched.Count);

        DeliverReplay(1);
        DeliverReplay(2);
        Assert.True(_dispatched.Count == 2, "replayed frames already delivered off the live tap must be de-duped");

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
        Assert.Equal(3, _receiver.LastGlobalSeqNo);
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

    [Fact(DisplayName = "a walk reaching its bound catches up and hands its slot back")]
    public void AWalkReachingItsBoundCatchesUp()
    {
        AnswerReplaying(7, 500);
        Assert.Equal(7, _receiver.ReplaySessionId);

        CompleteReplay();

        Assert.True(_receiver.IsCaughtUp, "the recording held the whole log when the request was served");
        Assert.False(_receiver.IsAwaitingReplay, "there is nothing further to ask for");
        Assert.Equal(-1, _receiver.ReplaySessionId);
        Assert.True(_receiver.CompletePending, "the slot is released, not left to the idle TTL");
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
    // skips both watchdogs for it: no heartbeat, so the Replayer's idle TTL reclaims the slot out from under a
    // healthy replay, and no stall watchdog once it stops delivering.
    [Fact(DisplayName = "replay session 0 is heart-beaten and watchdogged like any other")]
    public void ReplaySessionZeroIsHeartbeatedAndWatchdoggedLikeAnyOther()
    {
        AnswerReplaying(0, 500);
        Assert.True(_receiver.ReplaySessionId == 0,
                    "session 0 is a replay this client is riding, not the absence of one");

        AdvancePastTimers();

        Assert.True(_actions.HeartbeatsSent == 1,
                    "a slot held on session 0 must still be heart-beaten, or the idle TTL reclaims it");
        Assert.True(_receiver.IsAwaitingReplay, "and its stall watchdog must still re-request it");
        Assert.Equal(-1, _receiver.ReplaySessionId);
    }

    // ── ReplayPending and ReplayUnavailable ───────────────────────────────────────────────────────

    [Fact(DisplayName = "ReplayPending holds at the gap without assigning a session")]
    public void ReplayPendingHoldsAtTheGapWithoutAssigningASession()
    {
        DeliverTap(1);
        DeliverTap(5); // gap -> awaiting a replay
        Assert.True(_receiver.IsAwaitingReplay);

        DeliverPending(_receiver.RequestId);

        Assert.True(_receiver.IsAwaitingReplay, "no free Replayer slot -> keep holding at the gap");
        Assert.Equal(-1, _receiver.ReplaySessionId);
        Assert.True(_dispatched.Count == 1, "must not advance past the hole while pending");
    }

    // A Replayer whose archive failed the globalSeqNo-1 integrity check refuses rather than serving history its own
    // check rejected. Served anyway, the replay's first frame would not be 1 and this client would abort, taking
    // down every co-located app over one node's bad archive.
    [Fact(DisplayName = "ReplayUnavailable holds without aborting or advancing, and reports once per episode")]
    public void ReplayUnavailableHoldsWithoutAbortingOrAdvancing()
    {
        DeliverTap(1);
        DeliverTap(5); // gap -> awaiting a replay
        Assert.True(_receiver.IsAwaitingReplay);
        Assert.False(_receiver.IsCaughtUp);

        CaptureLogs(); // installed after the setup gap, so it captures only the refusal below
        DeliverUnavailable(_receiver.RequestId);

        Assert.True(_receiver.IsAwaitingReplay, "a refusal must not stop the resend timer");
        Assert.Equal(-1, _receiver.ReplaySessionId);
        Assert.True(_dispatched.Count == 1, "must not advance past the hole");
        Assert.False(_receiver.IsCaughtUp, "consumer gates must stay shut while history is unavailable");
        Assert.Single(_logged);
        Assert.Equal(Logger.Severity.Fault, _logged[0].Severity);
        Assert.Equal(Logger.CoreEventCode.ReplayUnavailable, _logged[0].Code);

        // The Replayer answers every resend the same way; the fault line must not repeat per reply.
        DeliverUnavailable(_receiver.RequestId);
        Assert.True(_logged.Count == 1, "one episode is one line, not one per 500ms resend");
    }

    // A second, distinct outage hours later must report itself: the latch is on the episode, not on the process.
    // The intervening reply is what ends the first episode.
    [Fact(DisplayName = "a refusal after the Replayer recovered is reported again")]
    public void ARefusalAfterTheReplayerRecoveredIsReportedAgain()
    {
        DeliverTap(1);
        DeliverTap(5); // gap -> awaiting a replay

        CaptureLogs();
        DeliverUnavailable(_receiver.RequestId);
        Assert.Equal(1, Refusals());

        AnswerReplaying(7, 900);
        // Later: that node's archive breaks again, its Replayer restarts refusing, and our replay stops delivering,
        // so we re-request (a new requestId) and are refused a second time.
        AdvancePastTimers();
        DeliverUnavailable(_receiver.RequestId);

        Assert.True(Refusals() == 2, "a process-lifetime latch would leave every later outage silent");
    }

    // Being queued ends the episode too: a Replayer with integrityFailed latched answers ReplayUnavailable, never
    // ReplayPending, so a pending reply is proof it is serving again.
    [Fact(DisplayName = "a queued replay also ends the refusal episode")]
    public void AQueuedReplayAlsoEndsTheRefusalEpisode()
    {
        DeliverTap(1);
        DeliverTap(5);

        CaptureLogs();
        DeliverUnavailable(_receiver.RequestId);
        DeliverPending(_receiver.RequestId);
        AdvancePastTimers();
        DeliverUnavailable(_receiver.RequestId);

        Assert.Equal(2, Refusals());
    }

    [Fact(DisplayName = "a ReplayUnavailable for a superseded request says nothing and resets no clock")]
    public void ReplayUnavailableForASupersededRequestIsIgnored()
    {
        DeliverTap(1);
        DeliverTap(5);
        long stale = _receiver.RequestId - 1;
        int sends = _actions.RequestsSent;

        CaptureLogs();
        DeliverUnavailable(stale);
        _actions.ClockMs += 600; // > ResendIntervalMs since the CURRENT request went out
        _receiver.DoTimers(false);

        Assert.True(_logged.Count == 0, "a stale refusal says nothing about the Replayer's current state");
        Assert.True(sends + 1 == _actions.RequestsSent, "and must not reset the resend clock");
    }

    // ── the resend timer ──────────────────────────────────────────────────────────────────────────

    [Fact(DisplayName = "a request the Replayer never answered is re-sent once the interval elapses")]
    public void StuckAwaitingReplayResendsAfterTheIntervalElapses()
    {
        DeliverTap(1);
        DeliverTap(5); // gap -> arms the resend timer
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
        DeliverTap(1);
        DeliverTap(5); // gap -> a request that never goes out, there being no publication here
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

    // IsRecovering is what separates "the tap is ahead of my replay" from "there is a hole": narrowing it means a
    // walk supersedes itself on its own in-flight frames, widening it means a real gap goes unreported. This walks
    // every transition it must track.
    [Fact(DisplayName = "the recovering flag tracks the walk and awaiting-replay states")]
    public void RecoveringFlagTracksWalkAndAwaitingReplayState()
    {
        Assert.False(_receiver.IsRecovering, "nothing in flight yet");

        DeliverTap(1);
        DeliverTap(5); // skips 2..4 -> gap, re-requests a replay
        Assert.True(_receiver.IsRecovering, "awaiting the Replayer's answer is already mid-recovery");

        AnswerReplaying(7, 500);
        Assert.True(_receiver.IsRecovering, "a session id means history remains to replay");

        DeliverReplay(1); // opens on the anchored frame -> anchor consumed, frame deduped
        DeliverReplay(2);
        DeliverReplay(3);
        DeliverReplay(4); // closes the hole, draining the retained frame 5
        CompleteReplay();
        Assert.False(_receiver.IsRecovering, "a resume ends at its bound");

        // The result of the same transitions, entered the way production enters it: a resume whose replay opens on
        // a frame other than the one it anchored on falls back to a walk.
        DeliverTap(9); // another gap -> another resume
        AnswerReplaying(8, 500);
        DeliverReplay(42);
        Assert.Equal(FromStart, _receiver.RequestFromPosition);
        Assert.True(_receiver.IsRecovering, "awaiting the walk");

        AnswerReplaying(9, 500);
        // The walk has to close the hole it re-walked for: 9 is retained behind the missing 6..8, and the bound
        // refuses to end recovery while anything is still stranded there.
        DeliverReplay(6);
        DeliverReplay(7);
        DeliverReplay(8); // dispatching 8 drains the retained 9 straight over
        Assert.True(_receiver.IsRecovering, "mid-walk until the bound");
        CompleteReplay();
        Assert.False(_receiver.IsRecovering, "a walk ends at its bound too");
    }

    // IsCaughtUp is a state, not a latch: consumers gate real decisions on it, and a gateway's tap-stall watchdog
    // measures silence in DISPATCHED frames. A re-walk dispatches nothing until the replay passes the hole, so
    // leaving it latched makes a gateway diagnose its own recovery as a stalled sequencer.
    [Fact(DisplayName = "a tap gap revokes caught-up until the stream goes contiguous again")]
    public void TapGapRevokesCaughtUpUntilTheStreamGoesContiguousAgain()
    {
        DeliverTap(1);
        DeliverTap(2);
        Assert.True(_receiver.IsCaughtUp);
        Assert.Single(_caughtUpAt);

        DeliverTap(5); // skips 3, 4 -> gap (and 5 is retained, not dropped)
        Assert.False(_receiver.IsCaughtUp, "a hole means we are demonstrably not following live");

        DeliverReplay(2); // a resume opens on the frame it anchored at: already delivered, deduped
        DeliverReplay(3);
        Assert.False(_receiver.IsCaughtUp, "4 is still missing: the hole is not closed yet");
        Assert.Single(_caughtUpAt);

        DeliverReplay(4); // closes the hole; the retained tap frame 5 then hands straight over

        Assert.True(_receiver.IsCaughtUp, "the retained frame closes the seam with no further round trip");
        Assert.True(_caughtUpAt.Count == 2, "consumers must be able to re-arm on re-convergence");
    }

    // ── the retained-ahead FIFO ───────────────────────────────────────────────────────────────────

    [Fact(DisplayName = "retained tap frames hand straight over when the replay reaches them")]
    public void RetainedFramesCloseTheSeam()
    {
        AttachReplay(11, 4096);
        DeliverTap(3);
        DeliverTap(4);

        DeliverReplay(1);
        DeliverReplay(2);

        Assert.Equal(new List<long> { 1, 2, 3, 4 }, _dispatched); // the seam closes with no residual
        Assert.Equal(0, _receiver.RetainedFrameCount);
    }

    [Fact(DisplayName = "frames beyond the hole are retained and delivered exactly once when it closes")]
    public void FramesBeyondTheHoleAreRetainedAndDeliveredOnceItCloses()
    {
        DeliverTap(1);
        DeliverTap(4); // gap -> retained
        DeliverTap(5); // still ahead of the hole -> retained
        DeliverTap(6);
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
        DeliverTap(1);
        DeliverTapPadded(3, LargestFrameLength); // gap -> retained
        Assert.Equal(1, _receiver.RetainedFrameCount);

        DeliverReplay(1); // a resume opens on the frame it anchored at: already delivered, deduped
        DeliverReplay(2); // closes the hole -> 3 drains behind it

        Assert.Equal(new List<long> { 1, 2, 3 }, _dispatched);
        Assert.True(_receiver.IsCaughtUp, "an overflow would have forced a re-walk");
    }

    // The ranges legitimately overlap, since a re-walk restarts from the start while the tap keeps arriving.
    [Fact(DisplayName = "retained frames the replay has meanwhile covered are not redelivered")]
    public void RetainedFramesAlreadyCoveredByTheReplayAreNotRedelivered()
    {
        DeliverTap(1);
        DeliverTap(3); // gap -> retained
        DeliverTap(4); // retained

        DeliverReplay(1); // a resume opens on the frame it anchored at: already delivered, deduped
        DeliverReplay(2);
        DeliverReplay(3); // the replay covers a frame the tap already retained
        DeliverReplay(4);

        Assert.Equal(new List<long> { 1, 2, 3, 4 }, _dispatched); // each globalSeqNo dispatched exactly once
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
        Assert.Equal(Logger.CoreEventCode.TapGap, _logged[0].Code);

        DeliverReplay(8);
        Assert.True(_logged.Count == 1, "report the episode, not every frame of a walk retrying the same recording");

        // A later, distinct episode must be reported again rather than swallowed by the latch.
        DeliverReplay(2);
        Assert.Equal(2, _dispatched.Count);
        DeliverReplay(9);
        Assert.True(_logged.Count == 2, "the latch clears once history goes contiguous again");
    }

    [Fact(DisplayName = "dropped tap frames are reported once per episode")]
    public void DroppedTapFramesAreReportedOncePerEpisode()
    {
        DeliverTap(1);
        DeliverTap(3); // gap -> resume anchored on frame 1; 3 is retained

        CaptureLogs(); // installed after the setup gap, so it captures only the overflow lines
        DeliverTapTooBigToRetain(4);
        DeliverTapTooBigToRetain(5);
        Assert.True(OverflowReports() == 1, "one episode is one line, not one per dropped frame");

        // The re-walk is what covers the dropped frames, so the episode ends when it is requested.
        AnswerReplaying(7, 500);
        DeliverReplay(1);
        DeliverReplay(2);
        CompleteReplay();
        Assert.Equal(FromStart, _receiver.RequestFromPosition);

        DeliverTapTooBigToRetain(9);

        Assert.True(OverflowReports() == 2, "a later overflow is a distinct episode and names itself");
    }

    // ── the resume anchor ─────────────────────────────────────────────────────────────────────────

    [Fact(DisplayName = "a resume replay that opens at the wrong frame falls back to a walk")]
    public void ResumeAnchorMismatchFallsBackToWalk()
    {
        GoLive();
        DeliverTapAt(1, 1024);
        DeliverTapAt(2, 2048);
        DeliverTapAt(5, 8192); // gap -> resume anchored on globalSeqNo 2
        AttachReplay(12, 16384);

        DeliverReplay(4); // not the anchor: the active recording rotated under us

        Assert.True(_receiver.RequestFromPosition == FromStart, "must fall back to the walk, which needs no position");
        Assert.True(_receiver.IsAwaitingReplay);
    }

    // The anchor is consumed by the first frame of ONE replay episode: a retry of that same logical resume starts a
    // NEW episode and must re-arm the check, or the retried replay's first frame rides with nothing to validate
    // against and a rotated recording's frame is dispatched merely because it looks contiguous.
    [Fact(DisplayName = "a resume whose image closes short of its bound re-arms the anchor check")]
    public void ReplayImageClosingShortOfTheBoundOnAResumeReArmsTheAnchorCheck()
    {
        DeliverTap(1);
        DeliverTap(5); // gap -> resume anchored on frame 1
        AnswerReplaying(7, 500);
        DeliverReplay(1); // opens on the anchored frame -> anchor consumed, frame deduped
        Assert.Single(_dispatched);

        _receiver.OnReplayImageClosed(300); // stopped under us before frame 2 arrived
        Assert.True(_receiver.RequestFromPosition != FromStart, "still a resume retry, not a walk step");

        AnswerReplaying(8, 500);
        // The active recording rotated: the retried resume's first frame is NOT the frame it was anchored on, but it
        // happens to look contiguous.
        DeliverReplay(2);

        Assert.True(_dispatched.Count == 1, "must not accept an unanchored frame merely because it looks contiguous");
        Assert.True(_receiver.RequestFromPosition == FromStart, "the mismatch must fall back to a walk");
        Assert.True(_receiver.IsAwaitingReplay);
        Assert.Equal(-1, _receiver.ReplaySessionId);
        Assert.False(_receiver.IsCaughtUp);
    }

    [Fact(DisplayName = "a resume that stalls re-arms the anchor check")]
    public void ReplayStallOnAResumeReArmsTheAnchorCheck()
    {
        DeliverTap(1);
        DeliverTap(5); // gap -> resume anchored on frame 1
        AnswerReplaying(7, 500);
        DeliverReplay(1);
        Assert.Single(_dispatched);

        AdvancePastTimers(); // stopped advancing before frame 2 arrived
        Assert.True(_receiver.RequestFromPosition != FromStart, "still a resume retry, not a walk step");

        AnswerReplaying(8, 500);
        DeliverReplay(2); // rotated recording, looks contiguous but is not the anchored frame

        Assert.True(_dispatched.Count == 1, "must not accept an unanchored frame merely because it looks contiguous");
        Assert.True(_receiver.RequestFromPosition == FromStart, "the mismatch must fall back to a walk");
        Assert.True(_receiver.IsAwaitingReplay);
        Assert.Equal(-1, _receiver.ReplaySessionId);
        Assert.False(_receiver.IsCaughtUp);
    }

    // The same rotation seen one step earlier: the Replayer answers "nothing to replay" because the position is
    // already at its recording's tip. For a walk that means caught up; for a resume it cannot, because we resumed
    // only on account of a hole we know is open.
    [Fact(DisplayName = "NoReplayNeeded over an open hole falls back to a walk")]
    public void NoReplayNeededOverAnOpenHoleFallsBackToAWalk()
    {
        DeliverTap(1);
        DeliverTap(2);
        Assert.True(_receiver.IsCaughtUp);
        DeliverTap(6); // gap -> resume
        Assert.NotEqual(FromStart, _receiver.RequestFromPosition);

        AnswerReplaying(NoReplayNeeded, 0);

        Assert.True(_receiver.RequestFromPosition == FromStart, "re-walk instead of believing it");
        Assert.True(_receiver.IsAwaitingReplay);
        Assert.False(_receiver.IsCaughtUp, "the hole above globalSeqNo=2 is still open");
    }

    // A resume ends at the bound it was given, so it must declare itself caught up and hand the slot back, or the
    // slot sits until the idle TTL reclaims it.
    [Fact(DisplayName = "a resume reaching its bound catches up and requests nothing more")]
    public void ResumeReachingItsBoundCatchesUpAndRequestsNothingMore()
    {
        DeliverTap(1);
        Assert.Single(_caughtUpAt);
        DeliverTap(5); // gap -> resume
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
    // rediscover the hole until some later, unrelated tap frame exposed it.
    [Fact(DisplayName = "a resume reaching its bound with a retained hole still open re-walks instead")]
    public void ResumeReachingItsBoundWithARetainedHoleStillOpenReWalksInstead()
    {
        DeliverTap(1);
        Assert.Single(_caughtUpAt);
        DeliverTap(5); // gap -> resume anchored on frame 1; 5 is retained ahead of the hole
        AnswerReplaying(7, 500);

        DeliverReplay(1); // opens on the anchored frame -> anchor consumed, frame deduped
        DeliverReplay(2); // the bound is reached having covered only up to 2: 3, 4 unfilled

        CompleteReplay();

        Assert.False(_receiver.IsCaughtUp, "must not declare caught up with a retained frame behind a hole");
        Assert.True(_caughtUpAt.Count == 1, "no false re-convergence notification");
        Assert.True(_receiver.RequestFromPosition == FromStart, "falls back to a walk rather than trusting the bound");
        Assert.True(_receiver.IsAwaitingReplay);
        Assert.Equal(-1, _receiver.ReplaySessionId);
    }

    // The overflow half of the same guard. When the FIFO had to DROP tap frames the frontier is short by frames that
    // are gone from the tap for good, and every retained frame draining cleanly says nothing about them.
    [Fact(DisplayName = "a resume reaching its bound after dropped tap frames re-walks instead")]
    public void ResumeReachingItsBoundAfterDroppedTapFramesReWalksInstead()
    {
        DeliverTap(1);
        Assert.Single(_caughtUpAt);
        DeliverTap(3);               // gap -> resume anchored on frame 1; 3 is retained
        DeliverTapTooBigToRetain(4); // ahead of the hole too, but dropped rather than retained
        AnswerReplaying(7, 500);

        DeliverReplay(1); // opens on the anchored frame -> anchor consumed, frame deduped
        DeliverReplay(2); // closes the hole the resume was asked to cover, draining retained frame 3

        CompleteReplay();

        Assert.False(_receiver.IsCaughtUp, "frame 4 was dropped: the drained frontier is not the real one");
        Assert.True(_caughtUpAt.Count == 1, "no false re-convergence notification");
        Assert.True(_receiver.RequestFromPosition == FromStart,
                    "re-walks now rather than leaving the hole for a later tap gap");
        Assert.True(_receiver.IsAwaitingReplay);
    }

    // ── releasing the replay slot ─────────────────────────────────────────────────────────────────

    // The release was a fire-and-forget offer, so an offer that did not land was indistinguishable from one that
    // did, and nothing re-sent it, leaving the slot to the idle TTL. There is no publication in these tests, so every
    // send here is a send that never went out.
    [Fact(DisplayName = "a ReplayComplete that never went out stays pending")]
    public void AReplayCompleteThatNeverWentOutStaysPending()
    {
        DeliverTap(1);
        DeliverTap(5); // gap -> resume
        AnswerReplaying(7, 500);
        DeliverReplay(1);
        DeliverReplay(2);
        DeliverReplay(3);
        DeliverReplay(4);
        Assert.False(_receiver.CompletePending, "nothing to release until the resume reaches its bound");

        CompleteReplay();

        Assert.True(_receiver.IsCaughtUp);
        Assert.True(_receiver.CompletePending, "it never reached the wire: remember it");
        _receiver.DoTimers(false);
        Assert.True(_receiver.CompletePending, "the duty cycle retries the release rather than forgetting it");
    }

    // ReplayComplete names only the clientId, so one landing after a NEW request took a fresh slot would free the
    // slot that replay is riding.
    [Fact(DisplayName = "a new request drops a ReplayComplete that never went out")]
    public void ANewRequestDropsAReplayCompleteThatNeverWentOut()
    {
        DeliverTap(1);
        DeliverTap(5); // gap -> resume
        AnswerReplaying(7, 500);
        DeliverReplay(1);
        DeliverReplay(2);
        DeliverReplay(3);
        DeliverReplay(4);
        CompleteReplay();
        Assert.True(_receiver.CompletePending);

        DeliverTap(9); // a second gap -> new request, taking a fresh slot

        Assert.True(_receiver.IsAwaitingReplay);
        Assert.False(_receiver.CompletePending, "dropped: releasing now would free the new slot");
    }

    // ── the end of a walk ─────────────────────────────────────────────────────────────────────────
    // A walk ends at its bound, and reaching it is not by itself permission to declare caught up.

    [Fact(DisplayName = "frames still retained when a walk reaches its bound force a re-walk rather than caught-up")]
    public void RetainedResidualForcesReWalk()
    {
        AttachReplay(11, 4096);
        DeliverTap(5); // a hole below it that this walk never reached
        DeliverReplay(1);
        Assert.Equal(1, _receiver.RetainedFrameCount);

        CompleteReplay();

        Assert.False(_receiver.IsCaughtUp, "declaring caught up here would close the hole by fiat");
        Assert.Equal(FromStart, _receiver.RequestFromPosition);
        Assert.True(_receiver.IsAwaitingReplay);
    }

    // The negative control for the guard above: a walk whose retained frames all drained must finish.
    [Fact(DisplayName = "a walk reaching its bound with every retained frame drained catches up")]
    public void AWalkWithEveryRetainedFrameDrainedCatchesUp()
    {
        AnswerReplaying(7, 500);
        DeliverReplay(1);
        DeliverTap(3);    // ahead of the hole at 2 -> retained
        DeliverReplay(2); // closes it, so 3 drains straight over

        CompleteReplay();

        Assert.True(_receiver.IsCaughtUp, "nothing is outstanding: the guard must not fire here");
        Assert.False(_receiver.IsAwaitingReplay, "not a spurious re-walk");
    }

    // The same guard driven through to convergence: the re-walk must both happen AND be able to finish. Clearing the
    // overflow on drain forgets the drop and catches up over the hole; never clearing it leaves a client that can
    // never declare itself caught up again.
    [Fact(DisplayName = "a walk reaching its bound after dropped tap frames re-walks, then catches up")]
    public void AWalkAfterDroppedTapFramesReWalksThenCatchesUp()
    {
        AnswerReplaying(7, 500);
        DeliverReplay(1);
        DeliverReplay(2);
        DeliverTapTooBigToRetain(4); // the tap runs ahead of the walk, and this one is dropped

        CompleteReplay();

        Assert.False(_receiver.IsCaughtUp, "the walk reached its bound, but a tap frame above it was dropped");
        Assert.True(_caughtUpAt.Count == 0, "no false convergence notification: this opens the accept gate");
        Assert.True(_receiver.RequestFromPosition == FromStart, "re-walks rather than trusting the bound");

        // The re-walk replays what the drop lost, and this time nothing is dropped.
        AnswerReplaying(8, 900);
        DeliverReplay(3);
        DeliverReplay(4);
        CompleteReplay();

        Assert.True(_receiver.IsCaughtUp, "a re-walk that dropped nothing must be able to finish");
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
        for (long globalSeqNo = 1; globalSeqNo <= 20; globalSeqNo++)
        {
            DeliverReplay(globalSeqNo);
            Assert.False(CheckProgressAt(ClockMs + globalSeqNo * 60_000), $"at globalSeqNo {globalSeqNo}");
        }
    }

    [Fact(DisplayName = "a gap after a healthy run starts its own episode rather than inheriting one")]
    public void AGapAfterAHealthyRunStartsItsOwnEpisode()
    {
        CaptureLogs();
        Assert.False(CheckProgressAt(ClockMs)); // an episode anchored during cold start
        DeliverTap(1);                          // ... which then converges
        Assert.True(_receiver.IsCaughtUp);

        DeliverTap(5); // much later, a tap gap: not caught up again
        Assert.False(_receiver.IsCaughtUp);

        // Inheriting the cold-start clock would report this the instant the gap opened, before the re-walk it
        // triggers had any chance to converge.
        Assert.False(CheckProgressAt(PastDeadlineMs));
        Assert.True(_logged.Count == 1, "the tap-gap warn only: no convergence report");
        Assert.Equal(Logger.CoreEventCode.TapGap, _logged[0].Code);
    }

    [Fact(DisplayName = "a caught-up client that simply goes quiet is not reported")]
    public void ACaughtUpClientThatGoesQuietIsNotReported()
    {
        CaptureLogs();
        DeliverTap(1);
        Assert.True(_receiver.IsCaughtUp);

        // A tap that goes silent is a gateway's tap-stall watchdog's business, and it is not a convergence failure
        // at all: diagnosing it as one puts a second, wrong explanation on the same event.
        Assert.False(CheckProgressAt(ClockMs));
        Assert.False(CheckProgressAt(PastDeadlineMs));
        Assert.Empty(_logged);
    }

    [Fact(DisplayName = "the report names the state that tells the causes apart")]
    public void TheReportNamesTheStateThatTellsTheCausesApart()
    {
        DeliverTap(1);
        DeliverTap(5); // gap -> awaiting a replay
        DeliverUnavailable(_receiver.RequestId);

        CaptureLogs(); // installed after the refusal, so it captures only the report below
        Assert.False(CheckProgressAt(ClockMs));
        Assert.True(CheckProgressAt(PastDeadlineMs));

        Assert.Single(_logged);
        string text = _logged[0].Message;
        // Without these an operator cannot tell a refused node from one whose recording cannot cover the hole.
        Assert.Contains("lastGlobalSeqNo=1", text);
        Assert.Contains("awaitingReplay=true", text);
        Assert.Contains("replayerUnavailable=true", text);
    }

    [Fact(DisplayName = "the report stops naming a refusal once the Replayer serves again")]
    public void TheReportStopsNamingARefusalOnceTheReplayerServesAgain()
    {
        DeliverTap(1);
        DeliverTap(5); // gap -> awaiting a replay
        DeliverUnavailable(_receiver.RequestId);
        AnswerReplaying(7, 900);

        CaptureLogs(); // installed after the exchange above, so it captures only the report below
        Assert.False(CheckProgressAt(ClockMs));
        Assert.True(CheckProgressAt(PastDeadlineMs));

        Assert.Single(_logged);
        // The field is state, not "was ever refused": a stale true points the operator at a Replayer that is
        // answering fine, and away from the attached replay that is actually not delivering.
        Assert.Contains("replayerUnavailable=false", _logged[0].Message);
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
            new ReplayerRecovery(ClientId, _actions, e => _dispatched.Add(e.GlobalSeqNo),
                                 (leaderMemberId, leadershipTermId, globalSeqNo) => changes.Add(globalSeqNo), null);
        DeliverLeadershipChanged(1);

        Assert.Equal(new List<long> { 1 }, changes);
        Assert.True(_dispatched.Count == 0, "delivered twice");
    }

    [Fact(DisplayName = "onSequenced is required")]
    public void OnSequencedIsRequired()
    {
        Assert.Throws<ArgumentNullException>(() => new ReplayerRecovery(ClientId, _actions, null, null, null));
    }

    // ── Client id in use ───────────────────────────────────────────────────────────────────────────

    [Fact(DisplayName = "a notice that this client's id is in use latches, and is reported once")]
    public void AClientIdInUseNoticeLatchesAndIsReportedOnce()
    {
        CaptureLogs();
        Control(ControlMessages.ReplayClientIdInUse(ClientId));
        Control(ControlMessages.ReplayClientIdInUse(ClientId));

        Assert.True(_receiver.IsClientIdInUse);
        Assert.Single(_logged);
        Assert.Equal(Logger.CoreEventCode.ReplayClientIdCollision, _logged[0].Code);
    }

    [Fact(DisplayName = "a notice for another client's id is ignored")]
    public void AClientIdInUseNoticeForAnotherClientIsIgnored()
    {
        Control(ControlMessages.ReplayClientIdInUse(ClientId + 1));

        Assert.False(_receiver.IsClientIdInUse);
    }

    // ── restoring a snapshot ──────────────────────────────────────────────────────────────────────

    [Fact(DisplayName =
              "a restoring cold start asks the Replayer about its newest local snapshot, holding the live tap")]
    public void RestoringColdStartQueriesItsSnapshotFirst()
    {
        WriteSnapshot(1);
        WriteSnapshot(2);
        StartRestoring();

        Assert.Equal(1, _actions.QueriesSent);
        Assert.Equal(Source, _actions.QuerySourceId);
        Assert.True(_actions.QueryRound == 2, "the newest round it holds");
        Assert.False(_receiver.IsAwaitingReplay, "no replay before the answer");
        Assert.True(_receiver.IsRecovering);

        DeliverTap(40); // mid-stream, and no baseline: held, not fatal
        Assert.Empty(_dispatched);
    }

    [Fact(DisplayName = "an unanswered snapshot query is resent; a pending answer holds it")]
    public void UnansweredSnapshotQueryIsResent()
    {
        WriteSnapshot(2);
        StartRestoring();
        AdvancePastTimers();
        Assert.Equal(2, _actions.QueriesSent);

        DeliverPending(_receiver.RequestId);
        _actions.ClockMs += 100;
        _receiver.DoTimers(false);
        Assert.Equal(2, _actions.QueriesSent);
    }

    [Fact(DisplayName = "with no local snapshot, the cold start asks nothing and walks the recording from its start")]
    public void NoSnapshotWalksFromTheStart()
    {
        StartRestoring();

        Assert.Equal(0, _actions.QueriesSent);
        Assert.False(_receiver.IsRestoring);
        Assert.True(_receiver.IsAwaitingReplay);
        Assert.Equal(FromStart, _receiver.RequestFromPosition);
    }

    [Fact(DisplayName = "a snapshot is restored from its file, dispatching nothing, then resumes anchored at its cut")]
    public void SnapshotIsRestoredThenResumesAtItsCut()
    {
        WriteSnapshot(2);
        StartRestoring();
        AnswerLocation(2, 1);
        Assert.True(_receiver.IsRestoring);
        Assert.False(_receiver.IsAwaitingReplay, "the records come from the file, not a replay");

        _receiver.DoTimers(false);

        Assert.Equal(new List<string> { "header 4 2", "0 7" }, _restored);
        Assert.Empty(_dispatched);
        Assert.False(_receiver.IsRestoring);
        Assert.Equal(Cut, _receiver.LastGlobalSeqNo);
        Assert.True(_receiver.CurrentLeaderMemberId == 2, "the header's leader");
        Assert.True(_receiver.IsAwaitingReplay);
        Assert.NotEqual(FromStart, _receiver.RequestFromPosition);
        Assert.Equal(Cut * 1024, _receiver.RequestFromPosition);

        AttachReplay(22, 64 * 1024);
        DeliverReplayFrame(RestoreFrames.Started(Cut, 2), Cut); // the anchor, already dispatched
        DeliverReplay(11);
        DeliverReplay(12);
        Assert.Equal(new List<long> { 11, 12 }, _dispatched);
    }

    [Fact(DisplayName = "a restore reads a bounded number of records per duty cycle, holding the tap until its last")]
    public void RestoreIsPacedAcrossDutyCycles()
    {
        var records = new byte[ReplayerRecovery.MaxRestoreRecordsPerCycle + 1][];
        records[0] = SnapshotHeaderRecord;
        for (int i = 1; i < records.Length; i++)
        {
            records[i] = RestoreFrames.Record(i);
        }
        RestoreFrames.Write(_store, 2, 1, records);
        StartRestoring();
        AnswerLocation(2, 1, RestoreFrames.Digest.Of(records));

        _receiver.DoTimers(false);
        Assert.True(_receiver.IsRestoring);
        Assert.Equal(ReplayerRecovery.MaxRestoreRecordsPerCycle, _restored.Count);
        DeliverTap(40);
        Assert.Empty(_dispatched);

        _receiver.DoTimers(false);
        Assert.False(_receiver.IsRestoring);
        Assert.Equal(records.Length, _restored.Count);
        Assert.True(_receiver.IsAwaitingReplay);
    }

    [Fact(DisplayName =
              "a local snapshot the log does not confirm gives way to the next older one, and the last to a walk")]
    public void UnconfirmedSnapshotGivesWayToAnOlderOne()
    {
        for (long round = 1; round <= 4; round++)
        {
            WriteSnapshot(round);
        }
        string cut = Path.Combine(_directory.Path, "instance-" + (_stores - 1), "3.snapshot");
        File.WriteAllBytes(cut, File.ReadAllBytes(cut)[..40]);
        StartRestoring();

        Assert.Equal(4, _actions.QueryRound);
        AnswerLocation(-1, 1); // no sequenced end for round 4 in the index
        Assert.Equal(3, _actions.QueryRound);
        AnswerLocation(3, 1); // its file is cut short
        Assert.Equal(2, _actions.QueryRound);
        AnswerLocation(2, 1, new RestoreFrames.Digest(2, 26, 0xBAD)); // its file is not the one sequenced
        Assert.Equal(1, _actions.QueryRound);
        AnswerLocation(1, 1);
        _receiver.DoTimers(false);
        Assert.Equal(new List<string> { "header 4 2", "0 7" }, _restored);
        Assert.Equal(Cut, _receiver.LastGlobalSeqNo);

        SetUp();
        WriteSnapshot(2);
        StartRestoring();
        AnswerLocation(-1, 1);
        Assert.True(_actions.QueriesSent == 1, "nothing older to ask about");
        Assert.False(_receiver.IsRestoring);
        Assert.True(_receiver.IsAwaitingReplay);
        Assert.Equal(FromStart, _receiver.RequestFromPosition);
    }

    [Fact(DisplayName = "after a restore, a fall-back to the walk resumes at the snapshot instead")]
    public void AfterARestoreTheWalkFallbackResumesAtTheSnapshot()
    {
        WriteSnapshot(2);
        StartRestoring();
        AnswerLocation(2, 1);
        _receiver.DoTimers(false);
        AttachReplay(22, 64 * 1024);

        DeliverReplay(12); // not the anchor

        Assert.True(_receiver.RequestFromPosition != FromStart, "the start holds nothing this instance may replay");
        Assert.Equal(Cut * 1024, _receiver.RequestFromPosition);
        AttachReplay(23, 64 * 1024);
        DeliverReplayFrame(RestoreFrames.Started(Cut, 2), Cut);
        DeliverReplay(11);
        Assert.Equal(new List<long> { 11 }, _dispatched);
    }

    [Fact(DisplayName = "a snapshot that cannot be restored stops recovery: its format, its header, or its records")]
    public void UnrestorableSnapshotStopsRecovery()
    {
        WriteSnapshot(2);
        StartRestoring();
        AnswerLocation(2, 9);
        Assert.Contains("formatVersion 9", _receiver.RestoreFailure);
        Assert.False(_receiver.IsRecovering);
        DeliverTap(40); // no baseline, and no longer an error either
        Assert.Empty(_dispatched);

        SetUp();
        byte[] header = (byte[])SnapshotHeaderRecord.Clone();
        header[0] = 9;
        RestoreFrames.Write(_store, 2, 1, header, SnapshotRecord);
        StartRestoring();
        AnswerLocation(2, 1, RestoreFrames.Digest.Of(header, SnapshotRecord));
        _receiver.DoTimers(false);
        Assert.Contains("header of version 9", _receiver.RestoreFailure);

        SetUp();
        WriteSnapshot(2);
        string file = Path.Combine(_directory.Path, "instance-" + (_stores - 1), "2.snapshot");
        byte[] damaged = File.ReadAllBytes(file);
        damaged[2 + SnapshotHeaderRecord.Length + 2] ^= 1; // the record's first byte; the trailer still matches
        File.WriteAllBytes(file, damaged);
        StartRestoring();
        AnswerLocation(2, 1);
        _receiver.DoTimers(false);
        Assert.Contains("damaged", _receiver.RestoreFailure);
        Assert.False(_receiver.IsRecovering);
        Assert.Null(new ReplayerRecovery(ClientId, _actions, Ignore, null, null).RestoreFailure);
    }

    [Fact(DisplayName =
              "a restart restores the snapshot again, holding the tap, and dispatches everything after its cut")]
    public void RestartRestoresAgainAndRedispatchesAfterTheCut()
    {
        WriteSnapshot(2);
        StartRestoring();
        AnswerLocation(2, 1);
        _receiver.DoTimers(false);
        AttachReplay(22, 64 * 1024);
        DeliverReplayFrame(RestoreFrames.Started(Cut, 2), Cut);
        DeliverReplay(11);
        DeliverTap(12);
        Assert.True(_receiver.IsCaughtUp);
        _dispatched.Clear();
        _restored.Clear();

        _receiver.Restart();

        Assert.False(_receiver.IsCaughtUp);
        Assert.True(_receiver.ReplaySessionId == -1, "the replay in flight is dropped");
        Assert.Equal(2, _actions.QueriesSent);
        DeliverTap(13);
        Assert.Empty(_dispatched);
        AnswerLocation(2, 1);
        _receiver.DoTimers(false);
        Assert.Equal(new List<string> { "header 4 2", "0 7" }, _restored);
        Assert.Equal(Cut, _receiver.LastGlobalSeqNo);
        AttachReplay(23, 64 * 1024);
        DeliverReplayFrame(RestoreFrames.Started(Cut, 2), Cut);
        DeliverReplay(11);
        DeliverReplay(12);
        Assert.Equal(new List<long> { 11, 12, 13 }, _dispatched); // 13 from the tap, held until contiguous
    }

    [Fact(DisplayName = "a restart with no snapshot to restore walks the recording from its start again")]
    public void RestartWithoutASnapshotWalksFromTheStart()
    {
        DeliverTap(1);
        DeliverTap(2);

        _receiver.Restart();

        Assert.False(_receiver.IsCaughtUp);
        Assert.Equal(0, _receiver.LastGlobalSeqNo);
        Assert.True(_receiver.IsAwaitingReplay);
        Assert.Equal(FromStart, _receiver.RequestFromPosition);
        AttachReplay(21, 64 * 1024);
        DeliverReplay(1);
        DeliverReplay(2);
        Assert.Equal(new List<long> { 1, 2, 1, 2 }, _dispatched);
    }

    // ── helpers ───────────────────────────────────────────────────────────────────────────────────

    private static void Ignore(SequencedEvent sequencedEvent)
    {
    }

    private void StartRestoring()
    {
        _receiver.RestoreFrom(Source, _store, new RecordingRestoreHandler(_restored));
        _receiver.Start();
    }

    // This instance's file of `round`: the header and one record.
    private void WriteSnapshot(long round)
    {
        RestoreFrames.Write(_store, round, 1, SnapshotHeaderRecord, SnapshotRecord);
    }

    // The Replayer's answer: round `round` cut at Cut and ending as the files here do, or none.
    private void AnswerLocation(long round, long formatVersion)
    {
        AnswerLocation(round, formatVersion, RestoreFrames.Digest.Of(SnapshotHeaderRecord, SnapshotRecord));
    }

    private void AnswerLocation(long round, long formatVersion, RestoreFrames.Digest end)
    {
        Control(RestoreFrames.Location(ClientId, _receiver.RequestId, round, Cut, Cut * 1024, formatVersion, end));
    }

    private void DeliverReplayFrame(byte[] frame, long globalSeqNo)
    {
        _wire.Wrap(frame);
        _receiver.OnFrame(_wire, 0, frame.Length, globalSeqNo * 1024, ReceiveNs, true);
    }

    // Starts capturing log events here rather than in the constructor: several cases must see only what follows
    // their own setup, and the twins install their sinks at the same points for the same reason.
    private void CaptureLogs()
    {
        _logged.Clear();
        Logger.Install(new CallbackLoggerSink(_logged.Add));
    }

    // Refusals only: a re-request logs its own line between two of them, so Count cannot count these.
    private int Refusals()
    {
        return _logged.Count(e => Logger.CoreEventCode.ReplayUnavailable.Equals(e.Code));
    }

    // Retained-buffer overflows only: every gap/re-walk warn shares TapGap, so only the text separates them.
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

    private void DeliverUnavailable(long requestId)
    {
        Control(ControlMessages.ReplayUnavailable(ClientId, requestId));
    }

    private void Control(byte[] message)
    {
        _wire.Wrap(message);
        _receiver.OnControl(_wire, 0, message.Length);
    }

    private void DeliverLeadershipChanged(long globalSeqNo)
    {
        byte[] frame = Frames.LeadershipChanged(globalSeqNo, 2, 3);
        _wire.Wrap(frame);
        _receiver.OnFrame(_wire, 0, frame.Length, globalSeqNo * 1024, ReceiveNs, false);
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

    private void DeliverReplay(long globalSeqNo)
    {
        byte[] frame = Frames.ClusterHeartbeat(globalSeqNo);
        _wire.Wrap(frame);
        _receiver.OnFrame(_wire, 0, frame.Length, globalSeqNo * 1024, ReceiveNs, true);
    }

    private void DeliverTap(long globalSeqNo)
    {
        DeliverTapAt(globalSeqNo, globalSeqNo * 1024);
    }

    private void DeliverTapAt(long globalSeqNo, long position)
    {
        byte[] frame = Frames.ClusterHeartbeat(globalSeqNo);
        _wire.Wrap(frame);
        _receiver.OnFrame(_wire, 0, frame.Length, position, ReceiveNs, false);
    }

    // A live tap frame too big for the retained-ahead FIFO to hold: a heartbeat zero-padded past one RetainBlock,
    // which RetainFrame refuses outright. The cheapest of its three overflow triggers to drive; the other two need
    // 65536 frames or 16 MiB.
    private void DeliverTapTooBigToRetain(long globalSeqNo)
    {
        DeliverTapPadded(globalSeqNo, OversizedFrameLength);
    }

    // A live tap frame of `length` bytes: a heartbeat, decoded from the front, zero-padded to it.
    private void DeliverTapPadded(long globalSeqNo, int length)
    {
        var frame = new byte[length];
        Frames.ClusterHeartbeat(globalSeqNo).CopyTo(frame, 0);
        _wire.Wrap(frame);
        _receiver.OnFrame(_wire, 0, length, globalSeqNo * 1024, ReceiveNs, false);
    }
}
