package org.limitless.seqeron.replayer.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.limitless.seqeron.replayer.server.ReplayerService;
import org.limitless.seqeron.sbe.frame.ClusterHeartbeatEncoder;
import org.limitless.seqeron.sbe.replay.ReplayPendingEncoder;
import org.limitless.seqeron.sbe.replay.ReplayUnavailableEncoder;
import org.limitless.seqeron.sbe.replay.ReplayingEncoder;
import org.limitless.seqeron.util.Logger;

/**
 * Unit tests for the walk / gap-recovery state machine, and the twin of the C++
 * {@code ReplayerRecoveryTest.cpp}. {@link ReplayerRecovery} holds no Aeron runtime and no clock of its
 * own, so the suite drives it directly: its transport is recorded through {@link ReplayerRecoveryActions}
 * and its clock is owned by the test.
 *
 * <p>Case for case with the C++ twin, in the same order, so the two files walk side by side — the ports
 * are hand-kept and nothing but this correspondence enforces it. Where a name differs it is because the
 * Java case predates the port and already covered the same ground.
 */
class ReplayerRecoveryTest {
    private static final int CLIENT_ID = 4;
    private static final long NO_REPLAY_NEEDED = ReplayerService.NO_REPLAY_NEEDED;

    /** The walk terminator: nothing left to replay AND no recording named. */
    private static final long CHAIN_EXHAUSTED = -1;

    /**
     * The same field as {@link #CHAIN_EXHAUSTED} where the reply carries a session: "this answer names no
     * recording", so the walk-segment mismatch check has nothing to compare against.
     */
    private static final long NO_RECORDING = -1;

    /** Arrival stamp; carried through to SequencedEvent, asserted on by no test here. */
    private static final long RECEIVE_NS = 0;

    /** Stands in for the wall clock. The absolute value is arbitrary; only applied deltas matter. */
    private static final long CLOCK_MS = 3 * 60 * 60 * 1000L;

    /**
     * Past {@code RESEND_INTERVAL_MS} (500) and {@code REPLAY_STALL_TIMEOUT_MS} (5000) alike, so one duty
     * cycle fires whichever of the two the state under test has armed.
     */
    private static final long PAST_EVERY_TIMER_MS = 6_000;

    /** Past {@code RECOVERY_PROGRESS_TIMEOUT_MS}, the convergence alarm's deadline. */
    private static final long PAST_DEADLINE_MS = CLOCK_MS + 30_000;

    /** Bigger than one {@code RetainBlock}, which is what makes {@code retainFrame} refuse outright. */
    private static final int OVERSIZED_FRAME_LENGTH = 8192;

    private final List<Long> dispatched = new ArrayList<>();
    private final List<Long> caughtUpAt = new ArrayList<>();
    private final List<Logger.LoggerEvent> logged = new ArrayList<>();
    private RecordingActions actions;
    private ReplayerRecovery receiver;

    /** The transport recorded rather than performed — no publication, so nothing ever reaches the wire. */
    private static final class RecordingActions implements ReplayerRecoveryActions {
        long clockMs = CLOCK_MS;
        int requestsSent;
        int heartbeatsSent;

        @Override
        public void sendReplayRequest(final long requestId, final int segmentIndex, final long fromPosition) {
            ++requestsSent;
        }

        // False like a receiver with no publication: every send here is one that never went out, which is
        // the state the ReplayComplete retry tests need.
        @Override
        public boolean sendReplayComplete() {
            return false;
        }

        @Override
        public boolean sendReplayHeartbeat() {
            ++heartbeatsSent;
            return false;
        }

        @Override
        public void openReplay(final long replaySessionId) {
        }

        @Override
        public void closeReplay() {
        }

        @Override
        public void recoveryStalled(final boolean stalled) {
        }

        @Override
        public Integer memberId() {
            return 0;
        }

        @Override
        public long nowMs() {
            return clockMs;
        }
    }

    /**
     * Deliberately does NOT call {@code start()} — the C++ twin's fixture does not either, and several
     * cases here assert on the pre-request state ({@code isRecovering()} false, nothing awaited). The two
     * that need the cold-start request ask for it themselves.
     */
    @BeforeEach
    void setUp() {
        dispatched.clear();
        caughtUpAt.clear();
        logged.clear();
        actions = new RecordingActions();
        receiver = new ReplayerRecovery(CLIENT_ID, actions, event -> dispatched.add(event.globalSeqNo()), null,
                                        () -> caughtUpAt.add((long)dispatched.size()));
    }

    @AfterEach
    void tearDown() {
        Logger.reset();
    }

    // ── baseline ──────────────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("cold start requests the chain walk from segment 0")
    void coldStartWalksFromSegmentZero() {
        receiver.start();

        assertTrue(receiver.isAwaitingReplay());
        assertEquals(0, receiver.walkSegmentIndex());
        assertEquals(0, receiver.requestFromPosition());
    }

    @Test
    @DisplayName("a first frame at globalSeqNo 1 is the baseline and puts the client live")
    void firstFrameAtOneIsAccepted() {
        deliverTap(1);

        assertEquals(List.of(1L), dispatched);
        assertTrue(receiver.isCaughtUp());
    }

    @Test
    @DisplayName("a first frame that is not globalSeqNo 1 is fatal")
    void firstFrameMustBeOne() {
        attachReplay(11, 4096);

        assertThrows(IllegalStateException.class, () -> deliverReplay(7));
    }

    // Once the tap is dispatched mid-walk, a cold start races a live tap already carrying mid-stream
    // globalSeqNos against a replay that has not delivered 1 yet. That tap frame is neither a valid
    // baseline nor a fault — only the walk may establish the baseline.
    @Test
    @DisplayName("the live tap may not establish the baseline while the cold-start walk is in flight")
    void liveTapMayNotEstablishTheBaselineMidWalk() {
        answerReplaying(7, 500);
        assertTrue(receiver.isRecovering());

        deliverTap(15); // the live tip, far ahead of a replay that has not started delivering

        assertTrue(dispatched.isEmpty(), "must not adopt the live tap's mid-stream baseline");
        assertFalse(receiver.isCaughtUp());

        deliverReplay(1); // the walk supplies the real baseline

        assertEquals(List.of(1L), dispatched);
    }

    // ── gap detection and the replay-to-live seam ─────────────────────────────────────────────────

    @Test
    @DisplayName("a mid-stream gap withholds the out-of-order frame and requests a replay")
    void midStreamGapWithholdsTheOutOfOrderFrame() {
        deliverTap(1);
        deliverTap(2);
        assertEquals(2, dispatched.size());
        assertFalse(receiver.isAwaitingReplay());

        deliverTap(5); // skips 3, 4

        assertEquals(2, dispatched.size(), "the out-of-order frame itself must not be dispatched");
        assertTrue(receiver.isAwaitingReplay(), "a gap must re-request a replay");
        assertEquals(-1, receiver.replaySessionId(), "no session yet — only a request, until the Replayer answers");
    }

    @Test
    @DisplayName("a tap gap reports a structured diagnostic event")
    void tapGapReportsAStructuredDiagnosticEvent() {
        captureLogs();

        deliverTap(1);
        deliverTap(2);
        deliverTap(5); // skips 3, 4

        assertEquals(1, logged.size(), "exactly the one gap detected above, nothing from setup");
        final Logger.LoggerEvent event = logged.get(0);
        assertEquals(Logger.Component.ReplayerStreamReceiver, event.component());
        assertEquals(Logger.Severity.Warn, event.severity());
        assertEquals(Logger.EventCode.TapGap, event.code());
        assertEquals("tap gap: expected globalSeqNo=3 got 5 — resuming the recording at globalSeqNo=2",
                     event.message());
    }

    @Test
    @DisplayName("a live tap frame ahead of an in-flight walk is retained, not treated as a gap")
    void tapFrameMidWalkIsRetainedNotAGap() {
        attachReplay(11, 4096);
        final long requestIdBefore = receiver.requestId();

        deliverTap(9); // the tap runs ahead of the replay

        assertTrue(dispatched.isEmpty(), "no baseline yet — only the walk may establish one");
        assertEquals(1, receiver.retainedFrameCount());
        assertEquals(requestIdBefore, receiver.requestId(), "an in-flight walk must not be superseded");
    }

    @Test
    @DisplayName("a tap gap while the walk is still in flight does not supersede it")
    void tapGapMidWalkDoesNotSupersedeTheWalk() {
        attachReplay(11, 4096);
        deliverReplay(1); // baseline established, by the walk rather than by the tap
        final long requestIdBefore = receiver.requestId();

        deliverTap(5); // 2..4 are not lost — they are frames this walk has not reached yet

        assertEquals(requestIdBefore, receiver.requestId(), "an in-flight walk must run to completion");
        assertEquals(0, receiver.walkSegmentIndex(), "a mid-walk tap gap is expected, not a new gap");
        assertEquals(1, receiver.retainedFrameCount());
    }

    // The same rule one state later: not merely awaiting an answer but riding an assigned session.
    // Superseding the walk on each tap frame would restart it from segment 0 forever under any sustained
    // publish rate.
    @Test
    @DisplayName("a tap frame arriving during an assigned replay session does not supersede it")
    void tapFrameAheadOfAnAssignedReplayDoesNotSupersedeIt() {
        captureLogs();
        deliverTap(1);
        deliverTap(5); // the genuine steady-state gap
        assertTrue(receiver.isAwaitingReplay());
        assertEquals(1, logged.size());

        answerReplaying(99, 500);
        assertEquals(99, receiver.replaySessionId());
        assertFalse(receiver.isAwaitingReplay());
        assertTrue(receiver.isRecovering());

        deliverTap(12); // the tap running ahead, not a hole this walk will not cover

        assertEquals(99, receiver.replaySessionId(), "the in-flight walk must run to completion");
        assertFalse(receiver.isAwaitingReplay());
        assertEquals(-1, receiver.walkSegmentIndex(), "still the resume the gap asked for, not restarted");
        assertEquals(1, dispatched.size(), "the out-of-order frame itself is still withheld");
        assertEquals(1, logged.size(), "and it is not reported as a second gap");
    }

    // The replay->live seam: the tap is dispatched throughout a walk, so the frame at gseq == last + 1
    // lands the instant the replay reaches it. Discarding tap frames while recovering ends every walk one
    // guaranteed gap short of live.
    @Test
    @DisplayName("the tap frame at the seam is dispatched while the walk is still in flight")
    void liveTapFrameAtTheSeamIsDispatchedMidWalk() {
        answerReplaying(7, 500);
        deliverReplay(1);
        deliverReplay(2);
        assertEquals(2, dispatched.size());
        assertFalse(receiver.isCaughtUp(), "still riding replayed history");
        assertTrue(receiver.isRecovering());

        deliverTap(3); // the seam: first tap frame contiguous with what the replay delivered

        assertEquals(3, dispatched.size(), "the seam frame must be dispatched, not discarded as mid-walk noise");
        assertTrue(receiver.isCaughtUp(), "a contiguous frame off the live tap means we are following live");
    }

    @Test
    @DisplayName("a steady-state tap gap clears caught-up and resumes at the last dispatched frame")
    void steadyStateGapResumesAtLastFrame() {
        goLive();
        deliverTapAt(1, 1024);
        deliverTapAt(2, 2048);
        assertTrue(receiver.isCaughtUp());

        deliverTapAt(5, 8192); // 3 and 4 dropped

        assertFalse(receiver.isCaughtUp(), "consumers gate real decisions on this — it must not stay latched");
        assertTrue(receiver.isAwaitingReplay());
        assertEquals(-1, receiver.walkSegmentIndex(), "a gap resumes; it does not re-walk from a stale index");
        assertEquals(2048, receiver.requestFromPosition(), "resume at the frame last dispatched, not at 0");
        assertEquals(1, receiver.retainedFrameCount(), "the ahead-of-hole frame is kept, not dropped");
    }

    @Test
    @DisplayName("replayed frames already seen off the live tap are de-duped")
    void duplicateAndStaleReplayFramesAreDropped() {
        deliverTap(1);
        deliverTap(2);
        assertEquals(2, dispatched.size());

        deliverReplay(1);
        deliverReplay(2);
        assertEquals(2, dispatched.size(), "replayed frames already delivered off the live tap must be de-duped");

        deliverReplay(3);
        assertEquals(3, dispatched.size());
    }

    @Test
    @DisplayName("replayed history dispatches in order and de-dupes a redelivered frame")
    void dispatchesInOrderAndDeDupes() {
        attachReplay(11, 4096);

        deliverReplay(1);
        deliverReplay(2);
        deliverReplay(2); // redelivered
        deliverReplay(3);

        assertEquals(List.of(1L, 2L, 3L), dispatched);
        assertEquals(3, receiver.lastGlobalSeqNo());
    }

    // ── the Replayer's control-stream replies, and request/reply correlation ──────────────────────

    @Test
    @DisplayName("the chain running out marks the client caught up")
    void chainExhaustedMarksCaughtUp() {
        answerReplaying(NO_REPLAY_NEEDED, 0, CHAIN_EXHAUSTED);

        assertTrue(receiver.isCaughtUp());
        assertEquals(-1, receiver.walkSegmentIndex(), "steady/resume mode from here");
        assertFalse(receiver.isAwaitingReplay());
    }

    @Test
    @DisplayName("a Replaying carrying a session arms the replay without marking the client caught up")
    void replayingWithSessionArmsReplayWithoutMarkingCaughtUp() {
        answerReplaying(42, 1000);

        assertFalse(receiver.isCaughtUp(), "a session id means there IS history to replay first");
        assertFalse(receiver.isAwaitingReplay(), "onControl always clears awaiting once answered");
        assertEquals(42, receiver.replaySessionId());
    }

    @Test
    @DisplayName("a reply addressed to another replica on the shared control stream is ignored")
    void replayingForAnotherClientIdIsIgnored() {
        receiver.onControl(replayingBuffer(CLIENT_ID + 1, receiver.requestId(), 42, 1000, NO_RECORDING), 0,
                           replayingLength());

        assertEquals(-1, receiver.replaySessionId(), "a reply for a different replica must not be applied");
        assertFalse(receiver.isCaughtUp());
    }

    @Test
    @DisplayName("a completed segment advances the walk and re-requests the next one")
    void segmentCompleteAdvancesTheWalkAndReRequestsTheNextSegment() {
        answerReplaying(7, 500);
        assertEquals(7, receiver.replaySessionId());

        completeSegment();

        assertEquals(1, receiver.walkSegmentIndex(), "segment 0 done -> walk advances to segment 1");
        assertTrue(receiver.isAwaitingReplay(), "advancing re-requests the next segment");
        assertEquals(-1, receiver.replaySessionId(), "no session until the Replayer answers the new request");
    }

    // clientId alone cannot identify WHICH request a reply answers. A resend makes the Replayer stop the
    // in-flight session and start a new one, so both replies sit in order on the one shared control
    // publication and the stale one is always processed first.
    @Test
    @DisplayName("a reply carrying a superseded requestId is ignored")
    void staleReplyIsIgnored() {
        receiver.start();
        final long staleRequestId = receiver.requestId();

        receiver.onControl(replayingBuffer(staleRequestId - 1, 11, 4096, 7), 0, replayingLength());

        assertTrue(receiver.isAwaitingReplay(), "a stale reply must not stop the resend timer");
        assertEquals(-1, receiver.replaySessionId());
        assertNotEquals(11, receiver.replaySessionId());
    }

    @Test
    @DisplayName("the reply to the current request is still accepted after a stale one")
    void currentReplyIsStillAcceptedAfterAStaleOne() {
        final long stale = receiver.requestId();
        completeSegment(); // advances the walk -> new request, new requestId

        receiver.onControl(replayingBuffer(stale, 42, 900, NO_RECORDING), 0, replayingLength());
        answerReplaying(43, 1000);

        assertEquals(43, receiver.replaySessionId(), "the reply to the current request must be applied");
        assertFalse(receiver.isAwaitingReplay());
    }

    // A stale ReplayPending must not reset the request clock either: that is what paces the resend, so
    // honouring a superseded request's "wait" defers the retry the client is relying on.
    @Test
    @DisplayName("a ReplayPending for a superseded request does not push out the current resend deadline")
    void replayPendingForASupersededRequestIsIgnored() {
        final long stale = receiver.requestId();
        completeSegment();
        final int sends = actions.requestsSent;

        actions.clockMs += 300; // still inside the current request's resend interval
        deliverPending(stale);
        actions.clockMs += 300; // ... which has now elapsed, unless the stale "wait" pushed it out
        receiver.doTimers(false);

        assertEquals(sends + 1, actions.requestsSent,
                     "a superseded request's ReplayPending must not defer the current request's resend");
    }

    // ── a closed image, a completed segment, and the stall watchdog ───────────────────────────────

    @Test
    @DisplayName("a replay image closing at its bound completes the segment; short of it re-requests")
    void closedImageIsCompletionOnlyAtTheBound() {
        answerReplaying(11, 4096, 7);
        receiver.onReplayImageClosed(4096);
        assertEquals(1, receiver.walkSegmentIndex(), "at the bound: the segment is done");

        answerReplaying(12, 8192, 8);
        receiver.onReplayImageClosed(4000);
        assertEquals(1, receiver.walkSegmentIndex(), "short of the bound: the same segment is re-requested");
        assertTrue(receiver.isAwaitingReplay());
    }

    // Once Replaying arrives the resend timer is disarmed, and a bounded replay of an active recording
    // never closes its image — so an image that simply stops advancing leaves the client waiting on it
    // forever with nothing retrying.
    @Test
    @DisplayName("a replay that stops advancing re-requests the same segment")
    void replayThatStopsAdvancingReRequestsTheSameSegment() {
        captureLogs();
        answerReplaying(7, 500);
        final long requestId = receiver.requestId();
        assertFalse(receiver.isAwaitingReplay(), "a session was assigned, so nothing else is retrying");

        advancePastTimers();

        assertEquals(0, receiver.walkSegmentIndex(), "the walk must not advance over a segment that stalled");
        assertTrue(receiver.isAwaitingReplay(), "the same segment is re-requested instead");
        assertEquals(-1, receiver.replaySessionId());
        assertNotEquals(requestId, receiver.requestId(),
                        "re-requesting is a new request, so the reply to the stalled one is recognisably stale");
        assertEquals(1, logged.size(), "a stalled replay is reported, not silently absorbed");
    }

    // Session id 0 is an ordinary archive replaySessionId; -1 is the only value that means "no replay".
    // Testing >= 1 skips both watchdogs for it: no heartbeat, so the Replayer's idle TTL reclaims the slot
    // out from under a healthy replay, and no stall watchdog once it stops delivering.
    @Test
    @DisplayName("replay session 0 is heart-beaten and watchdogged like any other")
    void replaySessionZeroIsHeartbeatedAndWatchdoggedLikeAnyOther() {
        answerReplaying(0, 500);
        assertEquals(0, receiver.replaySessionId(),
                     "session 0 is a replay this client is riding, not the absence of one");

        advancePastTimers();

        assertEquals(1, actions.heartbeatsSent,
                     "a slot held on session 0 must still be heart-beaten, or the idle TTL reclaims it");
        assertTrue(receiver.isAwaitingReplay(), "and its stall watchdog must still re-request it");
        assertEquals(-1, receiver.replaySessionId());
    }

    // ── ReplayPending and ReplayUnavailable ───────────────────────────────────────────────────────

    @Test
    @DisplayName("ReplayPending holds at the gap without assigning a session")
    void replayPendingHoldsAtTheGapWithoutAssigningASession() {
        deliverTap(1);
        deliverTap(5); // gap -> awaiting a replay
        assertTrue(receiver.isAwaitingReplay());

        deliverPending(receiver.requestId());

        assertTrue(receiver.isAwaitingReplay(), "no free Replayer slot -> keep holding at the gap");
        assertEquals(-1, receiver.replaySessionId());
        assertEquals(1, dispatched.size(), "must not advance past the hole while pending");
    }

    // A Replayer whose archive failed the globalSeqNo-1 integrity check refuses rather than serving
    // history its own check rejected. Served anyway, the replay's first frame would not be 1 and this
    // client would abort, taking down every co-located app over one node's bad archive.
    @Test
    @DisplayName("ReplayUnavailable holds without aborting or advancing, and reports once per episode")
    void replayUnavailableHoldsWithoutAbortingOrAdvancing() {
        deliverTap(1);
        deliverTap(5); // gap -> awaiting a replay
        assertTrue(receiver.isAwaitingReplay());
        assertFalse(receiver.isCaughtUp());

        captureLogs(); // installed after the setup gap, so it captures only the refusal below
        deliverUnavailable(receiver.requestId());

        assertTrue(receiver.isAwaitingReplay(), "a refusal must not stop the resend timer");
        assertEquals(-1, receiver.replaySessionId());
        assertEquals(1, dispatched.size(), "must not advance past the hole");
        assertFalse(receiver.isCaughtUp(), "consumer gates must stay shut while history is unavailable");
        assertEquals(1, logged.size());
        assertEquals(Logger.Severity.Fault, logged.get(0).severity());
        assertEquals(Logger.EventCode.ReplayUnavailable, logged.get(0).code());

        // The Replayer answers every resend the same way; the fault line must not repeat per reply.
        deliverUnavailable(receiver.requestId());
        assertEquals(1, logged.size(), "one episode is one line, not one per 500ms resend");
    }

    // A second, distinct outage hours later must report itself: the latch is on the episode, not on the
    // process. The intervening reply is what ends the first episode.
    @Test
    @DisplayName("a refusal after the Replayer recovered is reported again")
    void aRefusalAfterTheReplayerRecoveredIsReportedAgain() {
        deliverTap(1);
        deliverTap(5); // gap -> awaiting a replay

        captureLogs();
        deliverUnavailable(receiver.requestId());
        assertEquals(1, refusals());

        answerReplaying(7, 900);
        // Later: that node's archive breaks again, its Replayer restarts refusing, and our replay stops
        // delivering — so we re-request (a new requestId) and are refused a second time.
        advancePastTimers();
        deliverUnavailable(receiver.requestId());

        assertEquals(2, refusals(), "a process-lifetime latch would leave every later outage silent");
    }

    // Being queued ends the episode too: a Replayer with integrityFailed latched answers ReplayUnavailable,
    // never ReplayPending, so a pending reply is proof it is serving again.
    @Test
    @DisplayName("a queued replay also ends the refusal episode")
    void aQueuedReplayAlsoEndsTheRefusalEpisode() {
        deliverTap(1);
        deliverTap(5);

        captureLogs();
        deliverUnavailable(receiver.requestId());
        deliverPending(receiver.requestId());
        advancePastTimers();
        deliverUnavailable(receiver.requestId());

        assertEquals(2, refusals());
    }

    @Test
    @DisplayName("a ReplayUnavailable for a superseded request says nothing and resets no clock")
    void replayUnavailableForASupersededRequestIsIgnored() {
        deliverTap(1);
        deliverTap(5);
        final long stale = receiver.requestId() - 1;
        final int sends = actions.requestsSent;

        captureLogs();
        deliverUnavailable(stale);
        actions.clockMs += 600; // > RESEND_INTERVAL_MS since the CURRENT request went out
        receiver.doTimers(false);

        assertTrue(logged.isEmpty(), "a stale refusal says nothing about the Replayer's current state");
        assertEquals(sends + 1, actions.requestsSent, "and must not reset the resend clock");
    }

    // ── the resend timer ──────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("a request the Replayer never answered is re-sent once the interval elapses")
    void stuckAwaitingReplayResendsAfterTheIntervalElapses() {
        deliverTap(1);
        deliverTap(5); // gap -> arms the resend timer
        assertTrue(receiver.isAwaitingReplay());
        final int sends = actions.requestsSent;

        actions.clockMs += 300; // inside RESEND_INTERVAL_MS (500)
        receiver.doTimers(false);
        assertEquals(sends, actions.requestsSent, "must not resend before RESEND_INTERVAL_MS elapses");
        assertTrue(receiver.isAwaitingReplay());

        actions.clockMs += 300; // 600ms since the request — past it
        receiver.doTimers(false);

        assertEquals(sends + 1, actions.requestsSent, "the duty cycle must re-send rather than give up");
        assertTrue(receiver.isAwaitingReplay(), "still stuck at the gap — a resend, not a new state");
        assertEquals(-1, receiver.replaySessionId());
        assertEquals(-1, receiver.walkSegmentIndex(), "a resend repeats the same request verbatim");
    }

    // Every send does ++requestId and onControl acts only on a reply carrying the CURRENT id, so a
    // per-poll retry runs the counter away and every reply that arrives is discarded as stale — leaving
    // the client awaiting a replay that can never correlate, hence never caught up.
    @Test
    @DisplayName("a request that never went out is not re-sent once per poll")
    void anUnsentRequestIsNotReSentOncePerPoll() {
        deliverTap(1);
        deliverTap(5); // gap -> a request that never goes out, there being no publication here
        final long before = receiver.requestId();

        for (int i = 0; i < 50; i++) {
            receiver.doTimers(false); // duty cycles, no clock advance
        }

        assertTrue(receiver.isAwaitingReplay(), "still waiting — nothing answered it");
        assertTrue(receiver.requestId() - before <= 1,
                   "the resend timer paces this; one id per poll outruns every reply in flight");
    }

    // ── isRecovering() and isCaughtUp() ───────────────────────────────────────────────────────────

    // isRecovering() is what separates "the tap is ahead of my replay" from "there is a hole": narrowing
    // it means a walk supersedes itself on its own in-flight frames, widening it means a real gap goes
    // unreported. This walks every transition it must track.
    @Test
    @DisplayName("the recovering flag tracks the walk and awaiting-replay states")
    void recoveringFlagTracksWalkAndAwaitingReplayState() {
        assertFalse(receiver.isRecovering(), "nothing in flight yet");

        deliverTap(1);
        deliverTap(5); // skips 2..4 -> gap, re-requests a replay
        assertTrue(receiver.isRecovering(), "awaiting the Replayer's answer is already mid-recovery");

        answerReplaying(7, 500);
        assertTrue(receiver.isRecovering(), "a session id means history remains to replay");

        deliverReplay(1); // opens on the anchored frame -> anchor consumed, frame deduped
        deliverReplay(2);
        deliverReplay(3);
        deliverReplay(4); // closes the hole, draining the retained frame 5
        completeSegment();
        assertFalse(receiver.isRecovering(), "a resume ends at its bound — there is no next segment to ask for");

        // The walk shape of the same transitions, entered the way production enters it: a resume whose
        // replay opens on a frame other than the one it anchored on falls back to the chain walk.
        deliverTap(9); // another gap -> another resume
        answerReplaying(8, 500);
        deliverReplay(42);
        assertEquals(0, receiver.walkSegmentIndex());
        assertTrue(receiver.isRecovering(), "awaiting the walk's first segment");

        answerReplaying(9, 500);
        // The walk has to close the hole it re-walked for: 9 is retained behind the missing 6..8, and the
        // terminator refuses to end recovery while anything is still stranded there.
        deliverReplay(6);
        deliverReplay(7);
        deliverReplay(8); // dispatching 8 drains the retained 9 straight over
        completeSegment();
        assertTrue(receiver.isRecovering(), "advancing to the next segment stays mid-walk");

        answerReplaying(NO_REPLAY_NEEDED, 0, CHAIN_EXHAUSTED);
        assertFalse(receiver.isRecovering(), "chain exhausted -> back to steady state");
    }

    // isCaughtUp() is a state, not a latch: consumers gate real decisions on it, and a gateway's tap-stall
    // watchdog measures silence in DISPATCHED frames. A re-walk dispatches nothing until the replay passes
    // the hole, so leaving it latched makes a gateway diagnose its own recovery as a stalled sequencer.
    @Test
    @DisplayName("a tap gap revokes caught-up until the stream goes contiguous again")
    void tapGapRevokesCaughtUpUntilTheStreamGoesContiguousAgain() {
        deliverTap(1);
        deliverTap(2);
        assertTrue(receiver.isCaughtUp());
        assertEquals(1, caughtUpAt.size());

        deliverTap(5); // skips 3, 4 -> gap (and 5 is retained, not dropped)
        assertFalse(receiver.isCaughtUp(), "a hole means we are demonstrably not following live");

        deliverReplay(2); // a resume opens on the frame it anchored at — already delivered, deduped
        deliverReplay(3);
        assertFalse(receiver.isCaughtUp(), "4 is still missing — the hole is not closed yet");
        assertEquals(1, caughtUpAt.size());

        deliverReplay(4); // closes the hole; the retained tap frame 5 then hands straight over

        assertTrue(receiver.isCaughtUp(), "the retained frame closes the seam with no further round trip");
        assertEquals(2, caughtUpAt.size(), "consumers must be able to re-arm on re-convergence");
    }

    // ── the retained-ahead FIFO ───────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("retained tap frames hand straight over when the replay reaches them")
    void retainedFramesCloseTheSeam() {
        attachReplay(11, 4096);
        deliverTap(3);
        deliverTap(4);

        deliverReplay(1);
        deliverReplay(2);

        assertEquals(List.of(1L, 2L, 3L, 4L), dispatched, "the seam closes with no residual");
        assertEquals(0, receiver.retainedFrameCount());
    }

    @Test
    @DisplayName("frames beyond the hole are retained and delivered exactly once when it closes")
    void framesBeyondTheHoleAreRetainedAndDeliveredOnceItCloses() {
        deliverTap(1);
        deliverTap(4); // gap -> retained
        deliverTap(5); // still ahead of the hole -> retained
        deliverTap(6);
        assertEquals(List.of(1L), dispatched, "nothing past the hole may be dispatched early");

        deliverReplay(1); // a resume opens on the frame it anchored at — already delivered, deduped
        deliverReplay(2);
        assertEquals(List.of(1L, 2L), dispatched, "3 is still missing");

        deliverReplay(3); // closes the hole -> 4, 5, 6 drain behind it

        assertEquals(List.of(1L, 2L, 3L, 4L, 5L, 6L), dispatched, "retained frames drain in order, exactly once");
        assertTrue(receiver.isCaughtUp());
    }

    // The ranges legitimately overlap, since a re-walk restarts from segment 0 while the tap keeps
    // arriving.
    @Test
    @DisplayName("retained frames the replay has meanwhile covered are not redelivered")
    void retainedFramesAlreadyCoveredByTheReplayAreNotRedelivered() {
        deliverTap(1);
        deliverTap(3); // gap -> retained
        deliverTap(4); // retained

        deliverReplay(1); // a resume opens on the frame it anchored at — already delivered, deduped
        deliverReplay(2);
        deliverReplay(3); // the replay covers a frame the tap already retained
        deliverReplay(4);

        assertEquals(List.of(1L, 2L, 3L, 4L), dispatched, "each globalSeqNo dispatched exactly once");
    }

    // A hole in REPLAYED history is the one invariant violation that produced no log and no counter: the
    // frame is dropped, the walk rides on, and recovery quietly never converges. Nothing unsafe follows —
    // contiguity still holds — so this reports rather than aborts.
    @Test
    @DisplayName("a gap in replayed history is reported once per episode")
    void gapInReplayedHistoryIsReportedOncePerEpisode() {
        deliverReplay(1);
        assertEquals(1, dispatched.size());

        captureLogs();    // installed after the baseline, so it captures only the hole below
        deliverReplay(7); // the recording chain does not cover 2..6

        assertEquals(1, dispatched.size(), "the frame past the hole must not be dispatched");
        assertEquals(1, logged.size());
        assertEquals(Logger.Severity.Warn, logged.get(0).severity());
        assertEquals(Logger.EventCode.TapGap, logged.get(0).code());

        deliverReplay(8);
        assertEquals(1, logged.size(), "report the episode, not every frame of a walk retrying the same chain");

        // A later, distinct episode must be reported again rather than swallowed by the latch.
        deliverReplay(2);
        assertEquals(2, dispatched.size());
        deliverReplay(9);
        assertEquals(2, logged.size(), "the latch clears once history goes contiguous again");
    }

    @Test
    @DisplayName("dropped tap frames are reported once per episode")
    void droppedTapFramesAreReportedOncePerEpisode() {
        deliverTap(1);
        deliverTap(3); // gap -> resume anchored on frame 1; 3 is retained

        captureLogs(); // installed after the setup gap, so it captures only the overflow lines
        deliverTapTooBigToRetain(4);
        deliverTapTooBigToRetain(5);
        assertEquals(1, overflowReports(), "one episode is one line, not one per dropped frame");

        // The re-walk is what covers the dropped frames, so the episode ends when it is requested.
        answerReplaying(7, 500);
        deliverReplay(1);
        deliverReplay(2);
        completeSegment();
        assertEquals(0, receiver.walkSegmentIndex());

        deliverTapTooBigToRetain(9);

        assertEquals(2, overflowReports(), "a later overflow is a distinct episode and names itself");
    }

    // ── the resume anchor ─────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("a resume replay that opens at the wrong frame falls back to the chain walk")
    void resumeAnchorMismatchFallsBackToWalk() {
        goLive();
        deliverTapAt(1, 1024);
        deliverTapAt(2, 2048);
        deliverTapAt(5, 8192); // gap → resume anchored on globalSeqNo 2
        attachReplay(12, 16384);

        deliverReplay(4); // not the anchor: the active recording rotated under us

        assertEquals(0, receiver.walkSegmentIndex(), "must fall back to the walk, which needs no position");
        assertEquals(0, receiver.requestFromPosition());
        assertTrue(receiver.isAwaitingReplay());
    }

    // The anchor is consumed by the first frame of ONE replay episode — a retry of that same logical
    // resume starts a NEW episode and must re-arm the check, or the retried replay's first frame rides
    // with nothing to validate against and a rotated recording's frame is dispatched merely because it
    // looks contiguous.
    @Test
    @DisplayName("a resume whose image closes short of its bound re-arms the anchor check")
    void replayImageClosingShortOfTheBoundOnAResumeReArmsTheAnchorCheck() {
        deliverTap(1);
        deliverTap(5); // gap -> resume anchored on frame 1
        answerReplaying(7, 500);
        deliverReplay(1); // opens on the anchored frame -> anchor consumed, frame deduped
        assertEquals(1, dispatched.size());

        receiver.onReplayImageClosed(300); // stopped under us before frame 2 arrived
        assertEquals(-1, receiver.walkSegmentIndex(), "still a resume retry, not a walk step");

        answerReplaying(8, 500);
        // The active recording rotated: the retried resume's first frame is NOT the frame it was anchored
        // on, but it happens to look contiguous.
        deliverReplay(2);

        assertEquals(1, dispatched.size(), "must not accept an unanchored frame merely because it looks contiguous");
        assertEquals(0, receiver.walkSegmentIndex(), "the mismatch must fall back to the chain walk");
        assertTrue(receiver.isAwaitingReplay());
        assertEquals(-1, receiver.replaySessionId());
        assertFalse(receiver.isCaughtUp());
    }

    @Test
    @DisplayName("a resume that stalls re-arms the anchor check")
    void replayStallOnAResumeReArmsTheAnchorCheck() {
        deliverTap(1);
        deliverTap(5); // gap -> resume anchored on frame 1
        answerReplaying(7, 500);
        deliverReplay(1);
        assertEquals(1, dispatched.size());

        advancePastTimers(); // stopped advancing before frame 2 arrived
        assertEquals(-1, receiver.walkSegmentIndex(), "still a resume retry, not a walk step");

        answerReplaying(8, 500);
        deliverReplay(2); // rotated recording, looks contiguous but is not the anchored frame

        assertEquals(1, dispatched.size(), "must not accept an unanchored frame merely because it looks contiguous");
        assertEquals(0, receiver.walkSegmentIndex(), "the mismatch must fall back to the chain walk");
        assertTrue(receiver.isAwaitingReplay());
        assertEquals(-1, receiver.replaySessionId());
        assertFalse(receiver.isCaughtUp());
    }

    // The same rotation seen one step earlier: the Replayer answers "nothing to replay" because the
    // position is already at its recording's tip. For a walk that means caught up; for a resume it cannot,
    // because we resumed only on account of a hole we know is open.
    @Test
    @DisplayName("NO_REPLAY_NEEDED over an open hole falls back to the chain walk")
    void noReplayNeededOverAnOpenHoleFallsBackToTheChainWalk() {
        deliverTap(1);
        deliverTap(2);
        assertTrue(receiver.isCaughtUp());
        deliverTap(6); // gap -> resume
        assertEquals(-1, receiver.walkSegmentIndex());

        answerReplaying(NO_REPLAY_NEEDED, 0);

        assertEquals(0, receiver.walkSegmentIndex(), "re-walk instead of believing it");
        assertTrue(receiver.isAwaitingReplay());
        assertFalse(receiver.isCaughtUp(), "the hole above globalSeqNo=2 is still open");
    }

    // A resume ends at the bound it was given and has no next segment to request — so unlike a walk step
    // it must declare itself caught up and hand the slot back, or the slot sits until the idle TTL
    // reclaims it.
    @Test
    @DisplayName("a resume reaching its bound catches up without requesting another segment")
    void resumeReachingItsBoundCatchesUpWithoutRequestingAnotherSegment() {
        deliverTap(1);
        assertEquals(1, caughtUpAt.size());
        deliverTap(5); // gap -> resume
        assertFalse(receiver.isCaughtUp());
        answerReplaying(7, 500);

        deliverReplay(1); // opens on the anchored frame -> anchor consumed, frame deduped
        deliverReplay(2);
        deliverReplay(3);
        deliverReplay(4); // closes the hole, draining the retained frame 5
        completeSegment();

        assertFalse(receiver.isAwaitingReplay(), "no follow-up request — a resume has no next segment");
        assertEquals(-1, receiver.replaySessionId());
        assertTrue(receiver.isCaughtUp(), "we hold everything the recording had when the request was served");
        assertEquals(2, caughtUpAt.size(), "re-fired, so consumers re-arm on re-convergence");
    }

    // A resume can reach its bound while a retained-ahead frame still sits behind a hole the replay never
    // covered. Declaring caught up on the bound alone is silently wrong: consumers gate real decisions on
    // it and nothing would rediscover the hole until some later, unrelated tap frame exposed it.
    @Test
    @DisplayName("a resume reaching its bound with a retained hole still open re-walks instead")
    void resumeReachingItsBoundWithARetainedHoleStillOpenReWalksInstead() {
        deliverTap(1);
        assertEquals(1, caughtUpAt.size());
        deliverTap(5); // gap -> resume anchored on frame 1; 5 is retained ahead of the hole
        answerReplaying(7, 500);

        deliverReplay(1); // opens on the anchored frame -> anchor consumed, frame deduped
        deliverReplay(2); // the bound is reached having covered only up to 2 — 3, 4 unfilled

        completeSegment();

        assertFalse(receiver.isCaughtUp(), "must not declare caught up with a retained frame behind a hole");
        assertEquals(1, caughtUpAt.size(), "no false re-convergence notification");
        assertEquals(0, receiver.walkSegmentIndex(), "falls back to the chain walk rather than trusting the bound");
        assertTrue(receiver.isAwaitingReplay());
        assertEquals(-1, receiver.replaySessionId());
    }

    // The overflow half of the same guard. When the FIFO had to DROP tap frames the frontier is short by
    // frames that are gone from the tap for good, and every retained frame draining cleanly says nothing
    // about them.
    @Test
    @DisplayName("a resume reaching its bound after dropped tap frames re-walks instead")
    void resumeReachingItsBoundAfterDroppedTapFramesReWalksInstead() {
        deliverTap(1);
        assertEquals(1, caughtUpAt.size());
        deliverTap(3);                // gap -> resume anchored on frame 1; 3 is retained
        deliverTapTooBigToRetain(4);  // ahead of the hole too, but dropped rather than retained
        answerReplaying(7, 500);

        deliverReplay(1); // opens on the anchored frame -> anchor consumed, frame deduped
        deliverReplay(2); // closes the hole the resume was asked to cover, draining retained frame 3

        completeSegment();

        assertFalse(receiver.isCaughtUp(), "frame 4 was dropped — the drained frontier is not the real one");
        assertEquals(1, caughtUpAt.size(), "no false re-convergence notification");
        assertEquals(0, receiver.walkSegmentIndex(), "re-walks now rather than leaving the hole for a later tap gap");
        assertTrue(receiver.isAwaitingReplay());
    }

    // ── releasing the replay slot ─────────────────────────────────────────────────────────────────

    // The release was a fire-and-forget offer, so an offer that did not land was indistinguishable from
    // one that did — and nothing re-sent it, leaving the slot to the idle TTL. There is no publication in
    // these tests, so every send here is a send that never went out.
    @Test
    @DisplayName("a ReplayComplete that never went out stays pending")
    void aReplayCompleteThatNeverWentOutStaysPending() {
        deliverTap(1);
        deliverTap(5); // gap -> resume
        answerReplaying(7, 500);
        deliverReplay(1);
        deliverReplay(2);
        deliverReplay(3);
        deliverReplay(4);
        assertFalse(receiver.completePending(), "nothing to release until the resume reaches its bound");

        completeSegment();

        assertTrue(receiver.isCaughtUp());
        assertTrue(receiver.completePending(), "it never reached the wire — remember it");
        receiver.doTimers(false);
        assertTrue(receiver.completePending(), "the duty cycle retries the release rather than forgetting it");
    }

    // ReplayComplete names only the clientId, so one landing after a NEW request took a fresh slot would
    // free the slot that replay is riding.
    @Test
    @DisplayName("a new request drops a ReplayComplete that never went out")
    void aNewRequestDropsAReplayCompleteThatNeverWentOut() {
        deliverTap(1);
        deliverTap(5); // gap -> resume
        answerReplaying(7, 500);
        deliverReplay(1);
        deliverReplay(2);
        deliverReplay(3);
        deliverReplay(4);
        completeSegment();
        assertTrue(receiver.completePending());

        deliverTap(9); // a second gap -> new request, taking a fresh slot

        assertTrue(receiver.isAwaitingReplay());
        assertFalse(receiver.completePending(), "dropped: releasing now would free the new slot");
    }

    // ── the walk's terminating NO_REPLAY_NEEDED ───────────────────────────────────────────────────
    // serveReplay sends NO_REPLAY_NEEDED for two different things and tells them apart by recordingId: a
    // walk that ran past the last recording (names none) versus a segment that is merely EMPTY (names the
    // recording it found nothing in). Only the first ends the walk — and ending it is not by itself
    // permission to declare caught up.

    @Test
    @DisplayName("frames still retained when the chain is exhausted force a re-walk rather than caught-up")
    void retainedResidualForcesReWalk() {
        attachReplay(11, 4096);
        deliverTap(5); // a hole below it that this walk never reached
        deliverReplay(1);
        assertEquals(1, receiver.retainedFrameCount());

        answerReplaying(NO_REPLAY_NEEDED, 0, CHAIN_EXHAUSTED);

        assertFalse(receiver.isCaughtUp(), "declaring caught up here would close the hole by fiat");
        assertEquals(0, receiver.walkSegmentIndex());
    }

    /** The negative control for the guard above: a walk whose retained frames all drained must finish. */
    @Test
    @DisplayName("the walk terminator with every retained frame drained still catches up")
    void walkTerminatorWithEveryRetainedFrameDrainedStillCatchesUp() {
        answerReplaying(7, 500, 5);
        deliverReplay(1);
        deliverTap(3);    // ahead of the hole at 2 -> retained
        deliverReplay(2); // closes it, so 3 drains straight over
        completeSegment();

        answerReplaying(NO_REPLAY_NEEDED, 0, CHAIN_EXHAUSTED);

        assertTrue(receiver.isCaughtUp(), "nothing is outstanding — the guard must not fire here");
        assertEquals(-1, receiver.walkSegmentIndex(), "steady/resume mode, not a spurious re-walk");
        assertFalse(receiver.isAwaitingReplay());
    }

    // The same guard driven through to convergence: the re-walk must both happen AND be able to finish.
    // Clearing the overflow on drain forgets the drop and catches up over the hole; never clearing it
    // leaves a client that can never declare itself caught up again.
    @Test
    @DisplayName("the walk terminator after dropped tap frames re-walks, then catches up")
    void walkTerminatorAfterDroppedTapFramesReWalksThenCatchesUp() {
        answerReplaying(7, 500, 5);
        deliverReplay(1);
        deliverReplay(2);
        deliverTapTooBigToRetain(4); // the tap runs ahead of the walk, and this one is dropped
        completeSegment();

        answerReplaying(NO_REPLAY_NEEDED, 0, CHAIN_EXHAUSTED);

        assertFalse(receiver.isCaughtUp(), "the chain was exhausted, but a tap frame above it was dropped");
        assertEquals(0, caughtUpAt.size(), "no false convergence notification — this opens the accept gate");
        assertEquals(0, receiver.walkSegmentIndex(), "re-walks the chain rather than trusting the terminator");

        // The re-walk replays what the drop lost, and this time nothing is dropped.
        answerReplaying(8, 900, 5);
        deliverReplay(3);
        deliverReplay(4);
        completeSegment();
        answerReplaying(NO_REPLAY_NEEDED, 0, CHAIN_EXHAUSTED);

        assertTrue(receiver.isCaughtUp(), "a re-walk that dropped nothing must be able to finish");
        assertEquals(1, caughtUpAt.size());
    }

    // An unclean restart can leave a recording created before anything was published to it. Taking the
    // NO_REPLAY_NEEDED that answers it as the walk terminator drops every later segment on the floor.
    @Test
    @DisplayName("an empty segment is skipped, not taken for the end of the chain")
    void emptySegmentAdvancesTheWalk() {
        answerReplaying(NO_REPLAY_NEEDED, 0, 7);

        assertFalse(receiver.isCaughtUp(), "an empty segment says nothing about the rest of the chain");
        assertEquals(1, receiver.walkSegmentIndex(), "keep walking rather than truncating the chain");
        assertTrue(receiver.isAwaitingReplay());
        assertTrue(caughtUpAt.isEmpty());

        // Segment 1 holds the history, and the walk proceeds through it normally.
        answerReplaying(9, 900, 8);

        assertEquals(9, receiver.replaySessionId());
        assertEquals(8, receiver.walkRecordingId());
        deliverReplay(1);
        assertEquals(1, receiver.walkSegmentIndex(), "still walking segment 1");
    }

    // ── the walk-segment recordingId check ────────────────────────────────────────────────────────
    // serveReplay re-resolves the recording chain on every request, and a stale still-recording span can
    // be dropped from it once a newer one supersedes it — shifting what a given segmentIndex denotes. A
    // retry of the SAME segment must land on the SAME recording it did originally.

    @Test
    @DisplayName("a walk segment retried on the same recording continues normally")
    void walkSegmentRetryOnTheSameRecordingContinuesNormally() {
        answerReplaying(7, 500, 5);
        assertEquals(5, receiver.walkRecordingId());
        final long requestId = receiver.requestId();

        receiver.onReplayImageClosed(312); // stopped under us, mid-segment -> retried
        assertEquals(0, receiver.walkSegmentIndex());
        assertNotEquals(requestId, receiver.requestId());

        answerReplaying(8, 700, 5);

        assertEquals(0, receiver.walkSegmentIndex(), "same recording -> the retry is trusted, not abandoned");
        assertEquals(5, receiver.walkRecordingId());
        assertEquals(8, receiver.replaySessionId());
    }

    @Test
    @DisplayName("a walk segment that resolves to a different recording restarts the walk")
    void shiftedRecordingChainRestartsTheWalk() {
        answerReplaying(11, 4096, 7); // segment 0 → recording 7
        assertEquals(7, receiver.walkRecordingId());
        completeSegment(); // → segment 1, nothing to compare against yet
        assertEquals(1, receiver.walkSegmentIndex());
        assertEquals(-1, receiver.walkRecordingId());

        answerReplaying(12, 8192, 8);         // segment 1 → recording 8
        receiver.onReplayImageClosed(0); // stopped short → re-request the SAME segment
        answerReplaying(13, 8192, 9);         // ... but it now resolves to recording 9

        assertEquals(0, receiver.walkSegmentIndex(), "the chain moved under the walk — start it over");
        assertEquals(-1, receiver.walkRecordingId(), "and with no stale expectation carried into it");
        assertTrue(receiver.isAwaitingReplay());
    }

    // ── the convergence alarm ─────────────────────────────────────────────────────────────────────
    // RecoveryProgressPolicyTest covers the verdict itself; what is locked here is the wiring around it —
    // that an in-order dispatch counts as progress, that catching up without dispatching does too, and
    // that the report carries the state telling the causes apart.

    @Test
    @DisplayName("recovery delivering nothing is reported once per episode")
    void recoveryDeliveringNothingIsReportedOncePerEpisode() {
        captureLogs();

        assertFalse(checkProgressAt(CLOCK_MS), "the first observation only anchors the clock");
        assertTrue(checkProgressAt(PAST_DEADLINE_MS));

        assertEquals(1, logged.size());
        assertEquals(Logger.Component.ReplayerStreamReceiver, logged.get(0).component());
        assertEquals(Logger.Severity.Fault, logged.get(0).severity());
        assertEquals(Logger.EventCode.RecoveryStalled, logged.get(0).code());

        // The condition persists for as long as the archive is broken; the caller logs, so it must not
        // repeat every duty cycle.
        assertFalse(checkProgressAt(PAST_DEADLINE_MS + 60_000));
        assertEquals(1, logged.size());
    }

    @Test
    @DisplayName("a walk that is still delivering is never reported, however long it runs")
    void aWalkThatIsStillDeliveringIsNeverReported() {
        captureLogs();

        // A cold start replaying a whole trading day: slow, but converging one frame at a time. Measuring
        // elapsed time instead of progress is exactly what would fence this.
        for (long globalSeqNo = 1; globalSeqNo <= 20; globalSeqNo++) {
            deliverReplay(globalSeqNo);
            assertFalse(checkProgressAt(CLOCK_MS + globalSeqNo * 60_000), "at globalSeqNo " + globalSeqNo);
        }
    }

    @Test
    @DisplayName("a gap after a healthy run starts its own episode rather than inheriting one")
    void aGapAfterAHealthyRunStartsItsOwnEpisode() {
        captureLogs();
        assertFalse(checkProgressAt(CLOCK_MS)); // an episode anchored during cold start
        deliverTap(1);                          // ... which then converges
        assertTrue(receiver.isCaughtUp());

        deliverTap(5); // much later, a tap gap: not caught up again
        assertFalse(receiver.isCaughtUp());

        // Inheriting the cold-start clock would report this the instant the gap opened, before the re-walk
        // it triggers had any chance to converge.
        assertFalse(checkProgressAt(PAST_DEADLINE_MS));
        assertEquals(1, logged.size(), "the tap-gap warn only — no convergence report");
        assertEquals(Logger.EventCode.TapGap, logged.get(0).code());
    }

    @Test
    @DisplayName("a caught-up client that simply goes quiet is not reported")
    void aCaughtUpClientThatGoesQuietIsNotReported() {
        captureLogs();
        deliverTap(1);
        assertTrue(receiver.isCaughtUp());

        // A tap that goes silent is a gateway's tap-stall watchdog's business, and it is not a convergence
        // failure at all — diagnosing it as one puts a second, wrong explanation on the same event.
        assertFalse(checkProgressAt(CLOCK_MS));
        assertFalse(checkProgressAt(PAST_DEADLINE_MS));
        assertTrue(logged.isEmpty());
    }

    @Test
    @DisplayName("the report names the state that tells the causes apart")
    void theReportNamesTheStateThatTellsTheCausesApart() {
        deliverTap(1);
        deliverTap(5); // gap -> awaiting a replay
        deliverUnavailable(receiver.requestId());

        captureLogs(); // installed after the refusal, so it captures only the report below
        assertFalse(checkProgressAt(CLOCK_MS));
        assertTrue(checkProgressAt(PAST_DEADLINE_MS));

        assertEquals(1, logged.size());
        final String text = logged.get(0).message();
        // Without these an operator cannot tell a refused node from one whose chain cannot cover the hole.
        assertTrue(text.contains("lastGlobalSeqNo=1"), text);
        assertTrue(text.contains("awaitingReplay=true"), text);
        assertTrue(text.contains("replayerUnavailable=true"), text);
    }

    @Test
    @DisplayName("the report stops naming a refusal once the Replayer serves again")
    void theReportStopsNamingARefusalOnceTheReplayerServesAgain() {
        deliverTap(1);
        deliverTap(5); // gap -> awaiting a replay
        deliverUnavailable(receiver.requestId());
        answerReplaying(7, 900);

        captureLogs(); // installed after the exchange above, so it captures only the report below
        assertFalse(checkProgressAt(CLOCK_MS));
        assertTrue(checkProgressAt(PAST_DEADLINE_MS));

        assertEquals(1, logged.size());
        // The field is state, not "was ever refused": a stale 1 points the operator at a Replayer that is
        // answering fine, and away from the attached replay that is actually not delivering.
        assertTrue(logged.get(0).message().contains("replayerUnavailable=false"), logged.get(0).message());
    }

    // ── helpers ───────────────────────────────────────────────────────────────────────────────────

    /**
     * Starts capturing log events here rather than in {@code setUp}: several cases must see only what
     * follows their own setup, and the C++ twin installs its sink at the same points for the same reason.
     */
    private void captureLogs() {
        logged.clear();
        Logger.install(logged::add);
    }

    /** Refusals only: a re-request logs its own line between two of them, so size() cannot count these. */
    private long refusals() {
        return logged.stream().filter(event -> Logger.EventCode.ReplayUnavailable == event.code()).count();
    }

    /** Retained-buffer overflows only: every gap/re-walk warn shares TapGap, so only the text separates them. */
    private long overflowReports() {
        return logged.stream().filter(event -> event.message().contains("retained-frame buffer full")).count();
    }

    /** Drives the receiver to the caught-up, steady state a live consumer runs in. */
    private void goLive() {
        answerReplaying(NO_REPLAY_NEEDED, 0, CHAIN_EXHAUSTED);
    }

    private void attachReplay(final long replaySessionId, final long catchUpPosition) {
        answerReplaying(replaySessionId, catchUpPosition, 7);
    }

    private void answerReplaying(final long replaySessionId, final long catchUpPosition) {
        answerReplaying(replaySessionId, catchUpPosition, NO_RECORDING);
    }

    private void answerReplaying(final long replaySessionId, final long catchUpPosition, final long recordingId) {
        receiver.onControl(replayingBuffer(receiver.requestId(), replaySessionId, catchUpPosition, recordingId), 0,
                           replayingLength());
    }

    private void deliverPending(final long requestId) {
        receiver.onControl(replayPendingBuffer(requestId), 0, replayPendingLength());
    }

    private void deliverUnavailable(final long requestId) {
        receiver.onControl(replayUnavailableBuffer(requestId), 0, replayUnavailableLength());
    }

    /** The replay image reaching the bound the Replayer gave it — how a segment completes. */
    private void completeSegment() {
        receiver.onReplayPosition(receiver.catchUpPosition());
    }

    /**
     * One duty cycle with the clock past every timer in it: an unanswered request is re-sent, and an
     * established replay that has delivered nothing is declared stalled.
     */
    private void advancePastTimers() {
        actions.clockMs += PAST_EVERY_TIMER_MS;
        receiver.doTimers(false);
    }

    private boolean checkProgressAt(final long nowMs) {
        actions.clockMs = nowMs;
        return receiver.checkRecoveryProgress();
    }

    private void deliverReplay(final long globalSeqNo) {
        receiver.onFrame(heartbeatFrame(globalSeqNo), 0, heartbeatLength(), globalSeqNo * 1024, RECEIVE_NS, true);
    }

    private void deliverTap(final long globalSeqNo) {
        deliverTapAt(globalSeqNo, globalSeqNo * 1024);
    }

    private void deliverTapAt(final long globalSeqNo, final long position) {
        receiver.onFrame(heartbeatFrame(globalSeqNo), 0, heartbeatLength(), position, RECEIVE_NS, false);
    }

    /**
     * A live tap frame too big for the retained-ahead FIFO to hold: a heartbeat zero-padded past one
     * RetainBlock, which retainFrame refuses outright. The cheapest of its three overflow triggers to
     * drive — the other two need 65536 frames or 16 MiB.
     */
    private void deliverTapTooBigToRetain(final long globalSeqNo) {
        // Decoded from the front, so the padding only has to make the record oversized.
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[OVERSIZED_FRAME_LENGTH]);
        buffer.putBytes(0, heartbeatFrame(globalSeqNo), 0, heartbeatLength());
        receiver.onFrame(buffer, 0, OVERSIZED_FRAME_LENGTH, globalSeqNo * 1024, RECEIVE_NS, false);
    }

    /** One synthesized ClusterHeartbeat — the cheapest well-formed frame there is, at 42 bytes. */
    private static UnsafeBuffer heartbeatFrame(final long globalSeqNo) {
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[256]);
        final ClusterHeartbeatEncoder frame = new ClusterHeartbeatEncoder();
        frame.wrapAndApplyHeader(buffer, 0, new org.limitless.seqeron.sbe.frame.MessageHeaderEncoder());
        frame.header().sourceId(-1).connectionId(-1).sessionId(-1)
            .systemEventType(org.limitless.seqeron.sequencer.SystemFrame.CLUSTER_HEARTBEAT)
            .globalSeqNo(globalSeqNo).timestamp(globalSeqNo * 1000);
        return buffer;
    }

    private static int heartbeatLength() {
        return org.limitless.seqeron.sbe.frame.MessageHeaderEncoder.ENCODED_LENGTH +
               ClusterHeartbeatEncoder.BLOCK_LENGTH;
    }

    private static UnsafeBuffer replayingBuffer(final long requestId, final long replaySessionId,
                                                final long catchUpPosition, final long recordingId) {
        return replayingBuffer(CLIENT_ID, requestId, replaySessionId, catchUpPosition, recordingId);
    }

    private static UnsafeBuffer replayingBuffer(final int clientId, final long requestId,
                                                final long replaySessionId, final long catchUpPosition,
                                                final long recordingId) {
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[256]);
        new ReplayingEncoder()
            .wrapAndApplyHeader(buffer, 0, new org.limitless.seqeron.sbe.replay.MessageHeaderEncoder())
            .clientId(clientId)
            .requestId(requestId)
            .replaySessionId(replaySessionId)
            .catchUpPosition(catchUpPosition)
            .recordingId(recordingId);
        return buffer;
    }

    private static int replayingLength() {
        return org.limitless.seqeron.sbe.replay.MessageHeaderEncoder.ENCODED_LENGTH
            + ReplayingEncoder.BLOCK_LENGTH;
    }

    private static UnsafeBuffer replayPendingBuffer(final long requestId) {
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[64]);
        new ReplayPendingEncoder()
            .wrapAndApplyHeader(buffer, 0, new org.limitless.seqeron.sbe.replay.MessageHeaderEncoder())
            .clientId(CLIENT_ID)
            .requestId(requestId);
        return buffer;
    }

    private static int replayPendingLength() {
        return org.limitless.seqeron.sbe.replay.MessageHeaderEncoder.ENCODED_LENGTH
            + ReplayPendingEncoder.BLOCK_LENGTH;
    }

    private static UnsafeBuffer replayUnavailableBuffer(final long requestId) {
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[64]);
        new ReplayUnavailableEncoder()
            .wrapAndApplyHeader(buffer, 0, new org.limitless.seqeron.sbe.replay.MessageHeaderEncoder())
            .clientId(CLIENT_ID)
            .requestId(requestId);
        return buffer;
    }

    private static int replayUnavailableLength() {
        return org.limitless.seqeron.sbe.replay.MessageHeaderEncoder.ENCODED_LENGTH
            + ReplayUnavailableEncoder.BLOCK_LENGTH;
    }
}
