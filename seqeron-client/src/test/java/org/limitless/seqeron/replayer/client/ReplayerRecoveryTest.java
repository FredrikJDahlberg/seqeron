package org.limitless.seqeron.replayer.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.limitless.seqeron.protocol.FrameLayer;
import org.limitless.seqeron.protocol.ReplayProtocol;
import org.limitless.seqeron.protocol.SnapshotHeader;
import org.limitless.seqeron.sbe.frame.ClusterHeartbeatEncoder;
import org.limitless.seqeron.sbe.frame.LeadershipChangedEncoder;
import org.limitless.seqeron.sbe.replay.ReplayPendingEncoder;
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
    private static final long CLIENT_ID = 4;
    private static final long NO_REPLAY_NEEDED = ReplayProtocol.NO_REPLAY_NEEDED;

    private static final long FROM_START = ReplayProtocol.FROM_START;

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
    private static final int OVERSIZED_FRAME_LENGTH = 32 * 1024;

    /** The largest frame the queue carries (§12): a full payload behind the 44-byte sequenced envelope. */
    private static final int LARGEST_FRAME_LENGTH = 44 + FrameLayer.MAX_PAYLOAD_LENGTH;

    /** The snapshot's cut; frame n starts at n * 1024, as everywhere here. */
    private static final long CUT = 10;

    /** The snapshot every round here holds: the header, term 4 and leader 2, then one record. */
    private static final byte[] SNAPSHOT_HEADER = RestoreFrames.header(4, 2);
    private static final byte[] SNAPSHOT_RECORD = RestoreFrames.record(7);

    @TempDir
    Path directory;
    private int stores;
    private SnapshotStore store;

    private final List<Long> dispatched = new ArrayList<>();
    private final List<String> restored = new ArrayList<>();
    private final List<Long> caughtUpAt = new ArrayList<>();
    private final List<Logger.LoggerEvent> logged = new ArrayList<>();
    private RecordingActions actions;
    private ReplayerRecovery receiver;

    /** The transport recorded rather than performed — no publication, so nothing ever reaches the wire. */
    private static final class RecordingActions implements ReplayerRecoveryActions {
        long clockMs = CLOCK_MS;
        int requestsSent;
        long lastCutClusterSeqNo = -2;
        final List<Boolean> stalledGauge = new ArrayList<>();

        @Override
        public void sendReplayRequest(final long requestId, final long fromPosition, final long cutClusterSeqNo) {
            ++requestsSent;
            lastCutClusterSeqNo = cutClusterSeqNo;
        }

        @Override
        public void openReplay(final long replaySessionId) {
        }

        @Override
        public void closeReplay() {
        }

        @Override
        public void recoveryStalled(final boolean stalled) {
            stalledGauge.add(stalled);
        }

        @Override
        public Integer memberId() {
            return 0;
        }

        @Override
        public long clientId() {
            return CLIENT_ID;
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
        restored.clear();
        caughtUpAt.clear();
        logged.clear();
        actions = new RecordingActions();
        store = new SnapshotStore(directory.resolve("instance-" + stores++));
        receiver = new ReplayerRecovery(actions, event -> dispatched.add(event.clusterSeqNo()), null,
                                        () -> caughtUpAt.add((long)dispatched.size()));
    }

    @AfterEach
    void tearDown() {
        Logger.reset();
        receiver.close();
    }

    // ── baseline ──────────────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("cold start walks the recording from its start")
    void coldStartWalksFromTheStart() {
        receiver.start();

        assertTrue(receiver.isAwaitingReplay());
        assertEquals(FROM_START, receiver.requestFromPosition());
    }

    @Test
    @DisplayName("a first frame at clusterSeqNo 1 is the baseline and puts the client live")
    void firstFrameAtOneIsAccepted() {
        deliverQueue(1);

        assertEquals(List.of(1L), dispatched);
        assertTrue(receiver.isCaughtUp());
    }

    @Test
    @DisplayName("a first frame that is not clusterSeqNo 1 is fatal")
    void firstFrameMustBeOne() {
        attachReplay(11, 4096);

        assertThrows(IllegalStateException.class, () -> deliverReplay(7));
    }

    // Once the queue is dispatched mid-walk, a cold start races a live queue already carrying mid-stream
    // clusterSeqNos against a replay that has not delivered 1 yet. That queue frame is neither a valid
    // baseline nor a fault — only the walk may establish the baseline.
    @Test
    @DisplayName("the live queue may not establish the baseline while the cold-start walk is in flight")
    void liveQueueMayNotEstablishTheBaselineMidWalk() {
        answerReplaying(7, 500);
        assertTrue(receiver.isRecovering());

        deliverQueue(15); // the live tip, far ahead of a replay that has not started delivering

        assertTrue(dispatched.isEmpty(), "must not adopt the live queue's mid-stream baseline");
        assertFalse(receiver.isCaughtUp());

        deliverReplay(1); // the walk supplies the real baseline

        assertEquals(List.of(1L), dispatched);
    }

    // ── a queue's continuity: groupSeqNo, across jumps in clusterSeqNo (spec §14 row 12) ──────────────────────

    @Test
    @DisplayName("a first frame at groupSeqNo 1 is the baseline, whatever its clusterSeqNo")
    void firstFrameAtGroupSeqNoOneIsAcceptedWhateverItsClusterSeqNo() {
        deliverQueueOnQueue(40, 1);

        assertEquals(List.of(40L), dispatched);
        assertTrue(receiver.isCaughtUp());
    }

    @Test
    @DisplayName("a first frame that is not groupSeqNo 1 is fatal, even at clusterSeqNo 1")
    void firstFrameMustBeGroupSeqNoOne() {
        attachReplay(11, 4096);

        assertThrows(IllegalStateException.class, () -> deliverReplayOnQueue(1, 2));
    }

    @Test
    @DisplayName("continuity follows groupSeqNo, so a jump in clusterSeqNo is no gap")
    void continuityFollowsGroupSeqNoAcrossJumpsInClusterSeqNo() {
        deliverQueueOnQueue(1, 1);
        deliverQueueOnQueue(5, 2);
        deliverQueueOnQueue(9, 3);

        assertEquals(List.of(1L, 5L, 9L), dispatched);
        assertTrue(receiver.isCaughtUp());
        assertEquals(0, actions.requestsSent, "nothing to recover");
    }

    @Test
    @DisplayName("a missing groupSeqNo is a gap, resumed at the last dispatched frame and drained in order")
    void aMissingGroupSeqNoIsAGap() {
        deliverQueueOnQueue(1, 1);
        deliverQueueOnQueue(9, 3);

        assertFalse(receiver.isCaughtUp());
        assertEquals(1024, receiver.requestFromPosition(), "resumes at frame 1, where it starts");
        assertEquals(1, receiver.retainedFrameCount());

        attachReplay(21, 3 * 1024);
        deliverReplayOnQueue(1, 1); // the anchor, already dispatched
        deliverReplayOnQueue(5, 2);

        assertEquals(List.of(1L, 5L, 9L), dispatched);
    }

    @Test
    @DisplayName("a frame whose clusterSeqNo is already passed is dropped")
    void aFrameWhoseClusterSeqNoIsPassedIsDropped() {
        deliverQueueOnQueue(1, 1);
        deliverQueueOnQueue(5, 2);
        deliverQueueOnQueue(5, 2);

        assertEquals(List.of(1L, 5L), dispatched);
    }

    // ── gap detection and the replay-to-live seam ─────────────────────────────────────────────────

    @Test
    @DisplayName("a mid-stream gap withholds the out-of-order frame and requests a replay")
    void midStreamGapWithholdsTheOutOfOrderFrame() {
        deliverQueue(1);
        deliverQueue(2);
        assertEquals(2, dispatched.size());
        assertFalse(receiver.isAwaitingReplay());

        deliverQueue(5); // skips 3, 4

        assertEquals(2, dispatched.size(), "the out-of-order frame itself must not be dispatched");
        assertTrue(receiver.isAwaitingReplay(), "a gap must re-request a replay");
        assertEquals(-1, receiver.replaySessionId(), "no session yet — only a request, until the Replayer answers");
    }

    @Test
    @DisplayName("a queue gap reports a structured diagnostic event")
    void queueGapReportsAStructuredDiagnosticEvent() {
        captureLogs();

        deliverQueue(1);
        deliverQueue(2);
        deliverQueue(5); // skips 3, 4

        assertEquals(1, logged.size(), "exactly the one gap detected above, nothing from setup");
        final Logger.LoggerEvent event = logged.get(0);
        assertEquals(Logger.CoreComponent.ReplayerStreamReceiver, event.component());
        assertEquals(Logger.Severity.Warn, event.severity());
        assertEquals(Logger.CoreEventCode.QueueGap, event.code());
        assertEquals("queue gap: expected groupSeqNo=3 got 5 — resuming the recording at clusterSeqNo=2",
                     event.message());
    }

    @Test
    @DisplayName("a live queue frame ahead of an in-flight walk is retained, not treated as a gap")
    void queueFrameMidWalkIsRetainedNotAGap() {
        attachReplay(11, 4096);
        final long requestIdBefore = receiver.requestId();

        deliverQueue(9); // the queue runs ahead of the replay

        assertTrue(dispatched.isEmpty(), "no baseline yet — only the walk may establish one");
        assertEquals(1, receiver.retainedFrameCount());
        assertEquals(requestIdBefore, receiver.requestId(), "an in-flight walk must not be superseded");
    }

    @Test
    @DisplayName("a queue gap while the walk is still in flight does not supersede it")
    void queueGapMidWalkDoesNotSupersedeTheWalk() {
        attachReplay(11, 4096);
        deliverReplay(1); // baseline established, by the walk rather than by the queue
        final long requestIdBefore = receiver.requestId();

        deliverQueue(5); // 2..4 are not lost — they are frames this walk has not reached yet

        assertEquals(requestIdBefore, receiver.requestId(), "an in-flight walk must run to completion");
        assertEquals(FROM_START, receiver.requestFromPosition(), "a mid-walk queue gap is expected, not a new gap");
        assertEquals(1, receiver.retainedFrameCount());
    }

    // The same rule one state later: not merely awaiting an answer but riding an assigned session.
    // Superseding the walk on each queue frame would restart it from the start forever under any sustained
    // publish rate.
    @Test
    @DisplayName("a queue frame arriving during an assigned replay session does not supersede it")
    void queueFrameAheadOfAnAssignedReplayDoesNotSupersedeIt() {
        captureLogs();
        deliverQueue(1);
        deliverQueue(5); // the genuine steady-state gap
        assertTrue(receiver.isAwaitingReplay());
        assertEquals(1, logged.size());

        answerReplaying(99, 500);
        assertEquals(99, receiver.replaySessionId());
        assertFalse(receiver.isAwaitingReplay());
        assertTrue(receiver.isRecovering());

        deliverQueue(12); // the queue running ahead, not a hole this walk will not cover

        assertEquals(99, receiver.replaySessionId(), "the in-flight walk must run to completion");
        assertFalse(receiver.isAwaitingReplay());
        assertNotEquals(FROM_START, receiver.requestFromPosition(), "still the resume the gap asked for, not restarted");
        assertEquals(1, dispatched.size(), "the out-of-order frame itself is still withheld");
        assertEquals(1, logged.size(), "and it is not reported as a second gap");
    }

    // The replay->live seam: the queue is dispatched throughout a walk, so the frame at gseq == last + 1
    // lands the instant the replay reaches it. Discarding queue frames while recovering ends every walk one
    // guaranteed gap short of live.
    @Test
    @DisplayName("the queue frame at the seam is dispatched while the walk is still in flight")
    void liveQueueFrameAtTheSeamIsDispatchedMidWalk() {
        answerReplaying(7, 500);
        deliverReplay(1);
        deliverReplay(2);
        assertEquals(2, dispatched.size());
        assertFalse(receiver.isCaughtUp(), "still riding replayed history");
        assertTrue(receiver.isRecovering());

        deliverQueue(3); // the seam: first queue frame contiguous with what the replay delivered

        assertEquals(3, dispatched.size(), "the seam frame must be dispatched, not discarded as mid-walk noise");
        assertTrue(receiver.isCaughtUp(), "a contiguous frame off the live queue means we are following live");
    }

    @Test
    @DisplayName("a steady-state queue gap clears caught-up and resumes at the last dispatched frame")
    void steadyStateGapResumesAtLastFrame() {
        goLive();
        deliverQueueAt(1, 1024);
        deliverQueueAt(2, 2048);
        assertTrue(receiver.isCaughtUp());

        deliverQueueAt(5, 8192); // 3 and 4 dropped

        assertFalse(receiver.isCaughtUp(), "consumers gate real decisions on this — it must not stay latched");
        assertTrue(receiver.isAwaitingReplay());
        assertNotEquals(FROM_START, receiver.requestFromPosition(), "a gap resumes; it does not re-walk from a stale index");
        assertEquals(2048, receiver.requestFromPosition(), "resume at the frame last dispatched, not at 0");
        assertEquals(1, receiver.retainedFrameCount(), "the ahead-of-hole frame is kept, not dropped");
    }

    @Test
    @DisplayName("replayed frames already seen off the live queue are de-duped")
    void duplicateAndStaleReplayFramesAreDropped() {
        deliverQueue(1);
        deliverQueue(2);
        assertEquals(2, dispatched.size());

        deliverReplay(1);
        deliverReplay(2);
        assertEquals(2, dispatched.size(), "replayed frames already delivered off the live queue must be de-duped");

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
        assertEquals(3, receiver.lastClusterSeqNo());
    }

    // ── the Replayer's control-stream replies, and request/reply correlation ──────────────────────

    @Test
    @DisplayName("nothing to replay from the start marks the client caught up")
    void nothingToReplayFromTheStartMarksCaughtUp() {
        answerReplaying(NO_REPLAY_NEEDED, 0);

        assertTrue(receiver.isCaughtUp());
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
        receiver.onControl(replayingBuffer(CLIENT_ID + 1, receiver.requestId(), 42, 1000), 0,
                           replayingLength());

        assertEquals(-1, receiver.replaySessionId(), "a reply for a different replica must not be applied");
        assertFalse(receiver.isCaughtUp());
    }

    @Test
    @DisplayName("a walk reaching its bound catches up")
    void aWalkReachingItsBoundCatchesUp() {
        answerReplaying(7, 500);
        assertEquals(7, receiver.replaySessionId());

        completeReplay();

        assertTrue(receiver.isCaughtUp(), "the recording held the whole log when the request was served");
        assertFalse(receiver.isAwaitingReplay(), "there is nothing further to ask for");
        assertEquals(-1, receiver.replaySessionId());
    }

    // clientId alone cannot identify WHICH request a reply answers. A resend makes the Replayer stop the
    // in-flight session and start a new one, so both replies sit in order on the one shared control
    // publication and the stale one is always processed first.
    @Test
    @DisplayName("a reply carrying a superseded requestId is ignored")
    void staleReplyIsIgnored() {
        receiver.start();
        final long staleRequestId = receiver.requestId();

        receiver.onControl(replayingBuffer(staleRequestId - 1, 11, 4096), 0, replayingLength());

        assertTrue(receiver.isAwaitingReplay(), "a stale reply must not stop the resend timer");
        assertEquals(-1, receiver.replaySessionId());
        assertNotEquals(11, receiver.replaySessionId());
    }

    @Test
    @DisplayName("the reply to the current request is still accepted after a stale one")
    void currentReplyIsStillAcceptedAfterAStaleOne() {
        final long stale = receiver.requestId();
        receiver.start(); // a request, and with it a new requestId

        receiver.onControl(replayingBuffer(stale, 42, 900), 0, replayingLength());
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
        receiver.start();
        final int sends = actions.requestsSent;

        actions.clockMs += 300; // still inside the current request's resend interval
        deliverPending(stale);
        actions.clockMs += 300; // ... which has now elapsed, unless the stale "wait" pushed it out
        receiver.doTimers(false);

        assertEquals(sends + 1, actions.requestsSent,
                     "a superseded request's ReplayPending must not defer the current request's resend");
    }

    // ── a closed image, a completed replay, and the stall watchdog ────────────────────────────────

    @Test
    @DisplayName("a replay image closing at its bound completes the replay; short of it re-requests")
    void closedImageIsCompletionOnlyAtTheBound() {
        answerReplaying(11, 4096);
        receiver.onReplayImageClosed(4000);
        assertTrue(receiver.isAwaitingReplay(), "short of the bound: the replay is re-requested");
        assertEquals(FROM_START, receiver.requestFromPosition());

        answerReplaying(12, 4096);
        receiver.onReplayImageClosed(4096);
        assertTrue(receiver.isCaughtUp(), "at the bound: the replay is done");
        assertFalse(receiver.isAwaitingReplay());
    }

    // Once Replaying arrives the resend timer is disarmed, and a bounded replay of an active recording
    // never closes its image — so an image that simply stops advancing leaves the client waiting on it
    // forever with nothing retrying.
    @Test
    @DisplayName("a replay that stops advancing re-requests it")
    void replayThatStopsAdvancingReRequestsIt() {
        captureLogs();
        answerReplaying(7, 500);
        final long requestId = receiver.requestId();
        assertFalse(receiver.isAwaitingReplay(), "a session was assigned, so nothing else is retrying");

        advancePastTimers();

        assertEquals(FROM_START, receiver.requestFromPosition(), "the walk is asked for again, from the start");
        assertTrue(receiver.isAwaitingReplay(), "a stalled replay is not taken for a complete one");
        assertEquals(-1, receiver.replaySessionId());
        assertNotEquals(requestId, receiver.requestId(),
                        "re-requesting is a new request, so the reply to the stalled one is recognisably stale");
        assertEquals(1, logged.size(), "a stalled replay is reported, not silently absorbed");
    }

    // Session id 0 is an ordinary archive replaySessionId; -1 is the only value that means "no replay".
    // Testing >= 1 skips the stall watchdog for it once it stops delivering.
    @Test
    @DisplayName("replay session 0 is watchdogged like any other")
    void replaySessionZeroIsWatchdoggedLikeAnyOther() {
        answerReplaying(0, 500);
        assertEquals(0, receiver.replaySessionId(),
                     "session 0 is a replay this client is riding, not the absence of one");

        advancePastTimers();

        assertTrue(receiver.isAwaitingReplay(), "its stall watchdog must re-request it");
        assertEquals(-1, receiver.replaySessionId());
    }

    // ── ReplayPending ─────────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("ReplayPending holds at the gap without assigning a session")
    void replayPendingHoldsAtTheGapWithoutAssigningASession() {
        deliverQueue(1);
        deliverQueue(5); // gap -> awaiting a replay
        assertTrue(receiver.isAwaitingReplay());

        deliverPending(receiver.requestId());

        assertTrue(receiver.isAwaitingReplay(), "no free Replayer slot -> keep holding at the gap");
        assertEquals(-1, receiver.replaySessionId());
        assertEquals(1, dispatched.size(), "must not advance past the hole while pending");
    }

    // ── the resend timer ──────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("a request the Replayer never answered is re-sent once the interval elapses")
    void stuckAwaitingReplayResendsAfterTheIntervalElapses() {
        deliverQueue(1);
        deliverQueue(5); // gap -> arms the resend timer
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
        assertNotEquals(FROM_START, receiver.requestFromPosition(), "a resend repeats the same request verbatim");
    }

    // Every send does ++requestId and onControl acts only on a reply carrying the CURRENT id, so a
    // per-poll retry runs the counter away and every reply that arrives is discarded as stale — leaving
    // the client awaiting a replay that can never correlate, hence never caught up.
    @Test
    @DisplayName("a request that never went out is not re-sent once per poll")
    void anUnsentRequestIsNotReSentOncePerPoll() {
        deliverQueue(1);
        deliverQueue(5); // gap -> a request that never goes out, there being no publication here
        final long before = receiver.requestId();

        for (int i = 0; i < 50; i++) {
            receiver.doTimers(false); // duty cycles, no clock advance
        }

        assertTrue(receiver.isAwaitingReplay(), "still waiting — nothing answered it");
        assertTrue(receiver.requestId() - before <= 1,
                   "the resend timer paces this; one id per poll outruns every reply in flight");
    }

    // ── isRecovering() and isCaughtUp() ───────────────────────────────────────────────────────────

    // isRecovering() is what separates "the queue is ahead of my replay" from "there is a hole": narrowing
    // it means a walk supersedes itself on its own in-flight frames, widening it means a real gap goes
    // unreported. This walks every transition it must track.
    @Test
    @DisplayName("the recovering flag tracks the walk and awaiting-replay states")
    void recoveringFlagTracksWalkAndAwaitingReplayState() {
        assertFalse(receiver.isRecovering(), "nothing in flight yet");

        deliverQueue(1);
        deliverQueue(5); // skips 2..4 -> gap, re-requests a replay
        assertTrue(receiver.isRecovering(), "awaiting the Replayer's answer is already mid-recovery");

        answerReplaying(7, 500);
        assertTrue(receiver.isRecovering(), "a session id means history remains to replay");

        deliverReplay(1); // opens on the anchored frame -> anchor consumed, frame deduped
        deliverReplay(2);
        deliverReplay(3);
        deliverReplay(4); // closes the hole, draining the retained frame 5
        completeReplay();
        assertFalse(receiver.isRecovering(), "a resume ends at its bound");

        // A resume whose bound leaves a retained frame stranded behind a hole resumes again.
        deliverQueue(9); // another gap -> another resume, anchored on frame 5
        answerReplaying(8, 500);
        deliverReplay(5);
        deliverReplay(6);
        completeReplay(); // 7 and 8 not covered: 9 is still stranded
        assertTrue(receiver.isRecovering(), "awaiting the next resume");

        answerReplaying(9, 500);
        deliverReplay(6); // anchored on frame 6, the last dispatched
        deliverReplay(7);
        deliverReplay(8); // dispatching 8 drains the retained 9 straight over
        assertTrue(receiver.isRecovering(), "mid-resume until the bound");
        completeReplay();
        assertFalse(receiver.isRecovering(), "that resume ends at its bound too");
    }

    // isCaughtUp() is a state, not a latch: consumers gate real decisions on it, and a gateway's queue-stall
    // watchdog measures silence in DISPATCHED frames. A re-walk dispatches nothing until the replay passes
    // the hole, so leaving it latched makes a gateway diagnose its own recovery as a stalled sequencer.
    @Test
    @DisplayName("a queue gap revokes caught-up until the stream goes contiguous again")
    void queueGapRevokesCaughtUpUntilTheStreamGoesContiguousAgain() {
        deliverQueue(1);
        deliverQueue(2);
        assertTrue(receiver.isCaughtUp());
        assertEquals(1, caughtUpAt.size());

        deliverQueue(5); // skips 3, 4 -> gap (and 5 is retained, not dropped)
        assertFalse(receiver.isCaughtUp(), "a hole means we are demonstrably not following live");

        deliverReplay(2); // a resume opens on the frame it anchored at — already delivered, deduped
        deliverReplay(3);
        assertFalse(receiver.isCaughtUp(), "4 is still missing — the hole is not closed yet");
        assertEquals(1, caughtUpAt.size());

        deliverReplay(4); // closes the hole; the retained queue frame 5 then hands straight over

        assertTrue(receiver.isCaughtUp(), "the retained frame closes the seam with no further round trip");
        assertEquals(2, caughtUpAt.size(), "consumers must be able to re-arm on re-convergence");
    }

    // ── the retained-ahead FIFO ───────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("retained queue frames hand straight over when the replay reaches them")
    void retainedFramesCloseTheSeam() {
        attachReplay(11, 4096);
        deliverQueue(3);
        deliverQueue(4);

        deliverReplay(1);
        deliverReplay(2);

        assertEquals(List.of(1L, 2L, 3L, 4L), dispatched, "the seam closes with no residual");
        assertEquals(0, receiver.retainedFrameCount());
    }

    @Test
    @DisplayName("frames beyond the hole are retained and delivered exactly once when it closes")
    void framesBeyondTheHoleAreRetainedAndDeliveredOnceItCloses() {
        deliverQueue(1);
        deliverQueue(4); // gap -> retained
        deliverQueue(5); // still ahead of the hole -> retained
        deliverQueue(6);
        assertEquals(List.of(1L), dispatched, "nothing past the hole may be dispatched early");

        deliverReplay(1); // a resume opens on the frame it anchored at — already delivered, deduped
        deliverReplay(2);
        assertEquals(List.of(1L, 2L), dispatched, "3 is still missing");

        deliverReplay(3); // closes the hole -> 4, 5, 6 drain behind it

        assertEquals(List.of(1L, 2L, 3L, 4L, 5L, 6L), dispatched, "retained frames drain in order, exactly once");
        assertTrue(receiver.isCaughtUp());
    }

    @Test
    @DisplayName("a frame of the largest payload ahead of the hole is retained, not an overflow")
    void largestFrameAheadOfTheHoleIsRetained() {
        deliverQueue(1);
        deliverQueuePadded(3, LARGEST_FRAME_LENGTH); // gap -> retained
        assertEquals(1, receiver.retainedFrameCount());

        deliverReplay(1); // a resume opens on the frame it anchored at — already delivered, deduped
        deliverReplay(2); // closes the hole -> 3 drains behind it

        assertEquals(List.of(1L, 2L, 3L), dispatched);
        assertTrue(receiver.isCaughtUp(), "an overflow would have forced a re-walk");
    }

    // The ranges legitimately overlap, since a re-walk restarts from the start while the queue keeps
    // arriving.
    @Test
    @DisplayName("retained frames the replay has meanwhile covered are not redelivered")
    void retainedFramesAlreadyCoveredByTheReplayAreNotRedelivered() {
        deliverQueue(1);
        deliverQueue(3); // gap -> retained
        deliverQueue(4); // retained

        deliverReplay(1); // a resume opens on the frame it anchored at — already delivered, deduped
        deliverReplay(2);
        deliverReplay(3); // the replay covers a frame the queue already retained
        deliverReplay(4);

        assertEquals(List.of(1L, 2L, 3L, 4L), dispatched, "each clusterSeqNo dispatched exactly once");
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
        deliverReplay(7); // the recording does not cover 2..6

        assertEquals(1, dispatched.size(), "the frame past the hole must not be dispatched");
        assertEquals(1, logged.size());
        assertEquals(Logger.Severity.Warn, logged.get(0).severity());
        assertEquals(Logger.CoreEventCode.QueueGap, logged.get(0).code());

        deliverReplay(8);
        assertEquals(1, logged.size(), "report the episode, not every frame of a walk retrying the same recording");

        // A later, distinct episode must be reported again rather than swallowed by the latch.
        deliverReplay(2);
        assertEquals(2, dispatched.size());
        deliverReplay(9);
        assertEquals(2, logged.size(), "the latch clears once history goes contiguous again");
    }

    @Test
    @DisplayName("dropped queue frames are reported once per episode")
    void droppedQueueFramesAreReportedOncePerEpisode() {
        deliverQueue(1);
        deliverQueue(3); // gap -> resume anchored on frame 1; 3 is retained

        captureLogs(); // installed after the setup gap, so it captures only the overflow lines
        deliverQueueTooBigToRetain(4);
        deliverQueueTooBigToRetain(5);
        assertEquals(1, overflowReports(), "one episode is one line, not one per dropped frame");

        // The resume is what covers the dropped frames, so the episode ends when it is requested.
        answerReplaying(7, 500);
        deliverReplay(1);
        deliverReplay(2);
        completeReplay();
        assertEquals(3072, receiver.requestFromPosition(), "at the retained frame 3, drained behind 2");

        deliverQueueTooBigToRetain(9);

        assertEquals(2, overflowReports(), "a later overflow is a distinct episode and names itself");
    }

    // ── the resume anchor ─────────────────────────────────────────────────────────────────────────

    // Positions follow from the frames and the recording changes only with the node's driver, so a resume
    // opening anywhere but its anchor means the recording is not what this client's positions say.
    @Test
    @DisplayName("a resume replay that opens at the wrong frame is fatal")
    void resumeAnchorMismatchIsFatal() {
        goLive();
        deliverQueueAt(1, 1024);
        deliverQueueAt(2, 2048);
        deliverQueueAt(5, 8192); // gap → resume anchored on clusterSeqNo 2
        attachReplay(12, 16384);

        deliverReplay(4); // not the anchor

        assertTrue(receiver.fault().contains("expected 2"), receiver.fault());
        assertFalse(receiver.isAwaitingReplay(), "nothing more is asked for");
        assertEquals(2, dispatched.size());
    }

    // The anchor is consumed by the first frame of ONE replay episode — a retry of that same logical
    // resume starts a NEW episode and must re-arm the check, or the retried replay's first frame rides
    // with nothing to validate against and a wrong frame is dispatched merely because it looks contiguous.
    @Test
    @DisplayName("a resume whose image closes short of its bound re-arms the anchor check")
    void replayImageClosingShortOfTheBoundOnAResumeReArmsTheAnchorCheck() {
        deliverQueue(1);
        deliverQueue(5); // gap -> resume anchored on frame 1
        answerReplaying(7, 500);
        deliverReplay(1); // opens on the anchored frame -> anchor consumed, frame deduped
        assertEquals(1, dispatched.size());

        receiver.onReplayImageClosed(300); // stopped under us before frame 2 arrived
        assertNotEquals(FROM_START, receiver.requestFromPosition(), "still a resume retry, not a walk step");

        answerReplaying(8, 500);
        // The retried resume's first frame is NOT the frame it was anchored on, but it looks contiguous.
        deliverReplay(2);

        assertEquals(1, dispatched.size(), "must not accept an unanchored frame merely because it looks contiguous");
        assertTrue(receiver.fault() != null, "the mismatch is fatal");
        assertEquals(-1, receiver.replaySessionId());
        assertFalse(receiver.isCaughtUp());
    }

    @Test
    @DisplayName("a resume that stalls re-arms the anchor check")
    void replayStallOnAResumeReArmsTheAnchorCheck() {
        deliverQueue(1);
        deliverQueue(5); // gap -> resume anchored on frame 1
        answerReplaying(7, 500);
        deliverReplay(1);
        assertEquals(1, dispatched.size());

        advancePastTimers(); // stopped advancing before frame 2 arrived
        assertNotEquals(FROM_START, receiver.requestFromPosition(), "still a resume retry, not a walk step");

        answerReplaying(8, 500);
        deliverReplay(2); // looks contiguous but is not the anchored frame

        assertEquals(1, dispatched.size(), "must not accept an unanchored frame merely because it looks contiguous");
        assertTrue(receiver.fault() != null, "the mismatch is fatal");
        assertEquals(-1, receiver.replaySessionId());
        assertFalse(receiver.isCaughtUp());
    }

    // For a walk "nothing to replay" means caught up. For a resume the Replayer answers it only when it
    // refuses the position; a recording that has not reached it yet is answered ReplayPending instead.
    @Test
    @DisplayName("NO_REPLAY_NEEDED for a resume is fatal")
    void noReplayNeededForAResumeIsFatal() {
        deliverQueue(1);
        deliverQueue(2);
        assertTrue(receiver.isCaughtUp());
        deliverQueue(6); // gap -> resume
        assertNotEquals(FROM_START, receiver.requestFromPosition());

        answerReplaying(NO_REPLAY_NEEDED, 0);

        assertTrue(receiver.fault().contains("refused a resume"), receiver.fault());
        assertFalse(receiver.isAwaitingReplay());
        assertFalse(receiver.isCaughtUp(), "the hole above clusterSeqNo=2 is still open");
    }

    // A resume ends at the bound it was given — so it must declare itself caught up and close its image,
    // which frees the Replayer's slot.
    @Test
    @DisplayName("a resume reaching its bound catches up and requests nothing more")
    void resumeReachingItsBoundCatchesUpAndRequestsNothingMore() {
        deliverQueue(1);
        assertEquals(1, caughtUpAt.size());
        deliverQueue(5); // gap -> resume
        assertFalse(receiver.isCaughtUp());
        answerReplaying(7, 500);

        deliverReplay(1); // opens on the anchored frame -> anchor consumed, frame deduped
        deliverReplay(2);
        deliverReplay(3);
        deliverReplay(4); // closes the hole, draining the retained frame 5
        completeReplay();

        assertFalse(receiver.isAwaitingReplay(), "no follow-up request");
        assertEquals(-1, receiver.replaySessionId());
        assertTrue(receiver.isCaughtUp(), "we hold everything the recording had when the request was served");
        assertEquals(2, caughtUpAt.size(), "re-fired, so consumers re-arm on re-convergence");
    }

    // A resume can reach its bound while a retained-ahead frame still sits behind a hole the replay never
    // covered. Declaring caught up on the bound alone is silently wrong: consumers gate real decisions on
    // it and nothing would rediscover the hole until some later, unrelated queue frame exposed it.
    @Test
    @DisplayName("a resume reaching its bound with a retained hole still open resumes again")
    void resumeReachingItsBoundWithARetainedHoleStillOpenResumesAgain() {
        deliverQueue(1);
        assertEquals(1, caughtUpAt.size());
        deliverQueue(5); // gap -> resume anchored on frame 1; 5 is retained ahead of the hole
        answerReplaying(7, 500);

        deliverReplay(1); // opens on the anchored frame -> anchor consumed, frame deduped
        deliverReplay(2); // the bound is reached having covered only up to 2 — 3, 4 unfilled

        completeReplay();

        assertFalse(receiver.isCaughtUp(), "must not declare caught up with a retained frame behind a hole");
        assertEquals(1, caughtUpAt.size(), "no false re-convergence notification");
        assertEquals(2048, receiver.requestFromPosition(), "resumes at the last dispatched frame, not the start");
        assertTrue(receiver.isAwaitingReplay());
        assertEquals(-1, receiver.replaySessionId());
    }

    // The overflow half of the same guard. When the FIFO had to DROP queue frames the frontier is short by
    // frames that are gone from the queue for good, and every retained frame draining cleanly says nothing
    // about them.
    @Test
    @DisplayName("a resume reaching its bound after dropped queue frames resumes again")
    void resumeReachingItsBoundAfterDroppedQueueFramesResumesAgain() {
        deliverQueue(1);
        assertEquals(1, caughtUpAt.size());
        deliverQueue(3); // gap -> resume anchored on frame 1; 3 is retained
        deliverQueueTooBigToRetain(4); // ahead of the hole too, but dropped rather than retained
        answerReplaying(7, 500);

        deliverReplay(1); // opens on the anchored frame -> anchor consumed, frame deduped
        deliverReplay(2); // closes the hole the resume was asked to cover, draining retained frame 3

        completeReplay();

        assertFalse(receiver.isCaughtUp(), "frame 4 was dropped — the drained frontier is not the real one");
        assertEquals(1, caughtUpAt.size(), "no false re-convergence notification");
        assertEquals(3072, receiver.requestFromPosition(),
                     "resumes now rather than leaving the hole for a later queue gap");
        assertTrue(receiver.isAwaitingReplay());
    }

    // ── the end of a walk ─────────────────────────────────────────────────────────────────────────
    // A walk ends at its bound, and reaching it is not by itself permission to declare caught up.

    @Test
    @DisplayName("frames still retained when a walk reaches its bound force a resume rather than caught-up")
    void retainedResidualForcesAResume() {
        attachReplay(11, 4096);
        deliverQueue(5); // a hole below it that this walk never reached
        deliverReplay(1);
        assertEquals(1, receiver.retainedFrameCount());

        completeReplay();

        assertFalse(receiver.isCaughtUp(), "declaring caught up here would close the hole by fiat");
        assertEquals(1024, receiver.requestFromPosition(), "resumes at the last dispatched frame");
        assertTrue(receiver.isAwaitingReplay());
    }

    /** The negative control for the guard above: a walk whose retained frames all drained must finish. */
    @Test
    @DisplayName("a walk reaching its bound with every retained frame drained catches up")
    void aWalkWithEveryRetainedFrameDrainedCatchesUp() {
        answerReplaying(7, 500);
        deliverReplay(1);
        deliverQueue(3); // ahead of the hole at 2 -> retained
        deliverReplay(2); // closes it, so 3 drains straight over

        completeReplay();

        assertTrue(receiver.isCaughtUp(), "nothing is outstanding — the guard must not fire here");
        assertFalse(receiver.isAwaitingReplay(), "not a spurious resume");
    }

    // The same guard driven through to convergence: the resume must both happen AND be able to finish.
    // Clearing the overflow on drain forgets the drop and catches up over the hole; never clearing it
    // leaves a client that can never declare itself caught up again.
    @Test
    @DisplayName("a walk reaching its bound after dropped queue frames resumes, then catches up")
    void aWalkAfterDroppedQueueFramesResumesThenCatchesUp() {
        answerReplaying(7, 500);
        deliverReplay(1);
        deliverReplay(2);
        deliverQueueTooBigToRetain(4); // the queue runs ahead of the walk, and this one is dropped

        completeReplay();

        assertFalse(receiver.isCaughtUp(), "the walk reached its bound, but a queue frame above it was dropped");
        assertEquals(0, caughtUpAt.size(), "no false convergence notification — this opens the accept gate");
        assertEquals(2048, receiver.requestFromPosition(), "resumes rather than trusting the bound");

        // The resume replays what the drop lost, and this time nothing is dropped.
        answerReplaying(8, 900);
        deliverReplay(2); // the anchor
        deliverReplay(3);
        deliverReplay(4);
        completeReplay();

        assertTrue(receiver.isCaughtUp(), "a resume that dropped nothing must be able to finish");
        assertEquals(1, caughtUpAt.size());
    }

    // ── the convergence alarm ─────────────────────────────────────────────────────────────────────
    // Measured by progress, not elapsed time: a cold start has no time bound. Locked here: an in-order
    // dispatch counts as progress, catching up without dispatching does too, the report carries the state
    // telling the causes apart, and each episode reports and clears exactly once.

    @Test
    @DisplayName("recovery delivering nothing is reported once per episode")
    void recoveryDeliveringNothingIsReportedOncePerEpisode() {
        captureLogs();

        assertFalse(checkProgressAt(CLOCK_MS), "the first observation only anchors the clock");
        assertTrue(checkProgressAt(PAST_DEADLINE_MS));

        assertEquals(1, logged.size());
        assertEquals(Logger.CoreComponent.ReplayerStreamReceiver, logged.get(0).component());
        assertEquals(Logger.Severity.Fault, logged.get(0).severity());
        assertEquals(Logger.CoreEventCode.RecoveryStalled, logged.get(0).code());

        // The condition persists for as long as the archive is broken; the caller logs, so it must not
        // repeat every duty cycle.
        assertFalse(checkProgressAt(PAST_DEADLINE_MS + 60_000));
        assertEquals(1, logged.size());
    }

    @Test
    @DisplayName("a walk that is still delivering is never reported, however long it runs")
    void aWalkThatIsStillDeliveringIsNeverReported() {
        captureLogs();

        // A cold start replaying a long log: slow, but converging one frame at a time. Measuring
        // elapsed time instead of progress is exactly what would fence this.
        for (long clusterSeqNo = 1; clusterSeqNo <= 20; clusterSeqNo++) {
            deliverReplay(clusterSeqNo);
            assertFalse(checkProgressAt(CLOCK_MS + clusterSeqNo * 60_000), "at clusterSeqNo " + clusterSeqNo);
        }
    }

    @Test
    @DisplayName("a gap after a healthy run starts its own episode rather than inheriting one")
    void aGapAfterAHealthyRunStartsItsOwnEpisode() {
        captureLogs();
        assertFalse(checkProgressAt(CLOCK_MS)); // an episode anchored during cold start
        deliverQueue(1); // ... which then converges
        assertTrue(receiver.isCaughtUp());

        deliverQueue(5); // much later, a queue gap: not caught up again
        assertFalse(receiver.isCaughtUp());

        // Inheriting the cold-start clock would report this the instant the gap opened, before the re-walk
        // it triggers had any chance to converge.
        assertFalse(checkProgressAt(PAST_DEADLINE_MS));
        assertEquals(1, logged.size(), "the queue-gap warn only — no convergence report");
        assertEquals(Logger.CoreEventCode.QueueGap, logged.get(0).code());
    }

    @Test
    @DisplayName("a caught-up client that simply goes quiet is not reported")
    void aCaughtUpClientThatGoesQuietIsNotReported() {
        captureLogs();
        deliverQueue(1);
        assertTrue(receiver.isCaughtUp());

        // A queue that goes silent is a gateway's queue-stall watchdog's business, and it is not a convergence
        // failure at all — diagnosing it as one puts a second, wrong explanation on the same event.
        assertFalse(checkProgressAt(CLOCK_MS));
        assertFalse(checkProgressAt(PAST_DEADLINE_MS));
        assertTrue(logged.isEmpty());
    }

    @Test
    @DisplayName("the report names the state that tells the causes apart")
    void theReportNamesTheStateThatTellsTheCausesApart() {
        deliverQueue(1);
        deliverQueue(5); // gap -> awaiting a replay

        captureLogs(); // installed after the gap, so it captures only the report below
        assertFalse(checkProgressAt(CLOCK_MS));
        assertTrue(checkProgressAt(PAST_DEADLINE_MS));

        assertEquals(1, logged.size());
        final String text = logged.get(0).message();
        // Without these an operator cannot tell an unanswering node from one whose recording cannot cover the hole.
        assertTrue(text.contains("lastClusterSeqNo=1"), text);
        assertTrue(text.contains("awaitingReplay=true"), text);
    }

    @Test
    @DisplayName("the stall gauge clears once, on the dispatch that ends a reported episode")
    void theStallGaugeClearsOnceOnTheDispatchThatEndsAReportedEpisode() {
        captureLogs();
        deliverReplay(1);
        assertTrue(actions.stalledGauge.isEmpty(), "progress with nothing reported has no edge to fall from");

        assertFalse(checkProgressAt(CLOCK_MS));
        assertTrue(checkProgressAt(PAST_DEADLINE_MS));
        deliverReplay(2); // the reported episode ends here
        deliverReplay(3); // ... and only here

        assertEquals(List.of(true, false), actions.stalledGauge);
    }

    @Test
    @DisplayName("a later episode is reported again rather than swallowed by the first")
    void aLaterEpisodeIsReportedAgain() {
        captureLogs();
        assertFalse(checkProgressAt(CLOCK_MS));
        assertTrue(checkProgressAt(PAST_DEADLINE_MS));
        deliverReplay(1); // one frame got through: the walk is slow, not stuck

        assertFalse(checkProgressAt(PAST_DEADLINE_MS + 1_000), "anchors a fresh episode");
        assertFalse(checkProgressAt(PAST_DEADLINE_MS + 30_000), "rather than inheriting the old clock");
        assertTrue(checkProgressAt(PAST_DEADLINE_MS + 31_000), "a second, distinct episode reports too");
        assertEquals(2, logged.size());
    }

    // ── Delivery ───────────────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("a leadership change reaches onSequenced when no leadership handler is given")
    void aLeadershipChangeReachesOnSequencedWithoutAHandler() {
        deliverLeadershipChanged(1);

        assertEquals(List.of(1L), dispatched, "dropped rather than delivered");
    }

    @Test
    @DisplayName("a leadership change with a handler goes to it alone")
    void aLeadershipChangeWithAHandlerGoesToItAlone() {
        final List<Long> changes = new ArrayList<>();
        receiver = new ReplayerRecovery(actions, event -> dispatched.add(event.clusterSeqNo()),
                                        (leaderMemberId, leadershipTermId, clusterSeqNo) -> changes.add(clusterSeqNo),
                                        null);
        deliverLeadershipChanged(1);

        assertEquals(List.of(1L), changes);
        assertTrue(dispatched.isEmpty(), "delivered twice");
    }

    @Test
    @DisplayName("onSequenced is required")
    void onSequencedIsRequired() {
        assertThrows(NullPointerException.class, () -> new ReplayerRecovery(actions, null, null, null));
    }

    // ── helpers ───────────────────────────────────────────────────────────────────────────────────

    // ── restoring a snapshot ──────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("a restoring cold start replays from its newest snapshot's cut first, holding the live queue")
    void restoringColdStartReplaysItsSnapshotsCutFirst() {
        RestoreFrames.write(store, 1, CUT - 5, (CUT - 5) * 1024, 1, SNAPSHOT_HEADER, SNAPSHOT_RECORD);
        writeSnapshot(2);
        startRestoring();

        assertEquals(1, actions.requestsSent);
        assertTrue(receiver.isAwaitingReplay());
        assertEquals(CUT * 1024, receiver.requestFromPosition(), "the newest round's cut");
        assertEquals(CUT, actions.lastCutClusterSeqNo, "the request names the cut, the position being a hint");
        assertFalse(receiver.isRestoring(), "no record before the cut is seen");
        assertTrue(receiver.isRecovering());

        deliverQueue(40); // mid-stream, and no baseline: held, not fatal
        assertTrue(dispatched.isEmpty());
    }

    @Test
    @DisplayName("an unanswered replay of the cut is resent; a pending answer holds it")
    void unansweredCutReplayIsResent() {
        writeSnapshot(2);
        startRestoring();
        advancePastTimers();
        assertEquals(2, actions.requestsSent);
        assertEquals(CUT * 1024, receiver.requestFromPosition());
        assertEquals(CUT, actions.lastCutClusterSeqNo, "the resend names the cut too");

        deliverPending(receiver.requestId());
        actions.clockMs += 100;
        receiver.doTimers(false);
        assertEquals(2, actions.requestsSent);
    }

    @Test
    @DisplayName("with no local snapshot, the cold start walks the recording from its start")
    void noSnapshotWalksFromTheStart() {
        startRestoring();

        assertEquals(1, actions.requestsSent);
        assertFalse(receiver.isRestoring());
        assertTrue(receiver.isAwaitingReplay());
        assertEquals(FROM_START, receiver.requestFromPosition());
        assertEquals(ReplayProtocol.NO_CUT, actions.lastCutClusterSeqNo, "a walk names no cut");
    }

    @Test
    @DisplayName("a snapshot whose cut the recording holds is restored from its file, then resumes anchored at its cut")
    void snapshotIsRestoredThenResumesAtItsCut() {
        writeSnapshot(2);
        startRestoring();
        attachReplay(21, 64 * 1024);
        deliverReplayFrame(RestoreFrames.started(CUT, 2), CUT);
        assertTrue(receiver.isRestoring());
        assertEquals(-1, receiver.replaySessionId(), "the records come from the file, not the replay");
        assertTrue(dispatched.isEmpty(), "the cut is not dispatched");

        receiver.doTimers(false);
        assertTrue(restored.isEmpty(), "the records are checked against the trailer before any is passed on");
        assertTrue(receiver.isRestoring());

        receiver.doTimers(false);

        assertEquals(List.of("header 4 2", "0 7"), restored);
        assertTrue(dispatched.isEmpty());
        assertFalse(receiver.isRestoring());
        assertEquals(CUT, receiver.lastClusterSeqNo());
        assertEquals(2, receiver.currentLeaderMemberId(), "the header's leader");
        assertTrue(receiver.isAwaitingReplay());
        assertEquals(CUT * 1024, receiver.requestFromPosition());

        attachReplay(22, 64 * 1024);
        deliverReplayFrame(RestoreFrames.started(CUT, 2), CUT); // the anchor, already dispatched
        deliverReplay(11);
        deliverReplay(12);
        assertEquals(List.of(11L, 12L), dispatched);
    }

    @Test
    @DisplayName("a cut found elsewhere in the recording than its file's position is resumed at where it was found")
    void restoreResumesAtTheCutsPositionInThisRecording() {
        writeSnapshot(2);
        startRestoring();
        attachReplay(21, 64 * 1024);
        deliverReplayFrame(RestoreFrames.started(CUT, 2), CUT + 7); // the file's cutPosition is a hint only
        receiver.doTimers(false);
        receiver.doTimers(false);

        assertFalse(receiver.isRestoring());
        assertEquals((CUT + 7) * 1024, receiver.requestFromPosition(), "not the file's");
    }

    @Test
    @DisplayName("after a restore, the queue's continuity resumes at the cut frame's groupSeqNo")
    void afterARestoreContinuityResumesAtTheCutsGroupSeqNo() {
        writeSnapshot(2);
        startRestoring();
        attachReplay(21, 64 * 1024);
        deliverReplayFrame(RestoreFrames.started(CUT, 4, 2, CUT * 1000), 4); // the queue's fourth frame
        receiver.doTimers(false);
        receiver.doTimers(false);

        attachReplay(22, 64 * 1024);
        deliverReplayFrame(RestoreFrames.started(CUT, 4, 2, CUT * 1000), 4);
        deliverReplayOnQueue(CUT + 3, 5);
        assertEquals(List.of(CUT + 3), dispatched, "groupSeqNo 5 follows the cut's 4");
    }

    @Test
    @DisplayName("a replay of the cut that stops advancing is requested again at the cut")
    void stalledCutReplayIsRequestedAgain() {
        writeSnapshot(2);
        startRestoring();
        attachReplay(21, 64 * 1024);
        advancePastTimers();

        assertTrue(receiver.isAwaitingReplay());
        assertEquals(CUT * 1024, receiver.requestFromPosition());
        attachReplay(22, 64 * 1024);
        deliverReplayFrame(RestoreFrames.started(CUT, 2), CUT);
        assertTrue(receiver.isRestoring());
    }

    @Test
    @DisplayName("a restore reads a bounded number of records per duty cycle, counted as its work, holding the queue "
                 + "until its last")
    void restoreIsPacedAcrossDutyCycles() {
        final byte[][] records = new byte[ReplayerRecovery.MAX_RESTORE_RECORDS_PER_CYCLE + 1][];
        records[0] = SNAPSHOT_HEADER;
        for (int i = 1; i < records.length; i++) {
            records[i] = RestoreFrames.record(i);
        }
        RestoreFrames.write(store, 2, CUT, CUT * 1024, 1, records);
        startRestoring();
        assertEquals(0, receiver.doTimers(false), "waiting for the cut is no work");
        replayTheCut();

        assertEquals(ReplayerRecovery.MAX_RESTORE_RECORDS_PER_CYCLE, receiver.doTimers(false));
        assertEquals(1, receiver.doTimers(false));
        assertTrue(restored.isEmpty(), "the check is paced the same");
        assertTrue(receiver.isRestoring());

        assertEquals(ReplayerRecovery.MAX_RESTORE_RECORDS_PER_CYCLE, receiver.doTimers(false));
        assertTrue(receiver.isRestoring());
        assertEquals(ReplayerRecovery.MAX_RESTORE_RECORDS_PER_CYCLE, restored.size());
        deliverQueue(40);
        assertTrue(dispatched.isEmpty());

        assertEquals(1, receiver.doTimers(false));
        assertFalse(receiver.isRestoring());
        assertEquals(records.length, restored.size());
        assertTrue(receiver.isAwaitingReplay());
        assertEquals(0, receiver.doTimers(false), "nothing once restored");
    }

    @Test
    @DisplayName("a snapshot whose cut the recording does not hold gives way to the next older one, and the last to a "
                 + "walk")
    void snapshotOfAnotherLogGivesWayToAnOlderOne() throws IOException {
        for (long round = 3; round >= 1; round--) {
            writeSnapshot(round); // newest first: a confirmation deletes the older files
        }
        final Path cutShort = storeDirectory().resolve("4.snapshot");
        Files.write(cutShort, Arrays.copyOf(Files.readAllBytes(storeDirectory().resolve("3.snapshot")), 40));
        startRestoring();

        assertEquals(1, actions.requestsSent, "round 4 is cut short: no replay for it");
        attachReplay(21, 64 * 1024);
        deliverReplayFrame(RestoreFrames.started(CUT, 3, 1), CUT); // another log's frame at round 3's cut
        assertFalse(receiver.isRestoring());
        assertEquals(2, actions.requestsSent, "round 2's cut, next");
        attachReplay(22, 64 * 1024);
        deliverReplay(CUT + 1); // round 2's cut is not where this recording has it
        assertFalse(receiver.isRestoring());
        assertEquals(3, actions.requestsSent);
        answerReplaying(NO_REPLAY_NEEDED, 0); // round 1's cut is no frame of this recording
        assertNull(receiver.fault());
        assertFalse(receiver.isRestoring());
        assertTrue(receiver.isAwaitingReplay());
        assertEquals(FROM_START, receiver.requestFromPosition());
        assertTrue(restored.isEmpty(), "nothing restored from a file the recording does not hold");
    }

    @Test
    @DisplayName("after a restore, a resume that opens anywhere but the cut is fatal")
    void afterARestoreAResumeOpeningAnywhereButTheCutIsFatal() {
        writeSnapshot(2);
        startRestoring();
        replayTheCut();
        receiver.doTimers(false); // the records' check
        receiver.doTimers(false);
        attachReplay(22, 64 * 1024);

        deliverReplay(12); // not the anchor

        assertTrue(receiver.fault().contains("expected " + CUT), receiver.fault());
        assertTrue(dispatched.isEmpty(), "nothing after a wrong cut may be dispatched");
    }

    @Test
    @DisplayName("a snapshot that cannot be restored stops recovery: its format or its header")
    void unrestorableSnapshotStopsRecovery() {
        RestoreFrames.write(store, 2, CUT, CUT * 1024, 9, SNAPSHOT_HEADER, SNAPSHOT_RECORD);
        startRestoring();
        assertTrue(receiver.restoreFailure().contains("formatVersion 9"));
        assertFalse(receiver.isRecovering());
        assertEquals(0, actions.requestsSent);
        deliverQueue(40); // no baseline, and no longer an error either
        assertTrue(dispatched.isEmpty());

        setUp();
        final byte[] header = SNAPSHOT_HEADER.clone();
        header[0] = 9;
        RestoreFrames.write(store, 2, CUT, CUT * 1024, 1, header, SNAPSHOT_RECORD);
        startRestoring();
        replayTheCut();
        receiver.doTimers(false); // the records' check
        receiver.doTimers(false);
        assertTrue(receiver.restoreFailure().contains("header of version 9"));
        assertFalse(receiver.isRecovering());
        assertNull(new ReplayerRecovery(actions, event -> { }, null, null).restoreFailure());
    }

    @Test
    @DisplayName("a snapshot whose records do not match its trailer gives way to the next older one before the handler "
                 + "sees any")
    void damagedSnapshotGivesWayToAnOlderOne() throws IOException {
        RestoreFrames.write(store, 1, CUT - 5, (CUT - 5) * 1024, 1, SNAPSHOT_HEADER, RestoreFrames.record(5));
        writeSnapshot(2);
        final Path file = storeDirectory().resolve("2.snapshot");
        final byte[] damaged = Files.readAllBytes(file);
        damaged[2 + SNAPSHOT_HEADER.length + 2] ^= 1; // the record's first byte; the trailer still matches
        Files.write(file, damaged);
        startRestoring();
        replayTheCut();

        receiver.doTimers(false);
        assertNull(receiver.restoreFailure());
        assertTrue(restored.isEmpty(), "nothing of the damaged file reaches the handler");
        assertFalse(receiver.isRestoring());
        assertEquals(2, actions.requestsSent, "round 1's cut, next");
        assertEquals((CUT - 5) * 1024, receiver.requestFromPosition());

        attachReplay(22, 64 * 1024);
        deliverReplayFrame(RestoreFrames.started(CUT - 5, 1), CUT - 5);
        receiver.doTimers(false);
        receiver.doTimers(false);
        assertEquals(List.of("header 4 2", "0 5"), restored);
        assertEquals(CUT - 5, receiver.lastClusterSeqNo());
        assertTrue(Files.exists(file), "the damaged file stays until its round is confirmed again");
    }

    private void startRestoring() {
        receiver.restoreFrom(store, new SnapshotRestoreHandler() {
            @Override
            public boolean supportsFormatVersion(final long formatVersion) {
                return formatVersion == 1;
            }

            @Override
            public void onSnapshotHeader(final SnapshotHeader header) {
                restored.add("header " + header.leadershipTermId() + " " + header.leaderMemberId());
            }

            @Override
            public void onSnapshotRecord(final DirectBuffer record, final int length, final int recordIndex,
                                         final long formatVersion) {
                assertEquals(1, formatVersion);
                restored.add(recordIndex + " " + record.getLong(0));
            }
        });
        receiver.start();
    }

    /** This instance's confirmed file of {@code round}, cut at {@link #CUT}: the header and one record. */
    private void writeSnapshot(final long round) {
        RestoreFrames.write(store, round, CUT, CUT * 1024, 1, SNAPSHOT_HEADER, SNAPSHOT_RECORD);
    }

    /** The current store's directory. */
    private Path storeDirectory() {
        return directory.resolve("instance-" + (stores - 1));
    }

    /** The replay of the cut opens on the round's {@code SnapshotStarted}, as the file recorded it. */
    private void replayTheCut() {
        attachReplay(21, 64 * 1024);
        deliverReplayFrame(RestoreFrames.started(CUT, 2), CUT);
    }

    private void deliverReplayFrame(final RestoreFrames.Frame frame, final long clusterSeqNo) {
        receiver.onFrame(frame.buffer(), 0, frame.length(), clusterSeqNo * 1024, RECEIVE_NS, true);
    }

    /**
     * Starts capturing log events here rather than in {@code setUp}: several cases must see only what
     * follows their own setup, and the C++ twin installs its sink at the same points for the same reason.
     */
    private void captureLogs() {
        logged.clear();
        Logger.install(logged::add);
    }

    /** Retained-buffer overflows only: every gap/re-walk warn shares QueueGap, so only the text separates them. */
    private long overflowReports() {
        return logged.stream().filter(event -> event.message().contains("retained-frame buffer full")).count();
    }

    /** Drives the receiver to the caught-up, steady state a live consumer runs in. */
    private void goLive() {
        answerReplaying(NO_REPLAY_NEEDED, 0);
    }

    private void attachReplay(final long replaySessionId, final long catchUpPosition) {
        answerReplaying(replaySessionId, catchUpPosition);
    }

    private void answerReplaying(final long replaySessionId, final long catchUpPosition) {
        receiver.onControl(replayingBuffer(receiver.requestId(), replaySessionId, catchUpPosition), 0,
                           replayingLength());
    }

    private void deliverPending(final long requestId) {
        receiver.onControl(replayPendingBuffer(requestId), 0, replayPendingLength());
    }

    private void deliverLeadershipChanged(final long clusterSeqNo) {
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[256]);
        final LeadershipChangedEncoder frame = new LeadershipChangedEncoder();
        frame.wrapAndApplyHeader(buffer, 0, new org.limitless.seqeron.sbe.frame.MessageHeaderEncoder());
        frame.header()
            .sourceId(-1)
            .connectionId(-1)
            .sessionId(-1)
            .systemEventType(org.limitless.seqeron.protocol.SystemFrame.LEADERSHIP_CHANGED)
            .clusterSeqNo(clusterSeqNo)
            .timestamp(clusterSeqNo * 1000)
            .groupSeqNo(clusterSeqNo);
        frame.newLeaderMemberId(2).leadershipTermId(3);
        receiver.onFrame(buffer, 0, org.limitless.seqeron.sbe.frame.MessageHeaderEncoder.ENCODED_LENGTH
            + LeadershipChangedEncoder.BLOCK_LENGTH, clusterSeqNo * 1024, RECEIVE_NS, false);
    }

    /** The replay image reaching the bound the Replayer gave it — how a replay completes. */
    private void completeReplay() {
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

    private void deliverReplay(final long clusterSeqNo) {
        receiver.onFrame(heartbeatFrame(clusterSeqNo), 0, heartbeatLength(), clusterSeqNo * 1024, RECEIVE_NS, true);
    }

    /** A replayed frame at {@code groupSeqNo} on a queue that does not carry every frame, starting at its 1024s. */
    private void deliverReplayOnQueue(final long clusterSeqNo, final long groupSeqNo) {
        receiver.onFrame(heartbeatFrame(clusterSeqNo, groupSeqNo), 0, heartbeatLength(), groupSeqNo * 1024, RECEIVE_NS,
                         true);
    }

    /** The same off the live queue. */
    private void deliverQueueOnQueue(final long clusterSeqNo, final long groupSeqNo) {
        receiver.onFrame(heartbeatFrame(clusterSeqNo, groupSeqNo), 0, heartbeatLength(), groupSeqNo * 1024, RECEIVE_NS,
                         false);
    }

    private void deliverQueue(final long clusterSeqNo) {
        deliverQueueAt(clusterSeqNo, clusterSeqNo * 1024);
    }

    private void deliverQueueAt(final long clusterSeqNo, final long position) {
        receiver.onFrame(heartbeatFrame(clusterSeqNo), 0, heartbeatLength(), position, RECEIVE_NS, false);
    }

    /**
     * A live queue frame too big for the retained-ahead FIFO to hold: a heartbeat zero-padded past one
     * RetainBlock, which retainFrame refuses outright. The cheapest of its three overflow triggers to
     * drive — the other two need 65536 frames or 16 MiB.
     */
    private void deliverQueueTooBigToRetain(final long clusterSeqNo) {
        deliverQueuePadded(clusterSeqNo, OVERSIZED_FRAME_LENGTH);
    }

    /** A live queue frame of {@code length} bytes: a heartbeat, decoded from the front, zero-padded to it. */
    private void deliverQueuePadded(final long clusterSeqNo, final int length) {
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[length]);
        buffer.putBytes(0, heartbeatFrame(clusterSeqNo), 0, heartbeatLength());
        receiver.onFrame(buffer, 0, length, clusterSeqNo * 1024, RECEIVE_NS, false);
    }

    /**
     * One synthesized ClusterHeartbeat — the cheapest well-formed frame there is, at 52 bytes — on a queue that
     * carries every frame, so its groupSeqNo is its clusterSeqNo.
     */
    private static UnsafeBuffer heartbeatFrame(final long clusterSeqNo) {
        return heartbeatFrame(clusterSeqNo, clusterSeqNo);
    }

    private static UnsafeBuffer heartbeatFrame(final long clusterSeqNo, final long groupSeqNo) {
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[256]);
        final ClusterHeartbeatEncoder frame = new ClusterHeartbeatEncoder();
        frame.wrapAndApplyHeader(buffer, 0, new org.limitless.seqeron.sbe.frame.MessageHeaderEncoder());
        frame.header()
            .sourceId(-1)
            .connectionId(-1)
            .sessionId(-1)
            .systemEventType(org.limitless.seqeron.protocol.SystemFrame.CLUSTER_HEARTBEAT)
            .clusterSeqNo(clusterSeqNo)
            .timestamp(clusterSeqNo * 1000)
            .groupSeqNo(groupSeqNo);
        return buffer;
    }

    private static int heartbeatLength() {
        return org.limitless.seqeron.sbe.frame.MessageHeaderEncoder.ENCODED_LENGTH +
               ClusterHeartbeatEncoder.BLOCK_LENGTH;
    }

    private static UnsafeBuffer replayingBuffer(final long requestId, final long replaySessionId,
                                                final long catchUpPosition) {
        return replayingBuffer(CLIENT_ID, requestId, replaySessionId, catchUpPosition);
    }

    private static UnsafeBuffer replayingBuffer(final long clientId, final long requestId,
                                                final long replaySessionId, final long catchUpPosition) {
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[256]);
        new ReplayingEncoder()
            .wrapAndApplyHeader(buffer, 0, new org.limitless.seqeron.sbe.replay.MessageHeaderEncoder())
            .clientId(clientId)
            .requestId(requestId)
            .replaySessionId(replaySessionId)
            .catchUpPosition(catchUpPosition);
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
}
