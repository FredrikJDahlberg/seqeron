package org.limitless.phixeron.replayer.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.limitless.phixeron.replayer.server.ReplayerService;
import org.limitless.phixeron.sbe.sequenced.Origin;
import org.limitless.phixeron.sbe.sequenced.TickEncoder;
import org.limitless.phixeron.sbe.unsequenced.ReplayingEncoder;

/**
 * Unit tests for the walk / gap-recovery state machine, and the twin of the C++
 * {@code ReplayerRecoveryTest.cpp}. {@link ReplayerRecovery} holds no Aeron runtime and no clock of its
 * own, so the suite drives it directly: its transport is recorded through {@link ReplayerRecoveryActions}
 * and its clock is owned by the test.
 */
class ReplayerRecoveryTest {
    private static final int CLIENT_ID = 4;
    private static final long NO_REPLAY_NEEDED = ReplayerService.NO_REPLAY_NEEDED;

    /** The walk terminator: nothing left to replay AND no recording named. */
    private static final long CHAIN_EXHAUSTED = -1;

    /** Arrival stamp; carried through to SequencedEvent, asserted on by no test here. */
    private static final long RECEIVE_NS = 0;

    /** Stands in for the wall clock. The absolute value is arbitrary; only applied deltas matter. */
    private static final long CLOCK_MS = 3 * 60 * 60 * 1000L;

    private final List<Long> dispatched = new ArrayList<>();
    private final List<Long> caughtUpAt = new ArrayList<>();
    private RecordingActions actions;
    private ReplayerRecovery receiver;

    /** The transport recorded rather than performed — no publication, so nothing ever reaches the wire. */
    private static final class RecordingActions implements ReplayerRecoveryActions {
        long clockMs = CLOCK_MS;
        int requestsSent;

        @Override
        public void sendReplayRequest(final long requestId, final int segmentIndex, final long fromPosition) {
            ++requestsSent;
        }

        @Override
        public boolean sendReplayComplete() {
            return false;
        }

        @Override
        public boolean sendReplayHeartbeat() {
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

    @BeforeEach
    void setUp() {
        dispatched.clear();
        caughtUpAt.clear();
        actions = new RecordingActions();
        receiver = new ReplayerRecovery(CLIENT_ID, actions, event -> dispatched.add(event.globalSeqNo()), null,
                                        () -> caughtUpAt.add((long)dispatched.size()));
        receiver.start();
    }

    @Test
    @DisplayName("cold start requests the chain walk from segment 0")
    void coldStartWalksFromSegmentZero() {
        assertTrue(receiver.isAwaitingReplay());
        assertEquals(0, receiver.walkSegmentIndex());
        assertEquals(0, receiver.requestFromPosition());
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

    @Test
    @DisplayName("a first frame that is not globalSeqNo 1 is fatal")
    void firstFrameMustBeOne() {
        attachReplay(11, 4096);

        assertThrows(IllegalStateException.class, () -> deliverReplay(7));
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

    @Test
    @DisplayName("a reply carrying a superseded requestId is ignored")
    void staleReplyIsIgnored() {
        final long staleRequestId = receiver.requestId();
        receiver.onControl(replayingBuffer(staleRequestId - 1, 11, 4096, 7), 0, replayingLength());

        assertTrue(receiver.isAwaitingReplay(), "a stale reply must not stop the resend timer");
        assertEquals(-1, receiver.replaySessionId());
        assertNotEquals(11, receiver.replaySessionId());
    }

    @Test
    @DisplayName("an empty segment is skipped, not taken for the end of the chain")
    void emptySegmentAdvancesTheWalk() {
        // NO_REPLAY_NEEDED naming a recording means "this segment is empty", not "the chain is exhausted".
        answerReplaying(NO_REPLAY_NEEDED, 0, 7);

        assertEquals(1, receiver.walkSegmentIndex(), "keep walking rather than truncating the chain");
        assertTrue(receiver.isAwaitingReplay());
        assertTrue(caughtUpAt.isEmpty());
    }

    @Test
    @DisplayName("the chain running out marks the client caught up")
    void chainExhaustedMarksCaughtUp() {
        answerReplaying(NO_REPLAY_NEEDED, 0, CHAIN_EXHAUSTED);

        assertTrue(receiver.isCaughtUp());
        assertEquals(-1, receiver.walkSegmentIndex(), "steady/resume mode from here");
        assertFalse(receiver.isAwaitingReplay());
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

    // ── helpers ───────────────────────────────────────────────────────────────────────────────────

    /** Drives the receiver to the caught-up, steady state a live consumer runs in. */
    private void goLive() {
        answerReplaying(NO_REPLAY_NEEDED, 0, CHAIN_EXHAUSTED);
    }

    private void attachReplay(final long replaySessionId, final long catchUpPosition) {
        answerReplaying(replaySessionId, catchUpPosition, 7);
    }

    private void answerReplaying(final long replaySessionId, final long catchUpPosition, final long recordingId) {
        receiver.onControl(replayingBuffer(receiver.requestId(), replaySessionId, catchUpPosition, recordingId), 0,
                           replayingLength());
    }

    /** The replay image reaching the bound the Replayer gave it — how a segment completes. */
    private void completeSegment() {
        receiver.onReplayPosition(receiver.catchUpPosition());
    }

    private void deliverReplay(final long globalSeqNo) {
        receiver.onFrame(tickFrame(globalSeqNo), 0, tickLength(), globalSeqNo * 1024, RECEIVE_NS, true);
    }

    private void deliverTap(final long globalSeqNo) {
        deliverTapAt(globalSeqNo, globalSeqNo * 1024);
    }

    private void deliverTapAt(final long globalSeqNo, final long position) {
        receiver.onFrame(tickFrame(globalSeqNo), 0, tickLength(), position, RECEIVE_NS, false);
    }

    private static UnsafeBuffer tickFrame(final long globalSeqNo) {
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[256]);
        final TickEncoder tick = new TickEncoder();
        tick.wrapAndApplyHeader(buffer, 0, new org.limitless.phixeron.sbe.sequenced.MessageHeaderEncoder());
        tick.header().sourceId(1).connectionId(0).sessionId(0).globalSeqNo(globalSeqNo).timestamp(globalSeqNo * 1000)
            .origin(Origin.Application);
        return buffer;
    }

    private static int tickLength() {
        return org.limitless.phixeron.sbe.sequenced.MessageHeaderEncoder.ENCODED_LENGTH + TickEncoder.BLOCK_LENGTH;
    }

    private static UnsafeBuffer replayingBuffer(final long requestId, final long replaySessionId,
                                                final long catchUpPosition, final long recordingId) {
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[256]);
        new ReplayingEncoder()
            .wrapAndApplyHeader(buffer, 0, new org.limitless.phixeron.sbe.unsequenced.MessageHeaderEncoder())
            .clientId(CLIENT_ID)
            .requestId(requestId)
            .replaySessionId(replaySessionId)
            .catchUpPosition(catchUpPosition)
            .recordingId(recordingId);
        return buffer;
    }

    private static int replayingLength() {
        return org.limitless.phixeron.sbe.unsequenced.MessageHeaderEncoder.ENCODED_LENGTH
            + ReplayingEncoder.BLOCK_LENGTH;
    }
}
