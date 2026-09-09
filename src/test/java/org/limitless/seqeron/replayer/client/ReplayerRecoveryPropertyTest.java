package org.limitless.seqeron.replayer.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.limitless.seqeron.replayer.server.ReplayerService;
import org.limitless.seqeron.sbe.frame.ClusterHeartbeatEncoder;
import org.limitless.seqeron.sbe.frame.MessageHeaderEncoder;
import org.limitless.seqeron.sbe.replay.ReplayPendingEncoder;
import org.limitless.seqeron.sbe.replay.ReplayUnavailableEncoder;
import org.limitless.seqeron.sbe.replay.ReplayingEncoder;
import org.limitless.seqeron.sequencer.SystemFrame;
import org.limitless.seqeron.util.Logger;

/**
 * {@link ReplayerRecovery} against a MODEL of the counterparty it talks to, rather than against scripted
 * stimuli: {@link ReplayerRecoveryTest} names one situation per case and asserts the decision taken in it,
 * which is what pins the protocol down, but it can only reach the interleavings someone thought to write.
 * Here the node's archive, its tap and its Replayer are simulated, a seeded generator drives faults through
 * them, and the two properties recovery exists to provide are asserted over whatever comes out.
 *
 * <p><b>Safety</b> — every dispatched frame is the next globalSeqNo, always. No gap, no duplicate, no
 * reordering, at any point in any run. This is the invariant the whole walk/resume/retain machinery is for,
 * and it is checked on every dispatch rather than at the end, so a violation names the frame that broke it.
 *
 * <p><b>Liveness</b> — once the faults stop, recovery converges: caught up, at the tip, having dispatched
 * every frame ever published. A state machine can hold the safety property by dispatching nothing, so
 * without this one the suite would pass on a client that gave up.
 *
 * <p>The e2e harnesses reach these paths too, but a chaos run produces single-digit gap episodes in
 * minutes, all of one shape — one dropped frame on a live tap. A run here is a few hundred, mixing drops
 * with lost requests, refusals, truncated images, stalled replays and recording rotations, in under a
 * second.
 *
 * <p>Seeds are fixed and listed, not drawn from the clock: a failing run must be re-runnable, and a suite
 * that fails on a different case each time is not a regression signal. Add seeds to widen the search; the
 * failing one is in the test name.
 */
class ReplayerRecoveryPropertyTest {
    private static final int CLIENT_ID = 4;

    /** One frame's span in the recording's position space. Opaque to the client — only ordering matters. */
    private static final int STRIDE = 64;

    /** Stands in for the wall clock. The absolute value is arbitrary; only applied deltas matter. */
    private static final long CLOCK_MS = 3 * 60 * 60 * 1000L;

    /** Arrival stamp; carried through to SequencedEvent, asserted on by no test here. */
    private static final long RECEIVE_NS = 0;

    /** The walk terminator: nothing left to replay AND no recording named. */
    private static final long CHAIN_EXHAUSTED = -1;

    private static final int CHAOS_STEPS = 600;

    /** Generous: convergence takes a bounded number of steps, and a run that needs them all still passes. */
    private static final int QUIESCE_STEPS = 20_000;

    /** Every gap logs, and a run makes hundreds — kept out of the build output rather than counted. */
    @BeforeEach
    void silenceLogger() {
        Logger.install(event -> { });
    }

    @AfterEach
    void restoreLogger() {
        Logger.reset();
    }

    @ParameterizedTest(name = "seed {0}")
    @ValueSource(longs = {1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610, 987, 1597})
    void staysGapFreeAndConvergesUnderRandomFaults(final long seed) {
        new Run(seed).execute();
    }

    /**
     * One seeded run: the node's archive, its tap and its Replayer, driving one {@link ReplayerRecovery}.
     *
     * <p>Implements {@link ReplayerRecoveryActions} itself — the client's every outbound act is a request
     * arriving at this Replayer, so recording them and serving them are the same object.
     */
    private static final class Run implements ReplayerRecoveryActions {
        private final Random rng;
        private final String tag;

        // ── the node's archive: the recording chain, complete by construction ──────────────────────────
        private final List<Segment> segments = new ArrayList<>();
        private long nextRecordingId;
        private long tip;

        // ── the Replayer's view of this one client ─────────────────────────────────────────────────────
        private long pendingRequestId = -1;
        private int pendingSegmentIndex;
        private long pendingFromPosition;
        private long nextSessionId = 1;

        /** Next globalSeqNo the open replay will deliver; {@code replayEndSeqNo < 0} means none is open. */
        private long replayCursor;
        private long replayEndSeqNo = -1;

        /** Last globalSeqNo the tap actually delivered — what a redelivery re-offers. */
        private long lastTapped;

        private long clockMs = CLOCK_MS;
        private boolean chaos = true;

        /** The safety property, one frame at a time. */
        private long expectedNext = 1;

        private ReplayerRecovery client;

        Run(final long seed) {
            this.rng = new Random(seed);
            this.tag = "seed=" + seed;
        }

        void execute() {
            // History before the client starts. A cold start over an EMPTY archive that then drops the very
            // first tap frame is the one designed abort — the first frame observed must be globalSeqNo 1 —
            // and not a recovery failure, so the model does not construct it.
            segments.add(new Segment(nextRecordingId++, 1));
            for (int i = 0; i < 3; ++i) {
                publish();
            }

            client = new ReplayerRecovery(CLIENT_ID, this, this::onSequenced, null, null);
            client.start();

            for (int step = 0; step < CHAOS_STEPS; ++step) {
                chaosStep();
            }

            // Quiescence: faults off, but the tap keeps running. A hole the chaos phase left open is only
            // ever discovered by the NEXT tap frame, so convergence has to be given one.
            chaos = false;
            for (int i = 0; i < 3; ++i) {
                deliverTap(publish());
            }
            for (int step = 0; step < QUIESCE_STEPS && !converged(); ++step) {
                if (pendingRequestId >= 0) {
                    serveRequest();
                } else if (replayEndSeqNo >= 0) {
                    deliverReplayFrames(4);
                } else {
                    tick();
                }
            }

            assertTrue(client.isCaughtUp(), tag + ": never re-converged after the faults stopped");
            assertEquals(tip, client.lastGlobalSeqNo(), tag + ": converged short of the tip");
            assertEquals(tip + 1, expectedNext, tag + ": caught up without having dispatched every frame");
        }

        private boolean converged() {
            return client.isCaughtUp() && client.lastGlobalSeqNo() == tip;
        }

        private void chaosStep() {
            switch (rng.nextInt(10)) {
                case 0, 1, 2, 3 -> {
                    final long globalSeqNo = publish();
                    // A tap drop is the fault the whole resume path exists for, so it is the common one.
                    if (!chance(20)) {
                        deliverTap(globalSeqNo);
                    }
                }
                case 4, 5 -> serveRequest();
                case 6, 7 -> deliverReplayFrames(1 + rng.nextInt(3));
                case 8 -> tick();
                default -> injectFault();
            }
        }

        private void injectFault() {
            switch (rng.nextInt(4)) {
                case 0 -> {
                    if (replayEndSeqNo >= 0) {
                        // The Replayer stopped this replay under us — its image closes SHORT of the bound.
                        client.onReplayImageClosed(positionOf(replayCursor));
                    }
                }
                case 1 -> {
                    clockMs += 6_000; // past REPLAY_STALL_TIMEOUT_MS as well as the resend interval
                    client.doTimers(false);
                }
                case 2 -> {
                    // The same tap frame offered twice. Both de-dupes have to hold: the contiguity one when
                    // it sits at or below the baseline, and the retained FIFO's when it is ahead of a hole.
                    if (lastTapped > 0) {
                        client.onFrame(frame(lastTapped), 0, FRAME_LENGTH, positionOf(lastTapped), RECEIVE_NS,
                                       false);
                    }
                }
                default -> segments.add(new Segment(nextRecordingId++, tip + 1));
            }
        }

        // ── the tap ────────────────────────────────────────────────────────────────────────────────────

        private long publish() {
            ++tip;
            segments.get(segments.size() - 1).last = tip;
            return tip;
        }

        private void deliverTap(final long globalSeqNo) {
            lastTapped = globalSeqNo;
            client.onFrame(frame(globalSeqNo), 0, FRAME_LENGTH, positionOf(globalSeqNo), RECEIVE_NS, false);
        }

        // ── the Replayer ───────────────────────────────────────────────────────────────────────────────

        private void serveRequest() {
            if (pendingRequestId < 0) {
                return;
            }
            final long requestId = pendingRequestId;
            final int segmentIndex = pendingSegmentIndex;
            final long fromPosition = pendingFromPosition;
            pendingRequestId = -1;
            if (requestId != client.requestId()) {
                return; // superseded by a request this run dropped — the Replayer would serve the newer one
            }

            if (chaos && chance(10)) {
                control(replayPending(requestId), REPLAY_PENDING_LENGTH);
                return;
            }
            if (chaos && chance(5)) {
                control(replayUnavailable(requestId), REPLAY_UNAVAILABLE_LENGTH);
                return;
            }

            if (segmentIndex < 0) { // a resume, anchored on a position rather than a segment
                final Segment active = segments.get(segments.size() - 1);
                final long anchor = fromPosition / STRIDE + 1;
                if (anchor < active.first || anchor > active.last) {
                    // The position no longer sits in the active recording — it rotated under the client.
                    control(replaying(requestId, ReplayerService.NO_REPLAY_NEEDED, 0, CHAIN_EXHAUSTED), REPLAYING_LENGTH);
                    return;
                }
                serveReplay(requestId, anchor, active.last, active.recordingId);
                return;
            }
            if (segmentIndex >= segments.size()) {
                control(replaying(requestId, ReplayerService.NO_REPLAY_NEEDED, 0, CHAIN_EXHAUSTED), REPLAYING_LENGTH);
                return;
            }
            final Segment segment = segments.get(segmentIndex);
            if (segment.last < segment.first) {
                // Empty, not exhausted: the recording is named, which is what tells the two apart.
                control(replaying(requestId, ReplayerService.NO_REPLAY_NEEDED, 0, segment.recordingId), REPLAYING_LENGTH);
                return;
            }
            serveReplay(requestId, segment.first, segment.last, segment.recordingId);
        }

        /** Bound at the tip the recording holds NOW — frames published later are the tap's problem, not this
         *  replay's, which is exactly how a bounded replay of an active recording behaves. */
        private void serveReplay(final long requestId, final long fromSeqNo, final long toSeqNo,
                                 final long recordingId) {
            replayCursor = fromSeqNo;
            replayEndSeqNo = toSeqNo;
            control(replaying(requestId, nextSessionId++, positionOf(toSeqNo + 1), recordingId), REPLAYING_LENGTH);
        }

        /**
         * Feeds up to {@code count} frames off the open replay, then reports where it has reached. Both the
         * frames and the position report can make the client abandon the replay (an anchor mismatch, the
         * bound being reached), which {@link #closeReplay()} records — hence the re-checks.
         */
        private void deliverReplayFrames(final int count) {
            for (int i = 0; i < count && replayEndSeqNo >= 0 && replayCursor <= replayEndSeqNo; ++i) {
                final long globalSeqNo = replayCursor++;
                client.onFrame(frame(globalSeqNo), 0, FRAME_LENGTH, positionOf(globalSeqNo), RECEIVE_NS, true);
            }
            if (replayEndSeqNo >= 0) {
                client.onReplayPosition(positionOf(replayCursor));
            }
        }

        private void control(final UnsafeBuffer buffer, final int length) {
            client.onControl(buffer, 0, length);
        }

        private void tick() {
            clockMs += 100 + rng.nextInt(900); // straddles RESEND_INTERVAL_MS
            client.doTimers(false);
            client.checkRecoveryProgress();
        }

        // ── ReplayerRecoveryActions: the client's outbound side ────────────────────────────────────────

        @Override
        public void sendReplayRequest(final long requestId, final int segmentIndex, final long fromPosition) {
            if (chaos && chance(15)) {
                return; // the offer did not land; only the resend timer recovers this
            }
            pendingRequestId = requestId;
            pendingSegmentIndex = segmentIndex;
            pendingFromPosition = fromPosition;
        }

        @Override
        public boolean sendReplayComplete() {
            return !chaos || chance(70);
        }

        @Override
        public boolean sendReplayHeartbeat() {
            return !chaos || chance(70);
        }

        @Override
        public void openReplay(final long replaySessionId) {
        }

        @Override
        public void closeReplay() {
            replayEndSeqNo = -1;
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

        // ── the safety property ────────────────────────────────────────────────────────────────────────

        private void onSequenced(final SequencedEvent event) {
            assertEquals(expectedNext, event.globalSeqNo(), tag + ": dispatched out of order");
            assertTrue(event.globalSeqNo() <= tip, tag + ": dispatched a frame that was never published");
            ++expectedNext;
        }

        private boolean chance(final int percent) {
            return rng.nextInt(100) < percent;
        }
    }

    /** One recording in the chain. {@code last < first} while it holds nothing yet. */
    private static final class Segment {
        final long recordingId;
        final long first;
        long last;

        Segment(final long recordingId, final long first) {
            this.recordingId = recordingId;
            this.first = first;
            this.last = first - 1;
        }
    }

    /** Where frame {@code globalSeqNo} starts. Frame 1 at 0, so the position after frame n is n*STRIDE. */
    private static long positionOf(final long globalSeqNo) {
        return (globalSeqNo - 1) * STRIDE;
    }

    // ── encoders: the same frames ReplayerRecoveryTest uses, kept identical to it ──────────────────────

    private static final int FRAME_LENGTH =
        MessageHeaderEncoder.ENCODED_LENGTH + ClusterHeartbeatEncoder.BLOCK_LENGTH;

    private static final int CONTROL_HEADER_LENGTH =
        org.limitless.seqeron.sbe.replay.MessageHeaderEncoder.ENCODED_LENGTH;
    private static final int REPLAYING_LENGTH = CONTROL_HEADER_LENGTH + ReplayingEncoder.BLOCK_LENGTH;
    private static final int REPLAY_PENDING_LENGTH = CONTROL_HEADER_LENGTH + ReplayPendingEncoder.BLOCK_LENGTH;
    private static final int REPLAY_UNAVAILABLE_LENGTH =
        CONTROL_HEADER_LENGTH + ReplayUnavailableEncoder.BLOCK_LENGTH;

    private static UnsafeBuffer frame(final long globalSeqNo) {
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[256]);
        final ClusterHeartbeatEncoder encoder = new ClusterHeartbeatEncoder();
        encoder.wrapAndApplyHeader(buffer, 0, new MessageHeaderEncoder());
        encoder.header().sourceId(-1).connectionId(-1).sessionId(-1)
            .systemEventType(SystemFrame.CLUSTER_HEARTBEAT)
            .globalSeqNo(globalSeqNo).timestamp(globalSeqNo * 1000);
        return buffer;
    }

    private static UnsafeBuffer replaying(final long requestId, final long replaySessionId,
                                          final long catchUpPosition, final long recordingId) {
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[256]);
        new ReplayingEncoder()
            .wrapAndApplyHeader(buffer, 0, new org.limitless.seqeron.sbe.replay.MessageHeaderEncoder())
            .clientId(CLIENT_ID)
            .requestId(requestId)
            .replaySessionId(replaySessionId)
            .catchUpPosition(catchUpPosition)
            .recordingId(recordingId);
        return buffer;
    }

    private static UnsafeBuffer replayPending(final long requestId) {
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[256]);
        new ReplayPendingEncoder()
            .wrapAndApplyHeader(buffer, 0, new org.limitless.seqeron.sbe.replay.MessageHeaderEncoder())
            .clientId(CLIENT_ID)
            .requestId(requestId);
        return buffer;
    }

    private static UnsafeBuffer replayUnavailable(final long requestId) {
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[256]);
        new ReplayUnavailableEncoder()
            .wrapAndApplyHeader(buffer, 0, new org.limitless.seqeron.sbe.replay.MessageHeaderEncoder())
            .clientId(CLIENT_ID)
            .requestId(requestId);
        return buffer;
    }
}
