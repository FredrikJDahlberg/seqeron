package org.limitless.seqeron.replayer.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.limitless.seqeron.protocol.ReplayProtocol;
import org.limitless.seqeron.protocol.SnapshotHeader;
import org.limitless.seqeron.protocol.SystemFrame;
import org.limitless.seqeron.sbe.frame.ClusterHeartbeatEncoder;
import org.limitless.seqeron.sbe.frame.MessageHeaderEncoder;
import org.limitless.seqeron.sbe.replay.ReplayPendingEncoder;
import org.limitless.seqeron.sbe.replay.ReplayUnavailableEncoder;
import org.limitless.seqeron.sbe.replay.ReplayingEncoder;
import org.limitless.seqeron.helpers.SplitMix64;
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
 * minutes, all of one kind — one dropped frame on a live tap. A run here is a few hundred, mixing drops
 * with lost requests, refusals, truncated images, stalled replays and recording rotations, in under a
 * second.
 *
 * <p><b>Restore</b> — a client restoring its source's snapshot (doc/snapshot.md §7) holds both properties from
 * the cut on, under the same faults, and its restored state plus the frames after the cut equals the state a full
 * replay builds. The log holds a superseded round, a late chunk of it, and another source's chunks among the
 * snapshot's; which snapshot the Replayer names is {@code SnapshotIndexTest}'s, and serializing it is {@code
 * SnapshotTakerTest}'s.
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

    /** The restored source, and the snapshot's frames: round 2, superseding round 1, cut at {@code CUT}. */
    private static final int SOURCE = 3;
    private static final int OTHER_SOURCE = 5;
    private static final long CUT = 9;
    private static final long SNAPSHOT_END = 17;
    private static final int FORMAT_VERSION = 1;

    @ParameterizedTest(name = "seed {0}")
    @ValueSource(longs = {1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610, 987, 1597})
    void staysGapFreeAndConvergesUnderRandomFaults(final long seed) {
        new Run(seed, false).execute();
    }

    @ParameterizedTest(name = "seed {0}")
    @ValueSource(longs = {1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610, 987, 1597})
    void restoresAndConvergesToTheFullReplayStateUnderRandomFaults(final long seed) {
        new Run(seed, true).execute();
    }

    /**
     * One seeded run: the node's archive, its tap and its Replayer, driving one {@link ReplayerRecovery}.
     *
     * <p>Implements {@link ReplayerRecoveryActions} itself — the client's every outbound act is a request
     * arriving at this Replayer, so recording them and serving them are the same object.
     */
    private static final class Run implements ReplayerRecoveryActions, SnapshotRestoreHandler {
        private final SplitMix64 rng;
        private final String tag;
        private final boolean restoring;

        /** Every frame that is not a heartbeat, by globalSeqNo: the snapshot rounds. */
        private final Map<Long, RestoreFrames.Frame> frames = new HashMap<>();

        /** The state: how many heartbeats, and the sum of their globalSeqNos — restored, then folded. */
        private long heartbeats;
        private long heartbeatSum;
        private int nextRecordIndex;

        // ── the node's archive: the recording chain, complete by construction ──────────────────────────
        private final List<Segment> segments = new ArrayList<>();
        private long nextRecordingId;
        private long tip;

        // ── the Replayer's view of this one client ─────────────────────────────────────────────────────
        private long pendingRequestId = -1;
        private boolean pendingIsQuery;
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

        Run(final long seed, final boolean restoring) {
            this.rng = new SplitMix64(seed);
            this.tag = "seed=" + seed + (restoring ? " restoring" : "");
            this.restoring = restoring;
        }

        void execute() {
            // History before the client starts. A cold start over an EMPTY archive that then drops the very
            // first tap frame is the one designed abort — the first frame observed must be globalSeqNo 1 —
            // and not a recovery failure, so the model does not construct it.
            segments.add(new Segment(nextRecordingId++, 1));
            if (restoring) {
                publishSnapshotRounds();
            }
            for (int i = 0; i < 3; ++i) {
                publish();
            }

            client = new ReplayerRecovery(CLIENT_ID, this, this::onSequenced, null, null);
            if (restoring) {
                client.restoreFrom(SOURCE, this);
                expectedNext = CUT + 1;
            }
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
            if (restoring) {
                long count = 0;
                long sum = 0;
                for (long globalSeqNo = 1; globalSeqNo <= tip; ++globalSeqNo) {
                    if (!frames.containsKey(globalSeqNo)) {
                        ++count;
                        sum += globalSeqNo;
                    }
                }
                assertEquals(count, heartbeats, tag + ": restored state plus the tail is not the full replay's");
                assertEquals(sum, heartbeatSum, tag + ": restored state plus the tail is not the full replay's");
            }
        }

        /**
         * Round 1 starts and is superseded by round 2 at {@link #CUT} before its source finishes it; round 2's
         * chunks then interleave with heartbeats, round 1's late chunk and another source's chunk, up to its end.
         * Its records hold the state at the cut: the heartbeats before it.
         */
        private void publishSnapshotRounds() {
            for (int i = 0; i < 5; ++i) {
                publish(); // heartbeats 1-5
            }
            final byte[] header = RestoreFrames.header(4, 2);
            publishFrame(RestoreFrames.started(6, 1));
            publishFrame(RestoreFrames.chunk(7, SOURCE, 1, 0, header));
            publish(); // heartbeat 8
            publishFrame(RestoreFrames.started(CUT, 2));
            final byte[] count = RestoreFrames.record(6);
            final byte[] sum = RestoreFrames.record(1 + 2 + 3 + 4 + 5 + 8);
            publishFrame(RestoreFrames.chunk(10, SOURCE, 1, 1, RestoreFrames.record(99)));
            publishFrame(RestoreFrames.chunk(11, SOURCE, 2, 0, header));
            publishFrame(RestoreFrames.chunk(12, OTHER_SOURCE, 2, 0, header));
            publish(); // heartbeat 13
            publishFrame(RestoreFrames.chunk(14, SOURCE, 2, 1, count));
            publishFrame(RestoreFrames.chunk(15, SOURCE, 2, 2, sum));
            publish(); // heartbeat 16
            publishFrame(RestoreFrames.end(SNAPSHOT_END, SOURCE, 2, FORMAT_VERSION, header, count, sum));
        }

        private void publishFrame(final RestoreFrames.Frame frame) {
            frames.put(publish(), frame);
        }

        private boolean converged() {
            return client.isCaughtUp() && client.lastGlobalSeqNo() == tip;
        }

        private void chaosStep() {
            switch (rng.roll(10)) {
                case 0, 1, 2, 3 -> {
                    final long globalSeqNo = publish();
                    // A tap drop is the fault the whole resume path exists for, so it is the common one.
                    if (!chance(20)) {
                        deliverTap(globalSeqNo);
                    }
                }
                case 4, 5 -> serveRequest();
                case 6, 7 -> deliverReplayFrames(1 + rng.roll(3));
                case 8 -> tick();
                default -> injectFault();
            }
        }

        private void injectFault() {
            switch (rng.roll(4)) {
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
                        deliver(lastTapped, false);
                    }
                }
                default -> rotate();
            }
        }

        /**
         * A new active recording. A restoring run's starts at globalSeqNo 1 and already holds the log, as a
         * restarted node's does once it has replayed it; a walk's only chain is the older model's.
         */
        private void rotate() {
            final Segment segment = new Segment(nextRecordingId++, restoring ? 1 : tip + 1);
            if (restoring) {
                segment.last = tip;
            }
            segments.add(segment);
        }

        // ── the tap ────────────────────────────────────────────────────────────────────────────────────

        private long publish() {
            ++tip;
            segments.get(segments.size() - 1).last = tip;
            return tip;
        }

        private void deliverTap(final long globalSeqNo) {
            lastTapped = globalSeqNo;
            deliver(globalSeqNo, false);
        }

        private void deliver(final long globalSeqNo, final boolean fromReplay) {
            final RestoreFrames.Frame special = frames.get(globalSeqNo);
            if (special != null) {
                client.onFrame(special.buffer(), 0, special.length(), positionOf(globalSeqNo), RECEIVE_NS, fromReplay);
            } else {
                client.onFrame(frame(globalSeqNo), 0, FRAME_LENGTH, positionOf(globalSeqNo), RECEIVE_NS, fromReplay);
            }
        }

        // ── the Replayer ───────────────────────────────────────────────────────────────────────────────

        private void serveRequest() {
            if (pendingRequestId < 0) {
                return;
            }
            final long requestId = pendingRequestId;
            final boolean query = pendingIsQuery;
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
            if (query) {
                final RestoreFrames.Frame location = RestoreFrames.location(
                    CLIENT_ID, requestId, 2, CUT, positionOf(CUT), positionOf(SNAPSHOT_END + 1), FORMAT_VERSION);
                client.onControl(location.buffer(), 0, location.length());
                return;
            }

            if (segmentIndex < 0) { // a resume, anchored on a position rather than a segment
                final Segment active = segments.get(segments.size() - 1);
                final long anchor = fromPosition / STRIDE + 1;
                if (anchor < active.first || anchor > active.last) {
                    // The position no longer sits in the active recording — it rotated under the client.
                    control(replaying(requestId, ReplayProtocol.NO_REPLAY_NEEDED, 0, CHAIN_EXHAUSTED), REPLAYING_LENGTH);
                    return;
                }
                serveReplay(requestId, anchor, active.last, active.recordingId);
                return;
            }
            if (segmentIndex >= segments.size()) {
                control(replaying(requestId, ReplayProtocol.NO_REPLAY_NEEDED, 0, CHAIN_EXHAUSTED), REPLAYING_LENGTH);
                return;
            }
            final Segment segment = segments.get(segmentIndex);
            if (segment.last < segment.first) {
                // Empty, not exhausted: the recording is named, which is what tells the two apart.
                control(replaying(requestId, ReplayProtocol.NO_REPLAY_NEEDED, 0, segment.recordingId), REPLAYING_LENGTH);
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
                deliver(replayCursor++, true);
            }
            if (replayEndSeqNo >= 0) {
                client.onReplayPosition(positionOf(replayCursor));
            }
        }

        private void control(final UnsafeBuffer buffer, final int length) {
            client.onControl(buffer, 0, length);
        }

        private void tick() {
            clockMs += 100 + rng.roll(900); // straddles RESEND_INTERVAL_MS
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
            pendingIsQuery = false;
            pendingSegmentIndex = segmentIndex;
            pendingFromPosition = fromPosition;
        }

        @Override
        public void sendSnapshotQuery(final long requestId, final int sourceId) {
            assertEquals(SOURCE, sourceId, tag);
            if (chaos && chance(15)) {
                return;
            }
            pendingRequestId = requestId;
            pendingIsQuery = true;
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
            if (!frames.containsKey(event.globalSeqNo())) {
                ++heartbeats;
                heartbeatSum += event.globalSeqNo();
            }
        }

        // ── SnapshotRestoreHandler: the restored state ─────────────────────────────────────────────────

        @Override
        public boolean supportsFormatVersion(final long formatVersion) {
            return formatVersion == FORMAT_VERSION;
        }

        @Override
        public void onSnapshotHeader(final SnapshotHeader header) {
            assertTrue(expectedNext == CUT + 1, tag + ": a restore after a frame was dispatched");
            heartbeats = 0;
            heartbeatSum = 0;
            nextRecordIndex = 0;
        }

        @Override
        public void onSnapshotRecord(final DirectBuffer record, final int length, final int recordIndex) {
            assertEquals(nextRecordIndex++, recordIndex, tag + ": records out of order");
            if (recordIndex == 0) {
                heartbeats = record.getLong(0);
            } else {
                heartbeatSum = record.getLong(0);
            }
        }

        private boolean chance(final int percent) {
            return rng.chance(percent);
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
