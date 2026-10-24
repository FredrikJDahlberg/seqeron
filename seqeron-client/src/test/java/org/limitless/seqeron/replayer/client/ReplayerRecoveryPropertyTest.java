package org.limitless.seqeron.replayer.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.limitless.seqeron.protocol.ReplayProtocol;
import org.limitless.seqeron.protocol.SnapshotHeader;
import org.limitless.seqeron.protocol.SystemFrame;
import org.limitless.seqeron.sbe.frame.ClusterHeartbeatEncoder;
import org.limitless.seqeron.sbe.frame.MessageHeaderEncoder;
import org.limitless.seqeron.sbe.replay.ReplayPendingEncoder;
import org.limitless.seqeron.sbe.replay.ReplayingEncoder;
import org.limitless.seqeron.helpers.SplitMix64;
import org.limitless.seqeron.util.Logger;

/**
 * {@link ReplayerRecovery} against a MODEL of the counterparty it talks to, rather than against scripted
 * stimuli: {@link ReplayerRecoveryTest} names one situation per case and asserts the decision taken in it,
 * which is what pins the protocol down, but it can only reach the interleavings someone thought to write.
 * Here the node's archive, its queue and its Replayer are simulated, a seeded generator drives faults through
 * them, and the two properties recovery exists to provide are asserted over whatever comes out.
 *
 * <p><b>Safety</b> — every dispatched frame is the queue's next, by groupSeqNo, always. No gap, no duplicate, no
 * reordering, at any point in any run. The queue is one group's, so its clusterSeqNos jump ({@link
 * #clusterSeqNoOf}), and the client must track it by groupSeqNo. This is the invariant the whole walk/resume/retain
 * machinery is for, and it is checked on every dispatch rather than at the end, so a violation names the frame that
 * broke it.
 *
 * <p><b>Liveness</b> — once the faults stop, recovery converges: caught up, at the tip, having dispatched
 * every frame ever published. A state machine can hold the safety property by dispatching nothing, so
 * without this one the suite would pass on a client that gave up.
 *
 * <p>The e2e harnesses reach these paths too, but a chaos run produces single-digit gap episodes in
 * minutes, all of one kind — one dropped frame on a live queue. A run here is a few hundred, mixing drops
 * with lost requests, held requests, truncated images, stalled replays and redeliveries, in under a
 * second.
 *
 * <p><b>Restore</b> — a client restoring its own snapshot (doc/snapshot.md §7) holds both properties from the cut
 * on, under the same faults, and its restored state plus the frames after the cut equals the state a full replay
 * builds. It holds files of two rounds: the newer was cut from another log, whose cut this recording does not hold
 * where the file says, so it gives way to the older. The log holds a superseded round and another source's end among
 * the snapshot's; confirming a file is {@code SnapshotTakerTest}'s.
 *
 * <p>Seeds are fixed and listed, not drawn from the clock: a failing run must be re-runnable, and a suite
 * that fails on a different case each time is not a regression signal. Add seeds to widen the search; the
 * failing one is in the test name.
 */
class ReplayerRecoveryPropertyTest {
    private static final long CLIENT_ID = 4;

    /** One frame's span in the recording's position space. Opaque to the client — only ordering matters. */
    private static final int STRIDE = 64;

    /** Stands in for the wall clock. The absolute value is arbitrary; only applied deltas matter. */
    private static final long CLOCK_MS = 3 * 60 * 60 * 1000L;

    /** Arrival stamp; carried through to SequencedEvent, asserted on by no test here. */
    private static final long RECEIVE_NS = 0;

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

    /** The restored source, and its snapshot: round 2, superseding round 1, cut at {@code CUT}. */
    private static final int SOURCE = 3;
    private static final int OTHER_SOURCE = 5;
    private static final long CUT = 9;
    private static final long SNAPSHOT_END = 12;
    private static final long NEXT_CUT = 13;
    private static final int FORMAT_VERSION = 1;

    /**
     * The clusterSeqNo of the queue's frame {@code groupSeqNo}: other groups' frames fall between this queue's, so
     * it increases along the queue by two and four in turn. A function of the frame, so it costs no draw and every
     * seed makes the same faults as before, and as in the other languages.
     */
    static long clusterSeqNoOf(final long groupSeqNo) {
        return 3 * groupSeqNo - groupSeqNo % 2;
    }

    @TempDir
    Path directory;

    @ParameterizedTest(name = "seed {0}")
    @ValueSource(longs = {1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610, 987, 1597})
    void staysGapFreeAndConvergesUnderRandomFaults(final long seed) {
        new Run(seed, null).execute();
    }

    @ParameterizedTest(name = "seed {0}")
    @ValueSource(longs = {1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610, 987, 1597})
    void restoresAndConvergesToTheFullReplayStateUnderRandomFaults(final long seed) {
        new Run(seed, new SnapshotStore(directory)).execute();
    }

    /**
     * One seeded run: the node's archive, its queue and its Replayer, driving one {@link ReplayerRecovery}.
     *
     * <p>Implements {@link ReplayerRecoveryActions} itself — the client's every outbound act is a request
     * arriving at this Replayer, so recording them and serving them are the same object.
     */
    private static final class Run implements ReplayerRecoveryActions, SnapshotRestoreHandler {
        private final SplitMix64 rng;
        private final String tag;
        private final boolean restoring;

        /** The client's own files, null for a run that restores nothing. */
        private final SnapshotStore store;

        /** Every frame that is not a heartbeat, by groupSeqNo: the snapshot rounds. */
        private final Map<Long, RestoreFrames.Frame> frames = new HashMap<>();

        /** The state: how many heartbeats, and the sum of their clusterSeqNos — restored, then folded. */
        private long heartbeats;
        private long heartbeatSum;
        private int nextRecordIndex;

        // ── the node's archive: the active recording, complete by construction ─────────────────────────
        private long tip;

        // ── the Replayer's view of this one client ─────────────────────────────────────────────────────
        private long pendingRequestId = -1;
        private long pendingFromPosition;
        private long nextSessionId = 1;

        /** Next groupSeqNo the open replay will deliver; {@code replayEndSeqNo < 0} means none is open. */
        private long replayCursor;
        private long replayEndSeqNo = -1;

        /** Last groupSeqNo the queue actually delivered — what a redelivery re-offers. */
        private long lastLive;

        private long clockMs = CLOCK_MS;
        private boolean chaos = true;

        /** The safety property, one frame at a time. */
        private long expectedNext = 1;

        private ReplayerRecovery client;

        Run(final long seed, final SnapshotStore store) {
            this.rng = new SplitMix64(seed);
            this.restoring = store != null;
            this.tag = "seed=" + seed + (restoring ? " restoring" : "");
            this.store = store;
        }

        void execute() {
            // History before the client starts. A cold start over an EMPTY archive that then drops the very
            // first queue frame is the one designed abort — the first frame observed must be groupSeqNo 1 —
            // and not a recovery failure, so the model does not construct it.
            if (restoring) {
                publishSnapshotRounds();
            }
            for (int i = 0; i < 3; ++i) {
                publish();
            }

            client = new ReplayerRecovery(this, this::onSequenced, null, null);
            if (restoring) {
                client.restoreFrom(store, this);
                expectedNext = CUT + 1;
            }
            client.start();

            for (int step = 0; step < CHAOS_STEPS; ++step) {
                chaosStep();
            }

            // Quiescence: faults off, but the queue keeps running. A hole the chaos phase left open is only
            // ever discovered by the NEXT queue frame, so convergence has to be given one.
            chaos = false;
            for (int i = 0; i < 3; ++i) {
                deliverQueue(publish());
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

            assertNull(client.fault(), tag);
            assertTrue(client.isCaughtUp(), tag + ": never re-converged after the faults stopped");
            assertEquals(clusterSeqNoOf(tip), client.lastClusterSeqNo(), tag + ": converged short of the tip");
            assertEquals(tip + 1, expectedNext, tag + ": caught up without having dispatched every frame");
            if (restoring) {
                long count = 0;
                long sum = 0;
                for (long groupSeqNo = 1; groupSeqNo <= tip; ++groupSeqNo) {
                    if (!frames.containsKey(groupSeqNo)) {
                        ++count;
                        sum += clusterSeqNoOf(groupSeqNo);
                    }
                }
                assertEquals(count, heartbeats, tag + ": restored state plus the tail is not the full replay's");
                assertEquals(sum, heartbeatSum, tag + ": restored state plus the tail is not the full replay's");
            }
        }

        /**
         * Round 1 starts and is superseded by round 2 at {@link #CUT} before its source ends it; round 2 ends among
         * heartbeats and another source's end, and round 3 starts and never ends. The client holds round 2's file,
         * whose records hold the state at the cut, the heartbeats before it, and a round 3 file from another log,
         * whose cut is a frame earlier there.
         */
        private void publishSnapshotRounds() {
            for (int i = 0; i < 5; ++i) {
                publish(); // heartbeats 1-5
            }
            final byte[] header = RestoreFrames.header(4, 2);
            publishFrame(started(6, 1));
            publish(); // heartbeat 7
            publish(); // heartbeat 8
            publishFrame(started(CUT, 2));
            final byte[] count = RestoreFrames.record(7);
            final byte[] sum =
                RestoreFrames.record(clusterSeqNoOf(1) + clusterSeqNoOf(2) + clusterSeqNoOf(3) + clusterSeqNoOf(4) +
                                     clusterSeqNoOf(5) + clusterSeqNoOf(7) + clusterSeqNoOf(8));
            publishFrame(RestoreFrames.end(clusterSeqNoOf(10), 10, OTHER_SOURCE, 2, FORMAT_VERSION, header));
            publish(); // heartbeat 11
            publishFrame(RestoreFrames.end(clusterSeqNoOf(SNAPSHOT_END), SNAPSHOT_END, SOURCE, 2, FORMAT_VERSION,
                                           header, count, sum));
            publishFrame(started(NEXT_CUT, 3));
            publish(); // heartbeat 14

            RestoreFrames.write(store, 3, clusterSeqNoOf(NEXT_CUT), positionOf(NEXT_CUT - 1), FORMAT_VERSION, header,
                                RestoreFrames.record(98));
            RestoreFrames.write(store, 2, clusterSeqNoOf(CUT), positionOf(CUT), FORMAT_VERSION, header, count, sum);
        }

        /** The round's {@code SnapshotStarted} as the queue's frame {@code groupSeqNo}. */
        private static RestoreFrames.Frame started(final long groupSeqNo, final long round) {
            return RestoreFrames.started(clusterSeqNoOf(groupSeqNo), groupSeqNo, round,
                                         clusterSeqNoOf(groupSeqNo) * 1000);
        }

        private void publishFrame(final RestoreFrames.Frame frame) {
            frames.put(publish(), frame);
        }

        private boolean converged() {
            return client.isCaughtUp() && client.lastClusterSeqNo() == clusterSeqNoOf(tip);
        }

        private void chaosStep() {
            switch (rng.roll(10)) {
                case 0, 1, 2, 3 -> {
                    final long groupSeqNo = publish();
                    // A queue drop is the fault the whole resume path exists for, so it is the common one.
                    if (!chance(20)) {
                        deliverQueue(groupSeqNo);
                    }
                }
                case 4, 5 -> serveRequest();
                case 6, 7 -> deliverReplayFrames(1 + rng.roll(3));
                case 8 -> tick();
                default -> injectFault();
            }
        }

        private void injectFault() {
            switch (rng.roll(3)) {
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
                default -> {
                    // The same queue frame offered twice. Both de-dupes have to hold: the contiguity one when
                    // it sits at or below the baseline, and the retained FIFO's when it is ahead of a hole.
                    if (lastLive > 0) {
                        deliver(lastLive, false);
                    }
                }
            }
        }

        // ── the queue ────────────────────────────────────────────────────────────────────────────────────

        private long publish() {
            return ++tip;
        }

        private void deliverQueue(final long groupSeqNo) {
            lastLive = groupSeqNo;
            deliver(groupSeqNo, false);
        }

        private void deliver(final long groupSeqNo, final boolean fromReplay) {
            final RestoreFrames.Frame special = frames.get(groupSeqNo);
            if (special != null) {
                client.onFrame(special.buffer(), 0, special.length(), positionOf(groupSeqNo), RECEIVE_NS, fromReplay);
            } else {
                client.onFrame(frame(groupSeqNo), 0, FRAME_LENGTH, positionOf(groupSeqNo), RECEIVE_NS, fromReplay);
            }
        }

        // ── the Replayer ───────────────────────────────────────────────────────────────────────────────

        private void serveRequest() {
            if (pendingRequestId < 0) {
                return;
            }
            final long requestId = pendingRequestId;
            final long fromPosition = pendingFromPosition;
            pendingRequestId = -1;
            if (requestId != client.requestId()) {
                return; // superseded by a request this run dropped — the Replayer would serve the newer one
            }

            if (chaos && chance(10)) {
                control(replayPending(requestId), REPLAY_PENDING_LENGTH);
                return;
            }
            if (fromPosition == ReplayProtocol.FROM_START) {
                serveReplay(requestId, 1, tip);
                return;
            }
            assertTrue(fromPosition % STRIDE == 0 && fromPosition / STRIDE + 1 <= tip,
                       tag + ": resumed at " + fromPosition + ", where no recorded frame starts");
            serveReplay(requestId, fromPosition / STRIDE + 1, tip);
        }

        /**
         * Bound at the tip the recording holds NOW — frames published later are the queue's problem, not this
         *  replay's, which is exactly how a bounded replay of an active recording behaves.
         */
        private void serveReplay(final long requestId, final long fromSeqNo, final long toSeqNo) {
            replayCursor = fromSeqNo;
            replayEndSeqNo = toSeqNo;
            control(replaying(requestId, nextSessionId++, positionOf(toSeqNo + 1)), REPLAYING_LENGTH);
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
        public void sendReplayRequest(final long requestId, final long fromPosition, final long cutClusterSeqNo) {
            if (chaos && chance(15)) {
                return; // the offer did not land; only the resend timer recovers this
            }
            pendingRequestId = requestId;
            pendingFromPosition = fromPosition;
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
        public long clientId() {
            return CLIENT_ID;
        }

        @Override
        public long nowMs() {
            return clockMs;
        }

        // ── the safety property ────────────────────────────────────────────────────────────────────────

        private void onSequenced(final SequencedEvent event) {
            assertEquals(expectedNext, event.groupSeqNo(), tag + ": dispatched out of order");
            assertEquals(clusterSeqNoOf(expectedNext), event.clusterSeqNo(), tag + ": dispatched another frame");
            assertTrue(event.groupSeqNo() <= tip, tag + ": dispatched a frame that was never published");
            ++expectedNext;
            if (!frames.containsKey(event.groupSeqNo())) {
                ++heartbeats;
                heartbeatSum += event.clusterSeqNo();
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
        public void onSnapshotRecord(final DirectBuffer record, final int length, final int recordIndex,
                                     final long formatVersion) {
            assertEquals(FORMAT_VERSION, formatVersion, tag + ": another format's records");
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

        /** Where frame {@code groupSeqNo} starts in the active recording: frame 1 at 0. */
        private long positionOf(final long groupSeqNo) {
            return (groupSeqNo - 1) * STRIDE;
        }
    }

    // ── encoders: the same frames ReplayerRecoveryTest uses, kept identical to it ──────────────────────

    private static final int FRAME_LENGTH =
        MessageHeaderEncoder.ENCODED_LENGTH + ClusterHeartbeatEncoder.BLOCK_LENGTH;

    private static final int CONTROL_HEADER_LENGTH =
        org.limitless.seqeron.sbe.replay.MessageHeaderEncoder.ENCODED_LENGTH;
    private static final int REPLAYING_LENGTH = CONTROL_HEADER_LENGTH + ReplayingEncoder.BLOCK_LENGTH;
    private static final int REPLAY_PENDING_LENGTH = CONTROL_HEADER_LENGTH + ReplayPendingEncoder.BLOCK_LENGTH;

    private static UnsafeBuffer frame(final long groupSeqNo) {
        final long clusterSeqNo = clusterSeqNoOf(groupSeqNo);
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[256]);
        final ClusterHeartbeatEncoder encoder = new ClusterHeartbeatEncoder();
        encoder.wrapAndApplyHeader(buffer, 0, new MessageHeaderEncoder());
        encoder.header()
            .sourceId(-1)
            .connectionId(-1)
            .sessionId(-1)
            .systemEventType(SystemFrame.CLUSTER_HEARTBEAT)
            .clusterSeqNo(clusterSeqNo)
            .timestamp(clusterSeqNo * 1000)
            .groupSeqNo(groupSeqNo);
        return buffer;
    }

    private static UnsafeBuffer replaying(final long requestId, final long replaySessionId,
                                          final long catchUpPosition) {
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[256]);
        new ReplayingEncoder()
            .wrapAndApplyHeader(buffer, 0, new org.limitless.seqeron.sbe.replay.MessageHeaderEncoder())
            .clientId(CLIENT_ID)
            .requestId(requestId)
            .replaySessionId(replaySessionId)
            .catchUpPosition(catchUpPosition);
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
}
