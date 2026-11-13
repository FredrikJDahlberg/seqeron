package org.limitless.seqeron.tools;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.aeron.Aeron;
import io.aeron.ChannelUri;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.agrona.CloseHelper;
import org.agrona.collections.LongArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.limitless.seqeron.protocol.PortLayout;
import org.limitless.seqeron.replayer.server.AeronReplayer;
import org.limitless.seqeron.replayer.server.DerivingReplayer;
import org.limitless.seqeron.replayer.server.Replayer;
import org.limitless.seqeron.replayer.server.RestartSafeLog;

/**
 * A replay derived from the Raft log is the archive's replay of the queue's recording, fragment for fragment, from
 * wherever it starts (doc/disk-load-plan.md, step 2a): from mid-history, which continues from a checkpoint, and on a
 * member that started from a cluster snapshot, whose derivation starts at the snapshot. A {@link DerivingReplayer}
 * of the test's own runs over a member's archive, which the test has record the member's queues, and each derived
 * replay is read beside the recorded one.
 */
@ExtendWith(InterruptingTestCallback.class)
class CheckpointedReplayTest {
    private static final String IPC_TERM_PROPERTY = "aeron.ipc.term.buffer.length";
    private static final String JOIN_PROPERTY = "replayer.snapshotJoinMs";
    private static final int RESPONSE_STREAM_ID = 125; // beside QueueFrames' and NodeDriver's
    private static final String REPLAY_CHANNEL = "aeron:ipc";
    private static final int RECORDED_STREAM_ID = 1207;
    private static final int DERIVED_STREAM_ID = 1209;
    private static final long TIMEOUT_NS = TimeUnit.SECONDS.toNanos(30);
    /** With the frame's envelope, a payload of the size production carries. */
    private static final int FILLER = 150;

    @RegisterExtension final SystemTestWatcher watcher = new SystemTestWatcher();

    @TempDir Path baseDir;

    private InProcessCluster cluster;
    /** The replays compared that started inside a frame. */
    private int insideFrames;
    private String ipcTerm;
    private String joinMs;
    private long checkpointLogBytes = DerivingReplayer.CHECKPOINT_LOG_BYTES;
    private int maxCheckpoints = DerivingReplayer.MAX_CHECKPOINTS;
    private long maxRunBytes = DerivingReplayer.MAX_RUN_BYTES;
    /** The runs the derivation kept, and how many of them run a sequencer. */
    private int runs;
    private int sequencedRuns;
    /** Whether each queue is purged to its middle frame once compared, and replayed from there. */
    private boolean purge;

    /** One fragment of a replay: where it ends, its flags and its bytes. */
    private record Fragment(long position, int flags, byte[] bytes) { }

    @BeforeEach
    void remember() {
        ipcTerm = System.getProperty(IPC_TERM_PROPERTY);
        joinMs = System.getProperty(JOIN_PROPERTY);
        watcher.dataCollector().add(baseDir);
    }

    @AfterEach
    void stopCluster() {
        if (cluster != null) {
            cluster.close();
        }
        restore(IPC_TERM_PROPERTY, ipcTerm);
        restore(JOIN_PROPERTY, joinMs);
    }

    private static void restore(final String property, final String value) {
        if (value == null) {
            System.clearProperty(property);
        } else {
            System.setProperty(property, value);
        }
    }

    @Test
    @InterruptAfter(240)
    void aReplayFromMidHistoryContinuesFromACheckpoint() {
        cluster = new InProcessCluster(baseDir);
        cluster.withQueueRecordings();
        cluster.startAll();
        final int leader = cluster.awaitLeader();
        send(leader, 200_000, FILLER);
        assertDerivedIsRecorded((leader + 1) % InProcessCluster.MEMBERS, 3);
        // One run per checkpoint and a few at the log's start: more, and FramePredictor has left the sequencer's
        // encoding.
        assertTrue(runs < 32,
                   runs + " runs kept: the predictor differs from the sequencer on entries it should derive");
        assertEquals(0, sequencedRuns);
    }

    /** Each producer's session close is an entry the predictor does not derive. */
    @Test
    @InterruptAfter(240)
    void runsPastTheirBoundRunASequencer() {
        checkpointLogBytes = 256 * 1024;
        maxRunBytes = 0;
        cluster = new InProcessCluster(baseDir);
        cluster.withQueueRecordings();
        cluster.startAll();
        final int leader = cluster.awaitLeader();
        for (int i = 0; i < 8; i++) {
            send(leader, 2_500, FILLER);
        }
        assertDerivedIsRecorded((leader + 1) % InProcessCluster.MEMBERS, 3);
        assertTrue(sequencedRuns > 1, sequencedRuns + " of " + runs + " runs run a sequencer");
    }

    /**
     * A log nothing purges keeps a bounded number of checkpoints, further apart, and a purge releases the ones below
     * every queue's start.
     */
    @Test
    @InterruptAfter(240)
    void checkpointsAreThinnedPastTheirBoundAndReleasedBelowAPurge() {
        checkpointLogBytes = 64 * 1024;
        maxCheckpoints = 8;
        purge = true;
        cluster = new InProcessCluster(baseDir);
        cluster.withQueueRecordings();
        cluster.startAll();
        final int leader = cluster.awaitLeader();
        final int member = (leader + 1) % InProcessCluster.MEMBERS;
        send(leader, 20_000, FILLER);
        assertTrue(cluster.commitPosition(member) / checkpointLogBytes > 4 * maxCheckpoints,
                   "too little log to thin the checkpoints twice");
        assertDerivedIsRecorded(member, 3);
    }

    /**
     * A client stopped inside a frame above the MTU resumes at a fragment of it, which the archive replays from. The
     * log's entries are above its MTU too, and a checkpoint is never inside one.
     */
    @Test
    @InterruptAfter(240)
    void aReplayFromInsideAFragmentedFrameStartsWithItsNextFragment() {
        cluster = new InProcessCluster(baseDir);
        cluster.withQueueRecordings();
        cluster.startAll();
        final int leader = cluster.awaitLeader();
        send(leader, 3_000, 8_000);
        assertDerivedIsRecorded((leader + 1) % InProcessCluster.MEMBERS, 3);
        assertTrue(insideFrames > 0, "no replay started inside a frame");
    }

    @Test
    @InterruptAfter(240)
    void aMemberStartedFromASnapshotDerivesFromIt() {
        restartFromASnapshot(null);
    }

    /** Its queues start again at position 0, so only the snapshot, and not the log's first entry, places them. */
    @Test
    @InterruptAfter(240)
    void aMemberStartedFromASnapshotWithAnotherTermLengthDerivesFromIt() {
        restartFromASnapshot(String.valueOf(8 * 1024 * 1024));
    }

    private void restartFromASnapshot(final String termLength) {
        System.setProperty(JOIN_PROPERTY, "200");
        cluster = new InProcessCluster(baseDir);
        cluster.withQueueRecordings();
        cluster.startAll(1, 0);
        final int leader = cluster.awaitLeader();
        final int member = (leader + 1) % InProcessCluster.MEMBERS;
        send(leader, 20_000, FILLER);
        final Path safeLog = baseDir.resolve("cluster-" + member).resolve(RestartSafeLog.FILE_NAME);
        final long deadline = System.nanoTime() + TIMEOUT_NS;
        while (RestartSafeLog.read(safeLog).safe().isEmpty()) {
            assertTrue(System.nanoTime() < deadline, "member " + member + " marked no snapshot restart-safe");
            Tests.sleep(50);
        }
        cluster.stop(member);
        if (termLength != null) {
            System.setProperty(IPC_TERM_PROPERTY, termLength);
        }
        try {
            cluster.start(member);
        } finally {
            restore(IPC_TERM_PROPERTY, ipcTerm);
        }
        send(cluster.awaitLeader(), 20_000, FILLER);
        final long[] starts = assertDerivedIsRecorded(member, 1);
        if (termLength == null) {
            assertTrue(starts[0] > 0, "the restarted member's queue starts at its cut");
        }
    }

    /**
     * Derives replays of each of the member's queues, from its recording's start and from two frames further in, and
     * holds each to the archive's replay of the recording. A member the test restarted recorded from wherever its queue
     * had reached, at or past where the queue starts.
     * @param checkpoints how many the derivation must have kept first
     * @return where each group's queue starts
     */
    private long[] assertDerivedIsRecorded(final int member, final int checkpoints) {
        final Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(cluster.aeronDir(member)));
        final AeronArchive archive =
            AeronArchive.connect(new AeronArchive.Context()
                                     .aeron(aeron)
                                     .ownsAeronClient(false)
                                     .controlRequestChannel(PortLayout.ARCHIVE_CONTROL_CHANNEL)
                                     .controlRequestStreamId(PortLayout.ARCHIVE_CONTROL_STREAM_ID)
                                     .controlResponseChannel(PortLayout.ARCHIVE_CONTROL_CHANNEL)
                                     .controlResponseStreamId(RESPONSE_STREAM_ID));
        try {
            final Replayer recorded = new AeronReplayer(aeron, archive, member);
            final DerivingReplayer deriving = new DerivingReplayer(
                recorded, aeron, archive, cluster.archiveDir(member), member,
                () -> cluster.derivationStart(member), checkpointLogBytes, maxCheckpoints, maxRunBytes);
            final long deadline = System.nanoTime() + TIMEOUT_NS;
            while (deriving.checkpointCount() < checkpoints) {
                assertTrue(System.nanoTime() < deadline,
                           "member " + member + "'s derivation kept " + deriving.checkpointCount() + " checkpoint(s)");
                if (deriving.doWork() == 0) {
                    Tests.yield();
                }
            }
            final long[] starts = new long[InProcessCluster.GROUP_IDS.size()];
            final LongArrayList middles = new LongArrayList();
            for (final int groupId : InProcessCluster.GROUP_IDS) {
                final Replayer.RecordingSpan span = recorded.listQueueRecordings(groupId)
                                                        .stream()
                                                        .filter(Replayer.RecordingSpan::active)
                                                        .findFirst()
                                                        .orElseThrow();
                final long tip = recorded.recordingPosition(span.recordingId());
                final LongArrayList frames = new LongArrayList();
                final LongArrayList seqNos = new LongArrayList();
                assertEquals(Replayer.ScanEnd.TIP,
                             recorded.scanQueueRecording(span.recordingId(), span.startPosition(), tip,
                                                         (position, clusterSeqNo) -> {
                                                             frames.addLong(position);
                                                             seqNos.addLong(clusterSeqNo);
                                                             return true;
                                                         }));
                final String queue = "member " + member + "'s queue of group " + groupId;
                final long derivedStart = deriving.listQueueRecordings(groupId).getFirst().startPosition();
                assertEquals(cluster.derivationStart(member).queueStarts()[groupId - 1], derivedStart,
                             queue + ": where it starts");
                assertTrue(span.startPosition() >= derivedStart,
                           queue + ": the recording starts at " + span.startPosition() + ", before the queue");
                // First: a scan ends where the derivation has brought the queue, and a heartbeat may just have moved
                // the tip.
                assertDerivedTipIsRecorded(recorded, deriving, span.recordingId(), groupId, queue);
                assertDerivedScanIsRecorded(deriving, groupId, tip, frames, seqNos, 0, queue);
                assertDerivedScanIsRecorded(deriving, groupId, tip, frames, seqNos, frames.size() / 2, queue);
                assertEquals(Replayer.ScanEnd.UNREADABLE,
                             deriving.scanQueueRecording(groupId, frames.getLong(frames.size() / 2) + 32, tip,
                                                         (position, clusterSeqNo) -> true),
                             queue + ": a scan from inside a frame");
                final List<Fragment> whole = assertDerivedIsRecorded(aeron, archive, deriving, span.recordingId(),
                                                                     groupId, span.startPosition(), tip, queue);
                for (final long from :
                     new long[] { frames.getLong(frames.size() / 2), frames.getLong(frames.size() - 2) }) {
                    assertDerivedIsRecorded(aeron, archive, deriving, span.recordingId(), groupId, from, tip, queue);
                }
                // The end of a fragment that is not its frame's last is inside the frame.
                final Fragment inside = whole.stream()
                                            .skip(whole.size() / 2)
                                            .filter(fragment -> (fragment.flags() & FrameDescriptor.END_FRAG_FLAG) == 0)
                                            .findFirst()
                                            .orElse(null);
                if (inside != null) {
                    assertDerivedIsRecorded(aeron, archive, deriving, span.recordingId(), groupId, inside.position(),
                                            tip, queue);
                    insideFrames++;
                }
                starts[InProcessCluster.GROUP_IDS.indexOf(groupId)] = derivedStart;
                middles.addLong(frames.getLong(frames.size() / 2));
            }
            runs = deriving.runCount();
            sequencedRuns = deriving.sequencedRunCount();
            System.out.printf("member %d: %d run(s), %d of them a sequencer's%n", member, runs, sequencedRuns);
            assertTrue(deriving.checkpointCount() <= maxCheckpoints,
                       deriving.checkpointCount() + " checkpoints kept, past the bound of " + maxCheckpoints);
            if (purge) {
                final int kept = deriving.checkpointCount();
                for (final int groupId : InProcessCluster.GROUP_IDS) {
                    final long middle = middles.getLong(InProcessCluster.GROUP_IDS.indexOf(groupId));
                    assertTrue(deriving.purgeSegments(groupId, middle));
                    assertEquals(middle, deriving.listQueueRecordings(groupId).getFirst().startPosition());
                }
                assertTrue(deriving.checkpointCount() < kept, "the purges released none of " + kept + " checkpoint(s)");
                for (final int groupId : InProcessCluster.GROUP_IDS) {
                    final Replayer.RecordingSpan span = recorded.listQueueRecordings(groupId)
                                                            .stream()
                                                            .filter(Replayer.RecordingSpan::active)
                                                            .findFirst()
                                                            .orElseThrow();
                    assertDerivedIsRecorded(aeron, archive, deriving, span.recordingId(), groupId,
                                            middles.getLong(InProcessCluster.GROUP_IDS.indexOf(groupId)),
                                            recorded.recordingPosition(span.recordingId()),
                                            "member " + member + "'s purged queue of group " + groupId);
                }
            }
            assertFalse(cluster.fatal(), "a member raised a fatal");
            assertFalse(cluster.terminated(), "a member terminated");
            return starts;
        } finally {
            CloseHelper.closeAll(archive, aeron);
        }
    }

    /** Holds a derived scan from the frame at {@code first} to the recording's frames from there. */
    private static void assertDerivedScanIsRecorded(final DerivingReplayer deriving, final int groupId, final long tip,
                                                    final LongArrayList frames, final LongArrayList seqNos,
                                                    final int first, final String queue) {
        final LongArrayList derivedFrames = new LongArrayList();
        final LongArrayList derivedSeqNos = new LongArrayList();
        assertEquals(Replayer.ScanEnd.TIP,
                     deriving.scanQueueRecording(groupId, frames.getLong(first), tip, (position, clusterSeqNo) -> {
                         derivedFrames.addLong(position);
                         derivedSeqNos.addLong(clusterSeqNo);
                         return true;
                     }), queue + ": the derived scan from frame " + first);
        assertEquals(frames.subList(first, frames.size()), derivedFrames, queue + ": positions from frame " + first);
        assertEquals(seqNos.subList(first, seqNos.size()), derivedSeqNos,
                     queue + ": clusterSeqNos from frame " + first);
    }

    /** The derived tip is the recording's whenever the queue is still, between two heartbeats. */
    private static void assertDerivedTipIsRecorded(final Replayer recorded, final DerivingReplayer deriving,
                                                   final long recordingId, final int groupId, final String queue) {
        final long deadline = System.nanoTime() + TIMEOUT_NS;
        long derived;
        long tip;
        while ((derived = deriving.recordingPosition(groupId)) != (tip = recorded.recordingPosition(recordingId))) {
            assertTrue(System.nanoTime() < deadline, queue + ": the derived tip is " + derived + ", recorded " + tip);
            if (deriving.doWork() == 0) {
                Tests.yield();
            }
        }
    }

    /** @return the recorded replay's fragments */
    private static List<Fragment> assertDerivedIsRecorded(final Aeron aeron, final AeronArchive archive,
                                                          final DerivingReplayer deriving, final long recordingId,
                                                          final int groupId, final long from, final long tip,
                                                          final String queue) {
        final long recordedSession =
            archive.startReplay(recordingId, from, tip - from, REPLAY_CHANNEL, RECORDED_STREAM_ID);
        final List<Fragment> expected = read(aeron, (int)recordedSession, RECORDED_STREAM_ID, tip, () -> 0);

        final long startedNs = System.nanoTime();
        final long derivedSession = deriving.startReplay(groupId, from, tip - from, DERIVED_STREAM_ID);
        final List<Fragment> actual = read(aeron, (int)derivedSession, DERIVED_STREAM_ID, tip, deriving::doWork);
        final long micros = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - startedNs);
        deriving.stopReplay(derivedSession);

        assertEquals(expected.size(), actual.size(), queue + ": fragments from " + from);
        for (int i = 0; i < expected.size(); i++) {
            final String fragment = queue + ": fragment " + i + " from " + from;
            assertEquals(expected.get(i).position(), actual.get(i).position(), fragment + ", position");
            assertEquals(expected.get(i).flags(), actual.get(i).flags(), fragment + ", flags");
            assertArrayEquals(expected.get(i).bytes(), actual.get(i).bytes(), fragment);
        }
        System.out.printf("%s: [%d,%d) derived as recorded, %d fragment(s) in %d us%n", queue, from, tip,
                          expected.size(), micros);
        return expected;
    }

    /** Reads a replay's fragments up to {@code endPosition}, doing {@code work} between polls. */
    private static List<Fragment> read(final Aeron aeron, final int sessionId, final int streamId,
                                       final long endPosition, final java.util.function.IntSupplier work) {
        final List<Fragment> fragments = new ArrayList<>();
        final long[] position = { 0 };
        final FragmentHandler handler = (buffer, offset, length, header) -> {
            final byte[] bytes = new byte[length];
            buffer.getBytes(offset, bytes);
            position[0] = header.position();
            fragments.add(new Fragment(position[0], header.flags() & 0xFF, bytes));
        };
        final long deadline = System.nanoTime() + TIMEOUT_NS;
        try (Subscription subscription =
                 aeron.addSubscription(ChannelUri.addSessionId(REPLAY_CHANNEL, sessionId), streamId)) {
            while (position[0] < endPosition) {
                if (work.getAsInt() + subscription.poll(handler, 100) == 0) {
                    if (System.nanoTime() > deadline) {
                        fail("the replay on stream " + streamId + " reached " + position[0] + " of " + endPosition);
                    }
                    Tests.yield();
                }
            }
        }
        return fragments;
    }

    private void send(final int member, final int frames, final int filler) {
        try (ConfirmProducer producer = new ConfirmProducer(cluster.aeronDir(member), member, frames, 0, filler)) {
            while (!producer.isDrained()) {
                assertFalse(producer.isStalled(), "the producer stalled with " + producer.held() + " held");
                producer.doWork();
            }
        }
    }
}
