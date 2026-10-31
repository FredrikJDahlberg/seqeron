package org.limitless.seqeron.tools;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.archive.CatalogView;
import io.aeron.cluster.RecordingLog;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.limitless.seqeron.replayer.server.RelayedCut;
import io.aeron.Aeron;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.limitless.seqeron.app.ClusterError;
import org.limitless.seqeron.app.Gateway;
import org.limitless.seqeron.app.GroupDirectory;
import org.limitless.seqeron.app.Payload;
import org.limitless.seqeron.app.SnapshotListener;
import org.limitless.seqeron.protocol.PortLayout;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.limitless.seqeron.protocol.SystemFrame;
import org.limitless.seqeron.replayer.server.RestartSafeLog;

/**
 * Cluster snapshots (doc/snapshot.md §10): a round is taken by the cluster's snapshot action, a member restarts from
 * the newest one it has marked restart-safe, and its queue starts again at the cut.
 */
@ExtendWith(InterruptingTestCallback.class)
class ClusterSnapshotTest {
    private static final String JOIN_PROPERTY = "replayer.snapshotJoinMs";

    @RegisterExtension
    final SystemTestWatcher watcher = new SystemTestWatcher();

    @TempDir
    Path baseDir;

    private InProcessCluster cluster;
    private Boolean gatewayRow;
    private String joinMs;

    @BeforeEach
    void remember() {
        joinMs = System.getProperty(JOIN_PROPERTY);
        watcher.dataCollector().add(baseDir);
    }

    @AfterEach
    void stopCluster() {
        if (cluster != null) {
            cluster.close();
        }
        if (joinMs == null) {
            System.clearProperty(JOIN_PROPERTY);
        } else {
            System.setProperty(JOIN_PROPERTY, joinMs);
        }
    }

    private void start(final String joinWindowMs, final int intervalSeconds) {
        System.setProperty(JOIN_PROPERTY, joinWindowMs);
        cluster = new InProcessCluster(baseDir);
        if (gatewayRow != null) {
            cluster.withGateway(1, GATEWAY_SOURCE_ID, "gateway-1", gatewayRow);
        }
        cluster.startAll(intervalSeconds, 0);
    }

    private RestartSafeLog.State safeLog(final int member) {
        return RestartSafeLog.read(baseDir.resolve("cluster-" + member).resolve(RestartSafeLog.FILE_NAME));
    }

    /** Waits for member {@code m}'s log to name a snapshot as safe. */
    private RestartSafeLog.State awaitSafe(final int member) {
        for (int i = 0; i < 600; i++) {
            final RestartSafeLog.State state = safeLog(member);
            if (!state.safe().isEmpty()) {
                return state;
            }
            Tests.sleep(50);
        }
        throw new AssertionError("member " + member + " marked no snapshot restart-safe in 30 s: " + safeLog(member));
    }

    @Test
    @InterruptAfter(180)
    void aMemberRestartsFromItsRestartSafeSnapshotAndItsQueueStartsAtTheCut() throws Exception {
        start("200", 1);
        final int leader = cluster.awaitLeader();
        final int member = (leader + 1) % InProcessCluster.MEMBERS;
        awaitSafe(member);

        cluster.stop(member);
        cluster.start(member);

        // The restarted member's queue is one recording, starting where its marker did, with the marker first.
        List<QueueFrames.Span> spans;
        do {
            Tests.sleep(100);
            spans = QueueFrames.recordings(cluster.aeronDir(member), InProcessCluster.GROUP_ID);
        } while (spans.stream().noneMatch(span -> span.active() && span.startPosition() > 0));
        final QueueFrames.Span restored = spans.stream().filter(QueueFrames.Span::active).findFirst().orElseThrow();
        assertEquals(1, spans.size(), "the earlier run's recording is purged: " + spans);

        final List<QueueFrames.Frame> first =
            QueueFrames.readFrom(cluster.aeronDir(member), InProcessCluster.GROUP_ID, restored.startPosition(), 3);
        assertTrue(first.getFirst().system());
        assertEquals(SystemFrame.SNAPSHOT_STARTED, first.getFirst().systemEventType(), "the marker is its first frame");
        assertTrue(first.getFirst().groupSeqNo() > 1, "and it is not the first frame of the queue");

        // The leader holds the same frame at the same position: positions agree across members.
        final List<QueueFrames.Frame> leaders = QueueFrames.readFrom(
            cluster.aeronDir(leader), InProcessCluster.GROUP_ID, restored.startPosition(), 3);
        assertEquals(first, leaders, "the restored queue is the leader's from the cut");

        assertFalse(cluster.fatal(), "a member raised a fatal");
        assertFalse(cluster.terminated(), "a member terminated");
        assertTrue(Files.exists(baseDir.resolve("cluster-" + member).resolve(RestartSafeLog.FILE_NAME)));
    }

    @Test
    @InterruptAfter(180)
    void aMemberWithNoRestartSafeSnapshotReplaysItsWholeLogAndReplaysTheSnapshotActionsInIt() throws Exception {
        start("3600000", 1); // no round is ever marked safe: the join window outlasts the test
        final int leader = cluster.awaitLeader();
        final int member = (leader + 1) % InProcessCluster.MEMBERS;
        while (QueueFrames.readFrom(cluster.aeronDir(member), InProcessCluster.GROUP_ID, 0, 40).stream()
                   .noneMatch(frame -> frame.systemEventType() == SystemFrame.SNAPSHOT_STARTED)) {
            Tests.sleep(200);
        }
        assertTrue(safeLog(member).safe().isEmpty());
        assertFalse(safeLog(member).cuts().isEmpty(), "a cut was recorded before its snapshot was acked");

        cluster.stop(member);
        cluster.start(member);

        // The launcher invalidated the snapshots, so the member replays from clusterSeqNo 1 into a new recording.
        List<QueueFrames.Span> spans;
        do {
            Tests.sleep(100);
            spans = QueueFrames.recordings(cluster.aeronDir(member), InProcessCluster.GROUP_ID);
        } while (spans.stream().filter(QueueFrames.Span::active).count() != 1 || spans.size() < 2);
        final QueueFrames.Span fresh = spans.stream().filter(QueueFrames.Span::active).findFirst().orElseThrow();
        assertEquals(0, fresh.startPosition());
        final List<QueueFrames.Frame> own = QueueFrames.readFrom(cluster.aeronDir(member), InProcessCluster.GROUP_ID, 0, 40);
        assertEquals(1, own.getFirst().groupSeqNo(), "the whole queue, from its first frame");
        assertEquals(own, QueueFrames.readFrom(cluster.aeronDir(leader), InProcessCluster.GROUP_ID, 0, 40),
                     "frame for frame what the leader holds, the replayed snapshot actions' markers included");

        assertFalse(cluster.fatal(), "a member raised a fatal");
        assertFalse(cluster.terminated(), "a member terminated");
    }

    @Test
    @InterruptAfter(180)
    void aMemberRestartedWithAnotherTermLengthStartsItsQueueAtZeroAndTheCutIsFoundByItsClusterSeqNo() throws Exception {
        start("200", 1);
        final int leader = cluster.awaitLeader();
        final int member = (leader + 1) % InProcessCluster.MEMBERS;
        awaitSafe(member);
        cluster.stop(member);

        final String termLength = System.getProperty("aeron.ipc.term.buffer.length");
        System.setProperty("aeron.ipc.term.buffer.length", String.valueOf(8 * 1024 * 1024));
        try {
            cluster.start(member);
        } finally {
            if (termLength == null) {
                System.clearProperty("aeron.ipc.term.buffer.length");
            } else {
                System.setProperty("aeron.ipc.term.buffer.length", termLength);
            }
        }

        List<QueueFrames.Span> spans;
        do {
            Tests.sleep(100);
            spans = QueueFrames.recordings(cluster.aeronDir(member), InProcessCluster.GROUP_ID);
        } while (spans.stream().filter(QueueFrames.Span::active).count() != 1);
        final QueueFrames.Span restored = spans.stream().filter(QueueFrames.Span::active).findFirst().orElseThrow();
        assertEquals(0, restored.startPosition(), "its positions are its own");

        final List<QueueFrames.Frame> own = QueueFrames.readFrom(cluster.aeronDir(member), InProcessCluster.GROUP_ID, 0, 3);
        assertEquals(SystemFrame.SNAPSHOT_STARTED, own.getFirst().systemEventType(), "the marker is still its first frame");
        assertTrue(own.getFirst().groupSeqNo() > 1);
        assertFalse(cluster.fatal(), "a member raised a fatal");
        assertFalse(cluster.terminated(), "a member terminated");
    }

    @Test
    @InterruptAfter(180)
    void aRoundHoldsSequencingForTheStateWriteAlone() throws Exception {
        start("200", 1);
        final int leader = cluster.awaitLeader();
        for (int round = 0; round < 6; round++) {
            awaitSafeRounds(leader, round + 1);
        }
        final List<QueueFrames.Frame> frames = QueueFrames.readFrom(cluster.aeronDir(leader), InProcessCluster.GROUP_ID, 0,
                                                                    50);
        long lastHeartbeat = -1;
        long widest = 0;
        int rounds = 0;
        for (final QueueFrames.Frame frame : frames) {
            rounds += frame.systemEventType() == SystemFrame.SNAPSHOT_STARTED ? 1 : 0;
            if (frame.systemEventType() == SystemFrame.CLUSTER_HEARTBEAT) {
                widest = lastHeartbeat < 0 ? widest : Math.max(widest, frame.timestampNs() - lastHeartbeat);
                lastHeartbeat = frame.timestampNs();
            }
        }
        assertTrue(rounds >= 3, "rounds in the first frames: " + rounds);
        // Heartbeats are a second apart; a round's hold must stay far below the 20 s a consumer waits for the next.
        assertTrue(widest < 5_000_000_000L, "the widest heartbeat gap across the rounds was " + widest / 1_000_000 + " ms");
    }

    /** Runs {@code clusterctl trim-log} against a member, in a process of its own as an operator would. */
    private int trimLog(final int member, final String... options) throws Exception {
        final List<String> command = new ArrayList<>(List.of(
            Path.of(System.getProperty("java.home"), "bin", "java").toString(),
            "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED",
            "-cp", System.getProperty("java.class.path"),
            "-Dclusterctl.memberId=" + member, "-Dclusterctl.baseDir=" + baseDir,
            "-Dclusterctl.aeronDir=" + cluster.aeronDir(member),
            ClusterCtl.class.getName(), "trim-log"));
        command.addAll(List.of(options));
        return new ProcessBuilder(command).inheritIO().start().waitFor();
    }

    /** The start position of the recording of a member's first log term, which a purged segment moves. */
    private long firstTermStart(final int member) {
        final long[] start = {-1};
        try (RecordingLog log = new RecordingLog(baseDir.resolve("cluster-" + member).toFile(), false)) {
            final long recordingId = log.entries().stream().filter(e -> e.type == RecordingLog.ENTRY_TYPE_TERM)
                                        .findFirst().orElseThrow().recordingId;
            CatalogView.listRecording(baseDir.resolve("archive-" + member).toFile(), recordingId,
                                      (controlSessionId, correlationId, id, startTimestamp, stopTimestamp,
                                       startPosition, stopPosition, initialTermId, segmentFileLength, termBufferLength,
                                       mtuLength, sessionId, streamId, strippedChannel, originalChannel,
                                       sourceIdentity) -> start[0] = startPosition);
        }
        return start[0];
    }

    @Test
    @InterruptAfter(240)
    void aMemberWhoseLogWasTrimmedRestartsFromItsSnapshot() throws Exception {
        start("200", 1);
        final int leader = cluster.awaitLeader();
        final int member = (leader + 1) % InProcessCluster.MEMBERS;
        awaitSafeRounds(member, 3);

        assertEquals(0, trimLog(member, "--dry-run"));
        assertEquals(0, trimLog(member));
        assertEquals(0, trimLog(member), "a second run finds the first's work done");
        // Less than a segment of log, so only the snapshots went and the log still begins at its first term.
        assertEquals(0, firstTermStart(member));

        cluster.stop(member);
        cluster.start(member);

        List<QueueFrames.Span> spans;
        do {
            Tests.sleep(100);
            spans = QueueFrames.recordings(cluster.aeronDir(member), InProcessCluster.GROUP_ID);
        } while (spans.stream().noneMatch(span -> span.active() && span.startPosition() > 0));
        final QueueFrames.Span restored = spans.stream().filter(QueueFrames.Span::active).findFirst().orElseThrow();
        assertEquals(QueueFrames.readFrom(cluster.aeronDir(leader), InProcessCluster.GROUP_ID, restored.startPosition(), 3),
                     QueueFrames.readFrom(cluster.aeronDir(member), InProcessCluster.GROUP_ID, restored.startPosition(), 3),
                     "the member recovered and holds the leader's queue from the cut");
        assertFalse(cluster.fatal(), "a member raised a fatal");
        assertFalse(cluster.terminated(), "a member terminated");
    }

    /** Waits until the member's log names {@code count} snapshots as safe. */
    private void awaitSafeRounds(final int member, final int count) {
        for (int i = 0; i < 1200 && safeLog(member).safe().size() < count; i++) {
            Tests.sleep(50);
        }
        assertTrue(safeLog(member).safe().size() >= count, "member " + member + " marked " + safeLog(member).safe().size()
                                                           + " snapshot(s) restart-safe, not " + count);
    }

    @Test
    @InterruptAfter(180)
    void aGatewayThatJoinedAndNeverConfirmsKeepsTheMembersCutUnsafeUntilItGoes() throws Exception {
        // Not the last row, so no designation follows: the gateway joins every round, and no instance ends one.
        gatewayRow = false;
        start("200", 1);
        final int member = (cluster.awaitLeader() + 1) % InProcessCluster.MEMBERS;
        awaitSafe(member);
        final int safeBefore;
        try (GatewayInstance gateway = new GatewayInstance(member)) {
            gateway.awaitCaughtUp();
            Tests.sleep(2_500); // the rounds cut before it joined, or in flight as it did, may be safe
            safeBefore = safeLog(member).safe().size();
            Tests.sleep(5_000);
            assertEquals(safeBefore, safeLog(member).safe().size(), "a round the gateway joined was marked safe");
        }
        awaitSafeRounds(member, safeBefore + 1);
        assertFalse(cluster.fatal(), "a member raised a fatal");
    }

    @Test
    @InterruptAfter(180)
    void aGatewayThatConfirmsItsRoundsLetsTheMembersCutBeSafe() throws Exception {
        gatewayRow = true;
        start("200", 1);
        final int member = (cluster.awaitLeader() + 1) % InProcessCluster.MEMBERS;
        awaitSafe(member);
        try (GatewayInstance gateway = new GatewayInstance(member)) {
            gateway.awaitCaughtUp();
            Tests.sleep(2_500);
            final int safeBefore = safeLog(member).safe().size();
            awaitSafeRounds(member, safeBefore + 2);
        }
        assertFalse(cluster.fatal(), "a member raised a fatal");
    }

    @Test
    @InterruptAfter(180)
    void aGatewayWithNoSnapshotFileOnAMemberRestartedAtACutFailsFromItsDoWork() throws Exception {
        gatewayRow = true;
        start("200", 1);
        final int member = (cluster.awaitLeader() + 1) % InProcessCluster.MEMBERS;
        awaitSafe(member);
        cluster.stop(member);
        cluster.start(member);
        while (QueueFrames.recordings(cluster.aeronDir(member), InProcessCluster.GROUP_ID).stream()
                   .noneMatch(span -> span.active() && span.startPosition() > 0)) {
            Tests.sleep(100);
        }

        // Its walk from the recording's start opens on the cut's marker, not on groupSeqNo 1.
        try (GatewayInstance gateway = new GatewayInstance(member)) {
            final RuntimeException failure = gateway.awaitFailure();
            assertTrue(failure.getMessage().contains("expected 1"), failure.getMessage());
            assertFalse(gateway.caughtUp.get(), "it caught up on a queue it does not hold from its start");
        }
        assertFalse(cluster.fatal(), "a member raised a fatal");
    }

    @Test
    @InterruptAfter(180)
    void aGatewayRestartedOnItsMemberRestoresItsConfirmedSnapshotAndCatchesUp() throws Exception {
        gatewayRow = true;
        start("200", 1);
        final int member = (cluster.awaitLeader() + 1) % InProcessCluster.MEMBERS;
        try (GatewayInstance gateway = new GatewayInstance(member)) {
            gateway.awaitCaughtUp();
            for (int i = 0; i < 600 && confirmedSnapshots() == 0; i++) {
                Tests.sleep(50);
            }
            assertTrue(confirmedSnapshots() > 0, "the gateway confirmed no snapshot in 30 s");
        }
        Tests.sleep(3_000); // the queue runs on past the cut, so its replay carries more than one frame

        // The replay of the cut is let go once its first frame shows the cut, and the frames after it in that poll
        // must not reach recovery, which has dispatched nothing yet and would take the next for a first frame.
        // Here the image the close closes stops the poll; where a close is asynchronous, as in Aeron's C client,
        // the adapter's own check drops them.
        try (GatewayInstance restarted = new GatewayInstance(member)) {
            restarted.awaitCaughtUp();
            assertNull(restarted.failure.get(), "doWork threw");
            assertEquals(1, restarted.restored.get(), "it restored its snapshot's one record");
        }
        assertFalse(cluster.fatal(), "a member raised a fatal");
    }

    /** How many snapshot files a gateway instance has confirmed. */
    private long confirmedSnapshots() throws IOException {
        final Path directory = baseDir.resolve("gateway-snapshots");
        if (!Files.isDirectory(directory)) {
            return 0;
        }
        try (Stream<Path> files = Files.list(directory)) {
            return files.filter(file -> file.getFileName().toString().endsWith(".snapshot")).count();
        }
    }

    @Test
    @InterruptAfter(180)
    void aGatewayHostRelayReportsEachMarkerAtItsStartInTheHostsOwnRecording() throws Exception {
        start("3600000", 1);
        // The host's Replayer consumes the reports; this one only records what it takes.
        final List<RelayedCut> cuts = new CopyOnWriteArrayList<>();
        cluster.startGatewayHost(new ConcurrentLinkedQueue<>() {
            @Override
            public RelayedCut poll() {
                final RelayedCut cut = super.poll();
                if (cut != null) {
                    cuts.add(cut);
                }
                return cut;
            }
        });
        for (int i = 0; i < 600 && cuts.size() < 3; i++) {
            Tests.sleep(50);
        }
        assertTrue(cuts.size() >= 3, "the relay reported " + cuts.size() + " marker(s) in 30 s");

        for (final RelayedCut cut : cuts) {
            final List<QueueFrames.Frame> frame = QueueFrames.readFrom(
                cluster.aeronDir(InProcessCluster.GATEWAY_HOST), cut.groupId(), cut.position(), 1);
            assertEquals(SystemFrame.SNAPSHOT_STARTED, frame.getFirst().systemEventType(), "at " + cut);
            assertEquals(cut.clusterSeqNo(), frame.getFirst().clusterSeqNo(), "at " + cut);
        }
        assertEquals(cuts.size(), cuts.stream().map(cut -> cut.clusterSeqNo() + "/" + cut.groupId()).distinct().count(),
                     "a marker is reported once per queue: " + cuts);
        assertFalse(cluster.fatal(), "a member or the host raised a fatal");
    }

    @Test
    @InterruptAfter(180)
    void aGatewayHostRestartedTwiceHoldsOneRecordingPerRelayedQueue() throws Exception {
        start("3600000", 1);
        for (int run = 0; run < 3; run++) {
            cluster.startGatewayHost();
            Tests.sleep(2_500);
            for (final int groupId : InProcessCluster.GROUP_IDS) {
                final List<QueueFrames.Span> spans =
                    QueueFrames.recordings(cluster.aeronDir(InProcessCluster.GATEWAY_HOST), groupId);
                assertEquals(1, spans.size(), "run " + run + ", group " + groupId + ": " + spans);
                assertTrue(spans.getFirst().active(), "run " + run + ", group " + groupId + ": " + spans);
            }
            if (run < 2) {
                cluster.stopGatewayHost();
            }
        }
        assertFalse(cluster.fatal(), "a member or the host raised a fatal");
    }

    private static final String TERM_PROPERTY = "aeron.ipc.term.buffer.length";
    private static final String SEGMENT_PROPERTY = "aeron.archive.segment.file.length";
    private static final int SMALL_SEGMENT = 64 * 1024;

    private static String setProperty(final String name, final String value) {
        final String before = System.getProperty(name);
        System.setProperty(name, value);
        return before;
    }

    private static void restoreProperty(final String name, final String before) {
        if (before == null) {
            System.clearProperty(name);
        } else {
            System.setProperty(name, before);
        }
    }

    /** The start of the gateway host's active recording of group 1's queue. */
    private long hostStart() {
        return hostStart(InProcessCluster.GROUP_ID);
    }

    private long hostStart(final int groupId) {
        return QueueFrames.recordings(cluster.aeronDir(InProcessCluster.GATEWAY_HOST), groupId)
            .stream().filter(QueueFrames.Span::active).findFirst().orElseThrow().startPosition();
    }

    /**
     * Runs a host with one gateway on it through rounds over a queue of small segments, carrying traffic for several
     * of them, and returns the start of its recording of group 1 once the traffic has drained and rounds have followed.
     * Closes the gateway first, so the host may be stopped.
     */
    private long hostStartAfterTraffic(final boolean gatewayConfirms, final boolean awaitPurge) throws Exception {
        final String term = setProperty(TERM_PROPERTY, String.valueOf(SMALL_SEGMENT));
        final String segment = setProperty(SEGMENT_PROPERTY, String.valueOf(SMALL_SEGMENT));
        try {
            gatewayRow = gatewayConfirms;
            start("200", 1);
            final int member = (cluster.awaitLeader() + 1) % InProcessCluster.MEMBERS;
            cluster.startGatewayHost();
            try (GatewayInstance gateway = new GatewayInstance(InProcessCluster.GATEWAY_HOST);
                 ConfirmProducer producer = new ConfirmProducer(cluster.aeronDir(member), member, 1_500, 5_000_000, 1_000)) {
                gateway.awaitCaughtUp();
                while (!producer.isDrained() && !producer.isStalled()) {
                    producer.doWork();
                    Tests.yield();
                }
                assertTrue(producer.isDrained(), "the producer did not drain");
                for (int i = 0; i < 1200 && awaitPurge && hostStart() == 0; i++) {
                    Tests.sleep(50);
                }
                Tests.sleep(2_500);
            }
            return hostStart();
        } finally {
            restoreProperty(TERM_PROPERTY, term);
            restoreProperty(SEGMENT_PROPERTY, segment);
        }
    }

    @Test
    @InterruptAfter(240)
    void aGatewayHostPurgesItsQueueBelowTheCutsItsGatewayConfirmsAndServesAfterARestart() throws Exception {
        final long start = hostStartAfterTraffic(true, true);
        assertTrue(start > 0, "the host's recording still starts at 0");
        assertEquals(0, start % SMALL_SEGMENT, "and a purge leaves it at a segment boundary");
        assertTrue(hostStart(InProcessCluster.OTHER_GROUP_ID) > 0, "group 2's too, from the same rounds");

        final List<QueueFrames.Frame> first =
            QueueFrames.readFrom(cluster.aeronDir(InProcessCluster.GATEWAY_HOST), InProcessCluster.GROUP_ID, start, 3);
        assertEquals(3, first.size(), "the recording is readable from its new start");
        final List<QueueFrames.Frame> run =
            QueueFrames.readFrom(cluster.aeronDir(InProcessCluster.GATEWAY_HOST), InProcessCluster.GROUP_ID, start, 100);
        for (int i = 1; i < run.size(); i++) {
            assertEquals(run.get(i - 1).groupSeqNo() + 1, run.get(i).groupSeqNo(), "a gap after " + run.get(i - 1));
        }

        cluster.stopGatewayHost();
        cluster.startGatewayHost();
        Tests.sleep(2_500);
        final List<QueueFrames.Span> spans =
            QueueFrames.recordings(cluster.aeronDir(InProcessCluster.GATEWAY_HOST), InProcessCluster.GROUP_ID);
        assertEquals(1, spans.size(), "a restarted host holds one recording: " + spans);
        final List<QueueFrames.Frame> frames = QueueFrames.readFrom(
            cluster.aeronDir(InProcessCluster.GATEWAY_HOST), InProcessCluster.GROUP_ID, spans.getFirst().startPosition(), 1);
        assertEquals(1, frames.size(), "it is readable from its start");
        assertTrue(frames.getFirst().groupSeqNo() > 1, "which is the member's purged start, not its first frame");
        assertFalse(cluster.fatal(), "a member or the host raised a fatal");
    }

    @Test
    @InterruptAfter(240)
    void aGatewayOnAHostThatNeverConfirmsHoldsTheHostsRecordingWhole() throws Exception {
        final long start = hostStartAfterTraffic(false, false);
        assertEquals(0, start, "a round the gateway joined was purged below");

        // Its instance gone, the next round does not wait for it: it misses the join window.
        for (int i = 0; i < 1200 && hostStart() == 0; i++) {
            Tests.sleep(50);
        }
        assertTrue(hostStart() > 0, "the host never purged once the gateway was gone");
        assertFalse(cluster.fatal(), "a member or the host raised a fatal");
    }

    @Test
    @InterruptAfter(240)
    void aRelayDisconnectedWhileEveryMemberPurgedPastItStopsAndItsRestartRelaysFromTheirStart() throws Exception {
        // A member's archive checks a replay's start, then registers the replay once its publication exists; a purge
        // in between deletes the segment it starts in. So a relay asking from a member's start while the member
        // purges times out, and the archive logs the replay's error to the relay's control session (stream 122), gone.
        watcher.ignoreErrorsMatching(s -> s.contains("recording segment file not created"));
        watcher.ignoreErrorsMatching(s -> s.contains("response publication is closed") && s.contains("streamId=122,"));
        final String term = setProperty(TERM_PROPERTY, String.valueOf(SMALL_SEGMENT));
        final String segment = setProperty(SEGMENT_PROPERTY, String.valueOf(SMALL_SEGMENT));
        try {
            start("200", 1);
            final int member = (cluster.awaitLeader() + 1) % InProcessCluster.MEMBERS;
            cluster.startGatewayHost();
            Tests.sleep(2_500); // it relays the first frames, well within one segment
            for (int m = 0; m < InProcessCluster.MEMBERS; m++) {
                cluster.cutArchive(m, true);
            }
            Tests.sleep(4_000); // past its source timeout: it has left every member

            try (ConfirmProducer producer = new ConfirmProducer(cluster.aeronDir(member), member, 1_500, 5_000_000, 1_000)) {
                while (!producer.isDrained() && !producer.isStalled()) {
                    producer.doWork();
                    Tests.yield();
                }
                assertTrue(producer.isDrained(), "the producer did not drain");
            }
            for (int m = 0; m < InProcessCluster.MEMBERS; m++) {
                for (int i = 0; i < 1200 && memberStart(m) < SMALL_SEGMENT; i++) {
                    Tests.sleep(50);
                }
                assertTrue(memberStart(m) >= SMALL_SEGMENT, "member " + m + " never purged past the relay");
            }

            for (int m = 0; m < InProcessCluster.MEMBERS; m++) {
                cluster.cutArchive(m, false);
            }
            for (int i = 0; i < 1200 && !cluster.fatal(InProcessCluster.GATEWAY_HOST); i++) {
                Tests.sleep(50);
            }
            assertTrue(cluster.fatal(InProcessCluster.GATEWAY_HOST), "the relay did not stop");

            // Its restart relays from a member's start, into a new recording.
            cluster.stopGatewayHost();
            cluster.startGatewayHost();
            Tests.sleep(2_500);
            final List<QueueFrames.Span> spans =
                QueueFrames.recordings(cluster.aeronDir(InProcessCluster.GATEWAY_HOST), InProcessCluster.GROUP_ID);
            assertEquals(1, spans.size(), "one recording: " + spans);
            final List<QueueFrames.Frame> frames = QueueFrames.readFrom(
                cluster.aeronDir(InProcessCluster.GATEWAY_HOST), InProcessCluster.GROUP_ID, spans.getFirst().startPosition(), 3);
            assertEquals(3, frames.size());
            assertTrue(frames.getFirst().groupSeqNo() > 1, "from the members' purged start, not the first frame");
            for (int m = 0; m < InProcessCluster.MEMBERS; m++) {
                assertFalse(cluster.fatal(m), "member " + m + " raised a fatal");
            }
            assertFalse(cluster.terminated(), "a member terminated");
        } finally {
            restoreProperty(TERM_PROPERTY, term);
            restoreProperty(SEGMENT_PROPERTY, segment);
        }
    }

    /** The start of member {@code m}'s active recording of group 1's queue. */
    private long memberStart(final int m) {
        return QueueFrames.recordings(cluster.aeronDir(m), InProcessCluster.GROUP_ID)
            .stream().filter(QueueFrames.Span::active).findFirst().orElseThrow().startPosition();
    }

    private static final int GATEWAY_SOURCE_ID = 40;

    /** The one record every gateway instance's snapshot holds. */
    private static final long SNAPSHOT_RECORD = 42;

    /** A gateway with a one-record snapshot, driven on its own thread against member {@code member}'s node. */
    private final class GatewayInstance implements AutoCloseable, Gateway.Listener, SnapshotListener {
        private final Aeron aeron;
        private final Gateway gateway;
        private final AtomicBoolean running = new AtomicBoolean(true);
        private final Thread duty;
        private final AtomicBoolean caughtUp = new AtomicBoolean();
        private final AtomicReference<RuntimeException> failure = new AtomicReference<>();
        private final AtomicInteger restored = new AtomicInteger();

        GatewayInstance(final int member) {
            aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(cluster.aeronDir(member)));
            gateway = Gateway.builder()
                .gatewayName("gateway-1")
                .clientId(60)
                .memberId(member)
                .egressChannel("aeron:udp?endpoint=" + PortLayout.DEFAULT_HOST + ":0")
                .ingressEndpoints(PortLayout.ingressEndpoints(InProcessCluster.MEMBERS))
                .listener(this)
                .sourceId(GATEWAY_SOURCE_ID)
                .snapshots(this, baseDir.resolve("gateway-snapshots"))
                .build();
            gateway.start(aeron);
            duty = new Thread(() -> {
                try {
                    while (running.get()) {
                        if (gateway.doWork() == 0) {
                            Tests.sleep(1);
                        }
                    }
                } catch (final RuntimeException ex) {
                    failure.set(ex);
                }
            }, "gateway-instance");
            duty.start();
        }

        void awaitCaughtUp() {
            for (int i = 0; i < 600 && !caughtUp.get() && failure.get() == null; i++) {
                Tests.sleep(50);
            }
            assertNull(failure.get(), "doWork threw");
            assertTrue(caughtUp.get(), "the gateway did not catch up in 30 s");
        }

        /** What {@code doWork} threw, which stopped this instance. */
        RuntimeException awaitFailure() {
            for (int i = 0; i < 600 && failure.get() == null; i++) {
                Tests.sleep(50);
            }
            assertTrue(failure.get() != null, "doWork threw nothing in 30 s");
            return failure.get();
        }

        @Override
        public void close() throws InterruptedException {
            running.set(false);
            duty.join(5_000);
            gateway.close();
            aeron.close();
        }

        @Override
        public void onGroupsResolved(final GroupDirectory directory) {
        }

        @Override
        public boolean onActivated(final int firstConnectionId) {
            return true;
        }

        @Override
        public void onStandby() {
        }

        @Override
        public void onSequenced(final Payload payload) {
        }

        @Override
        public void onConnectionOpened(final int connectionId, final DirectBuffer connectionData, final int offset,
                                       final int length) {
        }

        @Override
        public void onConnectionClosed(final int connectionId) {
        }

        @Override
        public void onCaughtUp(final long clusterSeqNo) {
            caughtUp.set(true);
        }

        @Override
        public void onClusterHeartbeat(final long clusterTimeNs, final long receiveTimeNs) {
        }

        @Override
        public void onFenced(final ClusterError fence, final String detail) {
            throw new AssertionError("gateway fenced: " + fence + " " + detail);
        }

        @Override
        public int formatVersion() {
            return 1;
        }

        @Override
        public int onSnapshot(final MutableDirectBuffer buffer, final int recordIndex) {
            if (recordIndex > 0) {
                return 0;
            }
            buffer.putLong(0, SNAPSHOT_RECORD);
            return Long.BYTES;
        }

        @Override
        public void onRestore(final DirectBuffer buffer, final int length, final int recordIndex,
                              final int formatVersion) {
            assertEquals(SNAPSHOT_RECORD, buffer.getLong(0));
            restored.incrementAndGet();
        }
    }
}
