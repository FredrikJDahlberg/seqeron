package org.limitless.seqeron.tools;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
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
    private int trimLog(final int member) throws Exception {
        return new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                                  "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED",
                                  "-cp", System.getProperty("java.class.path"),
                                  "-Dclusterctl.memberId=" + member, "-Dclusterctl.baseDir=" + baseDir,
                                  "-Dclusterctl.aeronDir=" + cluster.aeronDir(member),
                                  ClusterCtl.class.getName(), "trim-log")
            .inheritIO().start().waitFor();
    }

    @Test
    @InterruptAfter(240)
    void aMemberWhoseLogWasTrimmedRestartsFromItsSnapshot() throws Exception {
        start("200", 1);
        final int leader = cluster.awaitLeader();
        final int member = (leader + 1) % InProcessCluster.MEMBERS;
        awaitSafeRounds(member, 3);

        assertEquals(0, trimLog(member));
        assertEquals(0, trimLog(member), "a second run finds the first's work done");
        assertTrue(safeLog(member).trimmed());

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
}
