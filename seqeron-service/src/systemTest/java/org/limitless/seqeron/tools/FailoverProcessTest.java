package org.limitless.seqeron.tools;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.limitless.seqeron.protocol.SeqeronCounters;

/**
 * failover-test.sh as child processes: a leader SIGKILLed while two confirm producers on a follower stream across
 * it, then a cold follower started on the new leader. The producer sending through {@code PendingSends} must see
 * every frame once, in order, and the cold follower must catch up through a replay of the new leader's one
 * continuous recording. The untracked control's loss is what the kill cost, reported rather than asserted. SIGKILL,
 * since a leader that dies ends no egress stream, so every run costs what a crash does (doc/fault-tolerance.md
 * §1.2).
 */
class FailoverProcessTest {
    private static final int COUNT = 20_000;
    private static final int PACING_MICROS = 100;

    @TempDir
    Path baseDir;

    private ClusterProcesses cluster;

    @BeforeEach
    void startCluster(final TestInfo test) {
        cluster = new ClusterProcesses(baseDir, Path.of(System.getProperty("seqeron.systemTestLogs"),
                                                        test.getTestClass().orElseThrow().getSimpleName()));
        cluster.startAll();
        cluster.loadTopology(Path.of(System.getProperty("seqeron.topologyGroups")));
    }

    @AfterEach
    void stopCluster() {
        cluster.close();
    }

    @Test
    @Timeout(value = 180, unit = TimeUnit.SECONDS)
    void coldFollowerCatchesUpAndPendingSendsIsExactAcrossALeaderKill() {
        final int oldLeader = cluster.awaitLeader();
        final int producerMember = (oldLeader + 1) % ClusterProcesses.MEMBERS;
        final long sequencedBefore = cluster.counter(oldLeader, SeqeronCounters.SEQUENCER_INGRESS_MESSAGES_TYPE_ID);
        final String count = "probe.count=" + COUNT;
        final String pacing = "probe.pacingMicros=" + PACING_MICROS;
        final ClusterProcesses.Child confirm =
            cluster.probe("confirm", producerMember, "confirm", count, pacing, "probe.clientId=11");
        final ClusterProcesses.Child control = cluster.probe("control", producerMember, "confirm", count, pacing,
                                                             "probe.clientId=12", "probe.pendingSends=false");
        confirm.awaitLine("confirm: sending", Duration.ofSeconds(30));
        control.awaitLine("confirm: sending", Duration.ofSeconds(30));
        // Both mid-stream: the leader has sequenced half of one producer's frames between them.
        cluster.await("both producers mid-stream", Duration.ofSeconds(30), () ->
            cluster.counter(oldLeader, SeqeronCounters.SEQUENCER_INGRESS_MESSAGES_TYPE_ID) - sequencedBefore >= COUNT / 2);

        cluster.kill(oldLeader);
        final int newLeader = cluster.awaitLeader();
        assertNotEquals(oldLeader, newLeader);
        cluster.await("the new leader sequencing its term", Duration.ofSeconds(30), () ->
            cluster.counter(newLeader, SeqeronCounters.SEQUENCER_CURRENT_LEADER_MEMBER_ID_TYPE_ID) == newLeader);

        final long replaysBefore = cluster.counter(newLeader, SeqeronCounters.REPLAYER_REPLAYS_SERVED_COUNT_TYPE_ID);
        final ClusterProcesses.Child fresh = cluster.probe("fresh-follower", newLeader, "follow", "probe.clientId=9");
        fresh.awaitLine("following live", Duration.ofSeconds(30));
        final long replays =
            cluster.counter(newLeader, SeqeronCounters.REPLAYER_REPLAYS_SERVED_COUNT_TYPE_ID) - replaysBefore;

        // Each producer judges itself once its stream has drained; its own drain timeout bounds the wait.
        final int confirmStatus = confirm.awaitExit(Duration.ofSeconds(120));
        final int controlStatus = control.awaitExit(Duration.ofSeconds(120));
        System.out.printf("leader %d -> %d, producers on member %d%nPendingSends producer: %s%ncontrol: %s%s%n",
                          oldLeader, newLeader, producerMember, confirm.lastLine("confirm:"),
                          control.lastLine("confirm:"),
                          controlStatus == 0 ? "\n  (the control lost nothing: the kill had no frame in flight, so "
                                               + "this run proved no recovery)" : "");

        assertTrue(replays >= 1, "member " + newLeader + " served the cold follower no replay");
        assertEquals(0, confirmStatus, "PendingSends producer: " + confirm.lastLine("confirm:"));
    }
}
