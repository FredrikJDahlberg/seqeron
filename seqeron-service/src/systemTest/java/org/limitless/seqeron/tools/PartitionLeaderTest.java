package org.limitless.seqeron.tools;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * faulteron/partition-leader-test.sh: the leader of the docker cluster is cut off from its peers on every member
 * port but ingress while two confirm producers stream through a follower, then healed. The fault must have dropped
 * packets, the majority must elect a new leader meanwhile, the old leader must rejoin as a follower of the new term,
 * and the producer sending through {@code PendingSends} must see every frame once, in order. The untracked control's
 * loss is what the partition had in flight, reported rather than asserted.
 */
@Tag("docker")
class PartitionLeaderTest {
    private static final String CONFIRM_COUNT = System.getenv().getOrDefault("CONFIRM_COUNT", "40000");
    private static final String CONFIRM_PACING_MICROS = System.getenv().getOrDefault("CONFIRM_PACING_MICROS", "500");
    private static final long PARTITION_SECS = Long.parseLong(System.getenv().getOrDefault("PARTITION_SECS", "10"));
    private static final String INGRESS_MESSAGES = "seqeron.sequencer.ingressMessages";
    private static final String LEADER = "seqeron.sequencer.currentLeaderMemberId";

    private DockerCluster cluster;

    @BeforeEach
    void startCluster() {
        cluster = new DockerCluster(Path.of(System.getProperty("seqeron.systemTestLogs"), "PartitionLeaderTest"));
        cluster.start(Path.of(System.getProperty("seqeron.topologyGroups")));
    }

    @AfterEach
    void stopCluster() {
        cluster.close();
    }

    @Test
    @Timeout(value = 900, unit = TimeUnit.SECONDS)
    void majorityElectsAndPendingSendsIsExactAcrossALeaderPartition() throws Exception {
        final int old = cluster.awaitLeader(DockerCluster.deadline(60), DockerCluster.NO_MEMBER);
        final int producerMember = (old + 1) % DockerCluster.MEMBERS;
        final long before = cluster.counter(old, INGRESS_MESSAGES);
        final String count = "probe.count=" + CONFIRM_COUNT;
        final String pacing = "probe.pacingMicros=" + CONFIRM_PACING_MICROS;
        final ClusterProcesses.Child confirm = cluster.probe("confirm", producerMember, "confirm", count, pacing,
                                                             "probe.clientId=11");
        final ClusterProcesses.Child control = cluster.probe("control", producerMember, "confirm", count, pacing,
                                                             "probe.clientId=12", "probe.pendingSends=false");
        confirm.awaitLine("confirm: sending", DockerCluster.deadline(60));
        control.awaitLine("confirm: sending", DockerCluster.deadline(60));
        cluster.await("both producers mid-stream", DockerCluster.deadline(30),
                      () -> cluster.counter(old, INGRESS_MESSAGES) - before >= 4_000);

        assertEquals(0, cluster.faulteron(old, "attach").status(), "faulteron could not attach to node-" + old);
        final String[] block = new String[DockerCluster.internalPorts().length + 1];
        block[0] = "block";
        System.arraycopy(DockerCluster.internalPorts(), 0, block, 1, block.length - 1);
        assertEquals(0, cluster.faulteron(old, block).status(), "faulteron could not partition node-" + old);

        final int newLeader = cluster.awaitLeader(DockerCluster.deadline(30), old);
        // The fault window: the old leader stays cut off while the majority serves.
        Thread.sleep(TimeUnit.SECONDS.toMillis(PARTITION_SECS));

        final DockerCluster.Result stats = cluster.faulteron(old, "stats");
        Files.writeString(Path.of(System.getProperty("seqeron.systemTestLogs"), "PartitionLeaderTest", "rules.txt"),
                          stats.output());
        final long dropped = DockerCluster.dropped(stats.output());
        cluster.faulteron(old, "detach");

        // Healed, the old leader records the new one as its leader, in a later term.
        cluster.await("node-" + old + " to follow member " + newLeader, DockerCluster.deadline(60),
                      () -> cluster.counter(old, LEADER) == newLeader);
        final boolean running = cluster.isRunning(old);

        final int confirmStatus = confirm.awaitExit(DockerCluster.deadline(120));
        final int controlStatus = control.awaitExit(DockerCluster.deadline(120));
        System.out.printf("leader %d -> %d, producers on member %d, node-%d dropped %d packet(s)%n"
                          + "PendingSends producer: %s%ncontrol: %s%s%n",
                          old, newLeader, producerMember, old, dropped, confirm.lastLine("confirm:"),
                          control.lastLine("confirm:"),
                          controlStatus == 0 ? "\n  (the control lost nothing: the partition had no frame in "
                                               + "flight, so this run proved no recovery)" : "");

        assertAll(
            () -> assertTrue(dropped > 0, "the partition dropped nothing"),
            () -> assertTrue(newLeader != old, "no new leader while node-" + old + " was cut off"),
            () -> assertTrue(running, "node-" + old + " exited"),
            () -> assertEquals(0, confirmStatus, "PendingSends producer: " + confirm.lastLine("confirm:")));
    }
}
