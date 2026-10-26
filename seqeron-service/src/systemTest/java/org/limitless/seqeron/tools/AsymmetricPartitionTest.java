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
 * The link between the leader and one follower, and only that link, is cut
 * while a confirm producer streams through the other follower. The cut-off follower hears no leader and stands for
 * election, and the only member it can ask still has a leader, so the leader must keep leading: no election completes
 * while the link is cut. The producer sending through {@code PendingSends} must see every frame once, in order, and
 * the healed follower must catch up.
 */
@Tag("docker")
class AsymmetricPartitionTest {
    private static final String CONFIRM_COUNT = System.getenv().getOrDefault("CONFIRM_COUNT", "60000");
    private static final String CONFIRM_PACING_MICROS = System.getenv().getOrDefault("CONFIRM_PACING_MICROS", "500");
    private static final long FAULT_SECS = Long.parseLong(System.getenv().getOrDefault("FAULT_SECS", "15"));
    private static final int PINGS_AFTER_HEAL = 5;

    private DockerCluster cluster;
    private Path logDir;

    @BeforeEach
    void startCluster() {
        logDir = Path.of(System.getProperty("seqeron.systemTestLogs"), "AsymmetricPartitionTest");
        cluster = new DockerCluster(logDir);
        cluster.start(Path.of(System.getProperty("seqeron.topologyGroups")));
    }

    @AfterEach
    void stopCluster() {
        cluster.close();
    }

    @Test
    @Timeout(value = 900, unit = TimeUnit.SECONDS)
    void leaderKeepsLeadingWhileOneFollowerIsCutOff() throws Exception {
        final int leader = cluster.awaitLeader(DockerCluster.deadline(60), DockerCluster.NO_MEMBER);
        final int cutOff = (leader + 1) % DockerCluster.MEMBERS;
        final int producerMember = (leader + 2) % DockerCluster.MEMBERS;
        final ClusterProcesses.Child confirm = cluster.probe("confirm", producerMember, "confirm",
                                                             "probe.count=" + CONFIRM_COUNT,
                                                             "probe.pacingMicros=" + CONFIRM_PACING_MICROS,
                                                             "probe.clientId=11");
        confirm.awaitLine("confirm: sending", DockerCluster.deadline(60));
        final long changesBefore = cluster.leadershipChanges();

        assertEquals(0, cluster.faulteron(leader, "attach").status(), "faulteron could not attach to node-" + leader);
        assertEquals(0, cluster.faulteron(leader, "block", "--peer", "node-" + cutOff).status(),
                     "faulteron could not cut node-" + leader + " off from node-" + cutOff);
        final long faultStart = System.nanoTime();
        Thread.sleep(TimeUnit.SECONDS.toMillis(3));
        final boolean pingDuring = cluster.ping(cutOff);

        // The fault window: the follower stays cut off, and the leader should not be deposed.
        final long remainingMs = TimeUnit.SECONDS.toMillis(FAULT_SECS) - (System.nanoTime() - faultStart) / 1_000_000;
        if (remainingMs > 0) {
            Thread.sleep(remainingMs);
        }
        final long changesDuring = cluster.leadershipChanges();
        final int leaderDuring = cluster.agreedLeader(DockerCluster.NO_MEMBER);
        final DockerCluster.Result rules = cluster.heal(leader);
        Files.writeString(logDir.resolve("rules.txt"), rules.output());
        final long dropped = DockerCluster.dropped(rules.output());

        boolean caughtUp = false;
        for (int attempt = 1; attempt <= PINGS_AFTER_HEAL && !caughtUp; attempt++) {
            caughtUp = cluster.ping(cutOff);
        }
        final boolean pingAfter = caughtUp;
        final int confirmStatus = confirm.awaitExit(DockerCluster.deadline(120));
        System.out.printf("leader %d, cut off from %d, producer on %d: dropped %d packet(s)%n"
                          + "leadership changes before/during: %d / %d, leader during: %d%n"
                          + "ping through node-%d during the fault: %b%nPendingSends producer: %s%n",
                          leader, cutOff, producerMember, dropped, changesBefore, changesDuring, leaderDuring, cutOff,
                          pingDuring, confirm.lastLine("confirm:"));

        assertAll(
            () -> assertTrue(dropped > 0, "nothing between node-" + leader + " and node-" + cutOff + " was dropped"),
            () -> assertTrue(changesDuring == changesBefore && leaderDuring == leader,
                             "the cut-off follower deposed the leader: changes " + changesBefore + " -> "
                                 + changesDuring + ", leader " + leader + " -> " + leaderDuring),
            () -> assertEquals(0, confirmStatus, "PendingSends producer: " + confirm.lastLine("confirm:")),
            () -> assertTrue(pingAfter, "node-" + cutOff + " did not catch up after the heal"));
    }
}
