package org.limitless.seqeron.tools;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.limitless.seqeron.protocol.PortLayout;

/**
 * Everything to or from the leader's internal ports or its ingress port leaves
 * DELAY_MS plus up to JITTER_MS late, which reorders it, and DUPLICATE_PCT% of it is delivered twice. The worst case
 * stays under the 200 ms leader heartbeat timeout, so a slow link is not a dead one: no election runs, every member's
 * queue stays current, and the producer sending through {@code PendingSends} on a follower sees every frame once, in
 * order.
 */
@Tag("docker")
class JitteryLeaderTest {
    private static final String DELAY_MS = System.getenv().getOrDefault("DELAY_MS", "20");
    private static final String JITTER_MS = System.getenv().getOrDefault("JITTER_MS", "30");
    private static final String DUPLICATE_PCT = System.getenv().getOrDefault("DUPLICATE_PCT", "10");
    private static final long FAULT_SECS = Long.parseLong(System.getenv().getOrDefault("FAULT_SECS", "20"));
    private static final String CONFIRM_COUNT = System.getenv().getOrDefault("CONFIRM_COUNT", "60000");
    private static final String CONFIRM_PACING_MICROS = System.getenv().getOrDefault("CONFIRM_PACING_MICROS", "500");

    private DockerCluster cluster;
    private Path logDir;

    @BeforeEach
    void startCluster() {
        logDir = Path.of(System.getProperty("seqeron.systemTestLogs"), "JitteryLeaderTest");
        cluster = new DockerCluster(logDir);
        cluster.start(Path.of(System.getProperty("seqeron.topologyGroups")));
    }

    @AfterEach
    void stopCluster() {
        cluster.close();
    }

    @Test
    @Timeout(value = 900, unit = TimeUnit.SECONDS)
    void slowDuplicatingLeaderIsNotDeposed() throws Exception {
        final int leader = cluster.awaitLeader(DockerCluster.deadline(60), DockerCluster.NO_MEMBER);
        final int producerMember = (leader + 1) % DockerCluster.MEMBERS;
        final ClusterProcesses.Child confirm = cluster.probe("confirm", producerMember, "confirm",
                                                             "probe.count=" + CONFIRM_COUNT,
                                                             "probe.pacingMicros=" + CONFIRM_PACING_MICROS,
                                                             "probe.clientId=11");
        confirm.awaitLine("confirm: sending", DockerCluster.deadline(60));
        final long changesBefore = cluster.leadershipChanges();

        assertEquals(0, cluster.faulteron(leader, "attach").status(), "faulteron could not attach to node-" + leader);
        final List<String> rule = new ArrayList<>(List.of("rule", "--delay", DELAY_MS, "--jitter", JITTER_MS,
                                                          "--duplicate", DUPLICATE_PCT));
        rule.addAll(List.of(DockerCluster.internalPorts()));
        rule.add("" + PortLayout.ingressPort(leader));
        assertEquals(0, cluster.faulteron(leader, rule.toArray(String[]::new)).status(),
                     "faulteron could not impair node-" + leader);
        final long faultStart = System.nanoTime();
        Thread.sleep(TimeUnit.SECONDS.toMillis(3));

        final StringBuilder pings = new StringBuilder();
        for (int m = 0; m < DockerCluster.MEMBERS; m++) {
            if (cluster.ping(m)) {
                pings.append(m).append(' ');
            }
        }
        final long remainingMs = TimeUnit.SECONDS.toMillis(FAULT_SECS) - (System.nanoTime() - faultStart) / 1_000_000;
        if (remainingMs > 0) {
            Thread.sleep(remainingMs);
        }
        final DockerCluster.Result rules = cluster.heal(leader);
        Files.writeString(logDir.resolve("rules.txt"), rules.output());
        final long changesAfter = cluster.leadershipChanges();

        final int confirmStatus = confirm.awaitExit(DockerCluster.deadline(120));
        final long delayed = DockerCluster.counter(rules.output(), "delayed");
        final long duplicated = DockerCluster.counter(rules.output(), "duplicated");
        System.out.printf("leader %d, producer on %d: delayed %d, duplicated %d%n"
                          + "leadership changes before/after: %d / %d%n"
                          + "queues answering a ping under the fault: %s%nPendingSends producer: %s%n",
                          leader, producerMember, delayed, duplicated, changesBefore, changesAfter, pings,
                          confirm.lastLine("confirm:"));

        assertAll(
            () -> assertTrue(delayed > 0 && duplicated > 0, "the fault did not both delay and duplicate"),
            () -> assertEquals(changesBefore, changesAfter, "an election ran under the fault"),
            () -> assertEquals("0 1 2 ", pings.toString(), "not every queue answered a ping under the fault"),
            () -> assertEquals(0, confirmStatus, "PendingSends producer: " + confirm.lastLine("confirm:")));
    }
}
