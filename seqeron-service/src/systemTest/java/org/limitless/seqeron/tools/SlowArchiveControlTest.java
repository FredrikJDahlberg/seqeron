package org.limitless.seqeron.tools;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Every archive control message (schema 101) between the gateway host and
 * follower F2 is held DELAY_MS, in the direction DIRECTION names: {@code request} (delayed on the host) or
 * {@code response} (delayed on F2). The host is also cut from F1, so its relay connects to F2 under the delay. An
 * archive connect is two exchanges under one 1 s timeout: at DELAY_MS 500 or more the relay must give F2 up and
 * follow the leader, and well under it must follow F2, slowly. The producer on the host must see every frame once, in
 * order, a fresh follow on the host must catch up after the heal, and the host must log no FATAL.
 */
@Tag("docker")
class SlowArchiveControlTest {
    private static final String DIRECTION = System.getenv().getOrDefault("DIRECTION", "request");
    private static final int DELAY_MS = Integer.parseInt(System.getenv().getOrDefault("DELAY_MS", "1500"));
    private static final long FAULT_SECS = Long.parseLong(System.getenv().getOrDefault("FAULT_SECS", "10"));
    private static final String CONFIRM_COUNT = System.getenv().getOrDefault("CONFIRM_COUNT", "40000");
    private static final String CONFIRM_PACING_MICROS = System.getenv().getOrDefault("CONFIRM_PACING_MICROS", "500");
    private static final int ARCHIVE_MESSAGE_TIMEOUT_MS = 1000;
    private static final int CONNECT_EXCHANGES = 2;

    private DockerCluster cluster;
    private Path logDir;

    @BeforeEach
    void startCluster() {
        logDir = Path.of(System.getProperty("seqeron.systemTestLogs"),
                         "SlowArchiveControlTest-" + DIRECTION + "-" + DELAY_MS);
        cluster = new DockerCluster(logDir);
        cluster.start(Path.of(System.getProperty("seqeron.topologyGroups")));
    }

    @AfterEach
    void stopCluster() {
        cluster.close();
    }

    @Test
    @Timeout(value = 900, unit = TimeUnit.SECONDS)
    void relayGivesUpOnASlowArchiveOnlyPastItsConnectTimeout() throws Exception {
        assertTrue(DIRECTION.equals("request") || DIRECTION.equals("response"),
                   "DIRECTION is request or response: " + DIRECTION);
        final int leader = cluster.awaitLeader(DockerCluster.deadline(60), DockerCluster.NO_MEMBER);
        final int f1 = (leader + 1) % DockerCluster.MEMBERS;
        final int f2 = (leader + 2) % DockerCluster.MEMBERS;
        final boolean request = DIRECTION.equals("request");
        final int delayed = request ? DockerCluster.HOST : f2;
        final int peer = request ? f2 : DockerCluster.HOST;
        cluster.startGatewayHost(f1, f2, leader);
        final ClusterProcesses.Child confirm = cluster.probe("confirm", DockerCluster.HOST, "confirm",
                                                             "probe.count=" + CONFIRM_COUNT,
                                                             "probe.pacingMicros=" + CONFIRM_PACING_MICROS,
                                                             "probe.clientId=11");
        confirm.awaitLine("confirm: sending", DockerCluster.deadline(60));
        Thread.sleep(1000);

        assertEquals(0, cluster.faulteron(DockerCluster.HOST, "attach").status(), "faulteron could not attach to host");
        if (delayed != DockerCluster.HOST) {
            assertEquals(0, cluster.faulteron(delayed, "attach").status(),
                         "faulteron could not attach to node-" + delayed);
        }
        assertEquals(0, cluster.faulteron(delayed, "rule", "--peer", "node-" + peer, "--schema", "101", "--delay",
                                          "" + DELAY_MS).status(), "faulteron could not slow node-" + f2 + "'s archive");
        assertEquals(0, cluster.faulteron(DockerCluster.HOST, "block", "--peer", "node-" + f1).status(),
                     "faulteron could not cut the host from node-" + f1);
        Thread.sleep(TimeUnit.SECONDS.toMillis(FAULT_SECS));
        final StringBuilder rules = new StringBuilder(cluster.heal(DockerCluster.HOST).output());
        if (delayed != DockerCluster.HOST) {
            rules.append(cluster.heal(delayed).output());
        }
        Files.writeString(logDir.resolve("rules.txt"), rules.toString());

        final int confirmStatus = confirm.awaitExit(DockerCluster.deadline(120));
        final ClusterProcesses.Child fresh = cluster.probe("fresh-follower", DockerCluster.HOST, "follow",
                                                           "probe.clientId=9");
        boolean caught = true;
        try {
            fresh.awaitLine("following live", DockerCluster.deadline(30));
        } catch (final AssertionError ex) {
            caught = false;
        }
        fresh.kill();

        final List<String> sources = cluster.relaySources();
        final int expected = DELAY_MS * CONNECT_EXCHANGES >= ARCHIVE_MESSAGE_TIMEOUT_MS ? leader : f2;
        final boolean fatal = cluster.hostLog().contains("FATAL");
        final long dropped = DockerCluster.dropped(rules.toString());
        final long delayedCount = DockerCluster.counter(rules.toString(), "delayed");
        final boolean caughtUp = caught;
        System.out.printf("%s %d ms: leader %d, relay from %d then %d then %d%nrules: %s%n"
                          + "relay followed: %s%nrelay left: %s%nPendingSends producer: %s%n"
                          + "fresh follower caught up: %b%n",
                          DIRECTION, DELAY_MS, leader, f1, f2, leader, rules.toString().replace('\n', ' '), sources,
                          cluster.relayDepartures(), confirm.lastLine("confirm:"), caughtUp);

        assertAll(
            () -> assertTrue(dropped > 0, "the host dropped nothing from node-" + f1),
            () -> assertTrue(delayedCount > 0, "no archive control message was delayed"),
            () -> assertTrue(sources.size() >= 2 && sources.get(0).equals(DockerCluster.archiveEndpoint(f1))
                                 && sources.get(1).equals(DockerCluster.archiveEndpoint(expected)),
                             "the relay did not move from node-" + f1 + " to node-" + expected + ": " + sources),
            () -> assertEquals(0, confirmStatus, "PendingSends producer: " + confirm.lastLine("confirm:")),
            () -> assertTrue(caughtUp, "a fresh follower on the gateway host did not catch up"),
            () -> assertFalse(fatal, "the gateway host logged a FATAL"));
    }
}
