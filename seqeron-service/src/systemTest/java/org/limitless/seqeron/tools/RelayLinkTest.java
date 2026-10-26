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
 * faulteron/relay-link-test.sh: the link between the gateway host (node-3) and the member its relay reads is impaired,
 * chosen by FAULT. {@code loss} (default): LOSS_PCT% of the DATA between them and every NAK are dropped on the host,
 * and the relay must leave the member and resume on the next. {@code jitter}: the member's packets to the host are held
 * 20 ms plus up to 30 ms and 10% sent twice, which Aeron's image repairs, so the relay must stay on the member. The
 * relay tries follower F1, then F2, then the leader. Either way the producer on the host must see every frame once, in
 * order, a fresh follow on the host must catch up after the heal, and the host must log no FATAL.
 */
@Tag("docker")
class RelayLinkTest {
    private static final String FAULT = System.getenv().getOrDefault("FAULT", "loss");
    private static final String LOSS_PCT = System.getenv().getOrDefault("LOSS_PCT", "5");
    private static final long FAULT_SECS = Long.parseLong(System.getenv().getOrDefault("FAULT_SECS", "10"));
    private static final String CONFIRM_COUNT = System.getenv().getOrDefault("CONFIRM_COUNT", "40000");
    private static final String CONFIRM_PACING_MICROS = System.getenv().getOrDefault("CONFIRM_PACING_MICROS", "500");

    private DockerCluster cluster;
    private Path logDir;

    @BeforeEach
    void startCluster() {
        logDir = Path.of(System.getProperty("seqeron.systemTestLogs"), "RelayLinkTest-" + FAULT);
        cluster = new DockerCluster(logDir);
        cluster.start(Path.of(System.getProperty("seqeron.topologyGroups")));
    }

    @AfterEach
    void stopCluster() {
        cluster.close();
    }

    @Test
    @Timeout(value = 900, unit = TimeUnit.SECONDS)
    void relayMovesOnFromALossyLinkAndStaysOnAJitteryOne() throws Exception {
        assertTrue(FAULT.equals("loss") || FAULT.equals("jitter"), "FAULT is loss or jitter: " + FAULT);
        final boolean loss = FAULT.equals("loss");
        final int leader = cluster.awaitLeader(DockerCluster.deadline(60), DockerCluster.NO_MEMBER);
        final int f1 = (leader + 1) % DockerCluster.MEMBERS;
        final int f2 = (leader + 2) % DockerCluster.MEMBERS;
        cluster.startGatewayHost(f1, f2, leader);
        final ClusterProcesses.Child confirm = cluster.probe("confirm", DockerCluster.HOST, "confirm",
                                                             "probe.count=" + CONFIRM_COUNT,
                                                             "probe.pacingMicros=" + CONFIRM_PACING_MICROS,
                                                             "probe.clientId=11");
        confirm.awaitLine("confirm: sending", DockerCluster.deadline(60));
        Thread.sleep(1000);

        final int target = loss ? DockerCluster.HOST : f1;
        assertEquals(0, cluster.faulteron(target, "attach").status(), "faulteron could not attach to node-" + target);
        if (loss) {
            assertEquals(0, cluster.faulteron(target, "rule", "--peer", "node-" + f1, "--type", "data", "--loss",
                                              LOSS_PCT).status(), "faulteron could not drop the relay's DATA");
            assertEquals(0, cluster.faulteron(target, "block", "--peer", "node-" + f1, "--type", "nak").status(),
                         "faulteron could not block the relay's NAKs");
        } else {
            assertEquals(0, cluster.faulteron(target, "rule", "--peer", "node-" + DockerCluster.HOST, "--delay", "20",
                                              "--jitter", "30", "--duplicate", "10").status(),
                         "faulteron could not impair the relay's link");
        }
        Thread.sleep(TimeUnit.SECONDS.toMillis(FAULT_SECS));
        final DockerCluster.Result rules = cluster.heal(target);
        Files.writeString(logDir.resolve("rules.txt"), rules.output());

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
        final List<String> departures = cluster.relayDepartures();
        final boolean fatal = cluster.hostLog().contains("FATAL");
        final long droppedData = DockerCluster.dropped(rules.output(), "data");
        final long droppedNak = DockerCluster.dropped(rules.output(), "nak");
        final long delayed = DockerCluster.counter(rules.output(), "delayed");
        final long duplicated = DockerCluster.counter(rules.output(), "duplicated");
        final boolean caughtUp = caught;
        System.out.printf("%s: leader %d, relay from %d then %d, node-%d impaired%nrules: %s%n"
                          + "relay followed: %s%nrelay left: %s%nPendingSends producer: %s%n"
                          + "fresh follower caught up: %b%n",
                          FAULT, leader, f1, f2, target, rules.output().replace('\n', ' '), sources, departures,
                          confirm.lastLine("confirm:"), caughtUp);

        final String first = DockerCluster.archiveEndpoint(f1);
        assertAll(
            () -> {
                if (loss) {
                    assertTrue(droppedData > 0, "no data packet was dropped");
                    assertTrue(droppedNak > 0, "no nak packet was dropped");
                    // The reason is not asserted: the loss also holes F1's archive control stream, whose failure may
                    // come first.
                    assertTrue(sources.size() >= 2 && sources.get(0).equals(first)
                                   && sources.get(1).equals(DockerCluster.archiveEndpoint(f2)),
                               "the relay did not move from node-" + f1 + " to node-" + f2 + ": " + sources);
                } else {
                    assertTrue(delayed > 0, "no packet was delayed");
                    assertTrue(duplicated > 0, "no packet was duplicated");
                    assertEquals(List.of(first), sources, "the relay did not stay on node-" + f1);
                }
            },
            () -> assertEquals(0, confirmStatus, "PendingSends producer: " + confirm.lastLine("confirm:")),
            () -> assertTrue(caughtUp, "a fresh follower on the gateway host did not catch up"),
            () -> assertFalse(fatal, "the gateway host logged a FATAL"));
    }
}
