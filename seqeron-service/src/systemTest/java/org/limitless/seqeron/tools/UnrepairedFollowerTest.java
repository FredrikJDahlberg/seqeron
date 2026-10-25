package org.limitless.seqeron.tools;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.limitless.seqeron.protocol.PortLayout;

/**
 * faulteron/unrepaired-follower-test.sh: LOSS_PCT% of the log DATA sent to one follower's log port is dropped, and so
 * is one half of Aeron's repair, chosen by REPAIR: {@code nak} (every NAK from that port, on the follower) or
 * {@code retransmit} (every retransmission to that port, on the leader). The follower stops applying the log while
 * still hearing the leader, so no election is called. Its queue must fall behind under the fault, the producer on the
 * other follower must see every frame once, in order, and a follow cold-started under the fault and another after
 * the heal must each end caught up rather than evicted.
 */
@Tag("docker")
class UnrepairedFollowerTest {
    private static final String REPAIR = System.getenv().getOrDefault("REPAIR", "nak");
    private static final String LOSS_PCT = System.getenv().getOrDefault("LOSS_PCT", "5");
    private static final long FAULT_SECS = Long.parseLong(System.getenv().getOrDefault("FAULT_SECS", "20"));
    private static final String CONFIRM_COUNT = System.getenv().getOrDefault("CONFIRM_COUNT", "80000");
    private static final String CONFIRM_PACING_MICROS = System.getenv().getOrDefault("CONFIRM_PACING_MICROS", "500");
    private static final int PINGS_AFTER_HEAL = 5;

    private DockerCluster cluster;
    private Path logDir;

    @BeforeEach
    void startCluster() {
        logDir = Path.of(System.getProperty("seqeron.systemTestLogs"), "UnrepairedFollowerTest");
        cluster = new DockerCluster(logDir);
        cluster.start(Path.of(System.getProperty("seqeron.topologyGroups")));
    }

    @AfterEach
    void stopCluster() {
        cluster.close();
    }

    @Test
    @Timeout(value = 900, unit = TimeUnit.SECONDS)
    void starvedFollowerFallsBehindAndCatchesUpOnHeal() throws Exception {
        assertTrue(REPAIR.equals("nak") || REPAIR.equals("retransmit"), "REPAIR is nak or retransmit: " + REPAIR);
        final int leader = cluster.awaitLeader(DockerCluster.deadline(60), DockerCluster.NO_MEMBER);
        final int starved = (leader + 1) % DockerCluster.MEMBERS;
        final int producerMember = (leader + 2) % DockerCluster.MEMBERS;
        final int target = REPAIR.equals("nak") ? starved : leader;
        final ClusterProcesses.Child confirm = cluster.probe("confirm", producerMember, "confirm",
                                                             "probe.count=" + CONFIRM_COUNT,
                                                             "probe.pacingMicros=" + CONFIRM_PACING_MICROS,
                                                             "probe.clientId=11");
        confirm.awaitLine("confirm: sending", DockerCluster.deadline(60));
        final boolean pingBefore = cluster.ping(starved);

        final String logPort = "" + PortLayout.logPort(starved);
        assertEquals(0, cluster.faulteron(target, "attach").status(), "faulteron could not attach to node-" + target);
        assertEquals(0, cluster.faulteron(target, "rule", "--type", "data", "--loss", LOSS_PCT, logPort).status(),
                     "faulteron could not drop log DATA to node-" + starved);
        assertEquals(0, cluster.faulteron(target, "block", "--type", REPAIR, logPort).status(),
                     "faulteron could not block " + REPAIR + " on port " + logPort);
        final long faultStart = System.nanoTime();
        Thread.sleep(TimeUnit.SECONDS.toMillis(3));

        final boolean pingDuring = cluster.ping(starved);
        final ClusterProcesses.Child followDuring = cluster.probe("follow-during", starved, "follow",
                                                                  "probe.clientId=12");
        final long remainingMs = TimeUnit.SECONDS.toMillis(FAULT_SECS) - (System.nanoTime() - faultStart) / 1_000_000;
        if (remainingMs > 0) {
            Thread.sleep(remainingMs);
        }
        final DockerCluster.Result rules = cluster.heal(target);
        Files.writeString(logDir.resolve("rules.txt"), rules.output());
        final long healed = System.nanoTime();

        // A heal takes the retransmit of everything the hole held back, so the first ping may not be the one that passes.
        int attempts = 0;
        boolean caughtUp = false;
        while (attempts < PINGS_AFTER_HEAL && !caughtUp) {
            attempts++;
            caughtUp = cluster.ping(starved);
        }
        final boolean pingAfter = caughtUp;
        final long afterHealSecs = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - healed);
        final ClusterProcesses.Child followAfter = cluster.probe("follow-after", starved, "follow",
                                                                 "probe.clientId=13");

        final boolean[] followed = new boolean[2];
        cluster.poll(DockerCluster.deadline(30), () -> {
            followed[0] = followCaughtUp(followDuring);
            followed[1] = followCaughtUp(followAfter);
            return followed[0] && followed[1];
        });
        followDuring.kill();
        followAfter.kill();

        final int confirmStatus = confirm.awaitExit(DockerCluster.deadline(120));
        final int finalLeader = cluster.pollLeader(DockerCluster.deadline(5), DockerCluster.NO_MEMBER);
        final long droppedData = DockerCluster.dropped(rules.output(), "data");
        final long droppedRepair = DockerCluster.dropped(rules.output(), REPAIR);
        System.out.printf("leader %d, starving follower %d (%s on node-%d), producer on %d: dropped %d data, %d %s%n"
                          + "ping before/during/after: %b / %b / %b (attempt %d, %d s after the heal)%n"
                          + "follow during/after: %b / %b, leader after: %d%nPendingSends producer: %s%n",
                          leader, starved, REPAIR, target, producerMember, droppedData, droppedRepair, REPAIR,
                          pingBefore, pingDuring, pingAfter, attempts, afterHealSecs, followed[0], followed[1],
                          finalLeader, confirm.lastLine("confirm:"));

        assertAll(
            () -> assertTrue(droppedData > 0, "no data packet was dropped"),
            () -> assertTrue(droppedRepair > 0, "no " + REPAIR + " packet was dropped"),
            () -> assertTrue(pingBefore, "node-" + starved + "'s queue did not answer a ping before the fault"),
            () -> assertTrue(!pingDuring, "node-" + starved + "'s queue stayed current under the fault"),
            () -> assertEquals(0, confirmStatus, "PendingSends producer: " + confirm.lastLine("confirm:")),
            () -> assertTrue(pingAfter, "node-" + starved + " did not catch up after the heal"),
            () -> assertTrue(followed[0], "the follow cold-started under the fault did not end caught up; see "
                                              + followDuring.log()),
            () -> assertTrue(followed[1], "the follow cold-started after the heal did not catch up; see "
                                              + followAfter.log()));
    }

    /** Whether the follow's last word is that it is caught up rather than evicted from its queue. */
    private static boolean followCaughtUp(final ClusterProcesses.Child follow) {
        try {
            String last = "";
            for (final String line : Files.readAllLines(follow.log(), StandardCharsets.UTF_8)) {
                if (line.contains("following live") || line.contains("re-converged")
                    || line.contains("evicted from the queue")) {
                    last = line;
                }
            }
            return last.contains("following live") || last.contains("re-converged");
        } catch (final IOException ex) {
            return false;
        }
    }
}
