package org.limitless.seqeron.tools;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * faulteron/throttled-leader-test.sh: the leader's container is capped at CPUS CPUs while the cluster streams
 * ({@code docker update --cpus}), a gray failure no kill or network fault produces. Either the cluster replaces the
 * starved leader or it keeps leading, and either is correct. The producer on a follower must see every frame once, in
 * order; with its CPU back the old leader must rejoin and its queue catch up; and a follow cold-started on it at the
 * fault's start, its Replayer answering slower than the client's 500 ms resend, must end caught up with no control
 * reply dropped.
 */
@Tag("docker")
class ThrottledLeaderTest {
    private static final String CPUS = System.getenv().getOrDefault("CPUS", "0.05");
    private static final long FAULT_SECS = Long.parseLong(System.getenv().getOrDefault("FAULT_SECS", "20"));
    private static final String CONFIRM_COUNT = System.getenv().getOrDefault("CONFIRM_COUNT", "60000");
    private static final String CONFIRM_PACING_MICROS = System.getenv().getOrDefault("CONFIRM_PACING_MICROS", "500");
    private static final String LEADER = "seqeron.sequencer.currentLeaderMemberId";
    private static final String CONTROL_REPLIES_DROPPED = "seqeron.replayer.controlRepliesDroppedCount";
    private static final int PINGS_AFTER_HEAL = 5;

    private DockerCluster cluster;

    @BeforeEach
    void startCluster() {
        cluster = new DockerCluster(Path.of(System.getProperty("seqeron.systemTestLogs"), "ThrottledLeaderTest"));
        cluster.start(Path.of(System.getProperty("seqeron.topologyGroups")));
    }

    @AfterEach
    void stopCluster() {
        cluster.close();
    }

    @Test
    @Timeout(value = 900, unit = TimeUnit.SECONDS)
    void starvedLeaderIsReplacedOrKeepsLeadingAndRecovers() throws Exception {
        final int old = cluster.awaitLeader(DockerCluster.deadline(60), DockerCluster.NO_MEMBER);
        final int producerMember = (old + 1) % DockerCluster.MEMBERS;
        final ClusterProcesses.Child confirm = cluster.probe("confirm", producerMember, "confirm",
                                                             "probe.count=" + CONFIRM_COUNT,
                                                             "probe.pacingMicros=" + CONFIRM_PACING_MICROS,
                                                             "probe.clientId=11");
        confirm.awaitLine("confirm: sending", DockerCluster.deadline(60));

        cluster.throttle(old, CPUS);
        final long faultStart = System.nanoTime();
        final String cap = cluster.cpuMax(old);
        final ClusterProcesses.Child follow = cluster.probe("follow", old, "follow", "probe.clientId=12");

        final int newLeader = cluster.pollLeader(DockerCluster.deadline(30), old);
        final boolean replaced = newLeader != DockerCluster.NO_MEMBER;
        final boolean stillLeading = !replaced && cluster.pollLeader(DockerCluster.deadline(5),
                                                                     DockerCluster.NO_MEMBER) == old;
        final long remainingMs = TimeUnit.SECONDS.toMillis(FAULT_SECS) - (System.nanoTime() - faultStart) / 1_000_000;
        if (remainingMs > 0) {
            Thread.sleep(remainingMs);
        }
        final boolean running = cluster.isRunning(old);
        cluster.unthrottle(old);

        boolean rejoined = false;
        boolean caughtUp = false;
        if (running && (replaced
                            ? cluster.poll(DockerCluster.deadline(60), () -> cluster.counter(old, LEADER) == newLeader)
                            : stillLeading)) {
            rejoined = true;
            for (int attempt = 1; attempt <= PINGS_AFTER_HEAL && !caughtUp; attempt++) {
                caughtUp = cluster.ping(old);
            }
        }
        final int confirmStatus = confirm.awaitExit(DockerCluster.deadline(120));
        final boolean followed = cluster.poll(DockerCluster.deadline(30), () -> followCaughtUp(follow));
        follow.kill();
        final long controlRepliesDropped = cluster.counter(old, CONTROL_REPLIES_DROPPED);

        System.out.printf("node-%d capped at %s CPUs (cpu.max %s), producer on %d%n"
                          + "new leader: %s, old leader running: %b, rejoined / caught up: %b / %b%n"
                          + "follow on the old leader: caught up %b, control replies dropped %d%n"
                          + "PendingSends producer: %s%n",
                          old, CPUS, cap, producerMember,
                          replaced ? "member " + newLeader : stillLeading ? "none (node-" + old + " still leads)" : "none",
                          running, rejoined, caughtUp, followed, controlRepliesDropped, confirm.lastLine("confirm:"));

        final boolean rejoinedFinal = rejoined;
        final boolean caughtUpFinal = caughtUp;
        assertAll(
            () -> assertEquals(Math.round(Double.parseDouble(CPUS) * 100_000) + " 100000", cap,
                               "the cap was not applied"),
            () -> assertTrue(replaced || stillLeading, "no member leads"),
            () -> assertEquals(0, confirmStatus, "PendingSends producer: " + confirm.lastLine("confirm:")),
            () -> assertTrue(rejoinedFinal && caughtUpFinal,
                             "node-" + old + " did not rejoin, or its queue did not catch up"),
            () -> assertTrue(followed, "the follow cold-started on node-" + old + " did not catch up; see "
                                           + follow.log()),
            () -> assertEquals(0, controlRepliesDropped, "node-" + old + "'s Replayer dropped control replies"));
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
