package org.limitless.seqeron.tools;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * gap-recovery-test.sh: a consumer that is caught up, following live off its co-located member's queue, must heal a
 * dropped live queue frame: detect the clusterSeqNo gap, resume the member's recording at the hole, and keep
 * delivering. Members 1 and 2 start first so one of them leads, and member 0, the consumer's, is never killed. After a
 * leader failover the consumer is armed over SIGUSR1 to drop the next GAP_SIZE live frames, and a paced flood is
 * submitted. The pacing is load-bearing: an unpaced probe outruns the heal, every post-gap frame then arrives by
 * replay, and the delivered-while-caught-up count collapses.
 */
class GapRecoveryTest {
    private static final int FLOOD_FRAMES = 300;
    private static final int FLOOD_PACING_MICROS = 10_000;
    private static final int DELIVER_THRESHOLD = 120;
    private static final int CONSUMER_MEMBER = 0;
    private static final String GAP_SIZE = System.getenv().getOrDefault("GAP_SIZE", "1");
    private static final Pattern DELIVERED = Pattern.compile("n=(\\d+)");

    @TempDir
    Path baseDir;

    private ClusterProcesses cluster;

    @BeforeEach
    void startCluster(final TestInfo test) {
        cluster = new ClusterProcesses(baseDir, Path.of(System.getProperty("seqeron.systemTestLogs"),
                                                        test.getTestClass().orElseThrow().getSimpleName()));
    }

    @AfterEach
    void stopCluster() {
        cluster.close();
    }

    @Test
    @Timeout(value = 300, unit = TimeUnit.SECONDS)
    void consumerHealsADroppedLiveFrameAndKeepsDelivering() throws Exception {
        cluster.startMember(1, "seq-1");
        cluster.startMember(2, "seq-2");
        cluster.awaitActive();
        final int leader = cluster.awaitLeader();
        cluster.startMember(CONSUMER_MEMBER, "seq-" + CONSUMER_MEMBER);
        cluster.awaitActive();
        cluster.loadTopology(Path.of(System.getProperty("seqeron.topologyGroups")));

        // The fault-injection flag installs the SIGUSR1 handler; without it a stray signal would kill the process.
        final ClusterProcesses.Child consumer = cluster.probe("consumer", CONSUMER_MEMBER, "follow",
                                                              "probe.clientId=9", "probe.latencyStats=true",
                                                              "probe.faultInjection=true",
                                                              "probe.faultDropCount=" + GAP_SIZE);
        consumer.awaitLine("following live", Duration.ofSeconds(30));

        cluster.kill(leader);
        final int newLeader = cluster.awaitLeader();
        // Let the new tenure's recording start and settle.
        Thread.sleep(3000);

        consumer.signal("USR1");
        Thread.sleep(500);
        cluster.probe("flood", CONSUMER_MEMBER, "submit", "probe.count=" + FLOOD_FRAMES,
                      "probe.pacingMicros=" + FLOOD_PACING_MICROS).awaitExit(Duration.ofSeconds(60));
        Thread.sleep(8000);

        // SIGTERM flushes the delivery-latency report; wait for the exit before the teardown.
        consumer.terminate(Duration.ofSeconds(20));
        final int recoveries = consumer.lines("queue gap: expected groupSeqNo").size();
        int delivered = 0;
        for (final String line : consumer.lines("n=")) {
            final Matcher matcher = DELIVERED.matcher(line);
            if (matcher.find()) {
                delivered = Integer.parseInt(matcher.group(1));
                break;
            }
        }
        final int deliveredCount = delivered;
        System.out.printf("leader %d -> %d, gap size %s: %d recoveries, %d frames delivered (threshold %d)%n%s%n",
                          leader, newLeader, GAP_SIZE, recoveries, deliveredCount, DELIVER_THRESHOLD,
                          consumer.lines("queue gap"));

        assertAll(
            () -> assertTrue(recoveries >= 1, "the consumer logged no queue gap: the drop did not take effect"),
            () -> assertTrue(deliveredCount >= DELIVER_THRESHOLD, "the consumer delivered only " + deliveredCount
                                                                    + " frames after the gap"));
    }
}
