package org.limitless.seqeron.tools;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
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

/**
 * paused-subscriber-test.sh: a queue subscriber that stops polling must not take its member down. A co-located follow
 * on member 0 is SIGSTOPped and more than two queue windows of frames are flooded through member 1. Until the driver
 * evicts it, the stopped subscriber holds the queue's window, and a full window behind it back-pressures the
 * sequencer, which terminates after 5 s of back-pressure with no recording progress. seqeron's drivers evict well
 * inside that; at Aeron's defaults the eviction takes about 10 s and this fails. Every member must stay up, member 0
 * must log no queue-stall fatal, and the resumed follower must heal the hole its eviction left and re-converge.
 */
class PausedSubscriberTest {
    private static final int FLOOD_FRAMES = 2000;
    // More than two 8 MiB windows of the default 16 MiB IPC term.
    private static final int FLOOD_FILLER_BYTES = 8800;
    private static final long PAUSE_SECS = Long.parseLong(System.getenv().getOrDefault("PAUSE_SECS", "4"));
    // An evicted subscriber rests this long before it may rejoin the queue.
    private static final long EVICTION_REST_SECS = 12;
    private static final int FOLLOWER_MEMBER = 0;

    @TempDir
    Path baseDir;

    private ClusterProcesses cluster;
    private ClusterProcesses.Child follower;

    @BeforeEach
    void startCluster(final TestInfo test) {
        cluster = new ClusterProcesses(baseDir, Path.of(System.getProperty("seqeron.systemTestLogs"),
                                                        test.getTestClass().orElseThrow().getSimpleName()));
        cluster.startAll();
        cluster.loadTopology(Path.of(System.getProperty("seqeron.topologyGroups")));
    }

    @AfterEach
    void stopCluster() {
        if (follower != null) {
            follower.resume();
        }
        cluster.close();
    }

    @Test
    @Timeout(value = 300, unit = TimeUnit.SECONDS)
    void stoppedFollowerIsEvictedAndHealsWithoutTakingAMemberDown() throws Exception {
        follower = cluster.probe("follower", FOLLOWER_MEMBER, "follow", "probe.clientId=9");
        follower.awaitLine("following live", Duration.ofSeconds(30));

        follower.pause();
        cluster.probe("flood", 1, "submit", "probe.count=" + FLOOD_FRAMES, "probe.fillerBytes=" + FLOOD_FILLER_BYTES)
            .awaitExit(Duration.ofSeconds(120));
        Thread.sleep(TimeUnit.SECONDS.toMillis(PAUSE_SECS));

        follower.resume();
        Thread.sleep(TimeUnit.SECONDS.toMillis(EVICTION_REST_SECS));
        cluster.probe("after", 1, "submit", "probe.count=100", "probe.pacingMicros=10000")
            .awaitExit(Duration.ofSeconds(60));
        final boolean healed = poll(() -> !follower.lines("re-converged at clusterSeqNo").isEmpty(), 15);

        final int fatals = cluster.memberLog(FOLLOWER_MEMBER).lines("FATAL: the queue recording made no progress")
            .size();
        final int reconvergences = follower.lines("re-converged at clusterSeqNo").size();
        System.out.printf("queue-stall fatals on member %d: %d; follower re-convergences: %d%n", FOLLOWER_MEMBER,
                          fatals, reconvergences);

        assertAll(
            () -> {
                for (int m = 0; m < ClusterProcesses.MEMBERS; m++) {
                    assertTrue(cluster.isRunning(m), "member " + m + " went down");
                }
            },
            () -> assertEquals(0, fatals, "member " + FOLLOWER_MEMBER + " logged a queue-stall fatal"),
            () -> assertTrue(healed && reconvergences >= 1, "the follower did not re-converge: "
                                                              + follower.lines("queue gap")));
    }

    private static boolean poll(final java.util.function.BooleanSupplier condition, final int seconds)
        throws InterruptedException {
        final long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > end) {
                return false;
            }
            Thread.sleep(500);
        }
        return true;
    }
}
