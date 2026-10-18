package org.limitless.seqeron.tools;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.RandomWatcher;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import java.nio.file.Path;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

/**
 * Seeded faults against three members while a producer on a gateway host streams through {@code PendingSends}. Each
 * round stops a member and restarts it, or cuts its UDP and restores it: the leader or a follower, as the round draws.
 * The producer's queue, relayed from whichever member the host reaches, must show every frame exactly once, in order,
 * every member must be up at the end, and every queue recording on every member and on the host must be one log. The
 * seed fixes each round's draws, not which member leads when it runs; {@code -Pseed=<n>} replays them.
 */
@ExtendWith(InterruptingTestCallback.class)
class FailoverPropertyTest {
    private static final int ROUNDS = 6;
    private static final long PACING_NS = TimeUnit.MICROSECONDS.toNanos(200);
    private static final int MIN_FAULT_MS = 300;
    private static final int MAX_FAULT_MS = 1500;
    /** After a member returns, before the next round: long enough for it to catch up on what it missed. */
    private static final long SETTLE_MS = 500;

    @RegisterExtension
    final RandomWatcher random = new RandomWatcher(Long.getLong("seqeron.seed", System.nanoTime()));

    @RegisterExtension
    final SystemTestWatcher watcher = new SystemTestWatcher();

    @TempDir
    Path baseDir;

    private final ExecutorService faultThread = Executors.newSingleThreadExecutor();
    private final List<String> faultLog = new CopyOnWriteArrayList<>();
    private InProcessCluster cluster;

    @BeforeEach
    void startCluster() {
        watcher.dataCollector().add(baseDir);
        // Aeron Cluster replays the log to a follower catching up on a publication that reuses the log's session id.
        // Asked again while an earlier replay to that follower still exists, the driver refuses it as a clash, and the
        // follower asks again. Seen once in 21 runs, each of which still came out exact.
        watcher.ignoreErrorsMatching(s -> s.contains("clashing sessionId") && s.contains("alias=catchup-followerId-"));
        cluster = new InProcessCluster(baseDir);
        cluster.startAll();
    }

    @AfterEach
    void stopCluster() throws InterruptedException {
        faultThread.shutdownNow();
        faultThread.awaitTermination(30, TimeUnit.SECONDS);
        cluster.close();
    }

    @Test
    @InterruptAfter(300)
    void pendingSendsShowsEveryFrameOnceAcrossSeededFaults() throws Exception {
        final Random rng = random.random();
        cluster.awaitLeader();
        cluster.startGatewayHost();
        try (ConfirmProducer producer =
                 new ConfirmProducer(cluster.aeronDir(InProcessCluster.GATEWAY_HOST), InProcessCluster.GATEWAY_HOST,
                                     Integer.MAX_VALUE, PACING_NS)) {
            final Future<?> faults = faultThread.submit(() -> injectFaults(rng));
            while (!faults.isDone() && !producer.isStalled()) {
                producer.doWork();
                Tests.yield();
            }
            producer.stopSending();
            while (!producer.isDrained() && !producer.isStalled()) {
                producer.doWork();
                Tests.yield();
            }

            faultLog.forEach(System.out::println);
            System.out.printf("producer on the gateway host sent %d frame(s); resent %d%n", producer.sent(),
                              producer.resent());
            faults.get();
            assertTrue(producer.isDrained(),
                       "PendingSends still held " + producer.held() + " frame(s) after the drain timeout");
            producer.assertExactlyOnceInOrder();
            producer.assertOtherQueueShowsEveryEvenFrame();
            QueueRecordings.assertOneLog(cluster.archives(), producer.lastClusterSeqNo());
        }
        for (int m = 0; m < InProcessCluster.MEMBERS; m++) {
            assertTrue(cluster.isRunning(m), "member " + m + " is down");
        }
        assertFalse(cluster.fatal(), "a member raised a fatal");
        assertFalse(cluster.terminated(), "a member terminated");
    }

    /** One fault per round, each healed and settled before the next, so a quorum is always one fault away. */
    private void injectFaults(final Random rng) {
        for (int round = 1; round <= ROUNDS; round++) {
            final int leader = cluster.awaitLeader();
            final boolean drawsLeader = rng.nextBoolean();
            final int follower = (leader + 1 + rng.nextInt(2)) % InProcessCluster.MEMBERS;
            final int target = drawsLeader ? leader : follower;
            final boolean stop = rng.nextBoolean();
            final int faultMs = MIN_FAULT_MS + rng.nextInt(MAX_FAULT_MS - MIN_FAULT_MS);
            faultLog.add(String.format("round %d: %s member %d (%s) for %dms", round, stop ? "stop" : "cut", target,
                                       target == leader ? "leader" : "follower", faultMs));
            if (stop) {
                cluster.stop(target);
                Tests.sleep(faultMs);
                cluster.start(target);
            } else {
                cluster.cut(target, true);
                Tests.sleep(faultMs);
                cluster.cut(target, false);
            }
            Tests.sleep(SETTLE_MS);
        }
        cluster.awaitLeader();
    }
}
