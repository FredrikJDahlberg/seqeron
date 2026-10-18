package org.limitless.seqeron.tools;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

/**
 * failover-test.sh's confirm producer in one JVM: three members, a producer co-located with a follower sending
 * through {@code PendingSends}, and the leader stopped mid-stream. The producer's queue must show every frame
 * exactly once, in order, and once the old leader is back, every member's queue recordings must be one log.
 */
@ExtendWith(InterruptingTestCallback.class)
class FailoverConfirmTest {
    private static final int COUNT = 5_000;
    private static final long PACING_NS = TimeUnit.MICROSECONDS.toNanos(100); // failover-test.sh's

    @RegisterExtension
    final SystemTestWatcher watcher = new SystemTestWatcher();

    @TempDir
    Path baseDir;

    private InProcessCluster cluster;

    @BeforeEach
    void startCluster() {
        watcher.dataCollector().add(baseDir);
        cluster = new InProcessCluster(baseDir);
        cluster.startAll();
    }

    @AfterEach
    void stopCluster() {
        cluster.close();
    }

    @Test
    @InterruptAfter(120)
    void pendingSendsShowsEveryFrameOnceAcrossALeaderStop() throws Exception {
        final int leader = cluster.awaitLeader();
        final int member = (leader + 1) % InProcessCluster.MEMBERS;
        CompletableFuture<Void> stopping = null;
        try (ConfirmProducer producer = new ConfirmProducer(cluster.aeronDir(member), member, COUNT, PACING_NS)) {
            while (!producer.isDrained() && !producer.isStalled()) {
                producer.doWork();
                if (stopping == null && producer.sent() > COUNT / 2) {
                    stopping = CompletableFuture.runAsync(() -> cluster.stop(leader));
                }
                Tests.yield();
            }
            stopping.get(30, TimeUnit.SECONDS);

            // Reported rather than asserted, as failover-test.sh does: a stop can land between two frames.
            System.out.printf("member %d stopped, member %d leads; resent %d frame(s)%n", leader, producer.lastLeader(),
                              producer.resent());
            assertTrue(producer.isDrained(),
                       "PendingSends still held " + producer.held() + " frame(s) after the drain timeout");
            assertNotEquals(leader, producer.lastLeader(), "no new leader took over from member " + leader);
            producer.assertExactlyOnceInOrder();

            cluster.start(leader); // its recording rebuilds over the full-log replay
            QueueRecordings.assertOneLog(cluster.archives(), producer.lastClusterSeqNo());
        }
        assertFalse(cluster.fatal(), "a member raised a fatal");
        assertFalse(cluster.terminated(), "a member terminated");
    }
}
