package org.limitless.seqeron.tools;

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
import org.limitless.seqeron.protocol.SeqeronCounters;

/**
 * A tethered queue subscriber in a child JVM, {@code SIGSTOP}ped on the leader under a flood, holds the leader's queue
 * a window behind it (doc/plans/queue-off-duty-cycle-plan.md, step 6). The leader must sequence on while its queue is
 * held: its {@code clusterSeqNo} advances, a ping through another member's queue is echoed and the flood completes.
 * Its queue agent then skips the queue, every member stays up, and a consumer of the leader's queue heals by replay.
 */
class StoppedHolderTest {
    /** Under Aeron's 10 s client liveness timeout, which would otherwise drop the stopped holder first. */
    private static final int STALL_TIMEOUT_MS = 3_000;
    private static final int FLOOD_FRAMES = 2000;
    // More than two 8 MiB windows of the default 16 MiB IPC term.
    private static final int FLOOD_FILLER_BYTES = 8800;

    @TempDir Path baseDir;

    private ClusterProcesses cluster;
    private ClusterProcesses.Child holder;

    @BeforeEach
    void startCluster(final TestInfo test) {
        cluster = new ClusterProcesses(
            baseDir,
            Path.of(System.getProperty("seqeron.systemTestLogs"), test.getTestClass().orElseThrow().getSimpleName()));
        cluster.startAll("sequencer.queueStallTimeoutMs=" + STALL_TIMEOUT_MS);
        cluster.loadTopology(Path.of(System.getProperty("seqeron.topologyGroups")));
    }

    @AfterEach
    void stopCluster() {
        if (holder != null) {
            holder.resume();
        }
        cluster.close();
    }

    @Test
    @Timeout(value = 300, unit = TimeUnit.SECONDS)
    void aStoppedTetheredSubscriberIsSkippedWhileTheLeaderSequencesOn() {
        final int leader = cluster.awaitLeader();
        final int other = (leader + 1) % ClusterProcesses.MEMBERS;
        final ClusterProcesses.Child follower = cluster.probe("follower", leader, "follow", "probe.clientId=9");
        follower.awaitLine("following live", Duration.ofSeconds(30));
        holder = cluster.probe("holder", leader, "hold");
        holder.awaitLine("holding group", Duration.ofSeconds(30));

        holder.pause();
        final ClusterProcesses.Child flood = cluster.probe("flood", other, "submit", "probe.count=" + FLOOD_FRAMES,
                                                           "probe.fillerBytes=" + FLOOD_FILLER_BYTES);
        cluster.await("the leader's queue held", Duration.ofSeconds(60), () -> isStalled(leader));
        final long heldAt = clusterSeqNo(leader);
        cluster.await("the leader's clusterSeqNo advancing past " + heldAt + " while its queue is held",
                      Duration.ofSeconds(2), () -> clusterSeqNo(leader) > heldAt);
        assertTrue(isStalled(leader), "the leader's queue was skipped before its clusterSeqNo advanced");
        assertEquals(0, cluster.probe("ping", other, "ping").awaitExit(Duration.ofSeconds(60)),
                     "a ping through member " + other + "'s queue was not echoed");
        assertEquals(0, flood.awaitExit(Duration.ofSeconds(120)), "the flood failed");

        cluster.memberLog(leader).awaitLine("SKIPPED: group", Duration.ofSeconds(30));
        holder.resume();
        assertEquals(0,
                     cluster.probe("after", other, "submit", "probe.count=100", "probe.pacingMicros=10000")
                         .awaitExit(Duration.ofSeconds(60)),
                     "the frames after the skip failed");
        follower.awaitLine("re-converged at clusterSeqNo", Duration.ofSeconds(30));
        for (int m = 0; m < ClusterProcesses.MEMBERS; m++) {
            assertTrue(cluster.isRunning(m), "member " + m + " went down");
        }
    }

    private boolean isStalled(final int m) {
        return cluster.counter(m, SeqeronCounters.SEQUENCER_QUEUE_STALLED_TYPE_ID) == 1;
    }

    private long clusterSeqNo(final int m) {
        return cluster.counter(m, SeqeronCounters.SEQUENCER_CLUSTER_SEQ_NO_TYPE_ID);
    }
}
