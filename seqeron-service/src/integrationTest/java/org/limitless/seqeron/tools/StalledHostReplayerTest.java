package org.limitless.seqeron.tools;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.Aeron;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import java.nio.file.Path;
import org.agrona.collections.LongArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.limitless.seqeron.replayer.client.ReplayerStreamReceiver;
import org.limitless.seqeron.replayer.client.SequencedEvent;

/**
 * A gateway host's Replayer stopped as a SIGSTOP does, while a client cold-starts on the host: its requests go unread,
 * so it is answered nothing, neither {@code Replaying} nor {@code ReplayPending}. The client must neither fault nor
 * report caught up short of the queue's tip, and once the Replayer runs again must walk the host's queue, which the
 * relay kept filling meanwhile, in order from groupSeqNo 1.
 */
@ExtendWith(InterruptingTestCallback.class)
class StalledHostReplayerTest {
    private static final int HISTORY_FRAMES = 300;
    private static final int DURING_FRAMES = 50;
    private static final int FILLER = 1_000;
    /** Past several resends and the replay stall timeout's 5 s, under the 30 s recovery-progress timeout. */
    private static final long STALL_MS = 7_000;
    private static final long CONVERGE_TIMEOUT_MS = 30_000;
    private static final int CLIENT_ID = 32;

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
    void aClientStartedOnAStalledHostReplayerWaitsAndConvergesOnceItRuns() {
        final int member = cluster.awaitLeader();
        final long tipClusterSeqNo = send(member, HISTORY_FRAMES);
        cluster.startGatewayHost();
        final FaultableReplayer replayer = cluster.replayer(InProcessCluster.GATEWAY_HOST);
        final LongArrayList groupSeqNos = new LongArrayList();
        try (Aeron aeron = Aeron.connect(
                 new Aeron.Context().aeronDirectoryName(cluster.aeronDir(InProcessCluster.GATEWAY_HOST)))) {
            awaitHostQueue(aeron, tipClusterSeqNo);
            follow(aeron, groupSeqNos, replayer, tipClusterSeqNo);
        }
    }

    /**
     * Follows the host's queue to {@code tipClusterSeqNo} with its Replayer running, which leaves the relay's history
     * in the host's recording, so that the client under test starts on a queue that has moved on from groupSeqNo 1.
     */
    private void awaitHostQueue(final Aeron aeron, final long tipClusterSeqNo) {
        try (ReplayerStreamReceiver receiver =
                 new ReplayerStreamReceiver(CLIENT_ID + 1, (final SequencedEvent event) -> { }, null, null)) {
            receiver.start(aeron, InProcessCluster.GATEWAY_HOST, InProcessCluster.GROUP_ID);
            final long startedMs = System.currentTimeMillis();
            while (!receiver.isCaughtUp() || receiver.lastClusterSeqNo() < tipClusterSeqNo) {
                assertTrue(System.currentTimeMillis() - startedMs < CONVERGE_TIMEOUT_MS,
                           "the host never held the history");
                receiver.poll();
                Tests.yield();
            }
        }
    }

    private void follow(final Aeron aeron, final LongArrayList groupSeqNos, final FaultableReplayer replayer,
                        final long historyTipClusterSeqNo) {
        long tipClusterSeqNo = historyTipClusterSeqNo;
        try (ReplayerStreamReceiver receiver = new ReplayerStreamReceiver(
                 CLIENT_ID, (final SequencedEvent event) -> groupSeqNos.addLong(event.groupSeqNo()), null, null)) {
            replayer.starveRequests(true);
            try {
                receiver.start(aeron, InProcessCluster.GATEWAY_HOST, InProcessCluster.GROUP_ID);
                final long stalledAtMs = System.currentTimeMillis();
                // The relay fills the host's queue meanwhile.
                tipClusterSeqNo = send(cluster.awaitLeader(), DURING_FRAMES);
                while (System.currentTimeMillis() - stalledAtMs < STALL_MS) {
                    receiver.poll(); // a fault surfaces here
                    assertFalse(receiver.isCaughtUp(), "caught up with its Replayer stopped");
                    Tests.yield();
                }
                assertEquals(0, groupSeqNos.size(), "dispatched with its Replayer stopped");
            } finally {
                replayer.starveRequests(false);
            }

            final long resumedAtMs = System.currentTimeMillis();
            while (!receiver.isCaughtUp() || receiver.lastClusterSeqNo() < tipClusterSeqNo) {
                assertTrue(System.currentTimeMillis() - resumedAtMs < CONVERGE_TIMEOUT_MS,
                           "not caught up " + CONVERGE_TIMEOUT_MS + " ms after the Replayer ran, at groupSeqNo " +
                               groupSeqNos.size());
                receiver.poll();
                assertFalse(receiver.isCaughtUp() && receiver.lastClusterSeqNo() < tipClusterSeqNo,
                            "caught up at groupSeqNo " + groupSeqNos.size() + ", short of the queue's tip");
                Tests.yield();
            }
            System.out.printf("caught up %d ms after the Replayer ran, at groupSeqNo %d%n",
                              System.currentTimeMillis() - resumedAtMs, groupSeqNos.size());
        }
        for (int i = 0; i < groupSeqNos.size(); i++) {
            assertEquals(i + 1, groupSeqNos.getLong(i), "dispatched out of order, a hole or a repeat");
        }
        assertFalse(cluster.fatal(), "a member or the host raised a fatal");
    }

    /** Sends {@code frames} confirmed frames through {@code member}. @return the clusterSeqNo of the last */
    private long send(final int member, final int frames) {
        try (ConfirmProducer producer = new ConfirmProducer(cluster.aeronDir(member), member, frames, 0, FILLER)) {
            while (!producer.isDrained()) {
                assertFalse(producer.isStalled(), "the producer stalled with " + producer.held() + " held");
                producer.doWork();
            }
            return producer.lastClusterSeqNo();
        }
    }
}
