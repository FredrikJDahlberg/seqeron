package org.limitless.seqeron.tools;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.Aeron;
import io.aeron.AeronCounters;
import io.aeron.driver.status.StreamCounter;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import java.nio.file.Path;
import org.agrona.collections.LongArrayList;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.limitless.seqeron.protocol.ReplayProtocol;
import org.limitless.seqeron.replayer.client.ReplayerStreamReceiver;
import org.limitless.seqeron.replayer.client.SequencedEvent;
import org.limitless.seqeron.replayer.server.DerivingReplayer;
import org.limitless.seqeron.replayer.server.ReplayerServer;

/**
 * A client's cold start served off the Raft log (doc/disk-load-plan.md, step 2a): with {@code replayer.deriveFromLog} a
 * member's Replayer derives the replay through {@link DerivingReplayer}, once its derivation has placed each queue, and
 * an unchanged {@link ReplayerStreamReceiver} must walk each queue from groupSeqNo 1 in order, catch up at the queue's
 * tip and follow it live. The same walk off the queue's recording runs beside it, and each prints its rate.
 */
@ExtendWith(InterruptingTestCallback.class)
class DerivedReplayTest {
    private static final int HISTORY_FRAMES = 200_000;
    /** With the frame's envelope, a payload of the size production carries. */
    private static final int FILLER = 150;
    private static final int LIVE_FRAMES = 1_000;
    private static final long CONVERGE_TIMEOUT_MS = 60_000;
    private static final int CLIENT_ID = 31;

    @RegisterExtension final SystemTestWatcher watcher = new SystemTestWatcher();

    @TempDir Path baseDir;

    private InProcessCluster cluster;

    @BeforeEach
    void collect() {
        watcher.dataCollector().add(baseDir);
    }

    @AfterEach
    void stopCluster() {
        System.clearProperty(ReplayerServer.PROP_DERIVE_FROM_LOG);
        if (cluster != null) {
            cluster.close();
        }
    }

    @Test
    @InterruptAfter(240)
    void aColdStartWalksEachQueueOffTheLog() {
        System.setProperty(ReplayerServer.PROP_DERIVE_FROM_LOG, "true");
        walkEachQueue(true);
    }

    @Test
    @InterruptAfter(240)
    void aColdStartWalksEachQueueOffItsRecording() {
        walkEachQueue(false);
    }

    private void walkEachQueue(final boolean derived) {
        cluster = new InProcessCluster(baseDir);
        cluster.startAll();
        final int leader = cluster.awaitLeader();
        final int member = (leader + 1) % InProcessCluster.MEMBERS;
        final long tip = send(leader, HISTORY_FRAMES);
        for (final int groupId : InProcessCluster.GROUP_IDS) {
            walk(member, groupId, tip, derived);
        }
        assertFalse(cluster.fatal(), "a member raised a fatal");
        assertFalse(cluster.terminated(), "a member terminated");
    }

    /** Cold-starts a client on a follower's queue, then sends more and holds it to following live. */
    private void walk(final int member, final int groupId, final long tipClusterSeqNo, final boolean derived) {
        final LongArrayList groupSeqNos = new LongArrayList();
        boolean derivedReplay = false;
        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(cluster.aeronDir(member)));
             ReplayerStreamReceiver receiver = new ReplayerStreamReceiver(
                 CLIENT_ID + groupId,
                 (final SequencedEvent event) -> groupSeqNos.addLong(event.groupSeqNo()), null, () -> { })) {
            final long startedNs = System.nanoTime();
            receiver.start(aeron, member, groupId);
            converge(receiver, tipClusterSeqNo, groupSeqNos);
            final int walked = groupSeqNos.size();
            final double seconds = (System.nanoTime() - startedNs) / 1e9;
            derivedReplay = derivedReplay(aeron);
            System.out.printf("group %d, %s: %d frame(s) walked in %.0f ms, %.0f frames/s%n", groupId,
                              derived ? "derived from the log" : "replayed from the recording", walked, seconds * 1e3,
                              walked / seconds);

            converge(receiver, send(cluster.awaitLeader(), LIVE_FRAMES), groupSeqNos);
            assertTrue(groupSeqNos.size() > walked, "nothing reached it live after the walk");
        }
        for (int i = 0; i < groupSeqNos.size(); i++) {
            assertEquals(i + 1, groupSeqNos.getLong(i), "dispatched out of order, a hole or a repeat");
        }
        assertEquals(derived, derivedReplay, "whether its replay was derived");
    }

    private static void converge(final ReplayerStreamReceiver receiver, final long tipClusterSeqNo,
                                 final LongArrayList groupSeqNos) {
        final long deadlineMs = System.currentTimeMillis() + CONVERGE_TIMEOUT_MS;
        while (!receiver.isCaughtUp() || receiver.lastClusterSeqNo() < tipClusterSeqNo) {
            assertTrue(System.currentTimeMillis() < deadlineMs,
                       "not caught up after " + CONVERGE_TIMEOUT_MS + " ms, at groupSeqNo " + groupSeqNos.size());
            if (receiver.poll() == 0) {
                Tests.yield();
            }
        }
    }

    /**
     * Whether a {@link DerivingReplayer} has published a replay: the driver keeps a publication's counters while its
     * log buffer lingers, which outlasts the walk.
     */
    private static boolean derivedReplay(final Aeron aeron) {
        final CountersReader counters = aeron.countersReader();
        final boolean[] found = { false };
        counters.forEach((counterId, typeId, keyBuffer, label) -> {
            found[0] |= typeId == AeronCounters.DRIVER_PUBLISHER_LIMIT_TYPE_ID &&
                keyBuffer.getInt(StreamCounter.STREAM_ID_OFFSET) == ReplayProtocol.REPLAY_STREAM_ID &&
                keyBuffer.getStringAscii(StreamCounter.CHANNEL_OFFSET).contains(DerivingReplayer.DERIVED_ALIAS);
        });
        return found[0];
    }

    /** @return the clusterSeqNo of the last of {@code frames} confirmed frames sent through {@code member} */
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
