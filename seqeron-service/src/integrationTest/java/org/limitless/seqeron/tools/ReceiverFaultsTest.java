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
import java.util.function.BooleanSupplier;
import java.util.function.LongPredicate;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.collections.LongArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.limitless.seqeron.protocol.SeqeronCounters;
import org.limitless.seqeron.replayer.client.ReplayerStreamReceiver;
import org.limitless.seqeron.replayer.client.SequencedEvent;

/**
 * A client cold-started over a history, with the Replayer's reply or a run of its replay dropped on the client's side
 * of the transport, which the receiver's fault hooks do on the poll thread. Each case holds the client to dispatching
 * the queue from groupSeqNo 1 without a hole or a repeat, and to never being caught up short of the queue's tip.
 */
@ExtendWith(InterruptingTestCallback.class)
class ReceiverFaultsTest {
    /** At {@link #FILLER} bytes each, past the window of a 16 MiB IPC term, so a replay is still running when it is resent. */
    private static final int HISTORY_FRAMES = 1_500;
    private static final int FILLER = 8_000;
    private static final long CONVERGE_TIMEOUT_MS = 40_000;
    private static final int CLIENT_ID = 31;
    /** Past {@code ReplayerRecovery}'s 30 s no-progress timeout. */
    private static final long STALL_WINDOW_MS = 35_000;

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

    /** The resend carries the request's id, so the Replayer answers it with the replay it already started. */
    @Test
    @InterruptAfter(120)
    void aDroppedReplayingIsResentAndTheWalkConverges() {
        final int[] drops = {0};
        final BooleanSupplier controlFaults = () -> drops[0]++ < 2;
        final long replays = converge(send(cluster.awaitLeader()), controlFaults, null);
        assertTrue(drops[0] >= 3, "the Replayer's answers were not dropped and then let through: " + drops[0]);
        assertEquals(1, replays, "a resend started another replay");
    }

    /** The image's position still reaches its bound, so only the frames' groupSeqNos can show the hole. */
    @Test
    @InterruptAfter(120)
    void aRunOfDroppedReplayFragmentsIsHealedAndTheWalkConverges() {
        final long tipClusterSeqNo = send(cluster.awaitLeader());
        final long[] hole = holeAt(tipClusterSeqNo);
        final int[] drops = {0};
        final LongPredicate replayFaults = position -> position >= hole[0] && position < hole[1] && drops[0] < 5 && ++drops[0] > 0;
        final long replays = converge(tipClusterSeqNo, null, replayFaults);
        assertEquals(5, drops[0], "the run was not dropped, or was dropped again");
        assertTrue(replays > 1, "no replay healed the hole in the first: " + replays);
    }

    /**
     * A recording that does not cover a hole gives every replay the same one. The client resumes at each bound and is
     * never caught up, which it reports as {@code recoveryStalled} after the 30 s no-progress timeout, and it heals
     * once the hole is covered.
     */
    @Test
    @InterruptAfter(180)
    void aHoleEveryReplayRepeatsRaisesTheRecoveryStalledGaugeAndHealsOnceCovered() {
        final int member = cluster.awaitLeader();
        final long tipClusterSeqNo = send(member);
        final long[] hole = holeAt(tipClusterSeqNo);
        final LongArrayList groupSeqNos = new LongArrayList();
        final boolean[] holed = {true};
        final LongPredicate replayFaults = position -> holed[0] && position >= hole[0] && position < hole[1];
        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(cluster.aeronDir(member)));
             ReplayerStreamReceiver receiver = new ReplayerStreamReceiver(
                 CLIENT_ID, (final SequencedEvent event) -> groupSeqNos.addLong(event.groupSeqNo()), null, null, null,
                 null, replayFaults)) {
            final long servedBefore = replaysServed(aeron);
            receiver.start(aeron, member, InProcessCluster.GROUP_ID);
            final long startedMs = System.currentTimeMillis();
            while (System.currentTimeMillis() - startedMs < STALL_WINDOW_MS) {
                receiver.poll(); // a fault surfaces here
                assertFalse(receiver.isCaughtUp(), "caught up at groupSeqNo " + groupSeqNos.size() + " over a hole");
                if (System.currentTimeMillis() - startedMs < STALL_WINDOW_MS / 2) {
                    assertEquals(0, counter(aeron, SeqeronCounters.APP_RECOVERY_STALLED_TYPE_ID),
                                 "stalled before the no-progress timeout");
                }
                Tests.yield();
            }
            assertEquals(100, groupSeqNos.size(), "dispatched past the hole");
            assertEquals(1, counter(aeron, SeqeronCounters.APP_RECOVERY_STALLED_TYPE_ID),
                         "the stall was not reported");
            System.out.printf("%d replay(s) over the hole in %d ms%n", replaysServed(aeron) - servedBefore,
                              STALL_WINDOW_MS);

            holed[0] = false;
            final long coveredAtMs = System.currentTimeMillis();
            while (!receiver.isCaughtUp() || receiver.lastClusterSeqNo() < tipClusterSeqNo) {
                assertTrue(System.currentTimeMillis() - coveredAtMs < CONVERGE_TIMEOUT_MS,
                           "not caught up after the hole was covered, at groupSeqNo " + groupSeqNos.size());
                receiver.poll();
                Tests.yield();
            }
            assertEquals(0, counter(aeron, SeqeronCounters.APP_RECOVERY_STALLED_TYPE_ID), "the stall was not cleared");
        }
        for (int i = 0; i < groupSeqNos.size(); i++) {
            assertEquals(i + 1, groupSeqNos.getLong(i), "dispatched out of order, a hole or a repeat");
        }
        assertFalse(cluster.fatal(), "a member raised a fatal");
    }

    /**
     * Follows the queue with no faults to find where groupSeqNos 101..105 sit in the recording.
     * @return the position of groupSeqNo 101, and of 106
     */
    private long[] holeAt(final long tipClusterSeqNo) {
        final int member = cluster.awaitLeader();
        final Long2LongHashMap positions = new Long2LongHashMap(-1);
        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(cluster.aeronDir(member)));
             ReplayerStreamReceiver receiver = new ReplayerStreamReceiver(
                 CLIENT_ID + 1, (final SequencedEvent event) -> positions.put(event.groupSeqNo(), event.position()),
                 null, null)) {
            receiver.start(aeron, member, InProcessCluster.GROUP_ID);
            final long startedMs = System.currentTimeMillis();
            while (!receiver.isCaughtUp() || receiver.lastClusterSeqNo() < tipClusterSeqNo) {
                assertTrue(System.currentTimeMillis() - startedMs < CONVERGE_TIMEOUT_MS, "the probe never caught up");
                receiver.poll();
                Tests.yield();
            }
        }
        return new long[] {positions.get(101), positions.get(106)};
    }

    /** @return how many replays the Replayer started for the client */
    private long converge(final long tipClusterSeqNo, final BooleanSupplier controlFaults,
                          final LongPredicate replayFaults) {
        final int member = cluster.awaitLeader();
        final LongArrayList groupSeqNos = new LongArrayList();
        final long replays;
        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(cluster.aeronDir(member)));
             ReplayerStreamReceiver receiver = new ReplayerStreamReceiver(
                 CLIENT_ID, (final SequencedEvent event) -> groupSeqNos.addLong(event.groupSeqNo()), null, null, null,
                 controlFaults, replayFaults)) {
            final long servedBefore = replaysServed(aeron);
            receiver.start(aeron, member, InProcessCluster.GROUP_ID);
            final long startedMs = System.currentTimeMillis();
            while (!receiver.isCaughtUp() || receiver.lastClusterSeqNo() < tipClusterSeqNo) {
                assertTrue(System.currentTimeMillis() - startedMs < CONVERGE_TIMEOUT_MS,
                           "not caught up " + CONVERGE_TIMEOUT_MS + " ms after starting, at groupSeqNo " +
                               groupSeqNos.size());
                receiver.poll();
                assertFalse(receiver.isCaughtUp() && receiver.lastClusterSeqNo() < tipClusterSeqNo,
                            "caught up at groupSeqNo " + groupSeqNos.size() + ", short of the queue's tip");
                Tests.yield();
            }
            replays = replaysServed(aeron) - servedBefore;
            System.out.printf("caught up %d ms after starting, at groupSeqNo %d, by %d replay(s)%n",
                              System.currentTimeMillis() - startedMs, groupSeqNos.size(), replays);
        }
        for (int i = 0; i < groupSeqNos.size(); i++) {
            assertEquals(i + 1, groupSeqNos.getLong(i), "dispatched out of order, a hole or a repeat");
        }
        assertFalse(cluster.fatal(), "a member raised a fatal");
        return replays;
    }

    /** How many replays the member's Replayer has started. */
    private static long replaysServed(final Aeron aeron) {
        return counter(aeron, SeqeronCounters.REPLAYER_REPLAYS_SERVED_COUNT_TYPE_ID);
    }

    private static long counter(final Aeron aeron, final int typeId) {
        final long[] value = {-1};
        aeron.countersReader().forEach((counterId, type, key, label) -> {
            if (type == typeId) {
                value[0] = aeron.countersReader().getCounterValue(counterId);
            }
        });
        return value[0];
    }

    /** Sends the history through {@code member}, and waits until its queue shows it. @return the last clusterSeqNo */
    private long send(final int member) {
        try (ConfirmProducer producer =
                 new ConfirmProducer(cluster.aeronDir(member), member, HISTORY_FRAMES, 0, FILLER)) {
            while (!producer.isDrained()) {
                assertFalse(producer.isStalled(), "the producer stalled with " + producer.held() + " held");
                producer.doWork();
            }
            return producer.lastClusterSeqNo();
        }
    }
}
