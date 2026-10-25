package org.limitless.seqeron.tools;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.Aeron;
import io.aeron.Subscription;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import java.nio.file.Path;
import org.agrona.CloseHelper;
import org.agrona.collections.LongArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.limitless.seqeron.protocol.ReplayProtocol;
import org.limitless.seqeron.protocol.SeqeronCounters;
import org.limitless.seqeron.replayer.client.ReplayerStreamReceiver;
import org.limitless.seqeron.replayer.client.SequencedEvent;
import org.limitless.seqeron.sbe.replay.MessageHeaderDecoder;
import org.limitless.seqeron.sbe.replay.ReplayingDecoder;

/**
 * A client whose duty cycle stops during its cold start and resumes under the driver's 10 s client liveness timeout,
 * as a GC pause or a SIGSTOP does: no subscription of its own is polled meanwhile. The walk's replay image is
 * tethered, so the archive waits on it; the control stream and the queue are untethered. Every case asserts that the
 * client dispatches the queue from groupSeqNo 1 without a hole or a repeat, is caught up within {@code
 * ReplayerRecovery}'s 30 s progress timeout of resuming, and is never caught up short of the queue's tip.
 */
@ExtendWith(InterruptingTestCallback.class)
class PausedWalkTest {
    /**
     * At {@link #FILLER} bytes each, past the 8 MiB window of a 16 MiB IPC term: the paused replay holds the archive.
     */
    private static final int HISTORY_FRAMES = 1_500;
    private static final int FILLER = 8_000;
    /** Past the queue's untethered eviction, a window behind its fastest subscriber less a quarter. */
    private static final int FLOOD_FRAMES = 1_300;
    /** Under {@code ReplayerRecovery}'s 5 s replay stall timeout. */
    private static final long SHORT_PAUSE_MS = 2_000;
    /**
     * Past the replay stall timeout and the archive's 5 s replay connect timeout, and under the Replayer's 10 s
     * stalled-replay timeout.
     */
    private static final long LONG_PAUSE_MS = 7_000;
    private static final long CONVERGE_TIMEOUT_MS = 30_000;
    /** How long the Replayer of an idle member may take to answer a request. */
    private static final long REPLY_WAIT_MS = 1_000;
    private static final int CLIENT_ID = 30;

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

    /** The replay it rides waits for it, and it reads on where it stopped. */
    @Test
    @InterruptAfter(120)
    void aClientPausedMidWalkUnderTheReplayStallTimeoutConverges() {
        walkThroughPause(SHORT_PAUSE_MS, false, false);
    }

    /** As above: it reads the image before its replay stall watchdog judges the replay, so it asks for no other. */
    @Test
    @InterruptAfter(120)
    void aClientPausedMidWalkPastTheReplayStallTimeoutConverges() {
        walkThroughPause(LONG_PAUSE_MS, false, false);
    }

    /**
     * More than a window of frames reaches the queue while it is paused, so its driver evicts it from the queue. It
     * rejoins at the tip once rested, and the next frame there shows the hole, which a resume replay heals. Its walk
     * reaches its bound before then, short of the tip, which does not catch it up.
     */
    @Test
    @InterruptAfter(180)
    void aClientPausedMidWalkWhileItsQueueRunsAWindowAheadConverges() {
        walkThroughPause(LONG_PAUSE_MS, true, false);
    }

    /**
     * Paused with the Replayer's {@code Replaying} unread, it subscribes to the replay only after the archive gave it
     * up for want of a subscriber, which the archive logs as an error.
     */
    @Test
    @InterruptAfter(120)
    void aClientPausedBeforeReadingItsReplyPastTheArchiveConnectTimeoutConverges() {
        watcher.ignoreErrorsMatching(
            s -> s.contains("no connection established for replayChannel=") && s.contains("alias=seqeron-replay"));
        walkThroughPause(LONG_PAUSE_MS, false, true);
    }

    /**
     * Fills a member's queue with history, starts a client cold, pauses it at the point named, resumes it, and holds
     * it to the class's assertions.
     * @param pauseMs    the least it is paused for
     * @param flood      whether the queue is sent more than a window of frames while it is paused
     * @param atRequest  whether it pauses with the answer to its request unread, rather than mid-walk
     */
    private void walkThroughPause(final long pauseMs, final boolean flood, final boolean atRequest) {
        final int member = cluster.awaitLeader();
        long tipClusterSeqNo = send(member, HISTORY_FRAMES);
        final LongArrayList groupSeqNos = new LongArrayList();
        final boolean[] caughtUp = {false};
        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(cluster.aeronDir(member)));
             Subscription control =
                 aeron.addSubscription(ReplayProtocol.IPC_CHANNEL + "|tether=false", ReplayProtocol.CONTROL_STREAM_ID);
             ReplayerStreamReceiver receiver = new ReplayerStreamReceiver(
                 CLIENT_ID, (final SequencedEvent event) -> groupSeqNos.addLong(event.groupSeqNo()), null,
                 () -> caughtUp[0] = true)) {
            receiver.start(aeron, member, InProcessCluster.GROUP_ID);
            if (atRequest) {
                // Each poll sends a request it has none answered for, after reading the control stream, so an answer
                // seen before the next poll is unread.
                while (!replied(control)) {
                    receiver.poll();
                }
            } else {
                while (receiver.lastClusterSeqNo() == 0) {
                    receiver.poll();
                    Tests.yield();
                }
            }
            assertFalse(caughtUp[0], "it caught up before the pause");

            final long servedBeforePause = replaysServed(aeron);
            final long pausedAtMs = System.currentTimeMillis();
            if (flood) {
                tipClusterSeqNo = send(member, FLOOD_FRAMES);
            }
            Tests.sleep(Math.max(0, pauseMs - (System.currentTimeMillis() - pausedAtMs)));
            final long resumedAtMs = System.currentTimeMillis();
            System.out.printf("paused %d ms after groupSeqNo %d%n", resumedAtMs - pausedAtMs, groupSeqNos.size());

            while (!receiver.isCaughtUp() || receiver.lastClusterSeqNo() < tipClusterSeqNo) {
                assertTrue(System.currentTimeMillis() - resumedAtMs < CONVERGE_TIMEOUT_MS,
                           "not caught up " + CONVERGE_TIMEOUT_MS + " ms after resuming, at groupSeqNo " +
                               groupSeqNos.size());
                receiver.poll();
                assertFalse(receiver.isCaughtUp() && receiver.lastClusterSeqNo() < tipClusterSeqNo,
                            "caught up at groupSeqNo " + groupSeqNos.size() + ", short of the queue's tip");
                Tests.yield();
            }
            final long replays = replaysServed(aeron) - servedBeforePause;
            System.out.printf("caught up %d ms after resuming, at groupSeqNo %d, by %d replay(s) more%n",
                              System.currentTimeMillis() - resumedAtMs, groupSeqNos.size(), replays);
            if (flood || atRequest) {
                assertTrue(replays > 0, "no replay healed the queue's hole or replaced the one the archive gave up");
            } else {
                assertEquals(0, replays, "it gave up the replay it rode for another");
            }
        }
        for (int i = 0; i < groupSeqNos.size(); i++) {
            assertEquals(i + 1, groupSeqNos.getLong(i), "dispatched out of order, a hole or a repeat");
        }
        assertFalse(cluster.fatal(), "a member raised a fatal");
    }

    /** How many replays the member's Replayer has started. */
    private static long replaysServed(final Aeron aeron) {
        final long[] value = {-1};
        aeron.countersReader().forEach((counterId, type, key, label) -> {
            if (type == SeqeronCounters.REPLAYER_REPLAYS_SERVED_COUNT_TYPE_ID) {
                value[0] = aeron.countersReader().getCounterValue(counterId);
            }
        });
        return value[0];
    }

    /**
     * Sends {@code frames} confirmed frames through {@code member} and waits until its queue shows them. A second
     * producer's seqNos repeat the first's, so neither is held to exactly once here; the client's groupSeqNos are.
     * @return the clusterSeqNo of the last
     */
    private long send(final int member, final int frames) {
        try (ConfirmProducer producer = new ConfirmProducer(cluster.aeronDir(member), member, frames, 0, FILLER)) {
            while (!producer.isDrained()) {
                assertFalse(producer.isStalled(), "the producer stalled with " + producer.held() + " held");
                producer.doWork();
            }
            return producer.lastClusterSeqNo();
        }
    }

    /** Whether a {@code Replaying} reaches the control stream within {@link #REPLY_WAIT_MS}. */
    private static boolean replied(final Subscription control) {
        final MessageHeaderDecoder header = new MessageHeaderDecoder();
        final boolean[] replied = {false};
        final long deadlineMs = System.currentTimeMillis() + REPLY_WAIT_MS;
        while (!replied[0] && System.currentTimeMillis() < deadlineMs) {
            control.poll((buffer, offset, length, unused) -> replied[0] |=
                         header.wrap(buffer, offset).templateId() == ReplayingDecoder.TEMPLATE_ID, 10);
            Tests.yield();
        }
        return replied[0];
    }
}
