package org.limitless.seqeron.tools;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.Aeron;
import io.aeron.cluster.ElectionState;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.agrona.CloseHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.limitless.seqeron.app.Gateway;
import org.limitless.seqeron.protocol.SystemFrame;
import org.limitless.seqeron.replayer.client.ReplayerStreamReceiver;
import org.limitless.seqeron.replayer.client.SequencedEvent;

/**
 * Consensus time going back while a producer streams: the leader's clock stepped, or a failover onto members whose
 * clocks are behind. Consensus time is the leader host's wall clock: it stamps every frame and paces the 1 Hz {@code
 * ClusterHeartbeat}, whose absence is what every instance's queue-stall fence detects, on a monotonic clock. Safety is
 * asserted in every case; how long each queue went without a heartbeat, and whether the cluster elected, are reported.
 */
@ExtendWith(InterruptingTestCallback.class)
class ClusterClockStepTest {
    private static final long PACING_NS = TimeUnit.MILLISECONDS.toNanos(1);
    private static final int FRAMES_BEFORE_STEP = 2_000;
    /** How long the producer goes on after the step's length: time to show what follows it. */
    private static final long STREAM_PAST_STEP_MS = 10_000;
    private static final long FENCE_NS = TimeUnit.MILLISECONDS.toNanos(Gateway.DEFAULT_QUEUE_STALL_TIMEOUT_MS);
    /** Above the 3.2–3.4 s an election from a stopped leader took here. */
    private static final long ELECTION_ALLOWANCE_MS = 5_000;

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

    /** Under the 10 s driver timeout, the leader holds the heartbeat back for the step and leads on. */
    @Test
    @InterruptAfter(120)
    void aLeaderClockSteppedBackUnderTheDriverTimeoutHoldsTheHeartbeatAndLeadsOn() {
        final int leader = cluster.awaitLeader();
        final Outcome outcome = stream(leader, 5_000 / 2, false, () -> cluster.stepClock(leader, -5_000));
        assertFalse(outcome.terminated(), "the stepped leader terminated");
        assertEquals(0, outcome.newTerms(), "a step under the driver timeout started a new term");
        assertQueuesNeverSilentForTheFence(outcome);
    }

    /**
     * Past the queue-stall fence's default, so a heartbeat held for the whole step would fence. On Aeron 1.53.2 the
     * stepped member terminates at the driver timeout instead: its consensus module invokes its Aeron client only on a
     * slow tick paced by the stepped clock, and the client's monotonic service-interval check gives up after 10 s.
     */
    @Test
    @InterruptAfter(240)
    void aLeaderClockSteppedBackPastTheQueueStallFenceLosesNoFrame() {
        final int leader = cluster.awaitLeader();
        final long stepMs = Gateway.DEFAULT_QUEUE_STALL_TIMEOUT_MS + 10_000;
        assertQueuesNeverSilentForTheFence(stream(leader, stepMs / 2, false, () -> cluster.stepClock(leader, -stepMs)));
    }

    /**
     * Failover onto members whose clocks are behind the leader's from their start, so consensus time goes back by
     * the lag at the new term. Both followers lag, since which one wins the election is not the test's to choose.
     */
    @Test
    @InterruptAfter(180)
    void aFailoverOntoAMemberWhoseClockIsBehindUnderTheFenceLosesNoFrame() {
        failOverOntoLaggingMembers(10_000);
    }

    /** As above, by more than the queue-stall fence's default. */
    @Test
    @InterruptAfter(240)
    void aFailoverOntoAMemberWhoseClockIsBehindPastTheFenceLosesNoFrame() {
        failOverOntoLaggingMembers(Gateway.DEFAULT_QUEUE_STALL_TIMEOUT_MS + 10_000);
    }

    private void failOverOntoLaggingMembers(final long lagMs) {
        final int leader = cluster.awaitLeader();
        for (int m = 0; m < InProcessCluster.MEMBERS; m++) {
            if (m != leader) {
                cluster.stop(m);
                cluster.stepClock(m, -lagMs); // a lag it starts with, which no running consensus module sees step
                cluster.start(m);
                final int follower = m;
                Tests.await(() -> cluster.electionState(follower) == ElectionState.CLOSED &&
                                  cluster.commitPosition(follower) > 0);
            }
        }
        // The new leader's first frame is stamped an election after the old one's last: time goes back the lag less it.
        final Outcome outcome = stream(leader, lagMs - ELECTION_ALLOWANCE_MS, true, () -> cluster.stop(leader));
        assertNotEquals(leader, outcome.lastLeader(), "no lagging member took over from member " + leader);
        // The new leader's first heartbeat is due on its own clock, so the lag does not hold it back.
        assertQueuesNeverSilentForTheFence(outcome);
    }

    /** What a stream through a fault showed, its queues' watches closed. */
    private record Outcome(boolean terminated, int newTerms, int lastLeader, int faulted,
                           List<QueueClockWatch> watches) { }

    /**
     * Streams through a producer on a member other than {@code faulted}, runs {@code fault} once the stream is going,
     * and holds what every outcome must: the queue's stamps went back at least {@code backMs}, the frames arrived once
     * each in order, and every member holds one log, {@code faulted} once it is running again.
     * @param backMs       the least the fault takes consensus time back; the stream goes on for this and 10 s more
     * @param stopsFaulted whether {@code fault} stops {@code faulted}, whose queue is then not watched
     */
    private Outcome stream(final int faulted, final long backMs, final boolean stopsFaulted, final Runnable fault) {
        final int member = (faulted + 1) % InProcessCluster.MEMBERS;
        final List<QueueClockWatch> watches = new ArrayList<>();
        CompletableFuture<Void> faulting = null;
        try (ConfirmProducer producer =
                 new ConfirmProducer(cluster.aeronDir(member), member, Integer.MAX_VALUE, PACING_NS)) {
            for (int m = 0; m < InProcessCluster.MEMBERS; m++) {
                watches.add(new QueueClockWatch(cluster.aeronDir(m), m));
            }
            final long streamAfterFaultNs = TimeUnit.MILLISECONDS.toNanos(backMs + STREAM_PAST_STEP_MS);
            long faultedNs = 0;
            while (!producer.isDrained() && !producer.isStalled()) {
                producer.doWork();
                watches.forEach(QueueClockWatch::poll);
                if (faultedNs == 0 && producer.sent() >= FRAMES_BEFORE_STEP) {
                    if (stopsFaulted) {
                        // A client on a driver that stops exits the process, as Aeron's default error handler does.
                        watches.remove(faulted).close(); // watches are in member order
                    }
                    faulting = CompletableFuture.runAsync(fault);
                    faultedNs = System.nanoTime();
                } else if (faultedNs != 0 && System.nanoTime() - faultedNs > streamAfterFaultNs) {
                    producer.stopSending();
                }
                Tests.yield();
            }
            faulting.join();

            final boolean terminated = cluster.terminated();
            System.out.printf("fault on member %d; member %d leads after; a member terminated: %b%n", faulted,
                              producer.lastLeader(), terminated);
            long largestBackwardNs = 0;
            int newTerms = 0;
            for (final QueueClockWatch watch : watches) {
                System.out.println(watch);
                largestBackwardNs = Math.max(largestBackwardNs, watch.largestBackwardStampNs);
                newTerms = Math.max(newTerms, watch.leadershipChanges);
            }
            assertTrue(largestBackwardNs >= TimeUnit.MILLISECONDS.toNanos(backMs),
                       "no queue shows consensus time going back: its stamps went back " + largestBackwardNs +
                           " ns at most");
            assertTrue(producer.isDrained(),
                       "PendingSends still held " + producer.held() + " frame(s) after the drain timeout");
            producer.assertExactlyOnceInOrder();
            producer.assertOtherQueueShowsEveryEvenFrame();
            CloseHelper.closeAll(watches);
            if (terminated || !cluster.isRunning(faulted)) {
                cluster.stop(faulted);
                cluster.start(faulted); // its recording rebuilds over the full-log replay
            }
            QueueRecordings.assertOneLog(cluster.archives(), producer.lastClusterSeqNo());
            assertFalse(cluster.fatal(), "a member raised a fatal");
            return new Outcome(terminated, newTerms, producer.lastLeader(), faulted, List.copyOf(watches));
        } finally {
            CloseHelper.closeAll(watches);
        }
    }

    /** No queue that stayed up went silent for the fence's default, so no instance reading it would have fenced. */
    private static void assertQueuesNeverSilentForTheFence(final Outcome outcome) {
        for (final QueueClockWatch watch : outcome.watches()) {
            if (!outcome.terminated() || watch.memberId != outcome.faulted()) {
                assertTrue(watch.longestHeartbeatGapNs < FENCE_NS, watch + ": an instance reading it would fence");
            }
        }
    }

    /**
     * One member's queue as a caught-up consumer sees it: the longest it went without a {@code ClusterHeartbeat}, on a
     * monotonic clock as the queue-stall fence measures it, and how far its frames' consensus stamps went backward.
     */
    private static final class QueueClockWatch implements AutoCloseable {
        private static final int CLIENT_ID_BASE = 40;

        private final int memberId;
        private final Aeron aeron;
        private final ReplayerStreamReceiver receiver;
        private boolean caughtUp;
        private long lastHeartbeatNs;
        private long lastStampNs;
        private long longestHeartbeatGapNs;
        private long largestBackwardStampNs;
        private int leadershipChanges;

        QueueClockWatch(final String aeronDir, final int memberId) {
            this.memberId = memberId;
            aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDir));
            receiver = new ReplayerStreamReceiver(
                CLIENT_ID_BASE + memberId, this::onSequenced, (leaderMemberId, leadershipTermId, clusterSeqNo) -> {
                    if (caughtUp) {
                        leadershipChanges++;
                    }
                }, () -> caughtUp = true);
            receiver.start(aeron, memberId, InProcessCluster.GROUP_ID);
            while (!caughtUp) {
                receiver.poll();
                Tests.yield();
            }
        }

        void poll() {
            receiver.poll();
        }

        private void onSequenced(final SequencedEvent event) {
            if (!caughtUp) {
                return; // history has no heartbeat cadence to measure
            }
            final long stampNs = event.clusterTimestampNs();
            if (lastStampNs != 0) {
                largestBackwardStampNs = Math.max(largestBackwardStampNs, lastStampNs - stampNs);
            }
            lastStampNs = stampNs;
            if (event.isSystem() && event.systemEventType() == SystemFrame.CLUSTER_HEARTBEAT) {
                final long nowNs = System.nanoTime();
                if (lastHeartbeatNs != 0) {
                    longestHeartbeatGapNs = Math.max(longestHeartbeatGapNs, nowNs - lastHeartbeatNs);
                }
                lastHeartbeatNs = nowNs;
            }
        }

        @Override
        public String toString() {
            return String.format("member %d's queue: longest heartbeat gap %d ms, stamps back %d ms, %d new term(s)",
                                 memberId, TimeUnit.NANOSECONDS.toMillis(longestHeartbeatGapNs),
                                 TimeUnit.NANOSECONDS.toMillis(largestBackwardStampNs), leadershipChanges);
        }

        @Override
        public void close() {
            CloseHelper.closeAll(receiver, aeron);
        }
    }
}
