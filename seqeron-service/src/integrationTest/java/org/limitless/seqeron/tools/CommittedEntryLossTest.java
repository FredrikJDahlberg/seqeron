package org.limitless.seqeron.tools;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.Aeron;
import io.aeron.Subscription;
import io.aeron.cluster.ElectionState;
import io.aeron.cluster.service.Cluster;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import java.nio.file.Path;
import java.util.function.BooleanSupplier;
import org.agrona.collections.MutableLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.limitless.seqeron.protocol.FrameLayer;
import org.limitless.seqeron.protocol.SequencedFrameDecoder;

/**
 * A fault sequence under which Aeron Cluster 1.53.2 loses a committed entry, scripted step by step: each step waits on
 * the members' consensus-module counters rather than on time.
 * <ol>
 * <li>The leader is cut; the other two elect one of themselves, which commits its term's first entries with the
 * follower's acknowledgement.</li>
 * <li>The follower stops, and the old leader returns able to vote but not to copy the log, so the new leader,
 * re-elected with its vote, waits on it with its quorum position at the old leader's log end.</li> <li>The follower
 * restarts, also unable to copy the log, hearing the leader before the leader hears it. The leader's term gives it that
 * quorum position as the commit position, so it replays only up to there, which sets its log's term back to the one it
 * last replayed. Its next canvass names that term, and the leader's answer, where the following term began, makes it
 * truncate the entries it acknowledged.</li> <li>The leader is cut before either copies those entries back; the other
 * two elect without them, and the leader, rejoining, truncates what it committed.</li>
 * </ol>
 * Every member's queue recording must still be one log. The stuck run behind it took a follower restart and a leader
 * cut a round apart, its log replication timing out in place of the stream cut here. Fails on 1.53.2, so it runs only
 * under {@code -PknownAeronFaults}, against an Aeron upgrade.
 *
 * <p>The second test checks that the leader takes itself down as it truncates entries it applied, and that, restarted,
 * it rejoins with an active recording that is one log with the others'. Left up, it either cannot rejoin or rejoins
 * with a queue that differs from theirs.
 */
@ExtendWith(InterruptingTestCallback.class)
class CommittedEntryLossTest {
    @RegisterExtension final SystemTestWatcher watcher = new SystemTestWatcher();

    @TempDir Path baseDir;

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
    @EnabledIfSystemProperty(named = "seqeron.knownAeronFaults", matches = "true")
    void aCommittedEntrySurvivesAFollowerRestartAndALeaderCut() {
        loseACommittedEntry();

        QueueRecordings.assertOneLog(cluster.archives(), nextClusterSeqNo(cluster.awaitLeader()));
        assertFalse(cluster.fatal(), "a member raised a fatal");
        assertFalse(cluster.terminated(), "a member terminated");
    }

    @Test
    @InterruptAfter(120)
    void aMemberThatTruncatedACommittedEntryRestartsAndRejoins() {
        final int truncated = loseACommittedEntry();
        final long commitPosition = cluster.commitPosition(cluster.awaitLeader());
        await(() -> cluster.fatal(truncated) || isCaughtUp(truncated, commitPosition));
        if (cluster.fatal(truncated)) {
            System.out.printf("member %d raised a fatal: restarting it%n", truncated);
            cluster.stop(truncated);
            cluster.start(truncated);
            final long restartCommitPosition = cluster.commitPosition(cluster.awaitLeader());
            await(() -> isCaughtUp(truncated, restartCommitPosition));
        }

        QueueRecordings.assertActiveOneLog(cluster.archives(), nextClusterSeqNo(cluster.awaitLeader()));
        for (int m = 0; m < InProcessCluster.MEMBERS; m++) {
            assertTrue(m == truncated || !cluster.fatal(m), "member " + m + " raised a fatal");
        }
        assertFalse(cluster.terminated(), "a member terminated");
    }

    /** Scripts the four steps above; returns the leader cut in the fourth, which truncates what it committed. */
    private int loseACommittedEntry() {
        final int oldLeader = cluster.awaitLeader();
        await(() -> isFollowing(0) && isFollowing(1) && isFollowing(2));
        cluster.cut(oldLeader, true);
        final long oldLeaderCommit = cluster.commitPosition(oldLeader);
        int awaited;
        do {
            awaited = cluster.awaitLeader();
        } while (awaited == oldLeader);
        final int leader = awaited;
        final int follower = InProcessCluster.MEMBERS - oldLeader - leader;
        await(() -> cluster.commitPosition(follower) > oldLeaderCommit);
        log("committed past the old leader", oldLeader, leader, follower);

        cluster.stop(follower);
        cluster.cutAllButConsensus(oldLeader, true);
        cluster.cut(oldLeader, false);
        await(() -> cluster.electionState(leader) == ElectionState.LEADER_LOG_REPLICATION);
        log("re-elected, waiting on the old leader", oldLeader, leader, follower);

        cluster.cutAllButConsensus(follower, true);
        cluster.mute(follower, true);
        cluster.start(follower);
        await(() -> cluster.commitPosition(follower) > 0 && cluster.electionState(follower) == ElectionState.CANVASS);
        log("follower replayed to the old leader's log end", oldLeader, leader, follower);
        cluster.mute(follower, false);
        await(() -> cluster.electionState(follower).code() >= ElectionState.FOLLOWER_LOG_REPLICATION.code());
        log("follower joined the term", oldLeader, leader, follower);

        cluster.cut(leader, true);
        cluster.cutAllButConsensus(oldLeader, false);
        cluster.cutAllButConsensus(follower, false);
        await(() -> isLeading(oldLeader) || isLeading(follower));
        log("elected without the leader", oldLeader, leader, follower);
        cluster.cut(leader, false);
        return leader;
    }

    /** Whether member {@code m} is out of its election and has seen a commit. */
    private boolean isFollowing(final int m) {
        return cluster.electionState(m) == ElectionState.CLOSED && cluster.commitPosition(m) > 0;
    }

    /** Whether member {@code m} is out of its election and has committed past {@code commitPosition}. */
    private boolean isCaughtUp(final int m, final long commitPosition) {
        return isFollowing(m) && cluster.commitPosition(m) > commitPosition;
    }

    private boolean isLeading(final int m) {
        return cluster.role(m) == Cluster.Role.LEADER && cluster.electionState(m) == ElectionState.CLOSED;
    }

    private void log(final String step, final int oldLeader, final int leader, final int follower) {
        System.out.printf("%s: old leader %s, leader %s, follower %s%n", step, describe(oldLeader), describe(leader),
                          describe(follower));
    }

    private String describe(final int m) {
        return cluster.isRunning(m) ? m + " " + cluster.electionState(m) + " at commit " + cluster.commitPosition(m)
                                    : m + " down";
    }

    private static void await(final BooleanSupplier condition) {
        while (!condition.getAsBoolean()) {
            Tests.sleep(5);
        }
    }

    /** The clusterSeqNo of the next frame member {@code m}'s queue shows. */
    private long nextClusterSeqNo(final int m) {
        final SequencedFrameDecoder frame = new SequencedFrameDecoder();
        final MutableLong clusterSeqNo = new MutableLong();
        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(cluster.aeronDir(m)));
             Subscription queue = aeron.addSubscription(FrameLayer.QUEUE_CHANNEL, FrameLayer.QUEUE_STREAM_ID)) {
            while (clusterSeqNo.get() == 0) {
                queue.poll((buffer, offset, length, header) -> {
                    if (frame.wrap(buffer, offset, length)) {
                        clusterSeqNo.set(frame.clusterSeqNo());
                    }
                }, 1);
                Tests.yield();
            }
        }
        return clusterSeqNo.get();
    }
}
