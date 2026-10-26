package org.limitless.seqeron.tools;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.limitless.seqeron.protocol.PortLayout;

/**
 * faulteron/mute-voter-test.sh: one follower's RequestVote and Vote frames to its peers are dropped, the leader is
 * killed, and the follower is healed after {@code HOLD_SECS} without a leader. The rest of the follower's consensus
 * traffic flows, so the fault is latent until an election: no election runs and every queue answers a ping before the
 * kill, and with the leader dead the two survivors cannot reach a quorum. Healed, they elect a leader, the killed
 * member rejoins as its follower, and the producer on the muted follower, sending through {@code PendingSends}, sees
 * every frame once, in order.
 */
@Tag("docker")
class MuteVoterTest {
    private static final String CONFIRM_COUNT = System.getenv().getOrDefault("CONFIRM_COUNT", "60000");
    /** The whole leaderless stretch must stay under ClusterStreamSender's 5 s new-leader timeout. */
    private static final int HOLD_SECS = Integer.parseInt(System.getenv().getOrDefault("HOLD_SECS", "3"));
    /** A SIGKILLed member leaves its mark files active; a restart waits out their 10 s liveness timeout. */
    private static final long MARK_FILE_SETTLE_MS = 12_000;
    private static final String LEADER = "seqeron.sequencer.currentLeaderMemberId";

    private DockerCluster cluster;
    private Path logDir;

    @BeforeEach
    void startCluster() {
        logDir = Path.of(System.getProperty("seqeron.systemTestLogs"), "MuteVoterTest");
        cluster = new DockerCluster(logDir);
        cluster.start(Path.of(System.getProperty("seqeron.topologyGroups")));
    }

    @AfterEach
    void stopCluster() {
        cluster.close();
    }

    @Test
    @Timeout(value = 900, unit = TimeUnit.SECONDS)
    void mutedFollowerBlocksElectionUntilHealed() throws Exception {
        final int old = cluster.awaitLeader(DockerCluster.deadline(60), DockerCluster.NO_MEMBER);
        final int muted = (old + 1) % DockerCluster.MEMBERS;
        final int other = (old + 2) % DockerCluster.MEMBERS;
        final ClusterProcesses.Child confirm = cluster.probe("confirm", muted, "confirm",
                                                             "probe.count=" + CONFIRM_COUNT, "probe.clientId=11");
        confirm.awaitLine("confirm: sending", DockerCluster.deadline(60));

        // The peers' consensus ports only, so ballots reaching the muted member still arrive.
        final String peerA = "" + PortLayout.consensusPort(old);
        final String peerB = "" + PortLayout.consensusPort(other);
        assertEquals(0, cluster.faulteron(muted, "attach").status(), "faulteron could not attach to node-" + muted);
        assertEquals(0, cluster.faulteron(muted, "block", "--template", "request-vote", peerA, peerB).status(),
                     "faulteron could not mute RequestVote on node-" + muted);
        assertEquals(0, cluster.faulteron(muted, "block", "--template", "vote", peerA, peerB).status(),
                     "faulteron could not mute Vote on node-" + muted);
        final long changesBefore = cluster.leadershipChanges();
        Thread.sleep(2_000);
        final StringBuilder pinging = new StringBuilder();
        for (int m = 0; m < DockerCluster.MEMBERS; m++) {
            if (cluster.ping(m)) {
                pinging.append(m).append(' ');
            }
        }
        final long changesLatent = cluster.leadershipChanges();

        cluster.kill(old);
        final long killedAt = System.nanoTime();
        final int early = cluster.pollLeader(DockerCluster.deadline(HOLD_SECS), old);

        final DockerCluster.Result rules = cluster.heal(muted);
        Files.writeString(logDir.resolve("rules.txt"), rules.output());
        final long dropped = DockerCluster.dropped(rules.output());
        final long healedAt = System.nanoTime();
        final int newLeader = cluster.pollLeader(DockerCluster.deadline(30), old);
        final long electedAfterHealSecs = (System.nanoTime() - healedAt) / 1_000_000_000;

        final long settleMs = MARK_FILE_SETTLE_MS - (System.nanoTime() - killedAt) / 1_000_000;
        if (settleMs > 0) {
            Thread.sleep(settleMs);
        }
        boolean rejoined = false;
        if (newLeader != DockerCluster.NO_MEMBER) {
            cluster.restart(old, DockerCluster.deadline(120));
            rejoined = cluster.poll(DockerCluster.deadline(60), () -> cluster.counter(old, LEADER) == newLeader);
        }
        final boolean rejoinedAsFollower = rejoined;
        final int confirmStatus = confirm.awaitExit(DockerCluster.deadline(120));
        System.out.printf("leader %d, muted follower %d, dropped %d ballot(s)%n"
                          + "leadership changes before/latent: %d / %d, queues answering: %s%n"
                          + "elected within %ds of the kill: %s; new leader %d, %ds after the heal, rejoined %b%n"
                          + "PendingSends producer: %s%n",
                          old, muted, dropped, changesBefore, changesLatent, pinging.toString().trim(), HOLD_SECS,
                          early == DockerCluster.NO_MEMBER ? "no" : "member " + early, newLeader,
                          electedAfterHealSecs, rejoinedAsFollower,
                          confirm.lastLine("confirm:"));

        assertAll(
            () -> assertTrue(changesLatent == changesBefore && pinging.toString().equals("0 1 2 "),
                             "the fault showed before the leader died: changes " + changesBefore + " -> "
                                 + changesLatent + ", queues answering: " + pinging),
            () -> assertEquals(DockerCluster.NO_MEMBER, early,
                               "member " + early + " was elected while node-" + muted + " was muted"),
            () -> assertTrue(dropped > 0, "no ballot was dropped"),
            () -> assertTrue(newLeader != DockerCluster.NO_MEMBER, "no leader after the heal"),
            () -> assertTrue(rejoinedAsFollower, "node-" + old + " did not rejoin as a follower of " + newLeader),
            () -> assertEquals(0, confirmStatus, "PendingSends producer: " + confirm.lastLine("confirm:")));
    }
}
