package org.limitless.seqeron.tools;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.limitless.seqeron.protocol.SeqeronCounters;

/**
 * gateway-host-test.sh: clients on a host that runs no cluster member, across the loss of the member they read
 * through. The gateway host (member id 3) is a {@code ReplayerServer} whose relay copies group 1's queue off the
 * leader's archive first. A confirm producer on it submits over UDP ingress and checks its frames on that queue; the
 * leader is killed mid-stream, which is a failover and the relay's source going away. The producer must see every
 * frame once, in order, the relay must move to another member, and after the host's restart, which relays the queue
 * into a second local recording, a fresh follow on the host must cold-start through its Replayer and catch up.
 */
class GatewayHostTest {
    private static final int HOST = 3;
    private static final String COUNT = System.getenv().getOrDefault("CONFIRM_COUNT", "20000");
    private static final String PACING_MICROS = System.getenv().getOrDefault("CONFIRM_PACING_MICROS", "100");

    @TempDir
    Path baseDir;

    private ClusterProcesses cluster;

    @BeforeEach
    void startCluster(final TestInfo test) {
        cluster = new ClusterProcesses(baseDir, Path.of(System.getProperty("seqeron.systemTestLogs"),
                                                        test.getTestClass().orElseThrow().getSimpleName()));
        cluster.startAll();
        cluster.loadTopology(Path.of(System.getProperty("seqeron.topologyGroups")));
    }

    @AfterEach
    void stopCluster() {
        cluster.close();
    }

    @Test
    @Timeout(value = 300, unit = TimeUnit.SECONDS)
    void relayMovesOffAKilledLeaderAndARestartedHostServesAColdFollower() {
        final int leader = cluster.awaitLeader();
        // Every member has the leader's term before the relay is pointed at it.
        cluster.await("every member to know leader " + leader, Duration.ofSeconds(30), () -> {
            for (int m = 0; m < ClusterProcesses.MEMBERS; m++) {
                if (cluster.counter(m, SeqeronCounters.SEQUENCER_CURRENT_LEADER_MEMBER_ID_TYPE_ID) != leader) {
                    return false;
                }
            }
            return true;
        });
        // The leader first, so the kill takes away the member the relay is reading.
        final List<Integer> order = new ArrayList<>(List.of(leader));
        for (int m = 0; m < ClusterProcesses.MEMBERS; m++) {
            if (m != leader) {
                order.add(m);
            }
        }
        final int[] archives = order.stream().mapToInt(Integer::intValue).toArray();
        final ClusterProcesses.Child host = cluster.startGatewayHost("gateway-host", HOST, archives);
        host.awaitLine("ready —", Duration.ofSeconds(30));

        final ClusterProcesses.Child confirm = cluster.probe("confirm", HOST, "confirm", "probe.clientId=11",
                                                             "probe.count=" + COUNT,
                                                             "probe.pacingMicros=" + PACING_MICROS);
        confirm.awaitLine("confirm: sending", Duration.ofSeconds(30));
        // Mid-stream when the leader dies.
        cluster.await("the producer mid-stream", Duration.ofSeconds(30), () ->
            !host.lines("following member archive").isEmpty() && confirm.lastLine("confirm:") != null);

        cluster.kill(leader);
        final int confirmStatus = confirm.awaitExit(Duration.ofSeconds(120));
        final boolean switched = !host.lines("left member archive " + ClusterProcesses.archiveEndpoint(leader))
            .isEmpty() && host.lines("following member archive").size() >= 2;

        host.stop();
        final ClusterProcesses.Child restarted = cluster.startGatewayHost("gateway-host-restart", HOST, archives);
        restarted.awaitLine("ready —", Duration.ofSeconds(30));
        final ClusterProcesses.Child fresh = cluster.probe("fresh-follower", HOST, "follow", "probe.clientId=9");
        boolean caught = true;
        try {
            fresh.awaitLine("following live", Duration.ofSeconds(30));
        } catch (final AssertionError ex) {
            caught = false;
        }
        final boolean caughtUp = caught;
        System.out.printf("leader %d killed, relay order %s%nrelay: %s%nproducer on node %d: %s%n"
                          + "restarted host: %s%nfresh follower caught up: %b%n",
                          leader, order, host.lines("member archive"), HOST, confirm.lastLine("confirm:"),
                          restarted.lines("onto local recording"), caughtUp);

        assertAll(
            () -> assertEquals(0, confirmStatus, "producer on the host: " + confirm.lastLine("confirm:")),
            () -> assertTrue(switched, "the relay did not move off node " + leader + ": "
                                           + host.lines("member archive")),
            () -> assertTrue(caughtUp, "the fresh follower on the restarted host did not catch up"));
    }
}
