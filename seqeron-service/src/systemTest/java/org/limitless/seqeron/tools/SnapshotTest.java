package org.limitless.seqeron.tools;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.limitless.seqeron.protocol.SeqeronCounters;

/**
 * Application snapshots end to end (doc/snapshot.md) on a three-node cluster, with the
 * {@code TestGateway} pair taking part in rounds every 2 s (GW-T-A on member 0, GW-T-B on member 1, each restoring
 * from its own files) and a {@code TestApplication} replica on every member (source 16), the one on the leader
 * publishing its rounds. Every line a client sends is one marker and every client connection closes before the next
 * step, so the state an instance that restored must report is known at each step; every instance also compares its
 * own serialization with each round's sequenced SnapshotEnd and fences on a difference (spec section 16 A-7), so a
 * run with no FENCED line compared the restored state plus the tail against the publisher's, round after round.
 *
 * <ol>
 *   <li>Both instances up with snapshots; load through the active one; rounds complete.
 *   <li>The standby restarts: it restores, reports the expected state, and compares later rounds.
 *   <li>The active one is stopped: the restored instance takes over and serves.
 *   <li>The stopped one returns as a hot standby, restoring from the files it confirmed while active.
 *   <li>{@code clusterctl request-snapshot} starts a round, and both sources end it.
 *   <li>A follower's application replica restarts and restores.
 *   <li>The cluster leader is stopped: the new leader's replica publishes rounds, and the stopped member's clients
 *       restore when it returns.
 * </ol>
 */
class SnapshotTest {
    private static final String TEST_APPLICATION = "org.limitless.seqeron.tools.TestApplication";
    private static final String TEST_GATEWAY = "org.limitless.seqeron.tools.TestGateway";
    private static final int TEST_GATEWAY_PORT_BASE = 9200;
    private static final int SOURCE_ID = 9;
    private static final int APP_SOURCE_ID = 16;
    private static final Duration CATCHUP = Duration.ofSeconds(30);
    private static final int TIMEOUT_SECS = 30;
    private static final Pattern REQUESTED = Pattern.compile("round (\\d+) started");

    @TempDir
    Path baseDir;

    private ClusterProcesses cluster;
    private final List<ClusterProcesses.Child> instances = new ArrayList<>();
    private final List<String> failures = new ArrayList<>();
    private final ClusterProcesses.Child[] gateways = new ClusterProcesses.Child[2];
    private final ClusterProcesses.Child[] apps = new ClusterProcesses.Child[ClusterProcesses.MEMBERS];
    private final int[] gatewayRuns = new int[2];
    private final int[] appRuns = new int[ClusterProcesses.MEMBERS];
    private int clients;
    private int ctls;

    @BeforeEach
    void startCluster(final TestInfo test) {
        cluster = new ClusterProcesses(baseDir, Path.of(System.getProperty("seqeron.systemTestLogs"),
                                                        test.getTestClass().orElseThrow().getSimpleName()));
        cluster.startAll();
    }

    @AfterEach
    void stopCluster() {
        cluster.close();
    }

    @Test
    @Timeout(value = 600, unit = TimeUnit.SECONDS)
    void everyRestoreReachesTheExpectedStateAndNoInstanceDiverges() throws Exception {
        cluster.loadTopology(Path.of(System.getProperty("seqeron.snapshotTopology")));
        for (int m = 0; m < ClusterProcesses.MEMBERS; m++) {
            startApp(m);
        }

        // 1. Both instances with snapshots; load; rounds.
        startGateway(0);
        startGateway(1);
        int active = awaitActiveMember();
        int standby = 1 - active;
        int markers = 0;
        check("40 lines round-trip through the active instance", roundtrip(active, 40));
        markers += 40;
        check("40 more", roundtrip(active, 40));
        markers += 40;
        check("rounds complete after the load (the standby confirms them)", waitRounds(standby, 2, SOURCE_ID));
        check("the application's rounds complete (member " + standby + "'s replica)",
              waitRounds(standby, 2, APP_SOURCE_ID));
        check("no instance fenced", noFence());

        // 2. The standby restarts and restores.
        stopGateway(standby);
        startGateway(standby);
        check("it restored a snapshot", restores(standby) == 1);
        check("it caught up holding " + markers + " markers and no connection", caughtUpWith(standby, markers));
        check("20 lines round-trip through the active instance", roundtrip(active, 20));
        markers += 20;
        check("rounds complete, the restored instance comparing them", waitRounds(standby, 2, SOURCE_ID));
        check("no instance fenced", noFence());

        // 3. Failover onto the restored instance.
        stopGateway(active);
        check("the standby takes over", waitActive(standby));
        check("10 lines round-trip through it", roundtrip(standby, 10));
        markers += 10;
        check("it publishes rounds", waitRounds(standby, 2, SOURCE_ID));
        final int returning = active;
        active = standby;
        standby = returning;

        // 4. The stopped one returns as a hot standby.
        startGateway(standby);
        check("it restored a snapshot from the files it confirmed while active", restores(standby) == 1);
        check("it caught up holding " + markers + " markers and no connection", caughtUpWith(standby, markers));
        check("10 lines round-trip through the active instance", roundtrip(active, 10));
        markers += 10;
        check("rounds complete, the restored instance comparing them", waitRounds(standby, 2, SOURCE_ID));
        check("no instance fenced", noFence());

        // 5. An operator-requested round.
        final ClusterProcesses.Child request = cluster.clusterctl("request-snapshot-" + ++ctls, 0,
                                                                  "request-snapshot");
        request.awaitExit(Duration.ofSeconds(60));
        int requested = 0;
        for (final String line : request.lines("round")) {
            final Matcher matcher = REQUESTED.matcher(line);
            if (matcher.find()) {
                requested = Integer.parseInt(matcher.group(1));
            }
        }
        final int round = requested;
        check("it started a round", round > 0);
        check("the gateway pair ends round " + round, waitRoundConfirmed(active, round, SOURCE_ID));
        check("the application ends round " + round, waitRoundConfirmed(active, round, APP_SOURCE_ID));

        // 6. A follower's application replica restarts and restores.
        final int leader = cluster.awaitLeader();
        final int follower = (leader + 1) % ClusterProcesses.MEMBERS;
        stopApp(follower);
        startApp(follower);
        check("it restored a snapshot", apps[follower].lines("restoring round").size() > 0);
        check("rounds complete, the restored replica comparing them", waitRounds(follower, 2, APP_SOURCE_ID));
        check("no instance fenced", noFence());

        // 7. The cluster leader is stopped.
        final int killed = cluster.awaitLeader();
        stopApp(killed);
        if (killed < 2 && gateways[killed] != null) {
            stopGateway(killed);
        }
        check("the leader exited", cluster.stopMember(killed, Duration.ofSeconds(10)));
        final int newLeader = cluster.awaitLeader();
        check("another member leads", newLeader != killed);
        check("its replica opens its gate", poll(() -> appLeading(newLeader)));
        check("it publishes the application's rounds", waitRounds(newLeader, 2, APP_SOURCE_ID));
        final int gatewayAlive = killed < 2 ? 1 - killed : active;
        check("the gateway pair's rounds continue", waitRounds(gatewayAlive, 2, SOURCE_ID));
        cluster.startMember(killed, "seq-" + killed + "-restarted").awaitLine("serving replay",
                                                                              Duration.ofSeconds(30));
        startApp(killed);
        check("its replica restored a snapshot", apps[killed].lines("restoring round").size() > 0);
        if (killed < 2) {
            startGateway(killed);
            check("its gateway restored a snapshot", restores(killed) == 1);
        }
        check("rounds complete, the restored replica comparing them", waitRounds(killed, 2, APP_SOURCE_ID));
        check("no instance fenced", noFence());

        System.out.printf("markers sequenced: %d; latest gateway round, member 0 / 1: %d / %d%n", markers,
                          round(0, SOURCE_ID), round(1, SOURCE_ID));
        assertTrue(failures.isEmpty(), failures.size() + " check(s) failed: " + failures);
    }

    private void check(final String what, final boolean ok) {
        System.out.println((ok ? "  ok   " : "  FAIL ") + what);
        if (!ok) {
            failures.add(what);
        }
    }

    private void startGateway(final int m) {
        final String name = m == 0 ? "GW-T-A" : "GW-T-B";
        final ClusterProcesses.Child gateway = cluster.harness("gateway-" + name + "-" + ++gatewayRuns[m], m,
                                                               TEST_GATEWAY, "serve", "probe.clientId=10",
                                                               "probe.gatewayName=" + name,
                                                               "probe.listenPort=" + (TEST_GATEWAY_PORT_BASE + m),
                                                               "probe.snapshot=true", "probe.sourceId=" + SOURCE_ID,
                                                               "probe.snapshotDir=" + baseDir.resolve("snap")
                                                                   .resolve(name));
        gateways[m] = gateway;
        instances.add(gateway);
        gateway.awaitLine("Caught up", CATCHUP);
    }

    private void stopGateway(final int m) {
        gateways[m].terminate(Duration.ofSeconds(10));
    }

    private void startApp(final int m) {
        final ClusterProcesses.Child app = cluster.harness("app-" + m + "-" + ++appRuns[m], m, TEST_APPLICATION,
                                                           null, "probe.sourceId=" + APP_SOURCE_ID,
                                                           "probe.snapshotDir=" + baseDir.resolve("snap")
                                                               .resolve("app-" + m));
        apps[m] = app;
        instances.add(app);
        app.awaitLine("Caught up", CATCHUP);
    }

    private void stopApp(final int m) {
        apps[m].terminate(Duration.ofSeconds(10));
    }

    private boolean roundtrip(final int m, final int lines) {
        return cluster.harness("client-" + ++clients, m, TEST_GATEWAY, "client",
                               "probe.listenPort=" + (TEST_GATEWAY_PORT_BASE + m), "probe.count=" + lines)
            .awaitExit(Duration.ofSeconds(60)) == 0;
    }

    /** Whether the instance's last activation line says active; the lines are a state assignment. */
    private boolean isActive(final int m) {
        if (gateways[m] == null || !gateways[m].isAlive()) {
            return false;
        }
        String last = "";
        for (final String line : gateways[m].lines("is now")) {
            if (line.contains("is now active") || line.contains("is now standby")) {
                last = line;
            }
        }
        return last.contains("is now active");
    }

    private int awaitActiveMember() {
        final int[] active = {-1};
        assertTrue(poll(() -> {
            for (int m = 0; m < 2; m++) {
                if (isActive(m)) {
                    active[0] = m;
                    return true;
                }
            }
            return false;
        }), "no instance was designated active");
        return active[0];
    }

    private boolean waitActive(final int m) {
        return poll(() -> isActive(m) && !gateways[m].lines("gate OPEN").isEmpty());
    }

    private boolean appLeading(final int m) {
        String last = "";
        for (final String line : apps[m].lines("")) {
            if (line.contains("now leading") || line.contains("not leading")) {
                last = line;
            }
        }
        return last.contains("now leading");
    }

    /** The newest round the instance of a source on member {@code m} has confirmed, 0 for none since it started. */
    private long round(final int m, final int source) {
        final long value = cluster.counter(m, SeqeronCounters.APP_SNAPSHOT_ROUND_TYPE_ID, "snapshotRound source="
                                                                                              + source + " ");
        return value == ClusterProcesses.NO_VALUE ? 0 : value;
    }

    /** Waits until that instance has confirmed {@code n} rounds past where it is now, or past its first since it started. */
    private boolean waitRounds(final int m, final int n, final int source) {
        final long[] base = {0};
        if (!poll(() -> (base[0] = round(m, source)) > 0)) {
            return false;
        }
        return poll(() -> round(m, source) >= base[0] + n);
    }

    private boolean waitRoundConfirmed(final int m, final int round, final int source) {
        return poll(() -> round(m, source) >= round);
    }

    private boolean caughtUpWith(final int m, final int markers) {
        final List<String> lines = gateways[m].lines("Caught up");
        return !lines.isEmpty() && lines.get(lines.size() - 1).contains("0 connection(s) open, " + markers
                                                                          + " marker(s) sequenced");
    }

    private int restores(final int m) {
        return gateways[m].lines("restoring round").size();
    }

    private boolean noFence() {
        return instances.stream().allMatch(child -> child.lines("FENCED").isEmpty());
    }

    private static boolean poll(final BooleanSupplier condition) {
        final long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECS);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > end) {
                return false;
            }
            try {
                Thread.sleep(500);
            } catch (final InterruptedException ex) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return true;
    }
}
