package org.limitless.seqeron.tools;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
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

/**
 * A client whose cluster session is lost opens a new one rather than fencing, both when the
 * client went quiet and when the cluster went away, and its duty cycle keeps running while an attempt is pending; and
 * a gateway standby that loses its session in the same outage as the active instance takes the role over rather than
 * fencing too. The client is {@code TestApplication} on a follower. Its IPC ingress attempt never answers on a
 * follower and times out after IPC_CONNECT_TIMEOUT_MS, so a replacement that blocked the duty cycle would log a
 * heartbeat silence at least that long, where the pause alone logs one about a second longer than itself.
 *
 * <ol>
 *   <li>The application is SIGSTOPped past the cluster's 5 s session timeout, then resumed.
 *   <li>The other two members are SIGSTOPped long enough for the client's 5 s new-leader timeout to close its
 *       session, short enough for the members' 10 s service interval to survive.
 *   <li>GW-T-A, the designated gateway, on the leader and GW-T-B, the standby, beside the application are both
 *       stopped. The cluster times out both sessions and GW-T-A's closing promotes GW-T-B, which replaces its lost
 *       session and opens its gate; GW-T-A steps down rather than fencing.
 * </ol>
 */
class SessionReplaceTest {
    private static final String TEST_APPLICATION = "org.limitless.seqeron.tools.TestApplication";
    private static final String TEST_GATEWAY = "org.limitless.seqeron.tools.TestGateway";
    private static final int TEST_GATEWAY_PORT_BASE = 9200;
    private static final long CLIENT_PAUSE_SECS = Long.parseLong(System.getenv().getOrDefault("CLIENT_PAUSE_SECS", "7"));
    private static final long QUORUM_PAUSE_SECS = Long.parseLong(System.getenv().getOrDefault("QUORUM_PAUSE_SECS", "7"));
    // Past the 5 s session timeout, inside the standby's 5 s activation timeout.
    private static final long GATEWAY_PAUSE_SECS = Long.parseLong(System.getenv().getOrDefault("GATEWAY_PAUSE_SECS",
                                                                                              "7"));
    // Well past CLIENT_PAUSE_SECS, well inside the 20 s the facade allows a replacement.
    private static final int IPC_CONNECT_TIMEOUT_MS = 12_000;
    // Between the pause's silence and the IPC timeout's.
    private static final long SILENCE_LIMIT_MS = 10_000;
    private static final int REPLACE_TIMEOUT_SECS = 40;
    private static final Pattern SILENCE = Pattern.compile("resumed after (\\d+)ms");

    @TempDir
    Path baseDir;

    private ClusterProcesses cluster;
    private final List<ClusterProcesses.Child> paused = new ArrayList<>();

    @BeforeEach
    void startCluster(final TestInfo test) {
        cluster = new ClusterProcesses(baseDir, Path.of(System.getProperty("seqeron.systemTestLogs"),
                                                        test.getTestClass().orElseThrow().getSimpleName()));
        cluster.startAll();
    }

    @AfterEach
    void stopCluster() {
        paused.forEach(ClusterProcesses.Child::resume);
        cluster.close();
    }

    @Test
    @Timeout(value = 300, unit = TimeUnit.SECONDS)
    void sessionsAreReplacedWithoutFencingOrStoppingTheDutyCycle() throws Exception {
        final int leader = cluster.awaitLeader();
        final int appMember = (leader + 1) % ClusterProcesses.MEMBERS;
        // The groups and the application's row first: the cluster refuses everything before them.
        cluster.loadTopology(Path.of(System.getProperty("seqeron.topologyGroups")));

        final ClusterProcesses.Child app = cluster.harness("app", appMember, TEST_APPLICATION, null,
                                                           "probe.snapshotDir=" + baseDir.resolve("snapshots"),
                                                           "probe.ipcConnectTimeoutMs=" + IPC_CONNECT_TIMEOUT_MS);
        app.awaitLine("Caught up", Duration.ofSeconds(30));
        Thread.sleep(2000);

        final int phase1Before = replaced(app);
        final int phase1Line = app.lines("").size();
        pause(List.of(app), CLIENT_PAUSE_SECS);
        awaitReplaced(app, phase1Before);
        final int phase1 = replaced(app) - phase1Before;
        Thread.sleep(2000);
        long silenceMs = 0;
        final List<String> phase1Lines = app.lines("");
        for (final String line : phase1Lines.subList(Math.min(phase1Line, phase1Lines.size()), phase1Lines.size())) {
            final Matcher matcher = SILENCE.matcher(line);
            if (matcher.find()) {
                silenceMs = Math.max(silenceMs, Long.parseLong(matcher.group(1)));
            }
        }

        final int phase2Before = replaced(app);
        final List<ClusterProcesses.Child> others = new ArrayList<>();
        for (int m = 0; m < ClusterProcesses.MEMBERS; m++) {
            if (m != appMember) {
                others.add(cluster.memberLog(m));
            }
        }
        pause(others, QUORUM_PAUSE_SECS);
        awaitReplaced(app, phase2Before);
        final int phase2 = replaced(app) - phase2Before;
        Thread.sleep(2000);

        // Without its <snapshots> element: the consensus module resets every session's inactivity clock at each
        // snapshot, so a round every 2 s would keep the paused pair's sessions from ever timing out.
        final Path noRounds = baseDir.resolve("topology-no-rounds.xml");
        ClusterProcesses.withoutSnapshots(Path.of(System.getProperty("seqeron.gatewayTopology")), noRounds);
        cluster.loadTopology(noRounds);
        final ClusterProcesses.Child gatewayA = gateway(leader, "GW-T-A", 0);
        final ClusterProcesses.Child gatewayB = gateway(appMember, "GW-T-B", 1);
        gatewayA.awaitLine("gate OPEN", Duration.ofSeconds(30));
        gatewayB.awaitLine("Caught up", Duration.ofSeconds(30));
        Thread.sleep(2000);
        final int gatewayBLine = gatewayB.lines("").size();
        pause(List.of(gatewayA, gatewayB), GATEWAY_PAUSE_SECS);
        poll(() -> afterLine(gatewayB, gatewayBLine, "gate OPEN") + afterLine(gatewayB, gatewayBLine, "FENCED") > 0,
             REPLACE_TIMEOUT_SECS);
        Thread.sleep(2000);
        final int gatewayBOpen = afterLine(gatewayB, gatewayBLine, "gate OPEN");
        final int gatewayBFenced = gatewayB.lines("FENCED").size();

        final List<Integer> down = new ArrayList<>();
        for (int m = 0; m < ClusterProcesses.MEMBERS; m++) {
            if (!cluster.isRunning(m)) {
                down.add(m);
            }
        }
        final boolean appUp = app.isAlive();
        final int fenced = app.lines("FENCED").size();
        System.out.printf("members down: %s; replacements: paused %d, quorum lost %d; longest silence %d ms%n"
                          + "application running / fences: %b / %d; GW-T-B gate opened / fences: %d / %d%n",
                          down, phase1, phase2, silenceMs, appUp, fenced, gatewayBOpen, gatewayBFenced);
        final long silence = silenceMs;

        assertAll(
            () -> assertTrue(down.isEmpty(), "members down: " + down),
            () -> assertTrue(phase1 >= 1, "no replacement after the application was paused"),
            () -> assertTrue(silence < SILENCE_LIMIT_MS, "heartbeat silence " + silence + " ms during phase 1"),
            () -> assertTrue(phase2 >= 1, "no replacement after the quorum was lost"),
            () -> assertTrue(appUp, "the application exited"),
            () -> assertEquals(0, fenced, "the application was fenced"),
            () -> assertTrue(gatewayBOpen >= 1, "GW-T-B did not open its gate after the pause"),
            () -> assertEquals(0, gatewayBFenced, "GW-T-B was fenced"));
    }

    private ClusterProcesses.Child gateway(final int member, final String name, final int portIndex) {
        return cluster.harness("gateway-" + name, member, TEST_GATEWAY, "serve", "probe.clientId=10",
                               "probe.gatewayName=" + name, "probe.listenPort=" + (TEST_GATEWAY_PORT_BASE + portIndex),
                               "probe.snapshot=true", "probe.snapshotDir=" + baseDir.resolve("snapshots").resolve(name));
    }

    private void pause(final List<ClusterProcesses.Child> children, final long seconds) throws InterruptedException {
        paused.addAll(children);
        children.forEach(ClusterProcesses.Child::pause);
        Thread.sleep(TimeUnit.SECONDS.toMillis(seconds));
        children.forEach(ClusterProcesses.Child::resume);
    }

    private static int replaced(final ClusterProcesses.Child app) {
        return app.lines("replaces the lost one").size();
    }

    private static boolean awaitReplaced(final ClusterProcesses.Child app, final int before)
        throws InterruptedException {
        return poll(() -> replaced(app) > before, REPLACE_TIMEOUT_SECS);
    }

    private static int afterLine(final ClusterProcesses.Child child, final int line, final String text) {
        final List<String> lines = child.lines("");
        int count = 0;
        for (final String l : lines.subList(Math.min(line, lines.size()), lines.size())) {
            if (l.contains(text)) {
                count++;
            }
        }
        return count;
    }

    private static boolean poll(final BooleanSupplier condition, final int seconds) throws InterruptedException {
        final long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > end) {
                return false;
            }
            Thread.sleep(500);
        }
        return true;
    }
}
