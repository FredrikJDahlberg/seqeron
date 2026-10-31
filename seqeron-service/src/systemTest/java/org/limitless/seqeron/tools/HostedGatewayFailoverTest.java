package org.limitless.seqeron.tools;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.nio.file.Files;
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
 * A pair of hosted Gateways (hosted::Gateway, doc/hosted-facade-draft.md §17.5): GW-H-A is designated, GW-H-B is its
 * standby, each a host and a C++ stub, on different members. Needs {@code HOSTED_PROBE}, as the other hosted tests.
 */
class HostedGatewayFailoverTest {
    private static final String HOST = "org.limitless.seqeron.proxy.ClientProxy";
    private static final int SOURCE_ID = 9;
    private static final int CONNECTIONS = 3;
    private static final int TIMEOUT_SECS = 90;
    private static final Pattern FIRST = Pattern.compile("activated 1 first (\\d+)");

    @TempDir
    Path baseDir;

    private ClusterProcesses cluster;
    private ClusterProcesses.Child hostA;
    private ClusterProcesses.Child hostB;
    private ClusterProcesses.Child clientA;
    private ClusterProcesses.Child clientB;
    private int memberA;
    private int memberB;
    private final List<String> failures = new ArrayList<>();

    @BeforeEach
    void startCluster(final TestInfo test) {
        assumeTrue(System.getProperty("seqeron.hostedProbe") != null, "HOSTED_PROBE is not set");
        assumeTrue(Files.exists(probe()), probe() + " is not built");
        cluster = new ClusterProcesses(baseDir, Path.of(System.getProperty("seqeron.systemTestLogs"),
                                                        test.getTestClass().orElseThrow().getSimpleName(),
                                                        test.getTestMethod().orElseThrow().getName()));
        cluster.startAll();
        cluster.loadTopology(Path.of(System.getProperty("seqeron.snapshotTopology"))
                                 .resolveSibling("topology-test-hosted-gateway-pair.xml"));
        memberA = cluster.awaitLeader();
        memberB = (memberA + 1) % ClusterProcesses.MEMBERS;
    }

    @AfterEach
    void stopCluster() {
        for (final ClusterProcesses.Child child : new ClusterProcesses.Child[] {clientB, clientA, hostB, hostA}) {
            if (child != null) {
                child.stop();
            }
        }
        if (cluster != null) {
            cluster.close();
        }
    }

    /**
     * The active's stub dies while it holds three connections. Its host closes the façade, the standby is designated,
     * and its ids resume past the highest the first issued, so no connection is opened twice (H-4, H-9).
     */
    @Test
    @Timeout(value = 300, unit = TimeUnit.SECONDS)
    void theStandbyTakesOverWhenTheActiveClientDies() {
        startActive();
        startStandby();

        clientA.kill();
        check("the active's host lost its client", poll(() -> hostA.lastLine("CLIENT_LOST") != null));
        check("the standby was designated", poll(() -> clientB.lastLine("activated 1 first") != null));
        final long first = firstId(clientB.lastLine("activated 1 first"));
        check("its ids resumed past the active's (first " + first + ")", first >= CONNECTIONS);
        check("its ids were consecutive", poll(() -> clientB.lastLine("round 1 ids ok from " + first) != null));
        check("its markers came back under their connections",
              poll(() -> clientB.lastLine("round 1 sequenced 60") != null));
        check("the standby finished", poll(() -> clientB.lastLine("DONE") != null));

        verifyNoFaults();
        check("the active's host did not fence", hostA.lines("FENCED").isEmpty());
        assertTrue(failures.isEmpty(), failures.size() + " check(s) failed: " + failures);
    }

    /**
     * The active's host stops answering. Its stub gives the host up (H-9) and the cluster times its session out, so
     * the standby is designated and resumes the ids.
     */
    @Test
    @Timeout(value = 300, unit = TimeUnit.SECONDS)
    void theStandbyTakesOverWhenTheActiveHostIsPaused() {
        startActive();
        startStandby();

        hostA.pause();
        try {
            check("the active's stub gave up its host", poll(() -> clientA.lastLine("HOST_LOST") != null));
            check("the standby was designated", poll(() -> clientB.lastLine("activated 1 first") != null));
            final long first = firstId(clientB.lastLine("activated 1 first"));
            check("its ids resumed past the active's (first " + first + ")", first >= CONNECTIONS);
            check("its ids were consecutive", poll(() -> clientB.lastLine("round 1 ids ok from " + first) != null));
            check("its markers came back under their connections",
                  poll(() -> clientB.lastLine("round 1 sequenced 60") != null));
            check("the standby finished", poll(() -> clientB.lastLine("DONE") != null));
        } finally {
            hostA.resume();
        }
        verifyNoFaults();
        assertTrue(failures.isEmpty(), failures.size() + " check(s) failed: " + failures);
    }

    /**
     * The paused active comes back, its stub still attached. It stands down: it opens no connection after the
     * standby's takeover and the standby carries on (H-3, H-4).
     */
    @Test
    @Timeout(value = 300, unit = TimeUnit.SECONDS)
    void aResumedActiveDoesNotServeAgain() {
        startActive("--host-timeout-ms", "120000");
        startStandby();

        hostA.pause();
        try {
            check("the standby was designated", poll(() -> clientB.lastLine("activated 1 first") != null));
            check("the standby finished", poll(() -> clientB.lastLine("DONE") != null));
        } finally {
            hostA.resume();
        }
        final int opened = hostA.lines("opened connection").size();
        check("the resumed active stood down or was fenced",
              poll(() -> clientA.lastLine("standby") != null || hostA.lastLine("FENCED") != null
                      || hostA.lastLine("CLIENT_LOST") != null));
        pause(3000);
        check("it opened no connection after the takeover", hostA.lines("opened connection").size() == opened);
        check("its stub did not serve again", clientA.lines("activated 2").isEmpty());
        verifyNoFaults();
        assertTrue(failures.isEmpty(), failures.size() + " check(s) failed: " + failures);
    }

    private void verifyNoFaults() {
        check("no marker came back under another connection",
              clientA.lines("BAD CONNECTION").isEmpty() && clientB.lines("BAD CONNECTION").isEmpty());
        check("no id was out of order", clientB.lines("BAD IDS").isEmpty());
        check("the standby's host did not fence", hostB.lines("FENCED").isEmpty());
        check("the standby's host did not close its attachment", hostB.lines("CLIENT_").isEmpty());
        check("the standby's stub was not fenced", clientB.lines("FENCED").isEmpty());
    }

    /** GW-H-A is designated, serves three connections and holds them open, publishing steadily. */
    private void startActive(final String... clientArguments) {
        hostA = startHost("hostA", memberA, "GW-H-A", 31);
        final List<String> arguments = new ArrayList<>(List.of("--hold", "1"));
        arguments.addAll(List.of(clientArguments));
        clientA = startClient("clientA", memberA, arguments.toArray(new String[0]));
        check("the active was designated", poll(() -> clientA.lastLine("activated 1 first 0") != null));
        check("its ids ran on from the Role's", poll(() -> clientA.lastLine("round 1 ids ok from 0") != null));
        check("its markers came back under their connections",
              poll(() -> clientA.lastLine("round 1 sequenced 60") != null));
    }

    /** GW-H-B attaches and is not activated while the active lives. */
    private void startStandby() {
        hostB = startHost("hostB", memberB, "GW-H-B", 32);
        clientB = startClient("clientB", memberB);
        check("the standby attached", poll(() -> hostB.lastLine("ATTACHED") != null));
        pause(3000);
        check("the standby was not activated while the active lived", clientB.lastLine("activated") == null);
    }

    private static long firstId(final String line) {
        if (line == null) {
            return -1;
        }
        final Matcher matcher = FIRST.matcher(line);
        return matcher.find() ? Long.parseLong(matcher.group(1)) : -1;
    }

    private static Path probe() {
        return Path.of(System.getProperty("seqeron.hostedProbe", "")).resolveSibling("hosted_gateway_probe");
    }

    private ClusterProcesses.Child startHost(final String name, final int member, final String gateway,
                                             final int clientId) {
        final ClusterProcesses.Child host = cluster.harness(name, member, HOST, null, "probe.sourceId=" + SOURCE_ID,
                                                            "probe.clientId=" + clientId, "probe.gatewayName=" + gateway,
                                                            "probe.snapshotDir=" + baseDir.resolve("snap-" + name),
                                                            "host.spin=true");
        host.awaitLine("host ready", Duration.ofSeconds(30));
        return host;
    }

    private ClusterProcesses.Child startClient(final String name, final int member, final String... arguments) {
        final List<String> command = new ArrayList<>(List.of(probe().toString(), "--aeron-dir", cluster.aeronDir(member),
                                                             "--source-id", "" + SOURCE_ID, "--connections",
                                                             "" + CONNECTIONS, "--per-connection", "20"));
        command.addAll(List.of(arguments));
        return cluster.nativeProcess(name, command.toArray(new String[0]));
    }

    private void check(final String what, final boolean ok) {
        System.out.println((ok ? "  ok   " : "  FAIL ") + what);
        if (!ok) {
            failures.add(what);
        }
    }

    private static void pause(final long millis) {
        try {
            Thread.sleep(millis);
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }

    private static boolean poll(final BooleanSupplier condition) {
        final long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECS);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > end) {
                return false;
            }
            try {
                Thread.sleep(250);
            } catch (final InterruptedException ex) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return true;
    }
}
