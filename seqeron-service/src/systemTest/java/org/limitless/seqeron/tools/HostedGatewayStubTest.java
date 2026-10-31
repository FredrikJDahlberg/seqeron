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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * The C++ stub of a hosted Gateway (hosted::Gateway, doc/hosted-facade-draft.md §17.5) against the prototype host.
 * The stub is {@code hosted_gateway_probe}, beside the {@code hosted_probe} that {@code HOSTED_PROBE} names; without
 * it the test is skipped.
 */
class HostedGatewayStubTest {
    private static final String HOST = "org.limitless.seqeron.tools.HostedTestApplicationHost";
    private static final int SOURCE_ID = 9;
    private static final int TIMEOUT_SECS = 60;

    @TempDir
    Path baseDir;

    private ClusterProcesses cluster;
    private ClusterProcesses.Child host;
    private ClusterProcesses.Child client;
    private int leader;
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
                                 .resolveSibling("topology-test-hosted-gateway.xml"));
        leader = cluster.awaitLeader();
    }

    @AfterEach
    void stopCluster() {
        if (client != null) {
            client.stop();
        }
        if (host != null) {
            host.stop();
        }
        if (cluster != null) {
            cluster.close();
        }
    }

    /**
     * The stub opens three connections under the ids the Role named, publishes on each, and every marker comes back
     * under its own connection; the closes are announced (H-4, H-5).
     */
    @Test
    @Timeout(value = 300, unit = TimeUnit.SECONDS)
    void aCppGatewayOpensConnectionsUnderItsOwnIds() {
        startHost();
        startClient("--connections", "3", "--per-connection", "20", "--rounds", "1");
        check("the designation reached the stub", poll(() -> client.lastLine("activated 1 first 0") != null));
        check("the ids ran on from the Role's", poll(() -> client.lastLine("round 1 ids ok from 0") != null));
        check("the host opened three connections", poll(() -> host.lines("opened connection").size() == 3));
        check("every marker came back under its connection", poll(() -> client.lastLine("round 1 sequenced 60") != null));
        check("the façade announced each connection", poll(() -> client.lines("connection opened").size() == 3));
        check("the façade announced each close", poll(() -> client.lines("connection closed").size() == 3));
        check("the stub finished", poll(() -> client.lastLine("DONE") != null));
        verifyNoFaults();
        System.out.println(host.lastLine("queue:"));
        System.out.println(client.lastLine("state: "));
        assertTrue(failures.isEmpty(), failures.size() + " check(s) failed: " + failures);
    }

    /** The stub closes its own edge; the host asks again and the ids resume past the highest issued (H-4, H-3). */
    @Test
    @Timeout(value = 300, unit = TimeUnit.SECONDS)
    void aCppGatewayResumesItsIdsAfterItsEdgeClosed() {
        startHost();
        startClient("--connections", "3", "--per-connection", "20", "--rounds", "2");
        check("the first round finished", poll(() -> client.lastLine("round 1 closed") != null));
        check("the host asked again", poll(() -> client.lastLine("activated 2 first 3") != null));
        check("the second round's ids resumed", poll(() -> client.lastLine("round 2 ids ok from 3") != null));
        check("every marker of the second round came back under its connection",
              poll(() -> client.lastLine("round 2 sequenced 120") != null));
        check("the stub finished", poll(() -> client.lastLine("DONE") != null));
        check("the host opened six connections", host.lines("opened connection").size() == 6);
        verifyNoFaults();
        System.out.println(host.lastLine("queue:"));
        System.out.println(client.lastLine("state: "));
        assertTrue(failures.isEmpty(), failures.size() + " check(s) failed: " + failures);
    }

    private void verifyNoFaults() {
        check("no marker came back under another connection", client.lines("BAD CONNECTION").isEmpty());
        check("no id was out of order", client.lines("BAD IDS").isEmpty());
        check("the host did not fence", host.lines("FENCED").isEmpty());
        check("the host did not close the attachment", host.lines("CLIENT_").isEmpty());
        check("the stub was not fenced", client.lines("FENCED").isEmpty());
    }

    private static Path probe() {
        return Path.of(System.getProperty("seqeron.hostedProbe", "")).resolveSibling("hosted_gateway_probe");
    }

    private void startHost() {
        host = cluster.harness("host", leader, HOST, null, "probe.sourceId=" + SOURCE_ID, "probe.clientId=31",
                               "probe.gatewayName=GW-H-A", "probe.snapshotDir=" + baseDir.resolve("snap"),
                               "host.spin=true");
        host.awaitLine("host ready", Duration.ofSeconds(30));
    }

    private void startClient(final String... arguments) {
        final List<String> command = new ArrayList<>(List.of(probe().toString(), "--aeron-dir", cluster.aeronDir(leader),
                                                             "--source-id", "" + SOURCE_ID));
        command.addAll(List.of(arguments));
        client = cluster.nativeProcess("client", command.toArray(new String[0]));
    }

    private void check(final String what, final boolean ok) {
        System.out.println((ok ? "  ok   " : "  FAIL ") + what);
        if (!ok) {
            failures.add(what);
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
