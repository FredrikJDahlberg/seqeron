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
 * Two proxies on one media driver (doc/hosted-facade-draft.md §17.2): an Application's and a Consumer's, each named by
 * {@code proxy.hostName} and answering only an {@code Attach} that names it, though both read stream 301 and write
 * stream 302. Needs {@code HOSTED_PROBE} and the {@code hosted_consumer_probe} beside it.
 */
class HostedTwoHostsTest {
    private static final String APP_HOST = "alpha";
    private static final String CONSUMER_HOST = "beta";
    private static final int SOURCE_ID = 16;
    private static final int MARKERS = 500;
    private static final int TIMEOUT_SECS = 60;

    @TempDir
    Path baseDir;

    private ClusterProcesses cluster;
    private final List<ClusterProcesses.Child> children = new ArrayList<>();
    private final List<String> failures = new ArrayList<>();

    @BeforeEach
    void startCluster(final TestInfo test) {
        assumeTrue(System.getProperty("seqeron.hostedProbe") != null, "HOSTED_PROBE is not set");
        assumeTrue(Files.exists(consumerProbe()), consumerProbe() + " is not built");
        cluster = new ClusterProcesses(baseDir, Path.of(System.getProperty("seqeron.systemTestLogs"),
                                                        test.getTestClass().orElseThrow().getSimpleName(),
                                                        test.getTestMethod().orElseThrow().getName()));
        cluster.startAll();
        cluster.loadTopology(Path.of(System.getProperty("seqeron.snapshotTopology"))
                                 .resolveSibling("topology-test-hosted-quiet.xml"));
    }

    @AfterEach
    void stopCluster() {
        for (int i = children.size() - 1; i >= 0; i--) {
            children.get(i).stop();
        }
        if (cluster != null) {
            cluster.close();
        }
    }

    /** Each client is answered by its own proxy alone: the consumer on the app's driver sees every marker once. */
    @Test
    @Timeout(value = 300, unit = TimeUnit.SECONDS)
    void twoProxiesShareOneDriver() {
        final int leader = cluster.awaitLeader();
        final ClusterProcesses.Child appHost = track(cluster.proxy("appHost", leader, "proxy.hostName=" + APP_HOST,
                                                                   "proxy.sourceId=" + SOURCE_ID, "proxy.clientId=31",
                                                                   "proxy.snapshotDir=" + baseDir.resolve("snapApp"),
                                                                   "proxy.spin=true"));
        final ClusterProcesses.Child consumerHost = track(cluster.proxy(
            "consumerHost", leader, "proxy.hostName=" + CONSUMER_HOST, "proxy.sourceId=" + SOURCE_ID,
            "proxy.clientId=32", "proxy.consumerGroup=1", "proxy.snapshotDir=" + baseDir.resolve("snapConsumer"),
            "proxy.spin=true"));
        appHost.awaitLine("host ready", Duration.ofSeconds(30));
        consumerHost.awaitLine("host ready", Duration.ofSeconds(30));
        final ClusterProcesses.Child consumer = track(cluster.nativeProcess(
            "consumer", consumerProbe().toString(), "--aeron-dir", cluster.aeronDir(leader), "--source-id",
            "" + SOURCE_ID, "--host-name", CONSUMER_HOST));
        check("the consumer caught up", poll(() -> consumer.lastLine("Caught up") != null));
        final ClusterProcesses.Child app = track(cluster.nativeProcess(
            "app", System.getProperty("seqeron.hostedProbe"), "--aeron-dir", cluster.aeronDir(leader), "--source-id",
            "" + SOURCE_ID, "--host-name", APP_HOST, "--interval-ms", "1", "--max-markers", "" + MARKERS));
        check("the app published every marker", poll(() -> app.lastLine("state: ") != null
                                                          && app.lastLine("state: ").contains("own " + MARKERS)));
        check("every marker reached the consumer", poll(() -> consumer.lastLine("state: ") != null
                                                              && consumer.lastLine("state: ").contains("markers " + MARKERS)));
        check("the consumer saw none out of order or twice", consumer.lines("OUT OF ORDER").isEmpty());
        check("neither proxy fenced", appHost.lines("FENCED").isEmpty() && consumerHost.lines("FENCED").isEmpty());
        check("neither proxy closed an attachment", appHost.lines("CLIENT_").isEmpty()
                                                    && consumerHost.lines("CLIENT_").isEmpty());
        check("neither client was refused", app.lines("BUSY").isEmpty() && consumer.lines("BUSY").isEmpty());
        assertTrue(failures.isEmpty(), failures.size() + " check(s) failed: " + failures);
    }

    private ClusterProcesses.Child track(final ClusterProcesses.Child child) {
        children.add(child);
        return child;
    }

    private static Path consumerProbe() {
        return Path.of(System.getProperty("seqeron.hostedProbe", "")).resolveSibling("hosted_consumer_probe");
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
