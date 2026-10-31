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
 * The C++ stub of a hosted Consumer (hosted::Consumer, doc/hosted-facade-draft.md §17.1) against the prototype host,
 * on a follower of a cluster whose leader hosts an Application stub publishing the markers. The stub is
 * {@code hosted_consumer_probe}, beside the {@code hosted_probe} that {@code HOSTED_PROBE} names; without them the
 * test is skipped.
 */
class HostedConsumerStubTest {
    private static final String HOST = "org.limitless.seqeron.proxy.ClientProxy";
    private static final int SOURCE_ID = 16;
    private static final int MARKERS = 500;
    private static final int TIMEOUT_SECS = 60;
    private static final Pattern MARKERS_SEEN = Pattern.compile("markers (\\d+)");

    @TempDir
    Path baseDir;

    private ClusterProcesses cluster;
    private ClusterProcesses.Child appHost;
    private ClusterProcesses.Child app;
    private ClusterProcesses.Child consumerHost;
    private ClusterProcesses.Child consumer;
    private int leader;
    private int follower;
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
        leader = cluster.awaitLeader();
        follower = (leader + 1) % ClusterProcesses.MEMBERS;
    }

    @AfterEach
    void stopCluster() {
        for (final ClusterProcesses.Child child : new ClusterProcesses.Child[] {consumer, app, consumerHost, appHost}) {
            if (child != null) {
                child.stop();
            }
        }
        if (cluster != null) {
            cluster.close();
        }
    }

    /** The consumer attaches while the markers are being published and sees each once, in order. */
    @Test
    @Timeout(value = 300, unit = TimeUnit.SECONDS)
    void aCppConsumerFollowsTheQueueLive() {
        startConsumerHost();
        startConsumer();
        check("the consumer caught up", poll(() -> consumer.lastLine("Caught up") != null));
        startApp();
        check("every marker reached the consumer", poll(() -> markersSeen() == MARKERS));
        verifyNoFaults();
        System.out.println(consumer.lastLine("state: "));
        assertTrue(failures.isEmpty(), failures.size() + " check(s) failed: " + failures);
    }

    /** The consumer attaches after the markers are in the log: the host replays the history to it, then goes live. */
    @Test
    @Timeout(value = 300, unit = TimeUnit.SECONDS)
    void aCppConsumerReplaysTheHistoryThenFollowsLive() {
        startApp();
        check("the app published every marker", poll(() -> app.lastLine("state: ") != null
                                                          && app.lastLine("state: ").contains("own " + MARKERS)));
        startConsumerHost();
        startConsumer();
        check("the consumer caught up", poll(() -> consumer.lastLine("Caught up") != null));
        check("every marker reached the consumer", poll(() -> markersSeen() == MARKERS));
        verifyNoFaults();
        System.out.println(consumer.lastLine("state: "));
        assertTrue(failures.isEmpty(), failures.size() + " check(s) failed: " + failures);
    }

    /** A consumer has no stream 304: the host judges it lost by its heartbeat counter alone (H-9), and takes the next. */
    @Test
    @Timeout(value = 300, unit = TimeUnit.SECONDS)
    void theHostLosesAKilledConsumerByItsCounter() {
        startConsumerHost();
        startConsumer();
        check("the consumer caught up", poll(() -> consumer.lastLine("Caught up") != null));
        consumer.kill();
        check("the host judged it lost", poll(() -> consumerHost.lastLine("CLIENT_LOST") != null));
        startConsumer();
        check("its successor attached", poll(() -> consumer.lastLine("Caught up") != null));
        check("the consumer was not fenced", consumer.lines("FENCED").isEmpty());
        assertTrue(failures.isEmpty(), failures.size() + " check(s) failed: " + failures);
    }

    private long markersSeen() {
        final String last = consumer.lastLine("state: ");
        if (last == null) {
            return 0;
        }
        final Matcher matcher = MARKERS_SEEN.matcher(last);
        return matcher.find() ? Long.parseLong(matcher.group(1)) : 0;
    }

    private void verifyNoFaults() {
        check("none was delivered out of order or twice", consumer.lines("OUT OF ORDER").isEmpty());
        check("the consumer's host did not fence", consumerHost.lines("FENCED").isEmpty());
        check("the consumer's host did not close the attachment", consumerHost.lines("CLIENT_").isEmpty());
        check("the consumer was not fenced", consumer.lines("FENCED").isEmpty());
    }

    private static Path consumerProbe() {
        return Path.of(System.getProperty("seqeron.hostedProbe", "")).resolveSibling("hosted_consumer_probe");
    }

    private void startApp() {
        appHost = cluster.harness("appHost", leader, HOST, null, "probe.sourceId=" + SOURCE_ID, "probe.clientId=31",
                                  "probe.snapshotDir=" + baseDir.resolve("snapApp"), "host.spin=true");
        appHost.awaitLine("host ready", Duration.ofSeconds(30));
        app = cluster.nativeProcess("app", System.getProperty("seqeron.hostedProbe"), "--aeron-dir",
                                    cluster.aeronDir(leader), "--source-id", "" + SOURCE_ID, "--interval-ms", "1",
                                    "--max-markers", "" + MARKERS);
    }

    private void startConsumerHost() {
        consumerHost = cluster.harness("consumerHost", follower, HOST, null, "probe.sourceId=" + SOURCE_ID,
                                       "probe.clientId=32", "host.consumerGroup=1",
                                       "probe.snapshotDir=" + baseDir.resolve("snapConsumer"), "host.spin=true");
        consumerHost.awaitLine("host ready", Duration.ofSeconds(30));
    }

    private void startConsumer() {
        consumer = cluster.nativeProcess("consumer", consumerProbe().toString(), "--aeron-dir",
                                         cluster.aeronDir(follower), "--source-id", "" + SOURCE_ID);
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
