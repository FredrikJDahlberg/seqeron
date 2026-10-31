package org.limitless.seqeron.tools;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * The hosted-façade prototype's host holds its steady state to no per-frame allocation (doc/hosted-facade-draft.md §17.9):
 * its loop thread, running the real {@code Application} on the leader, allocates nothing across a window of delivered
 * payloads, published markers and heartbeats once the JIT has warmed (at most {@link #MAX_BYTES} over the window). There is no snapshot policy, so no cut is in
 * the window, and the window must see no change of gate epoch, whose log line allocates.
 */
class HostedAllocationTest {
    private static final String HOST = "org.limitless.seqeron.tools.HostedTestApplicationHost";
    private static final String CLIENT = "org.limitless.seqeron.tools.HostedTestApplicationClient";
    private static final int SOURCE_ID = 16;
    private static final int WARMUP_FRAMES = 100_000;
    private static final int WINDOW_FRAMES = 50_000;
    /** A one-off object in a window of a minute is not a per-frame cost; a cold loop allocates about 200 KB. */
    private static final long MAX_BYTES = 1024;
    private static final Pattern WINDOW = Pattern.compile("alloc window: (-?\\d+) bytes over (\\d+) frames, gate epoch (\\d+) to (\\d+)");

    @TempDir
    Path baseDir;

    private ClusterProcesses cluster;
    private ClusterProcesses.Child host;
    private ClusterProcesses.Child client;

    @BeforeEach
    void startCluster(final TestInfo test) {
        cluster = new ClusterProcesses(baseDir, Path.of(System.getProperty("seqeron.systemTestLogs"),
                                                        test.getTestClass().orElseThrow().getSimpleName()));
        cluster.startAll();
    }

    @AfterEach
    void stopCluster() {
        if (client != null) {
            client.stop();
        }
        if (host != null) {
            host.stop();
        }
        cluster.close();
    }

    @Test
    @Timeout(value = 300, unit = TimeUnit.SECONDS)
    void theHostsSteadyStateAllocatesNothing() {
        cluster.loadTopology(Path.of(System.getProperty("seqeron.snapshotTopology"))
                                 .resolveSibling("topology-test-hosted-quiet.xml"));
        final int leader = cluster.awaitLeader();
        host = cluster.harness("host", leader, HOST, null, "probe.sourceId=" + SOURCE_ID, "probe.clientId=31",
                               "probe.snapshotDir=" + baseDir.resolve("snap"),
                               "host.allocWarmupFrames=" + WARMUP_FRAMES, "host.allocWindowFrames=" + WINDOW_FRAMES);
        host.awaitLine("host ready", Duration.ofSeconds(30));
        client = cluster.harness("client", leader, CLIENT, null, "probe.sourceId=" + SOURCE_ID, "probe.intervalMs=1");
        host.awaitLine("alloc window", Duration.ofSeconds(240));

        final Matcher matcher = WINDOW.matcher(host.lastLine("alloc window"));
        matcher.find();
        System.out.println(host.lastLine("alloc window"));
        assertEquals(matcher.group(3), matcher.group(4), "the gate epoch moved inside the window");
        final long bytes = Long.parseLong(matcher.group(1));
        if (bytes < 0 || bytes > MAX_BYTES) {
            System.out.println("host lines from the window:");
            host.lines("[host").forEach(System.out::println);
        }
        assertTrue(bytes >= 0 && bytes <= MAX_BYTES, "bytes the host allocated over " + matcher.group(2) + " frames: " + bytes);
    }
}
