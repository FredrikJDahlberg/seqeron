package org.limitless.seqeron.tools;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * A cold replica replays a member's recorded history to caught up. It was written to chase a cold
 * start that never converged past about 32 MiB of history, the shared-replay-stream flow-control wedge
 * {@code ReplayerStreamReceiver.openReplaySubscription} documents, so a run that never catches up is that class of
 * bug and not a slow machine: the test asserts convergence within the cap and reports the rate without asserting it.
 * {@code REPLAY_PRELOAD} frames (default 20,000; 400,000 is about 70 MB, the case that used to hang) are flooded at
 * ingress first, padded to the size of the NewOrderSingle the flood once was. The second case keeps flooding four
 * times as many while the replica catches up, a moving target. Elapsed includes the replica's JVM start, a fixed cost
 * that dominates at small preloads. {@code TIMEOUT_SCALE} stretches the cap.
 */
class ReplayColdStartTest {
    private static final int PRELOAD = Integer.parseInt(System.getenv().getOrDefault("REPLAY_PRELOAD", "20000"));
    private static final double SCALE = Double.parseDouble(System.getenv().getOrDefault("TIMEOUT_SCALE", "1"));
    private static final Duration CAP = Duration.ofMillis((long)(120_000 * SCALE));
    // NewOrderSingle is a 185-byte block and an 8-byte header; a ProbeMarker is that header, an 8-byte seqNo and a
    // 2-byte length, so 175 bytes of filler makes the two the same size on the wire and in the archive.
    private static final String FILLER = "probe.fillerBytes=175";
    private static final int COLD_MEMBER = 1;

    @TempDir
    Path baseDir;

    private ClusterProcesses cluster;

    @BeforeEach
    void startCluster(final TestInfo test) {
        cluster = new ClusterProcesses(baseDir, Path.of(System.getProperty("seqeron.systemTestLogs"),
                                                        test.getTestClass().orElseThrow().getSimpleName(),
                                                        test.getTestMethod().orElseThrow().getName()));
        cluster.startAll();
        cluster.loadTopology(Path.of(System.getProperty("seqeron.topologyGroups")));
    }

    @AfterEach
    void stopCluster() {
        cluster.close();
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.MINUTES)
    void coldReplicaCatchesUpOnAQuietCluster() throws Exception {
        replay(false);
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.MINUTES)
    void coldReplicaCatchesUpWhileTheClusterKeepsSequencing() throws Exception {
        replay(true);
    }

    private void replay(final boolean loadDuring) throws Exception {
        final ClusterProcesses.Child preload =
            cluster.probe("preload", 0, "submit", "probe.count=" + PRELOAD, FILLER);
        assertEquals(0, preload.awaitExit(CAP), "preload; see " + preload.log());
        // The archive flushes behind the flood; the size is counted after it, as the recording is what is replayed.
        Thread.sleep(3000);
        final long archiveBytes = size(baseDir.resolve("archive-" + COLD_MEMBER));
        if (loadDuring) {
            cluster.probe("load", 0, "submit", "probe.count=" + PRELOAD * 4, FILLER);
        }

        // A replica of its own client id on member 1's Replayer walks the whole recording from scratch, as a
        // restarted one does, while everything else keeps running.
        final long start = System.nanoTime();
        final ClusterProcesses.Child cold = cluster.probe("cold", COLD_MEMBER, "follow", "probe.clientId=7");
        cold.awaitLine("Caught up", CAP);
        final double seconds = (System.nanoTime() - start) / 1e9;
        final double megabytes = archiveBytes / 1e6;
        System.out.printf("archive=%.1f MB  load-during=%s  elapsed=%.2fs  rate=%.1f MB/s%n", megabytes,
                          loadDuring ? "yes" : "no", seconds, megabytes / seconds);

        // Caught up must mean it read the history: the probe reports the frames it delivered in order on exit.
        assertTrue(cold.terminate(Duration.ofSeconds(10)), "the replica did not exit; see " + cold.log());
        final String delivered = cold.lastLine("frames delivered in order:");
        assertTrue(delivered != null && Long.parseLong(delivered.replaceAll("\\D", "")) >= PRELOAD,
                   "the replica delivered fewer than the " + PRELOAD + " preloaded frames: " + delivered);
    }

    /** The blocks the directory occupies, as {@code du} counts them: its segment files are sparse, so lengths lie. */
    private static long size(final Path directory) throws IOException, InterruptedException {
        final Process du = new ProcessBuilder("du", "-sk", directory.toString()).redirectErrorStream(true).start();
        final String output = new String(du.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        du.waitFor();
        return Long.parseLong(output.trim().split("\\s+")[0]) * 1024;
    }
}
