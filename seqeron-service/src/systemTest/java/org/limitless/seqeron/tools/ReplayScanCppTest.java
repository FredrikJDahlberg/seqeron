package org.limitless.seqeron.tools;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

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
 * The C++ {@code ReplayScan} against a member's own Replayer. The scan is {@code replay_scan_probe}, whose path is
 * {@code REPLAY_SCAN_PROBE} in the environment of the Gradle run; without it the test is skipped. The probe reads the
 * default queue from its start, resumes from the position of the frame halfway through what it read, and asks to
 * resume one byte into a frame, which the Replayer refuses (spec R-2).
 */
class ReplayScanCppTest {
    private static final int MARKERS = 500;
    private static final Pattern FULL = Pattern.compile("full state=(\\w+) frames=(\\d+) first=(-?\\d+) last=(-?\\d+)");
    private static final Pattern RESUME = Pattern.compile("resume index=(\\d+) position=(-?\\d+) clusterSeqNo=(-?\\d+)");
    private static final Pattern RESUMED =
        Pattern.compile("resumed state=(\\w+) frames=(\\d+) first=(-?\\d+) last=(-?\\d+)");
    private static final Pattern BOGUS = Pattern.compile("bogus state=(\\w+) frames=(\\d+)");

    @TempDir
    Path baseDir;

    private ClusterProcesses cluster;

    @BeforeEach
    void startCluster(final TestInfo test) {
        assumeTrue(System.getProperty("seqeron.replayScanProbe") != null, "REPLAY_SCAN_PROBE is not set");
        cluster = new ClusterProcesses(baseDir, Path.of(System.getProperty("seqeron.systemTestLogs"),
                                                        test.getTestClass().orElseThrow().getSimpleName(),
                                                        test.getTestMethod().orElseThrow().getName()));
        cluster.startAll();
        cluster.loadTopology(Path.of(System.getProperty("seqeron.topologyGroups")));
    }

    @AfterEach
    void stopCluster() {
        if (cluster != null) {
            cluster.close();
        }
    }

    @Test
    @Timeout(value = 3, unit = TimeUnit.MINUTES)
    void readsTheQueueFromTheStartAndFromARecordedPosition() throws Exception {
        final ClusterProcesses.Child preload = cluster.probe("preload", 0, "submit", "probe.count=" + MARKERS);
        assertEquals(0, preload.awaitExit(Duration.ofSeconds(60)), "preload; see " + preload.log());
        // The Replayer derives the queue behind ingress, so the markers are waited for rather than assumed.
        Thread.sleep(3000);

        final ClusterProcesses.Child scan = cluster.nativeProcess("scan", System.getProperty("seqeron.replayScanProbe"),
                                                                  "--aeron-dir", cluster.aeronDir(0));
        assertEquals(0, scan.awaitExit(Duration.ofSeconds(60)), "the scan did not finish; see " + scan.log());

        final Matcher full = match(scan, "full state=", FULL);
        assertEquals("Done", full.group(1));
        final long frames = Long.parseLong(full.group(2));
        assertTrue(frames >= MARKERS, "the scan read " + frames + " frames of the " + MARKERS + " submitted");

        final Matcher resume = match(scan, "resume index=", RESUME);
        final long index = Long.parseLong(resume.group(1));
        final Matcher resumed = match(scan, "resumed state=", RESUMED);
        assertEquals("Done", resumed.group(1));
        assertEquals(frames - index, Long.parseLong(resumed.group(2)), "a resume delivers the frames from its own");
        assertEquals(resume.group(3), resumed.group(3), "and starts on the frame it asked for");
        assertEquals(full.group(4), resumed.group(4), "and ends where the full read did");

        final Matcher bogus = match(scan, "bogus state=", BOGUS);
        assertEquals("Failed", bogus.group(1), "a position that is no frame boundary is refused");
        assertEquals("0", bogus.group(2));
    }

    private static Matcher match(final ClusterProcesses.Child scan, final String prefix, final Pattern pattern) {
        final String line = scan.lastLine(prefix);
        assertTrue(line != null, "no '" + prefix + "' line; see " + scan.log());
        final Matcher matcher = pattern.matcher(line);
        assertTrue(matcher.find(), "unparsable: " + line);
        return matcher;
    }
}
