package org.limitless.seqeron.tools;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
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
 * replayer-restart-test.sh: an app running through its own node's restart, and a cold start across a genuine
 * recording rotation. Member 0, started last after members 1 and 2 elect between themselves, is never the leader. A
 * backlog is flooded and a follow on member 0 catches up; member 0 is then SIGTERMed. The follow shares its embedded
 * media driver and must fail fast, within the driver-loss grace window, rather than spin against a dead driver.
 * Member 0 restarts, with no snapshots, by a full-log replay onto a brand new queue recording; a fresh follow must
 * then catch up from that recording alone.
 */
class ReplayerRestartTest {
    private static final int FLOOD_FRAMES = 1000;
    private static final int TARGET = 0;
    private static final Pattern RECORDING = Pattern.compile("queue recording (\\d+) live");
    private static final Pattern REPLY_ID = Pattern.compile("requests replays as client (\\d+)");

    @TempDir
    Path baseDir;

    private ClusterProcesses cluster;

    @BeforeEach
    void startCluster(final TestInfo test) {
        cluster = new ClusterProcesses(baseDir, Path.of(System.getProperty("seqeron.systemTestLogs"),
                                                        test.getTestClass().orElseThrow().getSimpleName()));
    }

    @AfterEach
    void stopCluster() {
        cluster.close();
    }

    @Test
    @Timeout(value = 300, unit = TimeUnit.SECONDS)
    void freshClientCatchesUpFromTheRestartedNodesNewRecordingAlone() {
        cluster.startMember(1, "seq-1");
        cluster.startMember(2, "seq-2");
        cluster.awaitActive();
        cluster.awaitLeader();
        final ClusterProcesses.Child first = cluster.startMember(TARGET, "seq-" + TARGET);
        cluster.awaitActive();
        cluster.loadTopology(Path.of(System.getProperty("seqeron.topologyGroups")));
        first.awaitLine("serving replay", Duration.ofSeconds(60));

        cluster.probe("flood", TARGET, "submit", "probe.count=" + FLOOD_FRAMES).awaitExit(Duration.ofSeconds(60));
        final ClusterProcesses.Child before = cluster.probe("client-before", TARGET, "follow", "probe.clientId=9");
        before.awaitLine("following live", Duration.ofSeconds(30));

        // The node dies and its co-located client must fail fast; Aeron's driver timeout is 10 s.
        final boolean memberExited = cluster.stopMember(TARGET, Duration.ofSeconds(10));
        boolean clientDied = true;
        try {
            before.awaitExit(Duration.ofSeconds(15));
        } catch (final AssertionError ex) {
            clientDied = false;
        }
        assertTrue(memberExited, "member " + TARGET + " did not exit");
        assertTrue(clientDied, "a co-located client outlived its node's media driver");

        final ClusterProcesses.Child restarted = cluster.startMember(TARGET, "seq-" + TARGET + "-restarted");
        restarted.awaitLine("serving replay", Duration.ofSeconds(30));
        final ClusterProcesses.Child after = cluster.probe("client-after", TARGET, "follow", "probe.clientId=9");
        after.awaitLine("following live", Duration.ofSeconds(30));

        final String oldRecording = recording(first);
        final String newRecording = recording(restarted);
        final String replyId = firstGroup(REPLY_ID, after);
        final String replay = "replay for client " + replyId + ": group 1's recording";
        final List<String> replays = restarted.lines(replay);
        final long other = replays.stream().filter(line -> !line.contains("recording " + newRecording + " ")).count();
        System.out.printf("recording before / after the restart: %s / %s%nreplays served to the fresh client: %d, "
                          + "of which not of the new recording: %d%n", oldRecording, newRecording, replays.size(),
                          other);

        assertAll(
            () -> assertTrue(newRecording != null, "the restarted node logged no queue recording"),
            () -> assertNotEquals(oldRecording, newRecording, "the restart reused the recording"),
            () -> assertTrue(replays.size() >= 1, "no replay was served to the fresh client"),
            () -> assertEquals(0, other, "a replay was of a recording other than " + newRecording));
    }

    private static String recording(final ClusterProcesses.Child member) {
        return firstGroup(RECORDING, member);
    }

    private static String firstGroup(final Pattern pattern, final ClusterProcesses.Child child) {
        for (final String line : child.lines("")) {
            final Matcher matcher = pattern.matcher(line);
            if (matcher.find()) {
                return matcher.group(1);
            }
        }
        return null;
    }
}
