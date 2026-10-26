package org.limitless.seqeron.tools;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * The operator tools as an operator runs them: {@code clusterctl}, {@code ClusterProbe} and {@code SbeLogPrinter} as
 * child JVMs against one three-member cluster. The tests share the cluster and run in order, ending with {@code
 * clusterctl shutdown}, after which the members' archives are dumped.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Timeout(value = 300, unit = TimeUnit.SECONDS)
class ClusterToolsTest {
    private static final Duration TOOL_TIMEOUT = Duration.ofSeconds(60);
    private static final int ANY_MEMBER = 1;

    @TempDir
    static Path baseDir;

    private static ClusterProcesses cluster;
    private static int runs;

    @BeforeAll
    static void startCluster() {
        cluster = new ClusterProcesses(baseDir, Path.of(System.getProperty("seqeron.systemTestLogs"),
                                                        ClusterToolsTest.class.getSimpleName()));
        cluster.startAll();
        cluster.loadTopology(Path.of(System.getProperty("seqeron.topologyGroups")));
    }

    @AfterAll
    static void stopCluster() {
        cluster.close();
    }

    /** What a finished tool wrote and how it exited. */
    private record Run(int status, String output) {
        void assertContains(final String text) {
            assertTrue(output.contains(text), "no \"" + text + "\" in:\n" + output);
        }
    }

    private static Run finish(final ClusterProcesses.Child tool) throws IOException {
        final int status = tool.awaitExit(TOOL_TIMEOUT);
        return new Run(status, Files.readString(tool.log()));
    }

    private static Run clusterctl(final int member, final String... arguments) throws IOException {
        return finish(cluster.clusterctl("clusterctl-" + ++runs, member, arguments));
    }

    private static Run probe(final String mode, final String... properties) throws IOException {
        return finish(cluster.probe("probe-" + mode + "-" + ++runs, ANY_MEMBER, mode, properties));
    }

    private static Run printer(final String... arguments) throws IOException {
        return finish(cluster.logPrinter("printer-" + ++runs, arguments));
    }

    @Test
    @Order(1)
    void clusterctlRefusesWhatItCannotRun() throws IOException {
        final Run none = clusterctl(0);
        assertEquals(2, none.status());
        none.assertContains("Usage: clusterctl.sh");

        final Run help = clusterctl(0, "help");
        assertEquals(0, help.status());
        help.assertContains("load-topology <file>");

        final Run snapshot = clusterctl(0, "snapshot");
        assertEquals(0, snapshot.status());
        snapshot.assertContains("snapshot: command is not supported");

        final Run noGateway = clusterctl(0, "activate");
        assertEquals(2, noGateway.status());
        noGateway.assertContains("missing <gatewayId>");
        final Run badGateway = clusterctl(0, "activate", "two");
        assertEquals(2, badGateway.status());
        badGateway.assertContains("must be an integer, got 'two'");

        final Run noFile = clusterctl(0, "load-topology");
        assertEquals(2, noFile.status());
        noFile.assertContains("missing <file>");
        final Run absent = clusterctl(0, "load-topology", baseDir.resolve("absent.xml").toString());
        assertEquals(2, absent.status());
        absent.assertContains("absent.xml");
    }

    @Test
    @Order(2)
    void clusterctlRefusesAMalformedTopologyAtItsLine() throws IOException {
        final Path broken = Files.writeString(baseDir.resolve("broken.xml"), "<topology>\n  <groups>\n");
        final Run run = clusterctl(0, "load-topology", broken.toString());
        assertEquals(2, run.status());
        run.assertContains("broken.xml:");
    }

    @Test
    @Order(3)
    void clusterctlRecordsAStartAndListsTheCounters() throws IOException {
        final Run start = clusterctl(0, "start");
        assertEquals(0, start.status());
        start.assertContains("system-started recorded at clusterSeqNo=");

        final Run counters = clusterctl(ANY_MEMBER, "counters");
        assertEquals(0, counters.status());
        assertTrue(counters.output().lines().anyMatch(line -> line.matches(".+ = \\d+")),
                   "a counter a line:\n" + counters.output());
    }

    @Test
    @Order(4)
    void clusterctlPassesOtherCommandsToClusterTool() throws IOException {
        final Run members = clusterctl(0, "list-members");
        assertEquals(0, members.status());
        members.assertContains("leaderMemberId=");
    }

    @Test
    @Order(5)
    void clusterctlTrimsNothingWithoutSnapshots() throws IOException {
        for (final String[] arguments : new String[][] {{"trim-log"}, {"trim-log", "--dry-run"}}) {
            final Run trim = clusterctl(0, arguments);
            assertEquals(0, trim.status());
            trim.assertContains("fewer than two restart-safe snapshots");
        }
    }

    @Test
    @Order(6)
    void clusterctlSaysWhyNoSnapshotRoundOrActivationFollowed() throws IOException {
        final Run snapshot = clusterctl(0, "request-snapshot");
        assertEquals(1, snapshot.status());
        snapshot.assertContains("does the loaded topology have a <snapshots> element?");

        final Run activate = clusterctl(0, "activate", "77");
        assertEquals(1, activate.status());
        activate.assertContains("is <gatewayId> a list row?");
    }

    @Test
    @Order(7)
    void theProbePingsSubmitsAndConfirms() throws IOException {
        final Run ping = probe("ping");
        assertEquals(0, ping.status());
        ping.assertContains("echoed at clusterSeqNo");

        final Run submit = probe("submit", "probe.count=25", "probe.fillerBytes=16");
        assertEquals(0, submit.status());
        submit.assertContains("submitted 25 ProbeMarker(s), filler 16 bytes");

        final Run confirm = probe("confirm", "probe.count=50", "probe.clientId=9");
        assertEquals(0, confirm.status(), confirm.output());
    }

    @Test
    @Order(8)
    void theProbeFollowsTheQueueUntilStopped() throws IOException {
        final ClusterProcesses.Child follow = cluster.probe("probe-follow", ANY_MEMBER, "follow", "probe.clientId=9");
        follow.awaitLine("following live", Duration.ofSeconds(30));
        assertTrue(follow.terminate(Duration.ofSeconds(20)), "follow exits on SIGTERM");
    }

    @Test
    @Order(9)
    void theProbeSaysWhatItDoesWhenAskedNothingItKnows() throws IOException {
        final Run help = probe("help");
        assertEquals(0, help.status());
        final Run unknown = probe("shout");
        assertEquals(1, unknown.status());
        assertEquals(help.output().lines().count(), unknown.output().lines().count(), "the same usage");
    }

    @Test
    @Order(10)
    void shutdownOnAFollowerDoesNothingAndOnTheLeaderStopsTheCluster() throws IOException {
        final int leader = cluster.awaitLeader();
        final int follower = (leader + 1) % ClusterProcesses.MEMBERS;
        final Run onFollower = clusterctl(follower, "shutdown");
        assertEquals(0, onFollower.status());
        onFollower.assertContains("this node is not the leader");
        assertTrue(cluster.isRunning(follower), "a follower's shutdown stops nothing");

        final Run onLeader = clusterctl(leader, "shutdown");
        assertEquals(0, onLeader.status());
        onLeader.assertContains("system-stopped recorded at clusterSeqNo=");
        onLeader.assertContains("cluster abort requested");
        for (int m = 0; m < ClusterProcesses.MEMBERS; m++) {
            assertTrue(cluster.memberLog(m).awaitDead(Duration.ofSeconds(30)), "member " + m + " stopped");
        }
    }

    @Test
    @Order(11)
    void theLogPrinterListsItsSchemasAndDumpsAStoppedMembersArchive() throws IOException {
        final Run schemas = printer("--list-schemas");
        assertEquals(0, schemas.status());
        for (final String name : new String[] {"frame", "probe", "replay", "sequencer"}) {
            assertTrue(schemas.output().lines().anyMatch(name::equals), name + " listed:\n" + schemas.output());
        }

        final Path archive = baseDir.resolve("archive-0");
        final Run dump = printer(archive.toString(), "--oneline");
        assertEquals(0, dump.status());
        dump.assertContains("[Catalog] Recording ID");
        dump.assertContains("ProbeMarker");
        dump.assertContains("ClusterStopped");

        final Run only = printer("--schema", "frame", archive.toString(), "--oneline");
        assertEquals(0, only.status());
        only.assertContains("ClusterStopped");
        assertFalse(only.output().contains("ProbeMarker ="), "the probe schema is not loaded");
    }

    @Test
    @Order(12)
    void theLogPrinterRefusesBadArguments() throws IOException {
        final Path archive = baseDir.resolve("archive-0");
        assertEquals(1, printer().status());
        final Run schema = printer("--schema", "nowhere", archive.toString());
        assertEquals(1, schema.status());
        schema.assertContains("Unknown schema 'nowhere'");
        final Run both = printer("--schema", "frame", "--spec", "x.sbeir", archive.toString());
        assertEquals(1, both.status());
        both.assertContains("mutually exclusive");
        final Run stream = printer(archive.toString(), "--stream", "many");
        assertEquals(1, stream.status());
        stream.assertContains("Not a stream id: many");
        final Run payload = printer(archive.toString(), "-o", "0");
        assertEquals(1, payload.status());
        payload.assertContains("Not a payloadId: 0");
        final Run missing = printer(archive.toString(), "--stream");
        assertEquals(1, missing.status());
        missing.assertContains("Missing value for --stream");
        final Run extra = printer(archive.toString(), "another");
        assertEquals(1, extra.status());
        extra.assertContains("Unexpected argument: another");
        final Run empty = printer(baseDir.resolve("no-such-dir").toString());
        assertEquals(1, empty.status());
    }
}
