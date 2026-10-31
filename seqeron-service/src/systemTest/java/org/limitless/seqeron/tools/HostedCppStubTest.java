package org.limitless.seqeron.tools;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

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
 * The C++ stub (hosted::Application, doc/hosted-facade-draft.md §17) against the prototype host, on the leader of a
 * cluster with no snapshot policy. The stub is {@code hosted_probe}, whose path is {@code HOSTED_PROBE} in the
 * environment of the Gradle run; without it the test is skipped.
 */
class HostedCppStubTest {
    private static final String HOST = "org.limitless.seqeron.tools.HostedTestApplicationHost";
    private static final int SOURCE_ID = 16;
    private static final int WINDOW = 1024;
    private static final int MARKERS = 20_000;
    private static final int TIMEOUT_SECS = 60;
    private static final Pattern PLACED = Pattern.compile("placed (\\d+)");
    private static final Pattern PEAK_QUEUED = Pattern.compile("peakQueued (\\d+)");
    private static final Pattern OWN = Pattern.compile("own (\\d+)");
    private static final Pattern PUBLISHED = Pattern.compile("published (\\d+)");
    private static final Pattern EPOCH_DROPS = Pattern.compile("epochDrops (\\d+)");
    private static final Pattern DECLINED = Pattern.compile("declined (\\d+)");

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
        cluster = new ClusterProcesses(baseDir, Path.of(System.getProperty("seqeron.systemTestLogs"),
                                                        test.getTestClass().orElseThrow().getSimpleName(),
                                                        test.getTestMethod().orElseThrow().getName()));
        cluster.startAll();
        cluster.loadTopology(Path.of(System.getProperty("seqeron.snapshotTopology"))
                                 .resolveSibling("topology-test-hosted-quiet.xml"));
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

    /** The stub attaches, sees the gate open, and its markers are sequenced exactly once and in order. */
    @Test
    @Timeout(value = 300, unit = TimeUnit.SECONDS)
    void aCppStubAttachesAndItsMarkersAreSequencedInOrder() {
        startHost("host.spin=true");
        startClient("--interval-ms", "1", "--max-markers", "2000", "--check-refusals", "1");
        check("the stub caught up", poll(() -> client.lastLine("Caught up") != null));
        check("the gate opened", poll(() -> client.lastLine("now leading") != null));
        check("the local checks refused a retired payloadId", poll(() -> client.lastLine("REFUSED retired payloadId") != null));
        check("the local checks refused an oversized payload", poll(() -> client.lastLine("REFUSED oversized payload") != null));
        check("2000 markers were offered", poll(() -> number(client, "state: ", PUBLISHED) == 2000));
        check("every one was sequenced exactly once", poll(() -> number(client, "state: ", OWN) == 2000));
        check("none was sequenced out of order or twice", client.lines("OUT OF ORDER").isEmpty());
        check("the host did not fence", host.lines("FENCED").isEmpty());
        System.out.println(client.lastLine("state: "));
        assertTrue(failures.isEmpty(), failures.size() + " check(s) failed: " + failures);
    }

    /** A full tracker and a full window: the stub declines at the window and the host loses none (H-5). */
    @Test
    @Timeout(value = 300, unit = TimeUnit.SECONDS)
    void aCppStubKeepsTheCommandWindow() {
        startHost("host.spin=true", "host.commandWindow=" + WINDOW, "host.pendingCapacity=4", "host.placePerCycle=2");
        startClient("--flood", "1", "--max-markers", "" + MARKERS);
        check("the gate opened", poll(() -> client.lastLine("now leading") != null));
        check("the stub declined at the window", poll(() -> number(client, "state: ", DECLINED) > 0));
        check("the stub offered " + MARKERS, poll(() -> number(client, "state: ", PUBLISHED) == MARKERS));
        check("the host placed every one", poll(() -> number(host, "queue:", PLACED) == MARKERS));
        check("every one was sequenced exactly once", poll(() -> number(client, "state: ", OWN) == MARKERS));
        check("none was sequenced out of order or twice", client.lines("OUT OF ORDER").isEmpty());
        final long peak = number(host, "queue:", PEAK_QUEUED);
        check("the queue never held more than the window (peak " + peak + " of " + WINDOW + ")", peak > 0 && peak <= WINDOW);
        check("the host did not close the attachment", host.lines("CLIENT_WINDOW").isEmpty());
        System.out.println(host.lastLine("queue:"));
        System.out.println(client.lastLine("state: "));
        assertTrue(failures.isEmpty(), failures.size() + " check(s) failed: " + failures);
    }

    /**
     * A gate edge: the hosted member leads, is flooded and is paused until deposed. The stub stops publishing at the
     * Gate that closes, and what the host held at the edge is dropped by its epoch, so what was sequenced is what was
     * offered less what was dropped (H-3).
     */
    @Test
    @Timeout(value = 300, unit = TimeUnit.SECONDS)
    void aCppStubStopsAtAGateEdgeAndTheHostDropsWhatItHeld() {
        startHost("host.spin=true", "host.commandWindow=" + WINDOW, "host.pendingCapacity=4", "host.placePerCycle=2");
        startClient("--flood", "1");
        check("the gate opened", poll(() -> client.lastLine("now leading") != null));
        check("the stub offered markers", poll(() -> number(client, "state: ", PUBLISHED) >= 2000));
        final ClusterProcesses.Child member = cluster.memberLog(leader);
        try {
            member.pause();
            sleep(4500);
        } finally {
            member.resume();
        }
        check("another member leads", poll(() -> cluster.awaitLeader() != leader));
        check("the gate closed", poll(() -> client.lastLine("not leading") != null));
        check("the host dropped commands queued at the edge", poll(() -> number(host, "queue:", EPOCH_DROPS) > 0));
        sleep(3000);
        final long published = number(client, "state: ", PUBLISHED);
        final long drops = number(host, "queue:", EPOCH_DROPS);
        final long own = number(client, "state: ", OWN);
        check("nothing was sequenced beyond what was placed (offered " + published + ", dropped " + drops
              + ", sequenced " + own + ")", own <= published - drops);
        check("none was sequenced out of order or twice", client.lines("OUT OF ORDER").isEmpty());
        check("the stub published nothing once the gate closed", number(client, "state: ", PUBLISHED) == published);
        System.out.println(host.lastLine("queue:"));
        System.out.println(client.lastLine("state: "));
        assertTrue(failures.isEmpty(), failures.size() + " check(s) failed: " + failures);
    }

    private static void sleep(final long ms) {
        try {
            Thread.sleep(ms);
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }

    private void startHost(final String... properties) {
        final List<String> all = new ArrayList<>(List.of("probe.sourceId=" + SOURCE_ID, "probe.clientId=31",
                                                         "probe.snapshotDir=" + baseDir.resolve("snap")));
        all.addAll(List.of(properties));
        host = cluster.harness("host", leader, HOST, null, all.toArray(new String[0]));
        host.awaitLine("host ready", Duration.ofSeconds(30));
    }

    private void startClient(final String... arguments) {
        final List<String> command = new ArrayList<>(List.of(System.getProperty("seqeron.hostedProbe"), "--aeron-dir",
                                                             cluster.aeronDir(leader), "--source-id",
                                                             "" + SOURCE_ID));
        command.addAll(List.of(arguments));
        client = cluster.nativeProcess("client", command.toArray(new String[0]));
    }

    private static long number(final ClusterProcesses.Child child, final String line, final Pattern pattern) {
        final String last = child.lastLine(line);
        if (last == null) {
            return 0;
        }
        final Matcher matcher = pattern.matcher(last);
        return matcher.find() ? Long.parseLong(matcher.group(1)) : 0;
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
