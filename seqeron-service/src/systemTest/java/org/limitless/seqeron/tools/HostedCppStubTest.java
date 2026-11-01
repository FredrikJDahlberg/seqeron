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
import org.limitless.seqeron.protocol.PortLayout;
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
    private static final String HOST = "org.limitless.seqeron.proxy.ClientProxy";
    private static final int SOURCE_ID = 16;
    private static final int WINDOW = 1024;
    private static final int MARKERS = 20_000;
    private static final int TIMEOUT_SECS = 60;
    private static final Pattern PLACED = Pattern.compile("placed (\\d+)");
    private static final Pattern PEAK_QUEUED = Pattern.compile("peakQueued (\\d+)");
    private static final Pattern OWN = Pattern.compile("own (\\d+)");
    private static final Pattern PUBLISHED = Pattern.compile("published (\\d+)");
    private static final Pattern EPOCH_DROPS = Pattern.compile("epochDrops (\\d+)");
    private static final Pattern REPLIED = Pattern.compile("replied (\\d+)");
    private static final Pattern REPLIES = Pattern.compile("replies (\\d+)");
    private static final Pattern BAD = Pattern.compile("bad (\\d+)");
    private static final Pattern DECLINED = Pattern.compile("declined (\\d+)");

    @TempDir
    Path baseDir;

    private ClusterProcesses cluster;
    private ClusterProcesses.Child host;
    private ClusterProcesses.Child client;
    private int leader;
    private int hostMember;
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
        hostMember = leader;
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
        startHost("proxy.spin=true");
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
        startHost("proxy.spin=true", "proxy.commandWindow=" + WINDOW, "proxy.pendingCapacity=4", "proxy.placePerCycle=2");
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
        startHost("proxy.spin=true", "proxy.commandWindow=" + WINDOW, "proxy.pendingCapacity=4", "proxy.placePerCycle=2");
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

    /** GroupsResolved reaches the stub, its group-mask check is the direct one's, and a Reply returns under its request's ref. */
    @Test
    @Timeout(value = 300, unit = TimeUnit.SECONDS)
    void aCppStubResolvesGroupsAndRepliesUnderTheRequestsRef() {
        startHost("proxy.spin=true", "proxy.commandWindow=" + WINDOW, "proxy.pendingCapacity=4", "proxy.placePerCycle=2");
        startClient("--interval-ms", "1", "--max-markers", "2000", "--check-groups", "1", "--reply-every", "10");
        check("the stub was handed the group directory", poll(() -> client.lastLine("group 1 test mask 1") != null));
        check("the gate opened", poll(() -> client.lastLine("now leading") != null));
        check("a mask naming a group the table lacks was refused", poll(() -> client.lastLine("REFUSED unknown group") != null));
        check("a mask naming the own group was published", poll(() -> client.lastLine("published to the own group by mask") != null));
        check("every marker was sequenced", poll(() -> number(client, "state: ", OWN) == 2000));
        check("every tenth was answered", poll(() -> number(client, "state: ", REPLIED) == 200));
        check("every reply was sequenced once under its request's ref", poll(() -> number(client, "state: ", REPLIES) == 200));
        check("none came back under another ref", number(client, "state: ", BAD) == 0 && client.lines("BAD REPLY").isEmpty());
        check("no marker carried another group mask", client.lines("UNEXPECTED groupMask").isEmpty());
        check("the host did not close the attachment", host.lines("CLIENT_WINDOW").isEmpty());
        check("the host did not fence", host.lines("FENCED").isEmpty());
        System.out.println(host.lastLine("queue:"));
        System.out.println(client.lastLine("state: "));
        assertTrue(failures.isEmpty(), failures.size() + " check(s) failed: " + failures);
    }

    /**
     * The host's façade connects while the cluster has no leader (both followers paused, the leader stood down for want
     * of a quorum), a wait of seconds in the cluster connect. The wait spins on the façade's idle strategy, which
     * writes the host heartbeat (H-10), so the stub on its default 3 s host timeout keeps the host. How the connect
     * ends is Aeron's: one whose request reached no leader may time out after the election, and the host then exits 70.
     */
    @Test
    @Timeout(value = 300, unit = TimeUnit.SECONDS)
    void aCppStubKeepsAHostWhoseFacadeIsConnecting() {
        startHost("proxy.spin=true");
        final List<ClusterProcesses.Child> followers = new ArrayList<>();
        for (int m = 0; m < ClusterProcesses.MEMBERS; m++) {
            if (m != leader) {
                followers.add(cluster.memberLog(m));
            }
        }
        try {
            followers.forEach(ClusterProcesses.Child::pause);
            sleep(4000);
            startClient("--interval-ms", "10", "--max-markers", "100");
            sleep(5000);
            check("the host is still connecting", host.lines("ATTACHED").isEmpty() && host.isAlive());
            check("the stub kept the host through the wait", client.isAlive() && client.lines("HOST_LOST").isEmpty());
        } finally {
            followers.forEach(ClusterProcesses.Child::resume);
        }
        assertTrue(failures.isEmpty(), failures.size() + " check(s) failed: " + failures);
    }

    /**
     * The host closes the attachment and stays up: the stub, paused past the host's 2 s client timeout, is dropped
     * ({@code CLIENT_LOST}) and, resumed, finds the host's heartbeat fresh but its events image gone, and exits
     * {@code HOST_LOST} (§17.6).
     */
    @Test
    @Timeout(value = 300, unit = TimeUnit.SECONDS)
    void aCppStubWhoseAttachmentTheHostClosedExits() {
        startHost("proxy.spin=true");
        startClient("--interval-ms", "10");
        check("the gate opened", poll(() -> client.lastLine("now leading") != null));
        try {
            client.pause();
            sleep(3500);
        } finally {
            client.resume();
        }
        check("the host closed the attachment", poll(() -> host.lastLine("CLIENT_LOST") != null));
        check("the stub exited HOST_LOST", poll(() -> client.lastLine("HOST_LOST") != null) &&
                                               client.awaitDead(Duration.ofSeconds(10)) && client.exitValue() == 70);
        check("the host stayed up", host.isAlive());
        assertTrue(failures.isEmpty(), failures.size() + " check(s) failed: " + failures);
    }

    /**
     * The façade's cluster connect fails: the host runs on a follower, whose {@code aeron:ipc} ingress never answers,
     * and its UDP ingress names the ports of members this cluster does not have. The host says why and exits 70; the
     * stub loses it.
     */
    @Test
    @Timeout(value = 300, unit = TimeUnit.SECONDS)
    void aHostWhoseFacadeCannotReachTheClusterExits70() {
        hostMember = (leader + 1) % ClusterProcesses.MEMBERS;
        final String absent = "0=localhost:" + PortLayout.ingressPort(ClusterProcesses.MEMBERS) + ",1=localhost:" +
                              PortLayout.ingressPort(ClusterProcesses.MEMBERS + 1);
        startHost("proxy.spin=true", "proxy.ingressEndpoints=" + absent);
        startClient("--interval-ms", "10", "--max-markers", "100");
        final int status = host.awaitExit(Duration.ofSeconds(60));
        check("the host exited 70 (exited " + status + ")", status == 70);
        check("the host said why", host.lastLine("CLUSTER_UNREACHABLE") != null);
        check("the host did not crash", host.lines("Exception in thread").isEmpty());
        check("the stub lost the host", poll(() -> client.lastLine("HOST_LOST") != null));
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
        final List<String> all = new ArrayList<>(List.of("proxy.sourceId=" + SOURCE_ID, "proxy.clientId=31",
                                                         "proxy.snapshotDir=" + baseDir.resolve("snap")));
        all.addAll(List.of(properties));
        host = cluster.proxy("host", hostMember, all.toArray(new String[0]));
        host.awaitLine("host ready", Duration.ofSeconds(30));
    }

    private void startClient(final String... arguments) {
        final List<String> command = new ArrayList<>(List.of(System.getProperty("seqeron.hostedProbe"), "--aeron-dir",
                                                             cluster.aeronDir(hostMember), "--source-id",
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
