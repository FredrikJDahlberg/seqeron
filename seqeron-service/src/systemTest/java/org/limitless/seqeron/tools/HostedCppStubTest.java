package org.limitless.seqeron.tools;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.aeron.Aeron;
import io.aeron.Publication;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.AfterEach;
import org.agrona.concurrent.UnsafeBuffer;
import org.limitless.seqeron.protocol.PortLayout;
import org.limitless.seqeron.protocol.ProxyProtocol;
import org.limitless.seqeron.protocol.SeqeronCounters;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * The C++ stub (hosted::Application, doc/seqeron-protocol-spec.md §17) against the prototype host, on the leader of a
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
        check("the stub heard the cluster clock", poll(() -> client.lastLine("cluster time") != null));
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

    /**
     * The host exports its client's lag (H-14): the stub floods a 1 KiB window, and paused, falls behind what the host
     * goes on dispatching from it; resumed, it applies everything and the lag is 0.
     */
    @Test
    @Timeout(value = 300, unit = TimeUnit.SECONDS)
    void theHostExportsTheCppStubsLag() {
        startHost("proxy.spin=true", "proxy.clientTimeoutMs=10000", "proxy.commandWindow=" + WINDOW,
                  "proxy.pendingCapacity=4", "proxy.placePerCycle=2");
        startClient("--flood", "1", "--max-markers", "" + MARKERS);
        check("the gate opened", poll(() -> client.lastLine("now leading") != null));
        try {
            client.pause();
            check("the paused stub lags", poll(() -> lag() > 0));
        } finally {
            client.resume();
        }
        check("every marker was sequenced", poll(() -> number(client, "state: ", OWN) == MARKERS));
        check("the stub caught up with the host", poll(() -> lag() == 0));
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
     * writes the host heartbeat (H-10), so the stub on its default 3 s host timeout keeps the host. A connect whose
     * request reached no leader times out after the election, and the host connects again.
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
        check("the host attached", poll(() -> host.lastLine("ATTACHED") != null));
        check("the stub caught up", poll(() -> client.lastLine("Caught up") != null));
        check("the host stayed up", host.isAlive() && host.lines("CLUSTER_UNREACHABLE").isEmpty());
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
        check("its lag series went with it", lag() == ClusterProcesses.NO_VALUE);
        check("the stub exited HOST_LOST", poll(() -> client.lastLine("HOST_LOST") != null) &&
                                               client.awaitDead(Duration.ofSeconds(10)) && client.exitValue() == 70);
        check("the host stayed up", host.isAlive());
        assertTrue(failures.isEmpty(), failures.size() + " check(s) failed: " + failures);
    }

    /**
     * An orderly detach through back-pressure (H-9): with the host paused, stream 301 is filled from another
     * publication until it back-pressures, and the stub is told to detach. Its Detach waits out the back-pressure until
     * the host, resumed 300 ms later, drains the stream, and the host closes the attachment as detached, not lost.
     */
    @Test
    @Timeout(value = 300, unit = TimeUnit.SECONDS)
    void aCppStubDetachesThroughBackPressure() {
        startHost("proxy.spin=true");
        startClient("--interval-ms", "10");
        check("the gate opened", poll(() -> client.lastLine("now leading") != null));
        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(cluster.aeronDir(hostMember)));
             Publication flood = aeron.addPublication(ProxyProtocol.ATTACH_REQUEST_CHANNEL,
                                                      ProxyProtocol.ATTACH_REQUEST_STREAM)) {
            check("the flood reaches the host", poll(flood::isConnected));
            final UnsafeBuffer junk = new UnsafeBuffer(new byte[1024]);
            host.pause();
            try {
                long result;
                while ((result = flood.offer(junk)) > 0) {
                    Thread.onSpinWait();
                }
                check("stream 301 is back-pressured (" + result + ")", result == Publication.BACK_PRESSURED);
                client.signal("USR1");
                sleep(300);
            } finally {
                host.resume();
            }
        }
        check("the host closed the attachment as detached", poll(() -> host.lastLine("DETACHED") != null));
        check("the stub exited 0", client.awaitDead(Duration.ofSeconds(10)) && client.exitValue() == 0);
        check("the host did not take it for lost", host.lines("CLIENT_LOST").isEmpty());
        assertTrue(failures.isEmpty(), failures.size() + " check(s) failed: " + failures);
    }

    /**
     * The host drops the stub before its events subscription connected: the host, paused, holds the stub's Attach;
     * the stub, paused before it reads the Attached, misses the host's 2 s connect deadline ({@code CLIENT_LOST}) and,
     * resumed, finds no events image within its host timeout and exits {@code HOST_LOST}.
     */
    @Test
    @Timeout(value = 300, unit = TimeUnit.SECONDS)
    void aCppStubDroppedBeforeItsEventsConnectedExits() {
        startHost("proxy.spin=true");
        try {
            host.pause();
            startClient("--interval-ms", "10");
            sleep(1000);
            client.pause();
        } finally {
            host.resume();
        }
        try {
            check("the host closed the attachment", poll(() -> host.lastLine("CLIENT_LOST") != null));
        } finally {
            client.resume();
        }
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

    /**
     * The client dies while the façade's cluster connect fails (the cluster unreachable, as above): the host, which
     * finds the client's counter changed after its own 10 s wait, watches it for a client timeout, drops it
     * ({@code CLIENT_LOST}) rather than connecting again for nobody, and stays up for the next client.
     */
    @Test
    @Timeout(value = 300, unit = TimeUnit.SECONDS)
    void aHostWhoseClientDiesDuringAFailedConnectDropsIt() {
        hostMember = (leader + 1) % ClusterProcesses.MEMBERS;
        final String absent = "0=localhost:" + PortLayout.ingressPort(ClusterProcesses.MEMBERS) + ",1=localhost:" +
                              PortLayout.ingressPort(ClusterProcesses.MEMBERS + 1);
        startHost("proxy.spin=true", "proxy.ingressEndpoints=" + absent);
        startClient("--interval-ms", "10", "--max-markers", "100");
        sleep(3000);
        client.stop();
        check("the host dropped the client", poll(() -> host.lastLine("CLIENT_LOST") != null));
        check("the host did not connect again", host.lines("attempt 2 of").isEmpty());
        check("the host stayed up", host.isAlive() && host.lines("CLUSTER_UNREACHABLE").isEmpty());
        assertTrue(failures.isEmpty(), failures.size() + " check(s) failed: " + failures);
    }

    /**
     * Two hosts in one process (proxy.hosts), on a follower: host a cannot reach the cluster, host b can. Host a is
     * rebuilt in place after its third failed connect and its client exits HOST_LOST; host b's client keeps its host
     * and catches up, and the process stays up.
     */
    @Test
    @Timeout(value = 300, unit = TimeUnit.SECONDS)
    void twoHostsInOneProcessServeTheirClientsApart() {
        hostMember = (leader + 1) % ClusterProcesses.MEMBERS;
        final String absent = "0=localhost:" + PortLayout.ingressPort(ClusterProcesses.MEMBERS) + ",1=localhost:" +
                              PortLayout.ingressPort(ClusterProcesses.MEMBERS + 1);
        host = cluster.proxy("hosts", hostMember, "proxy.hosts=a,b", "proxy.spin=true",
                             "proxy.snapshotDir=" + baseDir.resolve("snap"), "proxy.a.sourceId=17",
                             "proxy.a.clientId=31", "proxy.a.ingressEndpoints=" + absent, "proxy.b.sourceId=16",
                             "proxy.b.clientId=32");
        check("both hosts are ready", poll(() -> host.lines("host ready").size() == 2));
        final ClusterProcesses.Child clientA = probe("client-a", cluster.aeronDir(hostMember), "a", 17);
        final ClusterProcesses.Child clientB = probe("client-b", cluster.aeronDir(hostMember), "b", SOURCE_ID);
        check("host b's client caught up", poll(() -> clientB.lastLine("Caught up") != null));
        check("host a was rebuilt in place", poll(() -> host.lastLine("host a exited 70: rebuilding it") != null));
        check("host a's client exited HOST_LOST", poll(() -> clientA.lastLine("HOST_LOST") != null));
        check("host b's client kept its host", clientB.isAlive() && clientB.lines("HOST_LOST").isEmpty());
        check("host b was not rebuilt", host.lines("host b exited").isEmpty());
        check("the process stayed up", host.isAlive());
        assertTrue(failures.isEmpty(), failures.size() + " check(s) failed: " + failures);
    }

    /**
     * A host inside a gateway host's ReplayerServer (proxy.hosts there): its Application is off the cluster, so the
     * gate opens once caught up through the gateway host's relay and Replayer, and the stub's markers are sequenced.
     * A lost client leaves the gateway host up.
     */
    @Test
    @Timeout(value = 300, unit = TimeUnit.SECONDS)
    void aHostInsideAGatewayHostServesItsClient() {
        final int gatewayHost = ClusterProcesses.MEMBERS;
        final ClusterProcesses.Child gateway =
            cluster.startGatewayHost("gateway-host", gatewayHost,
                                     List.of("proxy.hosts=gw", "proxy.spin=true", "proxy.sourceId=" + SOURCE_ID,
                                             "proxy.clientId=31", "proxy.snapshotDir=" + baseDir.resolve("snap")),
                                     0, 1, 2);
        check("the host is ready", poll(() -> gateway.lastLine("host ready") != null));
        client = probe("client", cluster.aeronDir(gatewayHost), "gw", SOURCE_ID, "--max-markers", "100");
        check("the gate opened off the cluster", poll(() -> client.lastLine("now leading") != null));
        check("every marker was sequenced", poll(() -> number(client, "state: ", OWN) == 100));
        client.stop();
        check("the host dropped the client", poll(() -> gateway.lastLine("CLIENT_LOST") != null
                                                    || gateway.lastLine("DETACHED") != null));
        check("the gateway host stayed up", gateway.isAlive() && gateway.lines("FATAL").isEmpty());
        assertTrue(failures.isEmpty(), failures.size() + " check(s) failed: " + failures);
    }

    private ClusterProcesses.Child probe(final String name, final String aeronDir, final String hostName,
                                         final int sourceId, final String... arguments) {
        final List<String> command = new ArrayList<>(List.of(System.getProperty("seqeron.hostedProbe"), "--aeron-dir",
                                                             aeronDir, "--host-name", hostName, "--source-id",
                                                             "" + sourceId, "--interval-ms", "10"));
        command.addAll(List.of(arguments));
        return cluster.nativeProcess(name, command.toArray(new String[0]));
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

    /** The host's export of its client's lag in bytes, {@link ClusterProcesses#NO_VALUE} while it has none. */
    private long lag() {
        return cluster.counter(hostMember, SeqeronCounters.HOSTED_LAG_TYPE_ID,
                               "seqeron.hosted.lag member=" + hostMember + " client=31");
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
