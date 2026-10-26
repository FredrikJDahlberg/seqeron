package org.limitless.seqeron.tools;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * A randomized fault loop against a live three-member cluster with a consumer replica on every
 * member, a gateway pair on members 0 and 1 and a low-rate load through the active gate. Each round draws one fault,
 * heals it, and checks the steady-state invariants; at the end every member's queue recording must be gap-free,
 * monotone and the same length. {@code SEED=<n>} replays a fault sequence; {@code ROUNDS}, {@code ONLY_FAULT} (one of
 * {@link Fault}, case-insensitive) and {@code STEADY_STATE_SECS} tune the run, {@code TIMEOUT_SCALE} stretches the
 * deadlines. Every fault is a signal or trigger file, as the single-shot tests use. Tagged {@code chaos}:
 * {@code systemTest} leaves it out unless run with {@code -Pchaos}.
 */
@Tag("chaos")
class ChaosTest {
    private enum Fault { KILL_LEADER, KILL_FOLLOWER, SIGKILL_NODE, PAUSE_NODE, QUEUE_DROP, QUEUE_STALL }

    private static final String TEST_GATEWAY = "org.limitless.seqeron.tools.TestGateway";
    private static final int TEST_GATEWAY_PORT_BASE = 9200;
    private static final int[] GATEWAY_IDS = {10, 11};
    private static final String[] GATEWAY_NAMES = {"GW-T-A", "GW-T-B"};
    private static final int CN = 0;
    // An unset workflow input arrives as the empty string.
    private static final long SEED = Long.parseLong(System.getenv().getOrDefault("SEED", "").isEmpty() ?
                                                        "" + new Random().nextInt(32768) : System.getenv("SEED"));
    private static final int ROUNDS = Integer.parseInt(System.getenv().getOrDefault("ROUNDS", "20"));
    private static final long STEADY_STATE_SECS = Long.parseLong(System.getenv().getOrDefault("STEADY_STATE_SECS", "4"));
    private static final double SCALE = Double.parseDouble(System.getenv().getOrDefault("TIMEOUT_SCALE", "1"));
    private static final int LOAD_BATCH_FRAMES = 25;
    private static final long PAUSE_SECS = 3;
    // Aeron's own 10 s timeouts, which a slow host does not lengthen.
    private static final long DRIVER_LOSS_GRACE_SECS = 15;
    private static final long MARK_FILE_SETTLE_SECS = 12;
    private static final long QUEUE_STALL_DEADLINE_SECS = 10;

    @TempDir
    Path baseDir;

    private ClusterProcesses cluster;
    private Path logDir;
    private final ClusterProcesses.Child[] replicas = new ClusterProcesses.Child[ClusterProcesses.MEMBERS];
    private final int[] recoveryStallBaseline = new int[ClusterProcesses.MEMBERS];
    private final int[] boots = new int[ClusterProcesses.MEMBERS];
    private final AtomicReferenceArray<ClusterProcesses.Child> gateways = new AtomicReferenceArray<>(2);
    private final List<String> history = new ArrayList<>();
    private final List<String> failures = new ArrayList<>();
    private Thread load;
    private volatile boolean loading;
    private int dropTarget = ClusterProcesses.NO_MEMBER;
    private int dropBefore;
    private Random rng;

    @BeforeEach
    void startCluster(final TestInfo test) {
        logDir = Path.of(System.getProperty("seqeron.systemTestLogs"), test.getTestClass().orElseThrow().getSimpleName());
        cluster = new ClusterProcesses(baseDir, logDir);
    }

    @AfterEach
    void stopCluster() {
        loading = false;
        if (cluster != null) {
            cluster.close();
        }
    }

    @Test
    @Timeout(value = 60, unit = TimeUnit.MINUTES)
    void clusterSurvivesRandomFaults() throws Exception {
        try {
            run();
        } catch (final Throwable ex) {
            record(ex);
            throw ex;
        }
    }

    /** Leaves the seed, the faults drawn and what failed beside the process logs, which CI uploads. */
    private void record(final Throwable ex) throws IOException {
        final StringWriter report = new StringWriter();
        report.write("SEED=" + SEED + " ROUNDS=" + ROUNDS + "\n");
        history.forEach(h -> report.write(h + "\n"));
        failures.forEach(f -> report.write("FAIL: " + f + "\n"));
        ex.printStackTrace(new PrintWriter(report, true));
        Files.createDirectories(logDir);
        Files.writeString(logDir.resolve("result.txt"), report.toString());
    }

    private void run() throws Exception {
        final List<Fault> menu = menu();
        rng = new Random(SEED);
        System.out.printf("SEED=%d ROUNDS=%d faults=%s (replay with SEED=%d)%n", SEED, ROUNDS, menu, SEED);
        bringUp();
        int passed = 0;
        for (int round = 1; round <= ROUNDS; round++) {
            final int leader = cluster.awaitLeader();
            final Fault fault = menu.get(rng.nextInt(menu.size()));
            history.add("round " + round + ": " + fault + " (leader was " + leader + ")");
            System.out.printf("-- round %d/%d: %s, leader %d%n", round, ROUNDS, fault, leader);
            failures.clear();
            inject(fault, leader);
            sleep(STEADY_STATE_SECS);
            checkInvariants(round);
            if (!failures.isEmpty()) {
                failures.forEach(f -> System.out.println("  FAIL: " + f));
                throw new AssertionError("chaos failed at round " + round + ": " + failures + "\n  "
                                         + String.join("\n  ", history) + "\n  reproduce with SEED=" + SEED
                                         + " ROUNDS=" + ROUNDS);
            }
            passed++;
        }
        final String sequence = verifySequence();
        System.out.printf("chaos survived: %d/%d rounds; %s (SEED=%d)%n", passed, ROUNDS, sequence, SEED);
    }

    private static List<Fault> menu() {
        final String only = System.getenv("ONLY_FAULT");
        if (only == null || only.isEmpty()) {
            return List.of(Fault.values());
        }
        return List.of(Fault.valueOf(only.replaceFirst("^fault_", "").replace('-', '_').toUpperCase()));
    }

    private void bringUp() throws IOException {
        for (int m = 0; m < ClusterProcesses.MEMBERS; m++) {
            cluster.startMember(m, "seq-" + m, true);
        }
        cluster.awaitActive();
        cluster.awaitLeader();
        cluster.loadTopology(Path.of(System.getProperty("seqeron.topologyGroups")));
        for (int m = 0; m < ClusterProcesses.MEMBERS; m++) {
            startReplica(m);
        }
        // Without <snapshots>: a restarted member would start its queue at the last cut, which the plain probe
        // replicas cannot follow (doc/snapshot.md §10.8).
        final Path noRounds = baseDir.resolve("topology-no-rounds.xml");
        ClusterProcesses.withoutSnapshots(Path.of(System.getProperty("seqeron.gatewayTopology")), noRounds);
        cluster.loadTopology(noRounds);
        for (int g = 0; g < 2; g++) {
            startGateway(g);
        }
        cluster.await("a gateway instance designated active", scaled(60), () -> activeGateway() >= 0);
        // One operator handover before the faults: the only path that submits a GatewayActivationRequested.
        final int standby = 1 - activeGateway();
        final ClusterProcesses.Child activate = cluster.clusterctl("activate", CN, "activate", "" + GATEWAY_IDS[standby]);
        assertEquals(0, activate.awaitExit(scaled(60)), "clusterctl activate; see " + activate.log());
        cluster.await(GATEWAY_NAMES[standby] + " to take the role", scaled(60), () -> activeGateway() == standby);
        loading = true;
        load = new Thread(this::loadLoop, "chaos-load");
        load.setDaemon(true);
        load.start();
    }

    private void inject(final Fault fault, final int leader) throws Exception {
        switch (fault) {
            case KILL_LEADER -> killLeader(leader);
            case KILL_FOLLOWER -> {
                final int f = follower(leader);
                System.out.println("FAULT kill-follower: member " + f);
                cluster.stopMember(f, scaled(10));
                cluster.kill(f);
                sleep(1);
                restart(f);
                restartColocated(f);
            }
            case SIGKILL_NODE -> {
                // Aimed at a follower: the only fault on the media-driver timeout path.
                final int f = follower(leader);
                System.out.println("FAULT sigkill-node: member " + f);
                cluster.kill(f);
                // The kill leaves the mark files active until Aeron ages them out.
                sleep(MARK_FILE_SETTLE_SECS);
                cluster.repairArchive(f);
                restart(f);
                sleep(2);
                restartColocated(f);
            }
            case PAUSE_NODE -> {
                final int p = rng.nextBoolean() ? leader : follower(leader);
                System.out.println("FAULT pause-node: SIGSTOP member " + p + " for " + PAUSE_SECS + "s");
                final ClusterProcesses.Child member = cluster.memberLog(p);
                member.pause();
                try {
                    sleep(PAUSE_SECS);
                } finally {
                    member.resume();
                }
            }
            case QUEUE_DROP -> {
                // The replica beside the leader: its queue is the busiest, so the gap there is under most pressure.
                dropTarget = leader;
                dropBefore = replicas[leader].lines("queue gap: expected groupSeqNo").size();
                System.out.println("FAULT queue-drop: SIGUSR1 member " + leader + "'s replica");
                replicas[leader].signal("USR1");
            }
            case QUEUE_STALL -> queueStall(follower(leader));
        }
    }

    private void killLeader(final int leader) throws Exception {
        System.out.println("FAULT kill-leader: member " + leader);
        cluster.stopMember(leader, scaled(10));
        cluster.kill(leader);
        // The restart waits for the survivors to elect: a leader restarted first just bounces and reclaims.
        try {
            final int next = cluster.awaitLeader();
            System.out.println("  new leader: member " + next);
        } catch (final AssertionError ex) {
            failures.add("no genuine failover: " + ex.getMessage());
        }
        sleep(1);
        restart(leader);
        sleep(2);
        restartColocated(leader);
    }

    private void queueStall(final int m) throws Exception {
        System.out.println("FAULT queue-stall: stop member " + m + "'s queue recording");
        final ClusterProcesses.Child member = cluster.memberLog(m);
        final Path trigger = baseDir.resolve("cluster-" + m).resolve("queue-stall-fault");
        cluster.expectExit(m);
        Files.createFile(trigger);
        if (!member.awaitDead(scaled(QUEUE_STALL_DEADLINE_SECS))) {
            failures.add("member " + m + " kept running " + QUEUE_STALL_DEADLINE_SECS + "s with no queue recording");
            cluster.kill(m);
        } else if (member.lines("FATAL:").stream().noneMatch(l -> l.contains("terminating this node"))) {
            failures.add("member " + m + " died without the FATAL queue-recording line");
        }
        // Else the restarted node re-arms it at once.
        Files.deleteIfExists(trigger);
        sleep(2);
        restart(m);
        sleep(2);
        restartColocated(m);
    }

    /** A killed member's driver died with it, so its co-located clients must have exited non-zero by now. */
    private void restart(final int m) {
        final ClusterProcesses.Child member = cluster.startMember(m, "seq-" + m + "-b" + ++boots[m], true);
        try {
            member.awaitLine("Running", scaled(15));
        } catch (final AssertionError ex) {
            failures.add("member " + m + " never came back: " + ex.getMessage());
        }
    }

    private void restartColocated(final int m) {
        assertDiedOnDriverLoss("replica-" + m, replicas[m]);
        final int g = gatewayOn(m);
        if (g >= 0) {
            final ClusterProcesses.Child gateway = gateways.get(g);
            if (gateway.isAlive() && accepting(TEST_GATEWAY_PORT_BASE + g)) {
                failures.add(GATEWAY_NAMES[g] + " is still accepting TCP with no media driver");
            }
            assertDiedOnDriverLoss(GATEWAY_NAMES[g], gateway);
        }
        replicas[m].stop();
        startReplica(m);
        // The gateway returns as whatever the cluster now says: its peer was promoted when its session closed.
        if (g >= 0) {
            gateways.get(g).stop();
            startGateway(g);
        }
    }

    private void assertDiedOnDriverLoss(final String label, final ClusterProcesses.Child child) {
        if (!child.awaitDead(scaled(DRIVER_LOSS_GRACE_SECS))) {
            failures.add(label + " still running " + DRIVER_LOSS_GRACE_SECS + "s after its media driver died");
        } else if (child.exitValue() == 0) {
            failures.add(label + " exited 0 on driver loss; a restart-on-failure supervisor would not restart it");
        }
    }

    private void startReplica(final int m) {
        // The same arguments every time, or a restarted replica loses its arm and later queue-drops are no-ops.
        replicas[m] = cluster.probe("replica-" + m + "-b" + boots[m], m, "follow", "probe.clientId=9",
                                    "probe.faultInjection=true");
        recoveryStallBaseline[m] = 0;
        try {
            replicas[m].awaitLine("following live", scaled(60));
        } catch (final AssertionError ex) {
            System.out.println("  WARN replica " + m + " not caught up: " + ex.getMessage());
        }
    }

    private void startGateway(final int g) {
        final int member = g;
        final ClusterProcesses.Child gateway =
            cluster.harness("gateway-" + GATEWAY_NAMES[g] + "-b" + ++boots[member], member, TEST_GATEWAY, "serve",
                            "probe.clientId=10", "probe.gatewayName=" + GATEWAY_NAMES[g],
                            "probe.listenPort=" + (TEST_GATEWAY_PORT_BASE + g));
        gateways.set(g, gateway);
        try {
            gateway.awaitLine("Caught up", scaled(60));
        } catch (final AssertionError ex) {
            failures.add(GATEWAY_NAMES[g] + " not caught up: " + ex.getMessage());
        }
    }

    /** The member a gateway instance runs beside, or -1. */
    private static int gatewayOn(final int m) {
        return m < 2 ? m : -1;
    }

    /** The instance the log last says is serving, or -1. The last activation line wins. */
    private int activeGateway() {
        for (int g = 0; g < 2; g++) {
            final ClusterProcesses.Child gateway = gateways.get(g);
            if (gateway != null && "active".equals(status(gateway))) {
                return g;
            }
        }
        return -1;
    }

    private static String status(final ClusterProcesses.Child gateway) {
        final List<String> lines = gateway.lines("is now ");
        if (lines.isEmpty()) {
            return "";
        }
        return lines.get(lines.size() - 1).contains("is now active") ? "active" : "standby";
    }

    /** One line through the gate, answered only if it round-tripped consensus. */
    private boolean roundTrip(final String name, final int port, final int lines) {
        final ClusterProcesses.Child client =
            cluster.harness(name, CN, TEST_GATEWAY, "client", "probe.listenPort=" + port, "probe.count=" + lines);
        if (client.awaitDead(scaled(60))) {
            return client.exitValue() == 0;
        }
        client.stop();
        return false;
    }

    /** Batched, so a batch cut by a failover just ends and the next reconnects. */
    private void loadLoop() {
        while (loading) {
            final int g = activeGateway();
            try {
                if (g < 0 || !roundTrip("gateway-load", TEST_GATEWAY_PORT_BASE + g, LOAD_BATCH_FRAMES)) {
                    Thread.sleep(500);
                } else {
                    Thread.sleep(1000);
                }
            } catch (final InterruptedException ex) {
                return;
            } catch (final AssertionError ex) {
                return;
            }
        }
    }

    private void checkInvariants(final int round) {
        // (a) a leader
        try {
            System.out.println("  ok: leader = member " + cluster.awaitLeader());
        } catch (final AssertionError ex) {
            failures.add("no leader");
        }
        // (b) ingress -> consensus -> queue, through the co-located probe
        final ClusterProcesses.Child ping = cluster.probe("ping-" + round, CN, "ping");
        if (!ping.awaitDead(scaled(60)) || ping.exitValue() != 0) {
            failures.add("round-trip probe failed; see " + ping.log());
            ping.stop();
        }
        // (b2) exactly one gateway instance serving, a line through it sequenced, and both instances running
        int serving = 0;
        for (int g = 0; g < 2; g++) {
            serving += "active".equals(status(gateways.get(g))) ? 1 : 0;
            if (!gateways.get(g).isAlive()) {
                failures.add(GATEWAY_NAMES[g] + " is not running");
            }
        }
        if (serving != 1) {
            failures.add(serving + " gateway instance(s) report active, the pair must have exactly one");
        } else if (!roundTrip("gateway-probe-" + round, TEST_GATEWAY_PORT_BASE + activeGateway(), 1)) {
            failures.add("gateway round-trip failed through the active gate");
        }
        // (c) every replica is up and not stalled; the alarm fires per episode, a fresh wedge a round or two later
        for (int m = 0; m < ClusterProcesses.MEMBERS; m++) {
            if (!replicas[m].isAlive()) {
                failures.add("member " + m + "'s replica is not running");
                continue;
            }
            final int stalls = replicas[m].lines("recovery has dispatched nothing").size();
            if (stalls > recoveryStallBaseline[m]) {
                failures.add("member " + m + "'s replica reported recovery stalled");
            }
            recoveryStallBaseline[m] = stalls;
        }
        // (c2) an armed queue-drop must have produced a gap resumed at the hole
        if (dropTarget != ClusterProcesses.NO_MEMBER) {
            final int resumes = replicas[dropTarget].lines("queue gap: expected groupSeqNo").size();
            if (resumes <= dropBefore) {
                failures.add("queue-drop on member " + dropTarget + " produced no gap episode");
            }
            dropTarget = ClusterProcesses.NO_MEMBER;
        }
    }

    /**
     * Every member's queue recordings must each run groupSeqNo 1..N without a gap, and all members must end at the
     * same N. A restarted member rotates its queue, so each recording is checked alone.
     */
    private String verifySequence() throws Exception {
        loading = false;
        load.interrupt();
        load.join(TimeUnit.SECONDS.toMillis(120));
        for (int g = 0; g < 2; g++) {
            gateways.get(g).stop();
        }
        for (final ClusterProcesses.Child replica : replicas) {
            replica.stop();
        }
        sleep(3);
        // Frozen so the 1 Hz heartbeat cannot land between the dumps, which are not atomic.
        final List<ClusterProcesses.Child> paused = new ArrayList<>();
        final long[] highWater = new long[ClusterProcesses.MEMBERS];
        try {
            for (int m = 0; m < ClusterProcesses.MEMBERS; m++) {
                paused.add(cluster.memberLog(m));
                cluster.memberLog(m).pause();
            }
            sleep(1);
            for (int m = 0; m < ClusterProcesses.MEMBERS; m++) {
                final ClusterProcesses.Child dump = cluster.logPrinter("sequenced-dump-" + m, m);
                assertEquals(0, dump.awaitExit(scaled(180)), "SbeLogPrinter on member " + m + "; see " + dump.log());
                highWater[m] = highWater(m, dump.log());
            }
        } finally {
            paused.forEach(ClusterProcesses.Child::resume);
        }
        assertTrue(highWater[0] == highWater[1] && highWater[1] == highWater[2],
                   "nodes diverged: high-water " + highWater[0] + " / " + highWater[1] + " / " + highWater[2]);
        return "all 3 nodes recorded groupSeqNo 1.." + highWater[0];
    }

    private static long highWater(final int m, final Path dump) throws IOException {
        long expected = 0;
        long max = 0;
        long total = 0;
        try (Stream<String> lines = Files.lines(dump)) {
            for (final String line : (Iterable<String>)lines::iterator) {
                if (line.contains("[Catalog] Recording ID")) {
                    expected = 0;
                } else if (line.contains("\"groupSeqNo\"")) {
                    final long n = Long.parseLong(line.replaceAll("[^0-9]", ""));
                    total++;
                    if (expected == 0) {
                        assertEquals(1, n, "member " + m + ": recording starts at groupSeqNo " + n);
                    } else {
                        assertEquals(expected + 1, n, "member " + m + ": gap or regression after " + expected);
                    }
                    expected = n;
                    max = Math.max(max, n);
                }
            }
        }
        assertTrue(total > 0, "member " + m + ": no sequenced messages decoded");
        return max;
    }

    private int follower(final int leader) {
        int p;
        do {
            p = rng.nextInt(ClusterProcesses.MEMBERS);
        } while (p == leader);
        return p;
    }

    private static boolean accepting(final int port) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress("127.0.0.1", port), 1000);
            return true;
        } catch (final IOException ex) {
            return false;
        }
    }

    private static Duration scaled(final long secs) {
        return Duration.ofMillis((long)(secs * 1000 * SCALE));
    }

    private static void sleep(final long secs) {
        try {
            Thread.sleep(TimeUnit.SECONDS.toMillis(secs));
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted", ex);
        }
    }
}
