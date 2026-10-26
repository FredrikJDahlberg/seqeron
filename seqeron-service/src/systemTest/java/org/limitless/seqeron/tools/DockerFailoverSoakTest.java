package org.limitless.seqeron.tools;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * ROUNDS leader failovers of the docker cluster, each under continuous ProbeMarker load, the
 * killed member restored between them so every round starts from a full three. A single failover against a
 * heartbeat-only archive proves only that the replay path connects: the frames it replays are the sequencer's own
 * heartbeats. Asserted, in the order they matter: every round is a genuine leadership change; a long-lived observer on
 * each node delivers in order across every failover ({@code ReplayerStreamReceiver} enforces gap-freedom, a hole
 * triggering a healing resume logged as "re-converged"); and a cold follow started at the end replays the whole
 * history, every tenure, off the final leader's one continuous recording and reaches "following live". There are no
 * snapshots, so every rejoin replays the whole Raft log, and the last round's holds fourteen rounds of load.
 */
@Tag("docker")
class DockerFailoverSoakTest {
    private static final int ROUNDS = Integer.parseInt(System.getenv().getOrDefault("ROUNDS", "15"));
    private static final long SOAK_SECS = Long.parseLong(System.getenv().getOrDefault("SOAK_SECS", "20"));
    private static final int SUBMIT_COUNT = Integer.parseInt(System.getenv().getOrDefault("SUBMIT_COUNT", "5000"));
    private static final String SUBMIT_PACING_MICROS = System.getenv().getOrDefault("SUBMIT_PACING_MICROS", "1000");
    private static final long MIN_DELIVERED = Long.parseLong(System.getenv().getOrDefault("MIN_DELIVERED", "1000"));
    // A SIGKILLed member's mark files stay active until the driver timeout (10 s) passes; a restart fails before then.
    private static final long MARK_FILE_SETTLE_SECS = Long.parseLong(System.getenv().getOrDefault(
        "SIGKILL_MARKFILE_SETTLE_SECS", "12"));
    private static final Pattern DELIVERED = Pattern.compile("frames delivered in order: (\\d+)");
    private static final Pattern REPLAY_BYTES = Pattern.compile("\\[0,(\\d+)\\)");
    private static final Pattern REPLY_ID = Pattern.compile("requests replays as client (\\d+)");

    private DockerCluster cluster;
    private Path logDir;
    private final AtomicBoolean loading = new AtomicBoolean(true);
    private Thread loadThread;
    private final List<ClusterProcesses.Child> loads = new ArrayList<>();

    @BeforeEach
    void startCluster() {
        logDir = Path.of(System.getProperty("seqeron.systemTestLogs"), "DockerFailoverSoakTest");
        cluster = new DockerCluster(logDir);
        cluster.start(Path.of(System.getProperty("seqeron.topologyGroups")), false);
    }

    @AfterEach
    void stopCluster() throws InterruptedException {
        loading.set(false);
        if (loadThread != null) {
            loadThread.join(TimeUnit.SECONDS.toMillis(30));
        }
        cluster.close();
    }

    @Test
    @Timeout(value = 3600, unit = TimeUnit.SECONDS)
    void followersDeliverInOrderAcrossEveryFailoverAndAColdClientReplaysTheWholeHistory() throws Exception {
        final ClusterProcesses.Child[] observers = new ClusterProcesses.Child[DockerCluster.MEMBERS];
        final int[] observerRuns = new int[DockerCluster.MEMBERS];
        for (int m = 0; m < DockerCluster.MEMBERS; m++) {
            observers[m] = observer(m, ++observerRuns[m]);
        }
        loadThread = new Thread(this::generateLoad, "load");
        loadThread.start();

        int leader = cluster.awaitLeader(DockerCluster.deadline(120), DockerCluster.NO_MEMBER);
        final StringBuilder tenures = new StringBuilder("" + leader);
        int failovers = 0;
        for (int round = 1; round <= ROUNDS; round++) {
            System.out.printf("round %d/%d: soaking %d s under load, leader = member %d%n", round, ROUNDS, SOAK_SECS,
                              leader);
            Thread.sleep(TimeUnit.SECONDS.toMillis(SOAK_SECS));
            final int killed = leader;
            cluster.kill(killed);
            leader = cluster.awaitLeader(DockerCluster.deadline(120), killed);
            failovers++;
            tenures.append(" -> ").append(leader);
            // Rejoin into a moving cluster: the load runs throughout the settle.
            Thread.sleep(TimeUnit.SECONDS.toMillis(MARK_FILE_SETTLE_SECS));
            cluster.restart(killed, DockerCluster.deadline(120));
            observers[killed] = observer(killed, ++observerRuns[killed]);
        }
        System.out.printf("final soak %d s under load%n", SOAK_SECS);
        Thread.sleep(TimeUnit.SECONDS.toMillis(SOAK_SECS));
        loading.set(false);
        loadThread.join(TimeUnit.SECONDS.toMillis(60));

        final ClusterProcesses.Child cold = cluster.probe("cold-follower", leader, "follow", "probe.clientId=9");
        boolean caught = true;
        try {
            cold.awaitLine("following live", DockerCluster.deadline(180));
        } catch (final AssertionError ex) {
            caught = false;
        }
        final boolean caughtUp = caught;

        // SIGTERM every probe so its barrier releases and its report prints.
        for (int m = 0; m < DockerCluster.MEMBERS; m++) {
            if (cluster.isRunning(m)) {
                cluster.terminateProbes(m);
            }
        }
        Thread.sleep(3000);
        cold.kill();

        final String replyId = firstGroup(REPLY_ID, cold.lines(""));
        final List<String> decisions = new ArrayList<>();
        for (final String line : cluster.nodeLog(leader).split("\n")) {
            if (line.contains("replay for client " + replyId + ":")) {
                decisions.add(line);
            }
        }
        long replayBytes = 0;
        if (!decisions.isEmpty()) {
            final Matcher matcher = REPLAY_BYTES.matcher(decisions.get(decisions.size() - 1));
            if (matcher.find()) {
                replayBytes = Long.parseLong(matcher.group(1));
            }
        }
        int submitted = 0;
        for (final ClusterProcesses.Child load : loads) {
            if (!load.lines("ProbeMarker(s)").isEmpty()) {
                submitted++;
            }
        }
        System.out.printf("leader tenures: %s%nfailovers: %d/%d; ProbeMarkers submitted (>=): %d%n"
                          + "cold client caught up: %b; replays: %d; replayed history bytes: %d%n", tenures,
                          failovers, ROUNDS, (long)submitted * SUBMIT_COUNT, caughtUp, decisions.size(), replayBytes);

        final long bytes = replayBytes;
        final int replays = decisions.size();
        final List<Runnable> checks = new ArrayList<>();
        for (int m = 0; m < DockerCluster.MEMBERS; m++) {
            final int member = m;
            final String deliveredText = firstGroupLast(DELIVERED, observers[m].lines(""));
            final long delivered = deliveredText == null ? 0 : Long.parseLong(deliveredText);
            final int reconverged = observers[m].lines("re-converged at clusterSeqNo").size();
            System.out.printf("observer node-%d: %d frames in order, %d re-convergence(s)%n", m, delivered,
                              reconverged);
            checks.add(() -> assertTrue(delivered >= MIN_DELIVERED, "observer on node-" + member + " delivered "
                                                                      + delivered + " < " + MIN_DELIVERED
                                                                      + " frames"));
        }
        final int completed = failovers;
        assertAll(
            () -> assertEquals(ROUNDS, completed, "failovers completed"),
            () -> assertTrue(caughtUp, "the cold client never caught up"),
            () -> assertTrue(replays >= 1, "no replay was served to the cold client"),
            () -> assertTrue(bytes > 100_000, "replayed only " + bytes + " bytes: the history is too thin to be "
                                               + "evidence"),
            () -> checks.forEach(Runnable::run));
    }

    private ClusterProcesses.Child observer(final int member, final int run) {
        return cluster.probe("observer-" + member + "-" + run, member, "follow", "probe.clientId=1");
    }

    /** Continuous ProbeMarkers through the first running member, restarted whenever a failover kills the client. */
    private void generateLoad() {
        int run = 0;
        while (loading.get()) {
            int target = DockerCluster.NO_MEMBER;
            for (int m = 0; m < DockerCluster.MEMBERS && target == DockerCluster.NO_MEMBER; m++) {
                if (cluster.isRunning(m)) {
                    target = m;
                }
            }
            if (target == DockerCluster.NO_MEMBER) {
                pause(1000);
                continue;
            }
            final ClusterProcesses.Child load = cluster.probe("load-" + ++run, target, "submit",
                                                              "probe.count=" + SUBMIT_COUNT,
                                                              "probe.pacingMicros=" + SUBMIT_PACING_MICROS);
            synchronized (loads) {
                loads.add(load);
            }
            try {
                load.awaitExit(Duration.ofSeconds(120));
            } catch (final AssertionError ex) {
                load.kill();
            }
            pause(500);
        }
    }

    private static void pause(final long ms) {
        try {
            Thread.sleep(ms);
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }

    private static String firstGroup(final Pattern pattern, final List<String> lines) {
        for (final String line : lines) {
            final Matcher matcher = pattern.matcher(line);
            if (matcher.find()) {
                return matcher.group(1);
            }
        }
        return null;
    }

    private static String firstGroupLast(final Pattern pattern, final List<String> lines) {
        String last = null;
        for (final String line : lines) {
            final Matcher matcher = pattern.matcher(line);
            if (matcher.find()) {
                last = matcher.group(1);
            }
        }
        return last;
    }
}
