package org.limitless.seqeron.tools;

import static org.junit.jupiter.api.Assertions.assertTrue;

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
import org.limitless.seqeron.protocol.SeqeronCounters;

/**
 * The hosted-façade prototype (doc/hosted-facade-draft.md) through restore, the snapshot cut and SIGKILL. One
 * {@code TestApplication} replica of source 16 is a host and a client; the other two members run the direct one. The
 * client writes {@code TestApplication}'s records byte for byte, so each side compares the other's rounds with the
 * sequenced {@code SnapshotEnd} and fences on a difference (spec A-7): a run with no FENCED line is the hosted state
 * agreeing with the direct state, round after round.
 */
class HostedSnapshotTest {
    private static final String TEST_APPLICATION = "org.limitless.seqeron.tools.TestApplication";
    private static final String HOST = "org.limitless.seqeron.tools.HostedTestApplicationHost";
    private static final String CLIENT = "org.limitless.seqeron.tools.HostedTestApplicationClient";
    private static final int SOURCE_ID = 16;
    private static final int CLIENT_TIMEOUT_MS = 2000;
    private static final int SLOW_CUT_MS = 8000;
    private static final int MAX_ELECTIONS = 8;
    private static final int LEADER_PAUSE_MS = 4500;
    private static final Duration CATCHUP = Duration.ofSeconds(40);
    private static final int TIMEOUT_SECS = 40;
    private static final Pattern RESTORING = Pattern.compile("restoring round (\\d+)");

    @TempDir
    Path baseDir;

    private ClusterProcesses cluster;
    private final List<ClusterProcesses.Child> instances = new ArrayList<>();
    private final List<String> failures = new ArrayList<>();
    private int hostedMember;
    private ClusterProcesses.Child host;
    private ClusterProcesses.Child client;
    private int hostRuns;
    private int clientRuns;
    private int hostClientId;

    @BeforeEach
    void startCluster(final TestInfo test) {
        cluster = new ClusterProcesses(baseDir, Path.of(System.getProperty("seqeron.systemTestLogs"),
                                                        test.getTestClass().orElseThrow().getSimpleName()));
        cluster.startAll();
    }

    @AfterEach
    void stopCluster() {
        instances.forEach(ClusterProcesses.Child::stop);
        cluster.close();
    }

    @Test
    @Timeout(value = 600, unit = TimeUnit.SECONDS)
    void hostedReplicaRestoresCutsAndSurvivesKills() throws Exception {
        final int direct = bootstrap(false);

        // 1. Steady state: attach, cut, rounds.
        startHost();
        startClient();
        check("the hosted pair is caught up", client.lastLine("Caught up") != null);
        check("the hosted replica's rounds complete (the façade compared them with the direct publisher's)",
              waitRounds(hostedMember, hostClientId, 2));
        check("a direct replica's rounds complete", waitRounds(direct, 11, 2));
        check("no instance fenced", noFence());

        // 2. The client is killed, and its successor attaches before the host has noticed.
        final int restoresBefore = host.lines("restoring round").size();
        final int lostBefore = host.lines("CLIENT_LOST").size();
        final long killedNs = System.nanoTime();
        client.kill();
        startClientNoWait();
        check("the host refused the second Attach while the first attachment was live",
              poll(() -> !host.lines("refused Attach from another client").isEmpty()));
        check("the host declared the client lost", poll(() -> host.lines("CLIENT_LOST").size() > lostBefore));
        final long lostAfterMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - killedNs);
        check("it did so within clientTimeoutMs + 3 s (" + lostAfterMs + " ms after the kill)",
              lostAfterMs < CLIENT_TIMEOUT_MS + 3000);
        client.awaitLine("Caught up", CATCHUP);
        check("the host restored a snapshot", host.lines("restoring round").size() > restoresBefore);
        check("the new client was handed records and holds markers", restoredMarkers(client) > 0);
        check("rounds complete after the restore", waitRounds(hostedMember, hostClientId, 2));
        check("no instance fenced", noFence());

        // 3. The host is killed.
        host.kill();
        check("the client exits 70 within its host timeout", client.awaitDead(Duration.ofSeconds(8))
                                                                 && client.exitValue() == 70);
        check("it said the host was lost", !client.lines("HOST_LOST").isEmpty());
        startHost();
        startClient();
        check("the restarted pair restored a snapshot", !host.lines("restoring round").isEmpty());
        check("the client holds markers", restoredMarkers(client) > 0);
        check("rounds complete after the restart", waitRounds(hostedMember, hostClientId, 2));
        check("no instance fenced", noFence());

        // 4. The client is killed in the middle of a cut.
        client.kill();
        check("the host dropped the old client", poll(() -> !host.lines("CLIENT_LOST").isEmpty()));
        startClientNoWait("client.stallCutNo=2");
        client.awaitLine("IN CUT", Duration.ofSeconds(60));
        final long confirmed = round(hostedMember, hostClientId);
        final int lostBeforeCut = host.lines("CLIENT_LOST").size();
        final long cutAt = System.nanoTime();
        client.kill();
        check("the host abandoned the round and did not hang", poll(() -> !host.lines("ABANDON round").isEmpty()));
        check("it declared the client lost", poll(() -> host.lines("CLIENT_LOST").size() > lostBeforeCut));
        check("the direct replicas keep completing rounds", waitRounds(direct, 11, 2));
        check("no instance fenced", noFence());
        startClientNoWait("client.ghost=true");
        client.awaitLine("Caught up", CATCHUP);
        final Matcher restored = RESTORING.matcher(host.lastLine("restoring round") == null ? ""
                                                                                            : host.lastLine("restoring round"));
        check("the restore was from round " + confirmed + ", the last confirmed, not the abandoned one",
              restored.find() && Long.parseLong(restored.group(1)) == confirmed);
        check("rounds resume", waitRounds(hostedMember, hostClientId, 2));
        System.out.printf("cut-to-kill %d ms%n", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - cutAt));

        // 5. A command from a publication that is not the attachment's is never read (H-15).
        check("the client sent a command from a second publication", !client.lines("sent one command").isEmpty());
        check("the host never read it: no ghost marker was sequenced", client.lines("GHOST MARKER SEQUENCED").isEmpty());
        check("no instance fenced", noFence());

        assertTrue(failures.isEmpty(), failures.size() + " check(s) failed: " + failures);
    }

    /** Loads the topology and starts the direct replicas; the hosted pair goes on a follower or on the leader. */
    private int bootstrap(final boolean onLeader) {
        return bootstrap(onLeader, "topology-test-hosted.xml");
    }

    private int bootstrap(final boolean onLeader, final String topology) {
        cluster.loadTopology(Path.of(System.getProperty("seqeron.snapshotTopology")).resolveSibling(topology));
        final int leader = cluster.awaitLeader();
        hostedMember = onLeader ? leader : (leader + 1) % ClusterProcesses.MEMBERS;
        for (int m = 0; m < ClusterProcesses.MEMBERS; m++) {
            if (m != hostedMember) {
                startDirect(m);
            }
        }
        System.out.printf("leader %d, hosted pair on member %d%n", leader, hostedMember);
        return (hostedMember + 1) % ClusterProcesses.MEMBERS;
    }

    @Test
    @Timeout(value = 300, unit = TimeUnit.SECONDS)
    void aCutLongerThanTheSessionTimeoutKeepsTheSession() {
        bootstrap(false, "topology-test-hosted-slow.xml");
        startHost();
        startClientNoWait("client.slowCutMs=" + SLOW_CUT_MS);
        check("a round is confirmed though each cut outlasts the 5 s session timeout",
              poll(() -> round(hostedMember, hostClientId) > 0, 2 * TIMEOUT_SECS));
        check("the host did not declare the client lost", host.lines("CLIENT_LOST").isEmpty());
        check("the host did not abandon a round", host.lines("ABANDON round").isEmpty());
        check("no instance fenced", noFence());
        assertTrue(failures.isEmpty(), failures.size() + " check(s) failed: " + failures);
    }

    /**
     * The hosted pair is a follower. The leader is paused until the other two members elect, and again, until the
     * hosted member wins (no member is killed or restarted); then its own member is paused to depose it. At each
     * change the gate epoch must rise, a command under the previous epoch must be dropped by the host and never
     * sequenced, and everything the client offered must be sequenced exactly once or counted as dropped.
     */
    @Test
    @Timeout(value = 400, unit = TimeUnit.SECONDS)
    void aHostedReplicaGainsAndLosesLeadershipUnderGateEpochs() {
        final int other = bootstrap(false);
        startHost();
        startClientNoWait("client.staleEpoch=true");
        check("the hosted pair is caught up", poll(() -> client.lastLine("Caught up") != null));
        check("as a follower it follows and publishes nothing",
              poll(() -> stateMarkers(client) >= 10) && lastNumber(client, "state: ", PUBLISHED) == 0);
        check("its rounds complete", waitRounds(hostedMember, hostClientId, 1));
        final long followerEpoch = lastNumber(host, "gate epoch", GATE_EPOCH);

        // 1. Leadership moves to the hosted member.
        int attempts = 0;
        while (cluster.awaitLeader() != hostedMember && attempts++ < MAX_ELECTIONS) {
            pauseLeaderUntilDeposed();
        }
        check("the hosted member was elected (after " + attempts + " pause(s) of the leader)",
              cluster.awaitLeader() == hostedMember);
        check("the gate opened", poll(() -> client.lastLine("now leading") != null));
        check("the client applied the host's gate epoch", poll(this::epochsAgree));
        final long leadingEpoch = lastNumber(host, "gate epoch", GATE_EPOCH);
        check("the host's gate epoch rose (" + followerEpoch + " to " + leadingEpoch + ")", leadingEpoch > followerEpoch);
        check("the host dropped the marker sent under the previous epoch (H-3), and said nothing back",
              poll(() -> lastNumber(host, "queue:", EPOCH_DROPS) >= 1));
        check("the client's markers were sequenced", poll(() -> lastNumber(client, "state: ", PUBLISHED) >= 20));
        check("the stale marker was never sequenced", client.lines("STALE MARKER SEQUENCED").isEmpty());
        check("the hosted replica's rounds complete as leader", waitRounds(hostedMember, hostClientId, 2));
        check("a direct replica's rounds complete", waitRounds(other, 11, 2));
        check("no instance fenced", noFence());

        // 2. Leadership leaves it, the pair alive throughout.
        final long markersBefore = stateMarkers(client);
        pauseLeaderUntilDeposed();
        check("the gate closed", poll(() -> client.lastLine("not leading") != null));
        check("the client applied the host's gate epoch", poll(this::epochsAgree));
        final long followingEpoch = lastNumber(host, "gate epoch", GATE_EPOCH);
        check("the host's gate epoch rose again (" + leadingEpoch + " to " + followingEpoch + ")",
              followingEpoch > leadingEpoch);
        check("another member leads", poll(() -> cluster.awaitLeader() != hostedMember));
        sleep(2500);
        final long published = lastNumber(client, "state: ", PUBLISHED);
        sleep(4500);
        check("the client publishes nothing as a follower", lastNumber(client, "state: ", PUBLISHED) == published);
        check("it follows the new leader's markers", poll(() -> stateMarkers(client) >= markersBefore + 20));
        check("the stale marker was still never sequenced", client.lines("STALE MARKER SEQUENCED").isEmpty());
        check("every marker the client offered was sequenced exactly once, or dropped by the host: published - " +
              "epoch drops (the stale one apart) = sequenced", poll(() -> {
                  return lastNumber(client, "state: ", PUBLISHED) - (lastNumber(host, "queue:", EPOCH_DROPS) - 1) ==
                         lastNumber(client, "state: ", OWN);
              }));
        System.out.println(client.lastLine("state: "));
        check("none was sequenced out of order or twice", client.lines("OWN MARKER OUT OF ORDER").isEmpty());
        check("the hosted replica's rounds complete as a follower", waitRounds(hostedMember, hostClientId, 2));
        check("no instance fenced", noFence());
        check("the host and client are still attached", host.isAlive() && client.isAlive());
        assertTrue(failures.isEmpty(), failures.size() + " check(s) failed: " + failures);
    }

    @Test
    @Timeout(value = 300, unit = TimeUnit.SECONDS)
    void aCorruptedRecordIsFencedByTheHost() {
        bootstrap(false);
        startHost();
        startClientNoWait("client.corruptRecord=true");
        check("the host fenced the hosted replica with SNAPSHOT_DIVERGED",
              poll(() -> host.lines("FENCED: SNAPSHOT_DIVERGED").size() == 1));
        check("the client heard it and exited 70", client.awaitDead(Duration.ofSeconds(10))
                                                       && client.exitValue() == 70);
        check("the client logged FENCED", !client.lines("FENCED").isEmpty());
        check("the host exited 70", host.awaitDead(Duration.ofSeconds(10)) && host.exitValue() == 70);
        check("no direct replica fenced", instances.stream().filter(c -> c != host && c != client)
                                              .allMatch(c -> c.lines("FENCED").isEmpty()));
        assertTrue(failures.isEmpty(), failures.size() + " check(s) failed: " + failures);
    }

    @Test
    @Timeout(value = 300, unit = TimeUnit.SECONDS)
    void aHostedLeaderPublishesAndItsRoundsAgreeWithTheDirectReplicas() {
        final int direct = bootstrap(true);
        startHost();
        startClientNoWait();
        check("the gate opened", poll(() -> client.lastLine("now leading") != null));
        check("the client's markers were sequenced", poll(() -> stateMarkers(client) >= 30));
        check("a direct replica's rounds complete (it compared the hosted replica's published ends)",
              waitRounds(direct, 11, 3));
        check("the hosted replica's rounds complete", waitRounds(hostedMember, hostClientId, 2));
        check("no instance fenced", noFence());
        System.out.println(host.lastLine("queue:"));
        assertTrue(failures.isEmpty(), failures.size() + " check(s) failed: " + failures);
    }

    private static final Pattern PUBLISHED = Pattern.compile("published (\\d+)");
    private static final Pattern EPOCH_DROPS = Pattern.compile("epochDrops (\\d+)");
    private static final Pattern OWN = Pattern.compile("own (\\d+)");
    private static final Pattern GATE_EPOCH = Pattern.compile("gate epoch (\\d+)");

    /** The client's last applied gate epoch is the host's current one. */
    private boolean epochsAgree() {
        return lastNumber(client, "canPublish", GATE_EPOCH) == lastNumber(host, "gate epoch", GATE_EPOCH);
    }

    /** SIGSTOPs the leader for longer than the heartbeat timeout, so the others elect, then waits for one leader. */
    private void pauseLeaderUntilDeposed() {
        final ClusterProcesses.Child leader = cluster.memberLog(cluster.awaitLeader());
        try {
            leader.pause();
            sleep(LEADER_PAUSE_MS);
        } finally {
            leader.resume();
        }
        cluster.awaitLeader();
    }

    private static long lastNumber(final ClusterProcesses.Child child, final String contains, final Pattern pattern) {
        final String line = child.lastLine(contains);
        if (line == null) {
            return 0;
        }
        final Matcher matcher = pattern.matcher(line);
        return matcher.find() ? Long.parseLong(matcher.group(1)) : 0;
    }

    private static void sleep(final long ms) {
        try {
            Thread.sleep(ms);
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }

    private static long stateMarkers(final ClusterProcesses.Child child) {
        final String line = child.lastLine("state: ");
        if (line == null) {
            return 0;
        }
        final Matcher matcher = Pattern.compile("state: (\\d+) marker").matcher(line);
        return matcher.find() ? Long.parseLong(matcher.group(1)) : 0;
    }

    private void startDirect(final int m) {
        final ClusterProcesses.Child app = cluster.harness("app-" + m, m, TEST_APPLICATION, null,
                                                           "probe.sourceId=" + SOURCE_ID,
                                                           "probe.snapshotDir=" + baseDir.resolve("snap")
                                                               .resolve("app-" + m));
        instances.add(app);
        app.awaitLine("Caught up", CATCHUP);
    }

    private void startHost() {
        hostRuns++;
        hostClientId = 30 + hostRuns;
        host = cluster.harness("host-" + hostRuns, hostedMember, HOST, null, "probe.sourceId=" + SOURCE_ID,
                               "probe.clientId=" + hostClientId, "host.clientTimeoutMs=" + CLIENT_TIMEOUT_MS,
                               "probe.snapshotDir=" + baseDir.resolve("snap").resolve("host"));
        instances.add(host);
        host.awaitLine("host ready", Duration.ofSeconds(30));
    }

    private void startClient() {
        startClientNoWait();
        client.awaitLine("Caught up", CATCHUP);
    }

    private void startClientNoWait(final String... properties) {
        final List<String> all = new ArrayList<>(List.of("probe.sourceId=" + SOURCE_ID));
        all.addAll(List.of(properties));
        client = cluster.harness("client-" + ++clientRuns, hostedMember, CLIENT, null, all.toArray(new String[0]));
        instances.add(client);
    }

    private static long restoredMarkers(final ClusterProcesses.Child child) {
        final String line = child.lastLine("restored record 0");
        if (line == null) {
            return 0;
        }
        final Matcher matcher = Pattern.compile("(\\d+) marker\\(s\\)").matcher(line);
        return matcher.find() ? Long.parseLong(matcher.group(1)) : 0;
    }

    private void check(final String what, final boolean ok) {
        System.out.println((ok ? "  ok   " : "  FAIL ") + what);
        if (!ok) {
            failures.add(what);
        }
    }

    private long round(final int m, final int clientId) {
        final long value = cluster.counter(m, SeqeronCounters.APP_SNAPSHOT_ROUND_TYPE_ID,
                                           "snapshotRound source=" + SOURCE_ID + " member=" + m + " client=" + clientId);
        return value == ClusterProcesses.NO_VALUE ? 0 : value;
    }

    /** Waits until that instance has confirmed {@code n} rounds past where it is now, or past its first. */
    private boolean waitRounds(final int m, final int clientId, final int n) {
        final long[] base = {0};
        if (!poll(() -> (base[0] = round(m, clientId)) > 0)) {
            return false;
        }
        return poll(() -> round(m, clientId) >= base[0] + n);
    }

    private boolean noFence() {
        return instances.stream().allMatch(child -> child.lines("FENCED").isEmpty());
    }

    private static boolean poll(final BooleanSupplier condition) {
        return poll(condition, TIMEOUT_SECS);
    }

    private static boolean poll(final BooleanSupplier condition, final int timeoutSecs) {
        final long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSecs);
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
