package org.limitless.seqeron.tools;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.limitless.seqeron.protocol.PortLayout;

/**
 * The three-node cluster of {@code docker/compose.yml}, driven through the {@code docker} CLI, with the faulteron
 * injector aimed at one of its members. Testcontainers' compose support refuses that file's fixed
 * {@code container_name}, which faulteron's {@code --network container:node-N} needs. A member's state is read with
 * {@code docker exec node-N clusterctl counters}, never from its log. The members' port base is
 * {@link PortLayout}'s, so the test JVM's {@code SEQERON_PORT_BASE} must be the containers'.
 */
final class DockerCluster implements AutoCloseable {
    static final int MEMBERS = 3;
    static final int NO_MEMBER = -1;
    /** What {@link #counter} reads from a member that is down or has not started its sequencer. */
    static final long NO_VALUE = Long.MIN_VALUE;

    private static final int TIMEOUT_SCALE = Integer.parseInt(System.getenv().getOrDefault("TIMEOUT_SCALE", "1"));
    private static final Path REPO_ROOT = Path.of(System.getProperty("seqeron.repoRoot"));
    private static final Path FAULTERON_DIR = Path.of(System.getenv().getOrDefault(
        "FAULTERON_DIR", REPO_ROOT.resolve("../faulteron").normalize().toString()));
    private static final List<String> COMPOSE =
        List.of("docker", "compose", "-f", REPO_ROOT.resolve("docker/compose.yml").toString());
    private static final Pattern COUNTER = Pattern.compile("^(\\S+) member=\\d+\\s+= (-?\\d+)$");
    private static final Pattern DROPPED = Pattern.compile("dropped=(\\d+)");
    private static final long POLL_MS = 500;

    private final Path logDir;

    DockerCluster(final Path logDir) {
        this.logDir = logDir;
    }

    /** {@code seconds} scaled by {@code TIMEOUT_SCALE}, which CI raises for its slower runners. */
    static Duration deadline(final int seconds) {
        return Duration.ofSeconds((long)seconds * TIMEOUT_SCALE);
    }

    /** Builds the injector image, brings the cluster up healthy, loads the groups, and waits for every queue. */
    void start(final Path topology) {
        mkdirs();
        compose(Duration.ofSeconds(120), "down", "-v", "--remove-orphans");
        require(run(Duration.ofSeconds(600), "docker", "build", "-q", "-t", "faulteron:local",
                    FAULTERON_DIR.toString()), "faulteron image did not build");
        require(compose(deadline(120).plusSeconds(600), "up", "--build", "-d", "--wait", "--wait-timeout",
                        "" + deadline(120).toSeconds()), "cluster did not come up healthy");
        final int leader = awaitLeader(deadline(60), NO_MEMBER);
        require(run(Duration.ofSeconds(30), "docker", "cp", topology.toString(), "node-0:/tmp/topology.xml"),
                "could not copy the topology");
        require(run(Duration.ofSeconds(60), "docker", "exec", "node-0", "clusterctl", "load-topology",
                    "/tmp/topology.xml"), "load-topology failed, leader " + leader);
        for (int m = 0; m < MEMBERS; m++) {
            final int member = m;
            await("member " + m + "'s queue to answer a ping", deadline(60), () -> ping(member));
        }
    }

    /**
     * Polls {@code condition} until it holds, or fails at the deadline.
     *
     * @param what      the condition, for the failure message
     * @param timeout   how long it may take
     * @param condition the condition
     */
    void await(final String what, final Duration timeout, final BooleanSupplier condition) {
        if (!poll(timeout, condition)) {
            throw new AssertionError("no " + what + " within " + timeout + "; logs in " + logDir);
        }
    }

    /** Polls {@code condition} until it holds, and says whether it did within {@code timeout}. */
    boolean poll(final Duration timeout, final BooleanSupplier condition) {
        final long end = System.nanoTime() + timeout.toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > end) {
                return false;
            }
            sleep(POLL_MS);
        }
        return true;
    }

    /**
     * The member every member but {@code excluded} records as leader. A cut-off leader keeps the record that made it
     * leader, so it is left out of the vote, and a record of {@code excluded} itself is not yet an answer.
     *
     * @param timeout  how long it may take
     * @param excluded the member to leave out, or {@link #NO_MEMBER}
     * @return the leader
     */
    int awaitLeader(final Duration timeout, final int excluded) {
        final int[] leader = {NO_MEMBER};
        await("agreed leader" + (excluded == NO_MEMBER ? "" : " other than member " + excluded), timeout, () -> {
            leader[0] = agreedLeader(excluded);
            return leader[0] != NO_MEMBER;
        });
        return leader[0];
    }

    /** As {@link #awaitLeader}, but {@link #NO_MEMBER} instead of a failure when none emerges in time. */
    int pollLeader(final Duration timeout, final int excluded) {
        final int[] leader = {NO_MEMBER};
        poll(timeout, () -> {
            leader[0] = agreedLeader(excluded);
            return leader[0] != NO_MEMBER;
        });
        return leader[0];
    }

    /** The member every member but {@code excluded} records as leader right now, or {@link #NO_MEMBER}. */
    int agreedLeader(final int excluded) {
        long agreed = NO_VALUE;
        for (int m = 0; m < MEMBERS; m++) {
            if (m == excluded) {
                continue;
            }
            final long view = counter(m, "seqeron.sequencer.currentLeaderMemberId");
            if (view < 0 || view >= MEMBERS || view == excluded || (agreed != NO_VALUE && view != agreed)) {
                return NO_MEMBER;
            }
            agreed = view;
        }
        return (int)agreed;
    }

    /** Leadership changes the members have applied, summed: one each per term, so an election adds to it. */
    long leadershipChanges() {
        long total = 0;
        for (int m = 0; m < MEMBERS; m++) {
            total += Math.max(0, counter(m, "seqeron.sequencer.leadershipChangeCount"));
        }
        return total;
    }

    /** The member's seqeron counters, by name without the member label; empty if its sequencer is not up. */
    Map<String, Long> counters(final int member) {
        final Result result = run(Duration.ofSeconds(30), "docker", "exec", "node-" + member, "clusterctl", "counters");
        final Map<String, Long> counters = new HashMap<>();
        if (result.status() == 0) {
            for (final String line : result.output().split("\n")) {
                final Matcher matcher = COUNTER.matcher(line.trim());
                if (matcher.matches()) {
                    counters.put(matcher.group(1), Long.parseLong(matcher.group(2)));
                }
            }
        }
        return counters;
    }

    /** One of the member's counters, or {@link #NO_VALUE}. */
    long counter(final int member, final String name) {
        return counters(member).getOrDefault(name, NO_VALUE);
    }

    /** One frame through consensus and back off the member's queue: true while that queue is current. */
    boolean ping(final int member) {
        return run(Duration.ofSeconds(30), probeCommand(member, "ping")).status() == 0;
    }

    /**
     * Starts a {@link ClusterProbe} in the member's container, its output in a file of its own.
     *
     * @param name       the process's log name
     * @param member     the member whose container it runs in
     * @param mode       the probe's mode
     * @param properties {@code key=value} system properties
     * @return the running probe
     */
    ClusterProcesses.Child probe(final String name, final int member, final String mode, final String... properties) {
        final Path log = logDir.resolve(name + ".log");
        try {
            final Process process = new ProcessBuilder(probeCommand(member, mode, properties))
                .redirectErrorStream(true).redirectOutput(log.toFile()).start();
            return new ClusterProcesses.Child(process, log);
        } catch (final IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    /** The faulteron CLI, in the member's network namespace. */
    Result faulteron(final int member, final String... arguments) {
        final List<String> command = new ArrayList<>(List.of("docker", "run", "--rm", "--privileged", "--network",
                                                            "container:node-" + member, "faulteron:local",
                                                            "faulteron"));
        command.addAll(List.of(arguments));
        return run(Duration.ofSeconds(60), command.toArray(String[]::new));
    }

    /** The member's faulteron rules' stats and then a detach, in one run, so the fault ends as soon as it is read. */
    Result heal(final int member) {
        return run(Duration.ofSeconds(60), "docker", "run", "--rm", "--privileged", "--network",
                   "container:node-" + member, "faulteron:local", "sh", "-c", "faulteron stats && faulteron detach");
    }

    /** Every member's archive, consensus, log and transfer port: what members speak among themselves. */
    static String[] internalPorts() {
        final List<String> ports = new ArrayList<>();
        for (int m = 0; m < MEMBERS; m++) {
            for (final int port : new int[] {PortLayout.archivePort(m), PortLayout.consensusPort(m),
                                             PortLayout.logPort(m), PortLayout.transferPort(m)}) {
                ports.add("" + port);
            }
        }
        return ports.toArray(String[]::new);
    }

    /** The sum of the {@code dropped} counters over the rules in {@code stats}. */
    static long dropped(final String stats) {
        long total = 0;
        final Matcher matcher = DROPPED.matcher(stats);
        while (matcher.find()) {
            total += Long.parseLong(matcher.group(1));
        }
        return total;
    }

    /** The {@code dropped} counters summed over the rules of one frame type in {@code stats}. */
    static long dropped(final String stats, final String type) {
        long total = 0;
        for (final String line : stats.split("\n")) {
            final String[] fields = line.trim().split("\\s+");
            if (fields.length > 1 && fields[1].equals(type)) {
                total += dropped(line);
            }
        }
        return total;
    }

    /** SIGKILLs the member's container, as a crash does: its mark files stay active. */
    void kill(final int member) {
        require(run(Duration.ofSeconds(30), "docker", "kill", "-s", "KILL", "node-" + member),
                "could not kill node-" + member);
    }

    /** Starts the member's stopped container and waits for its health check. */
    void restart(final int member, final Duration timeout) {
        require(run(Duration.ofSeconds(60), "docker", "start", "node-" + member), "could not start node-" + member);
        await("node-" + member + " to be healthy", timeout, () -> {
            final Result health = run(Duration.ofSeconds(30), "docker", "inspect", "-f",
                                      "{{.State.Health.Status}}", "node-" + member);
            return health.status() == 0 && health.output().trim().equals("healthy");
        });
    }

    boolean isRunning(final int member) {
        final Result result = run(Duration.ofSeconds(30), "docker", "inspect", "-f", "{{.State.Running}}",
                                  "node-" + member);
        return result.status() == 0 && result.output().trim().equals("true");
    }

    private List<String> probeCommand(final int member, final String mode, final String... properties) {
        final List<String> command = new ArrayList<>(List.of("docker", "exec", "node-" + member, "java"));
        command.addAll(ClusterProcesses.JAVA_OPTS);
        command.addAll(List.of("-Dprobe.memberId=" + member, "-Dprobe.aeronDir=/dev/shm/aeron-" + member,
                               "-Dprobe.ingressEndpoints=" + PortLayout.ingressEndpoints(
                                   List.of("node-0", "node-1", "node-2")),
                               "-Dprobe.egressHost=node-" + member));
        for (final String property : properties) {
            command.add("-D" + property);
        }
        command.addAll(List.of("-cp", "/opt/seqeron/lib/*", ClusterProbe.class.getName(), mode));
        return command;
    }

    private Result compose(final Duration timeout, final String... arguments) {
        final List<String> command = new ArrayList<>(COMPOSE);
        command.addAll(List.of(arguments));
        return run(timeout, command.toArray(String[]::new));
    }

    private void require(final Result result, final String message) {
        if (result.status() != 0) {
            throw new AssertionError(message + " (exit " + result.status() + "):\n" + result.output());
        }
    }

    /** Runs a command to completion, its stdout and stderr together. */
    private Result run(final Duration timeout, final String... command) {
        return run(timeout, List.of(command));
    }

    private Result run(final Duration timeout, final List<String> command) {
        try {
            final Path output = Files.createTempFile(logDir, "run-", ".out");
            final Process process = new ProcessBuilder(command).redirectErrorStream(true)
                .redirectOutput(output.toFile()).start();
            if (!process.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
                process.destroyForcibly();
                throw new AssertionError("timed out after " + timeout + ": " + String.join(" ", command));
            }
            final String text = new String(Files.readAllBytes(output), StandardCharsets.UTF_8);
            Files.delete(output);
            return new Result(process.exitValue(), text);
        } catch (final IOException ex) {
            throw new UncheckedIOException(ex);
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted", ex);
        }
    }

    private void mkdirs() {
        try {
            Files.createDirectories(logDir);
        } catch (final IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    private static void sleep(final long ms) {
        try {
            Thread.sleep(ms);
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted", ex);
        }
    }

    /** Keeps each member's log, then removes the containers and their volumes. */
    @Override
    public void close() {
        for (int m = 0; m < MEMBERS; m++) {
            try {
                Files.writeString(logDir.resolve("node-" + m + ".log"),
                                  compose(Duration.ofSeconds(60), "logs", "--no-color", "node-" + m).output());
            } catch (final IOException ex) {
                throw new UncheckedIOException(ex);
            }
        }
        compose(Duration.ofSeconds(120), "down", "-v", "--remove-orphans");
    }

    /** A finished command's exit status and its output. */
    record Result(int status, String output) {
    }
}
