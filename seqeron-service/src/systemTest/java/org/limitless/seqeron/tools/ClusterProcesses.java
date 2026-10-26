package org.limitless.seqeron.tools;

import io.aeron.CncFileDescriptor;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.service.Cluster;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.MappedByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.agrona.BufferUtil;
import org.agrona.IoUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;
import org.limitless.seqeron.protocol.PortLayout;
import org.limitless.seqeron.protocol.SeqeronCounters;
import org.limitless.seqeron.replayer.server.ReplayerServer;

/**
 * A three-member cluster of child JVMs off the uber jar, with the probes and clusterctl runs a test launches beside
 * it. State is read from each member's CnC counters, never its log, and a member that exits unasked fails the wait
 * in progress. Each process writes its own log under the test's log directory.
 */
final class ClusterProcesses implements AutoCloseable {
    static final int MEMBERS = 3;
    static final int NO_MEMBER = -1;
    /** What {@link #counter} reads before a member's driver or the counter exists. */
    static final long NO_VALUE = Long.MIN_VALUE;

    private static final String JAR = System.getProperty("seqeron.jar");
    // Set when the build measures coverage; each child then writes an exec file named for its log.
    private static final String JACOCO_AGENT = System.getProperty("seqeron.jacocoAgent");
    private static final String JACOCO_CHILDREN = System.getProperty("seqeron.jacocoChildren");
    private static final String JAVA = Path.of(System.getProperty("java.home"), "bin", "java").toString();
    // seqeron-home.sh's SEQERON_JAVA_OPTS.
    static final List<String> JAVA_OPTS = List.of(
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED", "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED", "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED",
        "--add-opens=java.base/java.util.zip=ALL-UNNAMED");
    private static final long POLL_MS = 50;
    private static final Duration EXIT_TIMEOUT = Duration.ofSeconds(10);

    private final Path baseDir;
    private final Path logDir;
    private final Child[] members = new Child[MEMBERS];
    private final boolean[] killed = new boolean[MEMBERS];
    private final List<Child> children = Collections.synchronizedList(new ArrayList<>());

    ClusterProcesses(final Path baseDir, final Path logDir) {
        this.baseDir = baseDir;
        this.logDir = logDir;
    }

    /** Starts every member and waits for each consensus module to be active under exactly one leader. */
    void startAll() {
        for (int m = 0; m < MEMBERS; m++) {
            startMember(m, "seq-" + m);
        }
        awaitActive();
        awaitLeader();
    }

    /**
     * Starts member {@code m}, its output in {@code logName}'s file. Starting a member that was stopped restarts it
     * on its own archive and directory.
     */
    Child startMember(final int m, final String logName) {
        return startMember(m, logName, false);
    }

    /** As {@link #startMember(int, String)}; {@code faultInjection} arms the queue-stall trigger file. */
    Child startMember(final int m, final String logName, final boolean faultInjection) {
        final String hosts = String.join(",", "localhost", "localhost", "localhost");
        killed[m] = false;
        members[m] = launch(logName,
                            List.of("-Xlog:gc", "-Dsequencer.memberId=" + m, "-Dsequencer.baseDir=" + baseDir,
                                    "-Dsequencer.aeronDir=" + aeronDir(m), "-Dsequencer.hosts=" + hosts, "-jar",
                                    JAR),
                         faultInjection ? Map.of("SEQERON_FAULT_INJECTION", "1") : Map.of());
        return members[m];
    }

    /** Waits for the consensus module of every started member to be active. */
    void awaitActive() {
        await("every started member active", Duration.ofSeconds(60), () -> {
            for (int m = 0; m < MEMBERS; m++) {
                if (members[m] != null && counter(m, ConsensusModule.Configuration.CONSENSUS_MODULE_STATE_TYPE_ID) !=
                                              ConsensusModule.State.ACTIVE.code()) {
                    return false;
                }
            }
            return true;
        });
    }

    /** SIGTERMs member {@code m}, as an operator would, and returns whether it exited within the timeout. */
    boolean stopMember(final int m, final Duration timeout) {
        killed[m] = true;
        return members[m].terminate(timeout);
    }

    /**
     * Starts a gateway host: a {@code ReplayerServer} with no member, its relay copying group 1's queue off the given
     * members' archives, tried in order.
     */
    Child startGatewayHost(final String logName, final int id, final int... archiveMembers) {
        final StringBuilder endpoints = new StringBuilder();
        for (final int m : archiveMembers) {
            endpoints.append(endpoints.length() == 0 ? "" : ",").append(archiveEndpoint(m));
        }
        return launch(logName, List.of("-Dreplayer.memberId=" + id, "-Dreplayer.baseDir=" + baseDir,
                                       "-Dreplayer.aeronDir=" + aeronDir(id), "-Dreplayer.archiveEndpoints=" + endpoints,
                                       "-Dreplayer.groups=1", "-cp", JAR, ReplayerServer.class.getName()));
    }

    /** A member's archive as the relay names it. */
    static String archiveEndpoint(final int m) {
        return "localhost:" + PortLayout.archivePort(m);
    }

    /** Loads a topology document through clusterctl on member 0, and waits for every member's Replayer. */
    void loadTopology(final Path document) {
        final Child ctl = launch("load-topology", List.of("-Dclusterctl.memberId=0", "-Dclusterctl.baseDir=" + baseDir,
                                                          "-Dclusterctl.aeronDir=" + aeronDir(0), "-cp", JAR,
                                                          ClusterCtl.class.getName(), "load-topology",
                                                          document.toString()));
        final int status = ctl.awaitExit(Duration.ofSeconds(30));
        if (status != 0) {
            throw new AssertionError("load-topology exited " + status + "; see " + ctl.log());
        }
        awaitReplayers();
    }

    /** Waits for every started member's Replayer to be serving. */
    void awaitReplayers() {
        await("every Replayer ready", Duration.ofSeconds(30), () -> {
            for (int m = 0; m < MEMBERS; m++) {
                if (members[m] != null && counter(m, SeqeronCounters.REPLAYER_READY_TYPE_ID) != 1) {
                    return false;
                }
            }
            return true;
        });
    }

    /**
     * Starts a {@link ClusterProbe} co-located with member {@code m}.
     *
     * @param name       the process's log name
     * @param m          the member whose driver it attaches to
     * @param mode       the probe's mode
     * @param properties {@code key=value} system properties beyond its member and Aeron directory
     * @return the running probe
     */
    Child probe(final String name, final int m, final String mode, final String... properties) {
        final List<String> args = new ArrayList<>(List.of("-Dprobe.memberId=" + m, "-Dprobe.aeronDir=" + aeronDir(m)));
        for (final String property : properties) {
            args.add("-D" + property);
        }
        args.addAll(List.of("-cp", JAR, ClusterProbe.class.getName(), mode));
        return launch(name, args);
    }

    /**
     * Starts a harness class from the test source set (a {@code TestApplication} or {@code TestGateway}) beside member
     * {@code m}, on the uber jar's classpath.
     *
     * @param name       the process's log name
     * @param m          the member whose driver it attaches to
     * @param mainClass  the fully qualified class name
     * @param argument   its mode argument, or null
     * @param properties {@code key=value} system properties beyond its member and Aeron directory
     * @return the running process
     */
    Child harness(final String name, final int m, final String mainClass, final String argument,
                  final String... properties) {
        final List<String> args = new ArrayList<>(List.of("-Dprobe.memberId=" + m, "-Dprobe.aeronDir=" + aeronDir(m)));
        for (final String property : properties) {
            args.add("-D" + property);
        }
        args.addAll(List.of("-cp", JAR + File.pathSeparator + System.getProperty("seqeron.testClasses"),
                            mainClass));
        if (argument != null) {
            args.add(argument);
        }
        return launch(name, args);
    }

    /** Runs clusterctl against member {@code m} to completion and returns its process, for its output and status. */
    Child clusterctl(final String name, final int m, final String... arguments) {
        final List<String> args = new ArrayList<>(List.of("-Dclusterctl.memberId=" + m,
                                                          "-Dclusterctl.baseDir=" + baseDir,
                                                          "-Dclusterctl.aeronDir=" + aeronDir(m), "-cp", JAR,
                                                          ClusterCtl.class.getName()));
        args.addAll(List.of(arguments));
        return launch(name, args);
    }

    /** Dumps member {@code m}'s archive through {@code SbeLogPrinter}, to the process's log. */
    Child logPrinter(final String name, final int m) {
        return launch(name, List.of("-cp", JAR, SbeLogPrinter.class.getName(),
                                    baseDir.resolve("archive-" + m).toString()));
    }

    /** Copies a topology document without its {@code <snapshots>} element. */
    static void withoutSnapshots(final Path from, final Path to) throws IOException {
        final List<String> kept = new ArrayList<>();
        for (final String line : Files.readAllLines(from)) {
            if (!line.contains("<snapshots")) {
                kept.add(line);
            }
        }
        Files.write(to, kept);
    }

    /** SIGKILLs member {@code m}, as a crash would, and waits for its process to exit. */
    void kill(final int m) {
        killed[m] = true;
        members[m].kill();
    }

    /** Declares that member {@code m} is about to exit by itself, so waits do not fail on it. */
    void expectExit(final int m) {
        killed[m] = true;
    }

    /** The directory the members keep their archives and cluster state under. */
    Path baseDir() {
        return baseDir;
    }

    /**
     * Runs {@code ArchiveTool verify} on a dead member's archive, answering yes to each prompt to truncate a torn last
     * fragment, so the archive opens again.
     */
    void repairArchive(final int m) {
        final Path archive = baseDir.resolve("archive-" + m);
        if (!Files.exists(archive.resolve("archive.catalog"))) {
            return;
        }
        final List<String> command = new ArrayList<>(List.of(JAVA));
        command.addAll(JAVA_OPTS);
        command.addAll(List.of("-cp", JAR, "io.aeron.archive.ArchiveTool", archive.toString(), "verify"));
        try {
            final Process process = new ProcessBuilder(command).redirectErrorStream(true)
                .redirectOutput(ProcessBuilder.Redirect.appendTo(logDir.resolve("archive-verify-" + m + ".log").toFile()))
                .start();
            process.getOutputStream().write("y\n".repeat(64).getBytes(StandardCharsets.UTF_8));
            process.getOutputStream().close();
            if (!process.waitFor(60, TimeUnit.SECONDS)) {
                process.destroyForcibly();
                throw new AssertionError("ArchiveTool verify of member " + m + " did not finish");
            }
        } catch (final IOException ex) {
            throw new UncheckedIOException(ex);
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }

    /** Member {@code m}'s current process, for its output. */
    Child memberLog(final int m) {
        return members[m];
    }

    boolean isRunning(final int m) {
        return members[m] != null && members[m].isAlive();
    }

    /** The one running member whose consensus module reports itself leader, once there is exactly one. */
    int awaitLeader() {
        final int[] leader = {NO_MEMBER};
        await("exactly one leader", Duration.ofSeconds(30), () -> {
            int leaders = 0;
            for (int m = 0; m < MEMBERS; m++) {
                if (isRunning(m) && counter(m, ConsensusModule.Configuration.CLUSTER_NODE_ROLE_TYPE_ID) ==
                                        Cluster.Role.LEADER.code()) {
                    leader[0] = m;
                    leaders++;
                }
            }
            return leaders == 1;
        });
        return leader[0];
    }

    /**
     * Polls {@code condition} until it holds, failing at the deadline or as soon as a member exits unasked.
     *
     * @param what      the condition, for the failure message
     * @param timeout   how long it may take
     * @param condition the condition
     */
    void await(final String what, final Duration timeout, final BooleanSupplier condition) {
        final long deadline = System.nanoTime() + timeout.toNanos();
        while (!condition.getAsBoolean()) {
            for (int m = 0; m < MEMBERS; m++) {
                if (members[m] != null && !killed[m] && !members[m].isAlive()) {
                    throw new AssertionError("waiting for " + what + ", member " + m + " exited " +
                                             members[m].process.exitValue() + "; see " + members[m].log());
                }
            }
            if (System.nanoTime() > deadline) {
                throw new AssertionError("no " + what + " within " + timeout + "; logs in " + logDir);
            }
            sleep(POLL_MS);
        }
    }

    /**
     * Member {@code m}'s first counter of the given type, read from its CnC file without an Aeron client, as
     * {@code MetricsExporter} does.
     *
     * @param m      the member
     * @param typeId the counter's type id
     * @return its value, or {@link #NO_VALUE} before the driver or the counter exists
     */
    long counter(final int m, final int typeId) {
        return counter(m, typeId, "");
    }

    /**
     * As {@link #counter(int, int)}, for the first counter of the type whose label contains {@code label}: the app
     * counters carry their source, member and client there.
     */
    long counter(final int m, final int typeId, final String label) {
        final File cncFile = new File(aeronDir(m), CncFileDescriptor.CNC_FILE);
        if (cncFile.length() < CncFileDescriptor.META_DATA_LENGTH) {
            return NO_VALUE;
        }
        final MappedByteBuffer cnc = IoUtil.mapExistingFile(cncFile, CncFileDescriptor.CNC_FILE);
        try {
            final UnsafeBuffer metaData = CncFileDescriptor.createMetaDataBuffer(cnc);
            if (metaData.getIntVolatile(CncFileDescriptor.cncVersionOffset(0)) == 0) {
                return NO_VALUE;
            }
            final CountersReader counters =
                new CountersReader(CncFileDescriptor.createCountersMetaDataBuffer(cnc, metaData),
                                   CncFileDescriptor.createCountersValuesBuffer(cnc, metaData));
            final long[] value = {NO_VALUE};
            counters.forEach((counterId, counterTypeId, keyBuffer, counterLabel) -> {
                if (counterTypeId == typeId && counterLabel.contains(label) && value[0] == NO_VALUE) {
                    value[0] = counters.getCounterValue(counterId);
                }
            });
            return value[0];
        } finally {
            BufferUtil.free(cnc);
        }
    }

    private String aeronDir(final int m) {
        return baseDir.resolve("aeron-" + m).toString();
    }

    private Child launch(final String name, final List<String> args) {
        return launch(name, args, Map.of());
    }

    private Child launch(final String name, final List<String> args, final Map<String, String> environment) {
        final List<String> command = new ArrayList<>(List.of(JAVA));
        command.addAll(JAVA_OPTS);
        if (JACOCO_AGENT != null) {
            command.add("-javaagent:" + JACOCO_AGENT + "=destfile=" + JACOCO_CHILDREN + "/" + name + ".exec");
        }
        command.addAll(args);
        final Path log = logDir.resolve(name + ".log");
        try {
            Files.createDirectories(logDir);
            final ProcessBuilder builder =
                new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(log.toFile());
            builder.environment().putAll(environment);
            final Process process = builder.start();
            final Child child = new Child(process, log);
            children.add(child);
            return child;
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

    @Override
    public void close() {
        for (final Child child : children) {
            child.stop();
        }
    }

    /** A child JVM and the file its output goes to. */
    static final class Child {
        private final Process process;
        private final Path log;

        Child(final Process process, final Path log) {
            this.process = process;
            this.log = log;
        }

        Path log() {
            return log;
        }

        boolean isAlive() {
            return process.isAlive();
        }

        /** Waits up to the timeout for this process to exit; whether it did. */
        boolean awaitDead(final Duration timeout) {
            try {
                return process.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS);
            } catch (final InterruptedException ex) {
                Thread.currentThread().interrupt();
                return false;
            }
        }

        int exitValue() {
            return process.exitValue();
        }

        /** Waits for a line of this process's output that contains {@code text}. */
        void awaitLine(final String text, final Duration timeout) {
            final long deadline = System.nanoTime() + timeout.toNanos();
            while (lastLine(text) == null) {
                if (!process.isAlive() && lastLine(text) == null) {
                    throw new AssertionError("exited " + process.exitValue() + " before \"" + text + "\"; see " + log);
                }
                if (System.nanoTime() > deadline) {
                    throw new AssertionError("no \"" + text + "\" within " + timeout + "; see " + log);
                }
                sleep(POLL_MS);
            }
        }

        /** The last line of this process's output that contains {@code text}, or null. */
        String lastLine(final String text) {
            try {
                // Replaces what is not UTF-8 rather than throwing, whatever encoding the child wrote.
                final String[] lines = new String(Files.readAllBytes(log), StandardCharsets.UTF_8).split("\n");
                for (int i = lines.length - 1; i >= 0; i--) {
                    if (lines[i].contains(text)) {
                        return lines[i];
                    }
                }
                return null;
            } catch (final IOException ex) {
                throw new UncheckedIOException(ex);
            }
        }

        /** Waits for this process to exit by itself and returns its status. */
        int awaitExit(final Duration timeout) {
            try {
                if (!process.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
                    throw new AssertionError("still running after " + timeout + "; see " + log);
                }
                return process.exitValue();
            } catch (final InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new AssertionError("interrupted", ex);
            }
        }

        /** The output lines that contain {@code text}, in order. */
        List<String> lines(final String text) {
            try {
                final List<String> lines = new ArrayList<>();
                for (final String line : new String(Files.readAllBytes(log), StandardCharsets.UTF_8).split("\n")) {
                    if (line.contains(text)) {
                        lines.add(line);
                    }
                }
                return lines;
            } catch (final IOException ex) {
                throw new UncheckedIOException(ex);
            }
        }

        /** SIGSTOP: the process, with its Aeron client, stops answering until {@link #resume}. */
        void pause() {
            signal("STOP");
        }

        void resume() {
            signal("CONT");
        }

        void signal(final String name) {
            try {
                new ProcessBuilder("kill", "-" + name, "" + process.pid()).inheritIO().start().waitFor();
            } catch (final IOException ex) {
                throw new UncheckedIOException(ex);
            } catch (final InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        }

        /** SIGTERM, and whether the process exited within the timeout; it is left running if not. */
        boolean terminate(final Duration timeout) {
            process.destroy();
            try {
                return process.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS);
            } catch (final InterruptedException ex) {
                Thread.currentThread().interrupt();
                return false;
            }
        }

        /** SIGKILL, and waits for the process to exit. */
        void kill() {
            try {
                process.destroyForcibly().waitFor();
            } catch (final InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        }

        /** SIGTERM, then SIGKILL if it has not exited within {@link #EXIT_TIMEOUT}. */
        void stop() {
            process.destroy();
            try {
                if (!process.waitFor(EXIT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)) {
                    process.destroyForcibly().waitFor();
                }
            } catch (final InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
