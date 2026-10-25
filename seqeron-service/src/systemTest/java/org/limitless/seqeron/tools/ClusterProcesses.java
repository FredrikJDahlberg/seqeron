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
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.agrona.BufferUtil;
import org.agrona.IoUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;
import org.limitless.seqeron.protocol.SeqeronCounters;

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
    private static final String JAVA = Path.of(System.getProperty("java.home"), "bin", "java").toString();
    // seqeron-home.sh's SEQERON_JAVA_OPTS.
    private static final List<String> JAVA_OPTS = List.of(
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED", "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED", "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED",
        "--add-opens=java.base/java.util.zip=ALL-UNNAMED");
    private static final long POLL_MS = 50;
    private static final Duration EXIT_TIMEOUT = Duration.ofSeconds(10);

    private final Path baseDir;
    private final Path logDir;
    private final Child[] members = new Child[MEMBERS];
    private final boolean[] killed = new boolean[MEMBERS];
    private final List<Child> children = new ArrayList<>();

    ClusterProcesses(final Path baseDir, final Path logDir) {
        this.baseDir = baseDir;
        this.logDir = logDir;
    }

    /** Starts every member and waits for each consensus module to be active under exactly one leader. */
    void startAll() {
        final String hosts = String.join(",", "localhost", "localhost", "localhost");
        for (int m = 0; m < MEMBERS; m++) {
            members[m] = launch("seq-" + m,
                                List.of("-Xlog:gc", "-Dsequencer.memberId=" + m, "-Dsequencer.baseDir=" + baseDir,
                                        "-Dsequencer.aeronDir=" + aeronDir(m), "-Dsequencer.hosts=" + hosts, "-jar",
                                        JAR));
        }
        await("every member active", Duration.ofSeconds(60), () -> {
            for (int m = 0; m < MEMBERS; m++) {
                if (counter(m, ConsensusModule.Configuration.CONSENSUS_MODULE_STATE_TYPE_ID) !=
                    ConsensusModule.State.ACTIVE.code()) {
                    return false;
                }
            }
            return true;
        });
        awaitLeader();
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
        await("every Replayer ready", Duration.ofSeconds(30), () -> {
            for (int m = 0; m < MEMBERS; m++) {
                if (counter(m, SeqeronCounters.REPLAYER_READY_TYPE_ID) != 1) {
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

    /** SIGKILLs member {@code m}, as a crash would, and waits for its process to exit. */
    void kill(final int m) {
        killed[m] = true;
        members[m].kill();
    }

    boolean isRunning(final int m) {
        return members[m].isAlive();
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
                if (!killed[m] && !members[m].isAlive()) {
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
            counters.forEach((counterId, counterTypeId, keyBuffer, label) -> {
                if (counterTypeId == typeId && value[0] == NO_VALUE) {
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
        final List<String> command = new ArrayList<>(List.of(JAVA));
        command.addAll(JAVA_OPTS);
        command.addAll(args);
        final Path log = logDir.resolve(name + ".log");
        try {
            Files.createDirectories(logDir);
            final Process process =
                new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(log.toFile()).start();
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

        private Child(final Process process, final Path log) {
            this.process = process;
            this.log = log;
        }

        Path log() {
            return log;
        }

        boolean isAlive() {
            return process.isAlive();
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
