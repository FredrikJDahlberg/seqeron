package org.limitless.seqeron.replayer.server;

import io.aeron.Aeron;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.archive.client.AeronArchive;
import io.aeron.driver.MediaDriver;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.agrona.CloseHelper;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.NoOpLock;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.limitless.seqeron.protocol.FrameLayer;
import org.limitless.seqeron.protocol.PortLayout;
import org.limitless.seqeron.util.IdleStrategies;
import org.limitless.seqeron.util.Logger;

/**
 * Runs one {@link ReplayerService}, in one of two places.
 *
 * <p><b>On a cluster member</b> it runs inside {@code SequencerServer}, which starts it with {@link #launch}: it
 * attaches to the member's embedded media driver to reach that member's archive over {@code aeron:ipc}, and
 * serves replays from it regardless of leadership. Its topology rows come from the member's sequencer, which
 * hands each over once as it publishes it; they are not handed over again, so this lives exactly as long as
 * that sequencer.
 *
 * <p><b>On a gateway host</b>, one that runs no member, {@link #main} runs it on its own media driver and
 * archive, and an {@link AeronQueueRelay} per group it relays copies a member's queue of that group onto a local
 * one over UDP, moving to the next member when that one is lost. Clients on the host use the local queues and
 * this Replayer exactly as they would on a member; they submit over UDP ingress.
 *
 * <p>System properties of {@link #main}:
 * <pre>
 *   replayer.archiveEndpoints  — the members' archive control endpoints, host:port, comma-separated,
 *                                tried in order; default each member's in SEQERON_HOSTS, and one of the
 *                                two is required
 *   replayer.groups            — the groupIds whose queues this host relays, comma-separated; required
 *   replayer.memberId          — this host's node id, one no member uses, which names its directories and labels
 *                                its counters; default 0
 *   replayer.aeronDir          — the Aeron directory; default PortLayout.aeronDir(memberId)
 *   replayer.idleStrategy      — duty-cycle idle strategy: {@code backoff} (default), {@code yielding}, or
 *                                {@code busyspin}; busy-spin pays only on an isolated core
 *   replayer.snapshotJoinMs    — how long after a cut an application or gateway has to join its round; default 10s
 *   replayer.host              — this host's name as the members reach it; default localhost
 *   replayer.baseDir           — data directory root; default {tmpdir}/seqeron-seq
 *   aeron.threading.mode       — the driver's threading mode; default DEDICATED (doc/ops.md, "Driver threads")
 *   aeron.ipc.term.buffer.length — the driver's IPC term length; default 16m (doc/ops.md, "Term lengths")
 *   aeron.timer.interval, aeron.untethered.window.limit.timeout, aeron.untethered.linger.timeout — default
 *                                10ms, 100ms, 100ms (doc/ops.md, "Untethered subscribers")
 * </pre>
 *
 * <p>Launch example (gateway host 3, members on m0/m1/m2):
 * <pre>
 *   SEQERON_HOSTS=m0,m1,m2 \
 *   java -Dreplayer.memberId=3 -Dreplayer.host=gw0 -Dreplayer.groups=1 \
 *        --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
 *        --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED \
 *        -cp seqeron-uber.jar \
 *        org.limitless.seqeron.replayer.server.ReplayerServer
 * </pre>
 */
public final class ReplayerServer {
    private static final String PROP_MEMBER_ID = "replayer.memberId";
    private static final String PROP_AERON_DIR = "replayer.aeronDir";
    private static final String PROP_IDLE_STRATEGY = "replayer.idleStrategy";
    private static final String PROP_ARCHIVE_ENDPOINTS = "replayer.archiveEndpoints";
    private static final String PROP_HOST = "replayer.host";
    private static final String PROP_BASE_DIR = "replayer.baseDir";
    private static final String PROP_GROUPS = "replayer.groups";
    private static final String PROP_SNAPSHOT_JOIN_MS = "replayer.snapshotJoinMs";

    /** Above a member's, since a host's purge cannot be undone (doc/snapshot.md §10.6). */
    static final long DEFAULT_SNAPSHOT_JOIN_MS = 10_000;

    /**
     * How long shutdown waits for the duty cycle's current iteration, which never blocks on a timeout of
     * its own; within Agrona's 10s shutdown-hook budget.
     */
    private static final long SHUTDOWN_JOIN_TIMEOUT_MS = 5_000;

    private final int memberId;
    private final ArchivingMediaDriver driver;
    private final Aeron aeron;
    private final AeronArchive archive;
    private final List<AeronQueueRelay> relays;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final Thread replayerThread;
    private final Thread relayThread;

    /**
     * A gateway host's settings, which {@link #fromProperties()} reads from the system properties above.
     *
     * @param nodeId           this host's node id, one no member uses, which names its directories and labels its
     *                         counters
     * @param aeronDir         the host's Aeron directory
     * @param baseDir          the root of the host's archive directory
     * @param archiveEndpoints the members' archive control endpoints, {@code host:port}, tried in order
     * @param host             this host's name as the members reach it
     * @param groups           the groupIds whose queues the host relays, ascending
     * @param idleStrategies   the idle strategy of every agent, the relay's and the Replayer's included
     */
    public record GatewayHostConfig(int nodeId, String aeronDir, String baseDir, List<String> archiveEndpoints,
                                    String host, List<Integer> groups, Supplier<IdleStrategy> idleStrategies) {
        /**
         * Reads the settings from the system properties, with their documented defaults.
         *
         * @return the settings
         * @throws IllegalArgumentException if neither {@code replayer.archiveEndpoints} nor {@code SEQERON_HOSTS}
         *                                  names a member archive, or {@code replayer.groups} names no valid group
         */
        public static GatewayHostConfig fromProperties() {
            final int nodeId = Integer.getInteger(PROP_MEMBER_ID, 0);
            final String archiveEndpoints =
                System.getProperty(PROP_ARCHIVE_ENDPOINTS, ReplayerServer.archiveEndpoints(PortLayout.HOSTS));
            if (archiveEndpoints == null) {
                throw new IllegalArgumentException(
                    PROP_ARCHIVE_ENDPOINTS + " or " + PortLayout.ENV_HOSTS +
                    " is required: on a cluster member the Replayer runs inside SequencerServer");
            }
            return new GatewayHostConfig(
                nodeId, System.getProperty(PROP_AERON_DIR, PortLayout.aeronDir(nodeId)),
                System.getProperty(PROP_BASE_DIR, System.getProperty("java.io.tmpdir") + "/seqeron-seq"),
                parseEndpoints(archiveEndpoints), System.getProperty(PROP_HOST, PortLayout.DEFAULT_HOST),
                parseGroups(System.getProperty(PROP_GROUPS, "")), IdleStrategies.fromProperty(PROP_IDLE_STRATEGY));
        }
    }

    /** Gateway host only: on a cluster member the Replayer runs inside {@code SequencerServer}. */
    public static void main(final String[] args) {
        final GatewayHostConfig config = GatewayHostConfig.fromProperties();
        final AtomicBoolean dutyCycleFatal = new AtomicBoolean();
        final ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();
        final ReplayerServer server = launchGatewayHost(config, () -> {
            dutyCycleFatal.set(true);
            barrier.signalAll();
        });

        barrier.await();
        final boolean stopped = server.stop();
        if (stopped) {
            Logger.info(Logger.CoreComponent.ReplayerServer, config.nodeId(), "Shutdown complete");
        }
        barrier.close();

        if (dutyCycleFatal.get()) {
            System.exit(NodeDriver.EXIT_FATAL);
        }
        if (!stopped) {
            System.exit(NodeDriver.EXIT_SHUTDOWN_TIMEOUT);
        }
    }

    /**
     * Starts a gateway host in this process: its own media driver and archive, the relays that copy a member's
     * queue of each relayed group onto the host's, and the Replayer; {@link #main} is this over
     * {@link GatewayHostConfig#fromProperties()}.
     *
     * @param config       the host's settings
     * @param fatalHandler run once if a duty cycle dies; the host should then stop
     * @return the running host, which the caller stops with {@link #stop()}, the driver and archive included
     */
    public static ReplayerServer launchGatewayHost(final GatewayHostConfig config, final Runnable fatalHandler) {
        return launchGatewayHost(config, fatalHandler, null);
    }

    /**
     * As {@link #launchGatewayHost(GatewayHostConfig, Runnable)}, with the relays reporting the snapshot markers they
     * publish to {@code cutReports}, which the host's Replayer consumes; a test passes one to see them.
     *
     * @param cutReports where the relays report each marker, or {@code null} for the host's own
     */
    public static ReplayerServer launchGatewayHost(final GatewayHostConfig config, final Runnable fatalHandler,
                                                   final Queue<RelayedCut> cutReports) {
        final ArchivingMediaDriver driver = launchDriver(config);
        try {
            final Queue<RelayedCut> reports = cutReports == null ? new ConcurrentLinkedQueue<>() : cutReports;
            return new ReplayerServer(
                config.nodeId(), driver, config.aeronDir(), config.archiveEndpoints(), config.host(), config.groups(),
                new ConcurrentLinkedQueue<>(), reports, config.idleStrategies(), fatalHandler,
                CutIntake.ofHost(reports, Long.getLong(PROP_SNAPSHOT_JOIN_MS, DEFAULT_SNAPSHOT_JOIN_MS)));
        } catch (final RuntimeException ex) {
            CloseHelper.quietClose(driver);
            throw ex;
        }
    }

    /**
     * Starts the Replayer of a cluster member, attached to that member's media driver.
     *
     * @param memberId       the member's id, which names its replay counters.
     * @param aeronDir       the member's Aeron directory.
     * @param idleStrategies the idle strategy of the duty-cycle thread.
     * @param topologyRows   the topology rows the member's sequencer hands over (see {@link QueueResolver}).
     * @param fatalHandler   run once if the duty cycle dies; the process should then exit.
     * @return the running Replayer, which the caller stops with {@link #stop()}.
     */
    public static ReplayerServer launch(final int memberId, final String aeronDir,
                                        final Supplier<IdleStrategy> idleStrategies, final Queue<byte[]> topologyRows,
                                        final Runnable fatalHandler) {
        return launch(memberId, aeronDir, idleStrategies, topologyRows, fatalHandler, null);
    }

    /**
     * As above, tracking the cluster snapshots the member takes (doc/snapshot.md §10.8).
     *
     * @param cutIntake the cuts the member hands over, its {@code restart-safe.log} and the join window; null to
     *                  track none
     */
    public static ReplayerServer launch(final int memberId, final String aeronDir,
                                        final Supplier<IdleStrategy> idleStrategies, final Queue<byte[]> topologyRows,
                                        final Runnable fatalHandler, final CutIntake cutIntake) {
        return new ReplayerServer(memberId, null, aeronDir, null, PortLayout.DEFAULT_HOST, List.of(), topologyRows,
                                  null, idleStrategies, fatalHandler, cutIntake);
    }

    private ReplayerServer(final int memberId, final ArchivingMediaDriver driver, final String aeronDir,
                           final List<String> archiveEndpoints, final String host, final List<Integer> groups,
                           final Queue<byte[]> topologyRows, final Queue<RelayedCut> cutReports,
                           final Supplier<IdleStrategy> idleStrategies, final Runnable fatalHandler,
                           final CutIntake cutIntake) {
        this.memberId = memberId;
        this.driver = driver;
        aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDir));
        archive = AeronArchive.connect(new AeronArchive.Context()
            .aeron(aeron)
            .ownsAeronClient(false)
            .controlRequestChannel(PortLayout.ARCHIVE_CONTROL_CHANNEL)
            .controlRequestStreamId(PortLayout.ARCHIVE_CONTROL_STREAM_ID)
            .controlResponseChannel(PortLayout.ARCHIVE_CONTROL_CHANNEL)
            .controlResponseStreamId(NodeDriver.REPLAYER_ARCHIVE_RESPONSE_STREAM_ID)
            .lock(NoOpLock.INSTANCE));

        final IdleStrategy idleStrategy = idleStrategies.get();
        relays = new ArrayList<>();
        int relayedGroups = QueueResolver.EVERY_GROUP;
        if (archiveEndpoints != null) {
            relayedGroups = 0;
            for (final int groupId : groups) {
                // The lowest relayed queue is the host's default queue, and it alone feeds the resolver.
                relays.add(new AeronQueueRelay(aeron, archive, archiveEndpoints, host, memberId, groupId,
                                               relays.isEmpty() ? topologyRows : null, cutReports,
                                               idleStrategies.get(), fatalHandler));
                relayedGroups |= 1 << (groupId - 1);
            }
        }
        final IdleStrategy relayIdleStrategy = relays.isEmpty() ? null : idleStrategies.get();
        relayThread = relays.isEmpty()
            ? null
            : new Thread(() -> AeronQueueRelay.run(relays, running, relayIdleStrategy), "relay-" + memberId);
        final ReplayerService replayer =
            new ReplayerService(aeron, archive, memberId, idleStrategy, topologyRows, relayedGroups, fatalHandler,
                                cutIntake);
        replayerThread = new Thread(() -> replayer.run(running), "replayer-" + memberId);
        if (relayThread != null) {
            relayThread.start();
        }
        replayerThread.start();

        Logger.info(Logger.CoreComponent.ReplayerServer, memberId, "Running | aeronDir=%s | idle=%s%s", aeronDir,
                    idleStrategy.getClass().getSimpleName(),
                    relays.isEmpty() ? ""
                                     : " | gateway host, groups " + groups + " from member archives " +
                            String.join(",", archiveEndpoints));
    }

    /**
     * Stops the duty cycle, then closes the archive session and Aeron client, and on a gateway host its driver and
     * archive. A duty-cycle thread still running after {@link #SHUTDOWN_JOIN_TIMEOUT_MS} leaves them all open rather
     * than closing them under it.
     *
     * @return whether the duty cycle stopped and everything was closed.
     */
    public boolean stop() {
        running.set(false);
        final boolean stopped = join(replayerThread) & (relayThread == null || join(relayThread));
        if (stopped) {
            CloseHelper.quietCloseAll(relays);
            archive.close();
            aeron.close();
            CloseHelper.quietClose(driver);
        } else {
            Logger.error(Logger.CoreComponent.ReplayerServer, Logger.CoreEventCode.ShutdownTimeout, memberId,
                         "duty-cycle thread still running %dms after being told to stop — exiting without "
                             + "closing the archive/Aeron client rather than closing them under it",
                         SHUTDOWN_JOIN_TIMEOUT_MS);
        }
        return stopped;
    }

    /**
     * A gateway host's own media driver and archive. The archive takes local clients only: this host's
     * Replayer and relay, over {@code aeron:ipc}.
     */
    private static ArchivingMediaDriver launchDriver(final GatewayHostConfig config) {
        final int nodeId = config.nodeId();
        final String aeronDir = config.aeronDir();
        final String baseDir = config.baseDir();
        final String host = config.host();
        final Supplier<IdleStrategy> idleStrategies = config.idleStrategies();
        final MediaDriver.Context driverCtx = NodeDriver.context(aeronDir, idleStrategies);
        NodeDriver.warnUnlessTmpfs(Logger.CoreComponent.ReplayerServer, nodeId, aeronDir);
        final Archive.Context archiveCtx = new Archive.Context()
                                               .aeronDirectoryName(aeronDir)
                                               .archiveDir(new File(baseDir + "/archive-" + nodeId))
                                               .controlChannelEnabled(false)
                                               .localControlChannel(PortLayout.ARCHIVE_CONTROL_CHANNEL)
                                               .localControlStreamId(PortLayout.ARCHIVE_CONTROL_STREAM_ID)
                                               .replicationChannel("aeron:udp?endpoint=" + host + ":0")
                                               .archiveClientContext(new AeronArchive.Context().controlResponseChannel(
                                                   "aeron:udp?endpoint=" + host + ":0"))
                                               .recordingEventsEnabled(false)
                                               .deleteArchiveOnStart(false)
                                               .idleStrategySupplier(idleStrategies);
        NodeDriver.recordChecksums(archiveCtx, Logger.CoreComponent.ReplayerServer, nodeId);
        return ArchivingMediaDriver.launch(driverCtx, archiveCtx);
    }

    /** Each member's archive control endpoint, {@code host:port,…} in member-id order, or {@code null} for none. */
    static String archiveEndpoints(final List<String> hosts) {
        if (hosts.isEmpty()) {
            return null;
        }
        final StringBuilder endpoints = new StringBuilder();
        for (int id = 0; id < hosts.size(); id++) {
            if (id > 0) {
                endpoints.append(',');
            }
            endpoints.append(hosts.get(id)).append(':').append(PortLayout.archivePort(id));
        }
        return endpoints.toString();
    }

    /** {@code g,g,…}: distinct groupIds 1–16, returned ascending. */
    static List<Integer> parseGroups(final String csv) {
        final List<Integer> groups = new ArrayList<>();
        for (final String field : csv.split(",")) {
            if (field.isBlank()) {
                continue;
            }
            final int groupId;
            try {
                groupId = Integer.parseInt(field.trim());
            } catch (final NumberFormatException ex) {
                throw new IllegalArgumentException(PROP_GROUPS + " names '" + field.trim() + "', not a groupId");
            }
            if (groupId < 1 || groupId > FrameLayer.MAX_GROUPS || groups.contains(groupId)) {
                throw new IllegalArgumentException(PROP_GROUPS + " names group " + groupId + " twice or outside 1-" +
                                                   FrameLayer.MAX_GROUPS);
            }
            groups.add(groupId);
        }
        if (groups.isEmpty()) {
            throw new IllegalArgumentException(PROP_GROUPS + " is required on a gateway host: no group is special");
        }
        groups.sort(null);
        return List.copyOf(groups);
    }

    /** {@code host:port,host:port,…}, blanks ignored. */
    private static List<String> parseEndpoints(final String csv) {
        final List<String> endpoints =
            Arrays.stream(csv.split(",")).map(String::trim).filter(endpoint -> !endpoint.isEmpty()).toList();
        if (endpoints.isEmpty()) {
            throw new IllegalArgumentException(PROP_ARCHIVE_ENDPOINTS + " names no member archive: '" + csv + "'");
        }
        return endpoints;
    }

    /** Whether the thread stopped within {@link #SHUTDOWN_JOIN_TIMEOUT_MS}. */
    private static boolean join(final Thread thread) {
        try {
            thread.join(SHUTDOWN_JOIN_TIMEOUT_MS);
        } catch (final InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
        return !thread.isAlive();
    }
}
