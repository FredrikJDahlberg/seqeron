package org.limitless.seqeron.replayer.server;

import io.aeron.Aeron;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.archive.client.AeronArchive;
import io.aeron.driver.MediaDriver;
import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.agrona.CloseHelper;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.NoOpLock;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.limitless.seqeron.protocol.PortLayout;
import org.limitless.seqeron.util.IdleStrategies;
import org.limitless.seqeron.util.Logger;

/**
 * Runs one {@link ReplayerService}, in one of two places.
 *
 * <p><b>On a cluster member</b> it runs inside {@code SequencerServer}, which starts it with {@link #launch}: it
 * attaches to the member's embedded media driver to reach that member's archive over {@code aeron:ipc}, and
 * serves replays from it regardless of leadership.
 *
 * <p><b>On a gateway host</b>, one that runs no member, {@link #main} runs it on its own media driver and
 * archive, and an {@link AeronTapRelay} copies a member's tap onto a local one over UDP, moving to the next
 * member when that one is lost. Clients on the host use the local tap and this Replayer exactly as they would
 * on a member; they submit over UDP ingress.
 *
 * <p>System properties of {@link #main}:
 * <pre>
 *   replayer.archiveEndpoints  — the members' archive control endpoints, host:port, comma-separated,
 *                                tried in order; default each member's in SEQERON_HOSTS, and one of the
 *                                two is required
 *   replayer.memberId          — this host's node id, one no member uses, which names its directories and labels
 *                                its counters; default 0
 *   replayer.aeronDir          — the Aeron directory; default {tmpdir}/seqeron-seq-aeron-{memberId}
 *   replayer.idleStrategy      — duty-cycle idle strategy: {@code backoff} (default), {@code yielding}, or
 *                                {@code busyspin}; busy-spin pays only on an isolated core
 *   replayer.host              — this host's name as the members reach it; default localhost
 *   replayer.baseDir           — data directory root; default {tmpdir}/seqeron-seq
 *   aeron.ipc.term.buffer.length — the driver's IPC term length; default 16m (doc/ops.md, "Term lengths")
 *   aeron.timer.interval, aeron.untethered.window.limit.timeout, aeron.untethered.linger.timeout — default
 *                                10ms, 100ms, 100ms (doc/ops.md, "Untethered subscribers")
 * </pre>
 *
 * <p>Launch example (gateway host 3, members on m0/m1/m2):
 * <pre>
 *   SEQERON_HOSTS=m0,m1,m2 \
 *   java -Dreplayer.memberId=3 -Dreplayer.host=gw0 \
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

    /**
     * How long shutdown waits for the duty cycle's current iteration, which never blocks on a timeout of
     * its own; within Agrona's 10s shutdown-hook budget.
     */
    private static final long SHUTDOWN_JOIN_TIMEOUT_MS = 5_000;

    private final int memberId;
    private final Aeron aeron;
    private final AeronArchive archive;
    private final AeronTapRelay relay;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final Thread replayerThread;
    private final Thread relayThread;

    /** Gateway host only: on a cluster member the Replayer runs inside {@code SequencerServer}. */
    public static void main(final String[] args) {
        final int memberId = Integer.getInteger(PROP_MEMBER_ID, 0);
        final String aeronDir = System.getProperty(
            PROP_AERON_DIR, System.getProperty("java.io.tmpdir") + "/seqeron-seq-aeron-" + memberId);
        final String archiveEndpoints = System.getProperty(PROP_ARCHIVE_ENDPOINTS, archiveEndpoints(PortLayout.HOSTS));
        if (archiveEndpoints == null) {
            throw new IllegalArgumentException(
                PROP_ARCHIVE_ENDPOINTS + " or " + PortLayout.ENV_HOSTS
                    + " is required: on a cluster member the Replayer runs inside SequencerServer");
        }
        final String host = System.getProperty(PROP_HOST, PortLayout.DEFAULT_HOST);
        final Supplier<IdleStrategy> idleStrategies = IdleStrategies.fromProperty(PROP_IDLE_STRATEGY);

        final ArchivingMediaDriver driver = launchDriver(memberId, aeronDir, host, idleStrategies);
        final AtomicBoolean dutyCycleFatal = new AtomicBoolean();
        final ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();
        final ReplayerServer server =
            new ReplayerServer(memberId, aeronDir, parseEndpoints(archiveEndpoints), host, idleStrategies, () -> {
                dutyCycleFatal.set(true);
                barrier.signalAll();
            });

        barrier.await();
        final boolean stopped = server.stop();
        if (stopped) {
            CloseHelper.quietClose(driver);
            Logger.info(Logger.CoreComponent.ReplayerServer, memberId, "Shutdown complete");
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
     * Starts the Replayer of a cluster member, attached to that member's media driver.
     *
     * @param memberId       the member's id, which names its replay counters.
     * @param aeronDir       the member's Aeron directory.
     * @param idleStrategies the idle strategy of the duty-cycle thread.
     * @param fatalHandler   run once if the duty cycle dies; the process should then exit.
     * @return the running Replayer, which the caller stops with {@link #stop()}.
     */
    public static ReplayerServer launch(final int memberId, final String aeronDir,
                                        final Supplier<IdleStrategy> idleStrategies, final Runnable fatalHandler) {
        return new ReplayerServer(memberId, aeronDir, null, PortLayout.DEFAULT_HOST, idleStrategies, fatalHandler);
    }

    private ReplayerServer(final int memberId, final String aeronDir, final List<String> archiveEndpoints,
                           final String host, final Supplier<IdleStrategy> idleStrategies,
                           final Runnable fatalHandler) {
        this.memberId = memberId;
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
        relay = archiveEndpoints == null
            ? null
            : new AeronTapRelay(aeron, archive, archiveEndpoints, host, memberId, idleStrategies.get(), fatalHandler);
        relayThread = relay == null ? null : new Thread(() -> relay.run(running), "relay-" + memberId);
        final ReplayerService replayer = new ReplayerService(aeron, archive, memberId, idleStrategy, fatalHandler);
        replayerThread = new Thread(() -> replayer.run(running), "replayer-" + memberId);
        if (relayThread != null) {
            relayThread.start();
        }
        replayerThread.start();

        Logger.info(Logger.CoreComponent.ReplayerServer, memberId, "Running | aeronDir=%s | idle=%s%s", aeronDir,
                    idleStrategy.getClass().getSimpleName(),
                    relay == null ? "" : " | gateway host, member archives " + String.join(",", archiveEndpoints));
    }

    /**
     * Stops the duty cycle, then closes the archive session and Aeron client. A duty-cycle thread still running
     * after {@link #SHUTDOWN_JOIN_TIMEOUT_MS} leaves both open rather than closing them under it.
     *
     * @return whether the duty cycle stopped and everything was closed.
     */
    public boolean stop() {
        running.set(false);
        final boolean stopped = join(replayerThread) & (relayThread == null || join(relayThread));
        if (stopped) {
            CloseHelper.quietClose(relay);
            archive.close();
            aeron.close();
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
    private static ArchivingMediaDriver launchDriver(final int nodeId, final String aeronDir, final String host,
                                                     final Supplier<IdleStrategy> idleStrategies) {
        final String baseDir = System.getProperty(PROP_BASE_DIR, System.getProperty("java.io.tmpdir") + "/seqeron-seq");
        final MediaDriver.Context driverCtx = NodeDriver.context(aeronDir, idleStrategies);
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
