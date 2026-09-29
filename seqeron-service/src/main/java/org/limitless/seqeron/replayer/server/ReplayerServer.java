package org.limitless.seqeron.replayer.server;

import io.aeron.Aeron;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.archive.client.AeronArchive;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
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
 * Launches one {@link ReplayerService}, in one of two places.
 *
 * <p><b>On a cluster member</b> (the default) it runs no media driver of its own: it attaches to the
 * member's Aeron directory to reach that member's archive over {@code aeron:ipc}, and serves replays from
 * it regardless of leadership.
 *
 * <p><b>On a gateway host</b>, one that runs no member, {@code replayer.archiveEndpoints} names the
 * members' archives. It then runs its own media driver and archive, and an {@link AeronTapRelay} copies a
 * member's tap onto a local one over UDP, moving to the next member when that one is lost. Clients on the
 * host use the local tap and this Replayer exactly as they would on a member; they submit over UDP ingress.
 *
 * <p>System properties:
 * <pre>
 *   replayer.memberId          — on a member, which one (0/1/2); on a gateway host, this host's node id,
 *                                which names its directories and labels its counters; default 0
 *   replayer.aeronDir          — the Aeron directory; default {tmpdir}/seqeron-seq-aeron-{memberId}
 *   replayer.idleStrategy      — duty-cycle idle strategy: {@code backoff} (default), {@code yielding}, or
 *                                {@code busyspin}; busy-spin pays only on an isolated core
 *   replayer.archiveEndpoints  — gateway host only: the members' archive control endpoints, host:port,
 *                                comma-separated, tried in order; setting it selects gateway-host mode
 *   replayer.host              — gateway host only: this host's name as the members reach it; default
 *                                localhost
 *   replayer.baseDir           — gateway host only: data directory root; default {tmpdir}/seqeron-seq
 * </pre>
 *
 * <p>Launch example (co-located with member 0):
 * <pre>
 *   java -Dreplayer.memberId=0 \
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

    /** Control-response stream of this service's archive session, distinct from SequencerService's 121. */
    private static final int ARCHIVE_CONTROL_RESPONSE_STREAM_ID = 120;

    /** Exit status of a node whose replay duty cycle died on an uncaught exception; restart it. */
    private static final int EXIT_DUTY_CYCLE_FATAL = 70;

    /**
     * Exit status when shutdown gave up waiting for a wedged duty-cycle thread, leaving the archive and
     * Aeron client unclosed: not an orderly stop, and not a dead thread either.
     */
    private static final int EXIT_SHUTDOWN_TIMEOUT = 71;

    /**
     * How long shutdown waits for the duty cycle's current iteration, which never blocks on a timeout of
     * its own; within Agrona's 10s shutdown-hook budget.
     */
    private static final long SHUTDOWN_JOIN_TIMEOUT_MS = 5_000;

    public static void main(final String[] args) {
        final int memberId = Integer.getInteger(PROP_MEMBER_ID, 0);
        final String aeronDir = System.getProperty(
            PROP_AERON_DIR, System.getProperty("java.io.tmpdir") + "/seqeron-seq-aeron-" + memberId);
        final String archiveEndpoints = System.getProperty(PROP_ARCHIVE_ENDPOINTS);
        final String host = System.getProperty(PROP_HOST, PortLayout.DEFAULT_HOST);
        final Supplier<IdleStrategy> idleStrategies = IdleStrategies.fromProperty(PROP_IDLE_STRATEGY);

        final ArchivingMediaDriver driver =
            archiveEndpoints == null ? null : launchDriver(memberId, aeronDir, host, idleStrategies);
        final Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDir));
        final AeronArchive archive = AeronArchive.connect(new AeronArchive.Context()
            .aeron(aeron)
            .ownsAeronClient(false)
            .controlRequestChannel(PortLayout.ARCHIVE_CONTROL_CHANNEL)
            .controlRequestStreamId(PortLayout.ARCHIVE_CONTROL_STREAM_ID)
            .controlResponseChannel(PortLayout.ARCHIVE_CONTROL_CHANNEL)
            .controlResponseStreamId(ARCHIVE_CONTROL_RESPONSE_STREAM_ID)
            .lock(NoOpLock.INSTANCE));

        final IdleStrategy idleStrategy = idleStrategies.get();
        final AtomicBoolean running = new AtomicBoolean(true);
        final AtomicBoolean dutyCycleFatal = new AtomicBoolean();
        final ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();
        final Runnable fatalHandler = () -> {
            dutyCycleFatal.set(true);
            barrier.signalAll();
        };

        final AeronTapRelay relay = archiveEndpoints == null
            ? null
            : new AeronTapRelay(aeron, archive, parseEndpoints(archiveEndpoints), host, memberId, idleStrategies.get(),
                                fatalHandler);
        final Thread relayThread = relay == null ? null : new Thread(() -> relay.run(running), "relay-" + memberId);
        final ReplayerService replayer = new ReplayerService(aeron, archive, memberId, idleStrategy, fatalHandler);
        final Thread replayerThread = new Thread(() -> replayer.run(running), "replayer-" + memberId);
        if (relayThread != null) {
            relayThread.start();
        }
        replayerThread.start();

        Logger.info(Logger.CoreComponent.ReplayerServer, memberId, "Running — Ctrl-C to stop | aeronDir=%s | idle=%s%s",
                    aeronDir, idleStrategy.getClass().getSimpleName(),
                    relay == null ? "" : " | gateway host, member archives " + archiveEndpoints);
        barrier.await();
        running.set(false);
        final boolean stopped = join(replayerThread) & (relayThread == null || join(relayThread));
        if (stopped) {
            CloseHelper.quietClose(relay);
            archive.close();
            aeron.close();
            CloseHelper.quietClose(driver);
            Logger.info(Logger.CoreComponent.ReplayerServer, memberId, "Shutdown complete");
        } else {
            Logger.error(Logger.CoreComponent.ReplayerServer, Logger.CoreEventCode.ShutdownTimeout, memberId,
                         "duty-cycle thread still running %dms after being told to stop — exiting without "
                             + "closing the archive/Aeron client rather than closing them under it",
                         SHUTDOWN_JOIN_TIMEOUT_MS);
        }
        barrier.close();

        if (dutyCycleFatal.get()) {
            System.exit(EXIT_DUTY_CYCLE_FATAL);
        }
        if (!stopped) {
            System.exit(EXIT_SHUTDOWN_TIMEOUT);
        }
    }

    /**
     * A gateway host's own media driver and archive. The archive takes local clients only: this host's
     * Replayer and relay, over {@code aeron:ipc}.
     */
    private static ArchivingMediaDriver launchDriver(final int nodeId, final String aeronDir, final String host,
                                                     final Supplier<IdleStrategy> idleStrategies) {
        final String baseDir = System.getProperty(PROP_BASE_DIR, System.getProperty("java.io.tmpdir") + "/seqeron-seq");
        final MediaDriver.Context driverCtx = new MediaDriver.Context()
                                                  .aeronDirectoryName(aeronDir)
                                                  .threadingMode(ThreadingMode.DEDICATED)
                                                  .conductorIdleStrategy(idleStrategies.get())
                                                  .senderIdleStrategy(idleStrategies.get())
                                                  .receiverIdleStrategy(idleStrategies.get())
                                                  .dirDeleteOnStart(true);
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
