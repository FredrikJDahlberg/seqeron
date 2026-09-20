package org.limitless.seqeron.replayer.server;

import io.aeron.Aeron;
import io.aeron.archive.client.AeronArchive;
import java.util.concurrent.atomic.AtomicBoolean;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.NoOpLock;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.limitless.seqeron.protocol.PortLayout;
import org.limitless.seqeron.util.IdleStrategies;
import org.limitless.seqeron.util.Logger;

/**
 * Launches one {@link ReplayerService} co-located with a Sequencer cluster member. It runs no media driver
 * of its own: it attaches to the member's Aeron directory to reach that member's archive over
 * {@code aeron:ipc}, and serves replays from it regardless of leadership.
 *
 * <p>System properties:
 * <pre>
 *   replayer.memberId      — which cluster member this ReplayerService co-locates with (0/1/2); default 0
 *   replayer.aeronDir      — that member's Aeron directory; default {tmpdir}/seqeron-seq-aeron-{memberId}
 *   replayer.idleStrategy  — duty-cycle idle strategy: {@code backoff} (default), {@code yielding}, or
 *                            {@code busyspin}; busy-spin pays only on an isolated core
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

        final Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDir));
        final AeronArchive archive = AeronArchive.connect(new AeronArchive.Context()
            .aeron(aeron)
            .ownsAeronClient(false)
            .controlRequestChannel(PortLayout.ARCHIVE_CONTROL_CHANNEL)
            .controlRequestStreamId(PortLayout.ARCHIVE_CONTROL_STREAM_ID)
            .controlResponseChannel(PortLayout.ARCHIVE_CONTROL_CHANNEL)
            .controlResponseStreamId(ARCHIVE_CONTROL_RESPONSE_STREAM_ID)
            .lock(NoOpLock.INSTANCE));

        final IdleStrategy idleStrategy = IdleStrategies.fromProperty(PROP_IDLE_STRATEGY).get();
        final AtomicBoolean running = new AtomicBoolean(true);
        final AtomicBoolean dutyCycleFatal = new AtomicBoolean();
        final ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();
        final ReplayerService replayer = new ReplayerService(aeron, archive, memberId, idleStrategy, () -> {
            dutyCycleFatal.set(true);
            barrier.signalAll();
        });
        final Thread replayerThread = new Thread(() -> replayer.run(running), "replayer-" + memberId);
        replayerThread.start();

        Logger.info(Logger.CoreComponent.ReplayerServer, memberId, "Running — Ctrl-C to stop | aeronDir=%s | idle=%s",
                    aeronDir, idleStrategy.getClass().getSimpleName());
        barrier.await();
        running.set(false);
        try {
            replayerThread.join(SHUTDOWN_JOIN_TIMEOUT_MS);
        } catch (final InterruptedException ie) {
            Thread.currentThread().interrupt();
        }

        final boolean stopped = !replayerThread.isAlive();
        if (stopped) {
            archive.close();
            aeron.close();
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
}
