package org.limitless.phixeron.replayer;

import io.aeron.Aeron;
import io.aeron.archive.client.AeronArchive;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.NoOpLock;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.agrona.concurrent.YieldingIdleStrategy;
import org.limitless.phixeron.util.Logger;

/**
 * Launches one {@link ReplayerService} co-located with a Sequencer cluster member.
 *
 * <p>The ReplayerService does not run its own media driver: it attaches to the member's Aeron directory
 * (the same one {@code SequencerNode} launched its {@code ClusteredMediaDriver} in) so it can reach
 * that member's local {@code Archive} over {@code aeron:ipc} — exactly like {@code OrderExecClient}
 * co-locates via {@code PHIXERON_ORDER_EXEC_AERON_DIR}. Every node runs one of these; each ReplayerService
 * serves replays from its own local archive regardless of leadership (each member records its own
 * complete copy of the sequenced stream — no cross-node replication). It is off the live path: apps
 * read the co-located {@code SequencerService} tap directly and only ask the ReplayerService to replay
 * history/gaps.
 *
 * <p>System properties:
 * <pre>
 *   replayer.memberId      — which cluster member this ReplayerService co-locates with (0/1/2); default 0
 *   replayer.aeronDir      — that member's Aeron directory; default {tmpdir}/phixeron-seq-aeron-{memberId}
 *   replayer.idleStrategy  — duty-cycle idle strategy: {@code backoff} (default), {@code yielding}, or
 *                            {@code busyspin}
 * </pre>
 *
 * <p>The default is {@code backoff} rather than {@code busyspin} or {@code yielding} because busy-spin
 * (and, under sustained contention, yielding too) only pays off when the ReplayerService thread owns an
 * isolated core. On the tuned target deployment (core-pinned, {@code isolcpus}/{@code nohz_full}) set
 * {@code -Dreplayer.idleStrategy=busyspin}; on an oversubscribed host (e.g. a dev box already running the
 * cluster's own busy-spin/backoff driver threads) busy-spin steals cycles from everything else, so the
 * default backs off instead. (The ReplayerService is off the live delivery path, so this only affects how
 * promptly it services replay requests.)
 *
 * <p>Launch example (co-located with member 0):
 * <pre>
 *   java -Dreplayer.memberId=0 \
 *        --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
 *        --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED \
 *        -cp phixeron-uber.jar \
 *        org.limitless.phixeron.replayer.ReplayerNode
 * </pre>
 */
public final class ReplayerNode {
    private static final String PROP_MEMBER_ID = "replayer.memberId";
    private static final String PROP_AERON_DIR = "replayer.aeronDir";
    private static final String PROP_IDLE_STRATEGY = "replayer.idleStrategy";

    /** Must match SequencerNode's Archive.localControlStreamId(100). */
    private static final int ARCHIVE_CONTROL_STREAM_ID = 100;

    /**
     * Control-response stream for the ReplayerService's own archive control session — distinct from the
     * member's SequencerService client (101) so archive replies never cross-talk, even though they
     * share the member's {@code aeron:ipc} driver.
     */
    private static final int ARCHIVE_CONTROL_RESPONSE_STREAM_ID = 120;

    /**
     * Exit status of a node whose replay duty cycle died on an uncaught exception (see {@code
     * ReplayerService.fatalDutyCycleFailure}), as opposed to the 0 of an orderly shutdown — the signal
     * process supervision needs to tell "restart me" from "I was told to stop". Mirrors SequencerNode's
     * EXIT_TAP_FATAL.
     */
    private static final int EXIT_DUTY_CYCLE_FATAL = 70;

    /**
     * Exit status of a node whose duty-cycle thread was still running when shutdown gave up waiting for
     * it (see {@link #main}). Distinct from {@link #EXIT_DUTY_CYCLE_FATAL} because the cause is
     * different — the thread is wedged, not dead — and from 0 because the archive/Aeron client were
     * deliberately left unclosed, so this is not an orderly stop.
     */
    private static final int EXIT_SHUTDOWN_TIMEOUT = 71;

    /**
     * How long shutdown waits for the duty-cycle thread to finish its current iteration. An iteration
     * is one request poll plus at most one archive control call — the startup self-check reads a single
     * fragment per cycle rather than waiting for one (see {@code ReplayerService.pollSelfCheck}), so
     * nothing here waits on a timeout of its own. Generous against that, and only ever reached if the
     * thread is genuinely stuck. Kept under Agrona's own 10s shutdown-hook budget, which this join plus
     * the archive/Aeron close that follows it have to fit inside.
     */
    private static final long SHUTDOWN_JOIN_TIMEOUT_MS = 5_000;

    public static void main(final String[] args) {
        final int memberId = Integer.getInteger(PROP_MEMBER_ID, 0);
        final String aeronDir = System.getProperty(
            PROP_AERON_DIR, System.getProperty("java.io.tmpdir") + "/phixeron-seq-aeron-" + memberId);

        final Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDir));
        // NoOpLock is safe: every archive control call is made from the single ReplayerService duty-cycle
        // thread below (all archive access lives in ReplayerService.poll()); main() only closes it after that
        // thread has joined.
        final AeronArchive archive =
            AeronArchive.connect(new AeronArchive.Context()
                .aeron(aeron)
                .ownsAeronClient(false)
                .controlRequestChannel("aeron:ipc")
                .controlRequestStreamId(ARCHIVE_CONTROL_STREAM_ID)
                .controlResponseChannel("aeron:ipc")
                .controlResponseStreamId(ARCHIVE_CONTROL_RESPONSE_STREAM_ID)
                .lock(NoOpLock.INSTANCE));

        final IdleStrategy idleStrategy = resolveIdleStrategy();
        final AtomicBoolean running = new AtomicBoolean(true);
        final AtomicBoolean dutyCycleFatal = new AtomicBoolean();
        // Wired the same way SequencerNode wires its own tapFatal: the duty-cycle thread cannot exit the
        // process itself (it may be mid-teardown of the very archive/aeron client main() still needs to
        // close cleanly), so it signals the barrier and main() exits non-zero after the normal shutdown
        // path below has run.
        final ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();
        final ReplayerService replayer = new ReplayerService(aeron, archive, memberId, idleStrategy, () -> {
            dutyCycleFatal.set(true);
            barrier.signalAll();
        });
        final Thread replayerThread = new Thread(() -> replayer.run(running), "replayer-" + memberId);
        replayerThread.start();

        Logger.info(Logger.Component.ReplayerNode, memberId, "Running — Ctrl-C to stop | aeronDir=%s | idle=%s",
                aeronDir, idleStrategy.getClass().getSimpleName());
        // NOT try-with-resources on the barrier. Agrona drives it from a JVM shutdown hook that signals
        // every barrier and then waits (10s) for each to be closed — so closing it is what releases the
        // JVM to finish exiting. Closing it first, as `try (barrier) { await(); } finally { …teardown }`
        // does, releases that hook before any teardown runs and leaves the rest racing the JVM's exit:
        // under a real SIGTERM the process died before it could log completion. SequencerNode gets this
        // right by listing the barrier first among its resources, so it closes last; here the teardown is
        // conditional, so it is spelled out and the barrier is closed explicitly at the end.
        barrier.await();
        running.set(false);
        try {
            replayerThread.join(SHUTDOWN_JOIN_TIMEOUT_MS);
        } catch (final InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
        // Only close what nothing is still using. The duty-cycle thread makes archive control calls and
        // polls Aeron subscriptions, so closing these under a thread that has not finished is a
        // use-after-close in someone else's stack. Leaking them for the moments before the process exits
        // costs nothing by comparison.
        final boolean stopped = !replayerThread.isAlive();
        if (stopped) {
            archive.close();
            aeron.close();
            Logger.info(Logger.Component.ReplayerNode, memberId, "Shutdown complete");
        } else {
            Logger.error(Logger.Component.ReplayerNode, Logger.EventCode.ShutdownTimeout, memberId,
                    "duty-cycle thread still running %dms after being told to stop — exiting without "
                            + "closing the archive/Aeron client rather than closing them under it",
                    SHUTDOWN_JOIN_TIMEOUT_MS);
        }
        barrier.close();

        if (dutyCycleFatal.get()) {
            System.exit(EXIT_DUTY_CYCLE_FATAL);
        }
        if (!stopped) {
            // The duty-cycle thread is not a daemon, so a wedged one would otherwise hold the JVM up
            // forever after main() returns.
            System.exit(EXIT_SHUTDOWN_TIMEOUT);
        }
    }

    /**
     * Resolves the duty-cycle idle strategy from replayer.idleStrategy (case-insensitive); see the
     * class Javadoc for why the default is backoff rather than busy-spin.
     * @return idle strategy
     */
    private static IdleStrategy resolveIdleStrategy() {
        final String name = System.getProperty(PROP_IDLE_STRATEGY, "backoff");
        return switch (name.toLowerCase(Locale.ROOT)) {
            case "busyspin" -> new BusySpinIdleStrategy();
            case "yielding" -> new YieldingIdleStrategy();
            case "backoff" -> new BackoffIdleStrategy();
            default -> throw new IllegalArgumentException(
                "Unknown " + PROP_IDLE_STRATEGY + "=" + name + " (expected 'backoff', 'yielding', or 'busyspin')");
        };
    }
}
