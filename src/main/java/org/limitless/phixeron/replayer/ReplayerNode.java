package org.limitless.phixeron.replayer;

import io.aeron.Aeron;
import io.aeron.archive.client.AeronArchive;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.NoOpLock;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.agrona.concurrent.YieldingIdleStrategy;

/**
 * Launches one {@link Replayer} co-located with a Sequencer cluster member.
 *
 * <p>The Replayer does not run its own media driver: it attaches to the member's Aeron directory
 * (the same one {@code SequencerNode} launched its {@code ClusteredMediaDriver} in) so it can reach
 * that member's local {@code Archive} over {@code aeron:ipc} — exactly like {@code OrderExecClient}
 * co-locates via {@code PHIXERON_ORDER_EXEC_AERON_DIR}. Every node runs one of these; each Replayer
 * serves replays from its own local archive regardless of leadership (each member records its own
 * complete copy of the sequenced stream — no cross-node replication). It is off the live path: apps
 * read the co-located {@code SequencerService} tap directly and only ask the Replayer to replay
 * history/gaps.
 *
 * <p>System properties:
 * <pre>
 *   replayer.memberId      — which cluster member this Replayer co-locates with (0/1/2); default 0
 *   replayer.aeronDir      — that member's Aeron directory; default {tmpdir}/phixeron-seq-aeron-{memberId}
 *   replayer.idleStrategy  — duty-cycle idle strategy: {@code yielding} (default) or {@code busyspin}
 * </pre>
 *
 * <p>The default is {@code yielding} rather than {@code busyspin} (which SequencerNode uses for its
 * media-driver agents) because busy-spin only pays off when the Replayer thread owns an isolated core.
 * On the tuned target deployment (core-pinned, {@code isolcpus}/{@code nohz_full}) set {@code
 * -Dreplayer.idleStrategy=busyspin}; on an oversubscribed host (e.g. a dev box already running the
 * cluster's busy-spin driver threads) busy-spin steals cycles, so the default yields. (The Replayer is
 * off the live delivery path, so this only affects how promptly it services replay requests.)
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
     * Control-response stream for the Replayer's own archive control session — distinct from the
     * member's SequencerService client (101) so archive replies never cross-talk, even though they
     * share the member's {@code aeron:ipc} driver.
     */
    private static final int ARCHIVE_CONTROL_RESPONSE_STREAM_ID = 120;

    public static void main(final String[] args) {
        final int memberId = Integer.getInteger(PROP_MEMBER_ID, 0);
        final String aeronDir = System.getProperty(
            PROP_AERON_DIR, System.getProperty("java.io.tmpdir") + "/phixeron-seq-aeron-" + memberId);

        final Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDir));
        // NoOpLock is safe: every archive control call is made from the single Replayer duty-cycle
        // thread below (all archive access lives in Replayer.poll()); main() only closes it after that
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
        final Replayer replayer = new Replayer(aeron, archive, memberId, idleStrategy);
        final AtomicBoolean running = new AtomicBoolean(true);
        final Thread replayerThread = new Thread(() -> replayer.run(running), "replayer-" + memberId);
        replayerThread.start();

        System.out.printf("[ReplayerNode/%d] Running — Ctrl-C to stop | aeronDir=%s | idle=%s%n", memberId, aeronDir,
                          idleStrategy.getClass().getSimpleName());
        try (ShutdownSignalBarrier barrier = new ShutdownSignalBarrier()) {
            barrier.await();
        } finally {
            running.set(false);
            try {
                replayerThread.join(2000);
            } catch (final InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
            archive.close();
            aeron.close();
            System.out.printf("[ReplayerNode/%d] Shutdown complete%n", memberId);
        }
    }

    // Resolves the duty-cycle idle strategy from replayer.idleStrategy (case-insensitive); see the
    // class Javadoc for why the default is yielding rather than busy-spin.
    private static IdleStrategy resolveIdleStrategy() {
        final String name = System.getProperty(PROP_IDLE_STRATEGY, "yielding");
        return switch (name.toLowerCase(Locale.ROOT)) {
            case "busyspin" -> new BusySpinIdleStrategy();
            case "yielding" -> new YieldingIdleStrategy();
            default -> throw new IllegalArgumentException(
                "Unknown " + PROP_IDLE_STRATEGY + "=" + name + " (expected 'yielding' or 'busyspin')");
        };
    }
}
