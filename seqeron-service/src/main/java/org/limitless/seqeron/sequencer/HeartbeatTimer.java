package org.limitless.seqeron.sequencer;

import java.util.concurrent.TimeUnit;
import org.limitless.seqeron.util.Logger;

/**
 * Re-arms the cluster clock from a cluster callback, under that callback's rule: never return with the work undone,
 * and never throw (a throw skips the message). It spins, bounded by the node terminating once the wait provably will
 * not clear.
 *
 * <p>Free of Aeron types: the transport is behind {@link Actions}. Single-threaded: the cluster-callback thread.
 */
public final class HeartbeatTimer {
    /** Everything {@link HeartbeatTimer} cannot do itself; {@link SequencerService} implements it against Aeron. */
    public interface Actions {
        /** @return whether the consensus module accepted the cluster-clock timer */
        boolean scheduleTimer(long deadline);

        /** The cluster's idle strategy — what a spin does between attempts. */
        void idle();

        /** Monotonic clock. Every deadline here measures elapsed real time, not cluster time. */
        long nanoTime();

        /** Ends the process at once: a fatal inside a spin, which a graceful shutdown would wait on forever. */
        void halt();

        /** The node this service runs on — log attribution only. */
        Integer memberId();

        /** Where sequencing has reached, for the log lines. */
        long clusterSeqNo();
    }

    /** Spins between clock reads, so the clock is not read on every iteration. */
    static final int SPINS_PER_CLOCK_CHECK = 1024;

    /**
     * How long the consensus module may refuse the cluster-clock timer before {@link #scheduleHeartbeat}
     * gives up on this node; a second without one accepted is wedged, not busy.
     */
    static final long HEARTBEAT_SCHEDULE_FATAL_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(1);

    private final Actions actions;

    private boolean fatalSignalled;

    /** @param actions the transport */
    public HeartbeatTimer(final Actions actions) {
        this.actions = actions;
    }

    /**
     * Re-arms the cluster clock, spinning until the consensus module accepts the timer. An unscheduled timer
     * would stop the clock silently until the next term, so a module that refuses it for {@link
     * #HEARTBEAT_SCHEDULE_FATAL_TIMEOUT_NS} terminates the node. (False means back-pressure only: Aeron
     * throws for a closed proxy publication.)
     * @param deadline cluster time the timer should fire at
     */
    public void scheduleHeartbeat(final long deadline) {
        int spins = 0;
        long backPressuredSinceNs = 0;
        while (!actions.scheduleTimer(deadline)) {
            if (++spins >= SPINS_PER_CLOCK_CHECK) {
                spins = 0;
                final long nowNs = actions.nanoTime();
                if (backPressuredSinceNs == 0) {
                    backPressuredSinceNs = nowNs; // first read only anchors the period
                } else if (!fatalSignalled && nowNs - backPressuredSinceNs >= HEARTBEAT_SCHEDULE_FATAL_TIMEOUT_NS) {
                    fatalSignalled = true;
                    Logger.fault(Logger.CoreComponent.SequencerService, Logger.CoreEventCode.ServiceError,
                                 actions.memberId(),
                                 "FATAL: the consensus module did not accept the cluster-clock timer for %ds of "
                                     + "continuous back-pressure at clusterSeqNo=%d — terminating this node; its peers "
                                     + "keep quorum and its restart replays the log",
                                 TimeUnit.NANOSECONDS.toSeconds(HEARTBEAT_SCHEDULE_FATAL_TIMEOUT_NS),
                                 actions.clusterSeqNo());
                    actions.halt(); // a graceful shutdown would wait for this thread, which stays in the spin
                }
            }
            actions.idle();
        }
    }
}
