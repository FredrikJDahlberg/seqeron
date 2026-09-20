package org.limitless.seqeron.sequencer;

import java.util.concurrent.TimeUnit;
import org.limitless.seqeron.util.Logger;

/**
 * The two things {@link SequencerService} does from a cluster callback that can back-pressure — put a frame
 * on the tap, and re-arm the cluster clock — under that callback's rule: never return with the work undone,
 * and never throw (a throw skips the message). Both spin, bounded by the node terminating once the wait
 * provably will not clear; one class, so a second failure falls through to the shutdown backstop.
 *
 * <p>Free of Aeron types: the transport is behind {@link Actions}. Single-threaded: the cluster-callback thread.
 */
public final class TapPublisher {
    /** Everything {@link TapPublisher} cannot do itself; {@link SequencerService} implements it against Aeron. */
    public interface Actions {
        /**
         * Offers the frame at offset 0 of the sequencer's buffer onto the tap.
         * @return the raw Aeron offer result — negative is a failure, classified by {@link #isUnrecoverable}
         */
        long offerFrame(int length);

        /** Whether an offer result is one no amount of retrying can clear (CLOSED / MAX_POSITION_EXCEEDED). */
        boolean isUnrecoverable(long offerResult);

        /** @return whether the consensus module accepted the cluster-clock timer */
        boolean scheduleTimer(long deadline);

        /** The cluster's idle strategy — what a spin does between attempts. */
        void idle();

        /** Monotonic clock. Every deadline here measures elapsed real time, not cluster time. */
        long nanoTime();

        /** Whether the co-located archive is still recording the tap. */
        boolean recordingActive();

        /** How far the archive has recorded the tap; only meaningful while {@link #recordingActive}. */
        long recordedPosition();

        /** Which recording that is, for the operator's log line. */
        long recordingId();

        /** The {@code seqeron.sequencer.tapBackPressureAlerts} counter. */
        void tapBackPressureAlert();

        /** The {@code seqeron.sequencer.tapStalled} gauge. */
        void tapStalled(boolean stalled);

        /** Brings the node down. Must not block: it signals a shutdown and returns, it does not perform one. */
        void signalFatal();

        /** Last resort, when the graceful shutdown {@link #signalFatal} kicked off has itself wedged. */
        void halt();

        /** The node this service runs on — log attribution only. */
        Integer memberId();

        /** Where sequencing has reached, for the log lines. */
        long globalSeqNo();
    }

    /**
     * How often, in wall time, back-pressure in {@link #emit} is alerted on and the stall re-evaluated.
     * Wall time, not a spin count: under a yielding idle strategy on a loaded host a spin count can stretch
     * to seconds, past the thresholds it samples.
     */
    static final long BACK_PRESSURE_ALERT_INTERVAL_NS = TimeUnit.MILLISECONDS.toNanos(10);

    /** Spins between clock reads while back-pressured, so the clock is not read on every iteration. */
    static final int SPINS_PER_CLOCK_CHECK = 1024;

    /** How long the recording may make no progress under back-pressure before the stall gauge is raised. */
    static final long SUSTAINED_BACKPRESSURE_THRESHOLD_NS = TimeUnit.MILLISECONDS.toNanos(200);

    /**
     * How long the recording may make <em>zero</em> progress under back-pressure before the node
     * terminates. An archive that is slow but still advancing is never timed out. 5x the stall gauge, on
     * the order of the cluster's 1s {@code sessionTimeoutNs}; re-tune against production storage.
     */
    static final long TAP_STALL_FATAL_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(1);

    /**
     * How long the consensus module may refuse the cluster-clock timer before {@link #scheduleHeartbeat}
     * gives up on this node; a second without one accepted is wedged, not busy. Matches
     * {@link #TAP_STALL_FATAL_TIMEOUT_NS}.
     */
    static final long HEARTBEAT_SCHEDULE_FATAL_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(1);

    /**
     * How long a signalled fatal may take before the process is halted outright: the graceful path closes
     * the archive that may be what is wedged. Well above the ~7s a healthy teardown takes, so it never
     * pre-empts one about to succeed.
     */
    static final long FATAL_SHUTDOWN_BACKSTOP_NS = TimeUnit.SECONDS.toNanos(30);

    private final Actions actions;

    /** Monotonic reading at the last observed recording advance; 0 when no stall is in progress. */
    private long stallSinceNs;
    private long stallRecordedPosition;
    private boolean stalled;

    private boolean fatalSignalled;
    private long fatalSignalledNs;

    public TapPublisher(final Actions actions) {
        this.actions = actions;
    }

    /**
     * Publishes the frame in the sequencer's buffer onto the tap. Spins on back-pressure, since a dropped
     * frame is an unrecoverable gap, but terminates the node once the recording is provably not draining.
     * The only exits are a landed offer and process death.
     * @param length of the encoded frame at offset 0 of the sequencer's buffer
     */
    public void emit(final int length) {
        int spins = 0;
        long nextAlertNs = 0;
        long result;
        while ((result = actions.offerFrame(length)) < 0) {
            if (actions.isUnrecoverable(result)) {
                fatalTapFailure("tap publication failed: " + result);
            }
            if (++spins >= SPINS_PER_CLOCK_CHECK) {
                spins = 0;
                final long nowNs = actions.nanoTime();
                if (nextAlertNs == 0) {
                    nextAlertNs = nowNs + BACK_PRESSURE_ALERT_INTERVAL_NS; // first read only anchors the period
                } else if (nowNs - nextAlertNs >= 0) {
                    nextAlertNs = nowNs + BACK_PRESSURE_ALERT_INTERVAL_NS;
                    onBackPressureThreshold(nowNs);
                }
            }
            actions.idle();
        }
        final boolean wasStalled = stalled;
        stallSinceNs = 0;
        stallRecordedPosition = 0;
        stalled = false;
        if (wasStalled && !fatalSignalled) {
            actions.tapStalled(false);
            Logger.info(Logger.CoreComponent.Sequencer, actions.memberId(),
                        "RECOVERED: tap back-pressure cleared at globalSeqNo=%d", actions.globalSeqNo());
        }
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
                } else if (fatalSignalled) {
                    haltIfShutdownStalled(nowNs); // keep the backstop alive: no heartbeat reaches it now
                } else if (nowNs - backPressuredSinceNs >= HEARTBEAT_SCHEDULE_FATAL_TIMEOUT_NS) {
                    fatalFailure(Logger.CoreEventCode.ServiceError,
                                 "the consensus module did not accept the cluster-clock timer for " +
                                     TimeUnit.NANOSECONDS.toSeconds(HEARTBEAT_SCHEDULE_FATAL_TIMEOUT_NS) +
                                     "s of continuous back-pressure");
                }
            }
            actions.idle();
        }
    }

    /**
     * Liveness check on the tap recording, run on the 1 Hz clock: a recording that stops back-pressures
     * nothing (the untethered app subscribers keep the publication connected), so {@link #emit}'s bound
     * would never see it.
     */
    public void checkRecordingAlive() {
        if (fatalSignalled) {
            haltIfShutdownStalled(actions.nanoTime());
        } else if (!actions.recordingActive()) {
            fatalTapFailure("the local archive stopped recording the tap (recording " + actions.recordingId() + ")");
        }
    }

    /** Whether this node has already been given up on — see {@link #fatalFailure}. */
    public boolean isFatalSignalled() {
        return fatalSignalled;
    }

    /**
     * One {@link #BACK_PRESSURE_ALERT_INTERVAL_NS} of continuous back-pressure has elapsed in {@link #emit}:
     * alert, then time how long the recording has made no progress. Kept out of the spin, so an emit whose
     * first offer lands pays none of it.
     * @param nowNs the clock reading that triggered this period, reused rather than read again
     */
    private void onBackPressureThreshold(final long nowNs) {
        if (fatalSignalled) {
            haltIfShutdownStalled(nowNs);
            return;
        }
        Logger.error(Logger.CoreComponent.Sequencer, Logger.CoreEventCode.TapBackpressure, actions.memberId(),
                     "ALERT: tap back-pressure at globalSeqNo=%d", actions.globalSeqNo());
        actions.tapBackPressureAlert();
        if (!actions.recordingActive()) {
            fatalTapFailure("the local archive stopped recording the tap (recording " + actions.recordingId() + ")");
            return;
        }
        final long recordedPosition = actions.recordedPosition();
        if (stallSinceNs == 0 || recordedPosition > stallRecordedPosition) {
            // The first observation only anchors the clock: how long back-pressure lasted before it is unknown.
            stallRecordedPosition = recordedPosition;
            stallSinceNs = nowNs;
            return;
        }
        final long stalledNs = nowNs - stallSinceNs;
        if (stalledNs >= TAP_STALL_FATAL_TIMEOUT_NS) {
            fatalTapFailure("the tap recording made no progress for " +
                            TimeUnit.NANOSECONDS.toMillis(TAP_STALL_FATAL_TIMEOUT_NS) +
                            "ms of continuous back-pressure");
        } else if (stalledNs >= SUSTAINED_BACKPRESSURE_THRESHOLD_NS && !stalled) {
            stalled = true;
            actions.tapStalled(true);
            Logger.error(
                Logger.CoreComponent.Sequencer, Logger.CoreEventCode.TapBackpressure, actions.memberId(),
                "STALLED: tap back-pressure sustained beyond %dms with no recording progress at globalSeqNo=%d",
                TimeUnit.NANOSECONDS.toMillis(SUSTAINED_BACKPRESSURE_THRESHOLD_NS), actions.globalSeqNo());
        }
    }

    /**
     * Terminates the node over its tap recording, latching the stall gauge so it does not clear on the way out.
     * @param reason what failed, for the operator
     */
    private void fatalTapFailure(final String reason) {
        if (!fatalSignalled) {
            actions.tapStalled(true);
        }
        fatalFailure(Logger.CoreEventCode.TapRecordingFailure,
                     reason + ", so it can no longer record the history it is responsible for");
    }

    /**
     * Brings this node down: peers keep quorum, and its restart rebuilds its recording over full-log replay.
     * Latched: later calls only fall through to {@link #haltIfShutdownStalled}.
     * @param code   which failure class this is, for the operator's log
     * @param reason what failed
     */
    private void fatalFailure(final Logger.EventCode code, final String reason) {
        if (fatalSignalled) {
            return;
        }
        fatalSignalled = true;
        fatalSignalledNs = actions.nanoTime();
        Logger.fault(Logger.CoreComponent.SequencerService, code, actions.memberId(),
                     "FATAL: %s at globalSeqNo=%d — terminating this node; its peers keep quorum and its restart "
                         + "replays the full log",
                     reason, actions.globalSeqNo());
        actions.signalFatal();
    }

    /**
     * Halts once the graceful teardown {@link Actions#signalFatal} started has taken {@link
     * #FATAL_SHUTDOWN_BACKSTOP_NS}.
     * @param nowNs monotonic clock reading
     */
    private void haltIfShutdownStalled(final long nowNs) {
        if (nowNs - fatalSignalledNs >= FATAL_SHUTDOWN_BACKSTOP_NS) {
            actions.halt();
        }
    }
}
