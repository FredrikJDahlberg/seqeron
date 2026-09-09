package org.limitless.seqeron.sequencer;

import java.util.concurrent.TimeUnit;
import org.limitless.seqeron.util.Logger;

/**
 * The two things {@link SequencerService} must do from inside a cluster callback that can back-pressure —
 * put a frame on the node-local tap, and re-arm the cluster clock — under the one rule those callbacks
 * impose: <b>never return with the work undone, and never throw.</b> Both spin, and both are bounded by the
 * same escalation: once the thing being waited on is provably not going to clear, the node terminates.
 *
 * <p>Split out of {@link SequencerService} for the reason {@link Sequencer} and {@link TapStallPolicy} are:
 * this is the decision half, free of every Aeron type, with the transport behind {@link Actions} so the
 * unit suite drives it directly. It also owns the node-fatal latch both paths escalate to, which is why
 * they live in one class rather than two — the second failure must fall through to the shutdown backstop
 * rather than start a second shutdown.
 *
 * <p>Why the failure paths signal and keep spinning rather than throw: an exception raised in any cluster
 * callback is caught by {@code Image.boundedControlledPoll}, which has <em>already advanced the log
 * position past the message</em>, and {@code AgentRunner} keeps the agent running — so the service would
 * resume at the next message, around a hole in its own recording, with {@code globalSeqNo} already
 * consumed. That is exactly the unrecoverable gap all of this exists to prevent.
 *
 * <p><b>Single-threaded.</b> Every method must be called from the one cluster-callback thread.
 */
public final class TapPublisher {
    /**
     * Everything {@link TapPublisher} cannot do itself: the offers, the archive's recording state, the two
     * gauges this discipline owns, and the way out. {@link SequencerService} implements it against Aeron;
     * the unit suite substitutes a recorder.
     */
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
     * How often, in wall time, back-pressure in {@link #emit} is alerted on and {@link TapStallPolicy}
     * re-evaluated. This used to be a spin count (1,000,000, documented as "~10 ms at ~10 ns/spin"), which
     * is only true when the loop spins on an idle core: the container idles with a {@code
     * YieldingIdleStrategy}, and on a loaded host a yield costs microseconds, so the same count took over
     * ten seconds — long enough that a node whose archive had died sat there spinning without ever
     * reaching the evaluation that would have terminated it. The stall thresholds below are wall-clock
     * durations, so what samples them has to be too.
     */
    static final long BACK_PRESSURE_ALERT_INTERVAL_NS = TimeUnit.MILLISECONDS.toNanos(10);

    /**
     * Spins between clock reads while back-pressured. {@code System.nanoTime} is cheap but not free, and
     * the idle strategy may be a busy-spin one, so the clock is not read on every iteration.
     */
    static final int SPINS_PER_CLOCK_CHECK = 1024;

    /**
     * How long tap-emit back-pressure must persist, continuously, before {@link #emit} treats it as a
     * genuine local-archive stall.
     */
    static final long SUSTAINED_BACKPRESSURE_THRESHOLD_NS = TimeUnit.MILLISECONDS.toNanos(200);

    /**
     * How long the tap recording may make <em>zero</em> progress, while {@link #emit} is back-pressured,
     * before this node gives up on the local archive and terminates (see {@link #fatalTapFailure}). Not a
     * back-pressure timeout: an archive draining slowly under load back-pressures continuously and keeps
     * advancing, and is left alone however long that lasts — this bounds only an archive that has stopped
     * draining. Kept at 5x the stall gauge, the same margin {@code sessionTimeoutNs} keeps over the
     * keep-alive interval, and lands this node's own fatal judgement in the same order of magnitude as
     * {@code sessionTimeoutNs}'s 1s — the other threshold governing how long this cluster tolerates a
     * dependency going quiet. Re-tune against real production storage before trusting it off loopback.
     */
    static final long TAP_STALL_FATAL_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(1);

    /**
     * How long the consensus module may refuse the cluster-clock timer, continuously, before {@link
     * #scheduleHeartbeat} gives up on this node. The same judgement {@link #TAP_STALL_FATAL_TIMEOUT_NS} makes
     * about the archive, applied to the other end of the service: back-pressure on the consensus-module
     * proxy is ordinary and self-clearing, and a full second of it without a single accepted timer is not
     * a busy module but a wedged one. Matched to that constant deliberately — both bound the same
     * question, "is the thing this node depends on still draining?", and there is no reason for the two
     * answers to differ.
     */
    static final long HEARTBEAT_SCHEDULE_FATAL_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(1);

    /**
     * How long after signalling a fatal tap failure the process may still be alive before it is halted
     * outright. The graceful path has to close the very archive that may be the thing wedged, so it can
     * hang; by this point the node is committed to dying and nothing is lost by skipping the niceties.
     * Well above the ~7s a healthy teardown takes when {@link #emit} is the wedged party — the container's
     * close has to wait out its own retry timeout and then interrupt this thread out of the spin — so the
     * backstop cannot pre-empt a shutdown that was about to succeed. It is a backstop, not a deadline.
     */
    static final long FATAL_SHUTDOWN_BACKSTOP_NS = TimeUnit.SECONDS.toNanos(30);

    private final Actions actions;

    /** When to stop waiting on a back-pressured tap and terminate instead. Pure; see {@link TapStallPolicy}. */
    private final TapStallPolicy stallPolicy =
        new TapStallPolicy(SUSTAINED_BACKPRESSURE_THRESHOLD_NS, TAP_STALL_FATAL_TIMEOUT_NS);

    private boolean fatalSignalled;
    private long fatalSignalledNs;

    public TapPublisher(final Actions actions) {
        this.actions = actions;
    }

    /**
     * Publishes the frame in the sequencer's buffer onto the node-local tap. Spins on back-pressure — the
     * tap recording is the authoritative history, so a dropped frame would be an unrecoverable gap — but
     * not blindly: {@link TapStallPolicy} watches the recording behind the tap, and once it is provably not
     * draining (or gone) the node terminates rather than wait out a failure that will not clear.
     *
     * <p><b>The only two exits are a landed offer and process death.</b> The spin does unwind on the way
     * out — closing the container interrupts this thread and the idle strategy raises {@code
     * AgentTerminationException} — but only once the process is already going down, which is why that stack
     * trace appears in the log after a fatal.
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
        if (stallPolicy.onEmitted() && !fatalSignalled) {
            actions.tapStalled(false);
            Logger.info(Logger.Component.Sequencer, actions.memberId(),
                        "RECOVERED: tap back-pressure cleared at globalSeqNo=%d", actions.globalSeqNo());
        }
    }

    /**
     * Re-arms the cluster clock, spinning until the consensus module accepts the timer — bounded, for the
     * same reason {@link #emit} is. Returning with the timer unscheduled would stop the clock outright:
     * nothing else re-arms it until the next leadership term, so every consumer's session clock would
     * silently stop advancing. Spinning forever is no better — the callback would never return and this
     * node would go dark with none of the failure paths ever running. So a consensus module that has not
     * accepted a timer for {@link #HEARTBEAT_SCHEDULE_FATAL_TIMEOUT_NS} is treated as wedged and this node
     * terminates, as visibly as it does when it cannot record its own tap. (The only false return is
     * back-pressure: Aeron throws for a closed/disconnected proxy publication rather than returning.)
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
                    fatalFailure(Logger.EventCode.ServiceError,
                                 "the consensus module did not accept the cluster-clock timer for " +
                                     TimeUnit.NANOSECONDS.toSeconds(HEARTBEAT_SCHEDULE_FATAL_TIMEOUT_NS) +
                                     "s of continuous back-pressure");
                }
            }
            actions.idle();
        }
    }

    /**
     * Liveness check on the co-located archive's recording of the tap — and the reason {@link #emit}'s
     * back-pressure bound is not enough on its own: <b>a recording that stops does not back-pressure
     * anything.</b> The tap publication still has the app replicas attached (untethered), so offers keep
     * landing and frames keep flowing live while nothing at all is being recorded — this node silently
     * losing the history it is responsible for, discovered only when someone later asks it for a replay.
     * The recording counter going away is the only symptom, so the caller runs this on the cluster's own
     * 1 Hz clock.
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
     * alert, then ask {@link TapStallPolicy} whether this is an archive that is merely busy or one that has
     * stopped draining. All of the per-period work lives here rather than in the spin, so the normal emit —
     * where the first offer lands — pays none of it.
     * @param nowNs the clock reading that triggered this period, reused rather than read again
     */
    private void onBackPressureThreshold(final long nowNs) {
        if (fatalSignalled) {
            haltIfShutdownStalled(nowNs);
            return;
        }
        Logger.error(Logger.Component.Sequencer, Logger.EventCode.ReplayerBackpressure, actions.memberId(),
                     "ALERT: replayer back-pressure at globalSeqNo=%d", actions.globalSeqNo());
        actions.tapBackPressureAlert();
        switch (stallPolicy.onBackPressure(nowNs, actions.recordingActive(), actions.recordedPosition())) {
        case STALLED -> {
            actions.tapStalled(true);
            Logger.error(
                Logger.Component.Sequencer, Logger.EventCode.ReplayerBackpressure, actions.memberId(),
                "STALLED: tap back-pressure sustained beyond %dms with no recording progress at globalSeqNo=%d",
                TimeUnit.NANOSECONDS.toMillis(SUSTAINED_BACKPRESSURE_THRESHOLD_NS), actions.globalSeqNo());
        }
        case FATAL_RECORDING_GONE ->
            fatalTapFailure("the local archive stopped recording the tap (recording " + actions.recordingId() + ")");
        case FATAL_NO_PROGRESS ->
            fatalTapFailure("the tap recording made no progress for " +
                            TimeUnit.NANOSECONDS.toMillis(TAP_STALL_FATAL_TIMEOUT_NS) +
                            "ms of continuous back-pressure");
        case CONTINUE -> {
        }
        }
    }

    /**
     * Gives up on this node, because its archive is the authoritative copy of the sequenced history and a
     * frame that cannot be recorded is a hole that no later work can fill: a node that cannot record is no
     * longer doing the job it exists to do, and is better dead than silently incomplete. Also latches the
     * stall gauge, so an operator watching it does not see the stall clear on the way out.
     * @param reason what failed, for the operator
     */
    private void fatalTapFailure(final String reason) {
        if (!fatalSignalled) {
            actions.tapStalled(true);
        }
        fatalFailure(Logger.EventCode.TapRecordingFailure,
                     reason + ", so it can no longer record the history it is responsible for");
    }

    /**
     * Brings this node down: the peers hold identical, complete recordings and keep quorum without it, and
     * its restart rebuilds everything it held from {@code globalSeqNo} 1 over the full-log replay it
     * performs anyway — so failing loudly and early costs the cluster nothing and costs a silently degraded
     * node everything. Latched: the first call decides, and later ones only fall through to {@link
     * #haltIfShutdownStalled}.
     * @param code   which failure class this is, for the operator's log
     * @param reason what failed
     */
    private void fatalFailure(final Logger.EventCode code, final String reason) {
        if (fatalSignalled) {
            return;
        }
        fatalSignalled = true;
        fatalSignalledNs = actions.nanoTime();
        Logger.fault(Logger.Component.SequencerService, code, actions.memberId(),
                     "FATAL: %s at globalSeqNo=%d — terminating this node; its peers keep quorum and its restart "
                         + "replays the full log",
                     reason, actions.globalSeqNo());
        actions.signalFatal();
    }

    /**
     * Backstop for the graceful teardown {@link Actions#signalFatal} kicks off: that path has to close the
     * very archive that may be what is wedged, so it can hang. By this point the node is committed to dying
     * and everything it holds is either replicated or replayable, so stop waiting and halt.
     * @param nowNs monotonic clock reading
     */
    private void haltIfShutdownStalled(final long nowNs) {
        if (nowNs - fatalSignalledNs >= FATAL_SHUTDOWN_BACKSTOP_NS) {
            actions.halt();
        }
    }
}
