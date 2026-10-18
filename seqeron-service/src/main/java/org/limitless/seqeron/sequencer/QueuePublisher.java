package org.limitless.seqeron.sequencer;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import org.agrona.DirectBuffer;
import org.limitless.seqeron.util.Logger;

/**
 * The three things {@link SequencerService} does from a cluster callback that can wait on the runtime — open
 * the queues, put a frame on one, and re-arm the cluster clock — under that callback's rule: never return with
 * the work undone, and never throw (a throw skips the message). Each spins, bounded by the node terminating
 * once the wait provably will not clear; one class, so the fatal decision is latched across them.
 *
 * <p>Free of Aeron types: the transport is behind {@link Actions}. Single-threaded: the cluster-callback thread.
 */
public final class QueuePublisher {
    /** Everything {@link QueuePublisher} cannot do itself; {@link SequencerService} implements it against Aeron. */
    public interface Actions {
        /** Creates group {@code groupId}'s queue and asks the co-located archive to record it. May throw. */
        void openQueue(int groupId);

        /**
         * Whether the archive has started recording group {@code groupId}'s queue; once it has, the queue's
         * recording is the one {@link #recordingActive} and {@link #recordedPosition} read.
         */
        boolean attachRecording(int groupId);

        /**
         * Offers the frame at offset 0 of {@code buffer} onto group {@code groupId}'s queue.
         * @return the raw Aeron offer result — negative is a failure, classified by {@link #isUnrecoverable}
         */
        long offerFrame(int groupId, DirectBuffer buffer, int length);

        /** Whether an offer result is one no amount of retrying can clear (CLOSED / MAX_POSITION_EXCEEDED). */
        boolean isUnrecoverable(long offerResult);

        /** @return whether the consensus module accepted the cluster-clock timer */
        boolean scheduleTimer(long deadline);

        /** The cluster's idle strategy — what a spin does between attempts. */
        void idle();

        /** Monotonic clock. Every deadline here measures elapsed real time, not cluster time. */
        long nanoTime();

        /** Whether the co-located archive is still recording group {@code groupId}'s queue. */
        boolean recordingActive(int groupId);

        /** How far the archive has recorded that queue; only meaningful while {@link #recordingActive}. */
        long recordedPosition(int groupId);

        /** Which recording that is, for the operator's log line. */
        long recordingId(int groupId);

        /** The {@code seqeron.sequencer.queueBackPressureAlerts} counter. */
        void queueBackPressureAlert();

        /** The {@code seqeron.sequencer.queueStalled} gauge. */
        void queueStalled(boolean stalled);

        /** Brings the node down gracefully. Must not block: it signals a shutdown and returns. */
        void signalFatal();

        /** Ends the process at once: a fatal inside a spin, which a graceful shutdown would wait on forever. */
        void halt();

        /** The node this service runs on — log attribution only. */
        Integer memberId();

        /** Where sequencing has reached, for the log lines. */
        long clusterSeqNo();
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
     * terminates. An archive that is slow but still advancing is never timed out. 5x the stall gauge; re-tune
     * against production storage.
     */
    static final long QUEUE_STALL_FATAL_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(1);

    /**
     * How long the consensus module may refuse the cluster-clock timer before {@link #scheduleHeartbeat}
     * gives up on this node; a second without one accepted is wedged, not busy. Matches
     * {@link #QUEUE_STALL_FATAL_TIMEOUT_NS}.
     */
    static final long HEARTBEAT_SCHEDULE_FATAL_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(1);

    /** How long {@link #activate} waits, for all of its queues together, for the archive to record them (§7.4). */
    static final long QUEUE_RECORDING_START_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(5);

    private static final int[] NO_GROUPS = new int[0];

    private final Actions actions;

    /** The groups whose queues are open, ascending; none before activation. */
    private int[] activeGroups = NO_GROUPS;

    /** Monotonic reading at the last observed recording advance; 0 when no stall is in progress. */
    private long stallSinceNs;
    private long stallRecordedPosition;
    private boolean stalled;

    private boolean fatalSignalled;

    public QueuePublisher(final Actions actions) {
        this.actions = actions;
    }

    /**
     * Opens a queue per group and waits until the archive records every one, for {@link
     * #QUEUE_RECORDING_START_TIMEOUT_NS} in all: a queue nothing records would back-pressure the first frame
     * forever. A queue that cannot be opened or recorded in time terminates the node, whose restart replays to
     * the same frame and tries again. Inside a callback that cannot return with the queues missing, so it halts.
     * @param groupIds the groups to activate, ascending
     */
    public void activate(final int[] groupIds) {
        activeGroups = groupIds.clone();
        final long deadlineNs = actions.nanoTime() + QUEUE_RECORDING_START_TIMEOUT_NS;
        for (final int groupId : activeGroups) {
            try {
                actions.openQueue(groupId);
            } catch (final RuntimeException ex) {
                fatalQueueFailure("group " + groupId + "'s queue could not be opened and recorded: " + ex, true);
                return;
            }
        }
        for (final int groupId : activeGroups) {
            while (!actions.attachRecording(groupId)) {
                if (actions.nanoTime() - deadlineNs >= 0) {
                    fatalQueueFailure("the local archive did not start recording group " + groupId + "'s queue within " +
                                          TimeUnit.NANOSECONDS.toMillis(QUEUE_RECORDING_START_TIMEOUT_NS) + "ms",
                                      true);
                    return;
                }
                actions.idle();
            }
        }
        Logger.info(Logger.CoreComponent.Sequencer, actions.memberId(),
                    "groups %s active: each queue opened and recorded at clusterSeqNo=%d",
                    Arrays.toString(activeGroups), actions.clusterSeqNo());
    }

    /**
     * Publishes a frame onto a group's queue. Spins on back-pressure, since a dropped
     * frame is an unrecoverable gap, but terminates the node once the recording is provably not draining.
     * The only exits are a landed offer and process death.
     *
     * <p>{@code offer}, not {@code tryClaim}: a claim saves one copy, only below the queue's MTU, of a frame the
     * cluster has already logged, replicated and archived, and an aborted claim pads this node's recording
     * alone, moving its positions off the other members' that {@code QueueRelay} resumes at.
     * @param groupId the queue's group
     * @param buffer  holding the frame from offset 0
     * @param length  of the frame
     */
    public void emit(final int groupId, final DirectBuffer buffer, final int length) {
        int spins = 0;
        long nextAlertNs = 0;
        long result;
        while ((result = actions.offerFrame(groupId, buffer, length)) < 0) {
            if (actions.isUnrecoverable(result)) {
                fatalQueueFailure("group " + groupId + "'s queue publication failed: " + result, true);
            }
            if (++spins >= SPINS_PER_CLOCK_CHECK) {
                spins = 0;
                final long nowNs = actions.nanoTime();
                if (nextAlertNs == 0) {
                    nextAlertNs = nowNs + BACK_PRESSURE_ALERT_INTERVAL_NS; // first read only anchors the period
                } else if (nowNs - nextAlertNs >= 0) {
                    nextAlertNs = nowNs + BACK_PRESSURE_ALERT_INTERVAL_NS;
                    onBackPressureThreshold(groupId, nowNs);
                }
            }
            actions.idle();
        }
        final boolean wasStalled = stalled;
        stallSinceNs = 0;
        stallRecordedPosition = 0;
        stalled = false;
        if (wasStalled && !fatalSignalled) {
            actions.queueStalled(false);
            Logger.info(Logger.CoreComponent.Sequencer, actions.memberId(),
                        "RECOVERED: queue back-pressure cleared at clusterSeqNo=%d", actions.clusterSeqNo());
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
                } else if (!fatalSignalled && nowNs - backPressuredSinceNs >= HEARTBEAT_SCHEDULE_FATAL_TIMEOUT_NS) {
                    fatalFailure(Logger.CoreEventCode.ServiceError,
                                 "the consensus module did not accept the cluster-clock timer for " +
                                     TimeUnit.NANOSECONDS.toSeconds(HEARTBEAT_SCHEDULE_FATAL_TIMEOUT_NS) +
                                     "s of continuous back-pressure",
                                 true);
                }
            }
            actions.idle();
        }
    }

    /**
     * Liveness check on every queue's recording, run on the 1 Hz clock: a recording that stops back-pressures
     * nothing (the untethered app subscribers keep the publication connected), so {@link #emit}'s bound
     * would never see it. Before activation there is nothing to check.
     */
    public void checkRecordingAlive() {
        for (final int groupId : activeGroups) {
            if (!fatalSignalled && !actions.recordingActive(groupId)) {
                fatalQueueFailure(stoppedRecording(groupId), false);
            }
        }
    }

    private String stoppedRecording(final int groupId) {
        return "the local archive stopped recording group " + groupId + "'s queue (recording " +
               actions.recordingId(groupId) + ")";
    }

    /** Whether this node has already been given up on — see {@link #fatalFailure}. */
    public boolean isFatalSignalled() {
        return fatalSignalled;
    }

    /**
     * One {@link #BACK_PRESSURE_ALERT_INTERVAL_NS} of continuous back-pressure has elapsed in {@link #emit}:
     * alert, then time how long the recording has made no progress. Kept out of the spin, so an emit whose
     * first offer lands pays none of it.
     * @param groupId the back-pressured queue's group
     * @param nowNs   the clock reading that triggered this period, reused rather than read again
     */
    private void onBackPressureThreshold(final int groupId, final long nowNs) {
        if (fatalSignalled) {
            return;
        }
        Logger.error(Logger.CoreComponent.Sequencer, Logger.CoreEventCode.QueueBackpressure, actions.memberId(),
                     "ALERT: group %d's queue back-pressure at clusterSeqNo=%d", groupId, actions.clusterSeqNo());
        actions.queueBackPressureAlert();
        if (!actions.recordingActive(groupId)) {
            fatalQueueFailure(stoppedRecording(groupId), true);
            return;
        }
        final long recordedPosition = actions.recordedPosition(groupId);
        if (stallSinceNs == 0 || recordedPosition > stallRecordedPosition) {
            // The first observation only anchors the clock: how long back-pressure lasted before it is unknown.
            stallRecordedPosition = recordedPosition;
            stallSinceNs = nowNs;
            return;
        }
        final long stalledNs = nowNs - stallSinceNs;
        if (stalledNs >= QUEUE_STALL_FATAL_TIMEOUT_NS) {
            fatalQueueFailure("group " + groupId + "'s queue recording made no progress for " +
                                  TimeUnit.NANOSECONDS.toMillis(QUEUE_STALL_FATAL_TIMEOUT_NS) +
                                  "ms of continuous back-pressure",
                              true);
        } else if (stalledNs >= SUSTAINED_BACKPRESSURE_THRESHOLD_NS && !stalled) {
            stalled = true;
            actions.queueStalled(true);
            Logger.error(
                Logger.CoreComponent.Sequencer, Logger.CoreEventCode.QueueBackpressure, actions.memberId(),
                "STALLED: group %d's queue back-pressure sustained beyond %dms with no recording progress at "
                    + "clusterSeqNo=%d",
                groupId, TimeUnit.NANOSECONDS.toMillis(SUSTAINED_BACKPRESSURE_THRESHOLD_NS), actions.clusterSeqNo());
        }
    }

    /**
     * Terminates the node over its queue recording, latching the stall gauge so it does not clear on the way out.
     * @param reason what failed, for the operator
     * @param inSpin whether this thread is inside a spin it cannot leave; see {@link #fatalFailure}
     */
    private void fatalQueueFailure(final String reason, final boolean inSpin) {
        if (!fatalSignalled) {
            actions.queueStalled(true);
        }
        fatalFailure(Logger.CoreEventCode.QueueRecordingFailure,
                     reason + ", so it can no longer record the history it is responsible for", inSpin);
    }

    /**
     * Brings this node down: peers keep quorum, and its restart rebuilds its recording over full-log replay.
     * From inside a spin it halts, since a graceful shutdown waits for this thread, which stays in the spin.
     * Latched.
     * @param code   which failure class this is, for the operator's log
     * @param reason what failed
     * @param inSpin whether this thread is inside a spin it cannot leave
     */
    private void fatalFailure(final Logger.EventCode code, final String reason, final boolean inSpin) {
        if (fatalSignalled) {
            return;
        }
        fatalSignalled = true;
        Logger.fault(Logger.CoreComponent.SequencerService, code, actions.memberId(),
                     "FATAL: %s at clusterSeqNo=%d — terminating this node; its peers keep quorum and its restart "
                         + "replays the full log",
                     reason, actions.clusterSeqNo());
        if (inSpin) {
            actions.halt();
        } else {
            actions.signalFatal();
        }
    }
}
