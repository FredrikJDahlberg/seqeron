package org.limitless.phixeron.sequencer;

/**
 * Pure decision logic behind {@code SequencerService.emit}'s handling of tap back-pressure: given how
 * long the offer has been failing and what the co-located archive's recording of the tap is doing
 * meanwhile, it decides whether to keep waiting, raise the stall gauge, or give up and terminate the
 * node. Free of every Aeron type so it is unit-testable without a cluster — the service reads the two
 * inputs off the archive's {@code RecordingPos} counter and applies the verdict, mirroring how {@link
 * Sequencer} is split from {@code SequencerService}.
 *
 * <p><b>The discriminator is recording progress, not elapsed back-pressure.</b> An archive draining
 * slowly under load back-pressures continuously while still making progress, and killing that node
 * would turn a load spike into an outage; an archive whose recording position has not moved at all is
 * not draining, and no amount of further waiting changes that. A recording counter that has gone away
 * is fatal at once — nothing is being recorded any more, so waiting cannot help either.
 */
public final class TapStallPolicy {
    /** What the service should do about the back-pressure it is currently spinning on. */
    public enum Action {
        /** Keep waiting: back-pressure is transient, or the archive is slow but still draining. */
        CONTINUE,
        /** Back-pressure sustained past the stall threshold with no recording progress. Edge-triggered. */
        STALLED,
        /** The archive is no longer recording the tap: nothing more can be recorded here. */
        FATAL_RECORDING_GONE,
        /** The recording has made no progress for the fatal timeout: the archive is not draining. */
        FATAL_NO_PROGRESS
    }

    private final long stallThresholdNs;
    private final long fatalTimeoutNs;

    /** Monotonic reading at the last observed recording advance; 0 when no stall is in progress. */
    private long progressNs;
    private long recordedPosition;
    private boolean stalled;

    /**
     * @param stallThresholdNs how long without recording progress before the stall gauge is raised
     * @param fatalTimeoutNs   how long without recording progress before the node must terminate
     */
    public TapStallPolicy(final long stallThresholdNs, final long fatalTimeoutNs) {
        this.stallThresholdNs = stallThresholdNs;
        this.fatalTimeoutNs = fatalTimeoutNs;
    }

    /**
     * Evaluates one back-pressure observation. The first observation of a stall only anchors the clock:
     * how long back-pressure had already lasted before it is unknown, so the timers start here.
     * @param nowNs            monotonic clock reading
     * @param recordingActive  whether the archive's recording of the tap is still live
     * @param recordedPosition that recording's current position; ignored unless {@code recordingActive}
     * @return what the service should do
     */
    public Action onBackPressure(final long nowNs, final boolean recordingActive, final long recordedPosition) {
        if (!recordingActive) {
            return Action.FATAL_RECORDING_GONE;
        }
        if (progressNs == 0 || recordedPosition > this.recordedPosition) {
            this.recordedPosition = recordedPosition;
            progressNs = nowNs;
            return Action.CONTINUE;
        }
        final long stalledNs = nowNs - progressNs;
        if (stalledNs >= fatalTimeoutNs) {
            return Action.FATAL_NO_PROGRESS;
        }
        if (stalledNs >= stallThresholdNs && !stalled) {
            stalled = true;
            return Action.STALLED;
        }
        return Action.CONTINUE;
    }

    /**
     * Records that the offer landed, clearing the stall state for the next frame.
     * @return true if the stall gauge had been raised and must now be cleared
     */
    public boolean onEmitted() {
        progressNs = 0;
        recordedPosition = 0;
        final boolean wasStalled = stalled;
        stalled = false;
        return wasStalled;
    }
}
