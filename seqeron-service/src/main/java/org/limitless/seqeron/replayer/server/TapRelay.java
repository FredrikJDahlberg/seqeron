package org.limitless.seqeron.replayer.server;

/**
 * What a gateway host's relay decides, free of Aeron: which member's tap recording it reads, where a
 * replay of it starts, which frames it republishes onto the local tap, and when it gives up on a member
 * or on itself. {@link AeronTapRelay} is the adapter.
 *
 * <p>It republishes exactly the frame after the last one it published, so the local tap holds the log
 * frame for frame whichever member each frame came from. Every member's active recording starts at
 * {@code globalSeqNo} 1 and its positions follow from the frames alone, so a replay on the next member
 * normally resumes at the position the last one reached. Nothing depends on that: a frame past the
 * next one means the position did not line up there, and the replay starts again from that
 * recording's start, skipping what is already published.
 *
 * <p>Single-threaded: the relay's duty cycle.
 */
final class TapRelay {
    /** What to do with one frame off a member's replay. */
    enum Verdict {
        /** The next frame: republish it. */
        PUBLISH,
        /** Already published: drop it. */
        SKIP,
        /** A hole: replay this member's recording again from its start. */
        REPLAY_FROM_START,
        /** A hole in a replay that began at its recording's start: that recording cannot serve. */
        DROP_SOURCE
    }

    /** {@link #replayFrom}'s answer for a member whose recording has not reached this relay yet. */
    static final long NOT_SUITABLE = -1;

    /** No frame for this long means the member is gone: three cluster heartbeats. */
    static final long SOURCE_TIMEOUT_MS = 3_000;

    /** Pause before trying the members again once every one of them has failed in a row. */
    static final long RETRY_INTERVAL_MS = 500;

    /** How long the local recording may make no progress under back-pressure before the relay stops. */
    static final long RECORDING_STALL_FATAL_MS = 1_000;

    private final int sourceCount;
    private int sourceIndex;
    private int failedAttempts;
    private long nextAttemptMs;
    private long lastFrameMs;

    private long lastGlobalSeqNo;
    private long resumePosition = NOT_SUITABLE;
    private boolean fromStart;
    private boolean forceStart;

    private boolean backPressured;
    private long backPressuredSinceMs;
    private long backPressuredRecordedPosition;

    /**
     * A relay that has published nothing.
     *
     * @param sourceCount how many member archives it may read from
     */
    TapRelay(final int sourceCount) {
        if (sourceCount < 1) {
            throw new IllegalArgumentException("a relay needs at least one member archive");
        }
        this.sourceCount = sourceCount;
    }

    /** Which member archive to read next, as an index into the configured list. */
    int sourceIndex() {
        return sourceIndex;
    }

    /** The last {@code globalSeqNo} republished; 0 before the first. */
    long lastGlobalSeqNo() {
        return lastGlobalSeqNo;
    }

    /** Whether an attempt on {@link #sourceIndex} may start now. */
    boolean mayAttempt(final long nowMs) {
        return nowMs >= nextAttemptMs;
    }

    /**
     * Where to replay a member's active tap recording from.
     *
     * @param startPosition    where that recording starts
     * @param recordedPosition how far it has recorded
     * @param nowMs            the clock, which starts the source timeout
     * @return the position, or {@link #NOT_SUITABLE} when the member has not recorded as far as this relay
     *     has published
     */
    long replayFrom(final long startPosition, final long recordedPosition, final long nowMs) {
        lastFrameMs = nowMs;
        if (forceStart || resumePosition < startPosition) {
            forceStart = false;
            fromStart = true;
            return startPosition;
        }
        if (resumePosition > recordedPosition) {
            return NOT_SUITABLE;
        }
        fromStart = false;
        return resumePosition;
    }

    /**
     * Judges one frame off the replay.
     *
     * @param globalSeqNo the frame's
     * @param nowMs       the clock; any frame proves the member alive
     */
    Verdict onFrame(final long globalSeqNo, final long nowMs) {
        lastFrameMs = nowMs;
        if (globalSeqNo <= lastGlobalSeqNo) {
            failedAttempts = 0;
            return Verdict.SKIP;
        }
        if (globalSeqNo == lastGlobalSeqNo + 1) {
            return Verdict.PUBLISH;
        }
        return onHole();
    }

    /** A fragment that is not a frame: the replay did not start on a frame boundary. */
    Verdict onUnreadable() {
        return onHole();
    }

    /**
     * The frame {@link #onFrame} said to publish is on the local tap.
     *
     * @param globalSeqNo    its {@code globalSeqNo}
     * @param sourcePosition the member recording's position just past it
     */
    void onPublished(final long globalSeqNo, final long sourcePosition) {
        lastGlobalSeqNo = globalSeqNo;
        resumePosition = sourcePosition;
        failedAttempts = 0;
        backPressured = false;
    }

    /**
     * The local tap refused the frame; it is offered again next cycle.
     *
     * @param nowMs            the clock
     * @param recordedPosition how far the local archive has recorded the local tap
     * @return true once the local recording has made no progress for {@link #RECORDING_STALL_FATAL_MS}
     */
    boolean onBackPressured(final long nowMs, final long recordedPosition) {
        lastFrameMs = nowMs; // the member is not the one holding this up
        if (!backPressured || recordedPosition > backPressuredRecordedPosition) {
            backPressured = true;
            backPressuredSinceMs = nowMs;
            backPressuredRecordedPosition = recordedPosition;
            return false;
        }
        return nowMs - backPressuredSinceMs >= RECORDING_STALL_FATAL_MS;
    }

    /** Whether the member has sent nothing for {@link #SOURCE_TIMEOUT_MS}. */
    boolean isSourceStalled(final long nowMs) {
        return nowMs - lastFrameMs >= SOURCE_TIMEOUT_MS;
    }

    /**
     * The member could not be read, or stopped sending: move to the next one, and pause once every member
     * has failed in a row.
     *
     * @param nowMs the clock
     */
    void onSourceLost(final long nowMs) {
        sourceIndex = (sourceIndex + 1) % sourceCount;
        forceStart = false;
        backPressured = false;
        if (++failedAttempts >= sourceCount) {
            failedAttempts = 0;
            nextAttemptMs = nowMs + RETRY_INTERVAL_MS;
        }
    }

    private Verdict onHole() {
        if (fromStart) {
            return Verdict.DROP_SOURCE;
        }
        forceStart = true;
        return Verdict.REPLAY_FROM_START;
    }
}
