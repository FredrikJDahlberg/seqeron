package org.limitless.seqeron.replayer.server;

/**
 * What a gateway host's relay of a member's Raft log decides, free of Aeron: which member's log it reads, where a
 * replay of it starts, and when it gives up on a member or on itself (doc/disk-load-plan.md, step 2a).
 * {@link AeronLogRelay} is the adapter.
 *
 * <p>The first member it reads fixes where the derivation starts. After that it resumes at the end of the last entry
 * it applied, on whichever member: members' log positions agree by construction.
 *
 * <p>Single-threaded: the relay's duty cycle.
 */
final class LogRelay {
    /** {@link #replayFrom}'s answer for a member whose log does not hold the position this relay continues from. */
    static final long NOT_SUITABLE = -1;

    /** No entry for this long means the member is gone: three cluster heartbeats. */
    static final long SOURCE_TIMEOUT_MS = 3_000;

    /** Pause before trying the members again once every one of them has failed in a row. */
    static final long RETRY_INTERVAL_MS = 500;

    private final int sourceCount;
    private final long stallTimeoutMs;
    private int sourceIndex;
    private int failedAttempts;
    private long nextAttemptMs;
    private long lastEntryMs;

    /** Where the next entry to apply starts, or {@link #NOT_SUITABLE} before a member has fixed the start. */
    private long position = NOT_SUITABLE;

    private long liveTarget = NOT_SUITABLE;
    private boolean live;

    private boolean backPressured;
    private long backPressuredSinceMs;
    private long backPressuredRecordedPosition;

    /**
     * @param sourceCount    how many member archives it may read from
     * @param stallTimeoutMs how long a local recording may make no progress under back-pressure, then it stops
     */
    LogRelay(final int sourceCount, final long stallTimeoutMs) {
        if (sourceCount < 1) {
            throw new IllegalArgumentException("a relay needs at least one member archive");
        }
        this.sourceCount = sourceCount;
        this.stallTimeoutMs = stallTimeoutMs;
    }

    long stallTimeoutMs() {
        return stallTimeoutMs;
    }

    /** Which member archive to read next, as an index into the configured list. */
    int sourceIndex() {
        return sourceIndex;
    }

    /** Where the next entry to apply starts, or {@link #NOT_SUITABLE} before a member has fixed the start. */
    long position() {
        return position;
    }

    /** Whether an attempt on {@link #sourceIndex} may start now. */
    boolean mayAttempt(final long nowMs) {
        return nowMs >= nextAttemptMs;
    }

    /**
     * Where to replay a member's log from.
     *
     * @param announcedStart   where that member's service started, which the derivation starts at if it has not yet
     * @param logStart         where the member's log recording starts
     * @param recordedPosition how far it has recorded
     * @param nowMs            the clock, which starts the source timeout
     * @return the position, or {@link #NOT_SUITABLE} when the member's log does not hold it
     */
    long replayFrom(final long announcedStart, final long logStart, final long recordedPosition, final long nowMs) {
        lastEntryMs = nowMs;
        final long from = position == NOT_SUITABLE ? announcedStart : position;
        if (from < logStart || from > recordedPosition) {
            return NOT_SUITABLE;
        }
        position = from;
        if (!live && liveTarget == NOT_SUITABLE) {
            liveTarget = recordedPosition;
        }
        return from;
    }

    /**
     * An entry is about to be applied.
     *
     * @param entryEnd where it ends in the log
     * @param nowMs    the clock; any entry proves the member alive
     */
    void onEntry(final long entryEnd, final long nowMs) {
        position = entryEnd;
        lastEntryMs = nowMs;
        live |= entryEnd > liveTarget;
        failedAttempts = 0;
    }

    /** A local queue took a frame. */
    void onOffered() {
        backPressured = false;
    }

    /**
     * A local queue refused a frame; it is offered again next cycle.
     *
     * @param nowMs            the clock
     * @param recordedPosition how far the local archive has recorded that queue
     * @return true once the local recording has made no progress for {@link #stallTimeoutMs}
     */
    boolean onBackPressured(final long nowMs, final long recordedPosition) {
        lastEntryMs = nowMs; // the member is not the one holding this up
        if (!backPressured || recordedPosition != backPressuredRecordedPosition) {
            backPressured = true;
            backPressuredSinceMs = nowMs;
            backPressuredRecordedPosition = recordedPosition;
            return false;
        }
        return nowMs - backPressuredSinceMs >= stallTimeoutMs;
    }

    /**
     * Whether the relay has caught up with its member: it has applied an entry recorded after the member's log was
     * last read at a connect, while it was not yet live. Once live it stays live across member changes.
     */
    boolean isLive() {
        return live;
    }

    /** Whether the member has sent nothing for {@link #SOURCE_TIMEOUT_MS}. */
    boolean isSourceStalled(final long nowMs) {
        return nowMs - lastEntryMs >= SOURCE_TIMEOUT_MS;
    }

    /**
     * The member could not be read, or stopped sending: move to the next one, and pause once every member has failed
     * in a row.
     */
    void onSourceLost(final long nowMs) {
        sourceIndex = (sourceIndex + 1) % sourceCount;
        liveTarget = NOT_SUITABLE;
        backPressured = false;
        if (++failedAttempts >= sourceCount) {
            failedAttempts = 0;
            nextAttemptMs = nowMs + RETRY_INTERVAL_MS;
        }
    }
}
