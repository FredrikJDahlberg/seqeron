package org.limitless.phixeron.sequencer;

/**
 * Location of a committed Raft log entry in Aeron Archive.
 * Used by outboundArchiveIndex to service slow-path ResendRequests.
 */
public record ArchivePosition(long recordingId, long startPosition, long endPosition) {

    /** Sentinel for open-ended replay (replay to recording end). */
    public static final long REPLAY_TO_END = Long.MAX_VALUE;
}