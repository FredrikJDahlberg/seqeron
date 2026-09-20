package org.limitless.seqeron.replayer.server;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Recording-chain stitching for {@link ReplayerService}: orders a node's tap recordings oldest to newest
 * and keeps only the newest active one, so a stale active entry is never read as a stopped segment.
 */
public final class ReplayRecordings {
    /**
     * One tap recording as read off an archive listing. {@code startPosition} is carried because a replay
     * must start at or after it, though it is always 0 today.
     */
    public record RecordingSpan(long recordingId, long startPosition, boolean active) { }

    private ReplayRecordings() {
    }

    /**
     * Orders {@code spans} by {@code recordingId} and keeps every stopped span plus the newest active one.
     * @param spans unordered recording spans from one archive listing
     * @return the spans to replay, oldest first
     */
    public static List<RecordingSpan> stitch(final List<RecordingSpan> spans) {
        final List<RecordingSpan> sorted = new ArrayList<>(spans);
        sorted.sort(Comparator.comparingLong(RecordingSpan::recordingId));

        long newestActiveId = -1; // recordingIds are non-negative; -1 means "no active span"
        for (final RecordingSpan span : sorted) {
            if (span.active()) {
                newestActiveId = span.recordingId();
            }
        }

        final List<RecordingSpan> chain = new ArrayList<>(sorted.size());
        for (final RecordingSpan span : sorted) {
            if (!span.active() || span.recordingId() == newestActiveId) {
                chain.add(span);
            }
        }
        return chain;
    }
}
