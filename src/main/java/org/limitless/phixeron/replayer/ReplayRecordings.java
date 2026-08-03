package org.limitless.phixeron.replayer;

import java.util.ArrayList;
import java.util.List;

/**
 * Pure recording-chain stitching for {@link ReplayerService#resolveSegments}: orders a node's tap
 * recordings oldest→newest and drops any second-and-later "active" (still-recording) entry rather than
 * mistaking it for a bounded historical segment. Free of every Aeron/Archive type so it is unit-testable
 * without one — see {@code ReplayerService.resolveSegments}'s Javadoc for why the chain is normally
 * length 1 and when a member restart can transiently leave more than one entry.
 */
public final class ReplayRecordings {
    /** One tap recording as read off an archive listing, before ordering/stitching. */
    public record RecordingSpan(long recordingId, long startTimestampMs, boolean active) {}

    private ReplayRecordings() {}

    /**
     * Orders {@code spans} oldest→newest by {@code startTimestampMs} and keeps every stopped span plus
     * at most the first active one encountered.
     * @param spans unordered recording spans from one archive listing
     * @return recordingIds to replay, oldest first
     */
    public static List<Long> stitch(final List<RecordingSpan> spans) {
        final List<RecordingSpan> sorted = new ArrayList<>(spans);
        sorted.sort((a, b) -> Long.compare(a.startTimestampMs(), b.startTimestampMs()));

        final List<Long> chain = new ArrayList<>(sorted.size());
        boolean keptActive = false;
        for (final RecordingSpan span : sorted) {
            if (!span.active() || !keptActive) {
                keptActive |= span.active();
                chain.add(span.recordingId());
            }
        }
        return chain;
    }
}
