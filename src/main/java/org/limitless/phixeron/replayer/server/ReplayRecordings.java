package org.limitless.phixeron.replayer.server;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Pure recording-chain stitching for {@link ReplayerService}: orders a node's tap
 * recordings oldest→newest and keeps at most one "active" (still-recording) entry — the newest — rather
 * than mistaking a stale one for a bounded historical segment. Free of every Aeron/Archive type so it is
 * unit-testable without one — see {@code ReplayerService.resolveSegments}'s Javadoc for why the chain is
 * normally length 1 and when a member restart can transiently leave more than one entry.
 */
public final class ReplayRecordings {
    /**
     * One tap recording as read off an archive listing, before ordering/stitching.
     *
     * <p>{@code startPosition} is carried rather than assumed to be 0: it is where the archive began
     * recording the tap publication, and every replay of this recording must start at or after it.
     * Today it is always 0 (SequencerService arms recording on a brand-new publication before emitting
     * anything), but a replay requested below it fails at the archive rather than degrading.
     */
    public record RecordingSpan(long recordingId, long startPosition, boolean active) { }

    private ReplayRecordings() {
    }

    /**
     * Orders {@code spans} oldest→newest by {@code recordingId} and keeps every stopped span plus the
     * newest active one.
     * <p>The key is {@code recordingId} — monotone by construction as the archive creates recordings.
     * <p>The active span kept is the newest.
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
