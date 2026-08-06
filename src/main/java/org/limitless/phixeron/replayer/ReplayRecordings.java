package org.limitless.phixeron.replayer;

import java.util.ArrayList;
import java.util.List;

/**
 * Pure recording-chain stitching for {@link ReplayerService#resolveSegments}: orders a node's tap
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
    public record RecordingSpan(long recordingId, long startPosition, boolean active) {}

    private ReplayRecordings() {}

    /**
     * Orders {@code spans} oldest→newest by {@code recordingId} and keeps every stopped span plus the
     * newest active one.
     *
     * <p>The key is {@code recordingId} — monotone by construction as the archive creates recordings —
     * and not the descriptor's {@code startTimestamp}, which is archive wall clock: a backward clock
     * step between two recordings inverts them, and the client's {@code globalSeqNo} de-dupe then
     * discards the older segment wholesale with no diagnostic.
     *
     * <p>The active span kept is the newest, not the first: an unclean shutdown leaves the pre-restart
     * recording unstopped alongside the live one, and that stale entry is only a prefix — with no
     * snapshots every restart replays the whole log, so the newest recording starts at {@code
     * globalSeqNo} 1 as well and holds complete history. Keeping the stale one instead ends the chain
     * before recent history, and a client that walks it to its end declares itself caught up, gaps on
     * the tap, re-walks, and never converges.
     * @param spans unordered recording spans from one archive listing
     * @return the spans to replay, oldest first
     */
    public static List<RecordingSpan> stitch(final List<RecordingSpan> spans) {
        final List<RecordingSpan> sorted = new ArrayList<>(spans);
        sorted.sort((a, b) -> Long.compare(a.recordingId(), b.recordingId()));

        long newestActiveId = -1;  // recordingIds are non-negative; -1 means "no active span"
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
