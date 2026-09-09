package org.limitless.seqeron.replayer.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.limitless.seqeron.replayer.server.ReplayRecordings.RecordingSpan;

/**
 * Unit tests for the pure recording-chain stitching behind {@link ReplayerService#resolveSegments}.
 * No Aeron/Archive runtime involved — {@link ReplayRecordings#stitch} only sorts and dedups the spans it is
 * handed.
 */
class ReplayRecordingsTest {
    /** A span at the startPosition every tap recording has in practice — see {@link RecordingSpan}. */
    private static RecordingSpan span(final long recordingId, final boolean active) {
        return new RecordingSpan(recordingId, 0, active);
    }

    /** The stitched chain reduced to recordingIds, which is what the ordering/dedup rules are about. */
    private static List<Long> ids(final List<RecordingSpan> chain) {
        return chain.stream().map(RecordingSpan::recordingId).toList();
    }

    @Test
    @DisplayName("a single active recording is the whole chain")
    void singleActiveRecordingIsTheWholeChain() {
        final List<RecordingSpan> chain = ReplayRecordings.stitch(List.of(span(42, true)));

        assertEquals(List.of(42L), ids(chain));
    }

    @Test
    @DisplayName("an empty listing stitches to an empty chain")
    void emptyListingStitchesToEmptyChain() {
        assertTrue(ReplayRecordings.stitch(List.of()).isEmpty());
    }

    @Test
    @DisplayName("stopped recordings plus a trailing active one are ordered oldest first")
    void stoppedRecordingsPlusTrailingActiveOrderedOldestFirst() {
        // A member restart leaves an earlier, stopped recording alongside the post-restart active one.
        final List<RecordingSpan> spans = List.of(span(2, true), span(1, false));

        assertEquals(List.of(1L, 2L), ids(ReplayRecordings.stitch(spans)));
    }

    @Test
    @DisplayName("input is sorted by recordingId regardless of listing order")
    void inputIsSortedByRecordingIdRegardlessOfListingOrder() {
        final List<RecordingSpan> spans = List.of(span(300, false), span(100, false), span(200, true));

        assertEquals(List.of(100L, 200L, 300L), ids(ReplayRecordings.stitch(spans)));
    }

    @Test
    @DisplayName("every stopped recording is kept even with none active")
    void everyStoppedRecordingIsKeptEvenWithNoneActive() {
        final List<RecordingSpan> spans = List.of(span(1, false), span(2, false), span(3, false));

        assertEquals(List.of(1L, 2L, 3L), ids(ReplayRecordings.stitch(spans)));
    }

    @Test
    @DisplayName("with two active spans the newest is kept and the stale one dropped")
    void staleActiveSpanIsDroppedInFavourOfTheNewest() {
        // An unclean shutdown leaves the pre-restart recording unstopped alongside the live one. The
        // stale entry is only a prefix (no snapshots — every restart replays the whole log, so the
        // newest recording starts at globalSeqNo 1 too), and keeping it instead would end the chain
        // before recent history: the client walks it out, declares itself caught up, then gaps forever.
        final List<RecordingSpan> spans = List.of(span(1, true), span(2, true));

        assertEquals(List.of(2L), ids(ReplayRecordings.stitch(spans)));
    }

    @Test
    @DisplayName("stopped spans newer than the kept active one are still kept")
    void stoppedSpansNewerThanTheKeptActiveOneAreStillKept() {
        // Only actives are deduplicated; every stopped span stays in the chain, in recordingId order.
        final List<RecordingSpan> spans = List.of(span(1, true), span(2, true), span(3, false));

        assertEquals(List.of(2L, 3L), ids(ReplayRecordings.stitch(spans)));
    }

    @Test
    @DisplayName("each span's startPosition survives the stitch")
    void startPositionSurvivesTheStitch() {
        // The chain is what ReplayerService replays each segment from, so a span that lost its
        // startPosition on the way through would be replayed from a hardcoded 0 — the assumption
        // doc/review A10 is about.
        final List<RecordingSpan> spans = List.of(new RecordingSpan(2, 8192, true), new RecordingSpan(1, 4096, false));

        final List<RecordingSpan> chain = ReplayRecordings.stitch(spans);

        assertEquals(List.of(4096L, 8192L), chain.stream().map(RecordingSpan::startPosition).toList());
    }

    @Test
    @DisplayName("stitching does not mutate the input list")
    void stitchingDoesNotMutateInput() {
        final List<RecordingSpan> spans = new java.util.ArrayList<>(List.of(span(2, false), span(1, false)));
        final List<RecordingSpan> original = List.copyOf(spans);

        ReplayRecordings.stitch(spans);

        assertEquals(original, spans);
    }
}
