package org.limitless.phixeron.replayer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.limitless.phixeron.replayer.ReplayChain.RecordingSpan;

/**
 * Unit tests for the pure recording-chain stitching behind {@link ReplayerService#resolveSegments}.
 * No Aeron/Archive runtime involved — {@link ReplayChain#stitch} only sorts and dedups the spans it is
 * handed.
 */
class ReplayChainTest {
    @Test
    @DisplayName("a single active recording is the whole chain")
    void singleActiveRecordingIsTheWholeChain() {
        final List<Long> chain = ReplayChain.stitch(List.of(new RecordingSpan(42, 1000, true)));

        assertEquals(List.of(42L), chain);
    }

    @Test
    @DisplayName("an empty listing stitches to an empty chain")
    void emptyListingStitchesToEmptyChain() {
        assertTrue(ReplayChain.stitch(List.of()).isEmpty());
    }

    @Test
    @DisplayName("stopped recordings plus a trailing active one are ordered oldest first")
    void stoppedRecordingsPlusTrailingActiveOrderedOldestFirst() {
        // A member restart leaves an earlier, stopped recording alongside the post-restart active one.
        final List<RecordingSpan> spans = List.of(new RecordingSpan(2, 2000, true), new RecordingSpan(1, 1000, false));

        assertEquals(List.of(1L, 2L), ReplayChain.stitch(spans));
    }

    @Test
    @DisplayName("input is sorted by startTimestampMs regardless of listing order")
    void inputIsSortedByStartTimestampRegardlessOfListingOrder() {
        // recordingIds deliberately out of timestamp order, so a chain sorted by recordingId (a bug)
        // would be caught.
        final List<RecordingSpan> spans = List.of(new RecordingSpan(100, 3000, false),
            new RecordingSpan(200, 1000, false), new RecordingSpan(300, 2000, true));

        assertEquals(List.of(200L, 300L, 100L), ReplayChain.stitch(spans));
    }

    @Test
    @DisplayName("every stopped recording is kept even with none active")
    void everyStoppedRecordingIsKeptEvenWithNoneActive() {
        final List<RecordingSpan> spans =
            List.of(new RecordingSpan(1, 1000, false), new RecordingSpan(2, 2000, false), new RecordingSpan(3, 3000, false));

        assertEquals(List.of(1L, 2L, 3L), ReplayChain.stitch(spans));
    }

    @Test
    @DisplayName("a second active span is dropped rather than mistaken for a bounded segment")
    void secondActiveSpanIsDropped() {
        // Defensive: normally there is at most one active (unstopped) recording per node. If the
        // archive transiently lists two, only the first (oldest) is trusted as "still recording" —
        // treating a later one as active too would make resolveSegments serve it as a live/open-ended
        // segment rather than a bounded historical one.
        final List<RecordingSpan> spans =
            List.of(new RecordingSpan(1, 1000, true), new RecordingSpan(2, 2000, true));

        assertEquals(List.of(1L), ReplayChain.stitch(spans));
    }

    @Test
    @DisplayName("stitching does not mutate the input list")
    void stitchingDoesNotMutateInput() {
        final List<RecordingSpan> spans = new java.util.ArrayList<>(
            List.of(new RecordingSpan(2, 2000, false), new RecordingSpan(1, 1000, false)));
        final List<RecordingSpan> original = List.copyOf(spans);

        ReplayChain.stitch(spans);

        assertEquals(original, spans);
    }
}
