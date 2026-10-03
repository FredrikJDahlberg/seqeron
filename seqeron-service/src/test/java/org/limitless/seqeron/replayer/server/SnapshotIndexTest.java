package org.limitless.seqeron.replayer.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.ArrayList;
import java.util.List;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Each source's sequenced snapshot ends, by round, read off the recording in order (doc/snapshot.md §5). */
class SnapshotIndexTest {
    private static final int SOURCE = 3;
    private static final int OTHER = 4;

    private final List<String> indexed = new ArrayList<>();
    private final SnapshotIndex index =
        new SnapshotIndex((sourceId, entry) -> indexed.add(sourceId + ":" + entry.round()));
    private final SnapshotFrames frames = new SnapshotFrames();
    private long position;

    /** Feeds one frame, at the position after the last; returns its start position. */
    private long feed(final byte[] frame) {
        final long start = position;
        position += frame.length;
        index.onFrame(new UnsafeBuffer(frame), 0, frame.length, start);
        return start;
    }

    @Test
    @DisplayName("a source's end is indexed with its round's cut, where its start is, and what it says")
    void endIsIndexed() {
        final long cut = frames.nextGlobalSeqNo();
        final long startedAt = feed(frames.started(1));
        assertNull(index.lookup(SOURCE, 1), "not before its end");
        feed(frames.end(SOURCE, 1, 3, 2018, 0xABCL));

        final SnapshotIndex.Entry entry = index.lookup(SOURCE, 1);
        assertEquals(1, entry.round());
        assertEquals(cut, entry.asOfGlobalSeqNo());
        assertEquals(startedAt, entry.asOfPosition());
        assertEquals(3, entry.formatVersion());
        assertEquals(3, entry.recordCount());
        assertEquals(2018, entry.length());
        assertEquals(0xABCL, entry.crc32c());
        assertEquals(List.of("3:1"), indexed);
    }

    @Test
    @DisplayName("every round's end stays, so an instance holding an older round still finds it")
    void everyRoundStays() {
        feed(frames.started(1));
        feed(frames.end(SOURCE, 1, 1, 10, 1));
        feed(frames.started(2));
        feed(frames.end(SOURCE, 2, 1, 20, 2));

        assertEquals(10, index.lookup(SOURCE, 1).length());
        assertEquals(20, index.lookup(SOURCE, 2).length());
        assertNull(index.lookup(SOURCE, 3));
        assertEquals(List.of("3:1", "3:2"), indexed);
    }

    @Test
    @DisplayName("sources interleave, each indexed on its own; a source's second end of a round changes nothing")
    void sourcesAreIndependent() {
        feed(frames.started(1));
        feed(frames.end(OTHER, 1, 1, 30, 3));
        feed(frames.end(SOURCE, 1, 1, 10, 1));
        feed(frames.end(SOURCE, 1, 1, 99, 9));
        feed(frames.started(2));
        feed(frames.end(OTHER, 2, 1, 40, 4));

        assertEquals(10, index.lookup(SOURCE, 1).length(), "the first end is the one sequenced");
        assertNull(index.lookup(SOURCE, 2));
        assertEquals(40, index.lookup(OTHER, 2).length());
        assertNull(index.lookup(99, 1));
        assertEquals(List.of("4:1", "3:1", "4:2"), indexed);
    }

    @Test
    @DisplayName("a round's end arriving after the next round started still indexes it, at its own cut")
    void lateEndOfAnEarlierRound() {
        final long cut = frames.nextGlobalSeqNo();
        feed(frames.started(1));
        feed(frames.started(2));
        feed(frames.end(SOURCE, 2, 1, 20, 2));
        feed(frames.end(SOURCE, 1, 1, 10, 1));
        assertEquals(cut, index.lookup(SOURCE, 1).asOfGlobalSeqNo(), "round 1's own cut, not round 2's");
        assertEquals(List.of("3:2"), indexed, "the counter follows the newest round only");
    }

    @Test
    @DisplayName("the end of a round whose start this recording does not hold is not indexed")
    void unknownRoundIsIgnored() {
        feed(frames.end(SOURCE, 7, 1, 10, 1));
        feed(frames.end(SOURCE, -1, 1, 10, 1));
        assertNull(index.lookup(SOURCE, 7));

        index.reset();
        feed(frames.started(1));
        for (long round = 2; round <= 1 + SnapshotIndex.REMEMBERED_ROUNDS; round++) {
            feed(frames.started(round));
        }
        feed(frames.end(SOURCE, 1, 1, 10, 1));
        assertNull(index.lookup(SOURCE, 1), "too many rounds back to remember its start");
    }
}
