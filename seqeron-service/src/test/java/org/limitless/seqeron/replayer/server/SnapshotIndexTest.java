package org.limitless.seqeron.replayer.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.ArrayList;
import java.util.List;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Each source's latest valid snapshot, read off the recording in order (doc/snapshot.md §5). */
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
        index.onFrame(new UnsafeBuffer(frame), 0, frame.length, start, position);
        return start;
    }

    @Test
    @DisplayName("a complete snapshot is indexed with its cut, where its start is and where its end ends")
    void completeSnapshotIsIndexed() {
        final byte[] a = SnapshotFrames.record(1302, 1);
        final byte[] b = SnapshotFrames.record(400, 2);
        final long cut = frames.nextGlobalSeqNo();
        final long startedAt = feed(frames.started(1));
        feed(frames.chunk(SOURCE, 1, 0, a));
        feed(frames.chunk(SOURCE, 1, 1, b));
        assertNull(index.lookup(SOURCE), "not before its end");
        feed(frames.end(SOURCE, 1, a, b));

        final SnapshotIndex.Entry entry = index.lookup(SOURCE);
        assertEquals(1, entry.round());
        assertEquals(cut, entry.asOfGlobalSeqNo());
        assertEquals(startedAt, entry.asOfPosition());
        assertEquals(position, entry.endPosition(), "just past the SnapshotEnd");
        assertEquals(3, entry.formatVersion());
        assertEquals(List.of("3:1"), indexed);
    }

    @Test
    @DisplayName("a snapshot that fails its check is not indexed, and the source's previous one stays")
    void invalidSnapshotKeepsThePrevious() {
        final byte[] a = SnapshotFrames.record(100, 1);
        feed(frames.started(1));
        feed(frames.chunk(SOURCE, 1, 0, a));
        feed(frames.end(SOURCE, 1, a));

        feed(frames.started(2));
        feed(frames.chunk(SOURCE, 2, 0, a));
        feed(frames.end(SOURCE, 2, 1, a.length, 0xBAD));
        assertEquals(1, index.lookup(SOURCE).round(), "a wrong CRC");

        feed(frames.started(3));
        feed(frames.chunk(SOURCE, 3, 0, a));
        feed(frames.chunk(SOURCE, 3, 2, a));
        feed(frames.end(SOURCE, 3, a, a));
        assertEquals(1, index.lookup(SOURCE).round(), "a missing chunk");
        assertEquals(List.of("3:1"), indexed);
    }

    @Test
    @DisplayName("sources interleave, each indexed on its own; a later round replaces an earlier one")
    void sourcesAreIndependent() {
        final byte[] a = SnapshotFrames.record(200, 1);
        final byte[] b = SnapshotFrames.record(300, 2);
        feed(frames.started(1));
        feed(frames.chunk(SOURCE, 1, 0, a));
        feed(frames.chunk(OTHER, 1, 0, b));
        feed(frames.end(OTHER, 1, b));
        feed(frames.end(SOURCE, 1, a));
        feed(frames.started(2));
        feed(frames.chunk(OTHER, 2, 0, a));
        feed(frames.end(OTHER, 2, a));

        assertEquals(1, index.lookup(SOURCE).round());
        assertEquals(2, index.lookup(OTHER).round());
        assertNull(index.lookup(99));
    }

    @Test
    @DisplayName("a round's end arriving after the next round started still indexes it, unless a later one is in")
    void lateEndOfAnEarlierRound() {
        final byte[] a = SnapshotFrames.record(200, 1);
        final long cut = frames.nextGlobalSeqNo();
        feed(frames.started(1));
        feed(frames.chunk(SOURCE, 1, 0, a));
        feed(frames.started(2));
        feed(frames.end(SOURCE, 1, a));
        assertEquals(cut, index.lookup(SOURCE).asOfGlobalSeqNo(), "round 1's own cut, not round 2's");
    }

    @Test
    @DisplayName("chunks of a round whose start this recording does not hold are not indexed")
    void unknownRoundIsIgnored() {
        final byte[] a = SnapshotFrames.record(200, 1);
        feed(frames.chunk(SOURCE, 7, 0, a));
        feed(frames.end(SOURCE, 7, a));
        assertNull(index.lookup(SOURCE));

        index.reset();
        feed(frames.started(1));
        for (long round = 2; round <= 1 + SnapshotIndex.REMEMBERED_ROUNDS; round++) {
            feed(frames.started(round));
        }
        feed(frames.chunk(SOURCE, 1, 0, a));
        feed(frames.end(SOURCE, 1, a));
        assertNull(index.lookup(SOURCE), "too many rounds back to remember its start");
    }
}
