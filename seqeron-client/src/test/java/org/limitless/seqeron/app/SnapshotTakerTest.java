package org.limitless.seqeron.app;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.limitless.seqeron.protocol.Publish;
import org.limitless.seqeron.protocol.SnapshotHeader;
import org.limitless.seqeron.protocol.SnapshotValidator;

/** A façade's side of snapshot rounds (doc/snapshot.md §4), driven through its {@code Actions} seam. */
class SnapshotTakerTest {
    private static final SnapshotHeader HEADER = new SnapshotHeader(3, 1, null);

    /** Encodes {@code records} records of 1000 bytes, each filled with its index, then answers 0. */
    private static final class State implements SnapshotListener {
        int records = 2;
        int serialized;
        int badLength;
        final List<String> restored = new ArrayList<>();

        @Override
        public int formatVersion() {
            return 5;
        }

        @Override
        public int onSnapshot(final MutableDirectBuffer buffer, final int recordIndex) {
            if (recordIndex == 0) {
                serialized++;
            }
            if (recordIndex == records) {
                return 0;
            }
            if (badLength != 0) {
                return badLength;
            }
            buffer.setMemory(0, 1000, (byte)recordIndex);
            return 1000;
        }

        @Override
        public void onRestore(final DirectBuffer buffer, final int length, final int recordIndex) {
            restored.add(recordIndex + ":" + length + ":" + buffer.getByte(0));
        }
    }

    /** Records what was placed; declines once {@link #budget} frames have gone. */
    private static final class Frames implements SnapshotTaker.Actions {
        final List<byte[]> chunks = new ArrayList<>();
        final List<Long> chunkRounds = new ArrayList<>();
        final List<long[]> ends = new ArrayList<>();
        int budget = Integer.MAX_VALUE;

        @Override
        public Publish publishChunk(final long round, final int chunkIndex, final DirectBuffer record,
                                    final int offset, final int length) {
            if (budget == 0) {
                return Publish.Declined;
            }
            budget--;
            assertEquals(chunks.size(), chunkIndex, "chunks go out in order, each once");
            final byte[] bytes = new byte[length];
            record.getBytes(offset, bytes);
            chunks.add(bytes);
            chunkRounds.add(round);
            return Publish.Published;
        }

        @Override
        public Publish publishEnd(final long round, final int chunkCount, final long length, final long crc32c,
                                  final int formatVersion) {
            if (budget == 0) {
                return Publish.Declined;
            }
            budget--;
            ends.add(new long[] {round, chunkCount, length, crc32c, formatVersion});
            return Publish.Published;
        }
    }

    private final State state = new State();
    private final Frames frames = new Frames();

    private SnapshotTaker participating() {
        final SnapshotTaker taker = new SnapshotTaker(state);
        taker.participating(true);
        return taker;
    }

    @Test
    @DisplayName("without a listener, or with the row off, a replica serializes and submits nothing")
    void notParticipating() {
        final SnapshotTaker noListener = new SnapshotTaker(null);
        noListener.participating(true);
        assertFalse(noListener.isParticipating());

        final SnapshotTaker rowOff = new SnapshotTaker(state);
        rowOff.participating(false);
        rowOff.onSnapshotStarted(1, HEADER, true);
        assertEquals(0, state.serialized);
        assertEquals(0, rowOff.submit(frames));
        assertTrue(rowOff.onSnapshotEnd(1, 3, 2018, 0), "nothing serialized, nothing to disagree with");
    }

    @Test
    @DisplayName("the publisher submits the header, its records in order, then an end its own frames validate")
    void publisherSubmitsRecordsThenEnd() {
        final SnapshotTaker taker = participating();
        taker.onSnapshotStarted(7, HEADER, true);
        assertEquals(1, state.serialized);
        assertEquals(4, taker.submit(frames), "two records, the header and the end");
        assertFalse(taker.isPublishing());

        assertEquals(3, frames.chunks.size());
        assertEquals(HEADER, SnapshotHeader.decode(new org.agrona.concurrent.UnsafeBuffer(frames.chunks.get(0)), 0,
                                                   frames.chunks.get(0).length));
        assertArrayEquals(filled(1), frames.chunks.get(2));
        final long[] end = frames.ends.get(0);
        assertEquals(7, end[0]);
        assertEquals(3, end[1]);
        assertEquals(2018, end[2]);
        assertEquals(5, end[4], "the listener's formatVersion");

        final SnapshotValidator validator = new SnapshotValidator();
        validator.reset(7);
        for (int i = 0; i < frames.chunks.size(); i++) {
            validator.onChunk(7, i, new org.agrona.concurrent.UnsafeBuffer(frames.chunks.get(i)), 0,
                              frames.chunks.get(i).length);
        }
        assertEquals(SnapshotValidator.State.COMPLETE, validator.onEnd(7, (int)end[1], end[2], end[3]));
        assertTrue(taker.onSnapshotEnd(7, (int)end[1], end[2], end[3]), "its own end agrees with it");
    }

    @Test
    @DisplayName("a declined frame is placed again next cycle, and no cycle places more than its budget")
    void declinedFramesWaitForTheNextCycle() {
        state.records = 40;
        final SnapshotTaker taker = participating();
        taker.onSnapshotStarted(2, HEADER, true);
        frames.budget = 5;
        assertEquals(5, taker.submit(frames));
        frames.budget = Integer.MAX_VALUE;
        assertEquals(SnapshotTaker.MAX_CHUNKS_PER_CYCLE, taker.submit(frames));
        while (taker.isPublishing()) {
            taker.submit(frames);
        }
        assertEquals(41, frames.chunks.size(), "the header and forty records, none lost or repeated");
        assertEquals(1, frames.ends.size());
    }

    @Test
    @DisplayName("a replica that does not publish submits nothing, and compares the source's end with its own")
    void otherReplicasCompare() {
        final SnapshotTaker publisher = participating();
        publisher.onSnapshotStarted(4, HEADER, true);
        publisher.submit(frames);
        final long[] end = frames.ends.get(0);

        final SnapshotTaker follower = participating();
        follower.onSnapshotStarted(4, HEADER, false);
        assertEquals(0, follower.submit(new Frames()));
        assertTrue(follower.onSnapshotEnd(4, (int)end[1], end[2], end[3]));

        final SnapshotTaker diverged = participating();
        state.records = 3;
        diverged.onSnapshotStarted(4, HEADER, false);
        assertFalse(diverged.onSnapshotEnd(4, (int)end[1], end[2], end[3]), "different state, different records");

        final SnapshotTaker otherRound = participating();
        otherRound.onSnapshotStarted(5, HEADER, false);
        assertTrue(otherRound.onSnapshotEnd(4, 99, 0, 0), "a late end of an earlier round is no evidence");
    }

    @Test
    @DisplayName("a publisher that loses the role stops for good, with no end")
    void lostRoleStops() {
        state.records = 40;
        final SnapshotTaker taker = participating();
        taker.onSnapshotStarted(6, HEADER, true);
        taker.submit(frames);
        taker.stopPublishing();
        assertEquals(0, taker.submit(frames));
        assertEquals(SnapshotTaker.MAX_CHUNKS_PER_CYCLE, frames.chunks.size());
        assertTrue(frames.ends.isEmpty());
    }

    @Test
    @DisplayName("a listener answering a length no record can have drops the round, with nothing submitted")
    void badLengthDropsTheRound() {
        for (final int length : new int[] {-1, 1303}) {
            state.badLength = length;
            final SnapshotTaker taker = participating();
            taker.onSnapshotStarted(3, HEADER, true);
            assertFalse(taker.isPublishing());
            assertEquals(0, taker.submit(frames));
            assertTrue(taker.onSnapshotEnd(3, 3, 2018, 0), "nothing serialized, nothing to disagree with");
        }
    }

    @Test
    @DisplayName("a new round supersedes one still being submitted: the old one gets no end, the new starts at 0")
    void newRoundSupersedes() {
        state.records = 40;
        final SnapshotTaker taker = participating();
        taker.onSnapshotStarted(8, HEADER, true);
        taker.submit(frames);
        state.records = 1;
        taker.onSnapshotStarted(9, HEADER, true);
        final Frames next = new Frames();
        while (taker.isPublishing()) {
            taker.submit(next);
        }
        assertEquals(List.of(9L, 9L), next.chunkRounds);
        assertEquals(1, next.ends.size());
        assertEquals(9, next.ends.get(0)[0]);
        assertTrue(frames.ends.isEmpty(), "round 8 never ended");
    }

    @Test
    @DisplayName("a restore reads only this build's format, makes the source take part, and hands its records on")
    void restoreHandsRecordsToTheListener() {
        final SnapshotTaker taker = new SnapshotTaker(state);
        assertTrue(taker.supportsFormatVersion(5));
        assertFalse(taker.supportsFormatVersion(6));

        taker.onSnapshotHeader(HEADER);
        assertTrue(taker.isParticipating(), "its topology row lies before the cut");
        taker.onSnapshotRecord(new org.agrona.concurrent.UnsafeBuffer(filled(1)), 1000, 0);
        assertEquals(List.of("0:1000:1"), state.restored);
    }

    private static byte[] filled(final int value) {
        final byte[] record = new byte[1000];
        java.util.Arrays.fill(record, (byte)value);
        return record;
    }
}
