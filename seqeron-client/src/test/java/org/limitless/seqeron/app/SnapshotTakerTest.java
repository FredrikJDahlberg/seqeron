package org.limitless.seqeron.app;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.limitless.seqeron.protocol.Publish;
import org.limitless.seqeron.protocol.SnapshotHeader;
import org.limitless.seqeron.replayer.client.SnapshotStore;

/** A façade's side of snapshot rounds (doc/snapshot.md §4), driven through its {@code Actions} seam. */
class SnapshotTakerTest {
    private static final SnapshotHeader HEADER =
        new SnapshotHeader(3, 1, List.of(new SnapshotHeader.Group(1, "orders")), null);

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

    /** Records the ends placed; declines once {@link #budget} have gone. */
    private static final class Frames implements SnapshotTaker.Actions {
        final List<long[]> ends = new ArrayList<>();
        int budget = Integer.MAX_VALUE;

        @Override
        public Publish publishEnd(final long round, final int recordCount, final long length, final long crc32c,
                                  final int formatVersion) {
            if (budget == 0) {
                return Publish.Declined;
            }
            budget--;
            ends.add(new long[] {round, recordCount, length, crc32c, formatVersion});
            return Publish.Published;
        }
    }

    @TempDir
    Path directory;

    private final State state = new State();
    private final Frames frames = new Frames();
    private final List<SnapshotStore> stores = new ArrayList<>();
    private int keepAlives;
    private final Runnable keepAlive = () -> keepAlives++;

    /** A participating instance with a directory of its own. */
    private SnapshotTaker participating() {
        final SnapshotTaker taker = new SnapshotTaker(state, store(), keepAlive);
        taker.participating(true);
        return taker;
    }

    /** Round {@code round}'s {@code SnapshotStarted}: cut at clusterSeqNo {@code round * 10}. */
    private static void started(final SnapshotTaker taker, final long round, final boolean mayPublish) {
        started(taker, round, HEADER, mayPublish);
    }

    private static void started(final SnapshotTaker taker, final long round, final SnapshotHeader header,
                                final boolean mayPublish) {
        taker.onSnapshotStarted(round, round * 10, round * 10 * 1024, round * 10_000, header, mayPublish);
    }

    @AfterEach
    void awaitWrites() {
        stores.forEach(SnapshotStore::awaitWrites);
    }

    private SnapshotStore store() {
        final SnapshotStore store = new SnapshotStore(directory.resolve("instance-" + stores.size()));
        stores.add(store);
        return store;
    }

    /**
     * The store of the instance {@link #participating} created {@code back} calls ago, 1 for the last, once its
     * writes are done.
     */
    private SnapshotStore storeOf(final int back) {
        final SnapshotStore store = stores.get(stores.size() - back);
        store.awaitWrites();
        return store;
    }

    @Test
    @DisplayName("without a listener, or with the row off, a replica serializes, writes and submits nothing")
    void notParticipating() {
        final SnapshotTaker noListener = new SnapshotTaker(null, null, keepAlive);
        noListener.participating(true);
        assertFalse(noListener.isParticipating());

        final SnapshotTaker rowOff = new SnapshotTaker(state, store(), keepAlive);
        rowOff.participating(false);
        started(rowOff, 1, true);
        assertEquals(0, state.serialized);
        assertEquals(-1, storeOf(1).latestRound(Long.MAX_VALUE));
        assertEquals(0, rowOff.submit(frames));
        assertTrue(rowOff.onSnapshotEnd(1, 3, 2018, 0), "nothing serialized, nothing to disagree with");
    }

    @Test
    @DisplayName("the round's file holds the header, the records and the cut; the publisher submits the end it matches")
    void publisherWritesThenSubmitsTheEnd() {
        final SnapshotTaker taker = participating();
        started(taker, 7, true);
        assertEquals(1, state.serialized);
        assertTrue(taker.isPublishing());
        assertEquals(1, taker.submit(frames), "the end, once");
        assertFalse(taker.isPublishing());
        assertEquals(0, taker.submit(frames));

        final long[] end = frames.ends.get(0);
        assertEquals(7, end[0]);
        assertEquals(3, end[1]);
        assertEquals(2052, end[2]);
        assertEquals(5, end[4], "the listener's formatVersion");
        assertNull(storeOf(1).open(7), "not confirmed before its end is sequenced");
        assertTrue(taker.onSnapshotEnd(7, (int)end[1], end[2], end[3]), "its own end agrees with it");

        final SnapshotStore.Reader reader = storeOf(1).open(7);
        assertNotNull(reader);
        assertEquals(3, reader.recordCount());
        assertEquals(end[2], reader.length());
        assertEquals(end[3], reader.crc32c());
        assertEquals(70, reader.cutClusterSeqNo());
        assertEquals(70 * 1024, reader.cutPosition());
        assertEquals(70_000, reader.cutTimestampNs());
        final List<byte[]> records = readAll(reader);
        assertEquals(HEADER, SnapshotHeader.decode(new UnsafeBuffer(records.get(0)), 0, records.get(0).length));
        assertArrayEquals(filled(0), records.get(1));
        assertArrayEquals(filled(1), records.get(2));
    }

    @Test
    @DisplayName("a declined end is placed again on the next cycle")
    void declinedEndWaitsForTheNextCycle() {
        final SnapshotTaker taker = participating();
        started(taker, 2, true);
        frames.budget = 0;
        assertEquals(0, taker.submit(frames));
        assertTrue(taker.isPublishing());
        frames.budget = Integer.MAX_VALUE;
        assertEquals(1, taker.submit(frames));
        assertEquals(1, frames.ends.size());
    }

    @Test
    @DisplayName("a replica that does not publish writes its file, submits nothing, and compares the source's end")
    void otherReplicasCompare() {
        final SnapshotTaker publisher = participating();
        started(publisher, 4, true);
        publisher.submit(frames);
        final long[] end = frames.ends.get(0);

        final SnapshotTaker follower = participating();
        started(follower, 4, false);
        assertEquals(0, follower.submit(new Frames()));
        assertTrue(follower.onSnapshotEnd(4, (int)end[1], end[2], end[3]));
        assertEquals(4, storeOf(1).latestRound(Long.MAX_VALUE));

        final SnapshotTaker diverged = participating();
        state.records = 3;
        started(diverged, 4, false);
        assertFalse(diverged.onSnapshotEnd(4, (int)end[1], end[2], end[3]), "different state, different records");
        assertEquals(-1, storeOf(1).latestRound(Long.MAX_VALUE), "its file is never confirmed");

        final SnapshotTaker otherRound = participating();
        started(otherRound, 5, false);
        assertTrue(otherRound.onSnapshotEnd(4, 99, 0, 0), "a late end of an earlier round is no evidence");
    }

    @Test
    @DisplayName("a matching end confirms its round's file, which replaces every older one; a mismatch confirms "
                 + "nothing")
    void matchingEndConfirmsTheRound() {
        final SnapshotTaker taker = participating();
        started(taker, 1, true);
        taker.submit(frames);
        final long[] end = frames.ends.get(0);
        assertEquals(0, taker.confirmedRound());
        assertTrue(taker.onSnapshotEnd(1, (int)end[1], end[2], end[3]));
        assertEquals(1, taker.confirmedRound());
        assertEquals(1, storeOf(1).latestRound(Long.MAX_VALUE));

        started(taker, 2, false);
        assertFalse(taker.onSnapshotEnd(2, 99, 0, 0));
        assertEquals(1, taker.confirmedRound());
        assertEquals(1, storeOf(1).latestRound(Long.MAX_VALUE), "a diverged instance keeps what it had");

        started(taker, 3, false);
        assertTrue(taker.onSnapshotEnd(3, (int)end[1], end[2], end[3]));
        assertEquals(3, taker.confirmedRound());
        assertEquals(3, storeOf(1).latestRound(Long.MAX_VALUE));
        assertEquals(-1, storeOf(1).latestRound(3), "the older files are gone");
    }

    @Test
    @DisplayName("a publisher that loses the role stops for good, with no end")
    void lostRoleStops() {
        final SnapshotTaker taker = participating();
        started(taker, 6, true);
        taker.stopPublishing();
        assertEquals(0, taker.submit(frames));
        assertTrue(frames.ends.isEmpty());
        assertEquals(-1, storeOf(1).latestRound(Long.MAX_VALUE), "no end, so nothing is confirmed");
    }

    @Test
    @DisplayName("a listener answering a length no record can have drops the round, with no file and no end")
    void badLengthDropsTheRound() {
        for (final int length : new int[] {-1, 65536}) {
            state.badLength = length;
            final SnapshotTaker taker = participating();
            started(taker, 3, true);
            assertFalse(taker.isPublishing());
            assertEquals(0, taker.submit(frames));
            assertEquals(-1, storeOf(1).latestRound(Long.MAX_VALUE));
            assertTrue(taker.onSnapshotEnd(3, 3, 2018, 0), "nothing serialized, nothing to disagree with");
        }
    }

    @Test
    @DisplayName("a new round supersedes one whose end is not yet placed: only the new one ends")
    void newRoundSupersedes() {
        final SnapshotTaker taker = participating();
        started(taker, 8, true);
        started(taker, 9, true);
        assertEquals(1, taker.submit(frames));
        assertEquals(1, frames.ends.size());
        assertEquals(9, frames.ends.get(0)[0]);
        assertTrue(taker.onSnapshotEnd(8, 99, 0, 0), "nothing held for round 8 any more");
    }

    @Test
    @DisplayName("a round started after the row turned off still ends the one being submitted")
    void roundAfterTheRowTurnedOffAbandonsTheOpenOne() {
        final SnapshotTaker taker = participating();
        started(taker, 8, true);
        taker.participating(false);
        started(taker, 9, true);
        assertFalse(taker.isPublishing());
        assertEquals(0, taker.submit(frames));
        assertTrue(frames.ends.isEmpty(), "round 8 never ended");
        assertTrue(taker.onSnapshotEnd(8, 99, 0, 0), "nothing held for round 8 any more");
    }

    @Test
    @DisplayName("a header longer than a record drops the round")
    void oversizedHeaderDropsTheRound() {
        final SnapshotHeader.GatewayState state35 = new SnapshotHeader.GatewayState(
            9, 10, 42, Collections.nCopies(1771, new SnapshotHeader.GatewayRow(10, 0, "GW")));
        final SnapshotTaker taker = participating();
        started(taker, 3, new SnapshotHeader(7, 2, List.of(), state35), true);
        assertEquals(0, state.serialized, "1771 rows outgrow a record");
        assertFalse(taker.isPublishing());
        assertNull(storeOf(1).open(3));
    }

    @Test
    @DisplayName("a restore reads only this build's format, makes the source take part, and hands its records on")
    void restoreHandsRecordsToTheListener() {
        final SnapshotTaker taker = new SnapshotTaker(state, store(), keepAlive);
        assertTrue(taker.supportsFormatVersion(5));
        assertFalse(taker.supportsFormatVersion(6));

        taker.onSnapshotHeader(HEADER);
        assertTrue(taker.isParticipating(), "its topology row lies before the cut");
        taker.onSnapshotRecord(new UnsafeBuffer(filled(1)), 1000, 0);
        assertEquals(List.of("0:1000:1"), state.restored);
    }

    @Test
    @DisplayName("serializing a round keeps the cluster session alive after every record")
    void serializationKeepsTheSessionAlive() {
        state.records = 3;
        final SnapshotTaker taker = participating();
        started(taker, 1, true);
        assertEquals(4, keepAlives, "the header and three records");
    }

    private static byte[] filled(final int value) {
        final byte[] record = new byte[1000];
        Arrays.fill(record, (byte)value);
        return record;
    }

    private static List<byte[]> readAll(final SnapshotStore.Reader reader) {
        final List<byte[]> records = new ArrayList<>();
        final UnsafeBuffer view = new UnsafeBuffer(0, 0);
        int length;
        while ((length = reader.next(view)) >= 0) {
            final byte[] record = new byte[length];
            view.getBytes(0, record);
            records.add(record);
        }
        assertEquals(SnapshotStore.Reader.END, length);
        reader.close();
        return records;
    }
}
