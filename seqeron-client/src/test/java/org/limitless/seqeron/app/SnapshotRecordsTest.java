package org.limitless.seqeron.app;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.limitless.seqeron.protocol.SnapshotHeader;
import org.limitless.seqeron.protocol.SnapshotValidator;

/** One instance's records of a round: the façade's header as record 0, then the listener's. */
class SnapshotRecordsTest {
    private static final SnapshotHeader HEADER = new SnapshotHeader(7, 2, null);

    @Test
    @DisplayName("records follow the header, and the snapshot's CRC is the shared golden value")
    void recordsFollowTheHeader() {
        final SnapshotRecords records = new SnapshotRecords();
        assertTrue(records.reset(HEADER, true));
        assertEquals(1, records.chunkCount());
        assertEquals(SnapshotHeader.APPLICATION_LENGTH, records.length());

        final UnsafeBuffer body = new UnsafeBuffer(body());
        records.append(body, 0, 1000);
        records.append(body, 1000, 1302);
        records.append(body, 2302, 698);
        assertEquals(4, records.chunkCount());
        assertEquals(3018, records.length());
        assertEquals(0x6c24f56bL, records.crc32c(), "the same value the C++ SnapshotRecordsTest asserts");

        final List<byte[]> kept = kept(records);
        assertEquals(4, kept.size());
        assertEquals(HEADER, SnapshotHeader.decode(new UnsafeBuffer(kept.get(0)), 0, kept.get(0).length));
        assertEquals(1000, kept.get(1).length);
        assertEquals(1302, kept.get(2).length);
        assertEquals(698, kept.get(3).length);
    }

    @Test
    @DisplayName("a header longer than a record is refused")
    void oversizedHeaderIsRefused() {
        final SnapshotHeader.GatewayState state = new SnapshotHeader.GatewayState(
            9, 10, 42, Collections.nCopies(35, new SnapshotHeader.GatewayRow(10, 0, "GW")));
        assertFalse(new SnapshotRecords().reset(new SnapshotHeader(7, 2, state), true), "35 rows outgrow a record");
    }

    @Test
    @DisplayName("an instance that does not publish keeps no records, only what it compares")
    void notRetainedKeepsOnlyTheTotals() {
        final SnapshotRecords kept = new SnapshotRecords();
        final SnapshotRecords counted = new SnapshotRecords();
        kept.reset(HEADER, true);
        counted.reset(HEADER, false);
        final UnsafeBuffer body = new UnsafeBuffer(body());
        for (final SnapshotRecords records : List.of(kept, counted)) {
            records.append(body, 0, 1302);
            records.append(body, 1302, 1302);
        }
        assertEquals(kept.chunkCount(), counted.chunkCount());
        assertEquals(kept.length(), counted.length());
        assertEquals(kept.crc32c(), counted.crc32c());
        assertEquals(List.of(), kept(counted));
    }

    @Test
    @DisplayName("records spanning many segments come back whole and in order, and validate as their chunks")
    void recordsSpanSegments() {
        final SnapshotRecords records = new SnapshotRecords();
        records.reset(HEADER, true);
        final List<byte[]> appended = new ArrayList<>();
        long total = SnapshotHeader.APPLICATION_LENGTH;
        for (int i = 0; total < 3L * SnapshotRecords.SEGMENT_LENGTH; i++) {
            final byte[] record = new byte[i % 7 == 0 ? 1302 : (i * 37) % 1303];
            for (int j = 0; j < record.length; j++) {
                record[j] = (byte)(i + j);
            }
            records.append(new UnsafeBuffer(record), 0, record.length);
            appended.add(record);
            total += record.length;
        }

        final List<byte[]> kept = kept(records);
        assertEquals(appended.size() + 1, kept.size());
        final SnapshotValidator validator = new SnapshotValidator();
        validator.reset(1);
        for (int i = 0; i < kept.size(); i++) {
            if (i > 0) {
                assertArrayEquals(appended.get(i - 1), kept.get(i), "record " + i);
            }
            validator.onChunk(1, i, new UnsafeBuffer(kept.get(i)), 0, kept.get(i).length);
        }
        assertEquals(SnapshotValidator.State.COMPLETE,
                     validator.onEnd(1, records.chunkCount(), records.length(), records.crc32c()));

        records.rewind();
        assertEquals(kept.size(), kept(records).size(), "rewind reads them again");
    }

    private static byte[] body() {
        final byte[] body = new byte[3000];
        for (int i = 0; i < body.length; i++) {
            body[i] = (byte)(i * 31 + 7);
        }
        return body;
    }

    private static List<byte[]> kept(final SnapshotRecords records) {
        final List<byte[]> kept = new ArrayList<>();
        final UnsafeBuffer view = new UnsafeBuffer(0, 0);
        int length;
        while ((length = records.nextRecord(view)) >= 0) {
            final byte[] record = new byte[length];
            view.getBytes(0, record);
            kept.add(record);
        }
        return kept;
    }
}
