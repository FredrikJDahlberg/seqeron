package org.limitless.seqeron.replayer.client;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.zip.CRC32C;
import java.util.stream.Stream;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.limitless.seqeron.protocol.SnapshotFormat;
import org.limitless.seqeron.protocol.SnapshotHeader;

/**
 * An instance's own snapshot files (doc/snapshot.md §4.1). The golden trailer is shared with the C++ and C# {@code
 * SnapshotStoreTest}s, so a file one language writes is one the others read.
 */
class SnapshotStoreTest {
    /**
     * Round 3's trailer over the application header and the 3000-byte body as three records, formatVersion 1, cut at
     * globalSeqNo 42, position 43008, timestamp 42000.
     */
    private static final String GOLDEN_TRAILER = "0300000000000000" + "ca0b000000000000" + "04000000" + "6bf5246c"
        + "01000000" + "2a00000000000000" + "00a8000000000000" + "10a4000000000000" + "534e5032";

    private static final long CUT_GLOBAL_SEQ_NO = 42;
    private static final long CUT_POSITION = 43008;
    private static final long CUT_TIMESTAMP_NS = 42000;

    @TempDir
    Path directory;

    @Test
    @DisplayName("a confirmed round reads back record by record, its trailer the shared golden bytes")
    void confirmedRoundReadsBack() throws IOException {
        final SnapshotStore store = new SnapshotStore(directory);
        final List<byte[]> records = records();
        write(store, 3, records);

        final byte[] file = Files.readAllBytes(directory.resolve("3.snapshot"));
        assertEquals(3018 + 2 * 4 + SnapshotStore.TRAILER_LENGTH, file.length);
        assertEquals(GOLDEN_TRAILER,
                     HexFormat.of().formatHex(Arrays.copyOfRange(file, file.length - SnapshotStore.TRAILER_LENGTH,
                                                                 file.length)));

        final SnapshotStore.Reader reader = store.open(3);
        assertNotNull(reader);
        assertEquals(4, reader.recordCount());
        assertEquals(3018, reader.length());
        assertEquals(0x6c24f56bL, reader.crc32c());
        assertEquals(1, reader.formatVersion());
        assertEquals(CUT_GLOBAL_SEQ_NO, reader.cutGlobalSeqNo());
        assertEquals(CUT_POSITION, reader.cutPosition());
        assertEquals(CUT_TIMESTAMP_NS, reader.cutTimestampNs());
        final UnsafeBuffer view = new UnsafeBuffer(0, 0);
        for (final byte[] record : records) {
            assertEquals(record.length, reader.next(view));
            final byte[] read = new byte[record.length];
            view.getBytes(0, read);
            assertArrayEquals(record, read);
        }
        assertEquals(SnapshotStore.Reader.END, reader.next(view));
        reader.close();
    }

    @Test
    @DisplayName("only a confirmed round counts, and the newest round below a bound is found")
    void onlyConfirmedRoundsCount() throws IOException {
        final SnapshotStore store = new SnapshotStore(directory);
        begin(store, 5);
        store.append(new UnsafeBuffer(new byte[8]), 0, 8);
        store.abandon();
        store.confirm(5);
        store.awaitWrites();
        assertEquals(List.of(), names(), "an abandoned write leaves no file, and confirms nothing");

        commit(store, 2, records());
        assertEquals(-1, store.latestRound(Long.MAX_VALUE), "a round the log has not confirmed");
        assertNull(store.open(2));
        store.confirm(1);
        store.awaitWrites();
        assertEquals(List.of("2.tmp"), names(), "a confirmation of another round confirms nothing");

        write(store, 4, records());
        Files.copy(directory.resolve("4.snapshot"), directory.resolve("3.snapshot")); // as a crash may leave one
        assertEquals(4, store.latestRound(Long.MAX_VALUE));
        assertEquals(3, store.latestRound(4));
        assertEquals(-1, store.latestRound(3));
        assertNull(store.open(7), "no such round");
    }

    @Test
    @DisplayName("a file cut short or of another round does not open; records that fail its trailer are damaged")
    void damagedFiles() throws IOException {
        final SnapshotStore store = new SnapshotStore(directory);
        write(store, 2, records());
        final Path file = directory.resolve("2.snapshot");
        final byte[] intact = Files.readAllBytes(file);

        Files.copy(file, directory.resolve("3.snapshot"));
        assertNull(store.open(3), "its trailer names round 2");

        Files.write(file, Arrays.copyOf(intact, intact.length - 1));
        assertNull(store.open(2), "cut short: no trailer");

        final byte[] flipped = intact.clone();
        flipped[2 + SnapshotHeader.APPLICATION_LENGTH + 2 + 5] ^= 1; // a body byte of record 1
        Files.write(file, flipped);
        assertEquals(List.of(18, 1000, 1302, 698, SnapshotStore.Reader.DAMAGED), readAll(store.open(2)));

        final byte[] oversized = intact.clone();
        oversized[2 + SnapshotHeader.APPLICATION_LENGTH + 1] = (byte)0xFF; // record 1's length, past the file's records
        Files.write(file, oversized);
        assertEquals(List.of(18, SnapshotStore.Reader.DAMAGED), readAll(store.open(2)));

        try (RandomAccessFile cut = new RandomAccessFile(file.toFile(), "rw")) {
            cut.write(intact);
            cut.seek(intact.length - SnapshotStore.TRAILER_LENGTH + 16);
            cut.write(5); // a recordCount the records do not reach
        }
        assertEquals(List.of(18, 1000, 1302, 698, SnapshotStore.Reader.DAMAGED), readAll(store.open(2)));
    }

    @Test
    @DisplayName("a confirmation replaces every older file, and any write a crash left behind")
    void confirmationReplacesOlderFiles() throws IOException {
        final SnapshotStore crashed = new SnapshotStore(directory);
        write(crashed, 1, records());
        commit(crashed, 2, records());
        begin(crashed, 3);

        final SnapshotStore store = new SnapshotStore(directory);
        write(store, 4, records());
        assertEquals(List.of("4.snapshot"), names());

        commit(store, 5, records());
        begin(store, 6); // round 5's end never came
        store.abandon();
        assertEquals(List.of("4.snapshot", "5.tmp"), names());
        write(store, 7, records());
        assertEquals(List.of("7.snapshot"), names());
    }

    @Test
    @DisplayName("a round whose file never became durable deletes nothing older")
    void undurableRoundDeletesNothing() throws IOException {
        final SnapshotStore store = new SnapshotStore(directory);
        write(store, 1, records());
        Files.createDirectories(directory.resolve("2.snapshot").resolve("occupied")); // the name cannot be taken
        write(store, 2, records());

        assertTrue(Files.isRegularFile(directory.resolve("1.snapshot")));
        assertTrue(Files.isRegularFile(directory.resolve("2.tmp")));
    }

    @Test
    @DisplayName("a record of the most bytes its length prefix can say reads back whole")
    void largestRecordReadsBack() {
        final SnapshotStore store = new SnapshotStore(directory);
        final byte[] largest = new byte[SnapshotFormat.MAX_RECORD_LENGTH];
        Arrays.fill(largest, (byte)7);
        write(store, 1, List.of(largest));

        final SnapshotStore.Reader reader = store.open(1);
        final UnsafeBuffer view = new UnsafeBuffer(0, 0);
        assertEquals(SnapshotFormat.MAX_RECORD_LENGTH, reader.next(view));
        assertEquals(7, view.getByte(SnapshotFormat.MAX_RECORD_LENGTH - 1));
        assertEquals(SnapshotStore.Reader.END, reader.next(view));
        reader.close();
    }

    /** The application header, then the 3000-byte body as records of 1000, 1302 and 698 bytes. */
    private static List<byte[]> records() {
        final UnsafeBuffer header = new UnsafeBuffer(new byte[SnapshotHeader.APPLICATION_LENGTH]);
        new SnapshotHeader(7, 2, null).encode(header, 0);
        final byte[] body = new byte[3000];
        for (int i = 0; i < body.length; i++) {
            body[i] = (byte)(i * 31 + 7);
        }
        return List.of(header.byteArray(), Arrays.copyOfRange(body, 0, 1000), Arrays.copyOfRange(body, 1000, 2302),
                       Arrays.copyOfRange(body, 2302, 3000));
    }

    /** Writes and confirms {@code round}'s file. */
    private static void write(final SnapshotStore store, final long round, final List<byte[]> records) {
        commit(store, round, records);
        store.confirm(round);
        store.awaitWrites();
    }

    /** Writes {@code round}'s file, as the instance does before the log confirms it. */
    private static void commit(final SnapshotStore store, final long round, final List<byte[]> records) {
        long length = 0;
        final CRC32C crc = new CRC32C();
        begin(store, round);
        for (final byte[] record : records) {
            store.append(new UnsafeBuffer(record), 0, record.length);
            length += record.length;
            crc.update(record);
        }
        store.commit(records.size(), length, crc.getValue(), 1);
    }

    private static void begin(final SnapshotStore store, final long round) {
        store.begin(round, CUT_GLOBAL_SEQ_NO, CUT_POSITION, CUT_TIMESTAMP_NS);
    }

    private List<String> names() throws IOException {
        try (Stream<Path> files = Files.list(directory)) {
            return files.map(path -> path.getFileName().toString()).sorted().toList();
        }
    }

    /** Each record's length in order, then what ends the read. */
    private static List<Integer> readAll(final SnapshotStore.Reader reader) {
        final List<Integer> lengths = new ArrayList<>();
        final UnsafeBuffer view = new UnsafeBuffer(0, 0);
        int length;
        do {
            length = reader.next(view);
            lengths.add(length);
        } while (length >= 0);
        reader.close();
        return lengths;
    }
}
