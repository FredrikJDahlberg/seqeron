package org.limitless.seqeron.replayer.client;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

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
import org.limitless.seqeron.protocol.SnapshotHeader;

/**
 * An instance's own snapshot files (doc/snapshot.md §4). The golden trailer is shared with the C++ {@code
 * SnapshotStoreTest}, so a file one language writes is one the other reads.
 */
class SnapshotStoreTest {
    /** Round 3's trailer over the application header and the 3000-byte body as three records, formatVersion 1. */
    private static final String GOLDEN_TRAILER = "0300000000000000" + "ca0b000000000000" + "04000000" + "6bf5246c"
        + "01000000" + "534e4150";

    @TempDir
    Path directory;

    @Test
    @DisplayName("a committed round reads back record by record, its trailer the shared golden bytes")
    void committedRoundReadsBack() throws IOException {
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
    @DisplayName("only a committed write counts, and the newest round below a bound is found")
    void onlyCommittedWritesCount() {
        final SnapshotStore store = new SnapshotStore(directory);
        store.begin(5);
        store.append(new UnsafeBuffer(new byte[8]), 0, 8);
        store.abandon();
        assertEquals(-1, store.latestRound(Long.MAX_VALUE), "an abandoned write leaves no file");

        write(store, 2, records());
        write(store, 4, records());
        store.begin(6); // never committed, as a crash leaves it
        store.append(new UnsafeBuffer(new byte[8]), 0, 8);

        assertEquals(4, store.latestRound(Long.MAX_VALUE));
        assertEquals(2, store.latestRound(4));
        assertEquals(-1, store.latestRound(2));
        assertNull(store.open(6));
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
        oversized[2 + SnapshotHeader.APPLICATION_LENGTH + 1] = (byte)0xFF; // record 1's length, past 1302
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
    @DisplayName("deleting before a round keeps it and every newer one, and clears a write a crash left behind")
    void deleteBeforeKeepsTheRoundAndNewer() throws IOException {
        final SnapshotStore crashed = new SnapshotStore(directory);
        write(crashed, 1, records());
        write(crashed, 2, records());
        write(crashed, 3, records());
        crashed.begin(4);

        new SnapshotStore(directory).deleteBefore(2);

        try (Stream<Path> files = Files.list(directory)) {
            assertEquals(List.of("2.snapshot", "3.snapshot"),
                         files.map(path -> path.getFileName().toString()).sorted().toList());
        }
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

    private static void write(final SnapshotStore store, final long round, final List<byte[]> records) {
        long length = 0;
        final CRC32C crc = new CRC32C();
        store.begin(round);
        for (final byte[] record : records) {
            store.append(new UnsafeBuffer(record), 0, record.length);
            length += record.length;
            crc.update(record);
        }
        store.commit(records.size(), length, crc.getValue(), 1);
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
