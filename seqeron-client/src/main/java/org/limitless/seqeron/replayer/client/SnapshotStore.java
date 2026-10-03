package org.limitless.seqeron.replayer.client;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.zip.CRC32C;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.limitless.seqeron.protocol.SnapshotFormat;
import org.limitless.seqeron.util.Logger;

/**
 * This instance's own snapshots, one file per round in a directory no other instance writes (doc/snapshot.md
 * §4). {@code <round>.snapshot} holds the records, each a little-endian uint16 length and its bytes, record 0
 * the façade's header, then a trailer:
 *
 * <pre>
 *  0  round          int64
 *  8  length         int64    the records' bytes, as SnapshotEnd.length
 * 16  recordCount    int32
 * 20  crc32c         uint32
 * 24  formatVersion  uint32
 * 28  magic          uint32   {@link #MAGIC}
 * </pre>
 *
 * A file is written as {@code <round>.tmp} and renamed once complete, without an fsync: one an OS crash tore
 * fails its trailer or its records, and a restore checks both. A write that fails leaves no file and is logged.
 * Not thread-safe. The C++ twin is {@code replayer/client/SnapshotStore.hpp}; keep the two in step.
 */
public final class SnapshotStore {
    /** Bytes after the records. */
    static final int TRAILER_LENGTH = 32;

    /** "SNAP", last in the file, so a file cut short has none. */
    static final int MAGIC = 0x50414E53;

    private static final String SUFFIX = ".snapshot";
    private static final String TEMPORARY_SUFFIX = ".tmp";
    private static final int RECORD_PREFIX_LENGTH = Short.BYTES;
    /** Each file stream's buffer: a multiple of every SSD page size, and few system calls per file. */
    private static final int IO_BUFFER_LENGTH = 64 * 1024;
    private static final ByteOrder LE = ByteOrder.LITTLE_ENDIAN;

    private final Path directory;
    private final UnsafeBuffer scratch =
        new UnsafeBuffer(new byte[RECORD_PREFIX_LENGTH + SnapshotFormat.MAX_RECORD_LENGTH]);
    private OutputStream out;
    private long writingRound;

    /**
     * @param directory this instance's own; created if missing
     * @throws UncheckedIOException if it cannot be
     */
    public SnapshotStore(final Path directory) {
        this.directory = directory;
        try {
            Files.createDirectories(directory);
        } catch (final IOException ex) {
            throw new UncheckedIOException("cannot create snapshot directory " + directory, ex);
        }
    }

    /**
     * Starts writing a round's file, dropping any write still open.
     * @param round its round
     */
    public void begin(final long round) {
        abandon();
        writingRound = round;
        try {
            out = new BufferedOutputStream(Files.newOutputStream(temporaryFile(round)), IO_BUFFER_LENGTH);
        } catch (final IOException ex) {
            failWrite(ex);
        }
    }

    /**
     * Appends one record to the file being written.
     * @param record holding it
     * @param offset of its first byte
     * @param length its length, at most {@link SnapshotFormat#MAX_RECORD_LENGTH}
     */
    public void append(final DirectBuffer record, final int offset, final int length) {
        if (out == null) {
            return;
        }
        scratch.putShort(0, (short)length, LE);
        scratch.putBytes(RECORD_PREFIX_LENGTH, record, offset, length);
        try {
            out.write(scratch.byteArray(), 0, RECORD_PREFIX_LENGTH + length);
        } catch (final IOException ex) {
            failWrite(ex);
        }
    }

    /**
     * Completes the file being written: its trailer, then its name.
     * @param recordCount   records appended
     * @param length        their bytes
     * @param crc32c        their CRC-32C
     * @param formatVersion the listener's
     */
    public void commit(final int recordCount, final long length, final long crc32c, final int formatVersion) {
        if (out == null) {
            return;
        }
        scratch.putLong(0, writingRound, LE);
        scratch.putLong(8, length, LE);
        scratch.putInt(16, recordCount, LE);
        scratch.putInt(20, (int)crc32c, LE);
        scratch.putInt(24, formatVersion, LE);
        scratch.putInt(28, MAGIC, LE);
        try {
            out.write(scratch.byteArray(), 0, TRAILER_LENGTH);
            out.close();
            out = null;
            Files.move(temporaryFile(writingRound), file(writingRound), StandardCopyOption.ATOMIC_MOVE,
                       StandardCopyOption.REPLACE_EXISTING);
        } catch (final IOException ex) {
            failWrite(ex);
        }
    }

    /** Drops the write in progress, if any. */
    public void abandon() {
        if (out == null) {
            return;
        }
        try {
            out.close();
        } catch (final IOException ignored) {
            // The file goes regardless.
        }
        out = null;
        try {
            Files.deleteIfExists(temporaryFile(writingRound));
        } catch (final IOException ignored) {
            // A leftover goes with the next deleteBefore.
        }
    }

    /**
     * Deletes the file of every round before {@code round}, and any write a crash left behind.
     * @param round the oldest round to keep
     */
    public void deleteBefore(final long round) {
        try (DirectoryStream<Path> files = Files.newDirectoryStream(directory)) {
            for (final Path path : files) {
                final String name = path.getFileName().toString();
                final long fileRound = roundOf(name);
                if (name.endsWith(TEMPORARY_SUFFIX) && out == null || fileRound >= 0 && fileRound < round) {
                    Files.deleteIfExists(path);
                }
            }
        } catch (final IOException ex) {
            Logger.log(Logger.CoreComponent.ReplayerStreamReceiver, Logger.Severity.Warn,
                       Logger.CoreEventCode.SnapshotStoreFailed, "cannot delete snapshots before round %d in %s: %s",
                       round, directory, ex);
        }
    }

    /**
     * The newest round below {@code belowRound} with a complete file, or -1.
     * @param belowRound {@link Long#MAX_VALUE} for the newest of all
     */
    public long latestRound(final long belowRound) {
        long latest = -1;
        try (DirectoryStream<Path> files = Files.newDirectoryStream(directory, "*" + SUFFIX)) {
            for (final Path path : files) {
                final long round = roundOf(path.getFileName().toString());
                if (round < belowRound && round > latest) {
                    latest = round;
                }
            }
        } catch (final IOException ex) {
            Logger.log(Logger.CoreComponent.ReplayerStreamReceiver, Logger.Severity.Warn,
                       Logger.CoreEventCode.SnapshotStoreFailed, "cannot list snapshots in %s: %s", directory, ex);
        }
        return latest;
    }

    /**
     * Opens a round's file to read its records.
     * @param round its round
     * @return null if it has none, or its trailer is cut short or names another round
     */
    public Reader open(final long round) {
        final Path path = file(round);
        try (FileChannel channel = FileChannel.open(path)) {
            final long size = channel.size();
            if (size < TRAILER_LENGTH) {
                return null;
            }
            final ByteBuffer trailer = ByteBuffer.allocate(TRAILER_LENGTH).order(LE);
            while (trailer.hasRemaining() && channel.read(trailer, size - TRAILER_LENGTH + trailer.position()) > 0) {
                // Reads until full; a short read is a file cut short underneath.
            }
            if (trailer.hasRemaining() || trailer.getInt(28) != MAGIC || trailer.getLong(0) != round) {
                return null;
            }
            final InputStream in = new BufferedInputStream(Files.newInputStream(path), IO_BUFFER_LENGTH);
            return new Reader(in, size - TRAILER_LENGTH, trailer.getInt(16), trailer.getLong(8),
                              trailer.getInt(20) & 0xFFFF_FFFFL, trailer.getInt(24));
        } catch (final NoSuchFileException ex) {
            return null;
        } catch (final IOException ex) {
            Logger.log(Logger.CoreComponent.ReplayerStreamReceiver, Logger.Severity.Warn,
                       Logger.CoreEventCode.SnapshotStoreFailed, "cannot open %s: %s", path, ex);
            return null;
        }
    }

    private void failWrite(final IOException ex) {
        Logger.log(Logger.CoreComponent.ReplayerStreamReceiver, Logger.Severity.Warn,
                   Logger.CoreEventCode.SnapshotStoreFailed, "cannot write round %d's snapshot in %s: %s",
                   writingRound, directory, ex);
        abandon();
    }

    private Path file(final long round) {
        return directory.resolve(round + SUFFIX);
    }

    private Path temporaryFile(final long round) {
        return directory.resolve(round + TEMPORARY_SUFFIX);
    }

    /** The round a complete file's name holds, or -1 for any other name. */
    private static long roundOf(final String name) {
        if (!name.endsWith(SUFFIX)) {
            return -1;
        }
        try {
            return Long.parseLong(name.substring(0, name.length() - SUFFIX.length()));
        } catch (final NumberFormatException ex) {
            return -1;
        }
    }

    /** One round's file, read record by record, its trailer known before the first. */
    public static final class Reader implements AutoCloseable {
        /** After the last record, which every record before it matched the trailer to. */
        public static final int END = -1;

        /** The records do not match the trailer, or could not be read. Latched. */
        public static final int DAMAGED = -2;

        private final InputStream in;
        private final long recordsLength;
        private final int recordCount;
        private final long length;
        private final long crc32c;
        private final int formatVersion;
        private final byte[] record = new byte[SnapshotFormat.MAX_RECORD_LENGTH];
        private final byte[] prefix = new byte[RECORD_PREFIX_LENGTH];
        private final CRC32C crc = new CRC32C();
        private long consumed;
        private long bytes;
        private int count;
        private boolean damaged;

        private Reader(final InputStream in, final long recordsLength, final int recordCount, final long length,
                       final long crc32c, final int formatVersion) {
            this.in = in;
            this.recordsLength = recordsLength;
            this.recordCount = recordCount;
            this.length = length;
            this.crc32c = crc32c;
            this.formatVersion = formatVersion;
        }

        /** The trailer's {@code recordCount}. */
        public int recordCount() {
            return recordCount;
        }

        /** The trailer's {@code length}. */
        public long length() {
            return length;
        }

        /** The trailer's {@code crc32c}. */
        public long crc32c() {
            return crc32c;
        }

        /** The trailer's {@code formatVersion}. */
        public int formatVersion() {
            return formatVersion;
        }

        /**
         * Points {@code view} at the next record, valid until the next call.
         * @return its length, {@link #END} or {@link #DAMAGED}
         */
        public int next(final UnsafeBuffer view) {
            if (damaged) {
                return DAMAGED;
            }
            if (consumed == recordsLength) {
                damaged = count != recordCount || bytes != length || crc.getValue() != crc32c;
                return damaged ? DAMAGED : END;
            }
            try {
                if (consumed + RECORD_PREFIX_LENGTH > recordsLength ||
                    in.readNBytes(prefix, 0, RECORD_PREFIX_LENGTH) != RECORD_PREFIX_LENGTH) {
                    return damage();
                }
                final int recordLength = (prefix[0] & 0xFF) | (prefix[1] & 0xFF) << 8;
                if (recordLength > SnapshotFormat.MAX_RECORD_LENGTH ||
                    consumed + RECORD_PREFIX_LENGTH + recordLength > recordsLength ||
                    in.readNBytes(record, 0, recordLength) != recordLength) {
                    return damage();
                }
                crc.update(record, 0, recordLength);
                consumed += RECORD_PREFIX_LENGTH + recordLength;
                bytes += recordLength;
                count++;
                view.wrap(record, 0, recordLength);
                return recordLength;
            } catch (final IOException ex) {
                return damage();
            }
        }

        @Override
        public void close() {
            try {
                in.close();
            } catch (final IOException ignored) {
                // Read-only: nothing is lost.
            }
        }

        private int damage() {
            damaged = true;
            return DAMAGED;
        }
    }
}
