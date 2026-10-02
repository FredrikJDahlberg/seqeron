package org.limitless.seqeron.app;

import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32C;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.limitless.seqeron.protocol.SnapshotFormat;
import org.limitless.seqeron.protocol.SnapshotHeader;

/**
 * One instance's snapshot of a round (doc/snapshot.md §4): the records serialized at the cut, each at most
 * {@link SnapshotFormat#MAX_RECORD_LENGTH} bytes and each carried by one {@code SnapshotChunk}, the façade's
 * header first. Only the instance that publishes keeps the records, packed into fixed-size segments, so a
 * snapshot is bounded by memory and not by a buffer's capacity; every other instance keeps only the count,
 * length and CRC it compares against the {@code SnapshotEnd}. The C++ twin is {@code
 * app/detail/SnapshotRecords.hpp}; keep the two in step.
 */
final class SnapshotRecords {
    /** Bytes per storage segment; a record and its 2-byte length never straddle two. */
    static final int SEGMENT_LENGTH = 1 << 20;

    private static final int RECORD_PREFIX_LENGTH = Short.BYTES;

    /** In place of a length: the rest of this segment is unused. No record is this long. */
    private static final int END_OF_SEGMENT = 0xFFFF;
    private static final ByteOrder LE = ByteOrder.LITTLE_ENDIAN;

    private final CRC32C crc = new CRC32C();
    private final List<UnsafeBuffer> segments = new ArrayList<>();
    private final UnsafeBuffer headerRecord = new UnsafeBuffer(new byte[SnapshotFormat.MAX_RECORD_LENGTH]);
    private boolean retain;
    private int chunkCount;
    private long length;
    private int writeOffset = SEGMENT_LENGTH;
    private int readSegment;
    private int readOffset;

    /**
     * Appends one record.
     * @param record holding it
     * @param offset of its first byte
     * @param count  its length, at most {@link SnapshotFormat#MAX_RECORD_LENGTH}
     */
    void append(final DirectBuffer record, final int offset, final int count) {
        SnapshotFormat.update(crc, record, offset, count);
        if (retain) {
            if (writeOffset + RECORD_PREFIX_LENGTH + count > SEGMENT_LENGTH) {
                if (writeOffset + RECORD_PREFIX_LENGTH <= SEGMENT_LENGTH) {
                    segments.get(segments.size() - 1).putShort(writeOffset, (short)END_OF_SEGMENT, LE);
                }
                segments.add(new UnsafeBuffer(new byte[SEGMENT_LENGTH]));
                writeOffset = 0;
            }
            final UnsafeBuffer segment = segments.get(segments.size() - 1);
            segment.putShort(writeOffset, (short)count, LE);
            segment.putBytes(writeOffset + RECORD_PREFIX_LENGTH, record, offset, count);
            writeOffset += RECORD_PREFIX_LENGTH + count;
        }
        chunkCount++;
        length += count;
    }

    /**
     * Starts a new snapshot, dropping the last one, with the façade's header as record 0.
     * @param header the façade's header
     * @param retain whether to keep the records, as only the publisher does
     * @return false if the header is longer than a record, as a gateway of more than 34 instances' is
     */
    boolean reset(final SnapshotHeader header, final boolean retain) {
        this.retain = retain;
        segments.clear();
        crc.reset();
        chunkCount = 0;
        length = 0;
        writeOffset = SEGMENT_LENGTH;
        rewind();
        if (header.encodedLength() > SnapshotFormat.MAX_RECORD_LENGTH) {
            return false;
        }
        append(headerRecord, 0, header.encode(headerRecord, 0));
        return true;
    }

    /** Drops the kept records, once submitted or abandoned; the totals stay. */
    void release() {
        segments.clear();
        writeOffset = SEGMENT_LENGTH;
        rewind();
    }

    /** Records appended so far, the header included: the {@code chunkCount}. */
    int chunkCount() {
        return chunkCount;
    }

    /** Their bytes: the {@code length}. */
    long length() {
        return length;
    }

    /** Their CRC-32C: the {@code crc32c}. */
    long crc32c() {
        return crc.getValue();
    }

    /** Starts {@link #nextRecord} over from record 0. */
    void rewind() {
        readSegment = 0;
        readOffset = 0;
    }

    /**
     * Points {@code view} at the next kept record.
     * @param view wrapped over the record
     * @return its length, or -1 after the last
     */
    int nextRecord(final UnsafeBuffer view) {
        final int lastSegment = segments.size() - 1;
        if (readSegment > lastSegment || readSegment == lastSegment && readOffset == writeOffset) {
            return -1;
        }
        if (readOffset + RECORD_PREFIX_LENGTH > SEGMENT_LENGTH ||
            (segments.get(readSegment).getShort(readOffset, LE) & 0xFFFF) == END_OF_SEGMENT) {
            readSegment++;
            readOffset = 0;
        }
        final UnsafeBuffer segment = segments.get(readSegment);
        final int count = segment.getShort(readOffset, LE) & 0xFFFF;
        view.wrap(segment, readOffset + RECORD_PREFIX_LENGTH, count);
        readOffset += RECORD_PREFIX_LENGTH + count;
        return count;
    }
}
