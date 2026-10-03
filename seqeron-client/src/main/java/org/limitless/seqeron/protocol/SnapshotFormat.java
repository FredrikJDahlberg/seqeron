package org.limitless.seqeron.protocol;

import java.util.zip.CRC32C;
import org.agrona.DirectBuffer;

/**
 * What a snapshot is (doc/snapshot.md §2): a sequence of records, the façade's header first, checked by the
 * CRC-32C of their bytes in order that its {@code SnapshotEnd} carries. The C++ twin is {@code
 * protocol/Snapshot.hpp}; keep the two in step.
 */
public final class SnapshotFormat {
    /** The most bytes one record holds: what its uint16 length prefix in the file can say. */
    public static final int MAX_RECORD_LENGTH = 65535;

    private SnapshotFormat() {
    }

    /**
     * The CRC-32C of a span of bytes, as {@code SnapshotEnd.crc32c} carries it.
     * @param buffer holding the bytes
     * @param offset of the first
     * @param length how many
     */
    public static long crc32c(final DirectBuffer buffer, final int offset, final int length) {
        final CRC32C crc = new CRC32C();
        update(crc, buffer, offset, length, new byte[MAX_RECORD_LENGTH]);
        return crc.getValue();
    }

    /**
     * Feeds a span of bytes into a running CRC-32C, whatever memory backs them.
     * @param crc     the running CRC
     * @param buffer  holding the bytes
     * @param offset  of the first
     * @param length  how many
     * @param scratch what off-heap bytes are copied through, so a caller's steady state allocates nothing
     */
    public static void update(final CRC32C crc, final DirectBuffer buffer, final int offset, final int length,
                              final byte[] scratch) {
        final byte[] array = buffer.byteArray();
        if (array != null) {
            crc.update(array, (int)buffer.wrapAdjustment() + offset, length);
            return;
        }
        for (int done = 0; done < length;) {
            final int count = Math.min(scratch.length, length - done);
            buffer.getBytes(offset + done, scratch, 0, count);
            crc.update(scratch, 0, count);
            done += count;
        }
    }
}
