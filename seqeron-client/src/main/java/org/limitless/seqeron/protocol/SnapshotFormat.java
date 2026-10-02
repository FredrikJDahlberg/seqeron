package org.limitless.seqeron.protocol;

import java.util.zip.CRC32C;
import org.agrona.DirectBuffer;
import org.limitless.seqeron.sbe.frame.SnapshotChunkEncoder;

/**
 * How a snapshot crosses the log (doc/snapshot.md §2): a sequence of records, each carried whole by one
 * {@code SnapshotChunk}, the façade's header first, and checked by the CRC-32C of their bytes in order that
 * its {@code SnapshotEnd} carries. The C++ twin is {@code protocol/Snapshot.hpp}; keep the two in step.
 */
public final class SnapshotFormat {
    /** The most bytes one record carries: the payload ceiling less the chunk's block and length prefix. */
    public static final int MAX_RECORD_LENGTH =
        FrameLayer.MAX_PAYLOAD_LENGTH - SnapshotChunkEncoder.BLOCK_LENGTH - SnapshotChunkEncoder.dataHeaderLength();

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
        update(crc, buffer, offset, length);
        return crc.getValue();
    }

    /**
     * Feeds a span of bytes into a running CRC-32C, whatever memory backs them.
     * @param crc    the running CRC
     * @param buffer holding the bytes
     * @param offset of the first
     * @param length how many
     */
    public static void update(final CRC32C crc, final DirectBuffer buffer, final int offset, final int length) {
        final byte[] array = buffer.byteArray();
        if (array != null) {
            crc.update(array, (int)buffer.wrapAdjustment() + offset, length);
            return;
        }
        final byte[] copy = new byte[length];
        buffer.getBytes(offset, copy);
        crc.update(copy);
    }
}
