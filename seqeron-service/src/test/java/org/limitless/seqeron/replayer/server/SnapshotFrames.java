package org.limitless.seqeron.replayer.server;

import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.limitless.seqeron.protocol.SnapshotFormat;
import org.limitless.seqeron.protocol.SystemFrame;
import org.limitless.seqeron.sbe.frame.MessageHeaderEncoder;
import org.limitless.seqeron.sbe.frame.SequencedSystemEncoder;
import org.limitless.seqeron.sbe.frame.SnapshotChunkEncoder;
import org.limitless.seqeron.sbe.frame.SnapshotEndEncoder;
import org.limitless.seqeron.sbe.frame.SnapshotStartedEncoder;

/** Snapshot frames as they sit on the tap, for the index's tests: each one its {@code globalSeqNo}. */
final class SnapshotFrames {
    private final MutableDirectBuffer frame = new ExpandableArrayBuffer(2048);
    private final MutableDirectBuffer body = new ExpandableArrayBuffer(2048);
    private long globalSeqNo;

    /** The next frame's {@code globalSeqNo}. */
    long nextGlobalSeqNo() {
        return globalSeqNo + 1;
    }

    byte[] started(final long round) {
        final SnapshotStartedEncoder encoder = new SnapshotStartedEncoder();
        encoder.wrapAndApplyHeader(frame, 0, new MessageHeaderEncoder());
        encoder.header().sourceId(-1).connectionId(-1).sessionId(-1).systemEventType(SystemFrame.SNAPSHOT_STARTED)
            .globalSeqNo(++globalSeqNo).timestamp(0);
        encoder.round(round);
        return copy(MessageHeaderEncoder.ENCODED_LENGTH + encoder.encodedLength());
    }

    byte[] chunk(final int sourceId, final long round, final int chunkIndex, final byte[] record) {
        final SnapshotChunkEncoder encoder = new SnapshotChunkEncoder();
        encoder.wrap(body, 0).round(round).chunkIndex(chunkIndex).putData(record, 0, record.length);
        return system(sourceId, SystemFrame.SNAPSHOT_CHUNK, encoder.encodedLength());
    }

    byte[] end(final int sourceId, final long round, final byte[]... records) {
        long length = 0;
        final java.util.zip.CRC32C crc = new java.util.zip.CRC32C();
        for (final byte[] record : records) {
            length += record.length;
            crc.update(record);
        }
        return end(sourceId, round, records.length, length, crc.getValue());
    }

    byte[] end(final int sourceId, final long round, final int chunkCount, final long length, final long crc32c) {
        final SnapshotEndEncoder encoder = new SnapshotEndEncoder();
        encoder.wrap(body, 0).round(round).chunkCount(chunkCount).length(length).crc32c(crc32c).formatVersion(3);
        return system(sourceId, SystemFrame.SNAPSHOT_END, encoder.encodedLength());
    }

    /** One record of {@code length} bytes, each {@code fill}. */
    static byte[] record(final int length, final int fill) {
        final byte[] record = new byte[Math.min(length, SnapshotFormat.MAX_RECORD_LENGTH)];
        java.util.Arrays.fill(record, (byte)fill);
        return record;
    }

    private byte[] system(final int sourceId, final int systemEventType, final int bodyLength) {
        final SequencedSystemEncoder encoder = new SequencedSystemEncoder();
        encoder.wrapAndApplyHeader(frame, 0, new MessageHeaderEncoder());
        encoder.header().sourceId(sourceId).connectionId(-1).sessionId(5).systemEventType(systemEventType)
            .globalSeqNo(++globalSeqNo).timestamp(0);
        encoder.putBody(body, 0, bodyLength);
        return copy(MessageHeaderEncoder.ENCODED_LENGTH + encoder.encodedLength());
    }

    private byte[] copy(final int length) {
        final byte[] bytes = new byte[length];
        frame.getBytes(0, bytes);
        return bytes;
    }
}
