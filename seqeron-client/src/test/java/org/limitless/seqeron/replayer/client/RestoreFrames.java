package org.limitless.seqeron.replayer.client;

import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.limitless.seqeron.protocol.SnapshotFormat;
import org.limitless.seqeron.protocol.SnapshotHeader;
import org.limitless.seqeron.protocol.SystemFrame;
import org.limitless.seqeron.sbe.frame.MessageHeaderEncoder;
import org.limitless.seqeron.sbe.frame.SequencedSystemEncoder;
import org.limitless.seqeron.sbe.frame.SnapshotChunkEncoder;
import org.limitless.seqeron.sbe.frame.SnapshotEndEncoder;
import org.limitless.seqeron.sbe.frame.SnapshotStartedEncoder;
import org.limitless.seqeron.sbe.replay.SnapshotLocationEncoder;

/** A snapshot's frames as a restore replays them, and the Replayer's answer locating it (doc/snapshot.md §5, §7). */
final class RestoreFrames {
    /** One encoded frame or control message. */
    record Frame(UnsafeBuffer buffer, int length) { }

    private RestoreFrames() {
    }

    static Frame started(final long globalSeqNo, final long round) {
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[128]);
        final SnapshotStartedEncoder encoder = new SnapshotStartedEncoder();
        encoder.wrapAndApplyHeader(buffer, 0, new MessageHeaderEncoder());
        encoder.header().sourceId(-1).connectionId(-1).sessionId(-1).systemEventType(SystemFrame.SNAPSHOT_STARTED)
            .globalSeqNo(globalSeqNo).timestamp(globalSeqNo * 1000);
        encoder.round(round);
        return new Frame(buffer, MessageHeaderEncoder.ENCODED_LENGTH + encoder.encodedLength());
    }

    static Frame chunk(final long globalSeqNo, final int sourceId, final long round, final int chunkIndex,
                       final byte[] record) {
        final MutableDirectBuffer body = new ExpandableArrayBuffer(2048);
        final SnapshotChunkEncoder encoder = new SnapshotChunkEncoder();
        encoder.wrap(body, 0).round(round).chunkIndex(chunkIndex).putData(record, 0, record.length);
        return system(globalSeqNo, sourceId, SystemFrame.SNAPSHOT_CHUNK, body, encoder.encodedLength());
    }

    /** The end the records {@code records} validate against. */
    static Frame end(final long globalSeqNo, final int sourceId, final long round, final int formatVersion,
                     final byte[]... records) {
        long length = 0;
        final java.util.zip.CRC32C crc = new java.util.zip.CRC32C();
        for (final byte[] record : records) {
            length += record.length;
            crc.update(record);
        }
        return end(globalSeqNo, sourceId, round, records.length, length, crc.getValue(), formatVersion);
    }

    static Frame end(final long globalSeqNo, final int sourceId, final long round, final int chunkCount,
                     final long length, final long crc32c, final int formatVersion) {
        final MutableDirectBuffer body = new ExpandableArrayBuffer(64);
        final SnapshotEndEncoder encoder = new SnapshotEndEncoder();
        encoder.wrap(body, 0).round(round).chunkCount(chunkCount).length(length).crc32c(crc32c)
            .formatVersion(formatVersion);
        return system(globalSeqNo, sourceId, SystemFrame.SNAPSHOT_END, body, encoder.encodedLength());
    }

    /** An application's header record. */
    static byte[] header(final long leadershipTermId, final int leaderMemberId) {
        final SnapshotHeader header = new SnapshotHeader(leadershipTermId, leaderMemberId, null);
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[header.encodedLength()]);
        header.encode(buffer, 0);
        return buffer.byteArray();
    }

    /** A record holding one long. */
    static byte[] record(final long value) {
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[Long.BYTES]);
        buffer.putLong(0, value);
        return buffer.byteArray();
    }

    /** The Replayer's answer; {@code round} −1 for none. */
    static Frame location(final int clientId, final long requestId, final long round, final long asOfGlobalSeqNo,
                          final long asOfPosition, final long endPosition, final long formatVersion) {
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[128]);
        final SnapshotLocationEncoder encoder = new SnapshotLocationEncoder();
        encoder.wrapAndApplyHeader(buffer, 0, new org.limitless.seqeron.sbe.replay.MessageHeaderEncoder())
            .clientId(clientId)
            .requestId(requestId)
            .round(round)
            .asOfGlobalSeqNo(asOfGlobalSeqNo)
            .asOfPosition(asOfPosition)
            .endPosition(endPosition)
            .formatVersion(formatVersion);
        final int headerLength = org.limitless.seqeron.sbe.replay.MessageHeaderEncoder.ENCODED_LENGTH;
        return new Frame(buffer, headerLength + encoder.encodedLength());
    }

    private static Frame system(final long globalSeqNo, final int sourceId, final int systemEventType,
                                final MutableDirectBuffer body, final int bodyLength) {
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[SnapshotFormat.MAX_RECORD_LENGTH + 128]);
        final SequencedSystemEncoder encoder = new SequencedSystemEncoder();
        encoder.wrapAndApplyHeader(buffer, 0, new MessageHeaderEncoder());
        encoder.header().sourceId(sourceId).connectionId(-1).sessionId(5).systemEventType(systemEventType)
            .globalSeqNo(globalSeqNo).timestamp(globalSeqNo * 1000);
        encoder.putBody(body, 0, bodyLength);
        return new Frame(buffer, MessageHeaderEncoder.ENCODED_LENGTH + encoder.encodedLength());
    }
}
