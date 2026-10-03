package org.limitless.seqeron.replayer.client;

import java.util.zip.CRC32C;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.limitless.seqeron.protocol.SnapshotHeader;
import org.limitless.seqeron.protocol.SystemFrame;
import org.limitless.seqeron.sbe.frame.MessageHeaderEncoder;
import org.limitless.seqeron.sbe.frame.SequencedSystemEncoder;
import org.limitless.seqeron.sbe.frame.SnapshotEndEncoder;
import org.limitless.seqeron.sbe.frame.SnapshotStartedEncoder;
import org.limitless.seqeron.sbe.replay.SnapshotLocationEncoder;

/**
 * A snapshot as a restore meets it (doc/snapshot.md §5, §7): the instance's own file, the round's frames, and the
 * Replayer's answer confirming it.
 */
final class RestoreFrames {
    /** One encoded frame or control message. */
    record Frame(UnsafeBuffer buffer, int length) { }

    /** What a {@code SnapshotEnd} says of a snapshot's records. */
    record Digest(int recordCount, long length, long crc32c) {
        static Digest of(final byte[]... records) {
            long length = 0;
            final CRC32C crc = new CRC32C();
            for (final byte[] record : records) {
                length += record.length;
                crc.update(record);
            }
            return new Digest(records.length, length, crc.getValue());
        }
    }

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

    /** The end the records {@code records} match. */
    static Frame end(final long globalSeqNo, final int sourceId, final long round, final int formatVersion,
                     final byte[]... records) {
        final Digest digest = Digest.of(records);
        final MutableDirectBuffer body = new ExpandableArrayBuffer(64);
        final SnapshotEndEncoder encoder = new SnapshotEndEncoder();
        encoder.wrap(body, 0).round(round).recordCount(digest.recordCount()).length(digest.length())
            .crc32c(digest.crc32c()).formatVersion(formatVersion);
        return system(globalSeqNo, sourceId, SystemFrame.SNAPSHOT_END, body, encoder.encodedLength());
    }

    /** Writes {@code records} as round {@code round}'s file, as the instance did when it serialized them. */
    static void write(final SnapshotStore store, final long round, final int formatVersion, final byte[]... records) {
        final Digest digest = Digest.of(records);
        store.begin(round);
        for (final byte[] record : records) {
            store.append(new UnsafeBuffer(record), 0, record.length);
        }
        store.commit(digest.recordCount(), digest.length(), digest.crc32c(), formatVersion);
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

    /** The Replayer's answer: round {@code round}'s cut and sequenced end, or none for {@code round} −1. */
    static Frame location(final int clientId, final long requestId, final long round, final long asOfGlobalSeqNo,
                          final long asOfPosition, final long formatVersion, final Digest end) {
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[128]);
        final SnapshotLocationEncoder encoder = new SnapshotLocationEncoder();
        encoder.wrapAndApplyHeader(buffer, 0, new org.limitless.seqeron.sbe.replay.MessageHeaderEncoder())
            .clientId(clientId)
            .requestId(requestId)
            .round(round)
            .asOfGlobalSeqNo(asOfGlobalSeqNo)
            .asOfPosition(asOfPosition)
            .formatVersion(formatVersion)
            .recordCount(end.recordCount())
            .length(end.length())
            .crc32c(end.crc32c());
        final int headerLength = org.limitless.seqeron.sbe.replay.MessageHeaderEncoder.ENCODED_LENGTH;
        return new Frame(buffer, headerLength + encoder.encodedLength());
    }

    private static Frame system(final long globalSeqNo, final int sourceId, final int systemEventType,
                                final MutableDirectBuffer body, final int bodyLength) {
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[256]);
        final SequencedSystemEncoder encoder = new SequencedSystemEncoder();
        encoder.wrapAndApplyHeader(buffer, 0, new MessageHeaderEncoder());
        encoder.header().sourceId(sourceId).connectionId(-1).sessionId(5).systemEventType(systemEventType)
            .globalSeqNo(globalSeqNo).timestamp(globalSeqNo * 1000);
        encoder.putBody(body, 0, bodyLength);
        return new Frame(buffer, MessageHeaderEncoder.ENCODED_LENGTH + encoder.encodedLength());
    }
}
