package org.limitless.seqeron.replayer.client;

import java.util.List;
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

/** A snapshot as a restore meets it (doc/snapshot.md §7): the instance's own file and the round's frames. */
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

    /** A {@code SnapshotStarted} on a queue that carries every frame, so its groupSeqNo is its clusterSeqNo. */
    static Frame started(final long clusterSeqNo, final long round) {
        return started(clusterSeqNo, clusterSeqNo, round, clusterSeqNo * 1000);
    }

    /** A {@code SnapshotStarted} stamped {@code timestampNs}: another log's, when it is not the file's. */
    static Frame started(final long clusterSeqNo, final long round, final long timestampNs) {
        return started(clusterSeqNo, clusterSeqNo, round, timestampNs);
    }

    /** A {@code SnapshotStarted} at {@code groupSeqNo} on its queue. */
    static Frame started(final long clusterSeqNo, final long groupSeqNo, final long round, final long timestampNs) {
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[128]);
        final SnapshotStartedEncoder encoder = new SnapshotStartedEncoder();
        encoder.wrapAndApplyHeader(buffer, 0, new MessageHeaderEncoder());
        encoder.header()
            .sourceId(-1)
            .connectionId(-1)
            .sessionId(-1)
            .systemEventType(SystemFrame.SNAPSHOT_STARTED)
            .clusterSeqNo(clusterSeqNo)
            .timestamp(timestampNs)
            .groupSeqNo(groupSeqNo);
        encoder.round(round);
        return new Frame(buffer, MessageHeaderEncoder.ENCODED_LENGTH + encoder.encodedLength());
    }

    /** The end the records {@code records} match, on a queue that carries every frame. */
    static Frame end(final long clusterSeqNo, final int sourceId, final long round, final int formatVersion,
                     final byte[]... records) {
        return end(clusterSeqNo, clusterSeqNo, sourceId, round, formatVersion, records);
    }

    /** The same at {@code groupSeqNo} on its queue. */
    static Frame end(final long clusterSeqNo, final long groupSeqNo, final int sourceId, final long round,
                     final int formatVersion, final byte[]... records) {
        final Digest digest = Digest.of(records);
        final MutableDirectBuffer body = new ExpandableArrayBuffer(64);
        final SnapshotEndEncoder encoder = new SnapshotEndEncoder();
        encoder.wrap(body, 0).round(round).recordCount(digest.recordCount()).length(digest.length())
            .crc32c(digest.crc32c()).formatVersion(formatVersion);
        return system(clusterSeqNo, groupSeqNo, sourceId, SystemFrame.SNAPSHOT_END, body, encoder.encodedLength());
    }

    /**
     * Writes {@code records} as round {@code round}'s file and confirms it, as the instance did when it serialized
     * them and the log's end agreed. The cut's timestamp is {@code cutClusterSeqNo * 1000}, as every frame's here.
     */
    static void write(final SnapshotStore store, final long round, final long cutClusterSeqNo, final long cutPosition,
                      final int formatVersion, final byte[]... records) {
        final Digest digest = Digest.of(records);
        store.begin(round, cutClusterSeqNo, cutPosition, cutClusterSeqNo * 1000);
        for (final byte[] record : records) {
            store.append(new UnsafeBuffer(record), 0, record.length);
        }
        store.commit(digest.recordCount(), digest.length(), digest.crc32c(), formatVersion);
        store.confirm(round);
        store.awaitWrites();
    }

    /** An application's header record. */
    static byte[] header(final long leadershipTermId, final int leaderMemberId) {
        final SnapshotHeader header = new SnapshotHeader(leadershipTermId, leaderMemberId, List.of(), null);
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

    private static Frame system(final long clusterSeqNo, final long groupSeqNo, final int sourceId,
                                final int systemEventType, final MutableDirectBuffer body, final int bodyLength) {
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[256]);
        final SequencedSystemEncoder encoder = new SequencedSystemEncoder();
        encoder.wrapAndApplyHeader(buffer, 0, new MessageHeaderEncoder());
        encoder.header()
            .sourceId(sourceId)
            .connectionId(-1)
            .sessionId(5)
            .systemEventType(systemEventType)
            .clusterSeqNo(clusterSeqNo)
            .timestamp(clusterSeqNo * 1000)
            .groupSeqNo(groupSeqNo);
        encoder.putBody(body, 0, bodyLength);
        return new Frame(buffer, MessageHeaderEncoder.ENCODED_LENGTH + encoder.encodedLength());
    }
}
