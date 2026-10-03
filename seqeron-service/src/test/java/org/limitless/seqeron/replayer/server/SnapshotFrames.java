package org.limitless.seqeron.replayer.server;

import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.limitless.seqeron.protocol.SystemFrame;
import org.limitless.seqeron.sbe.frame.MessageHeaderEncoder;
import org.limitless.seqeron.sbe.frame.SequencedSystemEncoder;
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

    /** A source's end of a round, of format 3. */
    byte[] end(final int sourceId, final long round, final int recordCount, final long length, final long crc32c) {
        final SnapshotEndEncoder encoder = new SnapshotEndEncoder();
        encoder.wrap(body, 0).round(round).recordCount(recordCount).length(length).crc32c(crc32c).formatVersion(3);
        return system(sourceId, SystemFrame.SNAPSHOT_END, encoder.encodedLength());
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
