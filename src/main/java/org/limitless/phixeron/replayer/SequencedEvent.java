package org.limitless.phixeron.replayer;

import org.agrona.DirectBuffer;
import org.limitless.phixeron.sbe.sequenced.Origin;

/**
 * One frame delivered in order off the sequenced stream, live or replayed. The Java twin of the C++
 * {@code SequencedEvent} struct in {@code sequencer/ClusterStreamReceiver.hpp}.
 *
 * <p><b>A flyweight, reused per dispatch.</b> {@link #buffer()} points into the subscription's term
 * buffer (or, for a frame drained from the retained-ahead FIFO, into that FIFO's storage), so both the
 * buffer contents and this object's fields are valid only for the duration of the handler call. A
 * consumer that needs a frame afterwards must copy it.
 */
public final class SequencedEvent {
    private long globalSeqNo;
    private int sourceId;
    private int connectionId;
    private long sourceSessionId;
    private long clusterTimestamp;
    private long receiveTimeNs;
    private Origin origin;
    private int templateId;
    private int blockLength;
    private int version;
    private DirectBuffer buffer;
    private int offset;
    private int length;
    private long position;

    /** Cluster-wide monotone sequence number; increments by exactly one per frame. */
    public long globalSeqNo() {
        return globalSeqNo;
    }

    /** Publishing gateway process ({@code header.sourceId}). */
    public int sourceId() {
        return sourceId;
    }

    /** Connection at that gateway ({@code header.connectionId}); routes the reply. */
    public int connectionId() {
        return connectionId;
    }

    /** Aeron Cluster client session the frame was submitted on ({@code header.sessionId}). */
    public long sourceSessionId() {
        return sourceSessionId;
    }

    /** Cluster consensus time (ms) at which the frame was committed. */
    public long clusterTimestamp() {
        return clusterTimestamp;
    }

    /** Wall-clock ns at receipt by this client. */
    public long receiveTimeNs() {
        return receiveTimeNs;
    }

    /** Which producer role emitted the frame ({@code header.origin}); see the {@code Origin} enum. */
    public Origin origin() {
        return origin;
    }

    /** Outer {@code messageHeader} templateId; picks the specific decode. */
    public int templateId() {
        return templateId;
    }

    /** Outer {@code messageHeader} blockLength; pass straight to a decoder's {@code wrap}. */
    public int blockLength() {
        return blockLength;
    }

    /** Outer {@code messageHeader} version; pass straight to a decoder's {@code wrap}. */
    public int version() {
        return version;
    }

    /** Buffer holding the raw sbe-sequenced frame; valid only during the handler call. */
    public DirectBuffer buffer() {
        return buffer;
    }

    /** Offset of the outer {@code messageHeader} within {@link #buffer()}. */
    public int offset() {
        return offset;
    }

    /** Total frame length in bytes. */
    public int length() {
        return length;
    }

    /** Recording/stream position of this frame's first byte — what a resumed replay is anchored on. */
    public long position() {
        return position;
    }

    void set(final long globalSeqNo, final int sourceId, final int connectionId, final long sourceSessionId,
             final long clusterTimestamp, final long receiveTimeNs, final Origin origin, final int templateId,
             final int blockLength, final int version, final DirectBuffer buffer, final int offset, final int length,
             final long position) {
        this.globalSeqNo = globalSeqNo;
        this.sourceId = sourceId;
        this.connectionId = connectionId;
        this.sourceSessionId = sourceSessionId;
        this.clusterTimestamp = clusterTimestamp;
        this.receiveTimeNs = receiveTimeNs;
        this.origin = origin;
        this.templateId = templateId;
        this.blockLength = blockLength;
        this.version = version;
        this.buffer = buffer;
        this.offset = offset;
        this.length = length;
        this.position = position;
    }
}
