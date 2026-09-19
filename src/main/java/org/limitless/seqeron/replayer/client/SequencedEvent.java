package org.limitless.seqeron.replayer.client;

import org.agrona.DirectBuffer;

/**
 * One frame delivered in order off the sequenced stream, live or replayed — the Java twin of the C++
 * {@code SequencedEvent} in {@code protocol/SequencedFrame.hpp}. The envelope is stripped: split on
 * {@link #isSystem()}, then dispatch on {@code (payloadId, templateId)} or {@link #systemEventType()}. Every
 * system frame but {@code LeadershipChanged}, which has its own callback, arrives here.
 *
 * <p>A flyweight: the buffer and every field are valid only during the handler call; copy to keep.
 */
public final class SequencedEvent {
    private long globalSeqNo;
    private int sourceId;
    private int connectionId;
    private long sourceSessionId;
    private long clusterTimestampNs;
    private long receiveTimeNs;
    private boolean system;
    private int payloadId;
    private int systemEventType;
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

    /** Cluster consensus time (epoch ns) at which the frame was committed. */
    public long clusterTimestampNs() {
        return clusterTimestampNs;
    }

    /** Wall-clock ns at receipt by this client. */
    public long receiveTimeNs() {
        return receiveTimeNs;
    }

    /** Which protocol {@link #templateId()} belongs to; template ids are unique only within one. 0 on a system frame. */
    public int payloadId() {
        return payloadId;
    }

    /** True if this frame is one of §7's system shapes rather than an application payload. */
    public boolean isSystem() {
        return system;
    }

    /** Which of §7's eleven events this frame carries; 0 on an application frame. */
    public int systemEventType() {
        return systemEventType;
    }

    /** The message's {@code messageHeader} templateId; picks the specific decode. */
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

    /** Buffer holding the message; valid only during the handler call. */
    public DirectBuffer buffer() {
        return buffer;
    }

    /** Offset of the message's {@code messageHeader} within {@link #buffer()}. */
    public int offset() {
        return offset;
    }

    /** Total message length in bytes, its {@code messageHeader} included. */
    public int length() {
        return length;
    }

    /** Recording/stream position of this frame's first byte — what a resumed replay is anchored on. */
    public long position() {
        return position;
    }

    void set(final long globalSeqNo, final int sourceId, final int connectionId, final long sourceSessionId,
             final long clusterTimestampNs, final long receiveTimeNs, final boolean system, final int payloadId,
             final int systemEventType, final int templateId, final int blockLength, final int version,
             final DirectBuffer buffer, final int offset, final int length, final long position) {
        this.globalSeqNo = globalSeqNo;
        this.sourceId = sourceId;
        this.connectionId = connectionId;
        this.sourceSessionId = sourceSessionId;
        this.clusterTimestampNs = clusterTimestampNs;
        this.receiveTimeNs = receiveTimeNs;
        this.system = system;
        this.payloadId = payloadId;
        this.systemEventType = systemEventType;
        this.templateId = templateId;
        this.blockLength = blockLength;
        this.version = version;
        this.buffer = buffer;
        this.offset = offset;
        this.length = length;
        this.position = position;
    }
}
