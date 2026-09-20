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
    SequencedEvent() {
    }

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

    /** Which of §7's twelve events this frame carries; 0 on an application frame. */
    public int systemEventType() {
        return systemEventType;
    }

    /** The message's {@code messageHeader} templateId; picks the specific decode. */
    public int templateId() {
        return templateId;
    }

    /**
     * What to wrap a decoder over {@link #offset()} with: the payload's own on an application frame, the
     * frame's own on a synthesized system frame, and 0 on a submitted one — whose decoder's compiled
     * {@code BLOCK_LENGTH} and {@code SCHEMA_VERSION} are the only ones there are.
     */
    public int blockLength() {
        return blockLength;
    }

    /** See {@link #blockLength()}. */
    public int version() {
        return version;
    }

    /** Buffer holding the message; valid only during the handler call. */
    public DirectBuffer buffer() {
        return buffer;
    }

    /**
     * Offset within {@link #buffer()} of what a consumer decodes: the payload, its own 8-byte
     * {@code MessageHeader} included, on an application frame; the body on a submitted system frame; the
     * frame's own block on one of the synthesized three.
     */
    public int offset() {
        return offset;
    }

    /** Length in bytes of what {@link #offset()} addresses; the envelope is not in it. */
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
