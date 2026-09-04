package org.limitless.phixeron.replayer.client;

import org.agrona.DirectBuffer;

/**
 * One frame delivered in order off the sequenced stream, live or replayed. The Java twin of the C++
 * {@code SequencedEvent} struct in {@code sequencer/SequencedFrame.hpp}.
 *
 * <p><b>A flyweight, reused per dispatch.</b> {@link #buffer()} points into the subscription's term
 * buffer (or, for a frame drained from the retained-ahead FIFO, into that FIFO's storage), so both the
 * buffer contents and this object's fields are valid only for the duration of the handler call. A
 * consumer that needs a frame afterwards must copy it.
 *
 * <p><b>The envelope is already stripped.</b> {@link #templateId()} and {@link #offset()} describe the
 * message, not the frame that carried it, and {@link #payloadId()} says which protocol that templateId
 * belongs to — so a consumer dispatches on the pair without knowing which shape arrived off the wire.
 */
public final class SequencedEvent {
    private long globalSeqNo;
    private int sourceId;
    private int connectionId;
    private long sourceSessionId;
    private long clusterTimestamp;
    private long receiveTimeNs;
    private int payloadId;
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

    /**
     * Which protocol {@link #templateId()} belongs to: {@link FrameView#CORE_PAYLOAD_ID} for seqeron's own
     * payloads, {@link FrameView#NO_PAYLOAD_ID} for a bare schema-202 message. Template ids are unique only
     * within a protocol, so a consumer that matches one without checking this is reading some other
     * protocol's numbering as its own.
     */
    public int payloadId() {
        return payloadId;
    }

    /** True if this frame carries a seqeron core payload. */
    public boolean isCore() {
        return payloadId == FrameView.CORE_PAYLOAD_ID;
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
             final long clusterTimestamp, final long receiveTimeNs, final int payloadId, final int templateId,
             final int blockLength, final int version, final DirectBuffer buffer, final int offset, final int length,
             final long position) {
        this.globalSeqNo = globalSeqNo;
        this.sourceId = sourceId;
        this.connectionId = connectionId;
        this.sourceSessionId = sourceSessionId;
        this.clusterTimestamp = clusterTimestamp;
        this.receiveTimeNs = receiveTimeNs;
        this.payloadId = payloadId;
        this.templateId = templateId;
        this.blockLength = blockLength;
        this.version = version;
        this.buffer = buffer;
        this.offset = offset;
        this.length = length;
        this.position = position;
    }
}
