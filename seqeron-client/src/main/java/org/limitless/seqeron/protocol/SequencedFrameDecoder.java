package org.limitless.seqeron.protocol;

import org.agrona.DirectBuffer;
import org.limitless.seqeron.sbe.frame.ClusterHeartbeatDecoder;
import org.limitless.seqeron.sbe.frame.GatewayActiveDecoder;
import org.limitless.seqeron.sbe.frame.LeadershipChangedDecoder;
import org.limitless.seqeron.sbe.frame.MessageHeaderDecoder;
import org.limitless.seqeron.sbe.frame.SequencedDecoder;
import org.limitless.seqeron.sbe.frame.SequencedHeaderDecoder;
import org.limitless.seqeron.sbe.frame.SequencedSystemDecoder;
import org.limitless.seqeron.sbe.frame.SequencedSystemHeaderDecoder;

/**
 * One frame off the tap with the envelope stripped — the Java twin of {@code FrameView} in
 * {@code protocol/SequencedFrame.hpp}; keep the two in step. {@link #isSystem()} says which family: an
 * application frame dispatches on {@code (payloadId, templateId)}, never templateId alone, and a system
 * frame on {@link #systemEventType()}.
 *
 * <p>A flyweight: {@link #buffer()} points into the caller's storage and every field is valid only until
 * the next {@link #wrap}.
 */
public final class SequencedFrameDecoder {
    /** A decoder holding no frame; {@link #wrap} points it at one. */
    public SequencedFrameDecoder() {
    }

    private final MessageHeaderDecoder messageHeader = new MessageHeaderDecoder();
    private final MessageHeaderDecoder payloadHeader = new MessageHeaderDecoder();
    private final SequencedHeaderDecoder frameHeader = new SequencedHeaderDecoder();
    private final SequencedSystemHeaderDecoder systemHeader = new SequencedSystemHeaderDecoder();

    private boolean system;
    private int payloadId;
    private int systemEventType;
    private int sourceId;
    private int connectionId;
    private long sourceSessionId;
    private long globalSeqNo;
    private long clusterTimestampNs;
    private int templateId;
    private int blockLength;
    private int version;
    private DirectBuffer buffer;
    private int payloadOffset;
    private int payloadLength;

    /**
     * Reads one fragment, stripping the envelope. {@code false} means the fragment is too short to be a
     * frame — a damaged recording — and the caller drops it.
     *
     * @return whether the fragment was readable as a frame
     */
    public boolean wrap(final DirectBuffer source, final int offset, final int length) {
        buffer = source;
        if (length < MessageHeaderDecoder.ENCODED_LENGTH) {
            return false;
        }
        messageHeader.wrap(source, offset);
        if (messageHeader.schemaId() != MessageHeaderDecoder.SCHEMA_ID) {
            return false;
        }
        final int blockOffset = offset + MessageHeaderDecoder.ENCODED_LENGTH;
        final int frameTemplateId = messageHeader.templateId();
        if (frameTemplateId == SequencedDecoder.TEMPLATE_ID) {
            return wrapPayload(source, offset, length, blockOffset);
        }
        if (frameTemplateId == SequencedSystemDecoder.TEMPLATE_ID) {
            return wrapSystemBody(source, offset, length, blockOffset);
        }
        if (frameTemplateId == ClusterHeartbeatDecoder.TEMPLATE_ID ||
            frameTemplateId == LeadershipChangedDecoder.TEMPLATE_ID ||
            frameTemplateId == GatewayActiveDecoder.TEMPLATE_ID) {
            return wrapSynthesized(source, offset, length, blockOffset);
        }
        return false;
    }

    /** The application family: one opaque payload carrying its own {@code MessageHeader}. */
    private boolean wrapPayload(final DirectBuffer source, final int offset, final int length,
                                final int blockOffset) {
        final int prefixOffset = blockOffset + SequencedHeaderDecoder.ENCODED_LENGTH;
        if (prefixOffset + SequencedDecoder.payloadHeaderLength() > offset + length) {
            return false;
        }
        system = false;
        frameHeader.wrap(source, blockOffset);
        payloadId = frameHeader.payloadId();
        systemEventType = 0;
        readIdentity(frameHeader.sourceId(), frameHeader.connectionId(), frameHeader.sessionId(),
                     frameHeader.globalSeqNo(), frameHeader.timestamp());

        payloadLength = source.getShort(prefixOffset, java.nio.ByteOrder.LITTLE_ENDIAN) & 0xFFFF;
        payloadOffset = prefixOffset + SequencedDecoder.payloadHeaderLength();
        if (payloadOffset + payloadLength > offset + length) {
            return false; // a truncated payload: the recording itself is damaged
        }
        // A payload too short for a MessageHeader is still a frame (§5, §13.2) and must reach the consumer
        // (P-3); the three stay 0, which no (payloadId, templateId) dispatch matches.
        if (payloadLength < MessageHeaderDecoder.ENCODED_LENGTH) {
            templateId = 0;
            blockLength = 0;
            version = 0;
            return true;
        }
        payloadHeader.wrap(source, payloadOffset);
        templateId = payloadHeader.templateId();
        blockLength = payloadHeader.blockLength();
        version = payloadHeader.version();
        return true;
    }

    /**
     * A submitted system event: its payload has no header, so a consumer decodes with compiled constants
     * (<b>V-3</b>).
     */
    private boolean wrapSystemBody(final DirectBuffer source, final int offset, final int length,
                                   final int blockOffset) {
        final int prefixOffset = blockOffset + SequencedSystemHeaderDecoder.ENCODED_LENGTH;
        if (prefixOffset + SequencedSystemDecoder.bodyHeaderLength() > offset + length) {
            return false;
        }
        readSystemHeader(source, blockOffset);
        payloadLength = source.getShort(prefixOffset, java.nio.ByteOrder.LITTLE_ENDIAN) & 0xFFFF;
        payloadOffset = prefixOffset + SequencedSystemDecoder.bodyHeaderLength();
        return payloadOffset + payloadLength <= offset + length;
    }

    /** One of the three synthesized events: no payload, its fields inline in the frame's own block. */
    private boolean wrapSynthesized(final DirectBuffer source, final int offset, final int length,
                                    final int blockOffset) {
        if (blockOffset + SequencedSystemHeaderDecoder.ENCODED_LENGTH > offset + length) {
            return false;
        }
        readSystemHeader(source, blockOffset);
        blockLength = messageHeader.blockLength();
        version = messageHeader.version();
        payloadOffset = blockOffset;
        payloadLength = length - MessageHeaderDecoder.ENCODED_LENGTH;
        return true;
    }

    private void readSystemHeader(final DirectBuffer source, final int blockOffset) {
        system = true;
        systemHeader.wrap(source, blockOffset);
        systemEventType = systemHeader.systemEventType();
        payloadId = 0;
        templateId = 0;
        blockLength = 0;
        version = 0;
        readIdentity(systemHeader.sourceId(), systemHeader.connectionId(), systemHeader.sessionId(),
                     systemHeader.globalSeqNo(), systemHeader.timestamp());
    }

    private void readIdentity(final int frameSourceId, final int frameConnectionId, final long frameSessionId,
                              final long frameGlobalSeqNo, final long frameTimestamp) {
        sourceId = frameSourceId;
        connectionId = frameConnectionId;
        sourceSessionId = frameSessionId;
        globalSeqNo = frameGlobalSeqNo;
        clusterTimestampNs = frameTimestamp;
    }

    /** Whether this frame is one of the four system messages; if so {@link #payloadId()} means nothing. */
    public boolean isSystem() {
        return system;
    }

    /** Which protocol {@link #templateId()} belongs to; 0 on a system frame. */
    public int payloadId() {
        return payloadId;
    }

    /** Which of §7's twelve events this frame carries; 0 on an application frame. */
    public int systemEventType() {
        return systemEventType;
    }

    /** Publishing producer process ({@code header.sourceId}); −1 on a frame the cluster synthesized. */
    public int sourceId() {
        return sourceId;
    }

    /** Connection at that producer ({@code header.connectionId}); routes the reply. */
    public int connectionId() {
        return connectionId;
    }

    /** Cluster session the frame was submitted on ({@code header.sessionId}), as the sequencer stamped it. */
    public long sourceSessionId() {
        return sourceSessionId;
    }

    /** Cluster-wide monotone sequence number; increments by exactly one per frame. */
    public long globalSeqNo() {
        return globalSeqNo;
    }

    /** Cluster consensus time (epoch ns) at which the frame was committed ({@code header.timestamp}). */
    public long clusterTimestampNs() {
        return clusterTimestampNs;
    }

    /** The payload's own templateId, never the envelope's; 0 on a system frame. */
    public int templateId() {
        return templateId;
    }

    /**
     * What to wrap a decoder over {@link #payloadOffset()} with: the payload's own on an application frame,
     * the frame's own on a synthesized one, and 0 on a submitted system frame (use compiled constants).
     */
    public int blockLength() {
        return blockLength;
    }

    /** See {@link #blockLength()}. */
    public int version() {
        return version;
    }

    /** Buffer the frame sits in; valid only while the frame it was wrapped over is. */
    public DirectBuffer buffer() {
        return buffer;
    }

    /**
     * Offset within {@link #buffer()} of what a consumer decodes: the payload, its own 8-byte
     * {@code MessageHeader} included, on an application frame; the payload on a submitted system frame; the
     * frame's own block on one of the synthesized three.
     */
    public int payloadOffset() {
        return payloadOffset;
    }

    /** Length in bytes of what {@link #payloadOffset()} addresses; the envelope is not in it. */
    public int payloadLength() {
        return payloadLength;
    }
}
