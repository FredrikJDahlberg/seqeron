package org.limitless.phixeron.replayer.client;

import org.agrona.DirectBuffer;
import org.limitless.phixeron.sbe.frame.ClusterHeartbeatDecoder;
import org.limitless.phixeron.sbe.frame.GatewayActiveDecoder;
import org.limitless.phixeron.sbe.frame.LeadershipChangedDecoder;
import org.limitless.phixeron.sbe.frame.MessageHeaderDecoder;
import org.limitless.phixeron.sbe.frame.SequencedDecoder;
import org.limitless.phixeron.sbe.frame.SequencedHeaderDecoder;
import org.limitless.phixeron.sbe.frame.SequencedSystemDecoder;
import org.limitless.phixeron.sbe.frame.SequencedSystemHeaderDecoder;

/**
 * One frame off the tap, unwrapped: everything a consumer dispatches on, with the envelope already
 * stripped. The Java twin of the C++ {@code FrameView} in {@code sequencer/SequencedFrame.hpp}; keep the
 * two in step.
 *
 * <p>Every fragment on the tap is one of five shapes (doc/seqeron-protocol-spec.md §4), and the
 * discriminator is the same 2-byte field at offset 16 of every one of them. A {@code Sequenced} frame
 * carries one opaque application payload named by {@link #payloadId()}; the four system shapes carry
 * seqeron's own vocabulary, named by {@link #systemEventType()}. {@link #isSystem()} says which.
 *
 * <p>Stripping the envelope <b>here, once</b>, is the point: every consumer then dispatches on
 * {@code (payloadId, templateId)} for an application frame — never templateId alone, which is unique per
 * schema only — and on {@code systemEventType} for a system one.
 *
 * <p>A flyweight, reused per frame: {@link #buffer()} points into the caller's storage and every field is
 * valid only until the next {@link #wrap}.
 */
public final class SequencedFrameDecoder {
    private final MessageHeaderDecoder messageHeader = new MessageHeaderDecoder();
    private final MessageHeaderDecoder payloadHeader = new MessageHeaderDecoder();
    private final SequencedHeaderDecoder frameHeader = new SequencedHeaderDecoder();
    private final SequencedSystemHeaderDecoder systemHeader = new SequencedSystemHeaderDecoder();

    private boolean system;
    private int payloadId;
    private int systemEventType;
    private int sourceId;
    private int connectionId;
    private long sessionId;
    private long globalSeqNo;
    private long timestamp;
    private int templateId;
    private int blockLength;
    private int version;
    private DirectBuffer buffer;
    private int payloadOffset;
    private int payloadLength;

    /**
     * Reads one fragment, stripping the envelope.
     *
     * <p>Bounds are checked because a short fragment would otherwise be read past its end. It should not
     * happen — the sequencer validates every frame on ingress and the recording is what it wrote — so
     * {@code false} here means the recording itself is damaged, and the caller drops the frame.
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

    /** The application family: the body is one opaque payload carrying its own {@code MessageHeader}. */
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
        if (payloadLength < MessageHeaderDecoder.ENCODED_LENGTH || payloadOffset + payloadLength > offset + length) {
            return false; // an empty or truncated payload names no message to dispatch on
        }
        payloadHeader.wrap(source, payloadOffset);
        templateId = payloadHeader.templateId();
        blockLength = payloadHeader.blockLength();
        version = payloadHeader.version();
        return true;
    }

    /**
     * A submitted system event: the body is its own SBE block with no framing of its own, so there is no
     * inner header to read and a consumer supplies {@code BLOCK_LENGTH} and {@code SCHEMA_VERSION} from
     * its own compiled constants (<b>V-3</b>).
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

    /**
     * One of the three the sequencer synthesizes: no body at all, its fields inline in the frame's own
     * block. {@link #payloadOffset()} is that block, so a consumer wraps the frame's own decoder over it
     * with {@link #blockLength()} and {@link #version()}.
     */
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
        sessionId = frameSessionId;
        globalSeqNo = frameGlobalSeqNo;
        timestamp = frameTimestamp;
    }

    /** Whether this frame is one of the four system shapes; if so {@link #payloadId()} means nothing. */
    public boolean isSystem() {
        return system;
    }

    /** Which protocol {@link #templateId()} belongs to; 0 on a system frame. */
    public int payloadId() {
        return payloadId;
    }

    /** Which of §7's eleven events this frame carries; 0 on an application frame. */
    public int systemEventType() {
        return systemEventType;
    }

    public int sourceId() {
        return sourceId;
    }

    public int connectionId() {
        return connectionId;
    }

    public long sessionId() {
        return sessionId;
    }

    public long globalSeqNo() {
        return globalSeqNo;
    }

    public long timestamp() {
        return timestamp;
    }

    /** The payload's own templateId, never the envelope's; 0 on a system frame. */
    public int templateId() {
        return templateId;
    }

    /**
     * What to wrap a decoder over {@link #payloadOffset()} with: the payload's own on an application
     * frame, the frame's own on one of the synthesized three, and 0 on a submitted system frame — whose
     * body carries no declaration, so its decoder's compiled constants are the only ones there are.
     */
    public int blockLength() {
        return blockLength;
    }

    /** See {@link #blockLength()}. */
    public int version() {
        return version;
    }

    public DirectBuffer buffer() {
        return buffer;
    }

    /**
     * Offset within {@link #buffer()} of what a consumer decodes: the payload, its own 8-byte
     * {@code MessageHeader} included, on an application frame; the body on a submitted system frame; the
     * frame's own block on one of the synthesized three.
     */
    public int payloadOffset() {
        return payloadOffset;
    }

    public int payloadLength() {
        return payloadLength;
    }
}
