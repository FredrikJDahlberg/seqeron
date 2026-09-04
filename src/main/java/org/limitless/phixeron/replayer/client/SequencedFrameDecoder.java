package org.limitless.phixeron.replayer.client;

import org.agrona.DirectBuffer;
import org.limitless.phixeron.sbe.frame.MessageHeaderDecoder;
import org.limitless.phixeron.sbe.frame.SequencedDecoder;
import org.limitless.phixeron.sbe.frame.SequencedHeaderDecoder;

/**
 * One frame off the tap, unwrapped: everything a consumer dispatches on, with the envelope already
 * stripped. The Java twin of the C++ {@code FrameView} in {@code sequencer/SequencedFrame.hpp}; keep the
 * two in step.
 *
 * <p>Every fragment on the tap is a {@code Sequenced} frame (schema 210) whose header carries the identity
 * and whose body is one opaque payload named by {@link #payloadId()}; the fields here describe the
 * <em>payload</em>. Stripping the envelope <b>here, once</b>, is the point: every consumer then dispatches
 * on {@code (payloadId, templateId)} — never templateId alone, which is unique per schema only.
 *
 * <p>A flyweight, reused per frame: {@link #buffer()} points into the caller's storage and every field is
 * valid only until the next {@link #wrap}.
 */
public final class SequencedFrameDecoder {
    /** The one payloadId the cluster tier owns and decodes (doc/seqeron-protocol-spec.md §6.1). */
    public static final int CORE_PAYLOAD_ID = org.limitless.phixeron.sequencer.CoreFrame.PAYLOAD_ID;

    private final MessageHeaderDecoder messageHeader = new MessageHeaderDecoder();
    private final MessageHeaderDecoder payloadHeader = new MessageHeaderDecoder();
    private final SequencedHeaderDecoder frameHeader = new SequencedHeaderDecoder();

    private int payloadId;
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
        return wrapEnvelope(source, offset, length);
    }

    private boolean wrapEnvelope(final DirectBuffer source, final int offset, final int length) {
        final int prefixOffset = offset + MessageHeaderDecoder.ENCODED_LENGTH + SequencedHeaderDecoder.ENCODED_LENGTH;
        if (messageHeader.templateId() != SequencedDecoder.TEMPLATE_ID ||
            prefixOffset + SequencedDecoder.payloadHeaderLength() > offset + length) {
            return false;
        }
        frameHeader.wrap(source, offset + MessageHeaderDecoder.ENCODED_LENGTH);
        payloadId = frameHeader.payloadId();
        sourceId = frameHeader.sourceId();
        connectionId = frameHeader.connectionId();
        sessionId = frameHeader.sessionId();
        globalSeqNo = frameHeader.globalSeqNo();
        timestamp = frameHeader.timestamp();

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

    /** Which protocol {@link #templateId()} belongs to. */
    public int payloadId() {
        return payloadId;
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

    /** The message's own templateId, never the envelope's. */
    public int templateId() {
        return templateId;
    }

    public int blockLength() {
        return blockLength;
    }

    public int version() {
        return version;
    }

    public DirectBuffer buffer() {
        return buffer;
    }

    /** Offset of the message within {@link #buffer()}, its own 8-byte {@code MessageHeader} included. */
    public int payloadOffset() {
        return payloadOffset;
    }

    public int payloadLength() {
        return payloadLength;
    }
}
