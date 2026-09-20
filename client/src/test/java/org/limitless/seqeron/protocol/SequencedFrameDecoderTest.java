package org.limitless.seqeron.protocol;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.limitless.seqeron.sbe.frame.ClusterHeartbeatEncoder;
import org.limitless.seqeron.sbe.frame.MessageHeaderEncoder;
import org.limitless.seqeron.sbe.frame.SequencedDecoder;
import org.limitless.seqeron.sbe.frame.SequencedEncoder;
import org.limitless.seqeron.sbe.frame.SequencedHeaderDecoder;
import org.limitless.seqeron.sbe.frame.SequencedSystemDecoder;
import org.limitless.seqeron.sbe.frame.SequencedSystemEncoder;
import org.limitless.seqeron.sbe.frame.SequencedSystemHeaderDecoder;

/**
 * What the decoder does with a fragment that is not a whole frame. The sequencer validates every frame
 * on ingress and the recording is what it wrote, so a rejection here means the recording itself is
 * damaged — but the read must fail as a {@code false}, never as an exception or a read past the
 * fragment: {@code Image.poll} hands whatever a fragment handler throws to the Aeron error handler and
 * advances the subscriber position anyway, so a throw here loses the frame silently.
 *
 * <p>The C++ twin is {@code SequencedFrameTest.cpp} over {@code unwrapFrame}, case for case in the same
 * order, so a divergence shows up as a missing case rather than as a decode failure on a live tap. The
 * one case with no twin is the offset one: {@code unwrapFrame} takes a pointer, so its caller offsets.
 */
class SequencedFrameDecoderTest {
    private static final int PAYLOAD_ID = 2;
    private static final int SOURCE_ID = 7;
    private static final int CONNECTION_ID = 42;
    private static final long SESSION_ID = 0x5EE5_1000L;
    private static final long TIMESTAMP = 1_700_000_000_000_000_000L;

    /** Offsets inside the outer {@code MessageHeader}. */
    private static final int TEMPLATE_ID_OFFSET = 2;
    private static final int SCHEMA_ID_OFFSET = 4;

    /** Offset of the body's length prefix: past the framing header and the 34-byte composite. */
    private static final int PREFIX_OFFSET =
        MessageHeaderEncoder.ENCODED_LENGTH + SequencedHeaderDecoder.ENCODED_LENGTH;

    private final SequencedFrameDecoder view = new SequencedFrameDecoder();

    @Test
    @DisplayName("a fragment shorter than a MessageHeader is not a frame")
    void shorterThanAMessageHeader() {
        final byte[] frame = payloadFrame(1, 8);
        for (int length = 0; length < MessageHeaderEncoder.ENCODED_LENGTH; length++) {
            assertFalse(wrap(Arrays.copyOf(frame, length)), length + " bytes cannot name a template");
        }
    }

    @Test
    @DisplayName("a foreign schemaId is not a frame, whatever its templateId says")
    void foreignSchemaId() {
        final byte[] frame = payloadFrame(1, 8);
        final MutableDirectBuffer damaged = new UnsafeBuffer(frame);
        damaged.putShort(SCHEMA_ID_OFFSET, (short)(MessageHeaderEncoder.SCHEMA_ID + 1), ByteOrder.LITTLE_ENDIAN);
        assertFalse(wrap(frame), "template ids are unique per schema, so the schema has to match first");
    }

    @Test
    @DisplayName("an unknown templateId in seqeron's own schema is not a frame")
    void unknownTemplateId() {
        final byte[] frame = payloadFrame(1, 8);
        final MutableDirectBuffer damaged = new UnsafeBuffer(frame);
        damaged.putShort(TEMPLATE_ID_OFFSET, (short)999, ByteOrder.LITTLE_ENDIAN);
        assertFalse(wrap(frame), "only the five sequenced shapes decode");
    }

    @Test
    @DisplayName("an application frame cut before its payload prefix is rejected")
    void applicationFrameCutBeforeItsPrefix() {
        final byte[] frame = payloadFrame(1, 8);
        final int prefixEnd = PREFIX_OFFSET + SequencedDecoder.payloadHeaderLength();
        for (int length = MessageHeaderEncoder.ENCODED_LENGTH; length < prefixEnd; length++) {
            assertFalse(wrap(Arrays.copyOf(frame, length)),
                        "the payload's length is unreadable at " + length + " bytes");
        }
        assertTrue(wrap(Arrays.copyOf(frame, prefixEnd + 8)), "the whole frame still decodes");
    }

    @Test
    @DisplayName("an application payload declaring more bytes than the fragment holds is rejected")
    void applicationPayloadRunsPastTheFragment() {
        final byte[] frame = payloadFrame(1, 8);
        final MutableDirectBuffer damaged = new UnsafeBuffer(frame);
        damaged.putShort(PREFIX_OFFSET, (short)9, ByteOrder.LITTLE_ENDIAN);
        assertFalse(wrap(frame), "a payload prefix is a claim about the fragment, not a fact");
    }

    @Test
    @DisplayName("a submitted system frame cut before its body prefix is rejected")
    void systemFrameCutBeforeItsPrefix() {
        final byte[] frame = systemFrame(1, SystemFrame.CONNECTION_CLOSED, 0);
        final int prefixEnd = PREFIX_OFFSET + SequencedSystemDecoder.bodyHeaderLength();
        for (int length = MessageHeaderEncoder.ENCODED_LENGTH; length < prefixEnd; length++) {
            assertFalse(wrap(Arrays.copyOf(frame, length)), "the body's length is unreadable at " + length + " bytes");
        }
        assertTrue(wrap(frame), "an empty body is a whole frame (§5)");
    }

    @Test
    @DisplayName("a system body declaring more bytes than the fragment holds is rejected")
    void systemBodyRunsPastTheFragment() {
        final byte[] frame = systemFrame(1, SystemFrame.CLUSTER_STARTED, 8);
        final MutableDirectBuffer damaged = new UnsafeBuffer(frame);
        damaged.putShort(PREFIX_OFFSET, (short)9, ByteOrder.LITTLE_ENDIAN);
        assertFalse(wrap(frame));
    }

    @Test
    @DisplayName("a synthesized frame cut inside its header composite is rejected")
    void synthesizedFrameCutInsideItsHeader() {
        final byte[] frame = clusterHeartbeatFrame(1);
        final int headerEnd = MessageHeaderEncoder.ENCODED_LENGTH + SequencedSystemHeaderDecoder.ENCODED_LENGTH;
        for (int length = MessageHeaderEncoder.ENCODED_LENGTH; length < headerEnd; length++) {
            assertFalse(wrap(Arrays.copyOf(frame, length)),
                        "globalSeqNo is not readable at " + length + " bytes, so P-3 cannot count");
        }
        assertTrue(wrap(frame));
    }

    /**
     * The property behind the cases above, swept over every shape and every cut. The fragment handed to
     * {@link SequencedFrameDecoder#wrap} is a copy of exactly that length, so Agrona's own bounds checks
     * fire on any read past its end.
     */
    @Test
    @DisplayName("every truncation of every shape is rejected or stays inside the fragment")
    void everyTruncationIsRejectedOrStaysInsideTheFragment() {
        for (final Map.Entry<String, byte[]> shape : everyShape().entrySet()) {
            final byte[] whole = shape.getValue();
            assertTrue(wrap(whole), shape.getKey() + " does not decode whole");
            for (int length = 0; length < whole.length; length++) {
                final byte[] cut = Arrays.copyOf(whole, length);
                final String where = shape.getKey() + " cut to " + length + " of " + whole.length + " bytes";
                if (wrap(cut)) {
                    assertTrue(view.payloadOffset() + view.payloadLength() <= length,
                               where + " decodes with a payload running past the fragment");
                }
            }
        }
    }

    /** Java only: {@code unwrapFrame} takes a pointer, so its caller does the offsetting. */
    @Test
    @DisplayName("the bounds are taken from the fragment, not from the buffer behind it")
    void boundsAreRelativeToTheFragment() {
        final byte[] frame = payloadFrame(1, 8);
        final MutableDirectBuffer batch = new ExpandableArrayBuffer(frame.length * 2);
        final int offset = frame.length;
        batch.putBytes(offset, frame);

        assertTrue(view.wrap(batch, offset, frame.length));
        assertFalse(view.wrap(batch, offset, frame.length - 1),
                    "the bytes past the fragment belong to the next one, not to this payload");
    }

    private boolean wrap(final byte[] fragment) {
        return view.wrap(new UnsafeBuffer(fragment), 0, fragment.length);
    }

    private static Map<String, byte[]> everyShape() {
        final Map<String, byte[]> shapes = new LinkedHashMap<>();
        shapes.put("an empty payload", payloadFrame(1, 0));
        shapes.put("a payload shorter than a MessageHeader", payloadFrame(2, 3));
        shapes.put("a payload naming its own message", payloadFrame(3, 24));
        shapes.put("a system event with no body", systemFrame(4, SystemFrame.CONNECTION_CLOSED, 0));
        shapes.put("a system event with a body", systemFrame(5, SystemFrame.CLUSTER_STARTED, 8));
        shapes.put("a synthesized ClusterHeartbeat", clusterHeartbeatFrame(6));
        return shapes;
    }

    private static byte[] payloadFrame(final long globalSeqNo, final int payloadLength) {
        final MutableDirectBuffer frame = new ExpandableArrayBuffer(payloadLength + 64);
        final SequencedEncoder encoder = new SequencedEncoder();
        encoder.wrapAndApplyHeader(frame, 0, new MessageHeaderEncoder());
        encoder.header().sourceId(SOURCE_ID).connectionId(CONNECTION_ID).sessionId(SESSION_ID).payloadId(PAYLOAD_ID)
            .globalSeqNo(globalSeqNo).timestamp(TIMESTAMP);
        encoder.putPayload(new UnsafeBuffer(filler(payloadLength)), 0, payloadLength);
        return copy(frame, MessageHeaderEncoder.ENCODED_LENGTH + encoder.encodedLength());
    }

    private static byte[] systemFrame(final long globalSeqNo, final int systemEventType, final int bodyLength) {
        final MutableDirectBuffer frame = new ExpandableArrayBuffer(bodyLength + 64);
        final SequencedSystemEncoder encoder = new SequencedSystemEncoder();
        encoder.wrapAndApplyHeader(frame, 0, new MessageHeaderEncoder());
        encoder.header().sourceId(SOURCE_ID).connectionId(CONNECTION_ID).sessionId(SESSION_ID)
            .systemEventType(systemEventType).globalSeqNo(globalSeqNo).timestamp(TIMESTAMP);
        encoder.putBody(new UnsafeBuffer(filler(bodyLength)), 0, bodyLength);
        return copy(frame, MessageHeaderEncoder.ENCODED_LENGTH + encoder.encodedLength());
    }

    private static byte[] clusterHeartbeatFrame(final long globalSeqNo) {
        final MutableDirectBuffer frame = new ExpandableArrayBuffer(64);
        final ClusterHeartbeatEncoder encoder = new ClusterHeartbeatEncoder();
        encoder.wrapAndApplyHeader(frame, 0, new MessageHeaderEncoder());
        encoder.header().sourceId(-1).connectionId(-1).sessionId(-1)
            .systemEventType(SystemFrame.CLUSTER_HEARTBEAT).globalSeqNo(globalSeqNo).timestamp(TIMESTAMP);
        return copy(frame, MessageHeaderEncoder.ENCODED_LENGTH + encoder.encodedLength());
    }

    private static byte[] filler(final int length) {
        final byte[] bytes = new byte[length];
        for (int i = 0; i < length; i++) {
            bytes[i] = (byte)(i * 31 + 7);
        }
        return bytes;
    }

    private static byte[] copy(final MutableDirectBuffer source, final int length) {
        final byte[] bytes = new byte[length];
        source.getBytes(0, bytes);
        return bytes;
    }
}
