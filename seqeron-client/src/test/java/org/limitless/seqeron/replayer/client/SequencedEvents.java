package org.limitless.seqeron.replayer.client;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.limitless.seqeron.protocol.FrameLayer;
import org.limitless.seqeron.protocol.SequencedFrameDecoder;
import org.limitless.seqeron.sbe.frame.MessageHeaderEncoder;
import org.limitless.seqeron.sbe.frame.SequencedEncoder;
import org.limitless.seqeron.sbe.frame.SequencedSystemEncoder;

/**
 * Builds a {@link SequencedEvent} from outside its package, for the tests of what consumes one. An event is
 * a view over a real tap frame, so this encodes one rather than setting fields: the same bytes the sequencer
 * would emit, read back through the same decoder.
 */
public final class SequencedEvents {
    private SequencedEvents() {
    }

    /**
     * @param id a {@code payloadId}, or a {@code systemEventType} when {@code system}
     */
    public static SequencedEvent of(final long sourceSessionId, final boolean system, final int id,
                                    final DirectBuffer body, final int offset, final int length) {
        final MutableDirectBuffer frame = new UnsafeBuffer(new byte[FrameLayer.MAX_INGRESS_LENGTH]);
        final int frameLength = system ? encodeSystem(frame, sourceSessionId, id, body, offset, length)
                                       : encodePayload(frame, sourceSessionId, id, body, offset, length);

        final SequencedFrameDecoder decoder = new SequencedFrameDecoder();
        if (!decoder.wrap(frame, 0, frameLength)) {
            throw new IllegalStateException("encoded frame did not read back as one");
        }
        final SequencedEvent event = new SequencedEvent(decoder);
        event.set(0, 0);
        return event;
    }

    private static int encodePayload(final MutableDirectBuffer frame, final long sourceSessionId, final int payloadId,
                                     final DirectBuffer body, final int offset, final int length) {
        final SequencedEncoder sequenced = new SequencedEncoder();
        sequenced.wrapAndApplyHeader(frame, 0, new MessageHeaderEncoder());
        sequenced.header().sourceId(0).connectionId(0).sessionId(sourceSessionId).payloadId(payloadId)
            .globalSeqNo(0).timestamp(0);
        sequenced.putPayload(body, offset, length);
        return MessageHeaderEncoder.ENCODED_LENGTH + sequenced.encodedLength();
    }

    private static int encodeSystem(final MutableDirectBuffer frame, final long sourceSessionId,
                                    final int systemEventType, final DirectBuffer body, final int offset,
                                    final int length) {
        final SequencedSystemEncoder sequenced = new SequencedSystemEncoder();
        sequenced.wrapAndApplyHeader(frame, 0, new MessageHeaderEncoder());
        sequenced.header().sourceId(0).connectionId(0).sessionId(sourceSessionId).systemEventType(systemEventType)
            .globalSeqNo(0).timestamp(0);
        sequenced.putBody(body, offset, length);
        return MessageHeaderEncoder.ENCODED_LENGTH + sequenced.encodedLength();
    }
}
