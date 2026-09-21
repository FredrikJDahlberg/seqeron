package org.limitless.seqeron.app;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.ByteOrder;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.limitless.seqeron.replayer.client.SequencedEvents;
import org.limitless.seqeron.sbe.frame.MessageHeaderEncoder;

/**
 * Unit tests for the two ways a payload is addressed: past its own {@code MessageHeader} when it has one,
 * and from its first byte when it has none (spec §13.2). No Aeron runtime — the event is a view over an
 * encoded tap frame.
 */
class PayloadTest {
    private static final int PAYLOAD_ID = 5;

    private final Payload payload = new Payload();

    @Test
    @DisplayName("a payload carrying no MessageHeader is addressed whole by payloadOffset")
    void headerlessPayloadIsAddressedWhole() {
        final MutableDirectBuffer body = new UnsafeBuffer(new byte[Long.BYTES]);
        body.putLong(0, 0x0123456789ABCDEFL, ByteOrder.LITTLE_ENDIAN);
        payload.wrap(SequencedEvents.of(1, false, PAYLOAD_ID, body, 0, Long.BYTES));

        assertEquals(Long.BYTES, payload.payloadLength(), "the raw body's every byte is the payload");
        assertEquals(0x0123456789ABCDEFL,
                     payload.buffer().getLong(payload.payloadOffset(), ByteOrder.LITTLE_ENDIAN));
    }

    @Test
    @DisplayName("bodyOffset skips the payload's own MessageHeader, so the two differ by its length")
    void bodyOffsetSkipsTheHeader() {
        final MutableDirectBuffer body = new UnsafeBuffer(new byte[Long.BYTES]);
        payload.wrap(SequencedEvents.of(1, false, PAYLOAD_ID, body, 0, Long.BYTES));

        assertEquals(MessageHeaderEncoder.ENCODED_LENGTH, payload.bodyOffset() - payload.payloadOffset());
        assertEquals(MessageHeaderEncoder.ENCODED_LENGTH, payload.payloadLength() - payload.bodyLength());
    }
}
