package org.limitless.seqeron.sequencer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.limitless.seqeron.sbe.frame.MessageHeaderDecoder;
import org.limitless.seqeron.sbe.frame.UnsequencedDecoder;
import org.limitless.seqeron.sbe.frame.UnsequencedSystemDecoder;
import org.limitless.seqeron.sequencer.IngressPublisher.Publish;

/**
 * Unit tests for the encode-and-offer preamble every producer writes. No Aeron runtime: what the sender
 * does with a frame is a boolean, and the frames themselves are read back with the same decoder a
 * consumer uses — which is what makes the round trip here worth asserting at all.
 *
 * <p>The C++ twin's coverage of this sits in {@code ClusterStreamSenderTest}, which drives the session
 * handshake through fake transports. That machine is {@code AeronCluster}'s on this side, so what is left
 * to test is the part seqeron still owns: the frame, and the three-valued outcome.
 */
class IngressPublisherTest {
    private static final int SOURCE_ID = 10;
    private static final int CONNECTION_ID = 7;
    private static final long SESSION_ID = 4242;
    private static final int PAYLOAD_ID = 6;

    /** Records what it was handed, and answers whatever the test told it to. */
    private static final class FakeSender implements IngressSender {
        private final ExpandableArrayBuffer sent = new ExpandableArrayBuffer();
        private boolean accept = true;
        private int length;
        private int calls;

        @Override
        public boolean send(final DirectBuffer frame, final int length) {
            calls++;
            this.length = length;
            sent.putBytes(0, frame, 0, length);
            return accept;
        }

        @Override
        public long clusterSessionId() {
            return SESSION_ID;
        }
    }

    private final FakeSender sender = new FakeSender();
    private final IngressPublisher publisher = new IngressPublisher();
    private final MessageHeaderDecoder messageHeader = new MessageHeaderDecoder();
    private final UnsequencedDecoder unsequenced = new UnsequencedDecoder();
    private final UnsequencedSystemDecoder unsequencedSystem = new UnsequencedSystemDecoder();

    private static MutableDirectBuffer payload(final int length) {
        final MutableDirectBuffer buffer = new UnsafeBuffer(new byte[Math.max(length, 1)]);
        for (int i = 0; i < length; i++) {
            buffer.putByte(i, (byte)('a' + i % 26));
        }
        return buffer;
    }

    @Test
    @DisplayName("an application payload is wrapped with the caller's identity and the sender's session")
    void payloadCarriesTheHeaderTheCallerAsked() {
        final MutableDirectBuffer body = payload(16);
        assertEquals(Publish.Published,
                     publisher.publishPayload(sender, SOURCE_ID, CONNECTION_ID, PAYLOAD_ID, body, 16));
        assertEquals(1, sender.calls);

        messageHeader.wrap(sender.sent, 0);
        assertEquals(UnsequencedDecoder.TEMPLATE_ID, messageHeader.templateId());
        unsequenced.wrap(sender.sent, MessageHeaderDecoder.ENCODED_LENGTH, messageHeader.blockLength(),
                         messageHeader.version());
        assertEquals(SOURCE_ID, unsequenced.header().sourceId());
        assertEquals(CONNECTION_ID, unsequenced.header().connectionId());
        assertEquals(SESSION_ID, unsequenced.header().sessionId(), "the sender's session, not the caller's");
        assertEquals(PAYLOAD_ID, unsequenced.header().payloadId());

        final byte[] carried = new byte[16];
        assertEquals(16, unsequenced.payloadLength());
        unsequenced.getPayload(carried, 0, carried.length);
        for (int i = 0; i < carried.length; i++) {
            assertEquals(body.getByte(i), carried[i], "payload byte " + i);
        }
    }

    @Test
    @DisplayName("a body above MAX_PAYLOAD_LENGTH is refused locally — nothing is encoded, nothing offered")
    void oversizeBodyIsRefusedWithoutTouchingTheSender() {
        final int tooLong = FrameLayer.MAX_PAYLOAD_LENGTH + 1;
        assertEquals(Publish.Refused,
                     publisher.publishPayload(sender, SOURCE_ID, CONNECTION_ID, PAYLOAD_ID, payload(tooLong),
                                              tooLong));
        assertEquals(0, sender.calls, "a Refused publish must not reach the transport");
    }

    @Test
    @DisplayName("a body at exactly MAX_PAYLOAD_LENGTH is admitted")
    void maximumBodyIsAdmitted() {
        final int atLimit = FrameLayer.MAX_PAYLOAD_LENGTH;
        assertEquals(Publish.Published,
                     publisher.publishPayload(sender, SOURCE_ID, CONNECTION_ID, PAYLOAD_ID, payload(atLimit),
                                              atLimit));
    }

    @Test
    @DisplayName("a transport that will not take the frame is Declined, which a caller may retry")
    void declinedIsTheTransportsAnswer() {
        sender.accept = false;
        assertEquals(Publish.Declined,
                     publisher.publishPayload(sender, SOURCE_ID, CONNECTION_ID, PAYLOAD_ID, payload(8), 8));
        assertEquals(1, sender.calls);
    }

    @Test
    @DisplayName("a system event is wrapped in the system family, named by systemEventType")
    void systemEventUsesTheSystemFamily() {
        final MutableDirectBuffer body = payload(12);
        assertEquals(Publish.Published,
                     publisher.publishSystem(sender, SOURCE_ID, CONNECTION_ID, SystemFrame.GATEWAY_STARTED,
                                             body, 12));

        messageHeader.wrap(sender.sent, 0);
        assertEquals(UnsequencedSystemDecoder.TEMPLATE_ID, messageHeader.templateId());
        unsequencedSystem.wrap(sender.sent, MessageHeaderDecoder.ENCODED_LENGTH, messageHeader.blockLength(),
                               messageHeader.version());
        // The uint16 at offset 16 is a systemEventType on this family and a payloadId on the other, which
        // is why a consumer splits by family before it reads it.
        assertEquals(SystemFrame.GATEWAY_STARTED, unsequencedSystem.header().systemEventType());
        assertEquals(12, unsequencedSystem.bodyLength());
    }

    @Test
    @DisplayName("one publisher, many frames: the buffer it owns is reused and never grows a frame's tail")
    void bufferIsReusedAcrossPublishes() {
        assertEquals(Publish.Published,
                     publisher.publishPayload(sender, SOURCE_ID, CONNECTION_ID, PAYLOAD_ID, payload(64), 64));
        final int longFrame = sender.length;
        assertEquals(Publish.Published,
                     publisher.publishPayload(sender, SOURCE_ID, CONNECTION_ID, PAYLOAD_ID, payload(8), 8));
        assertTrue(sender.length < longFrame, "the second frame is shorter than the first");
    }
}
