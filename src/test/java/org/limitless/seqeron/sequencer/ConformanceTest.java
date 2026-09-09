package org.limitless.seqeron.sequencer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.limitless.seqeron.replayer.client.SequencedFrameDecoder;
import org.limitless.seqeron.sbe.frame.ApplicationRegisteredEncoder;
import org.limitless.seqeron.sbe.frame.ClusterHeartbeatDecoder;
import org.limitless.seqeron.sbe.frame.ClusterStartedEncoder;
import org.limitless.seqeron.sbe.frame.ConnectionOpenedEncoder;
import org.limitless.seqeron.sbe.frame.GatewayActivationRequestedEncoder;
import org.limitless.seqeron.sbe.frame.GatewayActiveDecoder;
import org.limitless.seqeron.sbe.frame.GatewayRegisteredEncoder;
import org.limitless.seqeron.sbe.frame.GatewayStartedEncoder;
import org.limitless.seqeron.sbe.frame.LeadershipChangedDecoder;
import org.limitless.seqeron.sbe.frame.MessageHeaderDecoder;
import org.limitless.seqeron.sbe.frame.MessageHeaderEncoder;
import org.limitless.seqeron.sbe.frame.SequencedDecoder;
import org.limitless.seqeron.sbe.frame.SequencedHeaderDecoder;
import org.limitless.seqeron.sbe.frame.SequencedSystemDecoder;
import org.limitless.seqeron.sbe.frame.SequencedSystemHeaderDecoder;
import org.limitless.seqeron.sbe.frame.UnsequencedHeaderDecoder;
import org.limitless.seqeron.sbe.frame.UnsequencedSystemHeaderDecoder;

/**
 * The protocol conformance suite — doc/seqeron-protocol-spec.md §14, one nested section per row.
 *
 * <p>No Aeron, no media driver, sub-second, and the fixture is a <b>synthetic payload seqeron owns</b>:
 * the application vectors carry arbitrary bytes under a {@code payloadId} the cluster tier never opens,
 * so nothing here compiles against an application schema. That is what {@code SequencerTest} cannot say
 * (its copy-through exemplar is a session-schema {@code Heartbeat}) and what the repo split needs.
 *
 * <p>Rows 2 and 6 are schema-level and are mirrored in {@code core_tests}' {@code ConformanceTest.cpp};
 * the rest exercise {@link Sequencer}, which has no C++ implementation to mirror.
 */
class ConformanceTest {
    /** An allocated {@code payloadId} the cluster tier does not own and never opens. */
    private static final int PAYLOAD_ID = 2;

    private static final int SOURCE_ID = 7;
    private static final int CONNECTION_ID = 42;
    private static final long SESSION_ID = 0x5EE5_1000L;
    private static final long TIMESTAMP = 1_700_000_000_000L;

    /** clusterctl's reserved {@code sourceId} (§5): never a {@code gatewaySourceId}, so never list-checked. */
    private static final int CLUSTERCTL_SOURCE_ID = 2;

    /** A {@code gatewaySourceId} the list rows below claim. */
    private static final int LISTED_SOURCE_ID = 5;

    /** Offset of the body's length prefix in an ingress frame: past the outer header and the composite. */
    private static final int PREFIX_OFFSET =
        MessageHeaderEncoder.ENCODED_LENGTH + UnsequencedHeaderDecoder.ENCODED_LENGTH;
    /** Offsets inside the outer {@code MessageHeader}. */
    private static final int BLOCK_LENGTH_OFFSET = 0;
    private static final int TEMPLATE_ID_OFFSET = 2;
    private static final int SCHEMA_ID_OFFSET = 4;
    private static final int VERSION_OFFSET = 6;

    private final MutableDirectBuffer ingress = new ExpandableArrayBuffer(4096);
    private final Sequencer sequencer = new Sequencer();

    // ── Row 1. Copy fidelity (§5) ────────────────────────────────────────────────────────────────

    @ParameterizedTest(name = "payload of {0} bytes")
    @ValueSource(ints = {0, 1, 1316})
    @DisplayName("row 1: a sequenced payload is byte-identical to the ingress one, and payloadId is unchanged")
    void copyFidelity(final int payloadLength) {
        final byte[] payload = syntheticPayload(payloadLength);
        final int length = payloadFrame(PAYLOAD_ID, SOURCE_ID, payload);

        final int sequenced = sequencer.sequenceMessage(ingress, 0, length, SESSION_ID, TIMESTAMP);
        assertNotEquals(Sequencer.NO_FRAME, sequenced);

        final SequencedFrameDecoder view = wrapSequenced(sequenced);
        assertEquals(PAYLOAD_ID, view.payloadId(), "payloadId is copied verbatim");
        assertEquals(payloadLength, view.payloadLength());
        assertArrayEquals(payload, copy(view.buffer(), view.payloadOffset(), view.payloadLength()),
                          "the payload crosses byte-identical; it is never re-encoded (E-1)");
    }

    // ── Row 2. The prefix property (F-3) ─────────────────────────────────────────────────────────

    @Test
    @DisplayName("row 2: each unsequenced composite is the 18-byte prefix of its sequenced counterpart")
    void prefixProperty() {
        assertEquals(18, UnsequencedHeaderDecoder.ENCODED_LENGTH);
        assertEquals(18, UnsequencedSystemHeaderDecoder.ENCODED_LENGTH);
        assertEquals(34, SequencedHeaderDecoder.ENCODED_LENGTH);
        assertEquals(34, SequencedSystemHeaderDecoder.ENCODED_LENGTH);

        // The field at offset 16 is a payloadId on one pair and a systemEventType on the other, and is
        // written here to the same bits: what F-3 buys is that the two pairs are the same 18 bytes.
        final byte[] unsequenced = encodeUnsequencedHeader();
        final byte[] unsequencedSystem = encodeUnsequencedSystemHeader();
        assertArrayEquals(unsequenced, unsequencedSystem,
                          "the two ingress composites are byte-for-byte identical, offset 16 included");

        final byte[] sequenced = encodeSequencedHeader();
        final byte[] sequencedSystem = encodeSequencedSystemHeader();
        assertArrayEquals(sequenced, sequencedSystem, "and so are the two sequenced ones");

        assertArrayEquals(unsequenced, java.util.Arrays.copyOf(sequenced, 18),
                          "the ingress composite is the sequenced one's byte prefix — the copy-18 of §9.5");
        assertArrayEquals(unsequencedSystem, java.util.Arrays.copyOf(sequencedSystem, 18));
    }

    @Test
    @DisplayName("row 2: every frame-layer field sits at the offset §4.1 gives")
    void headerOffsets() {
        // The prefix property above says the composites agree with each other; this says they agree with
        // the spec. A permutation of two same-width fields passes the first and fails here, which is why
        // §4.1 writes the offsets down rather than leaving them implied by the field order.
        assertEquals(0, UnsequencedHeaderDecoder.sourceIdEncodingOffset());
        assertEquals(4, UnsequencedHeaderDecoder.connectionIdEncodingOffset());
        assertEquals(8, UnsequencedHeaderDecoder.sessionIdEncodingOffset());
        assertEquals(16, UnsequencedHeaderDecoder.payloadIdEncodingOffset());

        assertEquals(0, UnsequencedSystemHeaderDecoder.sourceIdEncodingOffset());
        assertEquals(4, UnsequencedSystemHeaderDecoder.connectionIdEncodingOffset());
        assertEquals(8, UnsequencedSystemHeaderDecoder.sessionIdEncodingOffset());
        assertEquals(16, UnsequencedSystemHeaderDecoder.systemEventTypeEncodingOffset());

        assertEquals(0, SequencedHeaderDecoder.sourceIdEncodingOffset());
        assertEquals(4, SequencedHeaderDecoder.connectionIdEncodingOffset());
        assertEquals(8, SequencedHeaderDecoder.sessionIdEncodingOffset());
        assertEquals(16, SequencedHeaderDecoder.payloadIdEncodingOffset());
        assertEquals(18, SequencedHeaderDecoder.globalSeqNoEncodingOffset());
        assertEquals(26, SequencedHeaderDecoder.timestampEncodingOffset());

        assertEquals(0, SequencedSystemHeaderDecoder.sourceIdEncodingOffset());
        assertEquals(4, SequencedSystemHeaderDecoder.connectionIdEncodingOffset());
        assertEquals(8, SequencedSystemHeaderDecoder.sessionIdEncodingOffset());
        assertEquals(16, SequencedSystemHeaderDecoder.systemEventTypeEncodingOffset());
        assertEquals(18, SequencedSystemHeaderDecoder.globalSeqNoEncodingOffset());
        assertEquals(26, SequencedSystemHeaderDecoder.timestampEncodingOffset());
    }

    @Test
    @DisplayName("row 2: the frame sizes are §4.2's table")
    void frameSizes() {
        assertEquals(8, MessageHeaderDecoder.ENCODED_LENGTH);
        assertEquals(2, SequencedDecoder.payloadHeaderLength(), "the var-data length prefix");
        assertEquals(2, SequencedSystemDecoder.bodyHeaderLength());

        assertEquals(28, FrameLayer.MIN_INGRESS_LENGTH, "ingress fixed overhead, both families");
        assertEquals(44, MessageHeaderDecoder.ENCODED_LENGTH + SequencedHeaderDecoder.ENCODED_LENGTH +
                         SequencedDecoder.payloadHeaderLength(), "sequenced fixed overhead, both families");

        // A ClusterHeartbeat is a template of its own: no length prefix and no body at all.
        assertEquals(42, tapSynthesized(1, ClusterHeartbeatDecoder.TEMPLATE_ID,
                                        SystemFrame.CLUSTER_HEARTBEAT).length,
                     "8 + 34, the cheapest frame in the system");

        assertEquals(44 + FrameLayer.MAX_PAYLOAD_LENGTH,
                     tapPayloadFrame(1, PAYLOAD_ID, syntheticPayload(FrameLayer.MAX_PAYLOAD_LENGTH)).length,
                     "§12's ceiling as a frame on the wire");
    }

    // ── Row 3. System frames round-trip unchanged (§7) ───────────────────────────────────────────

    @Test
    @DisplayName("row 3: each of the eight submitted events crosses with its body byte-identical")
    void submittedSystemEventsRoundTrip() {
        for (final Map.Entry<Integer, byte[]> event : submittedEvents().entrySet()) {
            final Sequencer target = new Sequencer();
            final int systemEventType = event.getKey();
            final byte[] body = event.getValue();

            // The two list-checked events (S-6 case 1) need a row naming their gatewayId first.
            if (systemEventType == SystemFrame.GATEWAY_STARTED ||
                systemEventType == SystemFrame.GATEWAY_ACTIVATION_REQUESTED) {
                loadList(target, 1);
            }
            // GatewayStarted must carry its row's gatewaySourceId; everything else publishes from an
            // unlisted id, which is S-6 case 3 and unchecked.
            final int sourceId = systemEventType == SystemFrame.GATEWAY_STARTED ? LISTED_SOURCE_ID
                                                                                : CLUSTERCTL_SOURCE_ID;
            final int length = systemFrame(systemEventType, sourceId, body);
            final int sequenced = target.sequenceMessage(ingress, 0, length, SESSION_ID, TIMESTAMP);
            assertNotEquals(Sequencer.NO_FRAME, sequenced, "systemEventType " + systemEventType + " was refused");

            final SequencedFrameDecoder view = wrapSequenced(target, sequenced);
            assertTrue(view.isSystem());
            assertEquals(systemEventType, view.systemEventType(), "systemEventType is copied verbatim");
            assertArrayEquals(body, copy(view.buffer(), view.payloadOffset(), view.payloadLength()),
                              "the body crosses byte-identical");
        }
    }

    @ParameterizedTest(name = "connectionData of {0} bytes")
    @ValueSource(ints = {0, 7, 1314})
    @DisplayName("row 3: ConnectionOpened's connectionData crosses unchanged at every size §7.1 allows")
    void connectionOpenedCarriesAnyConnectionData(final int dataLength) {
        // The body is the var-data prefix plus the data, so 1314 bytes of it fills MAX_PAYLOAD_LENGTH.
        final byte[] data = syntheticPayload(dataLength);
        final byte[] body = connectionOpenedBody(data);
        assertTrue(body.length <= FrameLayer.MAX_PAYLOAD_LENGTH);

        final int length = systemFrame(SystemFrame.CONNECTION_OPENED, SOURCE_ID, body);
        final int sequenced = sequencer.sequenceMessage(ingress, 0, length, SESSION_ID, TIMESTAMP);
        assertNotEquals(Sequencer.NO_FRAME, sequenced);

        final SequencedFrameDecoder view = wrapSequenced(sequenced);
        assertArrayEquals(body, copy(view.buffer(), view.payloadOffset(), view.payloadLength()));
    }

    @Test
    @DisplayName("row 3: the three synthesized frames carry their template, their fields and a redundant type")
    void synthesizedFramesCarryTheirOwnTemplates() {
        final int heartbeat = sequencer.clusterHeartbeat(TIMESTAMP);
        assertEquals(ClusterHeartbeatDecoder.TEMPLATE_ID, templateIdOf(heartbeat));
        assertEquals(42, heartbeat, "no body at all — the cheapest frame in the system (§15 step 10)");
        assertEquals(SystemFrame.CLUSTER_HEARTBEAT, wrapSequenced(heartbeat).systemEventType(),
                     "systemEventType is populated on a synthesized frame too, so offset 16 discriminates "
                     + "every frame on the tap");
        assertEquals(-1, wrapSequenced(heartbeat).sourceId(), "-1 marks the synthesized class (F-4)");

        final int leadership = sequencer.leadershipChanged(2, TIMESTAMP);
        assertEquals(LeadershipChangedDecoder.TEMPLATE_ID, templateIdOf(leadership));
        assertEquals(SystemFrame.LEADERSHIP_CHANGED, wrapSequenced(leadership).systemEventType());
        assertEquals(2, decodeLeadershipChanged(sequencer.buffer()).newLeaderMemberId());

        loadList(sequencer, 0);
        final int active = sequencer.pendingGatewayActivation(TIMESTAMP);
        assertEquals(GatewayActiveDecoder.TEMPLATE_ID, templateIdOf(active));
        assertEquals(SystemFrame.GATEWAY_ACTIVE, wrapSequenced(active).systemEventType());
        assertEquals(11, decodeGatewayActive(sequencer.buffer()).gatewayId());
    }

    // ── Row 4. The rejection table (§9.2, S-4/S-5/S-7) ───────────────────────────────────────────

    @Test
    @DisplayName("row 4 condition 1: a frame under 28 bytes or over MAX_INGRESS_LENGTH is refused")
    void conditionOneLength() {
        final int wellFormed = payloadFrame(PAYLOAD_ID, SOURCE_ID, syntheticPayload(4));
        assertRejected(() -> sequencer.sequenceMessage(ingress, 0, FrameLayer.MIN_INGRESS_LENGTH - 1, SESSION_ID,
                                                       TIMESTAMP));
        assertRejected(() -> sequencer.sequenceMessage(ingress, 0, 0, SESSION_ID, TIMESTAMP));

        payloadFrame(PAYLOAD_ID, SOURCE_ID, syntheticPayload(FrameLayer.MAX_PAYLOAD_LENGTH + 1));
        assertRejected(() -> sequencer.sequenceMessage(ingress, 0, FrameLayer.MAX_INGRESS_LENGTH + 1, SESSION_ID,
                                                       TIMESTAMP));
        assertEquals(0, wellFormed & 0, "the well-formed length above is only the fixture");
    }

    @Test
    @DisplayName("row 4 condition 2: a foreign schemaId, and any non-zero version, are refused")
    void conditionTwoSchemaAndVersion() {
        final int length = payloadFrame(PAYLOAD_ID, SOURCE_ID, syntheticPayload(4));
        ingress.putShort(SCHEMA_ID_OFFSET, (short)209, ByteOrder.LITTLE_ENDIAN);
        assertRejected(() -> sequencer.sequenceMessage(ingress, 0, length, SESSION_ID, TIMESTAMP));

        payloadFrame(PAYLOAD_ID, SOURCE_ID, syntheticPayload(4));
        ingress.putShort(VERSION_OFFSET, (short)1, ByteOrder.LITTLE_ENDIAN);
        assertRejected(() -> sequencer.sequenceMessage(ingress, 0, length, SESSION_ID, TIMESTAMP));
    }

    @Test
    @DisplayName("row 4 condition 3: a non-ingress templateId, the synthesized three included, is refused")
    void conditionThreeTemplateId() {
        for (final int templateId : new int[] {SequencedDecoder.TEMPLATE_ID, SequencedSystemDecoder.TEMPLATE_ID,
                                               ClusterHeartbeatDecoder.TEMPLATE_ID,
                                               LeadershipChangedDecoder.TEMPLATE_ID,
                                               GatewayActiveDecoder.TEMPLATE_ID}) {
            final int length = payloadFrame(PAYLOAD_ID, SOURCE_ID, syntheticPayload(4));
            ingress.putShort(TEMPLATE_ID_OFFSET, (short)templateId, ByteOrder.LITTLE_ENDIAN);
            assertRejected(() -> sequencer.sequenceMessage(ingress, 0, length, SESSION_ID, TIMESTAMP),
                           "templateId " + templateId);
        }
    }

    @Test
    @DisplayName("row 4 condition 4: a blockLength that is not 18 is refused on both ingress templates")
    void conditionFourBlockLength() {
        for (final int blockLength : new int[] {17, 19}) {
            final int payloadLength = payloadFrame(PAYLOAD_ID, SOURCE_ID, syntheticPayload(4));
            ingress.putShort(BLOCK_LENGTH_OFFSET, (short)blockLength, ByteOrder.LITTLE_ENDIAN);
            assertRejected(() -> sequencer.sequenceMessage(ingress, 0, payloadLength, SESSION_ID, TIMESTAMP),
                           "Unsequenced blockLength " + blockLength);

            final int systemLength = systemFrame(SystemFrame.CONNECTION_CLOSED, SOURCE_ID, new byte[0]);
            ingress.putShort(BLOCK_LENGTH_OFFSET, (short)blockLength, ByteOrder.LITTLE_ENDIAN);
            assertRejected(() -> sequencer.sequenceMessage(ingress, 0, systemLength, SESSION_ID, TIMESTAMP),
                           "UnsequencedSystem blockLength " + blockLength);
        }
    }

    @Test
    @DisplayName("row 4 condition 5: a length prefix that does not fit the frame exactly is refused")
    void conditionFiveLengthPrefix() {
        for (final int prefix : new int[] {5, 3, 65535}) {
            final int length = payloadFrame(PAYLOAD_ID, SOURCE_ID, syntheticPayload(4));
            ingress.putShort(PREFIX_OFFSET, (short)prefix, ByteOrder.LITTLE_ENDIAN);
            assertRejected(() -> sequencer.sequenceMessage(ingress, 0, length, SESSION_ID, TIMESTAMP),
                           "prefix " + prefix);
        }
    }

    @Test
    @DisplayName("row 4 condition 6: an ingress sourceId of -1 is refused")
    void conditionSixReservedSourceId() {
        final int length = payloadFrame(PAYLOAD_ID, Sequencer.NO_SOURCE_ID, syntheticPayload(4));
        assertRejected(() -> sequencer.sequenceMessage(ingress, 0, length, SESSION_ID, TIMESTAMP));
    }

    @Test
    @DisplayName("row 4 condition 7: payloadId 0 and core's retired 1 are refused")
    void conditionSevenPayloadId() {
        for (final int payloadId : new int[] {0, 1}) {
            final int length = payloadFrame(payloadId, SOURCE_ID, syntheticPayload(4));
            assertRejected(() -> sequencer.sequenceMessage(ingress, 0, length, SESSION_ID, TIMESTAMP),
                           "payloadId " + payloadId);
        }
    }

    @Test
    @DisplayName("row 4 condition 8: an unallocated or synthesis-only systemEventType is refused")
    void conditionEightSystemEventType() {
        for (final int systemEventType : new int[] {99, SystemFrame.LEADERSHIP_CHANGED,
                                                    SystemFrame.CLUSTER_HEARTBEAT, SystemFrame.GATEWAY_ACTIVE}) {
            final int length = systemFrame(systemEventType, SOURCE_ID, syntheticPayload(8));
            assertRejected(() -> sequencer.sequenceMessage(ingress, 0, length, SESSION_ID, TIMESTAMP),
                           "systemEventType " + systemEventType);
        }
    }

    @Test
    @DisplayName("row 4 condition 9: a GatewayStarted body short of its compiled block length is refused")
    void conditionNineBodyTooShort() {
        assertEquals(8, GatewayStartedEncoder.BLOCK_LENGTH);
        final int length = systemFrame(SystemFrame.GATEWAY_STARTED, SOURCE_ID, syntheticPayload(7));
        assertRejected(() -> sequencer.sequenceMessage(ingress, 0, length, SESSION_ID, TIMESTAMP));
    }

    @Test
    @DisplayName("row 4 condition 10: a frame failing S-6 is refused")
    void conditionTenS6() {
        loadList(sequencer, 1);
        final long before = sequencer.globalSeqNo();
        final int length = systemFrame(SystemFrame.GATEWAY_STARTED, LISTED_SOURCE_ID, gatewayStartedBody(99));
        assertRejected(() -> sequencer.sequenceMessage(ingress, 0, length, SESSION_ID, TIMESTAMP),
                       "a gatewayId no list row names", before);
    }

    // ── Row 4a. A rejection denies nothing (S-7, C-2) ────────────────────────────────────────────

    @Test
    @DisplayName("row 4a: two rejections each cost one counter tick, and the payloadId is still accepted after")
    void rejectionDeniesNothing() {
        final int bad = payloadFrame(PAYLOAD_ID, SOURCE_ID, syntheticPayload(4));
        ingress.putShort(PREFIX_OFFSET, (short)9, ByteOrder.LITTLE_ENDIAN);
        assertRejected(() -> sequencer.sequenceMessage(ingress, 0, bad, SESSION_ID, TIMESTAMP));

        payloadFrame(PAYLOAD_ID, SOURCE_ID, syntheticPayload(4));
        ingress.putShort(PREFIX_OFFSET, (short)9, ByteOrder.LITTLE_ENDIAN);
        assertRejected(() -> sequencer.sequenceMessage(ingress, 0, bad, SESSION_ID, TIMESTAMP));

        final byte[] payload = syntheticPayload(16);
        final int good = payloadFrame(PAYLOAD_ID, SOURCE_ID, payload);
        final int sequenced = sequencer.sequenceMessage(ingress, 0, good, SESSION_ID, TIMESTAMP);
        assertNotEquals(Sequencer.NO_FRAME, sequenced, "the rejections left no deny list behind (C-2)");
        assertEquals(1, sequencer.globalSeqNo(), "and cost no sequence number");
        assertArrayEquals(payload, copy(wrapSequenced(sequenced).buffer(), wrapSequenced(sequenced).payloadOffset(),
                                        payload.length));
        assertEquals(2, sequencer.rejectedFrameCount());
    }

    // ── Row 4b. The producer refuses before the wire (T-3, §12) ──────────────────────────────────

    @Test
    @DisplayName("row 4b: the encode method admits MAX_PAYLOAD_LENGTH, refuses one more, and survives it")
    void producerRefusesBeforeTheWire() {
        final Transport transport = new Transport();
        final MutableDirectBuffer frame = new ExpandableArrayBuffer(2048);
        final MutableDirectBuffer payload = new ExpandableArrayBuffer(FrameLayer.MAX_PAYLOAD_LENGTH + 1);

        final int exact = SystemFrame.wrapPayload(frame, SOURCE_ID, CONNECTION_ID, SESSION_ID, PAYLOAD_ID, payload,
                                                  FrameLayer.MAX_PAYLOAD_LENGTH);
        assertNotEquals(SystemFrame.REFUSED, exact);
        transport.offer(frame, exact);

        final int oversized = SystemFrame.wrapPayload(frame, SOURCE_ID, CONNECTION_ID, SESSION_ID, PAYLOAD_ID,
                                                      payload, FrameLayer.MAX_PAYLOAD_LENGTH + 1);
        assertEquals(SystemFrame.REFUSED, oversized,
                     "T-3: local and permanent, and distinguishable from a transport's back-pressure");
        assertEquals(1, transport.offers.size(), "nothing was offered to any transport");

        final int again = SystemFrame.wrapPayload(frame, SOURCE_ID, CONNECTION_ID, SESSION_ID, PAYLOAD_ID, payload,
                                                  16);
        assertNotEquals(SystemFrame.REFUSED, again, "the refusal left the producer usable");
        transport.offer(frame, again);
        assertEquals(2, transport.offers.size());

        // The same holds for the system family, whose body is bounded by the same constant.
        assertEquals(SystemFrame.REFUSED,
                     SystemFrame.wrap(frame, SOURCE_ID, CONNECTION_ID, SESSION_ID, SystemFrame.CONNECTION_OPENED,
                                      payload, FrameLayer.MAX_PAYLOAD_LENGTH + 1));
    }

    // ── Row 5. Synthesis determinism (S-3, F-2) ──────────────────────────────────────────────────

    @Test
    @DisplayName("row 5: two independent sequencers fed the same log emit byte-identical frames")
    void synthesisIsDeterministic() {
        assertArrayEquals(driveOneLog().toArray(new byte[0][]), driveOneLog().toArray(new byte[0][]));
    }

    /** Feeds one fixed message sequence through a fresh sequencer and collects every frame it emits. */
    private List<byte[]> driveOneLog() {
        final Sequencer target = new Sequencer();
        final List<byte[]> frames = new ArrayList<>();
        long timestamp = TIMESTAMP;

        collect(frames, target, target.leadershipChanged(0, timestamp));
        loadList(target, 0);
        collect(frames, target, target.sequenceMessage(ingress, 0, lastIngressLength, SESSION_ID, timestamp));
        collect(frames, target, target.pendingGatewayActivation(timestamp));

        for (int i = 0; i < 3; i++) {
            timestamp += Sequencer.CLUSTER_HEARTBEAT_INTERVAL_MS;
            collect(frames, target, target.clusterHeartbeat(timestamp));
            final int length = payloadFrame(PAYLOAD_ID, SOURCE_ID, syntheticPayload(8 + i));
            collect(frames, target, target.sequenceMessage(ingress, 0, length, SESSION_ID, timestamp));
        }
        collect(frames, target, target.leadershipChanged(1, timestamp));
        return frames;
    }

    // ── Row 6. The boundary payload sizes (§12) ─────────────────────────────────────────────────

    @Test
    @DisplayName("row 6: an empty, a one-byte and a ceiling-sized payload all cross intact")
    void boundaryPayloadSizes() {
        // The empty case is the one that found the short-payload defect: a frame carrying no payload is
        // legal (§5) and must still reach the consumer, or P-3's continuity read sees a gap that is not
        // there.
        for (final int payloadLength : new int[] { 0, 1, FrameLayer.MAX_PAYLOAD_LENGTH }) {
            final byte[] frame = tapPayloadFrame(11, PAYLOAD_ID, syntheticPayload(payloadLength));
            final SequencedFrameDecoder view = new SequencedFrameDecoder();
            assertTrue(view.wrap(new org.agrona.concurrent.UnsafeBuffer(frame), 0, frame.length),
                       "a payload of " + payloadLength + " bytes does not decode");
            assertFalse(view.isSystem());
            assertEquals(PAYLOAD_ID, view.payloadId());
            assertEquals(payloadLength, view.payloadLength());
            assertEquals(44 + payloadLength, frame.length, "44 + the payload (§4.2)");
        }
    }

    // ── Row 7. Selective consumption (P-1 to P-3) ────────────────────────────────────────────────

    @Test
    @DisplayName("row 7: an unallocated payloadId and an unhandled systemEventType are skipped, in sequence")
    void selectiveConsumption() {
        // A contiguous run over all five sequenced shapes: an application payload under a payloadId
        // nothing allocates, a submitted system event no consumer here handles, and the three synthesized.
        final List<byte[]> tap = new ArrayList<>();
        tap.add(tapPayloadFrame(1, 4095, syntheticPayload(8)));
        tap.add(tapSystemFrame(2, SystemFrame.PAYLOAD_ID_REGISTERED, payloadIdRegisteredBody()));
        tap.add(tapSynthesized(3, ClusterHeartbeatDecoder.TEMPLATE_ID, SystemFrame.CLUSTER_HEARTBEAT));
        tap.add(tapSynthesized(4, LeadershipChangedDecoder.TEMPLATE_ID, SystemFrame.LEADERSHIP_CHANGED));
        tap.add(tapSynthesized(5, GatewayActiveDecoder.TEMPLATE_ID, SystemFrame.GATEWAY_ACTIVE));

        // A consumer that recognises none of them still reads every one, and tracks continuity across all
        // five: the read is branch-free because globalSeqNo sits at 18 on every sequenced shape (F-3).
        long expected = 1;
        for (final byte[] frame : tap) {
            final SequencedFrameDecoder view = new SequencedFrameDecoder();
            assertTrue(view.wrap(new org.agrona.concurrent.UnsafeBuffer(frame), 0, frame.length),
                       "globalSeqNo " + expected + " is not readable, so P-3 would lose it");
            assertEquals(expected, view.globalSeqNo(), "continuity does not depend on payload comprehension");
            expected++;
        }

        // And an empty payload is a frame like any other: it names no message, but it holds a globalSeqNo.
        final byte[] empty = tapPayloadFrame(6, 4095, new byte[0]);
        final SequencedFrameDecoder view = new SequencedFrameDecoder();
        assertTrue(view.wrap(new org.agrona.concurrent.UnsafeBuffer(empty), 0, empty.length));
        assertEquals(6, view.globalSeqNo());
        assertEquals(0, view.templateId(), "no inner declaration, so nothing can dispatch on it (P-1)");
    }

    /** One synthesized shape, built directly so its globalSeqNo is the fixture's rather than a sequencer's. */
    private static byte[] tapSynthesized(final long globalSeqNo, final int templateId, final int systemEventType) {
        final MutableDirectBuffer frame = new ExpandableArrayBuffer(64);
        if (templateId == ClusterHeartbeatDecoder.TEMPLATE_ID) {
            final org.limitless.seqeron.sbe.frame.ClusterHeartbeatEncoder encoder =
                new org.limitless.seqeron.sbe.frame.ClusterHeartbeatEncoder();
            encoder.wrapAndApplyHeader(frame, 0, new MessageHeaderEncoder());
            stampSynthesized(encoder.header(), systemEventType, globalSeqNo);
            return copy(frame, 0, MessageHeaderEncoder.ENCODED_LENGTH + encoder.encodedLength());
        }
        if (templateId == LeadershipChangedDecoder.TEMPLATE_ID) {
            final org.limitless.seqeron.sbe.frame.LeadershipChangedEncoder encoder =
                new org.limitless.seqeron.sbe.frame.LeadershipChangedEncoder();
            encoder.wrapAndApplyHeader(frame, 0, new MessageHeaderEncoder());
            stampSynthesized(encoder.header(), systemEventType, globalSeqNo);
            encoder.newLeaderMemberId(1);
            return copy(frame, 0, MessageHeaderEncoder.ENCODED_LENGTH + encoder.encodedLength());
        }
        final org.limitless.seqeron.sbe.frame.GatewayActiveEncoder encoder =
            new org.limitless.seqeron.sbe.frame.GatewayActiveEncoder();
        encoder.wrapAndApplyHeader(frame, 0, new MessageHeaderEncoder());
        stampSynthesized(encoder.header(), systemEventType, globalSeqNo);
        encoder.gatewayId(11);
        return copy(frame, 0, MessageHeaderEncoder.ENCODED_LENGTH + encoder.encodedLength());
    }

    private static void stampSynthesized(final org.limitless.seqeron.sbe.frame.SequencedSystemHeaderEncoder header,
                                         final int systemEventType, final long globalSeqNo) {
        header.sourceId(-1).connectionId(-1).sessionId(-1).systemEventType(systemEventType)
            .globalSeqNo(globalSeqNo).timestamp(TIMESTAMP);
    }

    // ── Row 8. S-6's three cases ─────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("row 8: a GatewayStarted agreeing with its row binds; four disagreements are refused")
    void s6BindsAndRefuses() {
        loadList(sequencer, 1);
        final long afterList = sequencer.globalSeqNo();

        // Case 1, the positive: gatewayId 11 is a row, and its row's gatewaySourceId is what we carry.
        int length = systemFrame(SystemFrame.GATEWAY_STARTED, LISTED_SOURCE_ID, gatewayStartedBody(11));
        assertNotEquals(Sequencer.NO_FRAME, sequencer.sequenceMessage(ingress, 0, length, SESSION_ID, TIMESTAMP));

        // Case 1, negative a: the same frame under a different gatewaySourceId.
        final int wrongSource = systemFrame(SystemFrame.GATEWAY_STARTED, 6, gatewayStartedBody(11));
        assertRejected(() -> sequencer.sequenceMessage(ingress, 0, wrongSource, SESSION_ID + 1, TIMESTAMP));

        // Case 1, negative b: a gatewayId no row names, on a listed sourceId.
        final int unknownGateway = systemFrame(SystemFrame.GATEWAY_STARTED, LISTED_SOURCE_ID,
                                               gatewayStartedBody(404));
        assertRejected(() -> sequencer.sequenceMessage(ingress, 0, unknownGateway, SESSION_ID + 1, TIMESTAMP));

        // Case 1, negative c: an activation request naming an unlisted gatewayId.
        final int unknownActivation = systemFrame(SystemFrame.GATEWAY_ACTIVATION_REQUESTED, CLUSTERCTL_SOURCE_ID,
                                                  activationRequestedBody(404));
        assertRejected(() -> sequencer.sequenceMessage(ingress, 0, unknownActivation, SESSION_ID + 1, TIMESTAMP));

        // Case 2: another system frame claiming a listed sourceId on a session no GatewayStarted bound.
        final int unbound = systemFrame(SystemFrame.CONNECTION_OPENED, LISTED_SOURCE_ID, connectionOpenedBody(
            new byte[0]));
        assertRejected(() -> sequencer.sequenceMessage(ingress, 0, unbound, SESSION_ID + 1, TIMESTAMP));

        // Case 3, both negatives: an application payload under that same sourceId, and clusterctl's marker.
        final int application = payloadFrame(PAYLOAD_ID, LISTED_SOURCE_ID, syntheticPayload(8));
        assertNotEquals(Sequencer.NO_FRAME,
                        sequencer.sequenceMessage(ingress, 0, application, SESSION_ID + 1, TIMESTAMP),
                        "S-6 is scoped to the system family");
        final int marker = systemFrame(SystemFrame.CLUSTER_STARTED, CLUSTERCTL_SOURCE_ID, clusterStartedBody());
        assertNotEquals(Sequencer.NO_FRAME, sequencer.sequenceMessage(ingress, 0, marker, SESSION_ID + 2, TIMESTAMP),
                        "an unlisted sourceId is unchecked");

        assertEquals(afterList + 3, sequencer.globalSeqNo(), "the four refusals consumed no sequence number");
        assertEquals(4, sequencer.rejectedFrameCount());
    }

    // ── Row 9. Promotion order (§7.2, S-3) ───────────────────────────────────────────────────────

    @Test
    @DisplayName("row 9: bootstrap takes rank 0, and a close promotes the lowest surviving rank")
    void promotionOrder() {
        // One logical gateway, three instances: gatewayIds 11/12/13 at ranks 0/1/2.
        register(sequencer, 11, LISTED_SOURCE_ID, (short)0, 2);
        register(sequencer, 12, LISTED_SOURCE_ID, (short)1, 1);
        register(sequencer, 13, LISTED_SOURCE_ID, (short)2, 0);

        assertEquals(11, activatedGatewayId(sequencer), "bootstrap designates rank 0, and only rank 0");
        assertEquals(Sequencer.NO_FRAME, sequencer.pendingGatewayActivation(TIMESTAMP), "and nothing else");

        // The operator's request is forwarded, and answered one globalSeqNo behind it.
        final int request = systemFrame(SystemFrame.GATEWAY_ACTIVATION_REQUESTED, CLUSTERCTL_SOURCE_ID,
                                        activationRequestedBody(12));
        assertNotEquals(Sequencer.NO_FRAME, sequencer.sequenceMessage(ingress, 0, request, SESSION_ID, TIMESTAMP));
        final long forwarded = sequencer.globalSeqNo();
        final int answer = sequencer.pendingGatewayActivation(TIMESTAMP);
        assertNotEquals(Sequencer.NO_FRAME, answer);
        assertEquals(forwarded + 1, sequencer.globalSeqNo(), "the answer sits one behind the request");
        assertEquals(12, decodeGatewayActive(sequencer.buffer()).gatewayId());

        // Rank 0 binds a session and loses it: rank 1 is promoted.
        bind(sequencer, 11, 100L);
        assertNotEquals(Sequencer.NO_FRAME, sequencer.sessionClosed(100L, TIMESTAMP));
        assertEquals(12, decodeGatewayActive(sequencer.buffer()).gatewayId());

        // Rank 1 binds and loses it: rank 0 comes back — lowest-rank-excluding, not next-rank-up.
        bind(sequencer, 12, 101L);
        assertNotEquals(Sequencer.NO_FRAME, sequencer.sessionClosed(101L, TIMESTAMP));
        assertEquals(11, decodeGatewayActive(sequencer.buffer()).gatewayId());
    }

    @Test
    @DisplayName("row 9: a designated instance that never starts is handed on at the deadline, not before")
    void activationDeadline() {
        register(sequencer, 11, LISTED_SOURCE_ID, (short)0, 1);
        register(sequencer, 12, LISTED_SOURCE_ID, (short)1, 0);
        assertEquals(11, activatedGatewayId(sequencer));

        final long armed = TIMESTAMP;
        assertEquals(Sequencer.NO_FRAME,
                     sequencer.pendingGatewayActivationTimeout(armed + Sequencer.GATEWAY_ACTIVATION_TIMEOUT_MS - 1),
                     "not one heartbeat before the deadline");
        final int handover =
            sequencer.pendingGatewayActivationTimeout(armed + Sequencer.GATEWAY_ACTIVATION_TIMEOUT_MS);
        assertNotEquals(Sequencer.NO_FRAME, handover);
        assertEquals(12, decodeGatewayActive(sequencer.buffer()).gatewayId());
    }

    @Test
    @DisplayName("row 9: an instance that answered arms nothing further, and a sole instance promotes nothing")
    void answeredActivationAndSoleInstance() {
        register(sequencer, 11, LISTED_SOURCE_ID, (short)0, 1);
        register(sequencer, 12, LISTED_SOURCE_ID, (short)1, 0);
        assertEquals(11, activatedGatewayId(sequencer));
        bind(sequencer, 11, 100L);
        assertEquals(Sequencer.NO_FRAME,
                     sequencer.pendingGatewayActivationTimeout(TIMESTAMP + 10 *
                                                               Sequencer.GATEWAY_ACTIVATION_TIMEOUT_MS),
                     "a GatewayStarted drops the deadline it answered");

        final Sequencer sole = new Sequencer();
        register(sole, 21, 6, (short)0, 0);
        assertEquals(21, activatedGatewayId(sole));
        bind(sole, 21, 200L);
        final long before = sole.globalSeqNo();
        assertEquals(Sequencer.NO_PROMOTION_TARGET, sole.sessionClosed(200L, TIMESTAMP));
        assertEquals(before, sole.globalSeqNo(), "no sibling, so no frame and no sequence number");
    }

    @Test
    @DisplayName("row 9: two logical gateways bootstrapped back to back each keep their own deadline")
    void twoLogicalGatewaysKeepTheirOwnDeadlines() {
        register(sequencer, 11, LISTED_SOURCE_ID, (short)0, 3);
        register(sequencer, 12, LISTED_SOURCE_ID, (short)1, 2);
        register(sequencer, 21, 6, (short)0, 1);
        register(sequencer, 22, 6, (short)1, 0);
        assertEquals(11, activatedGatewayId(sequencer));
        assertEquals(21, activatedGatewayId(sequencer));

        // Neither answered, so both deadlines are live and both hand over at the same consensus time.
        final long deadline = TIMESTAMP + Sequencer.GATEWAY_ACTIVATION_TIMEOUT_MS;
        assertNotEquals(Sequencer.NO_FRAME, sequencer.pendingGatewayActivationTimeout(deadline));
        assertEquals(12, decodeGatewayActive(sequencer.buffer()).gatewayId());
        assertNotEquals(Sequencer.NO_FRAME, sequencer.pendingGatewayActivationTimeout(deadline));
        assertEquals(22, decodeGatewayActive(sequencer.buffer()).gatewayId());
    }

    // ── Fixtures ─────────────────────────────────────────────────────────────────────────────────

    /** Length of the frame the last {@code payloadFrame}/{@code systemFrame} call wrote into {@link #ingress}. */
    private int lastIngressLength;

    /** An in-memory stand-in for a transport, so row 4b can see what was offered without Aeron. */
    private static final class Transport {
        private final List<byte[]> offers = new ArrayList<>();

        void offer(final DirectBuffer buffer, final int length) {
            offers.add(copy(buffer, 0, length));
        }
    }

    /** Repeatable synthetic bytes — a payload seqeron owns, in the only sense a payload can be owned. */
    private static byte[] syntheticPayload(final int length) {
        final byte[] payload = new byte[length];
        for (int i = 0; i < length; i++) {
            payload[i] = (byte)(i * 31 + 7);
        }
        return payload;
    }

    /** Encodes one {@code Unsequenced} frame into {@link #ingress}; returns its length. */
    private int payloadFrame(final int payloadId, final int sourceId, final byte[] payload) {
        final org.limitless.seqeron.sbe.frame.UnsequencedEncoder encoder =
            new org.limitless.seqeron.sbe.frame.UnsequencedEncoder();
        encoder.wrapAndApplyHeader(ingress, 0, new MessageHeaderEncoder());
        encoder.header().sourceId(sourceId).connectionId(CONNECTION_ID).sessionId(-1).payloadId(payloadId);
        encoder.putPayload(new org.agrona.concurrent.UnsafeBuffer(payload), 0, payload.length);
        lastIngressLength = MessageHeaderEncoder.ENCODED_LENGTH + encoder.encodedLength();
        return lastIngressLength;
    }

    /** Encodes one {@code UnsequencedSystem} frame into {@link #ingress}; returns its length. */
    private int systemFrame(final int systemEventType, final int sourceId, final byte[] body) {
        final org.limitless.seqeron.sbe.frame.UnsequencedSystemEncoder encoder =
            new org.limitless.seqeron.sbe.frame.UnsequencedSystemEncoder();
        encoder.wrapAndApplyHeader(ingress, 0, new MessageHeaderEncoder());
        encoder.header().sourceId(sourceId).connectionId(CONNECTION_ID).sessionId(-1)
            .systemEventType(systemEventType);
        encoder.putBody(new org.agrona.concurrent.UnsafeBuffer(body), 0, body.length);
        lastIngressLength = MessageHeaderEncoder.ENCODED_LENGTH + encoder.encodedLength();
        return lastIngressLength;
    }

    /** A tap-side {@code Sequenced} frame, built directly rather than through the sequencer. */
    private static byte[] tapPayloadFrame(final long globalSeqNo, final int payloadId, final byte[] payload) {
        final MutableDirectBuffer frame = new ExpandableArrayBuffer(payload.length + 64);
        final org.limitless.seqeron.sbe.frame.SequencedEncoder encoder =
            new org.limitless.seqeron.sbe.frame.SequencedEncoder();
        encoder.wrapAndApplyHeader(frame, 0, new MessageHeaderEncoder());
        encoder.header().sourceId(SOURCE_ID).connectionId(CONNECTION_ID).sessionId(SESSION_ID).payloadId(payloadId)
            .globalSeqNo(globalSeqNo).timestamp(TIMESTAMP);
        encoder.putPayload(new org.agrona.concurrent.UnsafeBuffer(payload), 0, payload.length);
        return copy(frame, 0, MessageHeaderEncoder.ENCODED_LENGTH + encoder.encodedLength());
    }

    /** The same for the submitted system family. */
    private static byte[] tapSystemFrame(final long globalSeqNo, final int systemEventType, final byte[] body) {
        final MutableDirectBuffer frame = new ExpandableArrayBuffer(body.length + 64);
        final org.limitless.seqeron.sbe.frame.SequencedSystemEncoder encoder =
            new org.limitless.seqeron.sbe.frame.SequencedSystemEncoder();
        encoder.wrapAndApplyHeader(frame, 0, new MessageHeaderEncoder());
        encoder.header().sourceId(SOURCE_ID).connectionId(CONNECTION_ID).sessionId(SESSION_ID)
            .systemEventType(systemEventType).globalSeqNo(globalSeqNo).timestamp(TIMESTAMP);
        encoder.putBody(new org.agrona.concurrent.UnsafeBuffer(body), 0, body.length);
        return copy(frame, 0, MessageHeaderEncoder.ENCODED_LENGTH + encoder.encodedLength());
    }

    /** The nine submitted events of §7, each with a well-formed body. */
    private static Map<Integer, byte[]> submittedEvents() {
        final java.util.LinkedHashMap<Integer, byte[]> events = new java.util.LinkedHashMap<>();
        events.put(SystemFrame.CONNECTION_OPENED, connectionOpenedBody(new byte[0]));
        events.put(SystemFrame.CONNECTION_CLOSED, new byte[0]);
        events.put(SystemFrame.CLUSTER_STARTED, clusterStartedBody());
        events.put(SystemFrame.CLUSTER_STOPPED, clusterStartedBody());
        events.put(SystemFrame.GATEWAY_REGISTERED, gatewayRegisteredBody(11, LISTED_SOURCE_ID, (short)0, 1));
        events.put(SystemFrame.GATEWAY_STARTED, gatewayStartedBody(11));
        events.put(SystemFrame.PAYLOAD_ID_REGISTERED, payloadIdRegisteredBody());
        events.put(SystemFrame.GATEWAY_ACTIVATION_REQUESTED, activationRequestedBody(11));
        events.put(SystemFrame.APPLICATION_REGISTERED, applicationRegisteredBody(3));
        return events;
    }

    private static byte[] connectionOpenedBody(final byte[] connectionData) {
        final MutableDirectBuffer body = new ExpandableArrayBuffer(connectionData.length + 16);
        final ConnectionOpenedEncoder encoder = new ConnectionOpenedEncoder();
        encoder.wrap(body, 0);
        encoder.putConnectionData(connectionData, 0, connectionData.length);
        return copy(body, 0, encoder.encodedLength());
    }

    private static byte[] clusterStartedBody() {
        final MutableDirectBuffer body = new ExpandableArrayBuffer(16);
        final ClusterStartedEncoder encoder = new ClusterStartedEncoder();
        encoder.wrap(body, 0).correlationId(0x0102_0304_0506_0708L);
        return copy(body, 0, encoder.encodedLength());
    }

    private static byte[] gatewayRegisteredBody(final int gatewayId, final int gatewaySourceId,
                                                final short preferenceRank, final int remaining) {
        final MutableDirectBuffer body = new ExpandableArrayBuffer(64);
        final GatewayRegisteredEncoder encoder = new GatewayRegisteredEncoder();
        encoder.wrap(body, 0).remaining(remaining).gatewayId(gatewayId).gatewaySourceId(gatewaySourceId)
            .gatewayName("GW-" + gatewayId);
        encoder.preferenceRank(preferenceRank);
        return copy(body, 0, encoder.encodedLength());
    }

    private static byte[] gatewayStartedBody(final int gatewayId) {
        final MutableDirectBuffer body = new ExpandableArrayBuffer(16);
        final GatewayStartedEncoder encoder = new GatewayStartedEncoder();
        encoder.wrap(body, 0).gatewayId(gatewayId).firstConnectionId(1000);
        return copy(body, 0, encoder.encodedLength());
    }

    private static byte[] activationRequestedBody(final int gatewayId) {
        final MutableDirectBuffer body = new ExpandableArrayBuffer(16);
        final GatewayActivationRequestedEncoder encoder = new GatewayActivationRequestedEncoder();
        encoder.wrap(body, 0).gatewayId(gatewayId);
        return copy(body, 0, encoder.encodedLength());
    }

    private static byte[] applicationRegisteredBody(final int applicationSourceId) {
        final MutableDirectBuffer body = new ExpandableArrayBuffer(64);
        final ApplicationRegisteredEncoder encoder = new ApplicationRegisteredEncoder();
        encoder.wrap(body, 0).applicationSourceId(applicationSourceId).applicationName("BasicDataServer");
        return copy(body, 0, encoder.encodedLength());
    }

    private static byte[] payloadIdRegisteredBody() {
        final MutableDirectBuffer body = new ExpandableArrayBuffer(64);
        final org.limitless.seqeron.sbe.frame.PayloadIdRegisteredEncoder encoder =
            new org.limitless.seqeron.sbe.frame.PayloadIdRegisteredEncoder();
        encoder.wrap(body, 0).payloadId(4).protocolVersion(1).protocolName("basicdata");
        return copy(body, 0, encoder.encodedLength());
    }

    /** Publishes one list row; {@code remaining == 0} closes the list and arms the bootstrap. */
    private void register(final Sequencer target, final int gatewayId, final int gatewaySourceId,
                          final short preferenceRank, final int remaining) {
        final int length = systemFrame(SystemFrame.GATEWAY_REGISTERED, CLUSTERCTL_SOURCE_ID,
                                       gatewayRegisteredBody(gatewayId, gatewaySourceId, preferenceRank, remaining));
        assertNotEquals(Sequencer.NO_FRAME, target.sequenceMessage(ingress, 0, length, SESSION_ID, TIMESTAMP));
    }

    /** A one-row list for {@code gatewayId} 11 at rank 0, left open when {@code remaining > 0}. */
    private void loadList(final Sequencer target, final int remaining) {
        register(target, 11, LISTED_SOURCE_ID, (short)0, remaining);
    }

    /** Binds {@code sessionId} to {@code gatewayId} with the GatewayStarted that is the only thing that can. */
    private void bind(final Sequencer target, final int gatewayId, final long sessionId) {
        final int length = systemFrame(SystemFrame.GATEWAY_STARTED, gatewayId < 20 ? LISTED_SOURCE_ID : 6,
                                       gatewayStartedBody(gatewayId));
        assertNotEquals(Sequencer.NO_FRAME, target.sequenceMessage(ingress, 0, length, sessionId, TIMESTAMP));
    }

    /** Drains one synthesized {@code GatewayActive} and returns the {@code gatewayId} it designates. */
    private int activatedGatewayId(final Sequencer target) {
        final int length = target.pendingGatewayActivation(TIMESTAMP);
        assertNotEquals(Sequencer.NO_FRAME, length, "expected a GatewayActive");
        return decodeGatewayActive(target.buffer()).gatewayId();
    }

    /** Asserts a rejection cost nothing: no frame, no sequence number, no synthesis, one counter tick. */
    private void assertRejected(final java.util.function.IntSupplier call) {
        assertRejected(call, "", sequencer.globalSeqNo());
    }

    private void assertRejected(final java.util.function.IntSupplier call, final String what) {
        assertRejected(call, what, sequencer.globalSeqNo());
    }

    private void assertRejected(final java.util.function.IntSupplier call, final String what, final long before) {
        final long rejectedBefore = sequencer.rejectedFrameCount();
        assertEquals(Sequencer.NO_FRAME, call.getAsInt(), what);
        assertEquals(before, sequencer.globalSeqNo(), what + ": globalSeqNo must not move (S-5)");
        assertEquals(Sequencer.NO_FRAME, sequencer.pendingGatewayActivation(TIMESTAMP),
                     what + ": nothing at all is emitted, not even behind the rejection");
        assertEquals(rejectedBefore + 1, sequencer.rejectedFrameCount(), what + ": one counter tick (S-7)");
    }

    private static void collect(final List<byte[]> frames, final Sequencer target, final int length) {
        if (length != Sequencer.NO_FRAME) {
            frames.add(copy(target.buffer(), 0, length));
        }
    }

    private SequencedFrameDecoder wrapSequenced(final int length) {
        return wrapSequenced(sequencer, length);
    }

    private static SequencedFrameDecoder wrapSequenced(final Sequencer target, final int length) {
        final SequencedFrameDecoder view = new SequencedFrameDecoder();
        assertTrue(view.wrap(target.buffer(), 0, length), "the sequencer emitted something that is not a frame");
        return view;
    }

    private int templateIdOf(final int length) {
        assertNotEquals(Sequencer.NO_FRAME, length);
        return new MessageHeaderDecoder().wrap(sequencer.buffer(), 0).templateId();
    }

    private static GatewayActiveDecoder decodeGatewayActive(final DirectBuffer buffer) {
        final GatewayActiveDecoder decoder = new GatewayActiveDecoder();
        decoder.wrap(buffer, MessageHeaderDecoder.ENCODED_LENGTH, GatewayActiveDecoder.BLOCK_LENGTH,
                     GatewayActiveDecoder.SCHEMA_VERSION);
        return decoder;
    }

    private static LeadershipChangedDecoder decodeLeadershipChanged(final DirectBuffer buffer) {
        final LeadershipChangedDecoder decoder = new LeadershipChangedDecoder();
        decoder.wrap(buffer, MessageHeaderDecoder.ENCODED_LENGTH, LeadershipChangedDecoder.BLOCK_LENGTH,
                     LeadershipChangedDecoder.SCHEMA_VERSION);
        return decoder;
    }

    private static byte[] encodeUnsequencedHeader() {
        final MutableDirectBuffer buffer = new ExpandableArrayBuffer(64);
        new org.limitless.seqeron.sbe.frame.UnsequencedHeaderEncoder().wrap(buffer, 0)
            .sourceId(SOURCE_ID).connectionId(CONNECTION_ID).sessionId(SESSION_ID).payloadId(PAYLOAD_ID);
        return copy(buffer, 0, UnsequencedHeaderDecoder.ENCODED_LENGTH);
    }

    private static byte[] encodeUnsequencedSystemHeader() {
        final MutableDirectBuffer buffer = new ExpandableArrayBuffer(64);
        new org.limitless.seqeron.sbe.frame.UnsequencedSystemHeaderEncoder().wrap(buffer, 0)
            .sourceId(SOURCE_ID).connectionId(CONNECTION_ID).sessionId(SESSION_ID).systemEventType(PAYLOAD_ID);
        return copy(buffer, 0, UnsequencedSystemHeaderDecoder.ENCODED_LENGTH);
    }

    private static byte[] encodeSequencedHeader() {
        final MutableDirectBuffer buffer = new ExpandableArrayBuffer(64);
        new org.limitless.seqeron.sbe.frame.SequencedHeaderEncoder().wrap(buffer, 0)
            .sourceId(SOURCE_ID).connectionId(CONNECTION_ID).sessionId(SESSION_ID).payloadId(PAYLOAD_ID)
            .globalSeqNo(9).timestamp(TIMESTAMP);
        return copy(buffer, 0, SequencedHeaderDecoder.ENCODED_LENGTH);
    }

    private static byte[] encodeSequencedSystemHeader() {
        final MutableDirectBuffer buffer = new ExpandableArrayBuffer(64);
        new org.limitless.seqeron.sbe.frame.SequencedSystemHeaderEncoder().wrap(buffer, 0)
            .sourceId(SOURCE_ID).connectionId(CONNECTION_ID).sessionId(SESSION_ID).systemEventType(PAYLOAD_ID)
            .globalSeqNo(9).timestamp(TIMESTAMP);
        return copy(buffer, 0, SequencedSystemHeaderDecoder.ENCODED_LENGTH);
    }

    private static byte[] copy(final DirectBuffer buffer, final int offset, final int length) {
        final byte[] bytes = new byte[length];
        buffer.getBytes(offset, bytes, 0, length);
        return bytes;
    }
}
