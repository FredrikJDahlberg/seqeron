package org.limitless.phixeron.sequencer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.limitless.phixeron.sbe.frame.ClusterHeartbeatDecoder;
import org.limitless.phixeron.sbe.frame.ConnectionClosedDecoder;
import org.limitless.phixeron.sbe.frame.ConnectionOpenedDecoder;
import org.limitless.phixeron.sbe.frame.GatewayActiveDecoder;
import org.limitless.phixeron.sbe.frame.LeadershipChangedDecoder;
import org.limitless.phixeron.sbe.frame.MessageHeaderDecoder;

/**
 * Unit tests for the sequencer's replicated state machine.
 *
 * <p>These deliberately touch no Aeron runtime — no media driver, no cluster, no Aeron mocks. {@link
 * Sequencer} is a pure function of its inputs, so the whole state machine is exercised by calling it
 * and decoding the frames it writes, which is what makes these tests fast and stable enough to run
 * on every build. Everything Aeron-shaped ({@link SequencerService}'s tap publication, archive
 * recording, timer scheduling) stays covered by the end-to-end scripts under
 * {@code src/test/scripts}.
 *
 * <p>The invariants under test are the ones a replicated state machine cannot be allowed to break:
 * the same log produces byte-identical output on every node, {@code globalSeqNo} is gap-free across
 * every event type, and the copy-through preserves every byte of the payload past the
 * header.
 */
class SequencerTest {
    private static final int SOURCE_ID = 7;

    /** The second logical gateway, for the multi-pair cases: a distinct gatewaySourceId. */
    private static final int EXCHANGE_SOURCE_ID = 8;
    private static final int CONNECTION_ID = 42;
    private static final long SESSION_ID = 0x5EE51_0000L;
    private static final long TIMESTAMP = 1_700_000_000_000L;

    /**
     * An allocated payloadId the sequencer does not own — 3 is the FIX session family's — standing in
     * for any application protocol. Nothing here decodes one: the sequencer opens no payload, so this
     * suite may not either, and a test that reached for {@code sbe-session.xml}'s codecs would put an
     * application's dictionary on the cluster tier's compile classpath to assert it is never used.
     */
    private static final int SESSION_PAYLOAD_ID = 3;

    /**
     * The exemplar payload: opaque bytes with no SBE shape at all, which is exactly what the sequencer
     * sees. Every byte value in 0..255 appears, so a copy that dropped, sign-extended or reordered one
     * shows up as an array mismatch rather than a field that happens to still decode.
     */
    private static final byte[] OPAQUE_PAYLOAD = opaqueBytes(256);

    /** A second, differently-sized payload: the copy length is the only thing that varies here. */
    private static final byte[] SHORT_OPAQUE_PAYLOAD = opaqueBytes(37);

    /** unsequencedHeader is 18 bytes, sequencedHeader 34 — the delta every sequenced frame grows by. */
    private static final int HEADER_GROWTH =
        org.limitless.phixeron.sbe.frame.SequencedHeaderDecoder.ENCODED_LENGTH -
        org.limitless.phixeron.sbe.frame.UnsequencedHeaderDecoder.ENCODED_LENGTH;

    /** A payload's own 2-byte length prefix, the last thing before the payload in either family. */
    private static final int PAYLOAD_PREFIX_LENGTH =
        org.limitless.phixeron.sbe.frame.SequencedDecoder.payloadHeaderLength();

    /**
     * Bytes a {@code Sequenced} frame adds around its payload: the framing header, the 34-byte
     * {@code sequencedHeader} and the payload's own length prefix.
     */
    private static final int FRAME_OVERHEAD =
        org.limitless.phixeron.sbe.frame.MessageHeaderDecoder.ENCODED_LENGTH +
        org.limitless.phixeron.sbe.frame.SequencedHeaderDecoder.ENCODED_LENGTH + PAYLOAD_PREFIX_LENGTH;

    /** Offset of the payload's length prefix in an {@code Unsequenced} frame, and of the payload itself. */
    private static final int INGRESS_PREFIX_OFFSET = org.limitless.phixeron.sbe.frame.MessageHeaderDecoder.ENCODED_LENGTH +
                                                     org.limitless.phixeron.sbe.frame.UnsequencedHeaderDecoder.ENCODED_LENGTH;
    private static final int INGRESS_PAYLOAD_OFFSET =
        INGRESS_PREFIX_OFFSET + org.limitless.phixeron.sbe.frame.UnsequencedDecoder.payloadHeaderLength();

    /** {@code varDataEncoding}'s nullValue: "absent", not a 65535-byte payload. */
    private static final int NULL_PAYLOAD_LENGTH = 65535;

    private final Sequencer sequencer = new Sequencer();
    private final MutableDirectBuffer ingress = new ExpandableArrayBuffer(512);

    // ── Copy-through fidelity ─────────────────────────────────────────────────

    @Test
    @DisplayName("sequencing an ingress message copies every byte past the header composite")
    void sequenceMessagePreservesEveryBytePastTheHeader() {
        final int ingressLength = encodeIngressPayload(ingress, 0);

        final int length = sequencer.sequenceMessage(ingress, 0, ingressLength, SESSION_ID, TIMESTAMP);

        // Every byte value a payload can hold survives the copy unread and unmodified.
        assertPayloadCopiedThrough(sequencer.buffer(), length, OPAQUE_PAYLOAD);
    }

    @Test
    @DisplayName("sequencing stamps the header without disturbing the submitter's identity")
    void sequenceMessageStampsHeader() {
        final int ingressLength = encodeIngressPayload(ingress, 0);

        final int length = sequencer.sequenceMessage(ingress, 0, ingressLength, SESSION_ID, TIMESTAMP);

        final org.limitless.phixeron.sbe.frame.SequencedHeaderDecoder header = frameHeaderOf(sequencer.buffer());
        // Carried through from the ingress message…
        assertEquals(SOURCE_ID, header.sourceId());
        assertEquals(CONNECTION_ID, header.connectionId());
        // …and applied by the sequencer.
        assertEquals(SESSION_ID, header.sessionId());
        assertEquals(1L, header.globalSeqNo());
        assertEquals(TIMESTAMP, header.timestamp());
    }

    @Test
    @DisplayName("a payload of a different length survives the copy-through whole")
    void sequenceMessagePreservesADifferentlySizedPayload() {
        // Length is the only thing that varies between two payloads here, and it is the only thing the
        // copy arithmetic reads, so a second size is what exercises it. A payload is opaque: there is no
        // fixed block and no var-data to tell apart, only a prefix and that many bytes.
        final int ingressLength = encodeIngressPayload(ingress, 0, SHORT_OPAQUE_PAYLOAD);

        final int length = sequencer.sequenceMessage(ingress, 0, ingressLength, SESSION_ID, TIMESTAMP);

        assertPayloadCopiedThrough(sequencer.buffer(), length, SHORT_OPAQUE_PAYLOAD);
        // The whole ingress frame, minus nothing, plus the two int64s the sequenced header adds.
        assertEquals(ingressLength + HEADER_GROWTH, length);
    }

    @Test
    @DisplayName("the tap frame is a Sequenced envelope, its block exactly the header composite")
    void egressFrameIsASequencedEnvelope() {
        // The ingress and tap templates differ, and their blockLengths differ by exactly the stamp: the
        // envelope's block IS its header composite, so anything else here means the two are out of step.
        final int ingressLength = encodeIngressPayload(ingress, 0);
        final int ingressBlockLength = new MessageHeaderDecoder().wrap(ingress, 0).blockLength();

        sequencer.sequenceMessage(ingress, 0, ingressLength, SESSION_ID, TIMESTAMP);

        final MessageHeaderDecoder egress = new MessageHeaderDecoder().wrap(sequencer.buffer(), 0);
        assertEquals(ingressBlockLength + HEADER_GROWTH, egress.blockLength());
        assertEquals(org.limitless.phixeron.sbe.frame.SequencedDecoder.TEMPLATE_ID, egress.templateId());
        assertEquals(MessageHeaderDecoder.SCHEMA_ID, egress.schemaId());
    }

    @Test
    @DisplayName("a frame declaring a version this build does not encode is skipped")
    void frameFromAForeignVersionIsSkipped() {
        // The frame layer's own offsets are what every later read is measured from, and a version this
        // build has never encoded says they may not be where this code expects.
        final int ingressLength = encodeIngressPayload(ingress, 0);
        new org.limitless.phixeron.sbe.frame.MessageHeaderEncoder().wrap(ingress, 0)
            .version(MessageHeaderDecoder.SCHEMA_VERSION + 1);

        assertEquals(Sequencer.NO_FRAME, sequencer.sequenceMessage(ingress, 0, ingressLength, SESSION_ID, TIMESTAMP));
        assertEquals(0L, sequencer.globalSeqNo());
    }

    @Test
    @DisplayName("sequencing reads the ingress message at a non-zero offset")
    void sequenceMessageHonoursOffset() {
        // Aeron hands fragments at an arbitrary offset into a shared term buffer, never 0.
        final int offset = 96;
        final int ingressLength = encodeIngressPayload(ingress, offset);

        final int length = sequencer.sequenceMessage(ingress, offset, ingressLength, SESSION_ID, TIMESTAMP);

        assertPayloadCopiedThrough(sequencer.buffer(), length, OPAQUE_PAYLOAD);
        assertEquals(CONNECTION_ID, frameHeaderOf(sequencer.buffer()).connectionId());
    }

    // ── globalSeqNo ───────────────────────────────────────────────────────────

    @Test
    @DisplayName("globalSeqNo is gap-free and monotone across every event type")
    void globalSeqNoIsGapFreeAcrossEveryEventType() {
        // Forwarded ingress messages, clock heartbeats and elections all draw from the one counter; a
        // consumer that sees a gap treats it as lost data and re-walks history, so this must hold for
        // every emitting path. TCP lifecycle events are ordinary forwarded ingress — the gateway
        // publishes them — so they go through sequenceMessage here, not a synthesized encoder.
        final int ingressLength = encodeIngressPayload(ingress, 0);
        final MutableDirectBuffer lifecycle = new ExpandableArrayBuffer(64);

        final int connectedLength = encodeIngressConnectionOpened(lifecycle, 0);
        assertEquals(1L,
                     globalSeqNoOf(sequencer.sequenceMessage(lifecycle, 0, connectedLength, SESSION_ID, TIMESTAMP)));
        assertEquals(2L, globalSeqNoOf(sequencer.sequenceMessage(ingress, 0, ingressLength, SESSION_ID, TIMESTAMP)));
        assertEquals(3L, globalSeqNoOf(sequencer.clusterHeartbeat(TIMESTAMP + 1000)));
        assertEquals(4L, globalSeqNoOf(sequencer.leadershipChanged(2, TIMESTAMP + 1500)));
        assertEquals(5L, globalSeqNoOf(sequencer.sequenceMessage(ingress, 0, ingressLength, SESSION_ID, TIMESTAMP)));

        final int disconnectedLength = encodeIngressConnectionClosed(lifecycle, 0);
        assertEquals(
            6L,
            globalSeqNoOf(sequencer.sequenceMessage(lifecycle, 0, disconnectedLength, SESSION_ID, TIMESTAMP + 2000)));
        assertEquals(6L, sequencer.globalSeqNo());
    }

    @Test
    @DisplayName("a suppressed leadership event consumes no sequence number")
    void suppressedLeadershipEventConsumesNoSequenceNumber() {
        // Every node must consume the same count for the same log, so suppression has to happen before
        // the counter advances — not by encoding and discarding.
        sequencer.leadershipChanged(1, TIMESTAMP);
        assertEquals(1L, sequencer.globalSeqNo());

        assertEquals(Sequencer.NO_FRAME, sequencer.leadershipChanged(1, TIMESTAMP + 100));
        assertEquals(1L, sequencer.globalSeqNo());

        final int length = sequencer.leadershipChanged(2, TIMESTAMP + 200);
        assertNotEquals(Sequencer.NO_FRAME, length);
        assertEquals(2L, sequencer.globalSeqNo());
        final LeadershipChangedDecoder decoded = decodeLeadershipChanged(sequencer.buffer(), length);
        assertEquals(2, decoded.newLeaderMemberId());
        assertEquals(TIMESTAMP + 200, frameHeaderOf(sequencer.buffer()).timestamp());
        assertEquals(2, sequencer.currentLeaderMemberId());
    }

    // ── Ingress validation ────────────────────────────────────────────────────

    @Test
    @DisplayName("a frame too short to hold the header composite is skipped, not thrown on")
    void shortFrameIsSkippedRatherThanThrownOn() {
        // Throwing here would land on every node — the frame is already committed to the replicated log
        // — and again on every replay of it, leaving the log unreplayable and the cluster unrecoverable
        // without surgery (doc/review-2026-07-25.md #7). Before the length check this computed a
        // negative copyLength and threw out of putBytes.
        final int ingressLength = encodeIngressPayload(ingress, 0);

        assertEquals(Sequencer.NO_FRAME,
                     sequencer.sequenceMessage(ingress, 0, FrameLayer.MIN_INGRESS_LENGTH - 1, SESSION_ID, TIMESTAMP));
        assertEquals(0L, sequencer.globalSeqNo(), "a skipped message must consume no sequence number");

        // The next good message is still globalSeqNo 1: consumers detect loss by gaps, so a skip has to
        // leave the numbering contiguous rather than burn a number on a frame nobody will ever receive.
        assertEquals(1L, globalSeqNoOf(sequencer.sequenceMessage(ingress, 0, ingressLength, SESSION_ID, TIMESTAMP)));
    }

    @Test
    @DisplayName("a frame from a foreign schema is skipped")
    void foreignSchemaIsSkipped() {
        // Every offset sequenceMessage reads is a schema-210 offset; under another schema they address
        // something else entirely, so the frame is refused rather than re-stamped as if it were ours.
        final int ingressLength = encodeIngressPayload(ingress, 0);
        new org.limitless.phixeron.sbe.frame.MessageHeaderEncoder().wrap(ingress, 0).schemaId(999);

        assertEquals(Sequencer.NO_FRAME, sequencer.sequenceMessage(ingress, 0, ingressLength, SESSION_ID, TIMESTAMP));
        assertEquals(0L, sequencer.globalSeqNo());
    }

    // -- Frame validation ------------------------------------------------------
    // Each of these reads a field out of a frame the sequencer did not encode, and each check
    // establishes what the next may read. Admitting any of them decodes bytes whose shape nothing has
    // established -- and writes the result into authoritative, unreplayable history.

    @Test
    @DisplayName("a payload length that does not account for every byte is skipped")
    void payloadLengthThatDoesNotFitTheFrameIsSkipped() {
        // Short of the frame end silently drops the tail and re-emits the frame a size smaller; past it
        // reads beyond the fragment. Both are deterministic on every node, so neither would ever surface
        // as a divergence between them.
        final int length = encodeIngressConnectionOpened(ingress, 0);
        final int declared = ingress.getShort(INGRESS_PREFIX_OFFSET, java.nio.ByteOrder.LITTLE_ENDIAN) & 0xFFFF;

        putIngressPayloadLength(declared + 1);
        assertEquals(Sequencer.NO_FRAME, sequencer.sequenceMessage(ingress, 0, length, SESSION_ID, TIMESTAMP));
        putIngressPayloadLength(declared - 1);
        assertEquals(Sequencer.NO_FRAME, sequencer.sequenceMessage(ingress, 0, length, SESSION_ID, TIMESTAMP));
        putIngressPayloadLength(NULL_PAYLOAD_LENGTH);
        assertEquals(Sequencer.NO_FRAME, sequencer.sequenceMessage(ingress, 0, length, SESSION_ID, TIMESTAMP));

        putIngressPayloadLength(declared);
        assertNotEquals(Sequencer.NO_FRAME, sequencer.sequenceMessage(ingress, 0, length, SESSION_ID, TIMESTAMP));
        assertEquals(1L, sequencer.globalSeqNo(), "only the well-formed frame consumed a sequence number");
    }

    @Test
    @DisplayName("payloadId 0 is not a protocol and is skipped")
    void zeroPayloadIdIsSkipped() {
        final int length = encodeIngressConnectionOpened(ingress, 0);
        new org.limitless.phixeron.sbe.frame.UnsequencedHeaderEncoder()
            .wrap(ingress, org.limitless.phixeron.sbe.frame.MessageHeaderEncoder.ENCODED_LENGTH).payloadId(0);

        assertEquals(Sequencer.NO_FRAME, sequencer.sequenceMessage(ingress, 0, length, SESSION_ID, TIMESTAMP));
        assertEquals(0L, sequencer.globalSeqNo());
    }

    @Test
    @DisplayName("core's retired payloadId 1 is refused on ingress rather than copied through")
    void retiredCorePayloadIdIsRefusedOnIngress() {
        // Core is not an application and no longer rides a payload (§15 step 10). Refusing 1 rather than
        // reserving it is what makes a producer still on the old build fail loudly instead of having core
        // bytes copied onto the tap as an application payload nothing will ever decode.
        final int length = encodeIngressPayloadFrame(ingress, 0, SOURCE_ID, CONNECTION_ID, 1,
                                                     new ExpandableArrayBuffer(8), 4);

        assertEquals(Sequencer.NO_FRAME, sequencer.sequenceMessage(ingress, 0, length, SESSION_ID, TIMESTAMP));
        assertEquals(0L, sequencer.globalSeqNo());
    }

    @Test
    @DisplayName("a systemEventType with no ingress form is refused, template and all")
    void synthesisOnlyEventIsRefusedOnIngress() {
        // The cluster clock, the leadership record and the activation designation have no producer --
        // every node encodes its own copy (E-1's exception). Accepting one from a gateway would let it
        // forge any of the three, so both routes in are closed: the value at offset 16 (condition 8) and
        // the top-level template that carries it (condition 3).
        final MutableDirectBuffer body = new ExpandableArrayBuffer(64);
        final int asSystemBody = encodeIngressSystemFrame(ingress, 0, SOURCE_ID, CONNECTION_ID,
                                                          SystemFrame.CLUSTER_HEARTBEAT, body, 0);
        assertEquals(Sequencer.NO_FRAME, sequencer.sequenceMessage(ingress, 0, asSystemBody, SESSION_ID, TIMESTAMP));

        final org.limitless.phixeron.sbe.frame.ClusterHeartbeatEncoder encoder =
            new org.limitless.phixeron.sbe.frame.ClusterHeartbeatEncoder();
        encoder.wrapAndApplyHeader(ingress, 0, new org.limitless.phixeron.sbe.frame.MessageHeaderEncoder());
        encoder.header().sourceId(SOURCE_ID).connectionId(CONNECTION_ID).sessionId(-1)
            .systemEventType(SystemFrame.CLUSTER_HEARTBEAT).globalSeqNo(1).timestamp(TIMESTAMP);
        final int asOwnTemplate =
            org.limitless.phixeron.sbe.frame.MessageHeaderEncoder.ENCODED_LENGTH + encoder.encodedLength();
        assertEquals(Sequencer.NO_FRAME, sequencer.sequenceMessage(ingress, 0, asOwnTemplate, SESSION_ID, TIMESTAMP));

        assertEquals(0L, sequencer.globalSeqNo());
    }

    @Test
    @DisplayName("an unallocated systemEventType is refused")
    void unallocatedSystemEventTypeIsRefused() {
        final int length = encodeIngressSystemFrame(ingress, 0, SOURCE_ID, CONNECTION_ID, 4242,
                                                    new ExpandableArrayBuffer(8), 0);

        assertEquals(Sequencer.NO_FRAME, sequencer.sequenceMessage(ingress, 0, length, SESSION_ID, TIMESTAMP));
        assertEquals(0L, sequencer.globalSeqNo());
    }

    @Test
    @DisplayName("a system body too short for the field the sequencer reads is skipped")
    void systemBodyTooShortForItsBlockIsSkipped() {
        // Fitting the frame exactly says nothing about being long enough for a body field, and the floor
        // is the decoder's compiled block length -- never the wire's, which an SBE decoder ignores when it
        // reads a fixed-width field at its fixed offset.
        encodeIngressGatewayStarted(ingress, 0, 5);
        final int shortBody = org.limitless.phixeron.sbe.frame.GatewayStartedDecoder.BLOCK_LENGTH - 1;
        putIngressPayloadLength(shortBody);

        assertEquals(Sequencer.NO_FRAME, sequencer.sequenceMessage(ingress, 0, INGRESS_PAYLOAD_OFFSET + shortBody,
                                                                   SESSION_ID, TIMESTAMP));
        assertEquals(0L, sequencer.globalSeqNo());
    }

    @Test
    @DisplayName("a payload the sequencer does not own is sequenced without being decoded")
    void foreignPayloadIsSequencedOpaque() {
        // Selective consumption: core is the one payloadId the cluster tier decodes, and every other is
        // carried byte-identical. Nothing about a foreign payload -- not its length, not its contents --
        // may decide whether the frame is admitted.
        final byte[] opaque = { 0x00, (byte)0xFF, 0x7F, (byte)0x80, 0x01 };
        final MutableDirectBuffer payload = new ExpandableArrayBuffer(64);
        payload.putBytes(0, opaque);
        final org.limitless.phixeron.sbe.frame.UnsequencedEncoder encoder = new org.limitless.phixeron.sbe.frame.UnsequencedEncoder();
        encoder.wrapAndApplyHeader(ingress, 0, new org.limitless.phixeron.sbe.frame.MessageHeaderEncoder());
        encoder.header().sourceId(SOURCE_ID).connectionId(CONNECTION_ID).sessionId(-1).payloadId(4);
        encoder.putPayload(payload, 0, opaque.length);
        final int length = org.limitless.phixeron.sbe.frame.MessageHeaderEncoder.ENCODED_LENGTH + encoder.encodedLength();

        final int sequenced = sequencer.sequenceMessage(ingress, 0, length, SESSION_ID, TIMESTAMP);

        assertNotEquals(Sequencer.NO_FRAME, sequenced);
        assertEquals(4, frameHeaderOf(sequencer.buffer()).payloadId(), "the payloadId is carried, not rewritten");
        final byte[] roundTripped = new byte[opaque.length];
        sequencer.buffer().getBytes(FRAME_OVERHEAD, roundTripped);
        assertArrayEquals(opaque, roundTripped);
        assertEquals(FRAME_OVERHEAD + opaque.length, sequenced);
    }

    @Test
    @DisplayName("a blockLength that is not the header composite's exact length is skipped")
    void blockLengthThatIsNotTheHeaderCompositeIsSkipped() {
        // An equality, not a floor: the envelope's block IS its header composite, so a short blockLength
        // puts the payload's length prefix inside the header and a long one silently drops bytes off the
        // end. Either is written into authoritative, unreplayable history.
        final int ingressLength = encodeIngressPayload(ingress, 0);
        final int exact = org.limitless.phixeron.sbe.frame.UnsequencedHeaderDecoder.ENCODED_LENGTH;

        new org.limitless.phixeron.sbe.frame.MessageHeaderEncoder().wrap(ingress, 0).blockLength(exact - 1);
        assertEquals(Sequencer.NO_FRAME, sequencer.sequenceMessage(ingress, 0, ingressLength, SESSION_ID, TIMESTAMP));
        new org.limitless.phixeron.sbe.frame.MessageHeaderEncoder().wrap(ingress, 0).blockLength(exact + 1);
        assertEquals(Sequencer.NO_FRAME, sequencer.sequenceMessage(ingress, 0, ingressLength, SESSION_ID, TIMESTAMP));
        assertEquals(0L, sequencer.globalSeqNo(), "a skipped message must consume no sequence number");

        new org.limitless.phixeron.sbe.frame.MessageHeaderEncoder().wrap(ingress, 0).blockLength(exact);
        assertNotEquals(Sequencer.NO_FRAME, sequencer.sequenceMessage(ingress, 0, ingressLength, SESSION_ID,
                                                                      TIMESTAMP));
        assertEquals(1L, sequencer.globalSeqNo());
    }

    @Test
    @DisplayName("a frame above the payload ceiling is skipped, and one exactly at it is sequenced")
    void oversizedFrameIsSkipped() {
        // T-2's pinned constant, and condition 1's ceiling as the backstop behind it: a conforming producer
        // refuses this on its own stack, so a frame that reaches here is one that is not conforming. The
        // ceiling is compiled in and never read off this node's MTU -- a node checking a smaller one than
        // its peers would fork globalSeqNo.
        final MutableDirectBuffer payload = new ExpandableArrayBuffer(FrameLayer.MAX_PAYLOAD_LENGTH + 1);
        final int exact = encodeIngressPayloadFrame(ingress, 0, SOURCE_ID, CONNECTION_ID, SESSION_PAYLOAD_ID,
                                                    payload, FrameLayer.MAX_PAYLOAD_LENGTH);
        assertEquals(FrameLayer.MAX_INGRESS_LENGTH, exact);
        assertNotEquals(Sequencer.NO_FRAME, sequencer.sequenceMessage(ingress, 0, exact, SESSION_ID, TIMESTAMP));

        final int oversized = encodeIngressPayloadFrame(ingress, 0, SOURCE_ID, CONNECTION_ID, SESSION_PAYLOAD_ID,
                                                        payload, FrameLayer.MAX_PAYLOAD_LENGTH + 1);
        assertEquals(Sequencer.NO_FRAME, sequencer.sequenceMessage(ingress, 0, oversized, SESSION_ID, TIMESTAMP));
        assertEquals(1L, sequencer.globalSeqNo(), "a skipped message must consume no sequence number");
    }

    @Test
    @DisplayName("the sourceId reserved for the cluster's own frames is refused on ingress")
    void reservedSourceIdIsSkipped() {
        // -1 marks the synthesized class (F-4). Admitting one from a producer would let it forge that
        // class -- a heartbeat or a leadership record no node encoded.
        final MutableDirectBuffer payload = new ExpandableArrayBuffer(64);
        final int length = encodeIngressPayloadFrame(ingress, 0, Sequencer.NO_SOURCE_ID, CONNECTION_ID,
                                                     SESSION_PAYLOAD_ID, payload, 4);

        assertEquals(Sequencer.NO_FRAME, sequencer.sequenceMessage(ingress, 0, length, SESSION_ID, TIMESTAMP));
        assertEquals(0L, sequencer.globalSeqNo());
    }

    @Test
    @DisplayName("S-6: a core frame claims a listed sourceId only from a session bound to it")
    void listedSourceIdNeedsABoundSession() {
        // Without this a process that is not the gateway can speak for its logical gateway: publish a
        // GatewayStarted under someone else's sourceId and the sequencer promotes on its session close.
        sequencer.sequenceMessage(ingress, 0, encodeIngressGatewayRegistered(ingress, 0, 5, SOURCE_ID, "GW-A", 0, 0),
                                  SESSION_ID, TIMESTAMP);
        final long afterList = sequencer.globalSeqNo();
        final long rogue = SESSION_ID + 1;

        // Case 1's second half: the row's gatewaySourceId must be the one the frame carries.
        assertEquals(Sequencer.NO_FRAME,
                     sequencer.sequenceMessage(ingress, 0, encodeIngressGatewayStarted(ingress, 0, 5,
                                                                                       EXCHANGE_SOURCE_ID),
                                               rogue, TIMESTAMP));
        // Case 2: no binding, so this session may not speak for that logical gateway.
        assertEquals(Sequencer.NO_FRAME,
                     sequencer.sequenceMessage(ingress, 0, encodeIngressConnectionOpened(ingress, 0, 1), rogue,
                                               TIMESTAMP));
        // Case 3: the same sourceId under an application payload is not core's to check.
        assertNotEquals(Sequencer.NO_FRAME,
                        sequencer.sequenceMessage(ingress, 0,
                                                  encodeIngressPayloadFrame(ingress, 0, SOURCE_ID, CONNECTION_ID,
                                                                            SESSION_PAYLOAD_ID,
                                                                            new ExpandableArrayBuffer(8), 4),
                                                  rogue, TIMESTAMP));

        // The agreeing GatewayStarted binds, and only the session that made the binding is admitted.
        assertNotEquals(Sequencer.NO_FRAME,
                        sequencer.sequenceMessage(ingress, 0, encodeIngressGatewayStarted(ingress, 0, 5), rogue,
                                                  TIMESTAMP));
        assertNotEquals(Sequencer.NO_FRAME,
                        sequencer.sequenceMessage(ingress, 0, encodeIngressConnectionOpened(ingress, 0, 1), rogue,
                                                  TIMESTAMP));
        assertEquals(Sequencer.NO_FRAME,
                     sequencer.sequenceMessage(ingress, 0, encodeIngressConnectionOpened(ingress, 0, 2),
                                               SESSION_ID + 2, TIMESTAMP));
        assertEquals(afterList + 3, sequencer.globalSeqNo(), "every rejection left globalSeqNo where it was");
    }

    // ── Determinism ───────────────────────────────────────────────────────────

    @Test
    @DisplayName("replaying the same log on a second instance produces byte-identical frames")
    void replayingTheSameLogProducesByteIdenticalFrames() {
        // The core replicated-state-machine property: two nodes fed the same committed log must emit
        // the same bytes, so their tap recordings are interchangeable and any node can serve replay.
        // This is a regression guard — it fails the moment anything non-deterministic (a wall clock, a
        // random id, an iteration-order dependency) creeps into an encode path.
        final Sequencer nodeA = new Sequencer();
        final Sequencer nodeB = new Sequencer();

        final byte[][] framesA = runLog(nodeA);
        final byte[][] framesB = runLog(nodeB);

        assertEquals(framesA.length, framesB.length);
        for (int i = 0; i < framesA.length; i++) {
            assertArrayEquals(framesA[i], framesB[i], "frame " + i + " diverged between nodes");
        }
        assertEquals(nodeA.globalSeqNo(), nodeB.globalSeqNo());
        assertEquals(nodeA.currentLeaderMemberId(), nodeB.currentLeaderMemberId());
    }

    /** Drives one fixed "committed log" through a sequencer, returning every frame it emitted. */
    private byte[][] runLog(final Sequencer target) {
        final MutableDirectBuffer message = new ExpandableArrayBuffer(512);
        final int messageLength = encodeIngressPayload(message, 0);
        final MutableDirectBuffer lifecycle = new ExpandableArrayBuffer(64);
        final int connectedLength = encodeIngressConnectionOpened(lifecycle, 0);
        final java.util.List<byte[]> frames = new java.util.ArrayList<>();

        collect(frames, target, target.sequenceMessage(lifecycle, 0, connectedLength, SESSION_ID, TIMESTAMP));
        collect(frames, target, target.leadershipChanged(0, TIMESTAMP + 1));
        for (int i = 0; i < 5; i++) {
            collect(frames, target, target.sequenceMessage(message, 0, messageLength, SESSION_ID, TIMESTAMP + i));
            collect(frames, target, target.clusterHeartbeat(TIMESTAMP + 1000L * i));
        }
        collect(frames, target, target.leadershipChanged(0, TIMESTAMP + 9)); // suppressed
        collect(frames, target, target.leadershipChanged(1, TIMESTAMP + 10));
        final int disconnectedLength = encodeIngressConnectionClosed(lifecycle, 0);
        collect(frames, target, target.sequenceMessage(lifecycle, 0, disconnectedLength, SESSION_ID, TIMESTAMP + 11));
        return frames.toArray(new byte[0][]);
    }

    private static void collect(final java.util.List<byte[]> frames, final Sequencer target, final int length) {
        if (length == Sequencer.NO_FRAME) {
            return;
        }
        final byte[] frame = new byte[length];
        target.buffer().getBytes(0, frame);
        frames.add(frame);
    }

    // ── Snapshot ──────────────────────────────────────────────────────────────

    @Test
    @DisplayName("taking a snapshot is refused rather than persisting part of the replicated state")
    void takingASnapshotIsRefused() {
        // The one SequencerService call these tests make, and it starts no Aeron runtime: the throw
        // precedes any use of the publication, which is why null is safe to pass. This used to persist
        // globalSeqNo alone and drop the six other replicated fields, so a restored node re-emitted the
        // bootstrap GatewayActive and stopped producing frames identical to its peers'
        // (doc/review-2026-07-25.md #5). Recovery is full-log replay, which rebuilds all of it.
        assertThrows(UnsupportedOperationException.class, () -> new SequencerService(() -> { }).onTakeSnapshot(null));
    }

    // ── Lifecycle and clock frames ────────────────────────────────────────────

    @Test
    @DisplayName("TCP lifecycle frames carry the connection they describe")
    void tcpLifecycleFramesCarryTheConnectionTheyDescribe() {
        // These name an external event — a FIX client's TCP connection opening and closing — so unlike
        // a heartbeat or an election they have a real gateway process and connection behind them, and both
        // ids must survive sequencing. Without them a consumer can see that *a* session ended but not
        // which, which is the whole reason the gateway publishes these rather than the sequencer
        // synthesizing a cluster-session event under the same template ids.
        final MutableDirectBuffer lifecycle = new ExpandableArrayBuffer(64);

        final int connectedLength =
            sequencer.sequenceMessage(lifecycle, 0, encodeIngressConnectionOpened(lifecycle, 0), SESSION_ID, TIMESTAMP);
        assertEquals(SystemFrame.CONNECTION_OPENED, systemEventTypeOf(sequencer.buffer(), connectedLength));
        // The identity is the frame's, not the message's: a system body carries no header of its own.
        org.limitless.phixeron.sbe.frame.SequencedSystemHeaderDecoder header = systemHeaderOf(sequencer.buffer());
        assertEquals(SOURCE_ID, header.sourceId());
        assertEquals(CONNECTION_ID, header.connectionId());
        assertEquals(SESSION_ID, header.sessionId());
        assertEquals(1L, header.globalSeqNo());
        assertEquals(TIMESTAMP, header.timestamp());
        // An empty body copies zero bytes through — the degenerate end of the copy-through path every
        // other message exercises with a body.
        assertEquals(FRAME_OVERHEAD + ConnectionOpenedDecoder.BLOCK_LENGTH +
                     ConnectionOpenedDecoder.connectionDataHeaderLength(), connectedLength);

        final int disconnectedLength = sequencer.sequenceMessage(
            lifecycle, 0, encodeIngressConnectionClosed(lifecycle, 0), SESSION_ID, TIMESTAMP + 1);
        assertEquals(SystemFrame.CONNECTION_CLOSED, systemEventTypeOf(sequencer.buffer(), disconnectedLength));
        header = systemHeaderOf(sequencer.buffer());
        assertEquals(CONNECTION_ID, header.connectionId());
        assertEquals(TIMESTAMP + 1, header.timestamp());
        assertEquals(SESSION_ID, header.sessionId());
        assertEquals(FRAME_OVERHEAD + ConnectionClosedDecoder.BLOCK_LENGTH, disconnectedLength);
    }

    @Test
    @DisplayName("heartbeat carries the consensus timestamp and no session")
    void heartbeatCarriesConsensusTimestamp() {
        // The gateway's keepalive watchdog reads exactly this field to advance its session clock while
        // a counterparty is silent, so a heartbeat that lost its timestamp would stall every watchdog.
        final long heartbeatTime = TIMESTAMP + 60_000;
        final int length = sequencer.clusterHeartbeat(heartbeatTime);

        final MessageHeaderDecoder framing = new MessageHeaderDecoder().wrap(sequencer.buffer(), 0);
        assertEquals(ClusterHeartbeatDecoder.TEMPLATE_ID, framing.templateId());
        final org.limitless.phixeron.sbe.frame.SequencedSystemHeaderDecoder header = systemHeaderOf(sequencer.buffer());
        assertEquals(SystemFrame.CLUSTER_HEARTBEAT, header.systemEventType());
        assertEquals(heartbeatTime, header.timestamp());
        assertEquals(Sequencer.NO_SOURCE_ID, header.sessionId());
        assertEquals(1L, header.globalSeqNo());
        // The cheapest frame in the system: a framing header and the stamp, with no body at all.
        assertEquals(MessageHeaderDecoder.ENCODED_LENGTH + ClusterHeartbeatDecoder.BLOCK_LENGTH, length);
        assertEquals(42, length);
    }

    @Test
    @DisplayName("connectedClientCount tracks ConnectionOpened/ConnectionClosed pairs off the log")
    void connectedClientCountTracksLifecycleFrames() {
        final Sequencer seq = new Sequencer();
        final MutableDirectBuffer lifecycle = new ExpandableArrayBuffer(64);
        assertEquals(0, seq.connectedClientCount());

        seq.sequenceMessage(lifecycle, 0, encodeIngressConnectionOpened(lifecycle, 0, 1), SESSION_ID, TIMESTAMP);
        assertEquals(1, seq.connectedClientCount());

        seq.sequenceMessage(lifecycle, 0, encodeIngressConnectionOpened(lifecycle, 0, 2), SESSION_ID + 1, TIMESTAMP);
        assertEquals(2, seq.connectedClientCount());

        seq.sequenceMessage(lifecycle, 0, encodeIngressConnectionClosed(lifecycle, 0, 1), SESSION_ID, TIMESTAMP + 1);
        assertEquals(1, seq.connectedClientCount());

        // Ordinary application traffic must not perturb the count.
        final int orderLength = encodeIngressPayload(ingress, 0);
        seq.sequenceMessage(ingress, 0, orderLength, SESSION_ID + 1, TIMESTAMP + 2);
        assertEquals(1, seq.connectedClientCount());
    }

    @Test
    @DisplayName("connectedClientCount is a live gauge, not a running tally that drifts")
    void connectedClientCountIsALiveGauge() {
        // What a tally got wrong: a gateway that dies with clients attached publishes no
        // ConnectionClosed for any of them, so its connects stay counted for the rest of the day.
        // GatewayStarted is its successor declaring the epoch rolled — every connection still open under
        // that logical gateway belonged to the instance that went away, and died with its sockets.
        final Sequencer seq = new Sequencer();
        final MutableDirectBuffer buf = new ExpandableArrayBuffer(128);
        // S-6 wants a list to check the two GatewayStarteds against, and a bound session under each: a
        // ConnectionOpened claiming a listed sourceId is only admitted on one.
        seq.sequenceMessage(buf, 0, encodeIngressGatewayRegistered(buf, 0, 5, SOURCE_ID, "GW-A", 0, 0), SESSION_ID,
                            TIMESTAMP);
        seq.sequenceMessage(buf, 0, encodeIngressGatewayStarted(buf, 0, 5), SESSION_ID, TIMESTAMP);
        seq.sequenceMessage(buf, 0, encodeIngressConnectionOpened(buf, 0, 1), SESSION_ID, TIMESTAMP);
        seq.sequenceMessage(buf, 0, encodeIngressConnectionOpened(buf, 0, 2), SESSION_ID, TIMESTAMP);
        assertEquals(2, seq.connectedClientCount());

        seq.sequenceMessage(buf, 0, encodeIngressGatewayStarted(buf, 0, 5), SESSION_ID + 1, TIMESTAMP + 1);
        assertEquals(0, seq.connectedClientCount(), "the dead instance's connections are not still connected");

        // The successor's own connection counts, and counts once however many times the frame arrives.
        seq.sequenceMessage(buf, 0, encodeIngressConnectionOpened(buf, 0, 1), SESSION_ID + 1, TIMESTAMP + 2);
        seq.sequenceMessage(buf, 0, encodeIngressConnectionOpened(buf, 0, 1), SESSION_ID + 1, TIMESTAMP + 3);
        assertEquals(1, seq.connectedClientCount());

        // A disconnect matching nothing open cannot take the gauge below zero.
        seq.sequenceMessage(buf, 0, encodeIngressConnectionClosed(buf, 0, 1), SESSION_ID + 1, TIMESTAMP + 4);
        seq.sequenceMessage(buf, 0, encodeIngressConnectionClosed(buf, 0, 1), SESSION_ID + 1, TIMESTAMP + 5);
        assertEquals(0, seq.connectedClientCount());
    }

    // ── Standby promotion (GatewayActive) ─────────────────────────────────────

    @Test
    @DisplayName("the list's last row is followed by a bootstrap GatewayActive naming the rank-0 primary")
    void listsLastRowSynthesizesBootstrapActivation() {
        // Cold-start designation: one instance must open its gate and the standby must wait, so the
        // cluster names the primary's gatewayId behind the row that completes the list (doc/todo.md
        // item 18). The primary is derived from those rows — the rank-0 one — not configured.
        final int primaryGatewayId = 5;
        final Sequencer seq = new Sequencer();
        final MutableDirectBuffer buf = new ExpandableArrayBuffer(128);

        assertEquals(Sequencer.NO_FRAME, seq.pendingGatewayActivation(TIMESTAMP),
                     "nothing pending before the list");

        // gatewayId 5 (rank 0) is the primary, 6 (rank 1) the standby; remaining counts down to 0.
        seq.sequenceMessage(buf, 0, encodeIngressGatewayRegistered(buf, 0, primaryGatewayId, SOURCE_ID, "GW-A", 0, 1),
                            SESSION_ID, TIMESTAMP);
        assertEquals(Sequencer.NO_FRAME, seq.pendingGatewayActivation(TIMESTAMP),
                     "an incomplete list designates nobody — remaining is the whole completeness edge");

        final int endLength = seq.sequenceMessage(
            buf, 0, encodeIngressGatewayRegistered(buf, 0, 6, SOURCE_ID, "GW-B", 1, 0), SESSION_ID, TIMESTAMP);
        assertEquals(2L, globalSeqNoOf(seq, endLength)); // 2 list rows

        final int activationLength = seq.pendingGatewayActivation(TIMESTAMP + 1);
        assertNotEquals(Sequencer.NO_FRAME, activationLength);
        final GatewayActiveDecoder decoded = decodeGatewayActive(seq.buffer(), activationLength);
        assertEquals(primaryGatewayId, decoded.gatewayId()); // the rank-0 gatewayId, derived from the log
        // globalSeqNo, timestamp and provenance are the frame's; a core payload carries no header at all.
        assertEquals(3L, frameHeaderOf(seq.buffer()).globalSeqNo()); // the next globalSeqNo after the last row
        assertEquals(TIMESTAMP + 1, frameHeaderOf(seq.buffer()).timestamp());
        assertEquals(Sequencer.NO_SOURCE_ID, frameHeaderOf(seq.buffer()).sourceId()); // synthesized: no submitter

        // Fires once: re-running load-topology re-asserts the rows without re-designating the primary.
        seq.sequenceMessage(buf, 0, encodeIngressGatewayRegistered(buf, 0, 6, SOURCE_ID, "GW-B", 1, 0), SESSION_ID,
                            TIMESTAMP + 2);
        assertEquals(Sequencer.NO_FRAME, seq.pendingGatewayActivation(TIMESTAMP + 3));
    }

    @Test
    @DisplayName("an operator's GatewayActivationRequested is forwarded and answered with a synthesized GatewayActive")
    void activationRequestIsForwardedAndAnswered() {
        // The operator's act is the fact, and the designation stays the cluster's: the request is
        // sequenced like any other submitted event, and the GatewayActive answering it comes out of the
        // same path bootstrap and both promotions take — so no SequencedSystem is left without an
        // UnsequencedSystem antecedent, and the manual path gets the list validation the other three
        // have from iterating the list.
        final Sequencer seq = new Sequencer();
        final MutableDirectBuffer buf = new ExpandableArrayBuffer(512);
        seq.sequenceMessage(buf, 0, encodeIngressGatewayRegistered(buf, 0, 5, SOURCE_ID, "GW-A", 0, 1), SESSION_ID,
                            TIMESTAMP);
        seq.sequenceMessage(buf, 0, encodeIngressGatewayRegistered(buf, 0, 6, SOURCE_ID, "GW-B", 1, 0), SESSION_ID,
                            TIMESTAMP);
        assertNotEquals(Sequencer.NO_FRAME, seq.pendingGatewayActivation(TIMESTAMP)); // bootstrap designates 5
        assertEquals(3L, seq.globalSeqNo());

        final int requestLength = seq.sequenceMessage(
            buf, 0, encodeIngressActivationRequested(buf, 0, 6), SESSION_ID, TIMESTAMP + 1);
        assertNotEquals(Sequencer.NO_FRAME, requestLength, "the request is a submitted event and is forwarded");
        assertEquals(SystemFrame.GATEWAY_ACTIVATION_REQUESTED, systemEventTypeOf(seq.buffer(), requestLength));
        assertEquals(4L, seq.globalSeqNo());

        final int activation = seq.pendingGatewayActivation(TIMESTAMP + 2);
        assertNotEquals(Sequencer.NO_FRAME, activation);
        assertEquals(6, decodeGatewayActive(seq.buffer(), activation).gatewayId());
        assertEquals(5L, systemHeaderOf(seq.buffer()).globalSeqNo(), "the answer is one frame behind the request");
        assertEquals(Sequencer.NO_SOURCE_ID, systemHeaderOf(seq.buffer()).sourceId(), "synthesized: no submitter");
        assertEquals(Sequencer.NO_FRAME, seq.pendingGatewayActivation(TIMESTAMP + 3), "one activation, once");

        // And it arms the deadline like every other synthesized activation: instance 6 has to answer.
        assertEquals(5, decodeGatewayActive(
            seq.buffer(),
            seq.pendingGatewayActivationTimeout(TIMESTAMP + 2 + Sequencer.GATEWAY_ACTIVATION_TIMEOUT_MS)).gatewayId());
    }

    @Test
    @DisplayName("a GatewayActivationRequested naming an instance no list row does is rejected")
    void activationRequestForAnUnknownInstanceIsRejected() {
        // The validation the manual path lacked while it published GatewayActive directly: an operator
        // typo used to put a designation nothing could answer into unreplayable history.
        final Sequencer seq = new Sequencer();
        final MutableDirectBuffer buf = new ExpandableArrayBuffer(512);
        seq.sequenceMessage(buf, 0, encodeIngressGatewayRegistered(buf, 0, 5, SOURCE_ID, "GW-A", 0, 0), SESSION_ID,
                            TIMESTAMP);
        seq.pendingGatewayActivation(TIMESTAMP);
        final long globalSeqNo = seq.globalSeqNo();

        assertEquals(Sequencer.NO_FRAME,
                     seq.sequenceMessage(buf, 0, encodeIngressActivationRequested(buf, 0, 99), SESSION_ID,
                                         TIMESTAMP + 1));
        assertEquals(globalSeqNo, seq.globalSeqNo(), "a rejected request consumes no sequence number");
        assertEquals(Sequencer.NO_FRAME, seq.pendingGatewayActivation(TIMESTAMP + 2), "and designates nobody");
    }

    @Test
    @DisplayName("a complete list with no rank-0 row designates no primary and synthesizes no activation")
    void listWithNoPrimaryFailsClosed() {
        // The empty list this replaces stopped being expressible when the completeness edge became a
        // countdown: with no rows there is no remaining==0 to fire on, so the latch stays unset and a
        // later list still elects. clusterctl refuses to publish either shape; the sequencer's own
        // answer to a rank-0-less list that reached it anyway is still to activate nobody.
        final Sequencer seq = new Sequencer();
        final MutableDirectBuffer buf = new ExpandableArrayBuffer(128);
        seq.sequenceMessage(buf, 0, encodeIngressGatewayRegistered(buf, 0, 5, SOURCE_ID, "GW-A", 1, 1), SESSION_ID,
                            TIMESTAMP);
        seq.sequenceMessage(buf, 0, encodeIngressGatewayRegistered(buf, 0, 6, SOURCE_ID, "GW-B", 2, 0), SESSION_ID,
                            TIMESTAMP);
        assertEquals(Sequencer.NO_FRAME, seq.pendingGatewayActivation(TIMESTAMP + 1),
                     "no rank-0 row ⇒ no bootstrap activation (fail closed)");
    }

    @Test
    @DisplayName("closing the active gateway's session promotes the next-ranked sibling, named by gatewayId")
    void closingGatewaySessionPromotesStandby() {
        final Sequencer seq = new Sequencer();
        final MutableDirectBuffer buf = new ExpandableArrayBuffer(512);

        // One logical gateway (SOURCE_ID) served by an active/standby pair: gatewayId 5 rank 0, 6 rank 1.
        seq.sequenceMessage(buf, 0, encodeIngressGatewayRegistered(buf, 0, 5, SOURCE_ID, "GW-A", 0, 1), SESSION_ID,
                            TIMESTAMP);
        seq.sequenceMessage(buf, 0, encodeIngressGatewayRegistered(buf, 0, 6, SOURCE_ID, "GW-B", 1, 0), SESSION_ID,
                            TIMESTAMP);

        // GatewayStarted is how instance 5 declares which cluster session it is active on.
        final long gatewaySession = 0xA11CEL;
        seq.sequenceMessage(buf, 0, encodeIngressGatewayStarted(buf, 0, 5), gatewaySession, TIMESTAMP);

        // A session no gateway ever claimed is not one — its close promotes nothing.
        assertEquals(Sequencer.NO_FRAME, seq.sessionClosed(0xBEEFL, TIMESTAMP + 1));

        final int promotionLength = seq.sessionClosed(gatewaySession, TIMESTAMP + 2);
        assertNotEquals(Sequencer.NO_FRAME, promotionLength);
        final GatewayActiveDecoder decoded = decodeGatewayActive(seq.buffer(), promotionLength);
        // The standby's gatewayId — never the gatewaySourceId. Both instances share the sourceId, so a
        // promotion carrying it designated both at once, and the consumer could only survive that by
        // latching its first match, which made a restarting instance re-activate off a superseded frame.
        assertEquals(6, decoded.gatewayId());
        assertNotEquals(SOURCE_ID, decoded.gatewayId());
        assertEquals(4L, frameHeaderOf(seq.buffer()).globalSeqNo()); // 2 rows + GatewayStarted + promotion
        assertEquals(TIMESTAMP + 2, frameHeaderOf(seq.buffer()).timestamp());

        // The session is forgotten: a duplicate close does not re-promote.
        assertEquals(Sequencer.NO_FRAME, seq.sessionClosed(gatewaySession, TIMESTAMP + 3));
    }

    @Test
    @DisplayName("a session that merely echoes a gateway sourceId is not a gateway session")
    void echoingAGatewaySourceIdDoesNotMakeASessionAGateway() {
        // header.sourceId is a *routing* id, not a claim of identity: the OrderExecServer stamps the
        // originating gateway's sourceId onto every ExecutionReport and PortfolioQueryReply it submits.
        // Inferring "this session is a gateway" from it made an ordinary OrderExecServer restart promote
        // the standby out from under a healthy primary. Only GatewayStarted may claim a session.
        final Sequencer seq = new Sequencer();
        final MutableDirectBuffer buf = new ExpandableArrayBuffer(512);
        seq.sequenceMessage(buf, 0, encodeIngressGatewayRegistered(buf, 0, 5, SOURCE_ID, "GW-A", 0, 1), SESSION_ID,
                            TIMESTAMP);
        seq.sequenceMessage(buf, 0, encodeIngressGatewayRegistered(buf, 0, 6, SOURCE_ID, "GW-B", 1, 0), SESSION_ID,
                            TIMESTAMP);

        final long gatewaySession = 0xA11CEL;
        seq.sequenceMessage(buf, 0, encodeIngressGatewayStarted(buf, 0, 5), gatewaySession, TIMESTAMP);

        // The Heartbeat carries SOURCE_ID — a known gateway sourceId — on a session that is not a gateway.
        final long orderExecSession = 0x0EC1E47L;
        seq.sequenceMessage(buf, 0, encodeIngressPayload(buf, 0), orderExecSession, TIMESTAMP + 1);

        assertEquals(Sequencer.NO_FRAME, seq.sessionClosed(orderExecSession, TIMESTAMP + 2),
                     "echoing a gateway sourceId must not make a session promotable");
        assertNotEquals(Sequencer.NO_FRAME, seq.sessionClosed(gatewaySession, TIMESTAMP + 3),
                        "the session GatewayStarted claimed is still the one that promotes");
    }

    @Test
    @DisplayName("closing the only instance of a logical gateway promotes nothing")
    void soleGatewayInstanceHasNoStandbyToPromote() {
        // Fail closed: naming a nonexistent instance would activate no one while burning a globalSeqNo,
        // and naming the sourceId (as this used to) would activate everyone.
        final Sequencer seq = new Sequencer();
        final MutableDirectBuffer buf = new ExpandableArrayBuffer(512);
        seq.sequenceMessage(buf, 0, encodeIngressGatewayRegistered(buf, 0, 5, SOURCE_ID, "GW-A", 0, 0), SESSION_ID,
                            TIMESTAMP);

        final long gatewaySession = 0xA11CEL;
        seq.sequenceMessage(buf, 0, encodeIngressGatewayStarted(buf, 0, 5), gatewaySession, TIMESTAMP);

        assertEquals(Sequencer.NO_PROMOTION_TARGET, seq.sessionClosed(gatewaySession, TIMESTAMP + 1),
                     "distinct from NO_FRAME: this WAS a gateway session, just one with no standby");
        assertEquals(2L, seq.globalSeqNo(), "a promotion with no target consumes no sequence number");
    }

    // ── Activation deadline (a designated instance that never declares itself started) ────────────

    @Test
    @DisplayName("a designated primary that never declares itself started is handed over to the standby")
    void designatedPrimaryThatNeverStartsIsHandedOver() {
        // The gap this closes (review-3.md #6's follow-up): only GatewayStarted registers an instance,
        // and it is published at gate-open, AFTER catch-up. An instance that dies or wedges in cold start
        // was never registered, so no session close could ever promote past it — the cluster kept a
        // designated primary that was never going to serve, with nothing to say so.
        final Sequencer seq = new Sequencer();
        final MutableDirectBuffer buf = new ExpandableArrayBuffer(512);
        assertEquals(Sequencer.NO_FRAME, seq.pendingGatewayActivationTimeout(TIMESTAMP),
                     "nothing designated ⇒ no deadline to miss");

        seq.sequenceMessage(buf, 0, encodeIngressGatewayRegistered(buf, 0, 5, SOURCE_ID, "GW-A", 0, 1), SESSION_ID,
                            TIMESTAMP);
        seq.sequenceMessage(buf, 0, encodeIngressGatewayRegistered(buf, 0, 6, SOURCE_ID, "GW-B", 1, 0), SESSION_ID,
                            TIMESTAMP);
        assertNotEquals(Sequencer.NO_FRAME, seq.pendingGatewayActivation(TIMESTAMP)); // designates 5

        final long deadline = TIMESTAMP + Sequencer.GATEWAY_ACTIVATION_TIMEOUT_MS;
        assertEquals(Sequencer.NO_FRAME, seq.pendingGatewayActivationTimeout(deadline - 1),
                     "a cold start still inside the deadline is not overdue");

        final int handover = seq.pendingGatewayActivationTimeout(deadline);
        assertNotEquals(Sequencer.NO_FRAME, handover);
        assertEquals(6, decodeGatewayActive(seq.buffer(), handover).gatewayId());
        assertEquals(4L, seq.globalSeqNo()); // 2 list rows + bootstrap + this
    }

    @Test
    @DisplayName("a designated primary that did declare itself started is left alone")
    void designatedPrimaryThatStartedIsNotHandedOver() {
        final Sequencer seq = new Sequencer();
        final MutableDirectBuffer buf = new ExpandableArrayBuffer(512);
        seq.sequenceMessage(buf, 0, encodeIngressGatewayRegistered(buf, 0, 5, SOURCE_ID, "GW-A", 0, 1), SESSION_ID,
                            TIMESTAMP);
        seq.sequenceMessage(buf, 0, encodeIngressGatewayRegistered(buf, 0, 6, SOURCE_ID, "GW-B", 1, 0), SESSION_ID,
                            TIMESTAMP);
        seq.pendingGatewayActivation(TIMESTAMP);

        final long gatewaySession = 0xA11CEL;
        seq.sequenceMessage(buf, 0, encodeIngressGatewayStarted(buf, 0, 5), gatewaySession, TIMESTAMP + 1);
        final long globalSeqNo = seq.globalSeqNo();

        final long deadline = TIMESTAMP + Sequencer.GATEWAY_ACTIVATION_TIMEOUT_MS;
        assertEquals(Sequencer.NO_FRAME, seq.pendingGatewayActivationTimeout(deadline));
        // Disarmed, not merely quiet: a healthy primary must not be demoted one deadline later.
        assertEquals(Sequencer.NO_FRAME,
                     seq.pendingGatewayActivationTimeout(deadline + 10 * Sequencer.GATEWAY_ACTIVATION_TIMEOUT_MS));
        assertEquals(globalSeqNo, seq.globalSeqNo(), "an answered activation synthesizes nothing");
    }

    @Test
    @DisplayName("the hand-over arms the same deadline on the instance it names")
    void handoverArmsTheDeadlineOnTheInstanceItNames() {
        // A whole gateway tier that is down must converge on whichever instance comes up first, not on
        // whichever the cluster happened to designate before they all went away.
        final Sequencer seq = new Sequencer();
        final MutableDirectBuffer buf = new ExpandableArrayBuffer(512);
        seq.sequenceMessage(buf, 0, encodeIngressGatewayRegistered(buf, 0, 5, SOURCE_ID, "GW-A", 0, 1), SESSION_ID,
                            TIMESTAMP);
        seq.sequenceMessage(buf, 0, encodeIngressGatewayRegistered(buf, 0, 6, SOURCE_ID, "GW-B", 1, 0), SESSION_ID,
                            TIMESTAMP);
        seq.pendingGatewayActivation(TIMESTAMP);

        final long first = TIMESTAMP + Sequencer.GATEWAY_ACTIVATION_TIMEOUT_MS;
        assertEquals(6, decodeGatewayActive(seq.buffer(), seq.pendingGatewayActivationTimeout(first)).gatewayId());

        assertEquals(Sequencer.NO_FRAME, seq.pendingGatewayActivationTimeout(first + 1),
                     "the hand-over restarts the deadline rather than firing again immediately");

        final long second = first + Sequencer.GATEWAY_ACTIVATION_TIMEOUT_MS;
        assertEquals(5, decodeGatewayActive(seq.buffer(), seq.pendingGatewayActivationTimeout(second)).gatewayId(),
                     "the standby did not start either — the role goes back rather than stopping here");
    }

    @Test
    @DisplayName("an unanswered activation with no sibling to hand over to fails closed")
    void unansweredActivationWithNoSiblingFailsClosed() {
        final Sequencer seq = new Sequencer();
        final MutableDirectBuffer buf = new ExpandableArrayBuffer(512);
        seq.sequenceMessage(buf, 0, encodeIngressGatewayRegistered(buf, 0, 5, SOURCE_ID, "GW-A", 0, 0), SESSION_ID,
                            TIMESTAMP);
        seq.pendingGatewayActivation(TIMESTAMP);
        final long globalSeqNo = seq.globalSeqNo();

        final long deadline = TIMESTAMP + Sequencer.GATEWAY_ACTIVATION_TIMEOUT_MS;
        assertEquals(Sequencer.NO_PROMOTION_TARGET, seq.pendingGatewayActivationTimeout(deadline),
                     "distinct from NO_FRAME: an activation WAS missed, there is just nobody to hand it to");
        assertEquals(globalSeqNo, seq.globalSeqNo(), "a hand-over with no target consumes no sequence number");
        assertEquals(Sequencer.NO_FRAME,
                     seq.pendingGatewayActivationTimeout(deadline + Sequencer.GATEWAY_ACTIVATION_TIMEOUT_MS),
                     "reported once — a sole instance has nothing to re-designate to");
    }

    @Test
    @DisplayName("a promotion on session close arms the deadline too")
    void promotionOnSessionCloseArmsTheDeadline() {
        final Sequencer seq = new Sequencer();
        final MutableDirectBuffer buf = new ExpandableArrayBuffer(512);
        seq.sequenceMessage(buf, 0, encodeIngressGatewayRegistered(buf, 0, 5, SOURCE_ID, "GW-A", 0, 1), SESSION_ID,
                            TIMESTAMP);
        seq.sequenceMessage(buf, 0, encodeIngressGatewayRegistered(buf, 0, 6, SOURCE_ID, "GW-B", 1, 0), SESSION_ID,
                            TIMESTAMP);

        final long gatewaySession = 0xA11CEL;
        seq.sequenceMessage(buf, 0, encodeIngressGatewayStarted(buf, 0, 5), gatewaySession, TIMESTAMP);
        assertNotEquals(Sequencer.NO_FRAME, seq.sessionClosed(gatewaySession, TIMESTAMP)); // promotes 6

        // The standby it promoted has to declare itself started like any other designated instance —
        // otherwise a crash of the primary while the standby is also down leaves the same dead end.
        final int handover = seq.pendingGatewayActivationTimeout(TIMESTAMP + Sequencer.GATEWAY_ACTIVATION_TIMEOUT_MS);
        assertNotEquals(Sequencer.NO_FRAME, handover);
        assertEquals(5, decodeGatewayActive(seq.buffer(), handover).gatewayId());
    }

    @Test
    @DisplayName("a GatewayStarted naming an instance no list row does is rejected")
    void unknownGatewayInstancePromotesNothing() {
        final Sequencer seq = new Sequencer();
        final MutableDirectBuffer buf = new ExpandableArrayBuffer(512);
        seq.sequenceMessage(buf, 0, encodeIngressGatewayRegistered(buf, 0, 5, SOURCE_ID, "GW-A", 0, 1), SESSION_ID,
                            TIMESTAMP);
        seq.sequenceMessage(buf, 0, encodeIngressGatewayRegistered(buf, 0, 6, SOURCE_ID, "GW-B", 1, 0), SESSION_ID,
                            TIMESTAMP);

        final long rogueSession = 0xC0FFEEL;
        assertEquals(Sequencer.NO_FRAME,
                     seq.sequenceMessage(buf, 0, encodeIngressGatewayStarted(buf, 0, 99), rogueSession, TIMESTAMP),
                     "S-6 case 1 is total: a gatewayId the list does not name is rejected whatever it carries");

        assertEquals(Sequencer.NO_FRAME, seq.sessionClosed(rogueSession, TIMESTAMP + 1),
                     "the rejected frame bound nothing, so the close is not an active gateway's");
    }

    // ── Two logical gateways ──────────────────────────────────────────────────
    // The deployment runs more than one pair: the client-facing gateway (C++ FixGateway) and the
    // exchange-facing one (Java ExchangeGateway), each an active/standby pair under its own
    // gatewaySourceId. Everything below is a case where the sequencer used to hold one of something it
    // needs one of per pair. SOURCE_ID is the first pair; EXCHANGE_SOURCE_ID the second.

    @Test
    @DisplayName("the bootstrap activates the rank-0 primary of every logical gateway, not just one")
    void bootstrapActivatesEveryLogicalGatewaysPrimary() {
        final Sequencer seq = new Sequencer();
        final MutableDirectBuffer buf = new ExpandableArrayBuffer(512);
        final int endLength = loadTwoPairs(seq, buf);

        assertEquals(4L, globalSeqNoOf(seq, endLength)); // 4 list rows

        // One frame per call, in list order, on consecutive globalSeqNos — the adapter drains it.
        final int first = seq.pendingGatewayActivation(TIMESTAMP + 1);
        final GatewayActiveDecoder clientPair = decodeGatewayActive(seq.buffer(), first);
        assertEquals(5, clientPair.gatewayId());
        assertEquals(5L, frameHeaderOf(seq.buffer()).globalSeqNo());

        final int second = seq.pendingGatewayActivation(TIMESTAMP + 1);
        final GatewayActiveDecoder exchangePair = decodeGatewayActive(seq.buffer(), second);
        // Not the client pair's standby: a second rank-0 row used to overwrite the first designation, so
        // whichever logical gateway loaded last took the only bootstrap and the other never got one.
        assertEquals(8, exchangePair.gatewayId());
        assertEquals(6L, frameHeaderOf(seq.buffer()).globalSeqNo());

        assertEquals(Sequencer.NO_FRAME, seq.pendingGatewayActivation(TIMESTAMP + 1),
                     "two logical gateways, two activations");
    }

    @Test
    @DisplayName("losing one logical gateway's active instance promotes only its own sibling")
    void logicalGatewaysPromoteIndependently() {
        final Sequencer seq = new Sequencer();
        final MutableDirectBuffer buf = new ExpandableArrayBuffer(512);
        loadTwoPairs(seq, buf);

        final long clientSession = 0xA11CEL;
        final long exchangeSession = 0xB0B0L;
        seq.sequenceMessage(buf, 0, encodeIngressGatewayStarted(buf, 0, 5), clientSession, TIMESTAMP);
        seq.sequenceMessage(buf, 0, encodeIngressGatewayStarted(buf, 0, 8, EXCHANGE_SOURCE_ID), exchangeSession,
                            TIMESTAMP);

        final int promotion = seq.sessionClosed(exchangeSession, TIMESTAMP + 1);
        assertEquals(9, decodeGatewayActive(seq.buffer(), promotion).gatewayId(),
                     "the exchange pair's standby — promotionTarget filters on the closed instance's own "
                         + "gatewaySourceId");

        // The client pair is untouched: its active instance never lost anything.
        final int clientPromotion = seq.sessionClosed(clientSession, TIMESTAMP + 2);
        assertEquals(6, decodeGatewayActive(seq.buffer(), clientPromotion).gatewayId());
    }

    @Test
    @DisplayName("each logical gateway keeps its own activation deadline")
    void eachLogicalGatewayKeepsItsOwnActivationDeadline() {
        // The regression this guards: one outstanding activation for the whole cluster meant the second
        // bootstrap overwrote the first's deadline, so a designated primary that never arrived was never
        // handed over — silently, and only for whichever pair happened to be designated first.
        final Sequencer seq = new Sequencer();
        final MutableDirectBuffer buf = new ExpandableArrayBuffer(512);
        loadTwoPairs(seq, buf);
        assertNotEquals(Sequencer.NO_FRAME, seq.pendingGatewayActivation(TIMESTAMP)); // designates 5
        assertNotEquals(Sequencer.NO_FRAME, seq.pendingGatewayActivation(TIMESTAMP)); // designates 8

        final long deadline = TIMESTAMP + Sequencer.GATEWAY_ACTIVATION_TIMEOUT_MS;
        assertEquals(Sequencer.NO_FRAME, seq.pendingGatewayActivationTimeout(deadline - 1));

        // Both come due on the same heartbeat, and both are handed over — the adapter drains this too.
        assertEquals(6, decodeGatewayActive(seq.buffer(), seq.pendingGatewayActivationTimeout(deadline)).gatewayId());
        assertEquals(9, decodeGatewayActive(seq.buffer(), seq.pendingGatewayActivationTimeout(deadline)).gatewayId());
        assertEquals(Sequencer.NO_FRAME, seq.pendingGatewayActivationTimeout(deadline),
                     "both hand-overs restarted their own deadline");
    }

    @Test
    @DisplayName("one logical gateway answering its activation does not excuse the other")
    void answeringOneActivationLeavesTheOtherOnTheClock() {
        final Sequencer seq = new Sequencer();
        final MutableDirectBuffer buf = new ExpandableArrayBuffer(512);
        loadTwoPairs(seq, buf);
        seq.pendingGatewayActivation(TIMESTAMP); // designates 5
        seq.pendingGatewayActivation(TIMESTAMP); // designates 8

        // Only the client pair's primary comes up.
        seq.sequenceMessage(buf, 0, encodeIngressGatewayStarted(buf, 0, 5), 0xA11CEL, TIMESTAMP + 1);

        final long deadline = TIMESTAMP + Sequencer.GATEWAY_ACTIVATION_TIMEOUT_MS;
        assertEquals(9, decodeGatewayActive(seq.buffer(), seq.pendingGatewayActivationTimeout(deadline)).gatewayId(),
                     "the exchange pair is still overdue, and is walked past the answered entry to reach it");
        assertEquals(Sequencer.NO_FRAME, seq.pendingGatewayActivationTimeout(deadline),
                     "the healthy pair is disarmed, not merely quiet");
    }

    /**
     * Two active/standby pairs: 5/6 under SOURCE_ID, 8/9 under EXCHANGE_SOURCE_ID, in list order.
     * One complete list run — remaining counts down to 0 on the last row. Returns that row's frame
     * length, since it is the frame the bootstrap is synthesized behind.
     */
    private static int loadTwoPairs(final Sequencer seq, final MutableDirectBuffer buf) {
        seq.sequenceMessage(buf, 0, encodeIngressGatewayRegistered(buf, 0, 5, SOURCE_ID, "GW-A", 0, 3), SESSION_ID,
                            TIMESTAMP);
        seq.sequenceMessage(buf, 0, encodeIngressGatewayRegistered(buf, 0, 6, SOURCE_ID, "GW-B", 1, 2), SESSION_ID,
                            TIMESTAMP);
        seq.sequenceMessage(buf, 0, encodeIngressGatewayRegistered(buf, 0, 8, EXCHANGE_SOURCE_ID, "EGW-A", 0, 1),
                            SESSION_ID, TIMESTAMP);
        return seq.sequenceMessage(buf, 0, encodeIngressGatewayRegistered(buf, 0, 9, EXCHANGE_SOURCE_ID, "EGW-B", 1, 0),
                                   SESSION_ID, TIMESTAMP);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /**
     * Encodes a frame carrying {@link #OPAQUE_PAYLOAD} under a payloadId the sequencer does not own —
     * the standing exemplar of ordinary application traffic. Deliberately not a real message in a real
     * application schema: the sequencer reads nothing past the payloadId, so a payload with a decodable
     * shape would only invite a test to assert something the production path cannot see.
     */
    private static int encodeIngressPayload(final MutableDirectBuffer buffer, final int offset) {
        return encodeIngressPayload(buffer, offset, OPAQUE_PAYLOAD);
    }

    /** As above, for a payload of a caller-chosen length. */
    private static int encodeIngressPayload(final MutableDirectBuffer buffer, final int offset,
                                            final byte[] bytes) {
        final MutableDirectBuffer payload = new ExpandableArrayBuffer(bytes.length);
        payload.putBytes(0, bytes);
        return encodeIngressPayloadFrame(buffer, offset, SOURCE_ID, CONNECTION_ID, SESSION_PAYLOAD_ID, payload,
                                         bytes.length);
    }

    /** {@code length} bytes cycling through every value a byte can hold. */
    private static byte[] opaqueBytes(final int length) {
        final byte[] bytes = new byte[length];
        for (int i = 0; i < length; i++) {
            bytes[i] = (byte)i;
        }
        return bytes;
    }

    /**
     * Encodes a schema-200 list row, as {@code clusterctl load-topology} submits it. {@code remaining}
     * is the rows left after this one; 0 makes it the last, which is what the bootstrap fires on.
     */
    private static int encodeIngressGatewayRegistered(final MutableDirectBuffer buffer, final int offset,
                                                      final int gatewayId, final int gatewaySourceId,
                                                      final String gatewayName, final int preferenceRank,
                                                      final int remaining) {
        final MutableDirectBuffer payload = new ExpandableArrayBuffer(128);
        final org.limitless.phixeron.sbe.frame.GatewayRegisteredEncoder encoder = new org.limitless.phixeron.sbe.frame.GatewayRegisteredEncoder();
        encoder.wrap(payload, 0);
        encoder.remaining(remaining)
            .gatewayId(gatewayId)
            .gatewaySourceId(gatewaySourceId)
            .gatewayName(gatewayName)
            .preferenceRank((short)preferenceRank);
        // The operator tool's own sourceId (not a gateway's), outside the gateway-sourceId set.
        return encodeIngressSystemFrame(buffer, offset, 99, -1, SystemFrame.GATEWAY_REGISTERED, payload,
                                        encoder.encodedLength());
    }

    /**
     * Encodes a schema-200 ConnectionOpened, as the FIX gateway submits it when it accepts a TCP
     * connection. Header-only: the connection it describes is entirely in {@code header}.
     */
    private static int encodeIngressConnectionOpened(final MutableDirectBuffer buffer, final int offset) {
        return encodeIngressConnectionOpened(buffer, offset, CONNECTION_ID);
    }

    /** As above, for the one connection identity {@code connectedClientCount} keys its gauge on. */
    private static int encodeIngressConnectionOpened(final MutableDirectBuffer buffer, final int offset,
                                                    final int connectionId) {
        final MutableDirectBuffer payload = new ExpandableArrayBuffer(64);
        final org.limitless.phixeron.sbe.frame.ConnectionOpenedEncoder encoder = new org.limitless.phixeron.sbe.frame.ConnectionOpenedEncoder();
        encoder.wrap(payload, 0);
        encoder.putConnectionData(new byte[0], 0, 0);
        return encodeIngressSystemFrame(buffer, offset, SOURCE_ID, connectionId, SystemFrame.CONNECTION_OPENED,
                                        payload, encoder.encodedLength());
    }

    /** Encodes a schema-200 ConnectionClosed; the mirror of {@link #encodeIngressConnectionOpened}. */
    private static int encodeIngressConnectionClosed(final MutableDirectBuffer buffer, final int offset) {
        return encodeIngressConnectionClosed(buffer, offset, CONNECTION_ID);
    }

    /** As above, for a named connection. */
    private static int encodeIngressConnectionClosed(final MutableDirectBuffer buffer, final int offset,
                                                       final int connectionId) {
        final MutableDirectBuffer payload = new ExpandableArrayBuffer(64);
        final org.limitless.phixeron.sbe.frame.ConnectionClosedEncoder encoder = new org.limitless.phixeron.sbe.frame.ConnectionClosedEncoder();
        encoder.wrap(payload, 0);
        return encodeIngressSystemFrame(buffer, offset, SOURCE_ID, connectionId, SystemFrame.CONNECTION_CLOSED,
                                        payload, encoder.encodedLength());
    }

    /**
     * Encodes a schema-200 GatewayStarted, as an instance publishes it on activation: the one frame that
     * tells the sequencer which cluster session a given {@code gatewayId} is active on.
     */
    private static int encodeIngressGatewayStarted(final MutableDirectBuffer buffer, final int offset,
                                                   final int gatewayId) {
        return encodeIngressGatewayStarted(buffer, offset, gatewayId, SOURCE_ID);
    }
    private static int encodeIngressGatewayStarted(final MutableDirectBuffer buffer, final int offset,
                                                   final int gatewayId, final int sourceId) {
        final MutableDirectBuffer payload = new ExpandableArrayBuffer(64);
        final org.limitless.phixeron.sbe.frame.GatewayStartedEncoder encoder = new org.limitless.phixeron.sbe.frame.GatewayStartedEncoder();
        encoder.wrap(payload, 0);
        encoder.gatewayId(gatewayId).firstConnectionId(1);
        return encodeIngressSystemFrame(buffer, offset, sourceId, -1, SystemFrame.GATEWAY_STARTED, payload,
                                        encoder.encodedLength());
    }

    /** Encodes a GatewayActivationRequested, as {@code clusterctl activate} submits it. */
    private static int encodeIngressActivationRequested(final MutableDirectBuffer buffer, final int offset,
                                                        final int gatewayId) {
        final MutableDirectBuffer body = new ExpandableArrayBuffer(64);
        final org.limitless.phixeron.sbe.frame.GatewayActivationRequestedEncoder encoder =
            new org.limitless.phixeron.sbe.frame.GatewayActivationRequestedEncoder();
        encoder.wrap(body, 0);
        encoder.gatewayId(gatewayId);
        // clusterctl's own reserved sourceId (§5), which no list row claims.
        return encodeIngressSystemFrame(buffer, offset, 99, -1, SystemFrame.GATEWAY_ACTIVATION_REQUESTED, body,
                                        encoder.encodedLength());
    }

    private static GatewayActiveDecoder decodeGatewayActive(final MutableDirectBuffer buffer, final int length) {
        return decodeSynthesized(buffer, length, GatewayActiveDecoder.TEMPLATE_ID, SystemFrame.GATEWAY_ACTIVE,
                                 GatewayActiveDecoder.BLOCK_LENGTH, new GatewayActiveDecoder());
    }

    /**
     * Asserts the frame the sequencer just encoded carries {@code expected} byte for byte, under the
     * payloadId it arrived with and behind a length prefix that agrees with the length returned.
     *
     * <p>This is the whole copy-through contract as a consumer can observe it: the sequencer opens no
     * payload, so what it owes is the bytes back unchanged and a prefix that finds them, and neither is
     * anything an application decoder would tell us more about.
     *
     * @param length the sequencer's return value for this frame
     */
    private static void assertPayloadCopiedThrough(final MutableDirectBuffer buffer, final int length,
                                                   final byte[] expected) {
        assertEquals(SESSION_PAYLOAD_ID, frameHeaderOf(buffer).payloadId());
        final int declared = buffer.getShort(FRAME_OVERHEAD - PAYLOAD_PREFIX_LENGTH,
                                             java.nio.ByteOrder.LITTLE_ENDIAN) & 0xFFFF;
        assertEquals(expected.length, declared, "the tap frame's payload prefix");
        assertEquals(FRAME_OVERHEAD + expected.length, length, "the tap frame's total length");

        final byte[] actual = new byte[expected.length];
        buffer.getBytes(FRAME_OVERHEAD, actual);
        assertArrayEquals(expected, actual);
    }

    private static LeadershipChangedDecoder decodeLeadershipChanged(final MutableDirectBuffer buffer,
                                                                    final int length) {
        return decodeSynthesized(buffer, length, LeadershipChangedDecoder.TEMPLATE_ID,
                                 SystemFrame.LEADERSHIP_CHANGED, LeadershipChangedDecoder.BLOCK_LENGTH,
                                 new LeadershipChangedDecoder());
    }

    /**
     * Decodes one of the three the sequencer synthesizes. Each has a template of its own and carries its
     * fields inline, so there is no body and no length prefix: the frame is its framing header plus its
     * block, and the decoder is wrapped over that block with its own compiled constants (<b>V-3</b>).
     */
    private static <T> T decodeSynthesized(final MutableDirectBuffer buffer, final int length, final int templateId,
                                           final int systemEventType, final int blockLength, final T decoder) {
        final org.limitless.phixeron.sbe.frame.MessageHeaderDecoder header =
            new org.limitless.phixeron.sbe.frame.MessageHeaderDecoder().wrap(buffer, 0);
        assertEquals(org.limitless.phixeron.sbe.frame.MessageHeaderDecoder.SCHEMA_ID, header.schemaId());
        assertEquals(templateId, header.templateId());
        assertEquals(blockLength, header.blockLength());
        assertEquals(org.limitless.phixeron.sbe.frame.MessageHeaderDecoder.ENCODED_LENGTH + blockLength, length);
        assertEquals(systemEventType, systemHeaderOf(buffer).systemEventType(),
                     "offset 16 discriminates every frame on the tap, synthesized ones included");
        final int block = org.limitless.phixeron.sbe.frame.MessageHeaderDecoder.ENCODED_LENGTH;
        if (decoder instanceof GatewayActiveDecoder d) {
            d.wrap(buffer, block, blockLength, header.version());
        } else {
            ((LeadershipChangedDecoder)decoder).wrap(buffer, block, blockLength, header.version());
        }
        return decoder;
    }

    private long globalSeqNoOf(final int length) {
        return globalSeqNoOf(sequencer, length);
    }

    /** Reads globalSeqNo off the frame just encoded — its header, at a fixed offset past the framing one. */
    private static long globalSeqNoOf(final Sequencer target, final int length) {
        assertNotEquals(Sequencer.NO_FRAME, length);
        return frameHeaderOf(target.buffer()).globalSeqNo();
    }

    /** The frame's own header — the identity every message relies on, since it carries none itself. */
    private static org.limitless.phixeron.sbe.frame.SequencedHeaderDecoder frameHeaderOf(final MutableDirectBuffer buffer) {
        return new org.limitless.phixeron.sbe.frame.SequencedHeaderDecoder().wrap(buffer, org.limitless.phixeron.sbe.frame.MessageHeaderDecoder.ENCODED_LENGTH);
    }

    /**
     * The same header read under the system family's names. The two composites are byte-for-byte
     * identical apart from the name of the uint16 at offset 16, which is the property every field below
     * that offset being at a fixed place depends on (<b>F-3</b>).
     */
    private static org.limitless.phixeron.sbe.frame.SequencedSystemHeaderDecoder systemHeaderOf(
        final MutableDirectBuffer buffer) {
        return new org.limitless.phixeron.sbe.frame.SequencedSystemHeaderDecoder()
            .wrap(buffer, org.limitless.phixeron.sbe.frame.MessageHeaderDecoder.ENCODED_LENGTH);
    }

    /**
     * Wraps an already-encoded system body in an {@code UnsequencedSystem} frame, as every system producer
     * does. The body carries no {@code MessageHeader} of its own — {@code header.systemEventType} names it.
     * {@code sessionId} is advisory on ingress — the sequencer overwrites it — so it goes out as -1.
     */
    private static int encodeIngressSystemFrame(final MutableDirectBuffer buffer, final int offset,
                                                final int sourceId, final int connectionId,
                                                final int systemEventType, final MutableDirectBuffer body,
                                                final int bodyLength) {
        final org.limitless.phixeron.sbe.frame.UnsequencedSystemEncoder encoder =
            new org.limitless.phixeron.sbe.frame.UnsequencedSystemEncoder();
        encoder.wrapAndApplyHeader(buffer, offset, new org.limitless.phixeron.sbe.frame.MessageHeaderEncoder());
        encoder.header().sourceId(sourceId).connectionId(connectionId).sessionId(-1)
            .systemEventType(systemEventType);
        encoder.putBody(body, 0, bodyLength);
        return org.limitless.phixeron.sbe.frame.MessageHeaderEncoder.ENCODED_LENGTH + encoder.encodedLength();
    }

    /** The same, for a payload the sequencer does not own. */
    private static int encodeIngressPayloadFrame(final MutableDirectBuffer buffer, final int offset,
                                                 final int sourceId, final int connectionId, final int payloadId,
                                                 final MutableDirectBuffer payload, final int payloadLength) {
        final org.limitless.phixeron.sbe.frame.UnsequencedEncoder encoder = new org.limitless.phixeron.sbe.frame.UnsequencedEncoder();
        encoder.wrapAndApplyHeader(buffer, offset, new org.limitless.phixeron.sbe.frame.MessageHeaderEncoder());
        encoder.header().sourceId(sourceId).connectionId(connectionId).sessionId(-1).payloadId(payloadId);
        encoder.putPayload(payload, 0, payloadLength);
        return org.limitless.phixeron.sbe.frame.MessageHeaderEncoder.ENCODED_LENGTH + encoder.encodedLength();
    }

    /** Rewrites the declared payload length of the frame in {@link #ingress}, leaving its bytes alone. */
    private void putIngressPayloadLength(final int payloadLength) {
        ingress.putShort(INGRESS_PREFIX_OFFSET, (short)payloadLength, java.nio.ByteOrder.LITTLE_ENDIAN);
    }

    /** The {@code systemEventType} of the frame just encoded, after checking it is a well-formed one. */
    private static int systemEventTypeOf(final MutableDirectBuffer buffer, final int length) {
        systemBodyOffset(buffer, length);
        return systemHeaderOf(buffer).systemEventType();
    }

    /**
     * Offset of the body inside a {@code SequencedSystem} frame, checking the frame's shape on the way —
     * that it is one, and that the length it declares accounts for every byte the sequencer returned.
     */
    private static int systemBodyOffset(final MutableDirectBuffer buffer, final int length) {
        final org.limitless.phixeron.sbe.frame.MessageHeaderDecoder header = new org.limitless.phixeron.sbe.frame.MessageHeaderDecoder().wrap(buffer, 0);
        assertEquals(org.limitless.phixeron.sbe.frame.SequencedSystemDecoder.SCHEMA_ID, header.schemaId());
        assertEquals(org.limitless.phixeron.sbe.frame.SequencedSystemDecoder.TEMPLATE_ID, header.templateId());
        final int prefix = org.limitless.phixeron.sbe.frame.MessageHeaderDecoder.ENCODED_LENGTH +
                           org.limitless.phixeron.sbe.frame.SequencedSystemHeaderDecoder.ENCODED_LENGTH;
        final int bodyLength = buffer.getShort(prefix, java.nio.ByteOrder.LITTLE_ENDIAN) & 0xFFFF;
        final int body = prefix + org.limitless.phixeron.sbe.frame.SequencedSystemDecoder.bodyHeaderLength();
        assertEquals(length, body + bodyLength);
        return body;
    }
}
