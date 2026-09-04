package org.limitless.phixeron.sequencer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.limitless.phixeron.sbe.frame.ClientConnectedDecoder;
import org.limitless.phixeron.sbe.frame.ClientDisconnectedDecoder;
import org.limitless.phixeron.sbe.frame.ClusterHeartbeatDecoder;
import org.limitless.phixeron.sbe.frame.GatewayActiveDecoder;
import org.limitless.phixeron.sbe.frame.LeadershipChangedDecoder;
import org.limitless.phixeron.sbe.frame.MessageHeaderDecoder;
import org.limitless.phixeron.sbe.session.HeartbeatDecoder;
import org.limitless.phixeron.sbe.session.LogoutDecoder;

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
     * The FIX session family's payloadId (sbe-session.xml, schema 230). Used here as an exemplar of a
     * payload the sequencer does not own: it is copied through opaque, and this test decodes it only to
     * prove that every byte survived.
     */
    private static final int SESSION_PAYLOAD_ID = 3;

    /** unsequencedHeader is 18 bytes, sequencedHeader 34 — the delta every sequenced frame grows by. */
    private static final int HEADER_GROWTH =
        org.limitless.phixeron.sbe.frame.SequencedHeaderDecoder.ENCODED_LENGTH -
        org.limitless.phixeron.sbe.frame.UnsequencedHeaderDecoder.ENCODED_LENGTH;

    /**
     * Bytes a {@code Sequenced} frame adds around its payload: the framing header, the 34-byte
     * {@code sequencedHeader} and the payload's own length prefix.
     */
    private static final int FRAME_OVERHEAD =
        org.limitless.phixeron.sbe.frame.MessageHeaderDecoder.ENCODED_LENGTH +
        org.limitless.phixeron.sbe.frame.SequencedHeaderDecoder.ENCODED_LENGTH +
        org.limitless.phixeron.sbe.frame.SequencedDecoder.payloadHeaderLength();

    /** Where a payload's body starts in an encoded frame: the envelope, then the payload's own header. */
    private static final int SESSION_PAYLOAD_BODY_OFFSET =
        FRAME_OVERHEAD + org.limitless.phixeron.sbe.session.MessageHeaderDecoder.ENCODED_LENGTH;

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
    @DisplayName("sequencing an ingress message preserves every field past the header composite")
    void sequenceMessagePreservesEveryFieldPastTheHeader() {
        final int ingressLength = encodeIngressHeartbeat(ingress, 0);

        final int length = sequencer.sequenceMessage(ingress, 0, ingressLength, SESSION_ID, TIMESTAMP);

        final HeartbeatDecoder decoded = decodeHeartbeat(sequencer.buffer());
        // Every business field survives the opaque byte copy unread and unmodified.
        assertEquals("Client", decoded.direction().name());
        assertEquals("CLIENT", decoded.sender().trim());
        assertEquals("PHIXERON", decoded.target().trim());
        assertEquals(4321L, decoded.seqNum());
        assertEquals(1_699_999_999_000L, decoded.sendingTimeMs());
        assertEquals("Yes", decoded.possDupFlag().name());
        assertEquals("TESTREQ-0001", decoded.testReqID().trim());
    }

    @Test
    @DisplayName("sequencing stamps the header without disturbing the submitter's identity")
    void sequenceMessageStampsHeader() {
        final int ingressLength = encodeIngressHeartbeat(ingress, 0);

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
    @DisplayName("var-data (Logout text) survives the copy-through")
    void sequenceMessagePreservesVarData() {
        // Heartbeat is all fixed block; Logout carries var-data, so it exercises the copyLength
        // arithmetic past the end of the fixed block as well.
        final String reason = "counterparty requested disconnect";
        final int ingressLength = encodeIngressLogout(ingress, 0, reason);

        final int length = sequencer.sequenceMessage(ingress, 0, ingressLength, SESSION_ID, TIMESTAMP);

        final LogoutDecoder decoded = decodeLogout(sequencer.buffer());
        assertEquals("CLIENT", decoded.sender().trim());
        assertEquals(99L, decoded.seqNum());
        assertEquals(reason, decoded.text());
        // The whole ingress frame, minus nothing, plus the two int64s the sequenced header adds.
        assertEquals(ingressLength + HEADER_GROWTH, length);
    }

    @Test
    @DisplayName("the tap frame is a Sequenced envelope, its block exactly the header composite")
    void egressFrameIsASequencedEnvelope() {
        // The ingress and tap templates differ, and their blockLengths differ by exactly the stamp: the
        // envelope's block IS its header composite, so anything else here means the two are out of step.
        final int ingressLength = encodeIngressHeartbeat(ingress, 0);
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
        final int ingressLength = encodeIngressHeartbeat(ingress, 0);
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
        final int ingressLength = encodeIngressHeartbeat(ingress, offset);

        final int length = sequencer.sequenceMessage(ingress, offset, ingressLength, SESSION_ID, TIMESTAMP);

        final HeartbeatDecoder decoded = decodeHeartbeat(sequencer.buffer());
        assertEquals("TESTREQ-0001", decoded.testReqID().trim());
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
        final int ingressLength = encodeIngressHeartbeat(ingress, 0);
        final MutableDirectBuffer lifecycle = new ExpandableArrayBuffer(64);

        final int connectedLength = encodeIngressClientConnected(lifecycle, 0);
        assertEquals(1L,
                     globalSeqNoOf(sequencer.sequenceMessage(lifecycle, 0, connectedLength, SESSION_ID, TIMESTAMP)));
        assertEquals(2L, globalSeqNoOf(sequencer.sequenceMessage(ingress, 0, ingressLength, SESSION_ID, TIMESTAMP)));
        assertEquals(3L, globalSeqNoOf(sequencer.clusterHeartbeat(TIMESTAMP + 1000)));
        assertEquals(4L, globalSeqNoOf(sequencer.leadershipChanged(2, TIMESTAMP + 1500)));
        assertEquals(5L, globalSeqNoOf(sequencer.sequenceMessage(ingress, 0, ingressLength, SESSION_ID, TIMESTAMP)));

        final int disconnectedLength = encodeIngressClientDisconnected(lifecycle, 0);
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
        final int ingressLength = encodeIngressHeartbeat(ingress, 0);

        assertEquals(Sequencer.NO_FRAME,
                     sequencer.sequenceMessage(ingress, 0, Sequencer.MIN_FRAME_LENGTH - 1, SESSION_ID, TIMESTAMP));
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
        final int ingressLength = encodeIngressHeartbeat(ingress, 0);
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
        final int length = encodeIngressClientConnected(ingress, 0);
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
        final int length = encodeIngressClientConnected(ingress, 0);
        new org.limitless.phixeron.sbe.frame.UnsequencedHeaderEncoder()
            .wrap(ingress, org.limitless.phixeron.sbe.frame.MessageHeaderEncoder.ENCODED_LENGTH).payloadId(0);

        assertEquals(Sequencer.NO_FRAME, sequencer.sequenceMessage(ingress, 0, length, SESSION_ID, TIMESTAMP));
        assertEquals(0L, sequencer.globalSeqNo());
    }

    @Test
    @DisplayName("a core payload declaring a foreign schema is skipped rather than read as core")
    void corePayloadFromAForeignSchemaIsSkipped() {
        // payloadId 1 is a claim about the bytes, and P-4 says check it: reading a templateId out of an
        // unverified payload reads a foreign schema's numbering as core's, which silently turns some other
        // protocol's message into a GatewayStarted.
        final int length = encodeIngressClientConnected(ingress, 0);
        new org.limitless.phixeron.sbe.frame.MessageHeaderEncoder().wrap(ingress, INGRESS_PAYLOAD_OFFSET).schemaId(999);

        assertEquals(Sequencer.NO_FRAME, sequencer.sequenceMessage(ingress, 0, length, SESSION_ID, TIMESTAMP));
        assertEquals(0L, sequencer.globalSeqNo());
    }

    @Test
    @DisplayName("a synthesis-only core payload is refused on ingress")
    void synthesizedOnlyPayloadIsRefusedOnIngress() {
        // The cluster clock and the leadership record have no producer -- every node encodes its own copy
        // (E-1's exception). Accepting one from a gateway would let it forge either.
        final MutableDirectBuffer payload = new ExpandableArrayBuffer(64);
        final org.limitless.phixeron.sbe.frame.ClusterHeartbeatEncoder encoder = new org.limitless.phixeron.sbe.frame.ClusterHeartbeatEncoder();
        encoder.wrapAndApplyHeader(payload, 0, new org.limitless.phixeron.sbe.frame.MessageHeaderEncoder());
        final int length = encodeIngressFrame(ingress, 0, SOURCE_ID, CONNECTION_ID, payload,
                                              corePayloadLength(encoder.encodedLength()));

        assertEquals(Sequencer.NO_FRAME, sequencer.sequenceMessage(ingress, 0, length, SESSION_ID, TIMESTAMP));
        assertEquals(0L, sequencer.globalSeqNo());
    }

    @Test
    @DisplayName("a core payload too short for the field the sequencer reads is skipped")
    void corePayloadTooShortForItsBodyIsSkipped() {
        // Fitting the frame exactly says nothing about being long enough for a body field, and the floor
        // is the decoder's compiled block length -- never the wire's, which an SBE decoder ignores when it
        // reads a fixed-width field at its fixed offset.
        encodeIngressGatewayStarted(ingress, 0, 5);
        final int shortPayload = corePayloadLength(org.limitless.phixeron.sbe.frame.GatewayStartedDecoder.BLOCK_LENGTH - 1);
        putIngressPayloadLength(shortPayload);

        assertEquals(Sequencer.NO_FRAME, sequencer.sequenceMessage(ingress, 0, INGRESS_PAYLOAD_OFFSET + shortPayload,
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
        final int ingressLength = encodeIngressHeartbeat(ingress, 0);
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
        final int messageLength = encodeIngressHeartbeat(message, 0);
        final MutableDirectBuffer lifecycle = new ExpandableArrayBuffer(64);
        final int connectedLength = encodeIngressClientConnected(lifecycle, 0);
        final java.util.List<byte[]> frames = new java.util.ArrayList<>();

        collect(frames, target, target.sequenceMessage(lifecycle, 0, connectedLength, SESSION_ID, TIMESTAMP));
        collect(frames, target, target.leadershipChanged(0, TIMESTAMP + 1));
        for (int i = 0; i < 5; i++) {
            collect(frames, target, target.sequenceMessage(message, 0, messageLength, SESSION_ID, TIMESTAMP + i));
            collect(frames, target, target.clusterHeartbeat(TIMESTAMP + 1000L * i));
        }
        collect(frames, target, target.leadershipChanged(0, TIMESTAMP + 9)); // suppressed
        collect(frames, target, target.leadershipChanged(1, TIMESTAMP + 10));
        final int disconnectedLength = encodeIngressClientDisconnected(lifecycle, 0);
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
            sequencer.sequenceMessage(lifecycle, 0, encodeIngressClientConnected(lifecycle, 0), SESSION_ID, TIMESTAMP);
        assertEquals(ClientConnectedDecoder.TEMPLATE_ID, payloadTemplateIdOf(sequencer.buffer(), connectedLength));
        // The identity is the frame's, not the payload's: a core payload carries no header of its own.
        org.limitless.phixeron.sbe.frame.SequencedHeaderDecoder header = frameHeaderOf(sequencer.buffer());
        assertEquals(SOURCE_ID, header.sourceId());
        assertEquals(CONNECTION_ID, header.connectionId());
        assertEquals(SESSION_ID, header.sessionId());
        assertEquals(Sequencer.CORE_PAYLOAD_ID, header.payloadId());
        assertEquals(1L, header.globalSeqNo());
        assertEquals(TIMESTAMP, header.timestamp());
        // An empty payload copies zero body bytes through — the degenerate end of the copy-through path
        // every other message exercises with a body.
        assertEquals(FRAME_OVERHEAD + emptyCorePayloadLength(ClientConnectedDecoder.BLOCK_LENGTH) +
                     ClientConnectedDecoder.connectionDataHeaderLength(), connectedLength);

        final int disconnectedLength = sequencer.sequenceMessage(
            lifecycle, 0, encodeIngressClientDisconnected(lifecycle, 0), SESSION_ID, TIMESTAMP + 1);
        assertEquals(ClientDisconnectedDecoder.TEMPLATE_ID,
                     payloadTemplateIdOf(sequencer.buffer(), disconnectedLength));
        header = frameHeaderOf(sequencer.buffer());
        assertEquals(CONNECTION_ID, header.connectionId());
        assertEquals(TIMESTAMP + 1, header.timestamp());
        assertEquals(SESSION_ID, header.sessionId());
        assertEquals(FRAME_OVERHEAD + emptyCorePayloadLength(ClientDisconnectedDecoder.BLOCK_LENGTH),
                     disconnectedLength);
    }

    @Test
    @DisplayName("heartbeat carries the consensus timestamp and no session")
    void heartbeatCarriesConsensusTimestamp() {
        // The gateway's keepalive watchdog reads exactly this field to advance its session clock while
        // a counterparty is silent, so a heartbeat that lost its timestamp would stall every watchdog.
        final long heartbeatTime = TIMESTAMP + 60_000;
        final int length = sequencer.clusterHeartbeat(heartbeatTime);

        assertEquals(ClusterHeartbeatDecoder.TEMPLATE_ID, payloadTemplateIdOf(sequencer.buffer(), length));
        final org.limitless.phixeron.sbe.frame.SequencedHeaderDecoder header = frameHeaderOf(sequencer.buffer());
        assertEquals(heartbeatTime, header.timestamp());
        assertEquals(Sequencer.NO_SOURCE_ID, header.sessionId());
        assertEquals(Sequencer.CORE_PAYLOAD_ID, header.payloadId());
        assertEquals(1L, header.globalSeqNo());
        assertEquals(FRAME_OVERHEAD + emptyCorePayloadLength(ClusterHeartbeatDecoder.BLOCK_LENGTH), length);
    }

    @Test
    @DisplayName("connectedClientCount tracks ClientConnected/ClientDisconnected pairs off the log")
    void connectedClientCountTracksLifecycleFrames() {
        final Sequencer seq = new Sequencer();
        final MutableDirectBuffer lifecycle = new ExpandableArrayBuffer(64);
        assertEquals(0, seq.connectedClientCount());

        seq.sequenceMessage(lifecycle, 0, encodeIngressClientConnected(lifecycle, 0, 1), SESSION_ID, TIMESTAMP);
        assertEquals(1, seq.connectedClientCount());

        seq.sequenceMessage(lifecycle, 0, encodeIngressClientConnected(lifecycle, 0, 2), SESSION_ID + 1, TIMESTAMP);
        assertEquals(2, seq.connectedClientCount());

        seq.sequenceMessage(lifecycle, 0, encodeIngressClientDisconnected(lifecycle, 0, 1), SESSION_ID, TIMESTAMP + 1);
        assertEquals(1, seq.connectedClientCount());

        // Ordinary application traffic must not perturb the count.
        final int orderLength = encodeIngressHeartbeat(ingress, 0);
        seq.sequenceMessage(ingress, 0, orderLength, SESSION_ID + 1, TIMESTAMP + 2);
        assertEquals(1, seq.connectedClientCount());
    }

    @Test
    @DisplayName("connectedClientCount is a live gauge, not a running tally that drifts")
    void connectedClientCountIsALiveGauge() {
        // What a tally got wrong: a gateway that dies with clients attached publishes no
        // ClientDisconnected for any of them, so its connects stay counted for the rest of the day.
        // GatewayStarted is its successor declaring the epoch rolled — every connection still open under
        // that logical gateway belonged to the instance that went away, and died with its sockets.
        final Sequencer seq = new Sequencer();
        final MutableDirectBuffer buf = new ExpandableArrayBuffer(128);
        seq.sequenceMessage(buf, 0, encodeIngressClientConnected(buf, 0, 1), SESSION_ID, TIMESTAMP);
        seq.sequenceMessage(buf, 0, encodeIngressClientConnected(buf, 0, 2), SESSION_ID, TIMESTAMP);
        assertEquals(2, seq.connectedClientCount());

        seq.sequenceMessage(buf, 0, encodeIngressGatewayStarted(buf, 0, 5), SESSION_ID + 1, TIMESTAMP + 1);
        assertEquals(0, seq.connectedClientCount(), "the dead instance's connections are not still connected");

        // The successor's own connection counts, and counts once however many times the frame arrives.
        seq.sequenceMessage(buf, 0, encodeIngressClientConnected(buf, 0, 1), SESSION_ID + 1, TIMESTAMP + 2);
        seq.sequenceMessage(buf, 0, encodeIngressClientConnected(buf, 0, 1), SESSION_ID + 1, TIMESTAMP + 3);
        assertEquals(1, seq.connectedClientCount());

        // A disconnect matching nothing open cannot take the gauge below zero.
        seq.sequenceMessage(buf, 0, encodeIngressClientDisconnected(buf, 0, 1), SESSION_ID + 1, TIMESTAMP + 4);
        seq.sequenceMessage(buf, 0, encodeIngressClientDisconnected(buf, 0, 1), SESSION_ID + 1, TIMESTAMP + 5);
        assertEquals(0, seq.connectedClientCount());
    }

    // ── Standby promotion (GatewayActive) ─────────────────────────────────────

    @Test
    @DisplayName("the roster's last row is followed by a bootstrap GatewayActive naming the rank-0 primary")
    void rostersLastRowSynthesizesBootstrapActivation() {
        // Cold-start designation: one instance must open its gate and the standby must wait, so the
        // cluster names the primary's gatewayId behind the row that completes the roster (doc/todo.md
        // item 18). The primary is derived from those rows — the rank-0 one — not configured.
        final int primaryGatewayId = 5;
        final Sequencer seq = new Sequencer();
        final MutableDirectBuffer buf = new ExpandableArrayBuffer(128);

        assertEquals(Sequencer.NO_FRAME, seq.pendingGatewayBootstrapActivation(TIMESTAMP),
                     "nothing pending before the roster");

        // gatewayId 5 (rank 0) is the primary, 6 (rank 1) the standby; remaining counts down to 0.
        seq.sequenceMessage(buf, 0, encodeIngressGatewayRegistered(buf, 0, primaryGatewayId, SOURCE_ID, "GW-A", 0, 1),
                            SESSION_ID, TIMESTAMP);
        assertEquals(Sequencer.NO_FRAME, seq.pendingGatewayBootstrapActivation(TIMESTAMP),
                     "an incomplete roster designates nobody — remaining is the whole completeness edge");

        final int endLength = seq.sequenceMessage(
            buf, 0, encodeIngressGatewayRegistered(buf, 0, 6, SOURCE_ID, "GW-B", 1, 0), SESSION_ID, TIMESTAMP);
        assertEquals(2L, globalSeqNoOf(seq, endLength)); // 2 roster rows

        final int activationLength = seq.pendingGatewayBootstrapActivation(TIMESTAMP + 1);
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
        assertEquals(Sequencer.NO_FRAME, seq.pendingGatewayBootstrapActivation(TIMESTAMP + 3));
    }

    @Test
    @DisplayName("a complete roster with no rank-0 row designates no primary and synthesizes no activation")
    void rosterWithNoPrimaryFailsClosed() {
        // The empty roster this replaces stopped being expressible when the completeness edge became a
        // countdown: with no rows there is no remaining==0 to fire on, so the latch stays unset and a
        // later roster still elects. clusterctl refuses to publish either shape; the sequencer's own
        // answer to a rank-0-less roster that reached it anyway is still to activate nobody.
        final Sequencer seq = new Sequencer();
        final MutableDirectBuffer buf = new ExpandableArrayBuffer(128);
        seq.sequenceMessage(buf, 0, encodeIngressGatewayRegistered(buf, 0, 5, SOURCE_ID, "GW-A", 1, 1), SESSION_ID,
                            TIMESTAMP);
        seq.sequenceMessage(buf, 0, encodeIngressGatewayRegistered(buf, 0, 6, SOURCE_ID, "GW-B", 2, 0), SESSION_ID,
                            TIMESTAMP);
        assertEquals(Sequencer.NO_FRAME, seq.pendingGatewayBootstrapActivation(TIMESTAMP + 1),
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
        seq.sequenceMessage(buf, 0, encodeIngressHeartbeat(buf, 0), orderExecSession, TIMESTAMP + 1);

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
        assertNotEquals(Sequencer.NO_FRAME, seq.pendingGatewayBootstrapActivation(TIMESTAMP)); // designates 5

        final long deadline = TIMESTAMP + Sequencer.GATEWAY_ACTIVATION_TIMEOUT_MS;
        assertEquals(Sequencer.NO_FRAME, seq.pendingGatewayActivationTimeout(deadline - 1),
                     "a cold start still inside the deadline is not overdue");

        final int handover = seq.pendingGatewayActivationTimeout(deadline);
        assertNotEquals(Sequencer.NO_FRAME, handover);
        assertEquals(6, decodeGatewayActive(seq.buffer(), handover).gatewayId());
        assertEquals(4L, seq.globalSeqNo()); // 2 roster rows + bootstrap + this
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
        seq.pendingGatewayBootstrapActivation(TIMESTAMP);

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
        seq.pendingGatewayBootstrapActivation(TIMESTAMP);

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
        seq.pendingGatewayBootstrapActivation(TIMESTAMP);
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
    @DisplayName("a GatewayStarted from an unknown instance promotes nothing")
    void unknownGatewayInstancePromotesNothing() {
        final Sequencer seq = new Sequencer();
        final MutableDirectBuffer buf = new ExpandableArrayBuffer(512);
        seq.sequenceMessage(buf, 0, encodeIngressGatewayRegistered(buf, 0, 5, SOURCE_ID, "GW-A", 0, 1), SESSION_ID,
                            TIMESTAMP);
        seq.sequenceMessage(buf, 0, encodeIngressGatewayRegistered(buf, 0, 6, SOURCE_ID, "GW-B", 1, 0), SESSION_ID,
                            TIMESTAMP);

        final long rogueSession = 0xC0FFEEL;
        seq.sequenceMessage(buf, 0, encodeIngressGatewayStarted(buf, 0, 99), rogueSession, TIMESTAMP);

        assertEquals(Sequencer.NO_PROMOTION_TARGET, seq.sessionClosed(rogueSession, TIMESTAMP + 1),
                     "an instance with no Gateway row resolves to no group, so there is no sibling to promote");
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

        assertEquals(4L, globalSeqNoOf(seq, endLength)); // 4 roster rows

        // One frame per call, in roster order, on consecutive globalSeqNos — the adapter drains it.
        final int first = seq.pendingGatewayBootstrapActivation(TIMESTAMP + 1);
        final GatewayActiveDecoder clientPair = decodeGatewayActive(seq.buffer(), first);
        assertEquals(5, clientPair.gatewayId());
        assertEquals(5L, frameHeaderOf(seq.buffer()).globalSeqNo());

        final int second = seq.pendingGatewayBootstrapActivation(TIMESTAMP + 1);
        final GatewayActiveDecoder exchangePair = decodeGatewayActive(seq.buffer(), second);
        // Not the client pair's standby: a second rank-0 row used to overwrite the first designation, so
        // whichever logical gateway loaded last took the only bootstrap and the other never got one.
        assertEquals(8, exchangePair.gatewayId());
        assertEquals(6L, frameHeaderOf(seq.buffer()).globalSeqNo());

        assertEquals(Sequencer.NO_FRAME, seq.pendingGatewayBootstrapActivation(TIMESTAMP + 1),
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
        seq.sequenceMessage(buf, 0, encodeIngressGatewayStarted(buf, 0, 8), exchangeSession, TIMESTAMP);

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
        assertNotEquals(Sequencer.NO_FRAME, seq.pendingGatewayBootstrapActivation(TIMESTAMP)); // designates 5
        assertNotEquals(Sequencer.NO_FRAME, seq.pendingGatewayBootstrapActivation(TIMESTAMP)); // designates 8

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
        seq.pendingGatewayBootstrapActivation(TIMESTAMP); // designates 5
        seq.pendingGatewayBootstrapActivation(TIMESTAMP); // designates 8

        // Only the client pair's primary comes up.
        seq.sequenceMessage(buf, 0, encodeIngressGatewayStarted(buf, 0, 5), 0xA11CEL, TIMESTAMP + 1);

        final long deadline = TIMESTAMP + Sequencer.GATEWAY_ACTIVATION_TIMEOUT_MS;
        assertEquals(9, decodeGatewayActive(seq.buffer(), seq.pendingGatewayActivationTimeout(deadline)).gatewayId(),
                     "the exchange pair is still overdue, and is walked past the answered entry to reach it");
        assertEquals(Sequencer.NO_FRAME, seq.pendingGatewayActivationTimeout(deadline),
                     "the healthy pair is disarmed, not merely quiet");
    }

    /**
     * Two active/standby pairs: 5/6 under SOURCE_ID, 8/9 under EXCHANGE_SOURCE_ID, in roster order.
     * One complete roster run — remaining counts down to 0 on the last row. Returns that row's frame
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
     * Encodes a frame carrying a fully-populated session Heartbeat, as an Artio leg would submit it —
     * the all-fixed-block exemplar for the copy-through. The sequencer never opens payloadId 3; the test
     * decodes it afterwards only to prove every byte came across.
     */
    private static int encodeIngressHeartbeat(final MutableDirectBuffer buffer, final int offset) {
        final MutableDirectBuffer payload = new ExpandableArrayBuffer(128);
        final org.limitless.phixeron.sbe.session.HeartbeatEncoder encoder =
            new org.limitless.phixeron.sbe.session.HeartbeatEncoder();

        encoder.wrapAndApplyHeader(payload, 0, new org.limitless.phixeron.sbe.session.MessageHeaderEncoder());
        encoder.direction(org.limitless.phixeron.sbe.session.Direction.Client)
            .sender("CLIENT")
            .target("PHIXERON")
            .seqNum(4321L)
            .sendingTimeMs(1_699_999_999_000L)
            .possDupFlag(org.limitless.phixeron.sbe.session.PossDupFlag.Yes)
            .testReqID("TESTREQ-0001");

        return encodeIngressPayloadFrame(buffer, offset, SOURCE_ID, CONNECTION_ID, SESSION_PAYLOAD_ID, payload,
                                         corePayloadLength(encoder.encodedLength()));
    }

    /**
     * Encodes a schema-200 roster row, as {@code clusterctl load-topology} submits it. {@code remaining}
     * is the rows left after this one; 0 makes it the last, which is what the bootstrap fires on.
     */
    private static int encodeIngressGatewayRegistered(final MutableDirectBuffer buffer, final int offset,
                                                      final int gatewayId, final int gatewaySourceId,
                                                      final String gatewayName, final int preferenceRank,
                                                      final int remaining) {
        final MutableDirectBuffer payload = new ExpandableArrayBuffer(128);
        final org.limitless.phixeron.sbe.frame.GatewayRegisteredEncoder encoder = new org.limitless.phixeron.sbe.frame.GatewayRegisteredEncoder();
        encoder.wrapAndApplyHeader(payload, 0, new org.limitless.phixeron.sbe.frame.MessageHeaderEncoder());
        encoder.remaining(remaining)
            .gatewayId(gatewayId)
            .gatewaySourceId(gatewaySourceId)
            .gatewayName(gatewayName)
            .preferenceRank((short)preferenceRank);
        // The operator tool's own sourceId (not a gateway's), outside the gateway-sourceId set.
        return encodeIngressFrame(buffer, offset, 99, -1, payload, corePayloadLength(encoder.encodedLength()));
    }

    /** Encodes a frame carrying a session Logout, whose trailing {@code text} is var-data, not fixed block. */
    private static int encodeIngressLogout(final MutableDirectBuffer buffer, final int offset, final String reason) {
        final MutableDirectBuffer payload = new ExpandableArrayBuffer(128);
        final org.limitless.phixeron.sbe.session.LogoutEncoder encoder =
            new org.limitless.phixeron.sbe.session.LogoutEncoder();

        encoder.wrapAndApplyHeader(payload, 0, new org.limitless.phixeron.sbe.session.MessageHeaderEncoder());
        encoder.direction(org.limitless.phixeron.sbe.session.Direction.Client)
            .sender("CLIENT")
            .target("PHIXERON")
            .seqNum(99L)
            .sendingTimeMs(1_699_999_999_000L)
            .possDupFlag(org.limitless.phixeron.sbe.session.PossDupFlag.No)
            .text(reason);

        return encodeIngressPayloadFrame(buffer, offset, SOURCE_ID, CONNECTION_ID, SESSION_PAYLOAD_ID, payload,
                                         corePayloadLength(encoder.encodedLength()));
    }

    /**
     * Encodes a schema-200 ClientConnected, as the FIX gateway submits it when it accepts a TCP
     * connection. Header-only: the connection it describes is entirely in {@code header}.
     */
    private static int encodeIngressClientConnected(final MutableDirectBuffer buffer, final int offset) {
        return encodeIngressClientConnected(buffer, offset, CONNECTION_ID);
    }

    /** As above, for the one connection identity {@code connectedClientCount} keys its gauge on. */
    private static int encodeIngressClientConnected(final MutableDirectBuffer buffer, final int offset,
                                                    final int connectionId) {
        final MutableDirectBuffer payload = new ExpandableArrayBuffer(64);
        final org.limitless.phixeron.sbe.frame.ClientConnectedEncoder encoder = new org.limitless.phixeron.sbe.frame.ClientConnectedEncoder();
        encoder.wrapAndApplyHeader(payload, 0, new org.limitless.phixeron.sbe.frame.MessageHeaderEncoder());
        encoder.putConnectionData(new byte[0], 0, 0);
        return encodeIngressFrame(buffer, offset, SOURCE_ID, connectionId, payload,
                                  corePayloadLength(encoder.encodedLength()));
    }

    /** Encodes a schema-200 ClientDisconnected; the mirror of {@link #encodeIngressClientConnected}. */
    private static int encodeIngressClientDisconnected(final MutableDirectBuffer buffer, final int offset) {
        return encodeIngressClientDisconnected(buffer, offset, CONNECTION_ID);
    }

    /** As above, for a named connection. */
    private static int encodeIngressClientDisconnected(final MutableDirectBuffer buffer, final int offset,
                                                       final int connectionId) {
        final MutableDirectBuffer payload = new ExpandableArrayBuffer(64);
        final org.limitless.phixeron.sbe.frame.ClientDisconnectedEncoder encoder = new org.limitless.phixeron.sbe.frame.ClientDisconnectedEncoder();
        encoder.wrapAndApplyHeader(payload, 0, new org.limitless.phixeron.sbe.frame.MessageHeaderEncoder());
        return encodeIngressFrame(buffer, offset, SOURCE_ID, connectionId, payload,
                                  corePayloadLength(encoder.encodedLength()));
    }

    /**
     * Encodes a schema-200 GatewayStarted, as an instance publishes it on activation: the one frame that
     * tells the sequencer which cluster session a given {@code gatewayId} is active on.
     */
    private static int encodeIngressGatewayStarted(final MutableDirectBuffer buffer, final int offset,
                                                   final int gatewayId) {
        final MutableDirectBuffer payload = new ExpandableArrayBuffer(64);
        final org.limitless.phixeron.sbe.frame.GatewayStartedEncoder encoder = new org.limitless.phixeron.sbe.frame.GatewayStartedEncoder();
        encoder.wrapAndApplyHeader(payload, 0, new org.limitless.phixeron.sbe.frame.MessageHeaderEncoder());
        encoder.gatewayId(gatewayId).firstConnectionId(1);
        return encodeIngressFrame(buffer, offset, SOURCE_ID, -1, payload,
                                  corePayloadLength(encoder.encodedLength()));
    }

    private static GatewayActiveDecoder decodeGatewayActive(final MutableDirectBuffer buffer, final int length) {
        final int payload = corePayloadOffset(buffer, length, GatewayActiveDecoder.TEMPLATE_ID);
        final org.limitless.phixeron.sbe.frame.MessageHeaderDecoder header = new org.limitless.phixeron.sbe.frame.MessageHeaderDecoder().wrap(buffer, payload);
        return new GatewayActiveDecoder().wrap(buffer, payload + org.limitless.phixeron.sbe.frame.MessageHeaderDecoder.ENCODED_LENGTH,
                                               header.blockLength(), header.version());
    }

    private static HeartbeatDecoder decodeHeartbeat(final MutableDirectBuffer buffer) {
        final org.limitless.phixeron.sbe.session.MessageHeaderDecoder header =
            sessionPayloadHeader(buffer, HeartbeatDecoder.TEMPLATE_ID);
        return new HeartbeatDecoder().wrap(buffer, SESSION_PAYLOAD_BODY_OFFSET, header.blockLength(),
                                           header.version());
    }

    private static LogoutDecoder decodeLogout(final MutableDirectBuffer buffer) {
        final org.limitless.phixeron.sbe.session.MessageHeaderDecoder header =
            sessionPayloadHeader(buffer, LogoutDecoder.TEMPLATE_ID);
        return new LogoutDecoder().wrap(buffer, SESSION_PAYLOAD_BODY_OFFSET, header.blockLength(), header.version());
    }

    /**
     * The payload header of the frame just encoded, having first checked the envelope names this
     * payloadId and this template — the {@code (payloadId, templateId)} pair a real consumer dispatches
     * on, since template ids are unique per schema only.
     */
    private static org.limitless.phixeron.sbe.session.MessageHeaderDecoder sessionPayloadHeader(
        final MutableDirectBuffer buffer, final int templateId) {
        assertEquals(SESSION_PAYLOAD_ID, frameHeaderOf(buffer).payloadId());
        final org.limitless.phixeron.sbe.session.MessageHeaderDecoder header =
            new org.limitless.phixeron.sbe.session.MessageHeaderDecoder().wrap(buffer, FRAME_OVERHEAD);
        assertEquals(org.limitless.phixeron.sbe.session.MessageHeaderDecoder.SCHEMA_ID, header.schemaId());
        assertEquals(templateId, header.templateId());
        return header;
    }

    private static LeadershipChangedDecoder decodeLeadershipChanged(final MutableDirectBuffer buffer,
                                                                    final int length) {
        final int payload = corePayloadOffset(buffer, length, LeadershipChangedDecoder.TEMPLATE_ID);
        final org.limitless.phixeron.sbe.frame.MessageHeaderDecoder header = new org.limitless.phixeron.sbe.frame.MessageHeaderDecoder().wrap(buffer, payload);
        return new LeadershipChangedDecoder().wrap(buffer, payload + org.limitless.phixeron.sbe.frame.MessageHeaderDecoder.ENCODED_LENGTH,
                                                   header.blockLength(), header.version());
    }

    private long globalSeqNoOf(final int length) {
        return globalSeqNoOf(sequencer, length);
    }

    /** Reads globalSeqNo off the frame just encoded — its header, at a fixed offset past the framing one. */
    private static long globalSeqNoOf(final Sequencer target, final int length) {
        assertNotEquals(Sequencer.NO_FRAME, length);
        return frameHeaderOf(target.buffer()).globalSeqNo();
    }

    /** The frame's own header — the identity every core payload relies on, since it carries none itself. */
    private static org.limitless.phixeron.sbe.frame.SequencedHeaderDecoder frameHeaderOf(final MutableDirectBuffer buffer) {
        return new org.limitless.phixeron.sbe.frame.SequencedHeaderDecoder().wrap(buffer, org.limitless.phixeron.sbe.frame.MessageHeaderDecoder.ENCODED_LENGTH);
    }

    /**
     * Wraps an already-encoded core payload in an {@code Unsequenced} frame, as every core producer now
     * does. {@code sessionId} is advisory on ingress — the sequencer overwrites it — so it goes out as -1.
     */
    private static int encodeIngressFrame(final MutableDirectBuffer buffer, final int offset, final int sourceId,
                                          final int connectionId, final MutableDirectBuffer payload,
                                          final int payloadLength) {
        return encodeIngressPayloadFrame(buffer, offset, sourceId, connectionId, Sequencer.CORE_PAYLOAD_ID, payload,
                                         payloadLength);
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

    /** A core payload is its own framing header plus what the message encoder wrote. */
    private static int corePayloadLength(final int encodedLength) {
        return org.limitless.phixeron.sbe.frame.MessageHeaderEncoder.ENCODED_LENGTH + encodedLength;
    }

    /** The same, for a message whose block is all there is. */
    private static int emptyCorePayloadLength(final int blockLength) {
        return org.limitless.phixeron.sbe.frame.MessageHeaderEncoder.ENCODED_LENGTH + blockLength;
    }

    /** The inner templateId of the frame just encoded, after checking it is a well-formed core frame. */
    private static int payloadTemplateIdOf(final MutableDirectBuffer buffer, final int length) {
        final int payload = corePayloadOffset(buffer, length);
        return new org.limitless.phixeron.sbe.frame.MessageHeaderDecoder().wrap(buffer, payload).templateId();
    }

    /** As above, asserting the payload is the core message expected. */
    private static int corePayloadOffset(final MutableDirectBuffer buffer, final int length,
                                         final int expectedTemplateId) {
        final int payload = corePayloadOffset(buffer, length);
        assertEquals(expectedTemplateId, new org.limitless.phixeron.sbe.frame.MessageHeaderDecoder().wrap(buffer, payload).templateId());
        return payload;
    }

    /**
     * Offset of the payload inside a {@code Sequenced} frame, checking the frame's shape on the way —
     * that it is one, and that the length it declares accounts for every byte the sequencer returned.
     */
    private static int corePayloadOffset(final MutableDirectBuffer buffer, final int length) {
        final org.limitless.phixeron.sbe.frame.MessageHeaderDecoder header = new org.limitless.phixeron.sbe.frame.MessageHeaderDecoder().wrap(buffer, 0);
        assertEquals(org.limitless.phixeron.sbe.frame.SequencedDecoder.SCHEMA_ID, header.schemaId());
        assertEquals(org.limitless.phixeron.sbe.frame.SequencedDecoder.TEMPLATE_ID, header.templateId());
        assertEquals(Sequencer.CORE_PAYLOAD_ID, frameHeaderOf(buffer).payloadId());
        final int prefix = org.limitless.phixeron.sbe.frame.MessageHeaderDecoder.ENCODED_LENGTH +
                           org.limitless.phixeron.sbe.frame.SequencedHeaderDecoder.ENCODED_LENGTH;
        final int payloadLength = buffer.getShort(prefix, java.nio.ByteOrder.LITTLE_ENDIAN) & 0xFFFF;
        final int payload = prefix + org.limitless.phixeron.sbe.frame.SequencedDecoder.payloadHeaderLength();
        assertEquals(length, payload + payloadLength);
        return payload;
    }
}
