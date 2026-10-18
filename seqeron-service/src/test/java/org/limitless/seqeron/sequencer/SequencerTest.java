package org.limitless.seqeron.sequencer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.management.ThreadMXBean;
import io.aeron.archive.client.AeronArchive;
import java.lang.management.ManagementFactory;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.limitless.seqeron.protocol.FrameLayer;
import org.limitless.seqeron.protocol.SystemFrame;
import org.limitless.seqeron.sbe.frame.ClusterHeartbeatDecoder;
import org.limitless.seqeron.sbe.frame.ConnectionClosedDecoder;
import org.limitless.seqeron.sbe.frame.ConnectionOpenedDecoder;
import org.limitless.seqeron.sbe.frame.GatewayActiveDecoder;
import org.limitless.seqeron.sbe.frame.LeadershipChangedDecoder;
import org.limitless.seqeron.sbe.frame.MessageHeaderDecoder;
import org.limitless.seqeron.sbe.frame.SessionClosedDecoder;
import org.limitless.seqeron.sbe.frame.SnapshotStartedDecoder;

/**
 * Unit tests for the sequencer's replicated state machine.
 *
 * <p>These deliberately touch no Aeron runtime — no media driver, no cluster, no Aeron mocks. {@link
 * Sequencer} is a pure function of its inputs, so the whole state machine is exercised by calling it
 * and decoding the frames it writes, which is what makes these tests fast and stable enough to run
 * on every build. Everything Aeron-dependent ({@link SequencerService}'s queue publication, archive
 * recording, timer scheduling) stays covered by the end-to-end scripts under
 * {@code seqeron-service/src/test/scripts}.
 *
 * <p>The invariants under test are the ones a replicated state machine cannot be allowed to break:
 * the same log produces byte-identical output on every node, {@code clusterSeqNo} is gap-free across
 * every event type, and the copy-through preserves every byte of the payload past the
 * header.
 */
class SequencerTest {
    private static final int SOURCE_ID = 7;

    /** The second logical gateway, for the multi-pair cases: a distinct gatewaySourceId. */
    private static final int SECOND_SOURCE_ID = 8;
    private static final int CONNECTION_ID = 42;
    private static final long SESSION_ID = 0x5EE51_0000L;
    private static final long TIMESTAMP = 1_700_000_000_000_000_000L; // consensus time is epoch ns

    /**
     * An allocated payloadId the sequencer does not own, standing in for any application protocol. Nothing here decodes one: the sequencer opens no payload, so this
     * suite may not either, and a test that reached for an application schema's codecs would put an
     * application's dictionary on the cluster tier's compile classpath to assert it is never used.
     */
    private static final int SESSION_PAYLOAD_ID = 3;

    /**
     * The exemplar payload: opaque bytes with no SBE structure at all, which is exactly what the sequencer
     * sees. Every byte value in 0..255 appears, so a copy that dropped, sign-extended or reordered one
     * shows up as an array mismatch rather than a field that happens to still decode.
     */
    private static final byte[] OPAQUE_PAYLOAD = opaqueBytes(256);

    /** A second, differently-sized payload: the copy length is the only thing that varies here. */
    private static final byte[] SHORT_OPAQUE_PAYLOAD = opaqueBytes(37);

    /** unsequencedHeader is 18 bytes, sequencedHeader 34 — the delta every sequenced frame grows by. */
    private static final int HEADER_GROWTH =
        org.limitless.seqeron.sbe.frame.SequencedHeaderDecoder.ENCODED_LENGTH -
        org.limitless.seqeron.sbe.frame.UnsequencedHeaderDecoder.ENCODED_LENGTH;

    /** A payload's own 2-byte length prefix, the last thing before the payload in either family. */
    private static final int PAYLOAD_PREFIX_LENGTH =
        org.limitless.seqeron.sbe.frame.SequencedDecoder.payloadHeaderLength();

    /**
     * Bytes a {@code Sequenced} frame adds around its payload: the framing header, the 34-byte
     * {@code sequencedHeader} and the payload's own length prefix.
     */
    private static final int FRAME_OVERHEAD =
        org.limitless.seqeron.sbe.frame.MessageHeaderDecoder.ENCODED_LENGTH +
        org.limitless.seqeron.sbe.frame.SequencedHeaderDecoder.ENCODED_LENGTH + PAYLOAD_PREFIX_LENGTH;

    /** Offset of the payload's length prefix in an {@code Unsequenced} frame, and of the payload itself. */
    private static final int INGRESS_PREFIX_OFFSET = org.limitless.seqeron.sbe.frame.MessageHeaderDecoder.ENCODED_LENGTH +
                                                     org.limitless.seqeron.sbe.frame.UnsequencedHeaderDecoder.ENCODED_LENGTH;
    private static final int INGRESS_PAYLOAD_OFFSET =
        INGRESS_PREFIX_OFFSET + org.limitless.seqeron.sbe.frame.UnsequencedDecoder.payloadHeaderLength();

    /** {@code varDataEncoding}'s nullValue: "absent", not a 65535-byte payload. */
    private static final int NULL_PAYLOAD_LENGTH = 65535;

    private final FrameRecorder frames = new FrameRecorder();
    private final Sequencer sequencer = new Sequencer(0, frames);
    private final MutableDirectBuffer ingress = new ExpandableArrayBuffer(512);

    // ── Copy-through fidelity ─────────────────────────────────────────────────

    @Test
    @DisplayName("sequencing an ingress message copies every byte past the header composite")
    void sequenceMessagePreservesEveryBytePastTheHeader() {
        final int ingressLength = encodeIngressPayload(ingress, 0);

        assertTrue(sequencer.sequenceMessage(ingress, 0, ingressLength, SESSION_ID, TIMESTAMP));
        final int length = frames.lastLength();

        // Every byte value a payload can hold survives the copy unread and unmodified.
        assertPayloadCopiedThrough(frames.last(), length, OPAQUE_PAYLOAD);
    }

    @Test
    @DisplayName("sequencing stamps the header without disturbing the submitter's identity")
    void sequenceMessageStampsHeader() {
        final int ingressLength = encodeIngressPayload(ingress, 0);

        assertTrue(sequencer.sequenceMessage(ingress, 0, ingressLength, SESSION_ID, TIMESTAMP));
        final int length = frames.lastLength();

        final org.limitless.seqeron.sbe.frame.SequencedHeaderDecoder header = frameHeaderOf(frames.last());
        // Carried through from the ingress message…
        assertEquals(SOURCE_ID, header.sourceId());
        assertEquals(CONNECTION_ID, header.connectionId());
        // …and applied by the sequencer.
        assertEquals(SESSION_ID, header.sessionId());
        assertEquals(1L, header.clusterSeqNo());
        assertEquals(TIMESTAMP, header.timestamp());
    }

    @Test
    @DisplayName("a payload of a different length survives the copy-through whole")
    void sequenceMessagePreservesADifferentlySizedPayload() {
        // Length is the only thing that varies between two payloads here, and it is the only thing the
        // copy arithmetic reads, so a second size is what exercises it. A payload is opaque: there is no
        // fixed block and no var-data to tell apart, only a prefix and that many bytes.
        final int ingressLength = encodeIngressPayload(ingress, 0, SHORT_OPAQUE_PAYLOAD);

        assertTrue(sequencer.sequenceMessage(ingress, 0, ingressLength, SESSION_ID, TIMESTAMP));
        final int length = frames.lastLength();

        assertPayloadCopiedThrough(frames.last(), length, SHORT_OPAQUE_PAYLOAD);
        // The whole ingress frame, minus nothing, plus the two int64s the sequenced header adds.
        assertEquals(ingressLength + HEADER_GROWTH, length);
    }

    @Test
    @DisplayName("the queue frame is a Sequenced envelope, its block exactly the header composite")
    void egressFrameIsASequencedEnvelope() {
        // The ingress and queue templates differ, and their blockLengths differ by exactly the stamp: the
        // envelope's block IS its header composite, so anything else here means the two are out of step.
        final int ingressLength = encodeIngressPayload(ingress, 0);
        final int ingressBlockLength = new MessageHeaderDecoder().wrap(ingress, 0).blockLength();

        sequencer.sequenceMessage(ingress, 0, ingressLength, SESSION_ID, TIMESTAMP);

        final MessageHeaderDecoder egress = new MessageHeaderDecoder().wrap(frames.last(), 0);
        assertEquals(ingressBlockLength + HEADER_GROWTH, egress.blockLength());
        assertEquals(org.limitless.seqeron.sbe.frame.SequencedDecoder.TEMPLATE_ID, egress.templateId());
        assertEquals(MessageHeaderDecoder.SCHEMA_ID, egress.schemaId());
    }

    @Test
    @DisplayName("a frame declaring a version this build does not encode is skipped")
    void frameFromAForeignVersionIsSkipped() {
        // The frame layer's own offsets are what every later read is measured from, and a version this
        // build has never encoded says they may not be where this code expects.
        final int ingressLength = encodeIngressPayload(ingress, 0);
        new org.limitless.seqeron.sbe.frame.MessageHeaderEncoder().wrap(ingress, 0)
            .version(MessageHeaderDecoder.SCHEMA_VERSION + 1);

        assertFalse(sequencer.sequenceMessage(ingress, 0, ingressLength, SESSION_ID, TIMESTAMP));
        assertEquals(0L, sequencer.clusterSeqNo());
    }

    @Test
    @DisplayName("sequencing reads the ingress message at a non-zero offset")
    void sequenceMessageHonoursOffset() {
        // Aeron hands fragments at an arbitrary offset into a shared term buffer, never 0.
        final int offset = 96;
        final int ingressLength = encodeIngressPayload(ingress, offset);

        assertTrue(sequencer.sequenceMessage(ingress, offset, ingressLength, SESSION_ID, TIMESTAMP));
        final int length = frames.lastLength();

        assertPayloadCopiedThrough(frames.last(), length, OPAQUE_PAYLOAD);
        assertEquals(CONNECTION_ID, frameHeaderOf(frames.last()).connectionId());
    }

    // ── clusterSeqNo ───────────────────────────────────────────────────────────

    @Test
    @DisplayName("clusterSeqNo is gap-free and monotone across every event type")
    void clusterSeqNoIsGapFreeAcrossEveryEventType() {
        // Forwarded ingress messages, clock heartbeats and elections all draw from the one counter; a
        // consumer that sees a gap treats it as lost data and re-walks history, so this must hold for
        // every emitting path. TCP lifecycle events are ordinary forwarded ingress — the gateway
        // publishes them — so they go through sequenceMessage here, not a synthesized encoder.
        final int ingressLength = encodeIngressPayload(ingress, 0);
        final MutableDirectBuffer lifecycle = new ExpandableArrayBuffer(64);

        final int connectedLength = encodeIngressConnectionOpened(lifecycle, 0);
        assertTrue(sequencer.sequenceMessage(lifecycle, 0, connectedLength, SESSION_ID, TIMESTAMP));
        assertTrue(sequencer.sequenceMessage(ingress, 0, ingressLength, SESSION_ID, TIMESTAMP));
        sequencer.clusterHeartbeat(TIMESTAMP + 1000);
        sequencer.leadershipChanged(1, 2, TIMESTAMP + 1500);
        assertTrue(sequencer.sequenceMessage(ingress, 0, ingressLength, SESSION_ID, TIMESTAMP));
        final int disconnectedLength = encodeIngressConnectionClosed(lifecycle, 0);
        assertTrue(sequencer.sequenceMessage(lifecycle, 0, disconnectedLength, SESSION_ID, TIMESTAMP + 2000));

        assertEquals(6, frames.count());
        for (int i = 0; i < frames.count(); i++) {
            assertEquals(i + 1L, frameHeaderOf(frames.frame(i)).clusterSeqNo());
        }
        assertEquals(6L, sequencer.clusterSeqNo());
    }

    @Test
    @DisplayName("a term the same member wins again still gets its own leadership frame")
    void reElectedLeaderStillGetsALeadershipFrame() {
        // The re-election closed ingress too, so a producer needs this boundary to count what it lost.
        sequencer.leadershipChanged(1, 1, TIMESTAMP);
        assertEquals(1L, sequencer.clusterSeqNo());

        sequencer.leadershipChanged(2, 1, TIMESTAMP + 100);
        assertEquals(2, frames.count());
        assertEquals(2L, sequencer.clusterSeqNo());
        final LeadershipChangedDecoder decoded = decodeLeadershipChanged(frames.last(), frames.lastLength());
        assertEquals(1, decoded.newLeaderMemberId());
        assertEquals(2, decoded.leadershipTermId());
        assertEquals(TIMESTAMP + 100, frameHeaderOf(frames.last()).timestamp());
    }

    // ── Ingress validation ────────────────────────────────────────────────────

    @Test
    @DisplayName("a frame too short to hold the header composite is skipped, not thrown on")
    void shortFrameIsSkippedRatherThanThrownOn() {
        // Throwing here would land on every node — the frame is already committed to the replicated log
        // — and again on every replay of it, leaving the log unreplayable and the cluster unrecoverable
        // without surgery. Before the length check this computed a
        // negative copyLength and threw out of putBytes.
        final int ingressLength = encodeIngressPayload(ingress, 0);

        assertFalse(sequencer.sequenceMessage(ingress, 0, FrameLayer.MIN_INGRESS_LENGTH - 1, SESSION_ID, TIMESTAMP));
        assertEquals(0L, sequencer.clusterSeqNo(), "a skipped message must consume no sequence number");

        // The next good message is still clusterSeqNo 1: consumers detect loss by gaps, so a skip has to
        // leave the numbering contiguous rather than burn a number on a frame nobody will ever receive.
        assertEquals(1L, clusterSeqNoOf(sequencer.sequenceMessage(ingress, 0, ingressLength, SESSION_ID, TIMESTAMP)));
    }

    @Test
    @DisplayName("a frame from a foreign schema is skipped")
    void foreignSchemaIsSkipped() {
        // Every offset sequenceMessage reads is a schema-210 offset; under another schema they address
        // something else entirely, so the frame is refused rather than re-stamped as if it were ours.
        final int ingressLength = encodeIngressPayload(ingress, 0);
        new org.limitless.seqeron.sbe.frame.MessageHeaderEncoder().wrap(ingress, 0).schemaId(999);

        assertFalse(sequencer.sequenceMessage(ingress, 0, ingressLength, SESSION_ID, TIMESTAMP));
        assertEquals(0L, sequencer.clusterSeqNo());
    }

    // -- Frame validation ------------------------------------------------------
    // Each of these reads a field out of a frame the sequencer did not encode, and each check
    // establishes what the next may read. Admitting any of them decodes bytes whose layout nothing has
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
        assertFalse(sequencer.sequenceMessage(ingress, 0, length, SESSION_ID, TIMESTAMP));
        putIngressPayloadLength(declared - 1);
        assertFalse(sequencer.sequenceMessage(ingress, 0, length, SESSION_ID, TIMESTAMP));
        putIngressPayloadLength(NULL_PAYLOAD_LENGTH);
        assertFalse(sequencer.sequenceMessage(ingress, 0, length, SESSION_ID, TIMESTAMP));

        putIngressPayloadLength(declared);
        assertTrue(sequencer.sequenceMessage(ingress, 0, length, SESSION_ID, TIMESTAMP));
        assertEquals(1L, sequencer.clusterSeqNo(), "only the well-formed frame consumed a sequence number");
    }

    @Test
    @DisplayName("payloadId 0 is not a protocol and is skipped")
    void zeroPayloadIdIsSkipped() {
        final int length = encodeIngressConnectionOpened(ingress, 0);
        new org.limitless.seqeron.sbe.frame.UnsequencedHeaderEncoder()
            .wrap(ingress, org.limitless.seqeron.sbe.frame.MessageHeaderEncoder.ENCODED_LENGTH).payloadId(0);

        assertFalse(sequencer.sequenceMessage(ingress, 0, length, SESSION_ID, TIMESTAMP));
        assertEquals(0L, sequencer.clusterSeqNo());
    }

    @Test
    @DisplayName("core's retired payloadId 1 is refused on ingress rather than copied through")
    void retiredCorePayloadIdIsRefusedOnIngress() {
        // Core is not an application and no longer rides a payload (§6.1). Refusing 1 rather than
        // reserving it is what makes a producer still on the old build fail loudly instead of having core
        // bytes copied onto the queue as an application payload nothing will ever decode.
        final int length = encodeIngressPayloadFrame(ingress, 0, SOURCE_ID, CONNECTION_ID, 1,
                                                     new ExpandableArrayBuffer(8), 4);

        assertFalse(sequencer.sequenceMessage(ingress, 0, length, SESSION_ID, TIMESTAMP));
        assertEquals(0L, sequencer.clusterSeqNo());
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
        assertFalse(sequencer.sequenceMessage(ingress, 0, asSystemBody, SESSION_ID, TIMESTAMP));

        final org.limitless.seqeron.sbe.frame.ClusterHeartbeatEncoder encoder =
            new org.limitless.seqeron.sbe.frame.ClusterHeartbeatEncoder();
        encoder.wrapAndApplyHeader(ingress, 0, new org.limitless.seqeron.sbe.frame.MessageHeaderEncoder());
        encoder.header().sourceId(SOURCE_ID).connectionId(CONNECTION_ID).sessionId(-1)
            .systemEventType(SystemFrame.CLUSTER_HEARTBEAT).clusterSeqNo(1).timestamp(TIMESTAMP);
        final int asOwnTemplate =
            org.limitless.seqeron.sbe.frame.MessageHeaderEncoder.ENCODED_LENGTH + encoder.encodedLength();
        assertFalse(sequencer.sequenceMessage(ingress, 0, asOwnTemplate, SESSION_ID, TIMESTAMP));

        assertEquals(0L, sequencer.clusterSeqNo());
    }

    @Test
    @DisplayName("an unallocated systemEventType is refused")
    void unallocatedSystemEventTypeIsRefused() {
        final int length = encodeIngressSystemFrame(ingress, 0, SOURCE_ID, CONNECTION_ID, 4242,
                                                    new ExpandableArrayBuffer(8), 0);

        assertFalse(sequencer.sequenceMessage(ingress, 0, length, SESSION_ID, TIMESTAMP));
        assertEquals(0L, sequencer.clusterSeqNo());
    }

    @Test
    @DisplayName("a system payload too short for the field the sequencer reads is skipped")
    void systemBodyTooShortForItsBlockIsSkipped() {
        // Fitting the frame exactly says nothing about being long enough for a payload field, and the floor
        // is the decoder's compiled block length -- never the wire's, which an SBE decoder ignores when it
        // reads a fixed-width field at its fixed offset.
        encodeIngressGatewayStarted(ingress, 0, 5);
        final int shortBody = org.limitless.seqeron.sbe.frame.GatewayStartedDecoder.BLOCK_LENGTH - 1;
        putIngressPayloadLength(shortBody);

        assertFalse(sequencer.sequenceMessage(ingress, 0, INGRESS_PAYLOAD_OFFSET + shortBody, SESSION_ID, TIMESTAMP));
        assertEquals(0L, sequencer.clusterSeqNo());
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
        final org.limitless.seqeron.sbe.frame.UnsequencedEncoder encoder = new org.limitless.seqeron.sbe.frame.UnsequencedEncoder();
        encoder.wrapAndApplyHeader(ingress, 0, new org.limitless.seqeron.sbe.frame.MessageHeaderEncoder());
        encoder.header().sourceId(SOURCE_ID).connectionId(CONNECTION_ID).sessionId(-1).payloadId(4);
        encoder.putPayload(payload, 0, opaque.length);
        final int length = org.limitless.seqeron.sbe.frame.MessageHeaderEncoder.ENCODED_LENGTH + encoder.encodedLength();

        assertTrue(sequencer.sequenceMessage(ingress, 0, length, SESSION_ID, TIMESTAMP));
        final int sequenced = frames.lastLength();

        assertEquals(4, frameHeaderOf(frames.last()).payloadId(), "the payloadId is carried, not rewritten");
        final byte[] roundTripped = new byte[opaque.length];
        frames.last().getBytes(FRAME_OVERHEAD, roundTripped);
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
        final int exact = org.limitless.seqeron.sbe.frame.UnsequencedHeaderDecoder.ENCODED_LENGTH;

        new org.limitless.seqeron.sbe.frame.MessageHeaderEncoder().wrap(ingress, 0).blockLength(exact - 1);
        assertFalse(sequencer.sequenceMessage(ingress, 0, ingressLength, SESSION_ID, TIMESTAMP));
        new org.limitless.seqeron.sbe.frame.MessageHeaderEncoder().wrap(ingress, 0).blockLength(exact + 1);
        assertFalse(sequencer.sequenceMessage(ingress, 0, ingressLength, SESSION_ID, TIMESTAMP));
        assertEquals(0L, sequencer.clusterSeqNo(), "a skipped message must consume no sequence number");

        new org.limitless.seqeron.sbe.frame.MessageHeaderEncoder().wrap(ingress, 0).blockLength(exact);
        assertTrue(sequencer.sequenceMessage(ingress, 0, ingressLength, SESSION_ID, TIMESTAMP));
        assertEquals(1L, sequencer.clusterSeqNo());
    }

    @Test
    @DisplayName("a frame above the payload ceiling is skipped, and one exactly at it is sequenced")
    void oversizedFrameIsSkipped() {
        // T-2's pinned constant, and condition 1's ceiling as the backstop behind it: a conforming producer
        // refuses this on its own stack, so a frame that reaches here is one that is not conforming. The
        // ceiling is compiled in and never read off this node's MTU -- a node checking a smaller one than
        // its peers would fork clusterSeqNo.
        final MutableDirectBuffer payload = new ExpandableArrayBuffer(FrameLayer.MAX_PAYLOAD_LENGTH + 1);
        final int exact = encodeIngressPayloadFrame(ingress, 0, SOURCE_ID, CONNECTION_ID, SESSION_PAYLOAD_ID,
                                                    payload, FrameLayer.MAX_PAYLOAD_LENGTH);
        assertEquals(FrameLayer.MAX_INGRESS_LENGTH, exact);
        assertTrue(sequencer.sequenceMessage(ingress, 0, exact, SESSION_ID, TIMESTAMP));

        final int oversized = encodeIngressPayloadFrame(ingress, 0, SOURCE_ID, CONNECTION_ID, SESSION_PAYLOAD_ID,
                                                        payload, FrameLayer.MAX_PAYLOAD_LENGTH + 1);
        assertFalse(sequencer.sequenceMessage(ingress, 0, oversized, SESSION_ID, TIMESTAMP));
        assertEquals(1L, sequencer.clusterSeqNo(), "a skipped message must consume no sequence number");
    }

    @Test
    @DisplayName("the sourceId reserved for the cluster's own frames is refused on ingress")
    void reservedSourceIdIsSkipped() {
        // -1 marks the synthesized class (F-4). Admitting one from a producer would let it forge that
        // class -- a heartbeat or a leadership record no node encoded.
        final MutableDirectBuffer payload = new ExpandableArrayBuffer(64);
        final int length = encodeIngressPayloadFrame(ingress, 0, Sequencer.NO_SOURCE_ID, CONNECTION_ID,
                                                     SESSION_PAYLOAD_ID, payload, 4);

        assertFalse(sequencer.sequenceMessage(ingress, 0, length, SESSION_ID, TIMESTAMP));
        assertEquals(0L, sequencer.clusterSeqNo());
    }

    @Test
    @DisplayName("S-6: a core frame claims a listed sourceId only from a session bound to it")
    void listedSourceIdNeedsABoundSession() {
        // Without this a process that is not the gateway can speak for its logical gateway: publish a
        // GatewayStarted under someone else's sourceId and the sequencer promotes on its session close.
        sequencer.sequenceMessage(ingress, 0, encodeIngressGatewayRegistered(ingress, 0, 5, SOURCE_ID, "GW-A", 0, 0),
                                  SESSION_ID, TIMESTAMP);
        final long afterList = sequencer.clusterSeqNo();
        final long rogue = SESSION_ID + 1;

        // Case 1's second half: the row's gatewaySourceId must be the one the frame carries.
        assertFalse(sequencer.sequenceMessage(ingress, 0, encodeIngressGatewayStarted(ingress, 0, 5,
                                                                                      SECOND_SOURCE_ID),
                                              rogue, TIMESTAMP));
        // Case 2: no binding, so this session may not speak for that logical gateway.
        assertFalse(sequencer.sequenceMessage(ingress, 0, encodeIngressConnectionOpened(ingress, 0, 1), rogue,
                                              TIMESTAMP));
        // Case 4: the same sourceId under an application payload is not core's to check.
        assertTrue(sequencer.sequenceMessage(ingress, 0,
                                             encodeIngressPayloadFrame(ingress, 0, SOURCE_ID, CONNECTION_ID,
                                                                       SESSION_PAYLOAD_ID,
                                                                       new ExpandableArrayBuffer(8), 4),
                                             rogue, TIMESTAMP));

        // The agreeing GatewayStarted binds, and only the session that made the binding is admitted.
        assertTrue(sequencer.sequenceMessage(ingress, 0, encodeIngressGatewayStarted(ingress, 0, 5), rogue, TIMESTAMP));
        assertTrue(sequencer.sequenceMessage(ingress, 0, encodeIngressConnectionOpened(ingress, 0, 1), rogue,
                                             TIMESTAMP));
        assertFalse(sequencer.sequenceMessage(ingress, 0, encodeIngressConnectionOpened(ingress, 0, 2),
                                              SESSION_ID + 2, TIMESTAMP));
        assertEquals(afterList + 3, sequencer.clusterSeqNo(), "every rejection left clusterSeqNo where it was");
    }

    @Test
    @DisplayName("S-6: an instance the log no longer designates sequences nothing, until it is designated again")
    void deposedInstanceSequencesNothing() {
        // An operator handover leaves the old instance's session open: until it reads the GatewayActive that
        // deposed it, it still serves, and only the sequencer can keep what it sends off the log.
        sequencer.sequenceMessage(ingress, 0, encodeIngressGatewayRegistered(ingress, 0, 5, SOURCE_ID, "GW-A", 0, 1),
                                  SESSION_ID, TIMESTAMP);
        sequencer.sequenceMessage(ingress, 0, encodeIngressGatewayRegistered(ingress, 0, 6, SOURCE_ID, "GW-B", 1, 0),
                                  SESSION_ID, TIMESTAMP);
        final long first = 0xA11CEL;
        final long second = 0xB0B0L;
        sequencer.sequenceMessage(ingress, 0, encodeIngressGatewayStarted(ingress, 0, 5), first, TIMESTAMP);
        sequencer.sequenceMessage(ingress, 0, encodeIngressConnectionOpened(ingress, 0, 1), first, TIMESTAMP);

        sequencer.sequenceMessage(ingress, 0, encodeIngressActivationRequested(ingress, 0, 6), SESSION_ID, TIMESTAMP);
        final long deposed = sequencer.clusterSeqNo();
        assertFalse(sequencer.sequenceMessage(ingress, 0, encodeIngressPayload(ingress, 0), first, TIMESTAMP));
        assertFalse(sequencer.sequenceMessage(ingress, 0, encodeIngressConnectionOpened(ingress, 0, 2), first,
                                              TIMESTAMP));
        assertFalse(sequencer.sequenceMessage(ingress, 0, encodeIngressGatewayStarted(ingress, 0, 5), first,
                                              TIMESTAMP),
                    "nor may it declare itself started again");
        assertEquals(deposed, sequencer.clusterSeqNo());

        assertTrue(sequencer.sequenceMessage(ingress, 0, encodeIngressGatewayStarted(ingress, 0, 6), second,
                                             TIMESTAMP));
        assertTrue(sequencer.sequenceMessage(ingress, 0, encodeIngressPayload(ingress, 0), second, TIMESTAMP));

        // Asked back, the same session is admitted again.
        sequencer.sequenceMessage(ingress, 0, encodeIngressActivationRequested(ingress, 0, 5), SESSION_ID, TIMESTAMP);
        assertTrue(sequencer.sequenceMessage(ingress, 0, encodeIngressGatewayStarted(ingress, 0, 5), first, TIMESTAMP));
        assertTrue(sequencer.sequenceMessage(ingress, 0, encodeIngressPayload(ingress, 0), first, TIMESTAMP));
        assertFalse(sequencer.sequenceMessage(ingress, 0, encodeIngressPayload(ingress, 0), second, TIMESTAMP),
                    "and 6, deposed in turn, is not");
        assertEquals(4, sequencer.rejectedFrameCount());
    }

    @Test
    @DisplayName("S-6: designated again, an instance's still-bound session is admitted with no new GatewayStarted")
    void redesignatedInstanceKeepsItsBinding() {
        // Case 3 asks only that the latest GatewayActive name the bound instance and that no later
        // GatewayStarted replaced the binding; an operator's round trip does neither.
        sequencer.sequenceMessage(ingress, 0, encodeIngressGatewayRegistered(ingress, 0, 5, SOURCE_ID, "GW-A", 0, 1),
                                  SESSION_ID, TIMESTAMP);
        sequencer.sequenceMessage(ingress, 0, encodeIngressGatewayRegistered(ingress, 0, 6, SOURCE_ID, "GW-B", 1, 0),
                                  SESSION_ID, TIMESTAMP);
        final long first = 0xA11CEL;
        sequencer.sequenceMessage(ingress, 0, encodeIngressGatewayStarted(ingress, 0, 5), first, TIMESTAMP);

        sequencer.sequenceMessage(ingress, 0, encodeIngressActivationRequested(ingress, 0, 6), SESSION_ID, TIMESTAMP);
        assertFalse(sequencer.sequenceMessage(ingress, 0, encodeIngressPayload(ingress, 0), first, TIMESTAMP));
        sequencer.sequenceMessage(ingress, 0, encodeIngressActivationRequested(ingress, 0, 5), SESSION_ID, TIMESTAMP);
        assertTrue(sequencer.sequenceMessage(ingress, 0, encodeIngressPayload(ingress, 0), first, TIMESTAMP));
    }

    @Test
    @DisplayName("S-6: a replaced session stays refused after the session that replaced it closes")
    void replacedSessionStaysRefusedUntilItCloses() {
        sequencer.sequenceMessage(ingress, 0, encodeIngressGatewayRegistered(ingress, 0, 5, SOURCE_ID, "GW-A", 0, 1),
                                  SESSION_ID, TIMESTAMP);
        sequencer.sequenceMessage(ingress, 0, encodeIngressGatewayRegistered(ingress, 0, 6, SOURCE_ID, "GW-B", 1, 0),
                                  SESSION_ID, TIMESTAMP);
        final long replaced = 0xDEADL;
        final long replacing = 0xA11CEL;
        sequencer.sequenceMessage(ingress, 0, encodeIngressGatewayStarted(ingress, 0, 5), replaced, TIMESTAMP);
        sequencer.sequenceMessage(ingress, 0, encodeIngressGatewayStarted(ingress, 0, 5), replacing, TIMESTAMP);

        assertTrue(sequencer.sessionClosed(replacing, TIMESTAMP + 1));
        sequencer.sequenceMessage(ingress, 0, encodeIngressActivationRequested(ingress, 0, 5), SESSION_ID, TIMESTAMP);
        assertFalse(sequencer.sequenceMessage(ingress, 0, encodeIngressPayload(ingress, 0), replaced, TIMESTAMP));
        assertFalse(sequencer.sessionClosed(replaced, TIMESTAMP + 2), "and its own close promotes nothing");
    }

    @Test
    @DisplayName("a session that declares another gateway's instance leaves the first one unbound")
    void rebindingASessionToAnotherGatewayUnbindsTheFirst() {
        final Sequencer seq = new Sequencer(0, frames);
        final MutableDirectBuffer buf = new ExpandableArrayBuffer(512);
        loadTwoPairs(seq, buf); // designates 5 and 8
        final long session = 0xA11CEL;
        seq.sequenceMessage(buf, 0, encodeIngressGatewayStarted(buf, 0, 5), session, TIMESTAMP + 1);
        seq.sequenceMessage(buf, 0, encodeIngressGatewayStarted(buf, 0, 8, SECOND_SOURCE_ID), session, TIMESTAMP + 1);

        seq.clusterHeartbeat(TIMESTAMP + Sequencer.GATEWAY_ACTIVATION_TIMEOUT_NS);
        assertEquals(6, lastGatewayActive(), "5 is unanswered again, so it is handed over");
        assertTrue(seq.sessionClosed(session, TIMESTAMP + Sequencer.GATEWAY_ACTIVATION_TIMEOUT_NS + 1));
        assertEquals(9, promotedOnClose(session), "and the session's close is 8's");
    }

    // ── Determinism ───────────────────────────────────────────────────────────

    @Test
    @DisplayName("replaying the same log on a second instance produces byte-identical frames")
    void replayingTheSameLogProducesByteIdenticalFrames() {
        // The core replicated-state-machine property: two nodes fed the same committed log must emit
        // the same bytes, so their queue recordings are interchangeable and any node can serve replay.
        // This is a regression guard — it fails the moment anything non-deterministic (a wall clock, a
        // random id, an iteration-order dependency) creeps into an encode path.
        final byte[][] framesA = runLog();
        final byte[][] framesB = runLog();

        assertEquals(framesA.length, framesB.length);
        for (int i = 0; i < framesA.length; i++) {
            assertArrayEquals(framesA[i], framesB[i], "frame " + i + " diverged between nodes");
        }
    }

    /** Drives one fixed "committed log" through a fresh sequencer, returning every frame it published. */
    private static byte[][] runLog() {
        final FrameRecorder recorded = new FrameRecorder();
        final Sequencer target = new Sequencer(0, recorded);
        final MutableDirectBuffer message = new ExpandableArrayBuffer(512);
        final int messageLength = encodeIngressPayload(message, 0);
        final MutableDirectBuffer lifecycle = new ExpandableArrayBuffer(64);
        final int connectedLength = encodeIngressConnectionOpened(lifecycle, 0);

        target.sequenceMessage(lifecycle, 0, connectedLength, SESSION_ID, TIMESTAMP);
        target.leadershipChanged(0, 0, TIMESTAMP + 1);
        final MutableDirectBuffer marker = new ExpandableArrayBuffer(64);
        target.sequenceMessage(marker, 0, encodeIngressSnapshotPolicy(marker, 0, 2), SESSION_ID, TIMESTAMP + 2);
        for (int i = 0; i < 5; i++) {
            target.sequenceMessage(message, 0, messageLength, SESSION_ID, TIMESTAMP + i);
            target.clusterHeartbeat(TIMESTAMP + 1_000_000_000L * i);
        }
        target.sequenceMessage(marker, 0, encodeIngressSnapshotRequested(marker, 0, 7), SESSION_ID, TIMESTAMP + 8);
        target.leadershipChanged(1, 0, TIMESTAMP + 9);
        target.leadershipChanged(2, 1, TIMESTAMP + 10);
        final int disconnectedLength = encodeIngressConnectionClosed(lifecycle, 0);
        target.sequenceMessage(lifecycle, 0, disconnectedLength, SESSION_ID, TIMESTAMP + 11);
        return recorded.all().toArray(new byte[0][]);
    }

    // ── Snapshot ──────────────────────────────────────────────────────────────

    @Test
    @DisplayName("taking a snapshot is refused rather than persisting part of the replicated state")
    void takingASnapshotIsRefused() {
        // The one SequencerService call these tests make, and it starts no Aeron runtime: the throw
        // precedes any use of the publication, which is why null is safe to pass. This used to persist
        // clusterSeqNo alone and drop the six other replicated fields, so a restored node re-emitted the
        // bootstrap GatewayActive and stopped producing frames identical to its peers'
        // Recovery is full-log replay, which rebuilds all of it.
        assertThrows(UnsupportedOperationException.class,
                     () -> new SequencerService(0, new AeronArchive.Context(), () -> { }).onTakeSnapshot(null));
    }

    // ── Snapshot rounds (doc/snapshot.md §3) ──────────────────────────────────

    @Test
    @DisplayName("with no policy registered, a SnapshotRequested is sequenced and nothing follows it")
    void snapshotRequestWithoutAPolicyStartsNoRound() {
        final int request = encodeIngressSnapshotRequested(ingress, 0, 7);
        assertTrue(sequencer.sequenceMessage(ingress, 0, request, SESSION_ID, TIMESTAMP));
        assertEquals(SystemFrame.SNAPSHOT_REQUESTED, systemEventTypeOf(frames.last(), frames.lastLength()));
        sequencer.clusterHeartbeat(TIMESTAMP + 1_000_000_000_000L);
        assertEquals(2, frames.count(), "the request and the heartbeat, and no SnapshotStarted behind either");
        assertEquals(0L, sequencer.snapshotRound());
    }

    @Test
    @DisplayName("an operator request is answered by SnapshotStarted at the next clusterSeqNo, and each starts a round")
    void snapshotRequestIsAnsweredAtTheNextClusterSeqNo() {
        sequencer.sequenceMessage(ingress, 0, encodeIngressSnapshotPolicy(ingress, 0, 0), SESSION_ID, TIMESTAMP);
        for (long round = 1; round <= 2; round++) {
            final long timestamp = TIMESTAMP + round;
            final int request = frames.count();
            sequencer.sequenceMessage(ingress, 0, encodeIngressSnapshotRequested(ingress, 0, round), SESSION_ID,
                                      timestamp);
            assertEquals(request + 2, frames.count(), "one request, one round");
            final SnapshotStartedDecoder decoder = decodeSnapshotStarted(frames.last(), frames.lastLength());
            assertEquals(round, decoder.round(), "a new request supersedes the open round");
            final long requestSeqNo = frameHeaderOf(frames.frame(request)).clusterSeqNo();
            assertEquals(requestSeqNo + 1, systemHeaderOf(frames.last()).clusterSeqNo(), "R follows the request");
            assertEquals(Sequencer.NO_SOURCE_ID, systemHeaderOf(frames.last()).sourceId());
            assertEquals(timestamp, systemHeaderOf(frames.last()).timestamp());
        }
        sequencer.clusterHeartbeat(TIMESTAMP + 1_000_000_000_000L);
        assertNothingBehindHeartbeat("interval 0: rounds start only on request");
    }

    @Test
    @DisplayName("the interval counts from the policy row, then from each round's start, operator rounds included")
    void snapshotIntervalCountsFromTheLastStart() {
        final long interval = 10_000_000_000L;
        sequencer.sequenceMessage(ingress, 0, encodeIngressSnapshotPolicy(ingress, 0, 10), SESSION_ID, TIMESTAMP);
        sequencer.clusterHeartbeat(TIMESTAMP + interval - 1);
        assertNothingBehindHeartbeat("the interval has not passed");

        sequencer.clusterHeartbeat(TIMESTAMP + interval);
        assertEquals(1L, decodeSnapshotStarted(frames.last(), frames.lastLength()).round());

        final long requestedAt = TIMESTAMP + interval + interval / 2;
        sequencer.sequenceMessage(ingress, 0, encodeIngressSnapshotRequested(ingress, 0, 9), SESSION_ID, requestedAt);
        assertEquals(2L, decodeSnapshotStarted(frames.last(), frames.lastLength()).round());

        // A policy re-published after a round moves nothing: the interval still counts from round 2.
        sequencer.sequenceMessage(ingress, 0, encodeIngressSnapshotPolicy(ingress, 0, 10), SESSION_ID,
                                  requestedAt + 1);
        sequencer.clusterHeartbeat(TIMESTAMP + 2 * interval);
        assertNothingBehindHeartbeat("the interval counts from round 2");
        sequencer.clusterHeartbeat(requestedAt + interval);
        assertEquals(3L, decodeSnapshotStarted(frames.last(), frames.lastLength()).round());
        sequencer.clusterHeartbeat(requestedAt + interval);
        assertNothingBehindHeartbeat("and round 3 restarts it");
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

        assertTrue(sequencer.sequenceMessage(lifecycle, 0, encodeIngressConnectionOpened(lifecycle, 0), SESSION_ID,
                                             TIMESTAMP));
        final int connectedLength = frames.lastLength();
        assertEquals(SystemFrame.CONNECTION_OPENED, systemEventTypeOf(frames.last(), connectedLength));
        // The identity is the frame's, not the message's: a system payload carries no header of its own.
        org.limitless.seqeron.sbe.frame.SequencedSystemHeaderDecoder header = systemHeaderOf(frames.last());
        assertEquals(SOURCE_ID, header.sourceId());
        assertEquals(CONNECTION_ID, header.connectionId());
        assertEquals(SESSION_ID, header.sessionId());
        assertEquals(1L, header.clusterSeqNo());
        assertEquals(TIMESTAMP, header.timestamp());
        // An empty payload copies zero bytes through — the degenerate end of the copy-through path every
        // other message exercises with a payload.
        assertEquals(FRAME_OVERHEAD + ConnectionOpenedDecoder.BLOCK_LENGTH +
                     ConnectionOpenedDecoder.connectionDataHeaderLength(), connectedLength);

        assertTrue(sequencer.sequenceMessage(lifecycle, 0, encodeIngressConnectionClosed(lifecycle, 0), SESSION_ID,
                                             TIMESTAMP + 1));
        final int disconnectedLength = frames.lastLength();
        assertEquals(SystemFrame.CONNECTION_CLOSED, systemEventTypeOf(frames.last(), disconnectedLength));
        header = systemHeaderOf(frames.last());
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
        sequencer.clusterHeartbeat(heartbeatTime);
        final int length = frames.lastLength();

        final MessageHeaderDecoder framing = new MessageHeaderDecoder().wrap(frames.last(), 0);
        assertEquals(ClusterHeartbeatDecoder.TEMPLATE_ID, framing.templateId());
        final org.limitless.seqeron.sbe.frame.SequencedSystemHeaderDecoder header = systemHeaderOf(frames.last());
        assertEquals(SystemFrame.CLUSTER_HEARTBEAT, header.systemEventType());
        assertEquals(heartbeatTime, header.timestamp());
        assertEquals(Sequencer.NO_SOURCE_ID, header.sessionId());
        assertEquals(1L, header.clusterSeqNo());
        // The cheapest frame in the system: a framing header and the stamp, with no payload at all.
        assertEquals(MessageHeaderDecoder.ENCODED_LENGTH + ClusterHeartbeatDecoder.BLOCK_LENGTH, length);
        assertEquals(42, length);
    }

    // ── Standby promotion (GatewayActive) ─────────────────────────────────────

    @Test
    @DisplayName("the list's last row is followed by a bootstrap GatewayActive naming the rank-0 primary")
    void listsLastRowSynthesizesBootstrapActivation() {
        // Cold-start designation: one instance must open its gate and the standby must wait, so the
        // cluster names the primary's gatewayId behind the row that completes the list. The primary
        // is derived from those rows — the rank-0 one — not configured.
        final int primaryGatewayId = 5;
        final Sequencer seq = new Sequencer(0, frames);
        final MutableDirectBuffer buf = new ExpandableArrayBuffer(128);

        // gatewayId 5 (rank 0) is the primary, 6 (rank 1) the standby; remaining counts down to 0.
        seq.sequenceMessage(buf, 0, encodeIngressGatewayRegistered(buf, 0, primaryGatewayId, SOURCE_ID, "GW-A", 0, 1),
                            SESSION_ID, TIMESTAMP);
        assertEquals(1, frames.count(),
                     "an incomplete list designates nobody — remaining is the whole completeness edge");

        seq.sequenceMessage(buf, 0, encodeIngressGatewayRegistered(buf, 0, 6, SOURCE_ID, "GW-B", 1, 0), SESSION_ID,
                            TIMESTAMP + 1);
        assertEquals(3, frames.count(), "the last row, then the bootstrap behind it");
        assertEquals(2L, frameHeaderOf(frames.frame(1)).clusterSeqNo()); // 2 list rows

        assertEquals(primaryGatewayId, lastGatewayActive()); // the rank-0 gatewayId, derived from the log
        // clusterSeqNo, timestamp and provenance are the frame's; a core payload carries no header at all.
        assertEquals(3L, frameHeaderOf(frames.last()).clusterSeqNo()); // the next clusterSeqNo after the last row
        assertEquals(TIMESTAMP + 1, frameHeaderOf(frames.last()).timestamp()); // the last row's
        assertEquals(Sequencer.NO_SOURCE_ID, frameHeaderOf(frames.last()).sourceId()); // synthesized: no submitter

        // Fires once: re-running load-topology re-asserts the rows without re-designating the primary.
        seq.sequenceMessage(buf, 0, encodeIngressGatewayRegistered(buf, 0, 6, SOURCE_ID, "GW-B", 1, 0), SESSION_ID,
                            TIMESTAMP + 2);
        assertEquals(4, frames.count(), "the re-asserted row alone");
    }

    @Test
    @DisplayName("an operator's GatewayActivationRequested is forwarded and answered with a synthesized GatewayActive")
    void activationRequestIsForwardedAndAnswered() {
        // The operator's act is the fact, and the designation stays the cluster's: the request is
        // sequenced like any other submitted event, and the GatewayActive answering it comes out of the
        // same path bootstrap and both promotions take — so no SequencedSystem is left without an
        // UnsequencedSystem antecedent, and the manual path gets the list validation the other three
        // have from iterating the list.
        final Sequencer seq = new Sequencer(0, frames);
        final MutableDirectBuffer buf = new ExpandableArrayBuffer(512);
        seq.sequenceMessage(buf, 0, encodeIngressGatewayRegistered(buf, 0, 5, SOURCE_ID, "GW-A", 0, 1), SESSION_ID,
                            TIMESTAMP);
        seq.sequenceMessage(buf, 0, encodeIngressGatewayRegistered(buf, 0, 6, SOURCE_ID, "GW-B", 1, 0), SESSION_ID,
                            TIMESTAMP);
        assertEquals(5, lastGatewayActive()); // bootstrap designates 5
        assertEquals(3L, seq.clusterSeqNo());

        assertTrue(seq.sequenceMessage(buf, 0, encodeIngressActivationRequested(buf, 0, 6), SESSION_ID, TIMESTAMP + 1),
                   "the request is a submitted event and is forwarded");
        assertEquals(5, frames.count(), "the request, then its answer");
        assertEquals(SystemFrame.GATEWAY_ACTIVATION_REQUESTED, systemEventTypeOf(frames.frame(3), frames.length(3)));
        assertEquals(4L, frameHeaderOf(frames.frame(3)).clusterSeqNo());

        assertEquals(6, lastGatewayActive());
        assertEquals(5L, systemHeaderOf(frames.last()).clusterSeqNo(), "the answer is one frame behind the request");
        assertEquals(Sequencer.NO_SOURCE_ID, systemHeaderOf(frames.last()).sourceId(), "synthesized: no submitter");

        // And it arms the deadline like every other synthesized activation: instance 6 has to answer.
        seq.clusterHeartbeat(TIMESTAMP + 1 + Sequencer.GATEWAY_ACTIVATION_TIMEOUT_NS);
        assertEquals(5, lastGatewayActive());
    }

    @Test
    @DisplayName("only the cold-start designation marks bootstrap activated, not an operator's activation")
    void bootstrapActivationEmittedAnswersForTheBootstrapRunAlone() {
        // The gauge behind this is the "did the day open?" alarm, and the deployment it is read on is the
        // one whose list arrived truncated — which is also the deployment where an operator reaches for
        // clusterctl activate. That designation drains through this same list, so counting drains let the
        // manual repair answer the alarm that was there to say the repair is incomplete.
        final Sequencer seq = new Sequencer(0, frames);
        final MutableDirectBuffer buf = new ExpandableArrayBuffer(512);
        assertFalse(seq.bootstrapActivationEmitted());

        // remaining never reaches 0: the list is incomplete, so nothing bootstraps.
        seq.sequenceMessage(buf, 0, encodeIngressGatewayRegistered(buf, 0, 5, SOURCE_ID, "GW-A", 0, 1), SESSION_ID,
                            TIMESTAMP);
        assertEquals(1, frames.count());
        assertFalse(seq.bootstrapActivationEmitted());

        // The operator designates 5 by hand. A real GatewayActive comes out, and the day is still not open.
        seq.sequenceMessage(buf, 0, encodeIngressActivationRequested(buf, 0, 5), SESSION_ID, TIMESTAMP + 1);
        assertEquals(5, lastGatewayActive());
        assertFalse(seq.bootstrapActivationEmitted(), "an operator's activation is not the bootstrap");

        // The last row lands; now the cluster makes the cold-start designation itself.
        seq.sequenceMessage(buf, 0, encodeIngressGatewayRegistered(buf, 0, 6, SOURCE_ID, "GW-B", 1, 0), SESSION_ID,
                            TIMESTAMP + 3);
        assertEquals(5, lastGatewayActive());
        assertTrue(seq.bootstrapActivationEmitted());
    }

    @Test
    @DisplayName("a complete list that designates nobody does not mark bootstrap activated either")
    void bootstrapActivationEmittedStaysFalseWhenNothingIsDesignated() {
        // Fail closed: no rank-0 row, so the bootstrap run schedules nothing and no instance is ever
        // designated. The list did complete, which is why the gauge cannot be driven off that edge.
        final Sequencer seq = new Sequencer(0, frames);
        final MutableDirectBuffer buf = new ExpandableArrayBuffer(512);
        seq.sequenceMessage(buf, 0, encodeIngressGatewayRegistered(buf, 0, 5, SOURCE_ID, "GW-A", 1, 0), SESSION_ID,
                            TIMESTAMP);

        assertEquals(1, frames.count(), "the row alone");
        assertFalse(seq.bootstrapActivationEmitted());
    }

    @Test
    @DisplayName("a GatewayActivationRequested naming an instance no list row does is rejected")
    void activationRequestForAnUnknownInstanceIsRejected() {
        // The validation the manual path lacked while it published GatewayActive directly: an operator
        // typo used to put a designation nothing could answer into unreplayable history.
        final Sequencer seq = new Sequencer(0, frames);
        final MutableDirectBuffer buf = new ExpandableArrayBuffer(512);
        seq.sequenceMessage(buf, 0, encodeIngressGatewayRegistered(buf, 0, 5, SOURCE_ID, "GW-A", 0, 0), SESSION_ID,
                            TIMESTAMP);
        final long clusterSeqNo = seq.clusterSeqNo();

        assertFalse(seq.sequenceMessage(buf, 0, encodeIngressActivationRequested(buf, 0, 99), SESSION_ID,
                                        TIMESTAMP + 1));
        assertEquals(clusterSeqNo, seq.clusterSeqNo(),
                     "a rejected request consumes no sequence number, and designates nobody");
    }

    @Test
    @DisplayName("a complete list with no rank-0 row designates no primary and synthesizes no activation")
    void listWithNoPrimaryFailsClosed() {
        // The empty list this replaces stopped being expressible when the completeness edge became a
        // countdown: with no rows there is no remaining==0 to fire on, so the latch stays unset and a
        // later list still elects. clusterctl refuses to publish either list; the sequencer's own
        // answer to a rank-0-less list that reached it anyway is still to activate nobody.
        final Sequencer seq = new Sequencer(0, frames);
        final MutableDirectBuffer buf = new ExpandableArrayBuffer(128);
        seq.sequenceMessage(buf, 0, encodeIngressGatewayRegistered(buf, 0, 5, SOURCE_ID, "GW-A", 1, 1), SESSION_ID,
                            TIMESTAMP);
        seq.sequenceMessage(buf, 0, encodeIngressGatewayRegistered(buf, 0, 6, SOURCE_ID, "GW-B", 2, 0), SESSION_ID,
                            TIMESTAMP);
        assertEquals(2, frames.count(), "no rank-0 row ⇒ no bootstrap activation (fail closed)");
    }

    @Test
    @DisplayName("closing the active gateway's session promotes the next-ranked sibling, named by gatewayId")
    void closingGatewaySessionPromotesStandby() {
        final Sequencer seq = new Sequencer(0, frames);
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
        assertFalse(seq.sessionClosed(0xBEEFL, TIMESTAMP + 1));

        assertTrue(seq.sessionClosed(gatewaySession, TIMESTAMP + 2));
        assertEquals(6, promotedOnClose(gatewaySession));
        final int promotion = frames.count() - 2;
        final GatewayActiveDecoder decoded = decodeGatewayActive(frames.frame(promotion), frames.length(promotion));
        // The standby's gatewayId — never the gatewaySourceId. Both instances share the sourceId, so a
        // promotion carrying it designated both at once, and the consumer could only survive that by
        // latching its first match, which made a restarting instance re-activate off a superseded frame.
        assertEquals(6, decoded.gatewayId());
        assertNotEquals(SOURCE_ID, decoded.gatewayId());
        // 2 rows + bootstrap + GatewayStarted + the unclaimed session's SessionClosed + promotion
        assertEquals(6L, frameHeaderOf(frames.frame(promotion)).clusterSeqNo());
        assertEquals(TIMESTAMP + 2, frameHeaderOf(frames.frame(promotion)).timestamp());

        // The session is forgotten: a duplicate close does not re-promote.
        assertFalse(seq.sessionClosed(gatewaySession, TIMESTAMP + 3));
    }

    @Test
    @DisplayName("a restarted instance's stale session close does not demote the instance that replaced it")
    void staleGatewaySessionCloseDoesNotDemoteTheSuccessor() {
        // An instance that reconnects before the cluster times its old session out declared itself on the
        // new session while the dead one was still bound to the same gatewayId. The dead one's close then
        // read as "the active instance went away" and handed the role to the standby — while 5 was serving.
        final Sequencer seq = new Sequencer(0, frames);
        final MutableDirectBuffer buf = new ExpandableArrayBuffer(512);
        seq.sequenceMessage(buf, 0, encodeIngressGatewayRegistered(buf, 0, 5, SOURCE_ID, "GW-A", 0, 1), SESSION_ID,
                            TIMESTAMP);
        seq.sequenceMessage(buf, 0, encodeIngressGatewayRegistered(buf, 0, 6, SOURCE_ID, "GW-B", 1, 0), SESSION_ID,
                            TIMESTAMP);

        final long deadSession = 0xDEADL;
        final long liveSession = 0xA11CEL;
        seq.sequenceMessage(buf, 0, encodeIngressGatewayStarted(buf, 0, 5), deadSession, TIMESTAMP);
        seq.sequenceMessage(buf, 0, encodeIngressGatewayStarted(buf, 0, 5), liveSession, TIMESTAMP + 1);
        final long beforeClose = frameHeaderOf(frames.last()).clusterSeqNo();

        assertFalse(seq.sessionClosed(deadSession, TIMESTAMP + 2),
                    "the superseded session is not instance 5's session any more");

        // The live one still is, and its close is what promotes.
        assertTrue(seq.sessionClosed(liveSession, TIMESTAMP + 3));
        assertEquals(6, promotedOnClose(liveSession));
        assertEquals(beforeClose + 2, frameHeaderOf(frames.frame(frames.count() - 2)).clusterSeqNo(),
                     "the stale close published its SessionClosed and nothing else");
    }

    @Test
    @DisplayName("an instance superseded by an operator request promotes nothing when its session closes")
    void supersededInstanceCloseDoesNotDemoteTheActiveOne() {
        // An operator handover leaves the old instance's session bound. Its later close read as the active
        // instance going away, and with a third instance the role moved off the healthy one.
        final Sequencer seq = new Sequencer(0, frames);
        final MutableDirectBuffer buf = new ExpandableArrayBuffer(512);
        seq.sequenceMessage(buf, 0, encodeIngressGatewayRegistered(buf, 0, 5, SOURCE_ID, "GW-A", 0, 2), SESSION_ID,
                            TIMESTAMP);
        seq.sequenceMessage(buf, 0, encodeIngressGatewayRegistered(buf, 0, 6, SOURCE_ID, "GW-B", 1, 1), SESSION_ID,
                            TIMESTAMP);
        seq.sequenceMessage(buf, 0, encodeIngressGatewayRegistered(buf, 0, 7, SOURCE_ID, "GW-C", 2, 0), SESSION_ID,
                            TIMESTAMP);
        final long first = 0xA11CEL;
        final long third = 0xC0DEL;
        seq.sequenceMessage(buf, 0, encodeIngressGatewayStarted(buf, 0, 5), first, TIMESTAMP);
        seq.sequenceMessage(buf, 0, encodeIngressActivationRequested(buf, 0, 7), SESSION_ID, TIMESTAMP);
        seq.sequenceMessage(buf, 0, encodeIngressGatewayStarted(buf, 0, 7), third, TIMESTAMP);
        final long beforeClose = seq.clusterSeqNo();

        assertFalse(seq.sessionClosed(first, TIMESTAMP + 1), "5 was not designated, so its close hands nothing on");
        assertEquals(beforeClose + 1, seq.clusterSeqNo(), "only its SessionClosed");

        // 7's own close still promotes, to the lowest rank other than 7.
        assertTrue(seq.sessionClosed(third, TIMESTAMP + 2));
        assertEquals(5, promotedOnClose(third));
    }

    @Test
    @DisplayName("a session that merely echoes a gateway sourceId is not a gateway session")
    void echoingAGatewaySourceIdDoesNotMakeASessionAGateway() {
        // header.sourceId is a *routing* id, not a claim of identity: an application may stamp the
        // originating gateway's sourceId on a reply so that gateway can route it. Inferring "this session
        // is a gateway" from it would let that application's restart promote the standby out from under
        // a healthy primary. Only GatewayStarted may claim a session.
        final Sequencer seq = new Sequencer(0, frames);
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

        assertFalse(seq.sessionClosed(orderExecSession, TIMESTAMP + 2),
                    "echoing a gateway sourceId must not make a session promotable");
        assertTrue(seq.sessionClosed(gatewaySession, TIMESTAMP + 3),
                   "the session GatewayStarted claimed is still the one that promotes");
    }

    @Test
    @DisplayName("every session close is sequenced as SessionClosed, after the promotion it causes")
    void sessionCloseIsSequenced() {
        final Sequencer seq = new Sequencer(0, frames);
        final MutableDirectBuffer buf = new ExpandableArrayBuffer(512);
        final long session = 0xC0FFEEL;
        seq.sequenceMessage(buf, 0, encodeIngressPayload(buf, 0), session, TIMESTAMP);
        final int before = frames.count();

        assertFalse(seq.sessionClosed(session, TIMESTAMP + 1));
        assertEquals(before + 1, frames.count(), "one frame, though the session was no gateway's");
        assertEquals(session, decodeSessionClosed(frames.last(), frames.lastLength()).clusterSessionId());
        final org.limitless.seqeron.sbe.frame.SequencedSystemHeaderDecoder header = systemHeaderOf(frames.last());
        assertEquals(-1, header.sourceId(), "F-4: the cluster's own frame");
        assertEquals(-1, header.connectionId());
        assertEquals(-1L, header.sessionId(), "the closed session is the field's, not the header's");
        assertEquals(2L, header.clusterSeqNo());
        assertEquals(TIMESTAMP + 1, header.timestamp());
    }

    @Test
    @DisplayName("closing the only instance of a logical gateway promotes nothing")
    void soleGatewayInstanceHasNoStandbyToPromote() {
        // Fail closed: naming a nonexistent instance would activate no one while burning a clusterSeqNo,
        // and naming the sourceId (as this used to) would activate everyone.
        final Sequencer seq = new Sequencer(0, frames);
        final MutableDirectBuffer buf = new ExpandableArrayBuffer(512);
        seq.sequenceMessage(buf, 0, encodeIngressGatewayRegistered(buf, 0, 5, SOURCE_ID, "GW-A", 0, 0), SESSION_ID,
                            TIMESTAMP);

        final long gatewaySession = 0xA11CEL;
        seq.sequenceMessage(buf, 0, encodeIngressGatewayStarted(buf, 0, 5), gatewaySession, TIMESTAMP);

        assertFalse(seq.sessionClosed(gatewaySession, TIMESTAMP + 1));
        assertEquals(1L, seq.gatewayPromotionFailedCount(),
                     "counted, unlike a session that was never a gateway's: this one was, with no standby");
        assertEquals(4L, seq.clusterSeqNo(), "a promotion with no target consumes no sequence number, the close's own "
                                                + "SessionClosed one");
    }

    // ── Activation deadline (a designated instance that never declares itself started) ────────────

    @Test
    @DisplayName("a designated primary that never declares itself started is handed over to the standby")
    void designatedPrimaryThatNeverStartsIsHandedOver() {
        // The gap this closes: only GatewayStarted registers an instance,
        // and it is published at gate-open, AFTER catch-up. An instance that dies or wedges in cold start
        // was never registered, so no session close could ever promote past it — the cluster kept a
        // designated primary that was never going to serve, with nothing to say so.
        final Sequencer seq = new Sequencer(0, frames);
        final MutableDirectBuffer buf = new ExpandableArrayBuffer(512);
        seq.clusterHeartbeat(TIMESTAMP);
        assertNothingBehindHeartbeat("nothing designated ⇒ no deadline to miss");

        seq.sequenceMessage(buf, 0, encodeIngressGatewayRegistered(buf, 0, 5, SOURCE_ID, "GW-A", 0, 1), SESSION_ID,
                            TIMESTAMP);
        seq.sequenceMessage(buf, 0, encodeIngressGatewayRegistered(buf, 0, 6, SOURCE_ID, "GW-B", 1, 0), SESSION_ID,
                            TIMESTAMP);
        assertEquals(5, lastGatewayActive());

        final long deadline = TIMESTAMP + Sequencer.GATEWAY_ACTIVATION_TIMEOUT_NS;
        seq.clusterHeartbeat(deadline - 1);
        assertNothingBehindHeartbeat("a cold start still inside the deadline is not overdue");

        seq.clusterHeartbeat(deadline);
        assertEquals(6, lastGatewayActive());
        assertEquals(1L, seq.gatewayPromotionCount());
        assertEquals(7L, seq.clusterSeqNo()); // 3 heartbeats + 2 list rows + bootstrap + this
    }

    @Test
    @DisplayName("a designated primary that did declare itself started is left alone")
    void designatedPrimaryThatStartedIsNotHandedOver() {
        final Sequencer seq = new Sequencer(0, frames);
        final MutableDirectBuffer buf = new ExpandableArrayBuffer(512);
        seq.sequenceMessage(buf, 0, encodeIngressGatewayRegistered(buf, 0, 5, SOURCE_ID, "GW-A", 0, 1), SESSION_ID,
                            TIMESTAMP);
        seq.sequenceMessage(buf, 0, encodeIngressGatewayRegistered(buf, 0, 6, SOURCE_ID, "GW-B", 1, 0), SESSION_ID,
                            TIMESTAMP);

        final long gatewaySession = 0xA11CEL;
        seq.sequenceMessage(buf, 0, encodeIngressGatewayStarted(buf, 0, 5), gatewaySession, TIMESTAMP + 1);

        final long deadline = TIMESTAMP + Sequencer.GATEWAY_ACTIVATION_TIMEOUT_NS;
        seq.clusterHeartbeat(deadline);
        assertNothingBehindHeartbeat("an answered activation synthesizes nothing");
        // Disarmed, not merely quiet: a healthy primary must not be demoted one deadline later.
        seq.clusterHeartbeat(deadline + 10 * Sequencer.GATEWAY_ACTIVATION_TIMEOUT_NS);
        assertNothingBehindHeartbeat("an answered activation is disarmed");
    }

    @Test
    @DisplayName("the hand-over arms the same deadline on the instance it names")
    void handoverArmsTheDeadlineOnTheInstanceItNames() {
        // A whole gateway tier that is down must converge on whichever instance comes up first, not on
        // whichever the cluster happened to designate before they all went away.
        final Sequencer seq = new Sequencer(0, frames);
        final MutableDirectBuffer buf = new ExpandableArrayBuffer(512);
        seq.sequenceMessage(buf, 0, encodeIngressGatewayRegistered(buf, 0, 5, SOURCE_ID, "GW-A", 0, 1), SESSION_ID,
                            TIMESTAMP);
        seq.sequenceMessage(buf, 0, encodeIngressGatewayRegistered(buf, 0, 6, SOURCE_ID, "GW-B", 1, 0), SESSION_ID,
                            TIMESTAMP);

        final long first = TIMESTAMP + Sequencer.GATEWAY_ACTIVATION_TIMEOUT_NS;
        seq.clusterHeartbeat(first);
        assertEquals(6, lastGatewayActive());

        seq.clusterHeartbeat(first + 1);
        assertNothingBehindHeartbeat("the hand-over restarts the deadline rather than firing again immediately");

        seq.clusterHeartbeat(first + Sequencer.GATEWAY_ACTIVATION_TIMEOUT_NS);
        assertEquals(5, lastGatewayActive(),
                     "the standby did not start either — the role goes back rather than stopping here");
    }

    @Test
    @DisplayName("an unanswered activation with no sibling to hand over to fails closed")
    void unansweredActivationWithNoSiblingFailsClosed() {
        final Sequencer seq = new Sequencer(0, frames);
        final MutableDirectBuffer buf = new ExpandableArrayBuffer(512);
        seq.sequenceMessage(buf, 0, encodeIngressGatewayRegistered(buf, 0, 5, SOURCE_ID, "GW-A", 0, 0), SESSION_ID,
                            TIMESTAMP);

        final long deadline = TIMESTAMP + Sequencer.GATEWAY_ACTIVATION_TIMEOUT_NS;
        seq.clusterHeartbeat(deadline);
        assertNothingBehindHeartbeat("an activation was missed, and there is nobody to hand it to");
        assertEquals(1L, seq.gatewayPromotionFailedCount(), "which is counted");
        seq.clusterHeartbeat(deadline + Sequencer.GATEWAY_ACTIVATION_TIMEOUT_NS);
        assertEquals(1L, seq.gatewayPromotionFailedCount(),
                     "reported once — a sole instance has nothing to re-designate to");
    }

    @Test
    @DisplayName("a promotion on session close arms the deadline too")
    void promotionOnSessionCloseArmsTheDeadline() {
        final Sequencer seq = new Sequencer(0, frames);
        final MutableDirectBuffer buf = new ExpandableArrayBuffer(512);
        seq.sequenceMessage(buf, 0, encodeIngressGatewayRegistered(buf, 0, 5, SOURCE_ID, "GW-A", 0, 1), SESSION_ID,
                            TIMESTAMP);
        seq.sequenceMessage(buf, 0, encodeIngressGatewayRegistered(buf, 0, 6, SOURCE_ID, "GW-B", 1, 0), SESSION_ID,
                            TIMESTAMP);

        final long gatewaySession = 0xA11CEL;
        seq.sequenceMessage(buf, 0, encodeIngressGatewayStarted(buf, 0, 5), gatewaySession, TIMESTAMP);
        assertTrue(seq.sessionClosed(gatewaySession, TIMESTAMP)); // promotes 6

        // The standby it promoted has to declare itself started like any other designated instance —
        // otherwise a crash of the primary while the standby is also down leaves the same dead end.
        seq.clusterHeartbeat(TIMESTAMP + Sequencer.GATEWAY_ACTIVATION_TIMEOUT_NS);
        assertEquals(5, lastGatewayActive());
    }

    @Test
    @DisplayName("a GatewayStarted naming an instance no list row does is rejected")
    void unknownGatewayInstancePromotesNothing() {
        final Sequencer seq = new Sequencer(0, frames);
        final MutableDirectBuffer buf = new ExpandableArrayBuffer(512);
        seq.sequenceMessage(buf, 0, encodeIngressGatewayRegistered(buf, 0, 5, SOURCE_ID, "GW-A", 0, 1), SESSION_ID,
                            TIMESTAMP);
        seq.sequenceMessage(buf, 0, encodeIngressGatewayRegistered(buf, 0, 6, SOURCE_ID, "GW-B", 1, 0), SESSION_ID,
                            TIMESTAMP);

        final long rogueSession = 0xC0FFEEL;
        assertFalse(seq.sequenceMessage(buf, 0, encodeIngressGatewayStarted(buf, 0, 99), rogueSession, TIMESTAMP),
                    "S-6 case 1 is total: a gatewayId the list does not name is rejected whatever it carries");

        assertFalse(seq.sessionClosed(rogueSession, TIMESTAMP + 1),
                    "the rejected frame bound nothing, so the close is not an active gateway's");
    }

    // ── Two logical gateways ──────────────────────────────────────────────────
    // A deployment may run more than one pair, each an active/standby pair under its own
    // gatewaySourceId. Everything below is a case where the sequencer used to hold one of something it
    // needs one of per pair. SOURCE_ID is the first pair; SECOND_SOURCE_ID the second.

    @Test
    @DisplayName("the bootstrap activates the rank-0 primary of every logical gateway, not just one")
    void bootstrapActivatesEveryLogicalGatewaysPrimary() {
        final Sequencer seq = new Sequencer(0, frames);
        final MutableDirectBuffer buf = new ExpandableArrayBuffer(512);
        loadTwoPairs(seq, buf);

        assertEquals(6, frames.count(), "4 list rows, then one activation per logical gateway");
        assertEquals(4L, frameHeaderOf(frames.frame(3)).clusterSeqNo());

        // In list order, on consecutive clusterSeqNos behind the last row.
        assertEquals(5, gatewayActiveAt(4));
        assertEquals(5L, frameHeaderOf(frames.frame(4)).clusterSeqNo());
        // Not the client pair's standby: a second rank-0 row used to overwrite the first designation, so
        // whichever logical gateway loaded last took the only bootstrap and the other never got one.
        assertEquals(8, gatewayActiveAt(5));
        assertEquals(6L, frameHeaderOf(frames.frame(5)).clusterSeqNo());
    }

    @Test
    @DisplayName("losing one logical gateway's active instance promotes only its own sibling")
    void logicalGatewaysPromoteIndependently() {
        final Sequencer seq = new Sequencer(0, frames);
        final MutableDirectBuffer buf = new ExpandableArrayBuffer(512);
        loadTwoPairs(seq, buf);

        final long clientSession = 0xA11CEL;
        final long secondSession = 0xB0B0L;
        seq.sequenceMessage(buf, 0, encodeIngressGatewayStarted(buf, 0, 5), clientSession, TIMESTAMP);
        seq.sequenceMessage(buf, 0, encodeIngressGatewayStarted(buf, 0, 8, SECOND_SOURCE_ID), secondSession,
                            TIMESTAMP);

        assertTrue(seq.sessionClosed(secondSession, TIMESTAMP + 1));
        assertEquals(9, promotedOnClose(secondSession),
                     "the second pair's standby — promotionTarget filters on the closed instance's own "
                         + "gatewaySourceId");

        // The client pair is untouched: its active instance never lost anything.
        assertTrue(seq.sessionClosed(clientSession, TIMESTAMP + 2));
        assertEquals(6, promotedOnClose(clientSession));
    }

    @Test
    @DisplayName("each logical gateway keeps its own activation deadline")
    void eachLogicalGatewayKeepsItsOwnActivationDeadline() {
        // The regression this guards: one outstanding activation for the whole cluster meant the second
        // bootstrap overwrote the first's deadline, so a designated primary that never arrived was never
        // handed over — silently, and only for whichever pair happened to be designated first.
        final Sequencer seq = new Sequencer(0, frames);
        final MutableDirectBuffer buf = new ExpandableArrayBuffer(512);
        loadTwoPairs(seq, buf); // designates 5 and 8

        final long deadline = TIMESTAMP + Sequencer.GATEWAY_ACTIVATION_TIMEOUT_NS;
        seq.clusterHeartbeat(deadline - 1);
        assertNothingBehindHeartbeat("neither is due yet");

        // Both come due on the same heartbeat, and both are handed over behind it.
        seq.clusterHeartbeat(deadline);
        assertEquals(6, gatewayActiveAt(frames.count() - 2));
        assertEquals(9, lastGatewayActive());
        seq.clusterHeartbeat(deadline);
        assertNothingBehindHeartbeat("both hand-overs restarted their own deadline");
    }

    @Test
    @DisplayName("one logical gateway answering its activation does not excuse the other")
    void answeringOneActivationLeavesTheOtherOnTheClock() {
        final Sequencer seq = new Sequencer(0, frames);
        final MutableDirectBuffer buf = new ExpandableArrayBuffer(512);
        loadTwoPairs(seq, buf); // designates 5 and 8

        // Only the client pair's primary comes up.
        seq.sequenceMessage(buf, 0, encodeIngressGatewayStarted(buf, 0, 5), 0xA11CEL, TIMESTAMP + 1);

        final long deadline = TIMESTAMP + Sequencer.GATEWAY_ACTIVATION_TIMEOUT_NS;
        seq.clusterHeartbeat(deadline);
        assertEquals(9, lastGatewayActive(),
                     "the second pair is still overdue, and is walked past the answered entry to reach it");
        final int heartbeat = frames.count() - 2;
        assertEquals(SystemFrame.CLUSTER_HEARTBEAT, systemHeaderOf(frames.frame(heartbeat)).systemEventType(),
                     "and the client pair is not handed over");
        seq.clusterHeartbeat(deadline);
        assertNothingBehindHeartbeat("the healthy pair is disarmed, not merely quiet");
    }

    /**
     * Two active/standby pairs: 5/6 under SOURCE_ID, 8/9 under SECOND_SOURCE_ID, in list order.
     * One complete list run — remaining counts down to 0 on the last row, which the bootstrap follows.
     */
    private static void loadTwoPairs(final Sequencer seq, final MutableDirectBuffer buf) {
        seq.sequenceMessage(buf, 0, encodeIngressGatewayRegistered(buf, 0, 5, SOURCE_ID, "GW-A", 0, 3), SESSION_ID,
                            TIMESTAMP);
        seq.sequenceMessage(buf, 0, encodeIngressGatewayRegistered(buf, 0, 6, SOURCE_ID, "GW-B", 1, 2), SESSION_ID,
                            TIMESTAMP);
        seq.sequenceMessage(buf, 0, encodeIngressGatewayRegistered(buf, 0, 8, SECOND_SOURCE_ID, "GW2-A", 0, 1),
                            SESSION_ID, TIMESTAMP);
        seq.sequenceMessage(buf, 0, encodeIngressGatewayRegistered(buf, 0, 9, SECOND_SOURCE_ID, "GW2-B", 1, 0),
                            SESSION_ID, TIMESTAMP);
    }

    // ── Allocation ────────────────────────────────────────────────────────────

    @Test
    @DisplayName("once compiled, sequencing a frame allocates nothing, on a gateway's session or any other")
    void sequencingAllocatesNothingOnceCompiled() {
        final Sequencer seq = new Sequencer(0, (buffer, length) -> { });
        final MutableDirectBuffer buf = new ExpandableArrayBuffer(512);
        seq.sequenceMessage(buf, 0, encodeIngressGatewayRegistered(buf, 0, 5, SOURCE_ID, "GW-A", 0, 0), SESSION_ID,
                            TIMESTAMP);
        final long gatewaySession = 0xA11CEL; // above 127, as SESSION_ID is: the Long cache holds neither
        seq.sequenceMessage(buf, 0, encodeIngressGatewayStarted(buf, 0, 5), gatewaySession, TIMESTAMP);
        final int ingressLength = encodeIngressPayload(ingress, 0);
        final long firstClusterSeqNo = seq.clusterSeqNo();

        // The interpreter allocates what C2's escape analysis removes, so the first batch to be fully compiled
        // is the one that counts; one that never stops allocating fails.
        final ThreadMXBean threads = (ThreadMXBean)ManagementFactory.getThreadMXBean();
        final int frames = 100_000;
        long allocated = -1;
        int batches = 0;
        for (; batches < 50 && allocated != 0; batches++) {
            final long before = threads.getCurrentThreadAllocatedBytes();
            for (int i = 0; i < frames; i++) {
                seq.sequenceMessage(ingress, 0, ingressLength, gatewaySession, TIMESTAMP);
                seq.sequenceMessage(ingress, 0, ingressLength, SESSION_ID, TIMESTAMP);
            }
            allocated = threads.getCurrentThreadAllocatedBytes() - before;
        }

        assertEquals(firstClusterSeqNo + 2L * frames * batches, seq.clusterSeqNo()); // none rejected
        assertEquals(0, allocated);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /**
     * Encodes a frame carrying {@link #OPAQUE_PAYLOAD} under a payloadId the sequencer does not own —
     * the standing exemplar of ordinary application traffic. Deliberately not a real message in a real
     * application schema: the sequencer reads nothing past the payloadId, so a payload with a decodable
     * layout would only invite a test to assert something the production path cannot see.
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
        final org.limitless.seqeron.sbe.frame.GatewayRegisteredEncoder encoder = new org.limitless.seqeron.sbe.frame.GatewayRegisteredEncoder();
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

    /** As above, for a named connection. */
    private static int encodeIngressConnectionOpened(final MutableDirectBuffer buffer, final int offset,
                                                    final int connectionId) {
        final MutableDirectBuffer payload = new ExpandableArrayBuffer(64);
        final org.limitless.seqeron.sbe.frame.ConnectionOpenedEncoder encoder = new org.limitless.seqeron.sbe.frame.ConnectionOpenedEncoder();
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
        final org.limitless.seqeron.sbe.frame.ConnectionClosedEncoder encoder = new org.limitless.seqeron.sbe.frame.ConnectionClosedEncoder();
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
        final org.limitless.seqeron.sbe.frame.GatewayStartedEncoder encoder = new org.limitless.seqeron.sbe.frame.GatewayStartedEncoder();
        encoder.wrap(payload, 0);
        encoder.gatewayId(gatewayId).firstConnectionId(1);
        return encodeIngressSystemFrame(buffer, offset, sourceId, -1, SystemFrame.GATEWAY_STARTED, payload,
                                        encoder.encodedLength());
    }

    /** Encodes a GatewayActivationRequested, as {@code clusterctl activate} submits it. */
    private static int encodeIngressActivationRequested(final MutableDirectBuffer buffer, final int offset,
                                                        final int gatewayId) {
        final MutableDirectBuffer body = new ExpandableArrayBuffer(64);
        final org.limitless.seqeron.sbe.frame.GatewayActivationRequestedEncoder encoder =
            new org.limitless.seqeron.sbe.frame.GatewayActivationRequestedEncoder();
        encoder.wrap(body, 0);
        encoder.gatewayId(gatewayId);
        // clusterctl's own reserved sourceId (§5), which no list row claims.
        return encodeIngressSystemFrame(buffer, offset, 99, -1, SystemFrame.GATEWAY_ACTIVATION_REQUESTED, body,
                                        encoder.encodedLength());
    }

    /** Encodes a SnapshotPolicyRegistered, as {@code clusterctl load-topology} submits it. */
    private static int encodeIngressSnapshotPolicy(final MutableDirectBuffer buffer, final int offset,
                                                   final int intervalSeconds) {
        final MutableDirectBuffer body = new ExpandableArrayBuffer(16);
        final org.limitless.seqeron.sbe.frame.SnapshotPolicyRegisteredEncoder encoder =
            new org.limitless.seqeron.sbe.frame.SnapshotPolicyRegisteredEncoder();
        encoder.wrap(body, 0).intervalSeconds(intervalSeconds);
        return encodeIngressSystemFrame(buffer, offset, 99, -1, SystemFrame.SNAPSHOT_POLICY_REGISTERED, body,
                                        encoder.encodedLength());
    }

    /** Encodes a SnapshotRequested, as {@code clusterctl request-snapshot} submits it. */
    private static int encodeIngressSnapshotRequested(final MutableDirectBuffer buffer, final int offset,
                                                      final long correlationId) {
        final MutableDirectBuffer body = new ExpandableArrayBuffer(16);
        final org.limitless.seqeron.sbe.frame.SnapshotRequestedEncoder encoder =
            new org.limitless.seqeron.sbe.frame.SnapshotRequestedEncoder();
        encoder.wrap(body, 0).correlationId(correlationId);
        return encodeIngressSystemFrame(buffer, offset, 99, -1, SystemFrame.SNAPSHOT_REQUESTED, body,
                                        encoder.encodedLength());
    }

    private static SnapshotStartedDecoder decodeSnapshotStarted(final MutableDirectBuffer buffer, final int length) {
        return decodeSynthesized(buffer, length, SnapshotStartedDecoder.TEMPLATE_ID, SystemFrame.SNAPSHOT_STARTED,
                                 SnapshotStartedDecoder.BLOCK_LENGTH, new SnapshotStartedDecoder());
    }

    private static SessionClosedDecoder decodeSessionClosed(final MutableDirectBuffer buffer, final int length) {
        return decodeSynthesized(buffer, length, SessionClosedDecoder.TEMPLATE_ID, SystemFrame.SESSION_CLOSED,
                                 SessionClosedDecoder.BLOCK_LENGTH, new SessionClosedDecoder());
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
        assertEquals(expected.length, declared, "the queue frame's payload prefix");
        assertEquals(FRAME_OVERHEAD + expected.length, length, "the queue frame's total length");

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
     * Decodes one of the five the sequencer synthesizes. Each has a template of its own and carries its
     * fields inline, so there is no payload and no length prefix: the frame is its framing header plus its
     * block, and the decoder is wrapped over that block with its own compiled constants (<b>V-3</b>).
     */
    private static <T> T decodeSynthesized(final MutableDirectBuffer buffer, final int length, final int templateId,
                                           final int systemEventType, final int blockLength, final T decoder) {
        final org.limitless.seqeron.sbe.frame.MessageHeaderDecoder header =
            new org.limitless.seqeron.sbe.frame.MessageHeaderDecoder().wrap(buffer, 0);
        assertEquals(org.limitless.seqeron.sbe.frame.MessageHeaderDecoder.SCHEMA_ID, header.schemaId());
        assertEquals(templateId, header.templateId());
        assertEquals(blockLength, header.blockLength());
        assertEquals(org.limitless.seqeron.sbe.frame.MessageHeaderDecoder.ENCODED_LENGTH + blockLength, length);
        assertEquals(systemEventType, systemHeaderOf(buffer).systemEventType(),
                     "offset 16 discriminates every frame on the queue, synthesized ones included");
        final int block = org.limitless.seqeron.sbe.frame.MessageHeaderDecoder.ENCODED_LENGTH;
        if (decoder instanceof GatewayActiveDecoder d) {
            d.wrap(buffer, block, blockLength, header.version());
        } else if (decoder instanceof SnapshotStartedDecoder d) {
            d.wrap(buffer, block, blockLength, header.version());
        } else if (decoder instanceof SessionClosedDecoder d) {
            d.wrap(buffer, block, blockLength, header.version());
        } else {
            ((LeadershipChangedDecoder)decoder).wrap(buffer, block, blockLength, header.version());
        }
        return decoder;
    }

    /** Reads clusterSeqNo off the frame published last, after checking the call sequenced one. */
    private long clusterSeqNoOf(final boolean sequenced) {
        assertTrue(sequenced);
        return frameHeaderOf(frames.last()).clusterSeqNo();
    }

    /** The {@code gatewayId} a session's close promoted, after checking the close ended on its {@code SessionClosed}. */
    private int promotedOnClose(final long sessionId) {
        assertEquals(sessionId, decodeSessionClosed(frames.last(), frames.lastLength()).clusterSessionId());
        return gatewayActiveAt(frames.count() - 2);
    }

    /** The {@code gatewayId} of the {@code GatewayActive} published last. */
    private int lastGatewayActive() {
        return gatewayActiveAt(frames.count() - 1);
    }

    /** The {@code gatewayId} of the {@code GatewayActive} at {@code index}. */
    private int gatewayActiveAt(final int index) {
        return decodeGatewayActive(frames.frame(index), frames.length(index)).gatewayId();
    }

    /** Asserts the frame published last is a heartbeat: nothing ran off it. */
    private void assertNothingBehindHeartbeat(final String message) {
        assertEquals(SystemFrame.CLUSTER_HEARTBEAT, systemHeaderOf(frames.last()).systemEventType(), message);
    }

    /** The frame's own header — the identity every message relies on, since it carries none itself. */
    private static org.limitless.seqeron.sbe.frame.SequencedHeaderDecoder frameHeaderOf(final MutableDirectBuffer buffer) {
        return new org.limitless.seqeron.sbe.frame.SequencedHeaderDecoder().wrap(buffer, org.limitless.seqeron.sbe.frame.MessageHeaderDecoder.ENCODED_LENGTH);
    }

    /**
     * The same header read under the system family's names. The two composites are byte-for-byte
     * identical apart from the name of the uint16 at offset 16, which is the property every field below
     * that offset being at a fixed place depends on (<b>F-3</b>).
     */
    private static org.limitless.seqeron.sbe.frame.SequencedSystemHeaderDecoder systemHeaderOf(
        final MutableDirectBuffer buffer) {
        return new org.limitless.seqeron.sbe.frame.SequencedSystemHeaderDecoder()
            .wrap(buffer, org.limitless.seqeron.sbe.frame.MessageHeaderDecoder.ENCODED_LENGTH);
    }

    /**
     * Wraps an already-encoded system payload in an {@code UnsequencedSystem} frame, as every system producer
     * does. The payload carries no {@code MessageHeader} of its own — {@code header.systemEventType} names it.
     * {@code sessionId} is advisory on ingress — the sequencer overwrites it — so it goes out as -1.
     */
    private static int encodeIngressSystemFrame(final MutableDirectBuffer buffer, final int offset,
                                                final int sourceId, final int connectionId,
                                                final int systemEventType, final MutableDirectBuffer body,
                                                final int bodyLength) {
        final org.limitless.seqeron.sbe.frame.UnsequencedSystemEncoder encoder =
            new org.limitless.seqeron.sbe.frame.UnsequencedSystemEncoder();
        encoder.wrapAndApplyHeader(buffer, offset, new org.limitless.seqeron.sbe.frame.MessageHeaderEncoder());
        encoder.header().sourceId(sourceId).connectionId(connectionId).sessionId(-1)
            .systemEventType(systemEventType);
        encoder.putBody(body, 0, bodyLength);
        return org.limitless.seqeron.sbe.frame.MessageHeaderEncoder.ENCODED_LENGTH + encoder.encodedLength();
    }

    /** The same, for a payload the sequencer does not own. */
    private static int encodeIngressPayloadFrame(final MutableDirectBuffer buffer, final int offset,
                                                 final int sourceId, final int connectionId, final int payloadId,
                                                 final MutableDirectBuffer payload, final int payloadLength) {
        final org.limitless.seqeron.sbe.frame.UnsequencedEncoder encoder = new org.limitless.seqeron.sbe.frame.UnsequencedEncoder();
        encoder.wrapAndApplyHeader(buffer, offset, new org.limitless.seqeron.sbe.frame.MessageHeaderEncoder());
        encoder.header().sourceId(sourceId).connectionId(connectionId).sessionId(-1).payloadId(payloadId);
        encoder.putPayload(payload, 0, payloadLength);
        return org.limitless.seqeron.sbe.frame.MessageHeaderEncoder.ENCODED_LENGTH + encoder.encodedLength();
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
     * Offset of the payload inside a {@code SequencedSystem} frame, checking the frame's structure on the way —
     * that it is one, and that the length it declares accounts for every byte the sequencer returned.
     */
    private static int systemBodyOffset(final MutableDirectBuffer buffer, final int length) {
        final org.limitless.seqeron.sbe.frame.MessageHeaderDecoder header = new org.limitless.seqeron.sbe.frame.MessageHeaderDecoder().wrap(buffer, 0);
        assertEquals(org.limitless.seqeron.sbe.frame.SequencedSystemDecoder.SCHEMA_ID, header.schemaId());
        assertEquals(org.limitless.seqeron.sbe.frame.SequencedSystemDecoder.TEMPLATE_ID, header.templateId());
        final int prefix = org.limitless.seqeron.sbe.frame.MessageHeaderDecoder.ENCODED_LENGTH +
                           org.limitless.seqeron.sbe.frame.SequencedSystemHeaderDecoder.ENCODED_LENGTH;
        final int bodyLength = buffer.getShort(prefix, java.nio.ByteOrder.LITTLE_ENDIAN) & 0xFFFF;
        final int body = prefix + org.limitless.seqeron.sbe.frame.SequencedSystemDecoder.bodyHeaderLength();
        assertEquals(length, body + bodyLength);
        return body;
    }
}
