package org.limitless.phixeron.sequencer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.limitless.phixeron.sbe.sequenced.ClientConnectedDecoder;
import org.limitless.phixeron.sbe.sequenced.ClientDisconnectedDecoder;
import org.limitless.phixeron.sbe.sequenced.GatewayActiveDecoder;
import org.limitless.phixeron.sbe.sequenced.HeaderDecoder;
import org.limitless.phixeron.sbe.sequenced.LeadershipChangedDecoder;
import org.limitless.phixeron.sbe.sequenced.LogoutDecoder;
import org.limitless.phixeron.sbe.sequenced.MessageHeaderDecoder;
import org.limitless.phixeron.sbe.sequenced.NewOrderSingleDecoder;
import org.limitless.phixeron.sbe.sequenced.Origin;
import org.limitless.phixeron.sbe.sequenced.TickDecoder;

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
 * every event type, and the schema-200 → schema-202 copy-through preserves every byte past the
 * header.
 */
class SequencerTest {
    private static final int SOURCE_ID = 7;
    private static final int CONNECTION_ID = 42;
    private static final long SESSION_ID = 0x5EE51_0000L;
    private static final long TIMESTAMP = 1_700_000_000_000L;

    /** Ingress header composite is 16 bytes, sequenced is 32 — the delta every egress frame grows by. */
    private static final int HEADER_GROWTH =
        org.limitless.phixeron.sbe.sequenced.HeaderEncoder.ENCODED_LENGTH
        - org.limitless.phixeron.sbe.unsequenced.HeaderEncoder.ENCODED_LENGTH;

    private final Sequencer sequencer = new Sequencer();
    private final MutableDirectBuffer ingress = new ExpandableArrayBuffer(512);

    // ── Copy-through fidelity ─────────────────────────────────────────────────

    @Test
    @DisplayName("sequencing an ingress message preserves every field past the header composite")
    void sequenceMessagePreservesEveryFieldPastTheHeader() {
        final int ingressLength = encodeIngressNewOrderSingle(ingress, 0);

        final int length = sequencer.sequenceMessage(ingress, 0, ingressLength, SESSION_ID, TIMESTAMP);

        final NewOrderSingleDecoder decoded = decodeNewOrderSingle(sequencer.buffer(), length);
        // Every business field survives the opaque byte copy unread and unmodified.
        assertEquals("CLIENT", decoded.sender().trim());
        assertEquals("PHIXERON", decoded.target().trim());
        assertEquals(4321L, decoded.seqNum());
        assertEquals(1_699_999_999_000L, decoded.sendingTimeMs());
        assertEquals("ACCT01", decoded.account().trim());
        assertEquals("CLORD-000000000001", decoded.clOrdID().trim());
        assertEquals("AAPL", decoded.symbol().trim());
        assertEquals("Sell", decoded.side().name());
        assertEquals("Limit", decoded.ordType().name());
        assertEquals("Manual", decoded.handlInst().name());
        assertEquals("FillOrKill", decoded.timeInForce().name());
        assertEquals("Yes", decoded.possDupFlag().name());
        assertEquals(1_699_999_999_500L, decoded.transactTime());
        assertEquals(2500L, decoded.orderQty());
        assertEquals(19_950_000L, decoded.price());
        assertEquals("partial fill expected", decoded.text().trim());
    }

    @Test
    @DisplayName("sequencing stamps the header without disturbing the submitter's identity")
    void sequenceMessageStampsHeader() {
        final int ingressLength = encodeIngressNewOrderSingle(ingress, 0);

        final int length = sequencer.sequenceMessage(ingress, 0, ingressLength, SESSION_ID, TIMESTAMP);

        final HeaderDecoder header = decodeNewOrderSingle(sequencer.buffer(), length).header();
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
        // NewOrderSingle is all fixed block; Logout carries var-data, so it exercises the copyLength
        // arithmetic past the end of the fixed block as well.
        final String reason = "counterparty requested disconnect";
        final int ingressLength = encodeIngressLogout(ingress, 0, reason);

        final int length = sequencer.sequenceMessage(ingress, 0, ingressLength, SESSION_ID, TIMESTAMP);

        final MessageHeaderDecoder messageHeader = new MessageHeaderDecoder().wrap(sequencer.buffer(), 0);
        final LogoutDecoder decoded = new LogoutDecoder().wrap(sequencer.buffer(),
                                                               MessageHeaderDecoder.ENCODED_LENGTH,
                                                               messageHeader.blockLength(),
                                                               messageHeader.version());
        assertEquals("CLIENT", decoded.sender().trim());
        assertEquals(99L, decoded.seqNum());
        assertEquals(reason, decoded.text());
        // The whole ingress frame, minus nothing, plus the two int64s the sequenced header adds.
        assertEquals(ingressLength + HEADER_GROWTH, length);
    }

    @Test
    @DisplayName("egress blockLength grows by exactly the header composite delta")
    void egressBlockLengthGrowsByHeaderDelta() {
        final int ingressLength = encodeIngressNewOrderSingle(ingress, 0);
        final int ingressBlockLength =
            new org.limitless.phixeron.sbe.unsequenced.MessageHeaderDecoder().wrap(ingress, 0).blockLength();

        sequencer.sequenceMessage(ingress, 0, ingressLength, SESSION_ID, TIMESTAMP);

        final MessageHeaderDecoder egress = new MessageHeaderDecoder().wrap(sequencer.buffer(), 0);
        assertEquals(ingressBlockLength + HEADER_GROWTH, egress.blockLength());
        assertEquals(NewOrderSingleDecoder.TEMPLATE_ID, egress.templateId());
        assertEquals(NewOrderSingleDecoder.SCHEMA_ID, egress.schemaId());
    }

    @Test
    @DisplayName("sequencing reads the ingress message at a non-zero offset")
    void sequenceMessageHonoursOffset() {
        // Aeron hands fragments at an arbitrary offset into a shared term buffer, never 0.
        final int offset = 96;
        final int ingressLength = encodeIngressNewOrderSingle(ingress, offset);

        final int length = sequencer.sequenceMessage(ingress, offset, ingressLength, SESSION_ID, TIMESTAMP);

        final NewOrderSingleDecoder decoded = decodeNewOrderSingle(sequencer.buffer(), length);
        assertEquals("AAPL", decoded.symbol().trim());
        assertEquals(CONNECTION_ID, decoded.header().connectionId());
    }

    // ── globalSeqNo ───────────────────────────────────────────────────────────

    @Test
    @DisplayName("globalSeqNo is gap-free and monotone across every event type")
    void globalSeqNoIsGapFreeAcrossEveryEventType() {
        // Forwarded ingress messages, clock ticks and elections all draw from the one counter; a
        // consumer that sees a gap treats it as lost data and re-walks history, so this must hold for
        // every emitting path. TCP lifecycle events are ordinary forwarded ingress — the gateway
        // publishes them — so they go through sequenceMessage here, not a synthesized encoder.
        final int ingressLength = encodeIngressNewOrderSingle(ingress, 0);
        final MutableDirectBuffer lifecycle = new ExpandableArrayBuffer(64);

        final int connectedLength = encodeIngressClientConnected(lifecycle, 0);
        assertEquals(1L, globalSeqNoOf(sequencer.sequenceMessage(lifecycle, 0, connectedLength, SESSION_ID, TIMESTAMP)));
        assertEquals(2L, globalSeqNoOf(sequencer.sequenceMessage(ingress, 0, ingressLength, SESSION_ID, TIMESTAMP)));
        assertEquals(3L, globalSeqNoOf(sequencer.tick(TIMESTAMP + 1000)));
        assertEquals(4L, globalSeqNoOf(sequencer.leadershipChanged(2, TIMESTAMP + 1500)));
        assertEquals(5L, globalSeqNoOf(sequencer.sequenceMessage(ingress, 0, ingressLength, SESSION_ID, TIMESTAMP)));

        final int disconnectedLength = encodeIngressClientDisconnected(lifecycle, 0);
        assertEquals(6L, globalSeqNoOf(
            sequencer.sequenceMessage(lifecycle, 0, disconnectedLength, SESSION_ID, TIMESTAMP + 2000)));
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
        assertEquals(TIMESTAMP + 200, decoded.header().timestamp());
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
        final int ingressLength = encodeIngressNewOrderSingle(ingress, 0);

        assertEquals(Sequencer.NO_FRAME,
                     sequencer.sequenceMessage(ingress, 0, Sequencer.MIN_INGRESS_LENGTH - 1, SESSION_ID, TIMESTAMP));
        assertEquals(0L, sequencer.globalSeqNo(), "a skipped message must consume no sequence number");

        // The next good message is still globalSeqNo 1: consumers detect loss by gaps, so a skip has to
        // leave the numbering contiguous rather than burn a number on a frame nobody will ever receive.
        assertEquals(1L, globalSeqNoOf(sequencer.sequenceMessage(ingress, 0, ingressLength, SESSION_ID, TIMESTAMP)));
    }

    @Test
    @DisplayName("a frame from a foreign schema is skipped")
    void foreignSchemaIsSkipped() {
        // Every offset sequenceMessage reads is a schema-200 offset; under another schema they address
        // something else entirely, so the frame is refused rather than re-stamped as if it were ours.
        final int ingressLength = encodeIngressNewOrderSingle(ingress, 0);
        new org.limitless.phixeron.sbe.unsequenced.MessageHeaderEncoder().wrap(ingress, 0).schemaId(999);

        assertEquals(Sequencer.NO_FRAME, sequencer.sequenceMessage(ingress, 0, ingressLength, SESSION_ID, TIMESTAMP));
        assertEquals(0L, sequencer.globalSeqNo());
    }

    @Test
    @DisplayName("a blockLength the frame cannot back is skipped")
    void oversizedBlockLengthIsSkipped() {
        // blockLength feeds both the egress blockLength and the bounded Gateway decode, so a value
        // larger than the frame carries is rejected before either uses it.
        final int ingressLength = encodeIngressNewOrderSingle(ingress, 0);
        new org.limitless.phixeron.sbe.unsequenced.MessageHeaderEncoder().wrap(ingress, 0).blockLength(ingressLength);

        assertEquals(Sequencer.NO_FRAME, sequencer.sequenceMessage(ingress, 0, ingressLength, SESSION_ID, TIMESTAMP));
        assertEquals(0L, sequencer.globalSeqNo());
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
        final int messageLength = encodeIngressNewOrderSingle(message, 0);
        final MutableDirectBuffer lifecycle = new ExpandableArrayBuffer(64);
        final int connectedLength = encodeIngressClientConnected(lifecycle, 0);
        final java.util.List<byte[]> frames = new java.util.ArrayList<>();

        collect(frames, target, target.sequenceMessage(lifecycle, 0, connectedLength, SESSION_ID, TIMESTAMP));
        collect(frames, target, target.leadershipChanged(0, TIMESTAMP + 1));
        for (int i = 0; i < 5; i++) {
            collect(frames, target, target.sequenceMessage(message, 0, messageLength, SESSION_ID, TIMESTAMP + i));
            collect(frames, target, target.tick(TIMESTAMP + 1000L * i));
        }
        collect(frames, target, target.leadershipChanged(0, TIMESTAMP + 9));  // suppressed
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
        assertThrows(UnsupportedOperationException.class, () -> new SequencerService().onTakeSnapshot(null));
    }

    // ── Lifecycle and clock frames ────────────────────────────────────────────

    @Test
    @DisplayName("TCP lifecycle frames carry the connection they describe")
    void tcpLifecycleFramesCarryTheConnectionTheyDescribe() {
        // These name an external event — a FIX client's TCP connection opening and closing — so unlike
        // a tick or an election they have a real gateway process and connection behind them, and both
        // ids must survive sequencing. Without them a consumer can see that *a* session ended but not
        // which, which is the whole reason the gateway publishes these rather than the sequencer
        // synthesizing a cluster-session event under the same template ids.
        final MutableDirectBuffer lifecycle = new ExpandableArrayBuffer(64);

        final int connectedLength =
            sequencer.sequenceMessage(lifecycle, 0, encodeIngressClientConnected(lifecycle, 0), SESSION_ID, TIMESTAMP);
        final MessageHeaderDecoder messageHeader = new MessageHeaderDecoder().wrap(sequencer.buffer(), 0);
        assertEquals(ClientConnectedDecoder.TEMPLATE_ID, messageHeader.templateId());
        final ClientConnectedDecoder connectedDecoder = new ClientConnectedDecoder()
            .wrap(sequencer.buffer(), MessageHeaderDecoder.ENCODED_LENGTH, messageHeader.blockLength(),
                  messageHeader.version());
        HeaderDecoder header = connectedDecoder.header();
        // `origin` sits past the header composite, so it rides through the opaque byte copy
        // untouched — the sequencer never decodes it and must not disturb it.
        assertEquals(Origin.Gateway, connectedDecoder.origin());
        assertEquals(SOURCE_ID, header.sourceId());
        assertEquals(CONNECTION_ID, header.connectionId());
        assertEquals(SESSION_ID, header.sessionId());
        assertEquals(1L, header.globalSeqNo());
        assertEquals(TIMESTAMP, header.timestamp());
        // A header-only message copies zero body bytes through — the degenerate end of the
        // copy-through path every other message exercises with a body.
        assertEquals(connectedLength, MessageHeaderDecoder.ENCODED_LENGTH + ClientConnectedDecoder.BLOCK_LENGTH);

        final int disconnectedLength = sequencer.sequenceMessage(
            lifecycle, 0, encodeIngressClientDisconnected(lifecycle, 0), SESSION_ID, TIMESTAMP + 1);
        final MessageHeaderDecoder disconnectHeader = new MessageHeaderDecoder().wrap(sequencer.buffer(), 0);
        assertEquals(ClientDisconnectedDecoder.TEMPLATE_ID, disconnectHeader.templateId());
        header = new ClientDisconnectedDecoder()
            .wrap(sequencer.buffer(), MessageHeaderDecoder.ENCODED_LENGTH, disconnectHeader.blockLength(),
                  disconnectHeader.version())
            .header();
        assertEquals(CONNECTION_ID, header.connectionId());
        assertEquals(TIMESTAMP + 1, header.timestamp());
        assertEquals(SESSION_ID, header.sessionId());
        assertEquals(disconnectedLength, MessageHeaderDecoder.ENCODED_LENGTH + ClientDisconnectedDecoder.BLOCK_LENGTH);
    }

    @Test
    @DisplayName("tick carries the consensus timestamp and no session")
    void tickCarriesConsensusTimestamp() {
        // The gateway's keepalive watchdog reads exactly this field to advance its session clock while
        // a counterparty is silent, so a tick that lost its timestamp would stall every watchdog.
        final long tickTime = TIMESTAMP + 60_000;
        final int length = sequencer.tick(tickTime);

        final MessageHeaderDecoder messageHeader = new MessageHeaderDecoder().wrap(sequencer.buffer(), 0);
        assertEquals(TickDecoder.TEMPLATE_ID, messageHeader.templateId());
        final HeaderDecoder header = new TickDecoder()
            .wrap(sequencer.buffer(), MessageHeaderDecoder.ENCODED_LENGTH, messageHeader.blockLength(),
                  messageHeader.version())
            .header();
        assertEquals(tickTime, header.timestamp());
        assertEquals(Sequencer.NO_SOURCE_ID, header.sessionId());
        assertEquals(1L, header.globalSeqNo());
        assertEquals(length, MessageHeaderDecoder.ENCODED_LENGTH + TickDecoder.BLOCK_LENGTH);
    }

    // ── Standby promotion (GatewayActive) ─────────────────────────────────────

    @Test
    @DisplayName("the first EndBasicData is followed by a bootstrap GatewayActive naming the rank-0 primary")
    void firstEndBasicDataSynthesizesBootstrapActivation() {
        // Cold-start designation: one instance must open its gate and the standby must wait, so the
        // cluster names the primary's gatewayId behind the load-complete marker (doc/todo.md item 18).
        // The primary is derived from the Gateway rows in the load — the rank-0 row — not configured.
        final int primaryGatewayId = 5;
        final Sequencer seq = new Sequencer();
        final MutableDirectBuffer buf = new ExpandableArrayBuffer(128);

        assertEquals(Sequencer.NO_FRAME, seq.pendingBootstrapActivation(TIMESTAMP), "nothing pending before EndBasicData");

        // The load carries the topology: gatewayId 5 (rank 0) is the primary, 6 (rank 1) the standby.
        seq.sequenceMessage(buf, 0, encodeIngressGateway(buf, 0, primaryGatewayId, SOURCE_ID, "GW-A", 0), SESSION_ID,
                            TIMESTAMP);
        seq.sequenceMessage(buf, 0, encodeIngressGateway(buf, 0, 6, SOURCE_ID, "GW-B", 1), SESSION_ID, TIMESTAMP);

        final int endLength = seq.sequenceMessage(buf, 0, encodeIngressEndBasicData(buf, 0), SESSION_ID, TIMESTAMP);
        assertEquals(3L, globalSeqNoOf(seq, endLength));  // 2 Gateway rows + EndBasicData

        final int activationLength = seq.pendingBootstrapActivation(TIMESTAMP + 1);
        assertNotEquals(Sequencer.NO_FRAME, activationLength);
        final GatewayActiveDecoder decoded = decodeGatewayActive(seq.buffer(), activationLength);
        assertEquals(primaryGatewayId, decoded.gatewayId());       // the rank-0 gatewayId, derived from the log
        assertEquals(4L, decoded.header().globalSeqNo());          // takes the next globalSeqNo after EndBasicData
        assertEquals(TIMESTAMP + 1, decoded.header().timestamp());
        assertEquals(Sequencer.NO_SOURCE_ID, decoded.header().sourceId());  // synthesized: no submitter

        // Fires once: a re-emitted load (a leader change mid-load) does not re-designate the primary.
        seq.sequenceMessage(buf, 0, encodeIngressEndBasicData(buf, 0), SESSION_ID, TIMESTAMP + 2);
        assertEquals(Sequencer.NO_FRAME, seq.pendingBootstrapActivation(TIMESTAMP + 3));
    }

    @Test
    @DisplayName("EndBasicData with no Gateway row designates no primary and synthesizes no activation")
    void endBasicDataWithoutGatewayRowFailsClosed() {
        final Sequencer seq = new Sequencer();
        final MutableDirectBuffer buf = new ExpandableArrayBuffer(64);
        seq.sequenceMessage(buf, 0, encodeIngressEndBasicData(buf, 0), SESSION_ID, TIMESTAMP);
        assertEquals(Sequencer.NO_FRAME, seq.pendingBootstrapActivation(TIMESTAMP + 1),
                     "no Gateway topology ⇒ no bootstrap activation (fail closed)");
    }

    @Test
    @DisplayName("closing a gateway session promotes the standby with a GatewayActive on the gateway sourceId")
    void closingGatewaySessionPromotesStandby() {
        final Sequencer seq = new Sequencer();
        final MutableDirectBuffer buf = new ExpandableArrayBuffer(512);

        // The Gateway row makes SOURCE_ID a known gateway sourceId (rank-0 primary is gatewayId 5).
        seq.sequenceMessage(buf, 0, encodeIngressGateway(buf, 0, 5, SOURCE_ID, "GW-A", 0), SESSION_ID, TIMESTAMP);

        // A session that then publishes under that sourceId is a gateway session (NewOrderSingle carries
        // SOURCE_ID as its header.sourceId).
        final long gatewaySession = 0xA11CEL;
        seq.sequenceMessage(buf, 0, encodeIngressNewOrderSingle(buf, 0), gatewaySession, TIMESTAMP);

        // A session that never published under a gateway sourceId is not — its close promotes nothing.
        assertEquals(Sequencer.NO_FRAME, seq.sessionClosed(0xBEEFL, TIMESTAMP + 1));

        final int promotionLength = seq.sessionClosed(gatewaySession, TIMESTAMP + 2);
        assertNotEquals(Sequencer.NO_FRAME, promotionLength);
        final GatewayActiveDecoder decoded = decodeGatewayActive(seq.buffer(), promotionLength);
        assertEquals(SOURCE_ID, decoded.gatewayId());              // promotion carries that session's gateway sourceId
        assertEquals(3L, decoded.header().globalSeqNo());          // Gateway row + NewOrderSingle + promotion
        assertEquals(TIMESTAMP + 2, decoded.header().timestamp());

        // The session is forgotten: a duplicate close does not re-promote.
        assertEquals(Sequencer.NO_FRAME, seq.sessionClosed(gatewaySession, TIMESTAMP + 3));
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /** Encodes a fully-populated schema-200 NewOrderSingle, as the FIX gateway would submit it. */
    private static int encodeIngressNewOrderSingle(final MutableDirectBuffer buffer, final int offset) {
        final org.limitless.phixeron.sbe.unsequenced.MessageHeaderEncoder messageHeader =
            new org.limitless.phixeron.sbe.unsequenced.MessageHeaderEncoder();
        final org.limitless.phixeron.sbe.unsequenced.NewOrderSingleEncoder encoder =
            new org.limitless.phixeron.sbe.unsequenced.NewOrderSingleEncoder();

        encoder.wrapAndApplyHeader(buffer, offset, messageHeader);
        encoder.header().sourceId(SOURCE_ID).connectionId(CONNECTION_ID).sessionId(-1);
        encoder.sender("CLIENT")
            .target("PHIXERON")
            .seqNum(4321L)
            .sendingTimeMs(1_699_999_999_000L)
            .possDupFlag(org.limitless.phixeron.sbe.unsequenced.PossDupFlag.Yes)
            .account("ACCT01")
            .clOrdID("CLORD-000000000001")
            .handlInst(org.limitless.phixeron.sbe.unsequenced.HandlInst.Manual)
            .symbol("AAPL")
            .side(org.limitless.phixeron.sbe.unsequenced.Side.Sell)
            .transactTime(1_699_999_999_500L)
            .orderQty(2500L)
            .ordType(org.limitless.phixeron.sbe.unsequenced.OrdType.Limit)
            .price(19_950_000L)
            .timeInForce(org.limitless.phixeron.sbe.unsequenced.TimeInForce.FillOrKill)
            .text("partial fill expected")
            .tradeDate(1_699_920_000_000L)
            .maturityTime(1_700_086_400_000L);

        return org.limitless.phixeron.sbe.unsequenced.MessageHeaderEncoder.ENCODED_LENGTH + encoder.encodedLength();
    }

    /** Encodes a schema-200 Gateway topology row, as the BasicDataClient producer would submit it. */
    private static int encodeIngressGateway(final MutableDirectBuffer buffer, final int offset, final int gatewayId,
                                            final int gatewaySourceId, final String gatewayName,
                                            final int preferenceRank) {
        final org.limitless.phixeron.sbe.unsequenced.MessageHeaderEncoder messageHeader =
            new org.limitless.phixeron.sbe.unsequenced.MessageHeaderEncoder();
        final org.limitless.phixeron.sbe.unsequenced.GatewayEncoder encoder =
            new org.limitless.phixeron.sbe.unsequenced.GatewayEncoder();

        encoder.wrapAndApplyHeader(buffer, offset, messageHeader);
        // The producer's own sourceId (not a gateway's), outside the gateway-sourceId set.
        encoder.header().sourceId(99).connectionId(-1).sessionId(-1);
        encoder.progress().remainingSections(0).remainingItems(0);
        encoder.gatewayId(gatewayId)
            .gatewaySourceId(gatewaySourceId)
            .gatewayName(gatewayName)
            .preferenceRank((short) preferenceRank);
        return org.limitless.phixeron.sbe.unsequenced.MessageHeaderEncoder.ENCODED_LENGTH + encoder.encodedLength();
    }

    /** Encodes a schema-200 Logout, whose trailing {@code text} is var-data rather than fixed block. */
    private static int encodeIngressLogout(final MutableDirectBuffer buffer, final int offset, final String reason) {
        final org.limitless.phixeron.sbe.unsequenced.MessageHeaderEncoder messageHeader =
            new org.limitless.phixeron.sbe.unsequenced.MessageHeaderEncoder();
        final org.limitless.phixeron.sbe.unsequenced.LogoutEncoder encoder =
            new org.limitless.phixeron.sbe.unsequenced.LogoutEncoder();

        encoder.wrapAndApplyHeader(buffer, offset, messageHeader);
        encoder.header().sourceId(SOURCE_ID).connectionId(CONNECTION_ID).sessionId(-1);
        encoder.sender("CLIENT")
            .target("PHIXERON")
            .seqNum(99L)
            .sendingTimeMs(1_699_999_999_000L)
            .origin(org.limitless.phixeron.sbe.unsequenced.Origin.Client)
            .possDupFlag(org.limitless.phixeron.sbe.unsequenced.PossDupFlag.No)
            .text(reason);

        return org.limitless.phixeron.sbe.unsequenced.MessageHeaderEncoder.ENCODED_LENGTH + encoder.encodedLength();
    }

    /**
     * Encodes a schema-200 ClientConnected, as the FIX gateway submits it when it accepts a TCP
     * connection. Header-only: the connection it describes is entirely in {@code header}.
     */
    private static int encodeIngressClientConnected(final MutableDirectBuffer buffer, final int offset) {
        final org.limitless.phixeron.sbe.unsequenced.MessageHeaderEncoder messageHeader =
            new org.limitless.phixeron.sbe.unsequenced.MessageHeaderEncoder();
        final org.limitless.phixeron.sbe.unsequenced.ClientConnectedEncoder encoder =
            new org.limitless.phixeron.sbe.unsequenced.ClientConnectedEncoder();

        encoder.wrapAndApplyHeader(buffer, offset, messageHeader);
        encoder.header().sourceId(SOURCE_ID).connectionId(CONNECTION_ID).sessionId(-1);
        encoder.origin(org.limitless.phixeron.sbe.unsequenced.Origin.Gateway);

        return org.limitless.phixeron.sbe.unsequenced.MessageHeaderEncoder.ENCODED_LENGTH + encoder.encodedLength();
    }

    /** Encodes a schema-200 ClientDisconnected; the mirror of {@link #encodeIngressClientConnected}. */
    private static int encodeIngressClientDisconnected(final MutableDirectBuffer buffer, final int offset) {
        final org.limitless.phixeron.sbe.unsequenced.MessageHeaderEncoder messageHeader =
            new org.limitless.phixeron.sbe.unsequenced.MessageHeaderEncoder();
        final org.limitless.phixeron.sbe.unsequenced.ClientDisconnectedEncoder encoder =
            new org.limitless.phixeron.sbe.unsequenced.ClientDisconnectedEncoder();

        encoder.wrapAndApplyHeader(buffer, offset, messageHeader);
        encoder.header().sourceId(SOURCE_ID).connectionId(CONNECTION_ID).sessionId(-1);
        encoder.origin(org.limitless.phixeron.sbe.unsequenced.Origin.Gateway);

        return org.limitless.phixeron.sbe.unsequenced.MessageHeaderEncoder.ENCODED_LENGTH + encoder.encodedLength();
    }

    /** Encodes a schema-200 EndBasicData (header-only), as the BasicDataClient submits it to close a load. */
    private static int encodeIngressEndBasicData(final MutableDirectBuffer buffer, final int offset) {
        final org.limitless.phixeron.sbe.unsequenced.MessageHeaderEncoder messageHeader =
            new org.limitless.phixeron.sbe.unsequenced.MessageHeaderEncoder();
        final org.limitless.phixeron.sbe.unsequenced.EndBasicDataEncoder encoder =
            new org.limitless.phixeron.sbe.unsequenced.EndBasicDataEncoder();

        encoder.wrapAndApplyHeader(buffer, offset, messageHeader);
        encoder.header().sourceId(3).connectionId(-1).sessionId(-1);  // the BasicDataClient's sourceId

        return org.limitless.phixeron.sbe.unsequenced.MessageHeaderEncoder.ENCODED_LENGTH + encoder.encodedLength();
    }

    private static GatewayActiveDecoder decodeGatewayActive(final MutableDirectBuffer buffer, final int length) {
        final MessageHeaderDecoder messageHeader = new MessageHeaderDecoder().wrap(buffer, 0);
        assertEquals(GatewayActiveDecoder.TEMPLATE_ID, messageHeader.templateId());
        assertEquals(length, MessageHeaderDecoder.ENCODED_LENGTH + messageHeader.blockLength());
        return new GatewayActiveDecoder().wrap(buffer, MessageHeaderDecoder.ENCODED_LENGTH,
                                               messageHeader.blockLength(), messageHeader.version());
    }

    private static NewOrderSingleDecoder decodeNewOrderSingle(final MutableDirectBuffer buffer, final int length) {
        final MessageHeaderDecoder messageHeader = new MessageHeaderDecoder().wrap(buffer, 0);
        assertEquals(NewOrderSingleDecoder.TEMPLATE_ID, messageHeader.templateId());
        assertEquals(length, MessageHeaderDecoder.ENCODED_LENGTH + messageHeader.blockLength());
        return new NewOrderSingleDecoder().wrap(buffer, MessageHeaderDecoder.ENCODED_LENGTH,
                                                messageHeader.blockLength(), messageHeader.version());
    }

    private static LeadershipChangedDecoder decodeLeadershipChanged(final MutableDirectBuffer buffer,
                                                                    final int length) {
        final MessageHeaderDecoder messageHeader = new MessageHeaderDecoder().wrap(buffer, 0);
        assertEquals(LeadershipChangedDecoder.TEMPLATE_ID, messageHeader.templateId());
        return new LeadershipChangedDecoder().wrap(buffer, MessageHeaderDecoder.ENCODED_LENGTH,
                                                   messageHeader.blockLength(), messageHeader.version());
    }

    private long globalSeqNoOf(final int length) {
        return globalSeqNoOf(sequencer, length);
    }

    /** Reads globalSeqNo off whatever frame was just encoded — the header sits at a fixed offset. */
    private static long globalSeqNoOf(final Sequencer target, final int length) {
        assertNotEquals(Sequencer.NO_FRAME, length);
        return new HeaderDecoder().wrap(target.buffer(), MessageHeaderDecoder.ENCODED_LENGTH).globalSeqNo();
    }
}
