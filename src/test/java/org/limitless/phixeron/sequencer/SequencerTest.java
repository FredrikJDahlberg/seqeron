package org.limitless.phixeron.sequencer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.limitless.phixeron.sbe.sequenced.ClientConnectedDecoder;
import org.limitless.phixeron.sbe.sequenced.ClientDisconnectedDecoder;
import org.limitless.phixeron.sbe.sequenced.HeaderDecoder;
import org.limitless.phixeron.sbe.sequenced.LeadershipChangedDecoder;
import org.limitless.phixeron.sbe.sequenced.LogoutDecoder;
import org.limitless.phixeron.sbe.sequenced.MessageHeaderDecoder;
import org.limitless.phixeron.sbe.sequenced.NewOrderSingleDecoder;
import org.limitless.phixeron.sbe.sequenced.TickDecoder;

/**
 * Unit tests for the sequencer's replicated state machine.
 *
 * <p>These deliberately touch no Aeron runtime — no media driver, no cluster, no Aeron mocks. {@link
 * Sequencer} is a pure function of its inputs, so the whole state machine is exercised by calling it
 * and decoding the frames it writes, which is what makes these tests fast and stable enough to run
 * on every build. Everything Aeron-shaped ({@link SequencerService}'s tap publication, archive
 * recording, timer scheduling, snapshot I/O) stays covered by the end-to-end scripts under
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
        // Lifecycle events, ingress messages, clock ticks and elections all draw from the one counter;
        // a consumer that sees a gap treats it as lost data and re-walks history, so this must hold for
        // every emitting path.
        final int ingressLength = encodeIngressNewOrderSingle(ingress, 0);

        assertEquals(1L, globalSeqNoOf(sequencer.clientConnected(SESSION_ID, TIMESTAMP)));
        assertEquals(2L, globalSeqNoOf(sequencer.sequenceMessage(ingress, 0, ingressLength, SESSION_ID, TIMESTAMP)));
        assertEquals(3L, globalSeqNoOf(sequencer.tick(TIMESTAMP + 1000)));
        assertEquals(4L, globalSeqNoOf(sequencer.leadershipChanged(2, TIMESTAMP + 1500)));
        assertEquals(5L, globalSeqNoOf(sequencer.sequenceMessage(ingress, 0, ingressLength, SESSION_ID, TIMESTAMP)));
        assertEquals(6L, globalSeqNoOf(sequencer.clientDisconnected(SESSION_ID, TIMESTAMP + 2000)));
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
        final java.util.List<byte[]> frames = new java.util.ArrayList<>();

        collect(frames, target, target.clientConnected(SESSION_ID, TIMESTAMP));
        collect(frames, target, target.leadershipChanged(0, TIMESTAMP + 1));
        for (int i = 0; i < 5; i++) {
            collect(frames, target, target.sequenceMessage(message, 0, messageLength, SESSION_ID, TIMESTAMP + i));
            collect(frames, target, target.tick(TIMESTAMP + 1000L * i));
        }
        collect(frames, target, target.leadershipChanged(0, TIMESTAMP + 9));  // suppressed
        collect(frames, target, target.leadershipChanged(1, TIMESTAMP + 10));
        collect(frames, target, target.clientDisconnected(SESSION_ID, TIMESTAMP + 11));
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
    @DisplayName("restoring a snapshot continues the sequence where it left off")
    void snapshotRestoreContinuesTheSequence() {
        final int ingressLength = encodeIngressNewOrderSingle(ingress, 0);
        for (int i = 0; i < 17; i++) {
            sequencer.sequenceMessage(ingress, 0, ingressLength, SESSION_ID, TIMESTAMP);
        }
        final long snapshotted = sequencer.globalSeqNo();
        assertEquals(17L, snapshotted);

        // What SequencerService.loadSnapshot does with the int64 it reads off the snapshot image.
        final Sequencer restored = new Sequencer();
        restored.globalSeqNo(snapshotted);

        final int length = restored.sequenceMessage(ingress, 0, ingressLength, SESSION_ID, TIMESTAMP);
        assertEquals(18L, globalSeqNoOf(restored, length));
    }

    // ── Lifecycle and clock frames ────────────────────────────────────────────

    @Test
    @DisplayName("lifecycle and tick frames carry no submitter identity")
    void lifecycleFramesCarryNoSubmitterIdentity() {
        // These are synthesized by the sequencer itself, so there is no gateway process or TCP
        // connection behind them — consumers rely on the sentinel to tell them apart from forwarded
        // ingress traffic.
        final int connectedLength = sequencer.clientConnected(SESSION_ID, TIMESTAMP);
        final MessageHeaderDecoder messageHeader = new MessageHeaderDecoder().wrap(sequencer.buffer(), 0);
        assertEquals(ClientConnectedDecoder.TEMPLATE_ID, messageHeader.templateId());
        HeaderDecoder header = new ClientConnectedDecoder()
            .wrap(sequencer.buffer(), MessageHeaderDecoder.ENCODED_LENGTH, messageHeader.blockLength(),
                  messageHeader.version())
            .header();
        assertEquals(Sequencer.NO_SOURCE_ID, header.sourceId());
        assertEquals(Sequencer.NO_SOURCE_ID, header.connectionId());
        assertEquals(SESSION_ID, header.sessionId());
        assertEquals(1L, header.globalSeqNo());
        assertEquals(TIMESTAMP, header.timestamp());
        assertEquals(connectedLength, MessageHeaderDecoder.ENCODED_LENGTH + ClientConnectedDecoder.BLOCK_LENGTH);

        final int disconnectedLength = sequencer.clientDisconnected(SESSION_ID, TIMESTAMP + 1);
        final MessageHeaderDecoder disconnectHeader = new MessageHeaderDecoder().wrap(sequencer.buffer(), 0);
        assertEquals(ClientDisconnectedDecoder.TEMPLATE_ID, disconnectHeader.templateId());
        header = new ClientDisconnectedDecoder()
            .wrap(sequencer.buffer(), MessageHeaderDecoder.ENCODED_LENGTH, disconnectHeader.blockLength(),
                  disconnectHeader.version())
            .header();
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
            .possDupFlag(org.limitless.phixeron.sbe.unsequenced.PossDupFlag.No)
            .text(reason);

        return org.limitless.phixeron.sbe.unsequenced.MessageHeaderEncoder.ENCODED_LENGTH + encoder.encodedLength();
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
