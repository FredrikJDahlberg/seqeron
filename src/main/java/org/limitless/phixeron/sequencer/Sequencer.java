package org.limitless.phixeron.sequencer;

import org.agrona.DirectBuffer;
import org.agrona.ExpandableDirectByteBuffer;
import org.agrona.MutableDirectBuffer;
import org.limitless.phixeron.sbe.sequenced.GatewayActiveEncoder;
import org.limitless.phixeron.sbe.sequenced.HeaderEncoder;
import org.limitless.phixeron.sbe.sequenced.LeadershipChangedEncoder;
import org.limitless.phixeron.sbe.sequenced.MessageHeaderEncoder;
import org.limitless.phixeron.sbe.sequenced.TickEncoder;
import org.limitless.phixeron.sbe.unsequenced.EndBasicDataDecoder;
import org.limitless.phixeron.sbe.unsequenced.GatewayDecoder;
import org.limitless.phixeron.sbe.unsequenced.HeaderDecoder;
import org.limitless.phixeron.sbe.unsequenced.MessageHeaderDecoder;

/**
 * The sequencer's replicated state machine, free of every Aeron type.
 *
 * <p>Owns the entire replicated state ({@code globalSeqNo}, plus the leader id kept only to
 * de-duplicate leadership events) and all frame encoding. Each {@code sequence*}/event method
 * assigns the next {@code globalSeqNo}, encodes one {@code sbe-sequenced.xml} (schema 202) frame
 * into {@link #buffer()} starting at offset 0, and returns its length — or {@code 0} when the event
 * produces no frame. The caller publishes {@code buffer()[0, length)} and does nothing else: every
 * decision that must be identical on every node lives here.
 *
 * <p>That split is what makes the state machine testable without a cluster, a media driver, or any
 * Aeron mock — {@link SequencerService} is the thin adapter that owns the tap publication, the
 * archive, and timer scheduling, and it is the only part that needs a live cluster to exercise.
 *
 * <p><b>The copy-through trick.</b> Ingress messages arrive already SBE-encoded as {@code
 * sbe-unsequenced.xml} (schema 200) — the FIX gateway encodes every admin and application FIX
 * message that way and offers it directly to the cluster, with {@code header.sourceId} identifying
 * the submitting gateway <em>process</em>, {@code header.connectionId} the specific TCP connection
 * at that gateway, and {@code header.sessionId} the Aeron Cluster session. {@link
 * #sequenceMessage} does not need to know about individual FIX message types to re-stamp them:
 * {@code sbe-sequenced.xml} is deliberately kept byte-identical to {@code sbe-unsequenced.xml} past
 * the {@code header} composite (same field order/types/ids, same var-data layout), so it decodes
 * only the outer {@code MessageHeader} and the {@code header} composite (both always at a fixed
 * offset, regardless of {@code templateId}), then copies every remaining byte — the rest of the
 * fixed block plus all var-data — verbatim into a new schema-202 message whose {@code header}
 * carries the original {@code sourceId}/{@code connectionId}/{@code sessionId} plus the new {@code
 * globalSeqNo}/{@code timestamp}.
 *
 * <p><b>Determinism.</b> Every method is a pure function of its arguments and the current state —
 * no clock reads, no randomness, no I/O — so replaying the same call sequence on any node produces
 * byte-identical frames and the same final state. The consensus {@code timestamp} is always passed
 * in by the caller, never read here.
 *
 * <p>Single-threaded by contract: driven only from the cluster's conductor thread, so the shared
 * encoders and buffer need no synchronisation.
 */
public final class Sequencer {
    /**
     * header.sourceId/connectionId for events synthesized by the sequencer itself (Tick /
     * LeadershipChanged): a clock tick or an election has no gateway-process or TCP-level
     * connection id to carry, unlike the ingress messages it forwards.
     *
     * <p>ClientConnected/ClientDisconnected are deliberately not in that list. They denote a FIX
     * client's TCP session opening and closing — external events the gateway observes and publishes
     * on ingress like any other message, carrying the real sourceId/connectionId of the connection
     * they describe. The sequencer used to synthesize them for Aeron <em>cluster</em> sessions
     * instead, which named a different thing entirely and left the actual TCP lifecycle absent from
     * the log; nothing consumed the cluster-session form, so it was removed rather than renamed.
     */
    public static final int NO_SOURCE_ID = -1;

    /** {@link #leadershipChanged} and friends return this when the event produces no frame. */
    public static final int NO_FRAME = 0;

    // ── Ingress decode (schema 200, sbe-unsequenced.xml) ──────────────────────
    // Only the outer framing header and the generic `header` composite are ever decoded — body fields
    // are copied through as opaque bytes (see sequenceMessage), with one bounded exception: the Gateway
    // topology row, whose scalar fields feed the derived topology below.
    private final MessageHeaderDecoder ingressMsgHeaderDecoder = new MessageHeaderDecoder();
    private final HeaderDecoder ingressHeaderDecoder = new HeaderDecoder();
    private final GatewayDecoder gatewayDecoder = new GatewayDecoder();

    // ── Egress encode (schema 202, sbe-sequenced.xml) ─────────────────────────
    private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
    private final HeaderEncoder egressHeaderEncoder = new HeaderEncoder();
    private final LeadershipChangedEncoder leadershipChangedEncoder = new LeadershipChangedEncoder();
    private final TickEncoder tickEncoder = new TickEncoder();
    private final GatewayActiveEncoder gatewayActiveEncoder = new GatewayActiveEncoder();
    private final MutableDirectBuffer encodeBuffer = new ExpandableDirectByteBuffer(4096);

    // ── Topology — derived from the sequenced Gateway rows (doc/todo.md item 8c) ─
    // Not configured: the basic-data producer publishes one Gateway row per gateway instance, and the
    // sequencer builds this from them as they pass through sequenceMessage. Pure functions of the
    // ordered log, so every node agrees, and rebuilt on full-log replay (there are no snapshots).

    /**
     * Every logical gateway's {@code gatewaySourceId}, from the Gateway rows: the set of sourceIds a
     * cluster session may be a FIX gateway under (a session that publishes under one is tracked in
     * {@link #gatewaySessionSourceId}), and the sourceId a promotion {@code GatewayActive} carries.
     */
    private final java.util.Set<Integer> gatewaySourceIds = new java.util.HashSet<>();

    /**
     * The designated-primary {@code gatewayId} — the {@code gatewayId} of the rank-0 Gateway row —
     * named by the bootstrap {@code GatewayActive}. -1 until a rank-0 row is seen; a bootstrap with no
     * designated primary produces no frame (fail closed).
     */
    private int designatedPrimaryGatewayId = -1;

    // ── Replicated state (snapshotted; advanced identically on every node) ────

    /** Cluster-wide monotone counter; advanced for messages and lifecycle events alike. */
    private long globalSeqNo = 0;

    /**
     * memberId of whichever node last reported itself the leader; -1 until the first leadership
     * event. Kept only to de-duplicate leadership changes and to stamp {@code newLeaderMemberId}
     * onto the synthesized {@code LeadershipChanged}.
     */
    private int currentLeaderMemberId = -1;

    /**
     * Cluster sessions currently publishing under a gateway sourceId ({@link #gatewaySourceIds}) —
     * FIX gateway processes attached to cluster ingress — mapped to the sourceId each publishes under,
     * so {@link #sessionClosed} can promote with that session's own gateway sourceId. Added in {@link
     * #sequenceMessage} the first time a session stamps a gateway sourceId, removed in {@link
     * #sessionClosed}. Replicated state: built identically on every node, so every node promotes at
     * the same close.
     */
    private final java.util.Map<Long, Integer> gatewaySessionSourceId = new java.util.HashMap<>();

    /** True once the bootstrap {@code GatewayActive} has been synthesized (on the first EndBasicData). */
    private boolean bootstrapActivationEmitted = false;

    /**
     * Set when {@link #sequenceMessage} sequenced the EndBasicData that must be followed by the
     * bootstrap {@code GatewayActive}; consumed (and cleared) by {@link #pendingBootstrapActivation}.
     */
    private boolean bootstrapActivationPending = false;

    /** Topology is derived from the sequenced Gateway rows (see {@link #gatewaySourceIds}), not configured. */
    public Sequencer() {}

    /** The buffer every encode writes into, from offset 0. Valid up to the length just returned. */
    public MutableDirectBuffer buffer() {
        return encodeBuffer;
    }

    /** The last assigned sequence number; 0 before anything has been sequenced. */
    public long globalSeqNo() {
        return globalSeqNo;
    }

    /** Restores the counter from a snapshot. The only way state moves other than by an event. */
    public void globalSeqNo(final long value) {
        globalSeqNo = value;
    }

    /** memberId of the last observed leader; -1 until the first {@link #leadershipChanged}. */
    public int currentLeaderMemberId() {
        return currentLeaderMemberId;
    }

    /**
     * Re-stamps one ingress (schema 200) message as a sequenced (schema 202) frame, copying
     * everything past the {@code header} composite through verbatim — see the class Javadoc.
     *
     * @param buffer    holding the ingress message
     * @param offset    of the ingress message's outer {@code MessageHeader}
     * @param length    of the whole ingress message, framing header included
     * @param sessionId Aeron Cluster session the message arrived on
     * @param timestamp cluster consensus time to stamp
     * @return length of the encoded frame in {@link #buffer()}
     */
    public int sequenceMessage(final DirectBuffer buffer, final int offset, final int length, final long sessionId,
                               final long timestamp) {
        final long globalSeq = ++globalSeqNo;

        // Decode just enough of the ingress message to re-stamp it: the outer framing header (for
        // templateId/blockLength) and the `header` composite (for sourceId/connectionId) — both at
        // fixed offsets, independent of message type.
        ingressMsgHeaderDecoder.wrap(buffer, offset);
        final int templateId = ingressMsgHeaderDecoder.templateId();
        final int ingressBlockLen = ingressMsgHeaderDecoder.blockLength();

        final int ingressBodyOffset = offset + MessageHeaderDecoder.ENCODED_LENGTH;
        ingressHeaderDecoder.wrap(buffer, ingressBodyOffset);
        final int sourceId = ingressHeaderDecoder.sourceId();
        final int connectionId = ingressHeaderDecoder.connectionId();

        // Topology bookkeeping for standby promotion (see sessionClosed / pendingBootstrapActivation).
        // A Gateway row defines the topology; a session that publishes under a gateway sourceId is a FIX
        // gateway; the first EndBasicData designates the primary. All pure functions of the ordered log.
        if (gatewaySourceIds.contains(sourceId)) {
            gatewaySessionSourceId.put(sessionId, sourceId);
        }
        if (templateId == GatewayDecoder.TEMPLATE_ID) {
            // The one bounded exception to opaque copy-through: decode the Gateway row's scalars into
            // the derived topology (it is still copied through below like any other message). Its
            // gatewaySourceId joins the gateway-sourceId set; the rank-0 row names the designated primary.
            gatewayDecoder.wrap(buffer, ingressBodyOffset, ingressBlockLen, ingressMsgHeaderDecoder.version());
            gatewaySourceIds.add(gatewayDecoder.gatewaySourceId());
            if (gatewayDecoder.preferenceRank() == 0) {
                designatedPrimaryGatewayId = gatewayDecoder.gatewayId();
            }
        }
        if (templateId == EndBasicDataDecoder.TEMPLATE_ID && !bootstrapActivationEmitted) {
            bootstrapActivationEmitted = true;
            bootstrapActivationPending = true;
        }

        // sbe-sequenced.xml's header composite is sbe-unsequenced.xml's plus two int64 fields
        // (globalSeqNo, timestamp); every other field is byte-identical, so the egress blockLength
        // is simply the ingress blockLength with the header composite's growth added on.
        final int egressBlockLen = HeaderEncoder.ENCODED_LENGTH + (ingressBlockLen - HeaderDecoder.ENCODED_LENGTH);

        headerEncoder.wrap(encodeBuffer, 0)
            .blockLength(egressBlockLen)
            .templateId(templateId)
            .schemaId(MessageHeaderEncoder.SCHEMA_ID)
            .version(MessageHeaderEncoder.SCHEMA_VERSION);

        final int egressBodyOffset = MessageHeaderEncoder.ENCODED_LENGTH;
        egressHeaderEncoder.wrap(encodeBuffer, egressBodyOffset)
            .sourceId(sourceId)
            .connectionId(connectionId)
            .sessionId(sessionId)
            .globalSeqNo(globalSeq)
            .timestamp(timestamp);

        // Copy every byte after the ingress header composite — the rest of the fixed block plus all
        // var-data — verbatim; see the class Javadoc.
        final int copyFromOffset = ingressBodyOffset + HeaderDecoder.ENCODED_LENGTH;
        final int copyLength = length - MessageHeaderDecoder.ENCODED_LENGTH - HeaderDecoder.ENCODED_LENGTH;
        encodeBuffer.putBytes(egressBodyOffset + HeaderEncoder.ENCODED_LENGTH, buffer, copyFromOffset, copyLength);

        return egressBodyOffset + HeaderEncoder.ENCODED_LENGTH + copyLength;
    }

    /**
     * Encodes one internal clock frame carrying the consensus timestamp. Fires on every node (the
     * timer event is a committed log event delivered identically to all), so like every other event
     * here it advances each node's byte-identical tap and consumes a globalSeqNo on every node in
     * the same order. Consumers (the FIX gateway watchdog above all) read {@code header.timestamp}
     * off it to keep their session clock moving while a counterparty is silent.
     */
    public int tick(final long timestamp) {
        final long globalSeq = ++globalSeqNo;
        tickEncoder.wrapAndApplyHeader(encodeBuffer, 0, headerEncoder);
        tickEncoder.header()
            .sourceId(NO_SOURCE_ID)
            .connectionId(NO_SOURCE_ID)
            .sessionId(NO_SOURCE_ID)
            .globalSeqNo(globalSeq)
            .timestamp(timestamp);
        return MessageHeaderEncoder.ENCODED_LENGTH + tickEncoder.encodedLength();
    }

    /**
     * Encodes a {@code LeadershipChanged} event, or {@link #NO_FRAME} if this leader is already the
     * one on record. De-duplicating here (rather than at the caller) keeps the {@code globalSeqNo}
     * advance and the suppression decision in one place, so every node consumes the same number of
     * sequence numbers for the same log.
     */
    public int leadershipChanged(final int leaderMemberId, final long timestamp) {
        if (leaderMemberId == currentLeaderMemberId) {
            return NO_FRAME;
        }
        currentLeaderMemberId = leaderMemberId;
        final long globalSeq = ++globalSeqNo;
        leadershipChangedEncoder.wrapAndApplyHeader(encodeBuffer, 0, headerEncoder);
        leadershipChangedEncoder.header()
            .sourceId(NO_SOURCE_ID)
            .connectionId(NO_SOURCE_ID)
            .sessionId(NO_SOURCE_ID)
            .globalSeqNo(globalSeq)
            .timestamp(timestamp);
        leadershipChangedEncoder.newLeaderMemberId(leaderMemberId);
        return MessageHeaderEncoder.ENCODED_LENGTH + leadershipChangedEncoder.encodedLength();
    }

    /**
     * The bootstrap activation, synthesized once behind the first {@code EndBasicData}: the cluster
     * designates the primary FIX gateway by naming its {@code gatewayId} in a {@code GatewayActive},
     * so exactly one instance opens its accept gate at cold start and a standby waits. Returns the
     * frame length, or {@link #NO_FRAME} when none is pending. The adapter calls this right after the
     * {@link #sequenceMessage} that sequenced the EndBasicData, so it takes the next {@code
     * globalSeqNo} — identically on every node and on replay.
     */
    public int pendingBootstrapActivation(final long timestamp) {
        if (!bootstrapActivationPending) {
            return NO_FRAME;
        }
        bootstrapActivationPending = false;
        if (designatedPrimaryGatewayId < 0) {
            return NO_FRAME;  // no Gateway row designated a primary — nothing to activate (fail closed)
        }
        return gatewayActive(designatedPrimaryGatewayId, timestamp);
    }

    /**
     * A cluster session closed. If it was a FIX gateway session — it had published under one of {@link
     * #gatewaySourceIds} — synthesize a promotion {@code GatewayActive} carrying that sourceId (the
     * standby, sharing it, activates) and forget the session; otherwise {@link #NO_FRAME}. Driven
     * from the cluster's {@code onSessionClose}, a committed log event delivered identically to
     * every node, so the promotion lands at the same {@code globalSeqNo} everywhere.
     */
    public int sessionClosed(final long sessionId, final long timestamp) {
        final Integer sourceId = gatewaySessionSourceId.remove(sessionId);
        if (sourceId == null) {
            return NO_FRAME;
        }
        return gatewayActive(sourceId, timestamp);
    }

    /** Encodes one {@code GatewayActive} naming {@code gatewayId}; advances {@code globalSeqNo}. */
    private int gatewayActive(final int gatewayId, final long timestamp) {
        final long globalSeq = ++globalSeqNo;
        gatewayActiveEncoder.wrapAndApplyHeader(encodeBuffer, 0, headerEncoder);
        gatewayActiveEncoder.header()
            .sourceId(NO_SOURCE_ID)
            .connectionId(NO_SOURCE_ID)
            .sessionId(NO_SOURCE_ID)
            .globalSeqNo(globalSeq)
            .timestamp(timestamp);
        gatewayActiveEncoder.gatewayId(gatewayId);
        return MessageHeaderEncoder.ENCODED_LENGTH + gatewayActiveEncoder.encodedLength();
    }
}
