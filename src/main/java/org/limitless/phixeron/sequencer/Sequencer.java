package org.limitless.phixeron.sequencer;

import org.agrona.DirectBuffer;
import org.agrona.ExpandableDirectByteBuffer;
import org.agrona.MutableDirectBuffer;
import org.limitless.phixeron.sbe.sequenced.GatewayActiveEncoder;
import org.limitless.phixeron.sbe.sequenced.HeaderEncoder;
import org.limitless.phixeron.sbe.sequenced.LeadershipChangedEncoder;
import org.limitless.phixeron.sbe.sequenced.MessageHeaderEncoder;
import org.limitless.phixeron.sbe.sequenced.Origin;
import org.limitless.phixeron.sbe.sequenced.TickEncoder;
import org.limitless.phixeron.sbe.unsequenced.BasicDataGatewayDecoder;
import org.limitless.phixeron.sbe.unsequenced.ClientConnectedDecoder;
import org.limitless.phixeron.sbe.unsequenced.ClientDisconnectedDecoder;
import org.limitless.phixeron.sbe.unsequenced.EndBasicDataDecoder;
import org.limitless.phixeron.sbe.unsequenced.GatewayStartedDecoder;
import org.limitless.phixeron.sbe.unsequenced.HeaderDecoder;
import org.limitless.phixeron.sbe.unsequenced.MessageHeaderDecoder;
import org.limitless.phixeron.util.Logger;

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

    /**
     * {@link #sessionClosed} returns this instead of {@link #NO_FRAME} when the closing session <em>was</em>
     * an active gateway's, but {@link #promotionTarget} found no standby to hand over to (fail closed
     * rather than name a nonexistent instance). Distinct from {@code NO_FRAME} so the caller can tell "this
     * session was never a gateway's" from "a gateway just went away and nothing replaced it" — the latter
     * leaves the cluster with no active instance of that logical gateway and is worth alerting on.
     */
    public static final int NO_PROMOTION_TARGET = -1;

    /**
     * No gateway instance: {@link #promotionTarget} found no sibling, {@link #designatedPrimaryGatewayId} no rank-0
     * row.
     */
    private static final int NO_GATEWAY_ID = -1;

    /**
     * How long a designated instance has, in cluster time, to answer a {@code GatewayActive} with a
     * {@code GatewayStarted} before {@link #pendingGatewayActivationTimeout} hands the role to a sibling.
     *
     * <p>It must clear a legitimate cold start: the designated instance only declares itself started
     * once it has replayed the whole log (there are no snapshots), and that grows through the trading
     * day. Being generous costs nothing here, because the failure this bounds — a designated primary
     * that never arrives — is answered a minute late rather than not at all, while a premature
     * hand-over costs a role swap to an instance that may be no readier.
     *
     * <p>Package-private so {@code SequencerTest} drives exactly this deadline rather than hardcoding it
     * a second time.
     */
    static final long GATEWAY_ACTIVATION_TIMEOUT_MS = 60_000;

    /**
     * Smallest ingress message {@link #sequenceMessage} can re-stamp: the outer framing header plus the
     * {@code header} composite, the only two things it decodes. Anything shorter is malformed.
     */
    static final int MIN_INGRESS_LENGTH = MessageHeaderDecoder.ENCODED_LENGTH + HeaderDecoder.ENCODED_LENGTH;

    /**
     * Largest value the framing header's uint16 {@code blockLength} can carry — 65535 is SBE's null
     * value for the type. {@code MessageHeaderEncoder.blockLength(int)} casts to {@code short} without
     * complaint, so anything above this has to be refused before it is written.
     */
    private static final int MAX_BLOCK_LENGTH = 65534;

    static {
        // sequenceMessage stamps the ingress version onto a schema-202 frame, which is only truthful
        // while the two schemas version in lockstep — the same assumption the byte-identity of
        // everything past the header rests on. Bumping one XML's version without the other breaks it
        // silently on the wire, so fail at class load instead.
        if (MessageHeaderDecoder.SCHEMA_VERSION != MessageHeaderEncoder.SCHEMA_VERSION) {
            throw new IllegalStateException(
                "schema version mismatch: sbe-unsequenced.xml is at version " + MessageHeaderDecoder.SCHEMA_VERSION +
                " and sbe-sequenced.xml at " + MessageHeaderEncoder.SCHEMA_VERSION +
                "; the copy-through in sequenceMessage requires them to version together");
        }
    }

    // Ingress decode (schema 200, sbe-unsequenced.xml)
    // Only the outer framing header and the generic `header` composite are ever decoded — body fields
    // are copied through as opaque bytes (see sequenceMessage), with two bounded exceptions: the Gateway
    // topology row and GatewayStarted, whose scalar fields feed the derived topology below.
    private final MessageHeaderDecoder ingressMsgHeaderDecoder = new MessageHeaderDecoder();
    private final HeaderDecoder ingressHeaderDecoder = new HeaderDecoder();
    private final BasicDataGatewayDecoder gatewayDecoder = new BasicDataGatewayDecoder();
    private final GatewayStartedDecoder gatewayStartedDecoder = new GatewayStartedDecoder();

    // Egress encode (schema 202, sbe-sequenced.xml)
    private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
    private final HeaderEncoder egressHeaderEncoder = new HeaderEncoder();
    private final LeadershipChangedEncoder leadershipChangedEncoder = new LeadershipChangedEncoder();
    private final TickEncoder tickEncoder = new TickEncoder();
    private final GatewayActiveEncoder gatewayActiveEncoder = new GatewayActiveEncoder();
    private final MutableDirectBuffer encodeBuffer = new ExpandableDirectByteBuffer(4096);

    // Topology
    /** One Gateway row: an instance ({@code gatewayId}) of a logical gateway ({@code gatewaySourceId}). */
    private record GatewayRow(int gatewayId, int gatewaySourceId, short preferenceRank) { }

    /**
     * Every Gateway row seen, in log order, de-duplicated on {@code gatewayId} so a re-emitted load (a
     * leader change mid-load) re-asserts rather than duplicates. Read only by {@link #promotionTarget},
     * and only ever by index, so the iteration order is the log's and every node agrees.
     */
    private final java.util.List<GatewayRow> gatewayRows = new java.util.ArrayList<>();

    /**
     * The designated-primary {@code gatewayId} — the {@code gatewayId} of the rank-0 Gateway row —
     * named by the bootstrap {@code GatewayActive}. {@link #NO_GATEWAY_ID} until a rank-0 row is seen; a
     * bootstrap with no designated primary produces no frame (fail closed).
     */
    private int designatedPrimaryGatewayId = NO_GATEWAY_ID;

    // Replicated state (advanced identically on every node; not snapshotted)

    /** Cluster-wide monotone counter; advanced for messages and lifecycle events alike. */
    private long globalSeqNo = 0;

    /**
     * memberId of whichever node last reported itself the leader; -1 until the first leadership
     * event. Kept only to de-duplicate leadership changes and to stamp {@code newLeaderMemberId}
     * onto the synthesized {@code LeadershipChanged}.
     */
    private int currentLeaderMemberId = -1;

    /**
     * The cluster session each <em>active</em> FIX gateway instance is attached on, mapped to that
     * instance's {@code gatewayId}, so {@link #sessionClosed} knows which instance it just lost and can
     * promote a sibling. Added in {@link #sequenceMessage} on a {@code GatewayStarted}, removed in
     * {@link #sessionClosed}. Replicated state: built identically on every node, so every node promotes
     * at the same close.
     *
     * <p><b>{@code GatewayStarted} is the only thing that puts a session in here, and that is load-bearing.</b>
     * This used to key off {@code header.sourceId} landing in the set of known gateway sourceIds, which
     * is not an assertion the publisher makes about itself: that field is the <em>routing</em> id of the
     * gateway a message is travelling to or from, and other clients legitimately echo it — the
     * OrderExecServer stamps the originating gateway's sourceId onto every ExecutionReport and
     * PortfolioQueryReply it submits. Its cluster session was therefore recorded as a gateway's, and an
     * ordinary OrderExecServer restart promoted the standby out from under a perfectly healthy primary.
     * {@code GatewayStarted} is published by a gateway about itself, on activation and nowhere else, so
     * it is the one frame that means what this map needs it to mean.
     */
    private final java.util.Map<Long, Integer> activeGatewaySession = new java.util.HashMap<>();

    /**
     * The connections currently open at each gateway: {@code header.sourceId} to the set of {@code
     * header.connectionId}s that have had a {@code ClientConnected} and no {@code ClientDisconnected}
     * yet, maintained from those frames as they pass through {@link #sequenceMessage} — the same
     * pattern as {@link #gatewayRows}. Replicated state: every node sees the same frames in the same
     * order and holds the same set. Only ever {@code add}/{@code remove}/{@code size}-d, never
     * iterated, so its hash order cannot reach a frame.
     */
    private final java.util.Map<Integer, java.util.Set<Integer>> openConnections = new java.util.HashMap<>();

    /**
     * Count of TCP clients currently connected across every gateway: {@link #openConnections}'s total
     * size, maintained incrementally rather than summed.
     *
     * <p>A live gauge, not a running tally. It moves only when a connection actually enters or leaves
     * that map, so an unmatched {@code ClientDisconnected} cannot take it negative and a repeated
     * {@code ClientConnected} cannot double-count — and a gateway that dies without disconnecting its
     * clients has its still-open connections released by the {@code GatewayStarted} its successor
     * publishes (see {@link #releaseStaleConnections}), which is what keeps this from drifting upward
     * over a day of gateway restarts.
     */
    private int connectedClientCount = 0;

    /** True once the bootstrap {@code GatewayActive} has been synthesized (on the first EndBasicData). */
    private boolean bootstrapActivationEmitted = false;

    /**
     * The {@code gatewayId} of the last {@code GatewayActive} synthesized, until it is answered by a
     * {@code GatewayStarted} or times out; {@link #NO_GATEWAY_ID} when nothing is outstanding. Armed by
     * {@link #gatewayActive}, resolved by {@link #pendingGatewayActivationTimeout} — see there for why an
     * activation needs a deadline at all.
     */
    private int pendingActivationGatewayId = NO_GATEWAY_ID;

    /** Cluster time at which {@link #pendingActivationGatewayId} is treated as never having arrived. */
    private long activationDeadline = 0;

    /**
     * This node's cluster memberId, for the diagnostic slot in {@link #reject}'s log line. Node-local
     * and <em>not</em> replicated state — no state transition reads it, so it cannot reach a frame —
     * which is also why it is set rather than constructed: {@code cluster.memberId()} is still
     * {@code NULL_VALUE} while this class is being built (see {@code SequencerService.ensureCounters}).
     * Null until then, which the logger renders as no member context rather than a wrong one.
     */
    private Integer memberId;

    /**
     * Set when {@link #sequenceMessage} sequenced the EndBasicData that must be followed by the
     * bootstrap {@code GatewayActive}; consumed (and cleared) by {@link #pendingGatewayBootstrapActivation}.
     */
    private boolean bootstrapActivationPending = false;

    /** Topology is derived from the sequenced Gateway rows (see {@link #gatewayRows}), not configured. */
    public Sequencer() {
    }

    /** The buffer every encode writes into, from offset 0. Valid up to the length just returned. */
    public MutableDirectBuffer buffer() {
        return encodeBuffer;
    }

    /** The last assigned sequence number; 0 before anything has been sequenced. */
    public long globalSeqNo() {
        return globalSeqNo;
    }

    /** memberId of the last observed leader; -1 until the first {@link #leadershipChanged}. */
    public int currentLeaderMemberId() {
        return currentLeaderMemberId;
    }

    /** Count of TCP clients currently connected across every gateway; 0 before any {@code ClientConnected}. */
    public int connectedClientCount() {
        return connectedClientCount;
    }

    /**
     * Names this node in the rejection log line; see {@link #memberId}.
     * @param memberId this node's cluster memberId
     */
    public void memberId(final int memberId) {
        this.memberId = memberId;
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
     * @return length of the encoded frame in {@link #buffer()}, or {@link #NO_FRAME} if the ingress
     *     message was malformed and skipped
     */
    public int sequenceMessage(final DirectBuffer buffer, final int offset, final int length, final long sessionId,
                               final long timestamp) {
        if (length < MIN_INGRESS_LENGTH) {
            return reject("length " + length + " is below the " + MIN_INGRESS_LENGTH + "-byte minimum framing");
        }

        ingressMsgHeaderDecoder.wrap(buffer, offset);
        final int templateId = ingressMsgHeaderDecoder.templateId();
        final int ingressBlockLen = ingressMsgHeaderDecoder.blockLength();
        if (ingressMsgHeaderDecoder.schemaId() != MessageHeaderDecoder.SCHEMA_ID) {
            return reject("schemaId " + ingressMsgHeaderDecoder.schemaId() + " is not " +
                          MessageHeaderDecoder.SCHEMA_ID);
        }
        if (ingressBlockLen < HeaderDecoder.ENCODED_LENGTH ||
            MessageHeaderDecoder.ENCODED_LENGTH + ingressBlockLen > length) {
            return reject("blockLength " + ingressBlockLen + " does not fit a " + length + "-byte frame");
        }

        final int egressBlockLen = HeaderEncoder.ENCODED_LENGTH + (ingressBlockLen - HeaderDecoder.ENCODED_LENGTH);
        if (egressBlockLen > MAX_BLOCK_LENGTH) {
            return reject("blockLength " + ingressBlockLen + " leaves no room for the " +
                          (HeaderEncoder.ENCODED_LENGTH - HeaderDecoder.ENCODED_LENGTH) +
                          " bytes the sequenced header adds");
        }

        final long globalSeq = ++globalSeqNo;
        final int ingressBodyOffset = offset + MessageHeaderDecoder.ENCODED_LENGTH;
        ingressHeaderDecoder.wrap(buffer, ingressBodyOffset);

        final int sourceId = ingressHeaderDecoder.sourceId();
        final int connectionId = ingressHeaderDecoder.connectionId();
        if (templateId == BasicDataGatewayDecoder.TEMPLATE_ID) {
            gatewayDecoder.wrap(buffer, ingressBodyOffset, ingressBlockLen, ingressMsgHeaderDecoder.version());
            addGatewayRow(gatewayDecoder.gatewayId(), gatewayDecoder.gatewaySourceId(),
                          gatewayDecoder.preferenceRank());
            if (gatewayDecoder.preferenceRank() == 0) {
                designatedPrimaryGatewayId = gatewayDecoder.gatewayId();
            }
        }
        if (templateId == GatewayStartedDecoder.TEMPLATE_ID) {
            gatewayStartedDecoder.wrap(buffer, ingressBodyOffset, ingressBlockLen, ingressMsgHeaderDecoder.version());
            activeGatewaySession.put(sessionId, gatewayStartedDecoder.gatewayId());
            releaseStaleConnections(sourceId);
        }
        if (templateId == EndBasicDataDecoder.TEMPLATE_ID && !bootstrapActivationEmitted) {
            bootstrapActivationEmitted = true;
            bootstrapActivationPending = true;
        }
        if (templateId == ClientConnectedDecoder.TEMPLATE_ID) {
            if (openConnections.computeIfAbsent(sourceId, source -> new java.util.HashSet<>()).add(connectionId)) {
                connectedClientCount++;
            }
        } else if (templateId == ClientDisconnectedDecoder.TEMPLATE_ID) {
            final java.util.Set<Integer> open = openConnections.get(sourceId);
            if (open != null && open.remove(connectionId)) {
                connectedClientCount--;
            }
        }

        headerEncoder.wrap(encodeBuffer, 0)
            .blockLength(egressBlockLen)
            .templateId(templateId)
            .schemaId(MessageHeaderEncoder.SCHEMA_ID)
            .version(ingressMsgHeaderDecoder.version());

        final int egressBodyOffset = MessageHeaderEncoder.ENCODED_LENGTH;
        egressHeaderEncoder.wrap(encodeBuffer, egressBodyOffset)
            .sourceId(sourceId)
            .connectionId(connectionId)
            .sessionId(sessionId)
            .globalSeqNo(globalSeq)
            .timestamp(timestamp)
            .origin(Origin.get(ingressHeaderDecoder.origin().value()));

        // Copy every byte after the ingress header composite
        final int copyFromOffset = ingressBodyOffset + HeaderDecoder.ENCODED_LENGTH;
        final int copyLength = length - MessageHeaderDecoder.ENCODED_LENGTH - HeaderDecoder.ENCODED_LENGTH;
        encodeBuffer.putBytes(egressBodyOffset + HeaderEncoder.ENCODED_LENGTH, buffer, copyFromOffset, copyLength);
        return egressBodyOffset + HeaderEncoder.ENCODED_LENGTH + copyLength;
    }

    /**
     * Skips a malformed ingress message
     * @param reason rejection description
     */
    private int reject(final String reason) {
        Logger.error(Logger.Component.Sequencer, Logger.EventCode.MalformedIngressMessage, memberId,
                     "skipping malformed ingress message: %s (globalSeqNo stays %d)", reason, globalSeqNo);
        return NO_FRAME;
    }

    /**
     * Encodes one internal clock frame carrying the consensus timestamp. Consumers (the FIX gateway)
     * read {@code header.timestamp} off it to keep their session clock moving while a counterparty is silent.
     * @param timestamp now
     */
    public int tick(final long timestamp) {
        final long globalSeq = ++globalSeqNo;
        tickEncoder.wrapAndApplyHeader(encodeBuffer, 0, headerEncoder);
        tickEncoder.header()
            .sourceId(NO_SOURCE_ID)
            .connectionId(NO_SOURCE_ID)
            .sessionId(NO_SOURCE_ID)
            .globalSeqNo(globalSeq)
            .timestamp(timestamp)
            .origin(Origin.Application);
        return MessageHeaderEncoder.ENCODED_LENGTH + tickEncoder.encodedLength();
    }

    /**
     * Encodes a {@code LeadershipChanged} event, or {@link #NO_FRAME} if this leader is already the
     * one on record. De-duplicating here (rather than at the caller) keeps the {@code globalSeqNo}
     * advance and the suppression decision in one place, so every node consumes the same number of
     * sequence numbers for the same log.
     * @param leaderMemberId leader member identity
     * @param timestamp now
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
            .timestamp(timestamp)
            .origin(Origin.Application);
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
     * @param timestamp now
     */
    public int pendingGatewayBootstrapActivation(final long timestamp) {
        if (!bootstrapActivationPending) {
            return NO_FRAME;
        }
        bootstrapActivationPending = false;
        if (designatedPrimaryGatewayId == NO_GATEWAY_ID) {
            return NO_FRAME; // no Gateway row designated a primary — nothing to activate (fail closed)
        }
        return gatewayActive(designatedPrimaryGatewayId, timestamp);
    }

    /**
     * A cluster session closed. If it was the session an active FIX gateway declared itself on (via
     * {@code GatewayStarted}), promote a standby of the same logical gateway; otherwise no frame.
     * @param sessionId session identity
     * @param timestamp now
     * @return a {@code GatewayActive} frame length, {@link #NO_FRAME} if this wasn't a gateway session, or
     *     {@link #NO_PROMOTION_TARGET} if it was one but no standby could be found
     */
    public int sessionClosed(final long sessionId, final long timestamp) {
        final Integer closedGatewayId = activeGatewaySession.remove(sessionId);
        if (closedGatewayId == null) {
            return NO_FRAME;
        }
        final int promoted = promotionTarget(closedGatewayId);
        if (promoted == NO_GATEWAY_ID) {
            Logger.error(Logger.Component.Sequencer, Logger.EventCode.GatewayPromotionFailed, memberId,
                         "gateway instance %d's session closed with no standby to promote — this logical "
                             + "gateway has no active instance until one starts (globalSeqNo stays %d)",
                         closedGatewayId, globalSeqNo);
            return NO_PROMOTION_TARGET;
        }
        return gatewayActive(promoted, timestamp);
    }

    /**
     * The other way a logical gateway loses its active instance: the one just designated never declares
     * itself started. Every activation — bootstrap and promotion alike — is answered by a {@code
     * GatewayStarted} the instance publishes when it opens its accept gate, and that is the only frame
     * that registers it in {@link #activeGatewaySession}. An instance that dies, or wedges, before ever
     * getting there was therefore never a registered instance, so nothing about its session closing (or
     * never closing) can promote a sibling — the cluster simply has no gateway, and no path back.
     *
     * <p>This is that path: {@link #GATEWAY_ACTIVATION_TIMEOUT_MS} after a {@code GatewayActive}, an
     * instance that has not declared itself started hands the role to its next-ranked sibling, exactly as
     * {@link #sessionClosed} does. The promotion arms the same deadline on the instance it names, so a
     * whole gateway tier that is down converges the moment any instance comes up rather than depending on
     * which one the cluster happened to designate first.
     *
     * <p>Deterministic off the cluster clock: driven from the 1 Hz {@code Tick}'s consensus timestamp, so
     * every node evaluates the same deadline against the same time and synthesizes the same frame — like
     * {@link #pendingGatewayBootstrapActivation}, the caller invokes it right after the {@link #tick} it
     * belongs to and simply publishes what comes back.
     * @param timestamp now
     * @return a {@code GatewayActive} frame length, {@link #NO_FRAME} if nothing was overdue, or {@link
     *     #NO_PROMOTION_TARGET} if an activation went unanswered and there was no sibling to hand it to
     */
    public int pendingGatewayActivationTimeout(final long timestamp) {
        if (pendingActivationGatewayId == NO_GATEWAY_ID || timestamp < activationDeadline) {
            return NO_FRAME;
        }
        final int designated = pendingActivationGatewayId;
        pendingActivationGatewayId = NO_GATEWAY_ID;
        if (activeGatewaySession.containsValue(designated)) {
            return NO_FRAME;
        }
        final int promoted = promotionTarget(designated);
        if (promoted == NO_GATEWAY_ID) {
            Logger.error(Logger.Component.Sequencer, Logger.EventCode.GatewayPromotionFailed, memberId,
                         "gateway instance %d never declared itself started within %dms and has no sibling to "
                             + "promote — this logical gateway has no active instance until one starts "
                             + "(globalSeqNo stays %d)",
                         designated, GATEWAY_ACTIVATION_TIMEOUT_MS, globalSeqNo);
            return NO_PROMOTION_TARGET;
        }
        Logger.error(Logger.Component.Sequencer, Logger.EventCode.GatewayActivationTimeout, memberId,
                     "gateway instance %d never declared itself started within %dms of being designated — "
                         + "handing the role to instance %d",
                     designated, GATEWAY_ACTIVATION_TIMEOUT_MS, promoted);
        return gatewayActive(promoted, timestamp);
    }

    /**
     * The {@code gatewayId} to hand over to when instance {@code closedGatewayId} goes away: the
     * lowest-{@code preferenceRank} other instance of the same logical gateway, ties broken by log
     * order, or {@link #NO_GATEWAY_ID} if that instance has no known row or no sibling.
     *
     * <p>A <b>{@code gatewayId}</b>, never the {@code gatewaySourceId} this used to promote with. Those
     * are separate id spaces, and every instance of a pair shares the sourceId, so a {@code
     * GatewayActive} carrying one designated <em>both</em> instances at once — which the consumer could
     * only survive by latching the first match it ever saw, and that in turn made a restarting instance
     * re-activate itself off a superseded frame during cold-start replay.
     * @param closedGatewayId the instance whose session just closed
     */
    private int promotionTarget(final int closedGatewayId) {
        GatewayRow closed = null;
        for (final GatewayRow row : gatewayRows) {
            if (row.gatewayId() == closedGatewayId) {
                closed = row;
                break;
            }
        }
        if (closed == null) {
            return NO_GATEWAY_ID;
        }
        GatewayRow best = null;
        for (final GatewayRow row : gatewayRows) {
            if (row.gatewaySourceId() == closed.gatewaySourceId() && row.gatewayId() != closedGatewayId &&
                (best == null || row.preferenceRank() < best.preferenceRank())) {
                best = row;
            }
        }
        return best == null ? NO_GATEWAY_ID : best.gatewayId();
    }

    /**
     * Drops every connection still open under {@code gatewaySourceId}. A {@code GatewayStarted} is a new
     * instance declaring it has taken that logical gateway over, so anything still open under it belongs
     * to the instance that went away, whose sockets died with it — a crash cannot publish the {@code
     * ClientDisconnected}s that would have closed them out, which is the whole reason that frame exists
     * (it carries the {@code firstConnectionId} the new instance resumes allocating from for the same
     * reason). Without this, every gateway crash leaves its clients counted forever.
     *
     * <p>It cannot drop a live connection: a gateway publishes {@code GatewayStarted} before it opens its
     * accept gate ({@code FixGateway.cpp}), so none of its own {@code ClientConnected}s can precede it.
     * @param gatewaySourceId the logical gateway whose epoch just rolled
     */
    private void releaseStaleConnections(final int gatewaySourceId) {
        final java.util.Set<Integer> stale = openConnections.remove(gatewaySourceId);
        if (stale != null) {
            connectedClientCount -= stale.size();
        }
    }

    /** Records a Gateway row, replacing any earlier row for the same {@code gatewayId} in place. */
    private void addGatewayRow(final int gatewayId, final int gatewaySourceId, final short preferenceRank) {
        final GatewayRow row = new GatewayRow(gatewayId, gatewaySourceId, preferenceRank);
        for (int i = 0; i < gatewayRows.size(); i++) {
            if (gatewayRows.get(i).gatewayId() == gatewayId) {
                gatewayRows.set(i, row);
                return;
            }
        }
        gatewayRows.add(row);
    }

    /**
     * Encodes one {@code GatewayActive} naming {@code gatewayId}; advances {@code globalSeqNo}. Arms the
     * activation deadline on the instance it names — every activation is a claim the instance still has
     * to answer (see {@link #pendingGatewayActivationTimeout}), so arming here is what keeps bootstrap
     * and promotion from needing to remember to.
     * @param gatewayId gateway identity
     * @param timestamp now
     */
    private int gatewayActive(final int gatewayId, final long timestamp) {
        pendingActivationGatewayId = gatewayId;
        activationDeadline = timestamp + GATEWAY_ACTIVATION_TIMEOUT_MS;
        final long globalSeq = ++globalSeqNo;
        gatewayActiveEncoder.wrapAndApplyHeader(encodeBuffer, 0, headerEncoder);
        gatewayActiveEncoder.header()
            .sourceId(NO_SOURCE_ID)
            .connectionId(NO_SOURCE_ID)
            .sessionId(NO_SOURCE_ID)
            .globalSeqNo(globalSeq)
            .timestamp(timestamp)
            .origin(Origin.Application);
        gatewayActiveEncoder.gatewayId(gatewayId);
        return MessageHeaderEncoder.ENCODED_LENGTH + gatewayActiveEncoder.encodedLength();
    }
}
