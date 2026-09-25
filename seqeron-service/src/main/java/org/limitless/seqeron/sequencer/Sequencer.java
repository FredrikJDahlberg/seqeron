package org.limitless.seqeron.sequencer;

import org.agrona.DirectBuffer;
import org.agrona.ExpandableDirectByteBuffer;
import org.agrona.MutableDirectBuffer;
import org.limitless.seqeron.protocol.FrameLayer;
import org.limitless.seqeron.protocol.SystemFrame;
import org.limitless.seqeron.sbe.frame.ClusterHeartbeatEncoder;
import org.limitless.seqeron.sbe.frame.GatewayActivationRequestedDecoder;
import org.limitless.seqeron.sbe.frame.GatewayActiveEncoder;
import org.limitless.seqeron.sbe.frame.GatewayRegisteredDecoder;
import org.limitless.seqeron.sbe.frame.GatewayStartedDecoder;
import org.limitless.seqeron.sbe.frame.LeadershipChangedEncoder;
import org.limitless.seqeron.sbe.frame.MessageHeaderDecoder;
import org.limitless.seqeron.sbe.frame.MessageHeaderEncoder;
import org.limitless.seqeron.sbe.frame.SequencedEncoder;
import org.limitless.seqeron.sbe.frame.SequencedHeaderEncoder;
import org.limitless.seqeron.sbe.frame.SequencedSystemEncoder;
import org.limitless.seqeron.sbe.frame.SequencedSystemHeaderEncoder;
import org.limitless.seqeron.sbe.frame.UnsequencedDecoder;
import org.limitless.seqeron.sbe.frame.UnsequencedHeaderDecoder;
import org.limitless.seqeron.sbe.frame.UnsequencedSystemDecoder;
import org.limitless.seqeron.sbe.frame.UnsequencedSystemHeaderDecoder;
import org.limitless.seqeron.util.Logger;

/**
 * The sequencer's replicated state machine, free of every Aeron type: {@code globalSeqNo}, the gateway
 * list and election, and every frame encode. Each {@code sequence*}/event method encodes one frame into
 * {@link #buffer()} from offset 0 and returns its length, or {@link #NO_FRAME}; {@link SequencerService}
 * publishes it and decides nothing.
 *
 * <p>Sequencing copies the 18-byte ingress header, appends the 16-byte stamp and copies the payload through
 * unopened (<b>E-1</b>); no {@code payloadId} is decoded, only the system payloads state derives from
 * (<b>S-2</b>).
 *
 * <p>Deterministic: no clock reads, randomness or I/O — the consensus timestamp is always passed in — so
 * every node encodes byte-identical frames. Single-threaded: driven only from the cluster's service thread.
 */
public final class Sequencer {
    /**
     * {@code header.sourceId}/{@code connectionId}/{@code sessionId} of the frames the sequencer synthesizes
     * (<b>F-4</b>); a heartbeat or an election has no producer. Refused on ingress.
     */
    public static final int NO_SOURCE_ID = -1;

    /** {@link #pendingGatewayActivation} and friends return this when the event produces no frame. */
    public static final int NO_FRAME = 0;

    /**
     * {@link #sessionClosed}'s answer when an active gateway's session closed and no standby exists, so the
     * caller can tell a gateway tier left with no active instance from a session that was never a gateway's.
     */
    public static final int NO_PROMOTION_TARGET = -1;

    /** No gateway instance. */
    private static final int NO_GATEWAY_ID = -1;

    /**
     * How long, in cluster time, a designated instance has to answer {@code GatewayActive} with {@code
     * GatewayStarted} before {@link #pendingGatewayActivationTimeout} hands the role to a sibling. It is a
     * failover deadline, so it does not cover a cold start's replay: a pair still replaying trades the role
     * every period, one frame each, until one catches up. An instance that has answered is never swapped out.
     */
    static final long GATEWAY_ACTIVATION_TIMEOUT_MS = 5 * FrameLayer.CLUSTER_HEARTBEAT_INTERVAL_MS;

    /** {@link #GATEWAY_ACTIVATION_TIMEOUT_MS} in consensus time, which is epoch nanoseconds. */
    static final long GATEWAY_ACTIVATION_TIMEOUT_NS = 5 * FrameLayer.CLUSTER_HEARTBEAT_INTERVAL_NS;

    /** Core's retired {@code payloadId} (spec §6.1), refused on ingress so a stale producer fails loudly. */
    private static final int RETIRED_CORE_ID = 1;

    /** {@code varDataEncoding}'s {@code nullValue}: "absent", not a 65535-byte payload. */
    private static final int NULL_PAYLOAD_LENGTH = 65535;

    /** Offset of the payload's length prefix in a forwarded frame; the same in both families (<b>F-3</b>). */
    private static final int TAP_BODY_PREFIX_OFFSET =
        MessageHeaderEncoder.ENCODED_LENGTH + SequencedHeaderEncoder.ENCODED_LENGTH;

    /** Offset of the payload itself, one length prefix past that. */
    private static final int TAP_BODY_OFFSET = TAP_BODY_PREFIX_OFFSET + UnsequencedDecoder.payloadHeaderLength();

    // The two ingress header composites differ only in the name of the uint16 at offset 16.
    private final MessageHeaderDecoder msgHeaderDecoder = new MessageHeaderDecoder();
    private final UnsequencedHeaderDecoder frameHeaderDecoder = new UnsequencedHeaderDecoder();
    private final UnsequencedSystemHeaderDecoder systemHeaderDecoder = new UnsequencedSystemHeaderDecoder();

    // Only these three system payloads are opened; the rest are matched on systemEventType alone.
    private final GatewayRegisteredDecoder gatewayRegisteredDecoder = new GatewayRegisteredDecoder();
    private final GatewayStartedDecoder gatewayStartedDecoder = new GatewayStartedDecoder();
    private final GatewayActivationRequestedDecoder activationRequestedDecoder =
        new GatewayActivationRequestedDecoder();

    // tapHeaderEncoder writes the three stamp fields, at offsets common to both sequenced composites.
    private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
    private final SequencedHeaderEncoder tapHeaderEncoder = new SequencedHeaderEncoder();
    private final LeadershipChangedEncoder leadershipChangedEncoder = new LeadershipChangedEncoder();
    private final ClusterHeartbeatEncoder clusterHeartbeatEncoder = new ClusterHeartbeatEncoder();
    private final GatewayActiveEncoder gatewayActiveEncoder = new GatewayActiveEncoder();

    private final MutableDirectBuffer encodeBuffer = new ExpandableDirectByteBuffer(4096);

    /** One Gateway row: an instance ({@code gatewayId}) of a logical gateway ({@code gatewaySourceId}). */
    private record GatewayRow(int gatewayId, int gatewaySourceId, short preferenceRank) { }

    /**
     * Every list row seen, in log order, de-duplicated on {@code gatewayId} so a re-published list
     * re-asserts rather than duplicates. Only ever read by index, so every node iterates in log order.
     */
    private final java.util.List<GatewayRow> gatewayRows = new java.util.ArrayList<>();

    /**
     * The {@code gatewayId}s still to be designated: the rank-0 row of each logical gateway behind the first
     * complete list, and whatever {@code GatewayActivationRequested} has since named. One bootstrap entry per
     * logical gateway, since pairs elect independently; empty when no row is rank 0 (fail closed).
     */
    private final java.util.ArrayDeque<QueuedActivation> activationQueue = new java.util.ArrayDeque<>();

    /** A queued designation, and whether it is the bootstrap's — see {@link #bootstrapActivationEmitted}. */
    private record QueuedActivation(int gatewayId, boolean bootstrap) { }

    // Replicated state: advanced identically on every node.

    /** Cluster-wide monotone counter; advanced for messages and lifecycle events alike. */
    private long globalSeqNo = 0;

    /**
     * Ingress frames refused by §9.2 since this node started (<b>S-7</b>). Reaches no frame, but nodes that
     * applied the same log prefix must agree on it; mirrored onto the operator counter of the same name.
     */
    private long rejectedFrameCount = 0;

    /**
     * The cluster session each active gateway instance declared itself on, to its {@code gatewayId}, so
     * {@link #sessionClosed} knows which instance it lost. Only {@code GatewayStarted} binds a session:
     * {@code header.sourceId} is a routing id other clients legitimately echo, so it proves nothing.
     */
    private final java.util.Map<Long, Integer> activeGatewaySession = new java.util.HashMap<>();

    /**
     * Open connections per gateway: {@code sourceId} to the {@code connectionId}s with a {@code
     * ConnectionOpened} and no {@code ConnectionClosed} yet. Never iterated, so hash order reaches no frame.
     */
    private final java.util.Map<Integer, java.util.Set<Integer>> openConnections = new java.util.HashMap<>();

    /**
     * {@link #openConnections}'s total size. Moves only when a connection enters or leaves that map, so an
     * unmatched close or a repeated open cannot skew it; see {@link #releaseStaleConnections}.
     */
    private int connectedClientCount = 0;

    /** True once the first complete list queued its bootstrap run; a re-published list designates nobody. */
    private boolean bootstrapActivationQueued = false;

    /**
     * True once a <em>bootstrap</em> {@code GatewayActive} has left {@link #pendingGatewayActivation}: the day
     * is open. Separate from {@link #bootstrapActivationQueued} because a list with no rank-0 row queues
     * nothing, and a manual activation drains through the same queue without opening the day.
     */
    private boolean bootstrapActivationEmitted = false;

    /** An outstanding {@code GatewayActive}: which instance was named, and when it stops being excused. */
    private record PendingActivation(int gatewaySourceId, int gatewayId, long deadline) { }

    /**
     * Every {@code GatewayActive} not yet answered by {@code GatewayStarted}, at most one per logical
     * gateway. A list replaced in place rather than a map, so every node walks it in arm order.
     */
    private final java.util.List<PendingActivation> pendingActivations = new java.util.ArrayList<>();

    /**
     * This node's memberId, for the rejection log line only. Set rather than constructed because {@code
     * cluster.memberId()} is not known yet when this is built; null renders as no member context.
     */
    private Integer memberId;

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

    /** Count of TCP clients currently connected across every gateway; 0 before any {@code ConnectionOpened}. */
    public int connectedClientCount() {
        return connectedClientCount;
    }

    /** Ingress frames refused by §9.2 since this node started; see {@link #rejectedFrameCount}. */
    public long rejectedFrameCount() {
        return rejectedFrameCount;
    }

    /**
     * Names this node in the rejection log line; see {@link #memberId}.
     * @param memberId this node's cluster memberId
     */
    public void memberId(final int memberId) {
        this.memberId = memberId;
    }

    /**
     * Re-stamps one ingress frame as its sequenced counterpart, copying its payload through verbatim.
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
        if (length < MessageHeaderDecoder.ENCODED_LENGTH) {
            return reject("length " + length + " is below the " + MessageHeaderDecoder.ENCODED_LENGTH +
                          "-byte framing header");
        }
        msgHeaderDecoder.wrap(buffer, offset);
        if (msgHeaderDecoder.schemaId() != MessageHeaderDecoder.SCHEMA_ID) {
            return reject("schemaId " + msgHeaderDecoder.schemaId() + " is not " + MessageHeaderDecoder.SCHEMA_ID);
        }
        return sequenceFrame(buffer, offset, length, sessionId, timestamp);
    }

    /**
     * Validates one ingress frame of either family against §9.2, in §9.2's order — each condition
     * establishes what the next may read — then re-stamps it.
     */
    private int sequenceFrame(final DirectBuffer buffer, final int offset, final int length, final long sessionId,
                              final long timestamp) {
        if (length < FrameLayer.MIN_INGRESS_LENGTH) {
            return reject("length " + length + " is below the " + FrameLayer.MIN_INGRESS_LENGTH +
                          "-byte minimum framing");
        }
        if (length > FrameLayer.MAX_INGRESS_LENGTH) {
            return reject("length " + length + " is above the " + FrameLayer.MAX_INGRESS_LENGTH +
                          "-byte maximum framing");
        }
           if (msgHeaderDecoder.version() != MessageHeaderDecoder.SCHEMA_VERSION) {
            return reject("version " + msgHeaderDecoder.version() + " is not " +
                          MessageHeaderDecoder.SCHEMA_VERSION);
        }

        final int templateId = msgHeaderDecoder.templateId();
        final boolean system = templateId == UnsequencedSystemDecoder.TEMPLATE_ID;
        if (!system && templateId != UnsequencedDecoder.TEMPLATE_ID) {
            return reject("templateId " + templateId + " is neither Unsequenced (" + UnsequencedDecoder.TEMPLATE_ID +
                          ") nor UnsequencedSystem (" + UnsequencedSystemDecoder.TEMPLATE_ID + ")");
        }
        if (msgHeaderDecoder.blockLength() != UnsequencedHeaderDecoder.ENCODED_LENGTH) {
            return reject("blockLength " + msgHeaderDecoder.blockLength() + " is not " +
                          UnsequencedHeaderDecoder.ENCODED_LENGTH);
        }

        final int headerOffset = offset + MessageHeaderDecoder.ENCODED_LENGTH;
        final int prefixOffset = headerOffset + UnsequencedHeaderDecoder.ENCODED_LENGTH;
        final int bodyLength = buffer.getShort(prefixOffset, java.nio.ByteOrder.LITTLE_ENDIAN) & 0xFFFF;
        if (bodyLength == NULL_PAYLOAD_LENGTH || FrameLayer.MIN_INGRESS_LENGTH + bodyLength != length) {
            return reject("payload length " + bodyLength + " does not fit a " + length + "-byte frame");
        }

        frameHeaderDecoder.wrap(buffer, headerOffset);
        final int sourceId = frameHeaderDecoder.sourceId();
        if (sourceId == NO_SOURCE_ID) {
            return reject("sourceId " + NO_SOURCE_ID + " is reserved for the cluster's own frames");
        }
        final int connectionId = frameHeaderDecoder.connectionId();

        if (system) {
            systemHeaderDecoder.wrap(buffer, headerOffset);
            final int systemEventType = systemHeaderDecoder.systemEventType();
            final int blockLength = SystemFrame.ingressBlockLength(systemEventType);
            if (blockLength == SystemFrame.NOT_INGRESS_LEGAL) {
                return reject("systemEventType " + systemEventType + " is not an allocated, ingress-legal event");
            }
            if (bodyLength < blockLength) {
                return reject("systemEventType " + systemEventType + " payload of " + bodyLength +
                              " bytes is short of " + blockLength);
            }
            if (!applySystem(buffer, prefixOffset + UnsequencedDecoder.payloadHeaderLength(), systemEventType,
                             sourceId, connectionId, sessionId)) {
                return NO_FRAME;
            }
        } else {
            final int payloadId = frameHeaderDecoder.payloadId();
            if (payloadId == 0) {
                return reject("payloadId 0 is not a protocol");
            }
            if (payloadId == RETIRED_CORE_ID) {
                return reject("payloadId " + RETIRED_CORE_ID +
                              " is core's retired id and carries no application protocol");
            }
        }

        final long globalSeq = ++globalSeqNo;
        headerEncoder.wrap(encodeBuffer, 0)
            .blockLength(system ? SequencedSystemEncoder.BLOCK_LENGTH : SequencedEncoder.BLOCK_LENGTH)
            .templateId(system ? SequencedSystemEncoder.TEMPLATE_ID : SequencedEncoder.TEMPLATE_ID)
            .schemaId(MessageHeaderEncoder.SCHEMA_ID)
            .version(MessageHeaderEncoder.SCHEMA_VERSION);
        encodeBuffer.putBytes(MessageHeaderEncoder.ENCODED_LENGTH, buffer, headerOffset,
                              UnsequencedHeaderDecoder.ENCODED_LENGTH);
        tapHeaderEncoder.wrap(encodeBuffer, MessageHeaderEncoder.ENCODED_LENGTH)
            .sessionId(sessionId)
            .globalSeqNo(globalSeq)
            .timestamp(timestamp);
        encodeBuffer.putBytes(TAP_BODY_PREFIX_OFFSET, buffer, prefixOffset,
                              UnsequencedDecoder.payloadHeaderLength() + bodyLength);
        return TAP_BODY_OFFSET + bodyLength;
    }

    /**
     * Derives state from a system payload; the only place one is decoded. Returns whether the frame is
     * admitted. A system payload has no {@code MessageHeader}, so each decode supplies its own compiled
     * {@code BLOCK_LENGTH} and {@code SCHEMA_VERSION} (<b>V-3</b>); condition 9 has checked the length.
     */
    private boolean applySystem(final DirectBuffer buffer, final int bodyOffset, final int systemEventType,
                                final int sourceId, final int connectionId, final long sessionId) {
        if (systemEventType != SystemFrame.GATEWAY_STARTED && listClaims(sourceId) &&
            !boundToGateway(sessionId, sourceId)) {
            return rejectSystem("systemEventType " + systemEventType + " claims listed sourceId " + sourceId +
                                 " on a session no GatewayStarted bound");
        }

        final int version = MessageHeaderDecoder.SCHEMA_VERSION;
        switch (systemEventType) {
            case SystemFrame.GATEWAY_REGISTERED -> {
                gatewayRegisteredDecoder.wrap(buffer, bodyOffset, GatewayRegisteredDecoder.BLOCK_LENGTH, version);
                addGatewayRow(gatewayRegisteredDecoder.gatewayId(), gatewayRegisteredDecoder.gatewaySourceId(),
                              gatewayRegisteredDecoder.preferenceRank());
                if (gatewayRegisteredDecoder.remaining() == 0 && !bootstrapActivationQueued) {
                    bootstrapActivationQueued = true;
                    for (final GatewayRow row : gatewayRows) {
                        if (row.preferenceRank() == 0) {
                            activationQueue.add(new QueuedActivation(row.gatewayId(), true));
                        }
                    }
                }
            }
            case SystemFrame.GATEWAY_STARTED -> {
                gatewayStartedDecoder.wrap(buffer, bodyOffset, GatewayStartedDecoder.BLOCK_LENGTH, version);

                final int gatewayId = gatewayStartedDecoder.gatewayId();
                final GatewayRow row = rowFor(gatewayId);
                if (row == null) {
                    return rejectSystem("GatewayStarted names gatewayId " + gatewayId + ", which no list row does");
                }
                if (row.gatewaySourceId() != sourceId) {
                    return rejectSystem("GatewayStarted for gatewayId " + gatewayId + " carries sourceId " +
                                         sourceId + ", not its row's " + row.gatewaySourceId());
                }
                activeGatewaySession.values().removeIf(bound -> bound == gatewayId);
                activeGatewaySession.put(sessionId, gatewayId);
                releaseStaleConnections(sourceId);
            }
            case SystemFrame.GATEWAY_ACTIVATION_REQUESTED -> {
                activationRequestedDecoder.wrap(buffer, bodyOffset, GatewayActivationRequestedDecoder.BLOCK_LENGTH,
                                                version);
                final int gatewayId = activationRequestedDecoder.gatewayId();
                if (rowFor(gatewayId) == null) {
                    return rejectSystem("GatewayActivationRequested names gatewayId " + gatewayId +
                                         ", which no list row does");
                }
                activationQueue.add(new QueuedActivation(gatewayId, false));
            }
            case SystemFrame.CONNECTION_OPENED -> {
                if (openConnections.computeIfAbsent(sourceId, source -> new java.util.HashSet<>())
                        .add(connectionId)) {
                    connectedClientCount++;
                }
            }
            case SystemFrame.CONNECTION_CLOSED -> {
                final java.util.Set<Integer> open = openConnections.get(sourceId);
                if (open != null && open.remove(connectionId)) {
                    connectedClientCount--;
                }
            }
            default -> {
                // Allocated, ingress-legal and derived from by nothing: forwarded and not opened (S-2).
            }
        }
        return true;
    }

    /** Refuses a system frame; see {@link #reject}. */
    private boolean rejectSystem(final String reason) {
        reject(reason);
        return false;
    }

    /** Refuses an ingress frame (<b>S-7</b>): logged and counted, and {@code globalSeqNo} is not advanced. */
    private int reject(final String reason) {
        rejectedFrameCount++;
        Logger.error(Logger.CoreComponent.Sequencer, Logger.CoreEventCode.MalformedIngressMessage, memberId,
                     "skipping malformed ingress message: %s (globalSeqNo stays %d)", reason, globalSeqNo);
        return NO_FRAME;
    }

    /**
     * Encodes the 1 Hz cluster clock frame; consumers read its consensus timestamp while producers are silent.
     * @param timestamp now
     */
    public int clusterHeartbeat(final long timestamp) {
        final long globalSeq = ++globalSeqNo;
        clusterHeartbeatEncoder.wrapAndApplyHeader(encodeBuffer, 0, headerEncoder);
        stampSynthesized(clusterHeartbeatEncoder.header(), SystemFrame.CLUSTER_HEARTBEAT, globalSeq, timestamp);
        return MessageHeaderEncoder.ENCODED_LENGTH + clusterHeartbeatEncoder.encodedLength();
    }

    /**
     * Encodes a {@code LeadershipChanged} event, one per term. A term the same member wins again gets
     * one too: its election closed ingress, and a producer counts its losses against this frame.
     * @param leadershipTermId the term that begins here
     * @param leaderMemberId leader member identity
     * @param timestamp now
     */
    public int leadershipChanged(final long leadershipTermId, final int leaderMemberId, final long timestamp) {
        final long globalSeq = ++globalSeqNo;
        leadershipChangedEncoder.wrapAndApplyHeader(encodeBuffer, 0, headerEncoder);
        stampSynthesized(leadershipChangedEncoder.header(), SystemFrame.LEADERSHIP_CHANGED, globalSeq, timestamp);
        leadershipChangedEncoder.newLeaderMemberId(leaderMemberId);
        leadershipChangedEncoder.leadershipTermId(leadershipTermId);
        return MessageHeaderEncoder.ENCODED_LENGTH + leadershipChangedEncoder.encodedLength();
    }

    /**
     * The next queued activation, as a {@code GatewayActive} naming one {@code gatewayId}: the bootstrap run
     * behind the list's last row, or one a {@code GatewayActivationRequested} asked for.
     *
     * <p>One frame per call, each with its own {@code globalSeqNo}: the adapter loops until {@link #NO_FRAME},
     * publishing each before asking again, since every call re-encodes into the same {@link #buffer()}.
     * @param timestamp now
     * @return a {@code GatewayActive} frame length, or {@link #NO_FRAME} when none is pending
     */
    public int pendingGatewayActivation(final long timestamp) {
        final QueuedActivation queued = activationQueue.poll();
        if (queued == null) {
            return NO_FRAME;
        }
        bootstrapActivationEmitted |= queued.bootstrap();
        return gatewayActive(queued.gatewayId(), timestamp);
    }

    /** Whether a bootstrap {@code GatewayActive} has been emitted; the {@code bootstrap_activated} gauge. */
    public boolean bootstrapActivationEmitted() {
        return bootstrapActivationEmitted;
    }

    /**
     * A cluster session closed. If an active gateway instance was bound to it, promote its standby.
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
            Logger.error(Logger.CoreComponent.Sequencer, Logger.CoreEventCode.GatewayPromotionFailed, memberId,
                         "gateway instance %d's session closed with no standby to promote — this logical "
                             + "gateway has no active instance until one starts (globalSeqNo stays %d)",
                         closedGatewayId, globalSeqNo);
            return NO_PROMOTION_TARGET;
        }
        return gatewayActive(promoted, timestamp);
    }

    /**
     * Hands the role on from a designated instance that never answered with {@code GatewayStarted} — one
     * that died before binding a session, so {@link #sessionClosed} can never promote past it. Evaluated
     * off the heartbeat's consensus timestamp, so every node decides identically; the promotion arms the
     * same deadline on the instance it names.
     * @param timestamp now
     * @return a {@code GatewayActive} frame length, {@link #NO_FRAME} if nothing was overdue, or {@link
     *     #NO_PROMOTION_TARGET} if an activation went unanswered and there was no sibling to hand it to
     */
    public int pendingGatewayActivationTimeout(final long timestamp) {
        final int designated = takeOverdueActivation(timestamp);
        if (designated == NO_GATEWAY_ID) {
            return NO_FRAME;
        }
        final int promoted = promotionTarget(designated);
        if (promoted == NO_GATEWAY_ID) {
            Logger.error(Logger.CoreComponent.Sequencer, Logger.CoreEventCode.GatewayPromotionFailed, memberId,
                         "gateway instance %d never declared itself started within %dms and has no sibling to "
                             + "promote — this logical gateway has no active instance until one starts "
                             + "(globalSeqNo stays %d)",
                         designated, GATEWAY_ACTIVATION_TIMEOUT_MS, globalSeqNo);
            return NO_PROMOTION_TARGET;
        }
        Logger.error(Logger.CoreComponent.Sequencer, Logger.CoreEventCode.GatewayActivationTimeout, memberId,
                     "gateway instance %d never declared itself started within %dms of being designated — "
                         + "handing the role to instance %d",
                     designated, GATEWAY_ACTIVATION_TIMEOUT_MS, promoted);
        return gatewayActive(promoted, timestamp);
    }

    /**
     * Removes and returns the first overdue, unanswered activation, or {@link #NO_GATEWAY_ID}. Overdue ones
     * that were answered are dropped as resolved. Removed before the caller acts, so the resulting
     * {@code gatewayActive} cannot re-enter {@link #pendingActivations} mid-iteration.
     * @param timestamp now
     */
    private int takeOverdueActivation(final long timestamp) {
        for (final java.util.Iterator<PendingActivation> it = pendingActivations.iterator(); it.hasNext(); ) {
            final PendingActivation pending = it.next();
            if (timestamp < pending.deadline()) {
                continue;
            }
            it.remove();
            if (!activeGatewaySession.containsValue(pending.gatewayId())) {
                return pending.gatewayId();
            }
        }
        return NO_GATEWAY_ID;
    }

    /**
     * The instance to hand over to when {@code closedGatewayId} goes away: the lowest-rank other instance
     * of the same logical gateway, ties broken by log order, or {@link #NO_GATEWAY_ID}. A {@code gatewayId},
     * never a {@code gatewaySourceId}, which both instances of a pair share.
     * @param closedGatewayId the instance that went away
     */
    private int promotionTarget(final int closedGatewayId) {
        final GatewayRow closed = rowFor(closedGatewayId);
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
     * Drops every connection still open under {@code gatewaySourceId}: a {@code GatewayStarted} means the
     * previous instance is gone, and a crash cannot publish its {@code ConnectionClosed}s. It cannot drop a
     * live connection, because a gateway publishes {@code GatewayStarted} before it opens its accept gate.
     * @param gatewaySourceId the logical gateway whose instance just changed
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
     * Encodes one {@code GatewayActive} naming {@code gatewayId}, and arms that instance's activation
     * deadline (see {@link #pendingGatewayActivationTimeout}).
     * @param gatewayId gateway identity
     * @param timestamp now
     */
    private int gatewayActive(final int gatewayId, final long timestamp) {
        armActivationDeadline(gatewayId, timestamp);
        final long globalSeq = ++globalSeqNo;
        gatewayActiveEncoder.wrapAndApplyHeader(encodeBuffer, 0, headerEncoder);
        stampSynthesized(gatewayActiveEncoder.header(), SystemFrame.GATEWAY_ACTIVE, globalSeq, timestamp);
        gatewayActiveEncoder.gatewayId(gatewayId);
        return MessageHeaderEncoder.ENCODED_LENGTH + gatewayActiveEncoder.encodedLength();
    }

    /**
     * Stamps a synthesized frame's header: <b>F-4</b>'s {@code -1} identity, encoded on every node rather
     * than copied through (<b>E-1</b>'s exception). {@code systemEventType} is redundant with the template
     * id and written anyway, so offset 16 discriminates every frame on the tap.
     */
    private static void stampSynthesized(final SequencedSystemHeaderEncoder header, final int systemEventType,
                                         final long globalSeq, final long timestamp) {
        header.sourceId(NO_SOURCE_ID)
            .connectionId(NO_SOURCE_ID)
            .sessionId(NO_SOURCE_ID)
            .systemEventType(systemEventType)
            .globalSeqNo(globalSeq)
            .timestamp(timestamp);
    }

    /**
     * Puts {@code gatewayId} on the clock as its logical gateway's outstanding activation, superseding any
     * earlier one. An instance with no row is not armed: nothing could be promoted in its place.
     * @param gatewayId the instance just designated
     * @param timestamp now
     */
    private void armActivationDeadline(final int gatewayId, final long timestamp) {
        final GatewayRow row = rowFor(gatewayId);
        if (row == null) {
            return;
        }
        final PendingActivation armed =
            new PendingActivation(row.gatewaySourceId(), gatewayId, timestamp + GATEWAY_ACTIVATION_TIMEOUT_NS);
        for (int i = 0; i < pendingActivations.size(); i++) {
            if (pendingActivations.get(i).gatewaySourceId() == row.gatewaySourceId()) {
                pendingActivations.set(i, armed);
                return;
            }
        }
        pendingActivations.add(armed);
    }

    /** Whether any list row names {@code sourceId} as its logical gateway (<b>S-6</b> cases 2 and 3). */
    private boolean listClaims(final int sourceId) {
        for (final GatewayRow row : gatewayRows) {
            if (row.gatewaySourceId() == sourceId) {
                return true;
            }
        }
        return false;
    }

    /**
     * Whether {@code sessionId} was bound by a {@code GatewayStarted} naming an instance of
     * {@code sourceId} (<b>S-6</b> case 2). Bound to a different logical gateway does not count.
     */
    private boolean boundToGateway(final long sessionId, final int sourceId) {
        final Integer boundGatewayId = activeGatewaySession.get(sessionId);
        if (boundGatewayId == null) {
            return false;
        }
        final GatewayRow row = rowFor(boundGatewayId);
        return row != null && row.gatewaySourceId() == sourceId;
    }

    /** The Gateway row for {@code gatewayId}, or null if no load has named that instance. */
    private GatewayRow rowFor(final int gatewayId) {
        for (final GatewayRow row : gatewayRows) {
            if (row.gatewayId() == gatewayId) {
                return row;
            }
        }
        return null;
    }
}
