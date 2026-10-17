package org.limitless.seqeron.sequencer;

import java.util.concurrent.TimeUnit;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableDirectByteBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.IntArrayList;
import org.agrona.collections.Long2LongHashMap;
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
import org.limitless.seqeron.sbe.frame.SnapshotPolicyRegisteredDecoder;
import org.limitless.seqeron.sbe.frame.SnapshotStartedEncoder;
import org.limitless.seqeron.sbe.frame.UnsequencedDecoder;
import org.limitless.seqeron.sbe.frame.UnsequencedHeaderDecoder;
import org.limitless.seqeron.sbe.frame.UnsequencedSystemDecoder;
import org.limitless.seqeron.sbe.frame.UnsequencedSystemHeaderDecoder;
import org.limitless.seqeron.util.Logger;

/**
 * The sequencer's replicated state machine, free of every Aeron type: {@code globalSeqNo}, the gateway
 * list and election, the snapshot rounds, and every frame encode. Each event method hands its frames to
 * the {@link FrameSink} in {@code globalSeqNo} order, the event's own frame first and whatever it triggered
 * behind it; {@link SequencerService} publishes them and decides nothing.
 *
 * <p>Sequencing copies the 18-byte ingress header, appends the 16-byte stamp and copies the payload through
 * unopened (<b>E-1</b>); no {@code payloadId} is decoded, only the system payloads state derives from
 * (<b>S-2</b>).
 *
 * <p>Deterministic: no clock reads, randomness or I/O — the consensus timestamp is always passed in — so
 * every node encodes byte-identical frames. Single-threaded: driven only from the cluster's service thread.
 */
public final class Sequencer {
    /** Where every frame goes, in {@code globalSeqNo} order. */
    public interface FrameSink {
        /**
         * @param buffer holding the frame from offset 0; reused for the next frame once this returns
         * @param length of the frame
         */
        void publish(DirectBuffer buffer, int length);
    }

    /**
     * {@code header.sourceId}/{@code connectionId}/{@code sessionId} of the frames the sequencer synthesizes
     * (<b>F-4</b>); a heartbeat or an election has no producer. Refused on ingress.
     */
    public static final int NO_SOURCE_ID = -1;

    /** No gateway instance. */
    private static final int NO_GATEWAY_ID = -1;

    /** The gateway maps' missing value: no int32 id equals it, so no id can be refused as one. */
    private static final long NO_BINDING = Long.MIN_VALUE;

    /**
     * How long, in cluster time, a designated instance has to answer {@code GatewayActive} with {@code
     * GatewayStarted} before {@link #clusterHeartbeat} hands the role to a sibling. It is a
     * failover deadline, so it does not cover a cold start's replay: a pair still replaying trades the role
     * every period, one frame each, until one catches up. An instance that has answered is never swapped out.
     */
    static final long GATEWAY_ACTIVATION_TIMEOUT_MS = 5 * FrameLayer.CLUSTER_HEARTBEAT_INTERVAL_MS;

    /** {@link #GATEWAY_ACTIVATION_TIMEOUT_MS} in consensus time, which is epoch nanoseconds. */
    static final long GATEWAY_ACTIVATION_TIMEOUT_NS = TimeUnit.MILLISECONDS.toNanos(GATEWAY_ACTIVATION_TIMEOUT_MS);

    /** Core's retired {@code payloadId} (spec §6.1), refused on ingress so a stale producer fails loudly. */
    private static final int RETIRED_CORE_ID = 1;

    /** {@link #snapshotIntervalNs} before any {@code SnapshotPolicyRegistered}: rounds are off. */
    private static final long NO_SNAPSHOT_POLICY = -1;

    /** {@code varDataEncoding}'s {@code nullValue}: "absent", not a 65535-byte payload. */
    private static final int NULL_PAYLOAD_LENGTH = 65535;

    /** Offset of the payload's length prefix in a forwarded frame; the same in both families (<b>F-3</b>). */
    private static final int QUEUE_BODY_PREFIX_OFFSET =
        MessageHeaderEncoder.ENCODED_LENGTH + SequencedHeaderEncoder.ENCODED_LENGTH;

    /** Offset of the payload itself, one length prefix past that. */
    private static final int QUEUE_BODY_OFFSET = QUEUE_BODY_PREFIX_OFFSET + UnsequencedDecoder.payloadHeaderLength();

    // The two ingress header composites differ only in the name of the uint16 at offset 16.
    private final MessageHeaderDecoder msgHeaderDecoder = new MessageHeaderDecoder();
    private final UnsequencedHeaderDecoder frameHeaderDecoder = new UnsequencedHeaderDecoder();
    private final UnsequencedSystemHeaderDecoder systemHeaderDecoder = new UnsequencedSystemHeaderDecoder();

    // Only these four system payloads are opened; the rest are matched on systemEventType alone.
    private final GatewayRegisteredDecoder gatewayRegisteredDecoder = new GatewayRegisteredDecoder();
    private final GatewayStartedDecoder gatewayStartedDecoder = new GatewayStartedDecoder();
    private final GatewayActivationRequestedDecoder activationRequestedDecoder =
        new GatewayActivationRequestedDecoder();
    private final SnapshotPolicyRegisteredDecoder snapshotPolicyDecoder = new SnapshotPolicyRegisteredDecoder();

    // queueHeaderEncoder writes the three stamp fields, at offsets common to both sequenced composites.
    private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
    private final SequencedHeaderEncoder queueHeaderEncoder = new SequencedHeaderEncoder();
    private final LeadershipChangedEncoder leadershipChangedEncoder = new LeadershipChangedEncoder();
    private final ClusterHeartbeatEncoder clusterHeartbeatEncoder = new ClusterHeartbeatEncoder();
    private final GatewayActiveEncoder gatewayActiveEncoder = new GatewayActiveEncoder();
    private final SnapshotStartedEncoder snapshotStartedEncoder = new SnapshotStartedEncoder();

    private final MutableDirectBuffer encodeBuffer = new ExpandableDirectByteBuffer(4096);
    private final FrameSink sink;

    /**
     * One Gateway row: an instance ({@code gatewayId}) of a logical gateway ({@code gatewaySourceId}), and the
     * session its latest {@code GatewayStarted} bound, or {@link #NO_BINDING}.
     */
    private static final class GatewayRow {
        final int gatewayId;
        int gatewaySourceId;
        short preferenceRank;
        long boundSession = NO_BINDING;

        GatewayRow(final int gatewayId, final int gatewaySourceId, final short preferenceRank) {
            this.gatewayId = gatewayId;
            this.gatewaySourceId = gatewaySourceId;
            this.preferenceRank = preferenceRank;
        }
    }

    /**
     * Every list row seen, in log order, de-duplicated on {@code gatewayId} so a re-published list
     * re-asserts rather than duplicates. Only ever read by index, so every node iterates in log order.
     */
    private final java.util.List<GatewayRow> gatewayRows = new java.util.ArrayList<>();

    /**
     * The {@code gatewayId}s the frame being sequenced designates, published behind it: the rank-0 row of
     * each logical gateway behind the first complete list, or the one a {@code GatewayActivationRequested}
     * names. One bootstrap entry per logical gateway, since pairs elect independently; none when no row is
     * rank 0 (fail closed).
     */
    private final IntArrayList designations = new IntArrayList();

    // Replicated state: advanced identically on every node.

    /** Cluster-wide monotone counter; advanced for messages and lifecycle events alike. */
    private long globalSeqNo = 0;

    /**
     * Ingress frames refused by §9.2 since this node started (<b>S-7</b>). Reaches no frame, but nodes that
     * applied the same log prefix must agree on it. The service's operator counter counts these and the
     * messages that arrive with no client session, which never reach the sequencer.
     */
    private long rejectedFrameCount = 0;

    /**
     * Every session a {@code GatewayStarted} bound, to that frame's {@code gatewayId}, until the session closes.
     * Only {@code GatewayStarted} binds a session: {@code header.sourceId} is a routing id other clients
     * legitimately echo, so it proves nothing. A session that is not its instance's {@link
     * GatewayRow#boundSession} was replaced by a later {@code GatewayStarted} (<b>S-6</b> case 3).
     */
    private final Long2LongHashMap sessionGateway = new Long2LongHashMap(NO_BINDING);

    /** Each logical gateway's designated instance: {@code gatewaySourceId} to the last {@code GatewayActive}'s. */
    private final Long2LongHashMap designatedGateway = new Long2LongHashMap(NO_BINDING);

    /** True once the first complete list scheduled its bootstrap run; a re-published list designates nobody. */
    private boolean bootstrapActivationScheduled = false;

    /**
     * True once a <em>bootstrap</em> {@code GatewayActive} has been published: the day is open. Separate from
     * {@link #bootstrapActivationScheduled} because a list with no rank-0 row designates nobody, and a manual
     * activation goes through the same list without opening the day.
     */
    private boolean bootstrapActivationEmitted = false;

    /** Standbys promoted, on a session close or a missed activation deadline; the operator counter. */
    private long gatewayPromotionCount = 0;

    /** Promotions that found no standby to hand the role to; the operator counter. */
    private long gatewayPromotionFailedCount = 0;

    /** An outstanding {@code GatewayActive}: which instance was named, and when it stops being excused. */
    private record PendingActivation(int gatewaySourceId, int gatewayId, long deadline) { }

    /**
     * Every {@code GatewayActive} not yet answered by {@code GatewayStarted}, at most one per logical
     * gateway. A list replaced in place rather than a map, so every node walks it in arm order.
     */
    private final java.util.List<PendingActivation> pendingActivations = new java.util.ArrayList<>();

    /**
     * The latest {@code SnapshotPolicyRegistered}'s interval in consensus time, 0 for operator requests only,
     * or {@link #NO_SNAPSHOT_POLICY}. Rounds are cuts the sequencer tracks nothing about once started
     * (doc/snapshot.md §3), so this, {@link #snapshotRound} and {@link #lastSnapshotStart} are all of it.
     */
    private long snapshotIntervalNs = NO_SNAPSHOT_POLICY;

    /** The last round started; 0 before the first. */
    private long snapshotRound = 0;

    /** When the interval counts from: the last {@code SnapshotStarted}, or the policy row before any round. */
    private long lastSnapshotStart = 0;

    /** A {@code SnapshotRequested} being sequenced; its {@code SnapshotStarted} is published behind it. */
    private boolean snapshotRequested = false;

    /** This node's memberId, for the log lines only. */
    private final int memberId;

    /**
     * @param memberId this node's cluster memberId, for the log lines only
     * @param sink     where every frame goes
     */
    public Sequencer(final int memberId, final FrameSink sink) {
        this.memberId = memberId;
        this.sink = sink;
    }

    /** The last assigned sequence number; 0 before anything has been sequenced. */
    public long globalSeqNo() {
        return globalSeqNo;
    }

    /** Ingress frames refused by §9.2 since this node started; see {@link #rejectedFrameCount}. */
    public long rejectedFrameCount() {
        return rejectedFrameCount;
    }

    /** Standbys promoted since this node started. */
    public long gatewayPromotionCount() {
        return gatewayPromotionCount;
    }

    /** Promotions since this node started that found no standby. */
    public long gatewayPromotionFailedCount() {
        return gatewayPromotionFailedCount;
    }

    /**
     * Re-stamps one ingress frame of either family as its sequenced counterpart, copying its payload through
     * verbatim, then publishes whatever it triggered: the {@code GatewayActive}s it designates, then the
     * {@code SnapshotStarted} it requests. Validates against §9.2 in §9.2's order — each condition
     * establishes what the next may read.
     *
     * @param buffer    holding the ingress message
     * @param offset    of the ingress message's outer {@code MessageHeader}
     * @param length    of the whole ingress message, framing header included
     * @param sessionId Aeron Cluster session the message arrived on
     * @param timestamp cluster consensus time to stamp
     * @return whether the frame was sequenced; false if it was refused
     */
    public boolean sequenceMessage(final DirectBuffer buffer, final int offset, final int length, final long sessionId,
                                   final long timestamp) {
        if (length < FrameLayer.MIN_INGRESS_LENGTH) {
            return reject("length " + length + " is below the " + FrameLayer.MIN_INGRESS_LENGTH +
                          "-byte minimum framing");
        }
        if (length > FrameLayer.MAX_INGRESS_LENGTH) {
            return reject("length " + length + " is above the " + FrameLayer.MAX_INGRESS_LENGTH +
                          "-byte maximum framing");
        }
        msgHeaderDecoder.wrap(buffer, offset);
        if (msgHeaderDecoder.schemaId() != MessageHeaderDecoder.SCHEMA_ID) {
            return reject("schemaId " + msgHeaderDecoder.schemaId() + " is not " + MessageHeaderDecoder.SCHEMA_ID);
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

        if (deposed(sessionId)) {
            return reject("a frame arrived on a session whose gateway instance is no longer designated");
        }
        if (system && !applySystem(buffer, prefixOffset + UnsequencedDecoder.payloadHeaderLength(),
                                   systemHeaderDecoder.systemEventType(), sourceId, sessionId, timestamp)) {
            return false;
        }

        final long globalSeq = ++globalSeqNo;
        headerEncoder.wrap(encodeBuffer, 0)
            .blockLength(system ? SequencedSystemEncoder.BLOCK_LENGTH : SequencedEncoder.BLOCK_LENGTH)
            .templateId(system ? SequencedSystemEncoder.TEMPLATE_ID : SequencedEncoder.TEMPLATE_ID)
            .schemaId(MessageHeaderEncoder.SCHEMA_ID)
            .version(MessageHeaderEncoder.SCHEMA_VERSION);
        encodeBuffer.putBytes(MessageHeaderEncoder.ENCODED_LENGTH, buffer, headerOffset,
                              UnsequencedHeaderDecoder.ENCODED_LENGTH);
        queueHeaderEncoder.wrap(encodeBuffer, MessageHeaderEncoder.ENCODED_LENGTH)
            .sessionId(sessionId)
            .globalSeqNo(globalSeq)
            .timestamp(timestamp);
        encodeBuffer.putBytes(QUEUE_BODY_PREFIX_OFFSET, buffer, prefixOffset,
                              UnsequencedDecoder.payloadHeaderLength() + bodyLength);
        publish(QUEUE_BODY_OFFSET + bodyLength);

        for (int i = 0; i < designations.size(); i++) {
            publish(gatewayActive(designations.getInt(i), timestamp));
        }
        designations.clear();
        if (snapshotRequested) {
            snapshotRequested = false;
            if (snapshotIntervalNs != NO_SNAPSHOT_POLICY) {
                publish(snapshotStarted(timestamp));
            }
        }
        return true;
    }

    /**
     * Derives state from a system payload; the only place one is decoded. Returns whether the frame is
     * admitted. A system payload has no {@code MessageHeader}, so each decode supplies its own compiled
     * {@code BLOCK_LENGTH} and {@code SCHEMA_VERSION} (<b>V-3</b>); condition 9 has checked the length.
     */
    private boolean applySystem(final DirectBuffer buffer, final int bodyOffset, final int systemEventType,
                                final int sourceId, final long sessionId, final long timestamp) {
        if (systemEventType != SystemFrame.GATEWAY_STARTED && listClaims(sourceId) &&
            !boundToGateway(sessionId, sourceId)) {
            return reject("systemEventType " + systemEventType + " claims listed sourceId " + sourceId +
                          " on a session no GatewayStarted bound");
        }

        final int version = MessageHeaderDecoder.SCHEMA_VERSION;
        switch (systemEventType) {
            case SystemFrame.GATEWAY_REGISTERED -> {
                gatewayRegisteredDecoder.wrap(buffer, bodyOffset, GatewayRegisteredDecoder.BLOCK_LENGTH, version);
                addGatewayRow(gatewayRegisteredDecoder.gatewayId(), gatewayRegisteredDecoder.gatewaySourceId(),
                              gatewayRegisteredDecoder.preferenceRank());
                if (gatewayRegisteredDecoder.remaining() == 0 && !bootstrapActivationScheduled) {
                    bootstrapActivationScheduled = true;
                    for (final GatewayRow row : gatewayRows) {
                        if (row.preferenceRank == 0) {
                            designations.addInt(row.gatewayId);
                            bootstrapActivationEmitted = true;
                        }
                    }
                }
            }
            case SystemFrame.GATEWAY_STARTED -> {
                gatewayStartedDecoder.wrap(buffer, bodyOffset, GatewayStartedDecoder.BLOCK_LENGTH, version);

                final int gatewayId = gatewayStartedDecoder.gatewayId();
                final GatewayRow row = rowFor(gatewayId);
                if (row == null) {
                    return reject("GatewayStarted names gatewayId " + gatewayId + ", which no list row does");
                }
                if (row.gatewaySourceId != sourceId) {
                    return reject("GatewayStarted for gatewayId " + gatewayId + " carries sourceId " +
                                  sourceId + ", not its row's " + row.gatewaySourceId);
                }
                if (!isDesignated(gatewayId)) {
                    return reject("GatewayStarted names gatewayId " + gatewayId + ", which is not designated");
                }
                final long previous = sessionGateway.put(sessionId, gatewayId);
                if (previous != NO_BINDING && previous != gatewayId) {
                    unbind(rowFor((int)previous), sessionId);
                }
                row.boundSession = sessionId;
            }
            case SystemFrame.GATEWAY_ACTIVATION_REQUESTED -> {
                activationRequestedDecoder.wrap(buffer, bodyOffset, GatewayActivationRequestedDecoder.BLOCK_LENGTH,
                                                version);
                final int gatewayId = activationRequestedDecoder.gatewayId();
                if (rowFor(gatewayId) == null) {
                    return reject("GatewayActivationRequested names gatewayId " + gatewayId +
                                  ", which no list row does");
                }
                designations.addInt(gatewayId);
            }
            case SystemFrame.SNAPSHOT_POLICY_REGISTERED -> {
                snapshotPolicyDecoder.wrap(buffer, bodyOffset, SnapshotPolicyRegisteredDecoder.BLOCK_LENGTH,
                                           version);
                snapshotIntervalNs = TimeUnit.SECONDS.toNanos(snapshotPolicyDecoder.intervalSeconds());
                if (snapshotRound == 0) {
                    lastSnapshotStart = timestamp;
                }
            }
            case SystemFrame.SNAPSHOT_REQUESTED -> snapshotRequested = true;
            default -> {
                // Allocated, ingress-legal and derived from by nothing: forwarded and not opened (S-2).
            }
        }
        return true;
    }

    /**
     * Refuses an ingress frame (<b>S-7</b>): logged and counted, and {@code globalSeqNo} is not advanced.
     * @return false, for the caller to return
     */
    private boolean reject(final String reason) {
        rejectedFrameCount++;
        Logger.error(Logger.CoreComponent.Sequencer, Logger.CoreEventCode.MalformedIngressMessage, memberId,
                     "skipping malformed ingress message: %s (globalSeqNo stays %d)", reason, globalSeqNo);
        return false;
    }

    /**
     * Publishes the 1 Hz cluster clock frame — consumers read its consensus timestamp while producers are
     * silent — then everything that runs off it: a hand-over for each designated instance that never answered
     * with {@code GatewayStarted}, then an interval-triggered {@code SnapshotStarted}.
     * @param timestamp now
     */
    public void clusterHeartbeat(final long timestamp) {
        final long globalSeq = ++globalSeqNo;
        clusterHeartbeatEncoder.wrapAndApplyHeader(encodeBuffer, 0, headerEncoder);
        stampSynthesized(clusterHeartbeatEncoder.header(), SystemFrame.CLUSTER_HEARTBEAT, globalSeq, timestamp);
        publish(MessageHeaderEncoder.ENCODED_LENGTH + clusterHeartbeatEncoder.encodedLength());

        int overdue;
        while ((overdue = takeOverdueActivation(timestamp)) != NO_GATEWAY_ID) {
            handOverUnansweredActivation(overdue, timestamp);
        }
        if (snapshotIntervalNs > 0 && timestamp - lastSnapshotStart >= snapshotIntervalNs) {
            publish(snapshotStarted(timestamp));
        }
    }

    /**
     * Publishes a {@code LeadershipChanged} event, one per term. A term the same member wins again gets
     * one too: its election closed ingress, and a producer counts its losses against this frame.
     * @param leadershipTermId the term that begins here
     * @param leaderMemberId leader member identity
     * @param timestamp now
     */
    public void leadershipChanged(final long leadershipTermId, final int leaderMemberId, final long timestamp) {
        final long globalSeq = ++globalSeqNo;
        leadershipChangedEncoder.wrapAndApplyHeader(encodeBuffer, 0, headerEncoder);
        stampSynthesized(leadershipChangedEncoder.header(), SystemFrame.LEADERSHIP_CHANGED, globalSeq, timestamp);
        leadershipChangedEncoder.newLeaderMemberId(leaderMemberId);
        leadershipChangedEncoder.leadershipTermId(leadershipTermId);
        publish(MessageHeaderEncoder.ENCODED_LENGTH + leadershipChangedEncoder.encodedLength());
    }

    /** The last round started; 0 before the first. */
    public long snapshotRound() {
        return snapshotRound;
    }

    /** Encodes the next round's {@code SnapshotStarted}; its {@code globalSeqNo} is the round's cut. */
    private int snapshotStarted(final long timestamp) {
        lastSnapshotStart = timestamp;
        final long globalSeq = ++globalSeqNo;
        snapshotStartedEncoder.wrapAndApplyHeader(encodeBuffer, 0, headerEncoder);
        stampSynthesized(snapshotStartedEncoder.header(), SystemFrame.SNAPSHOT_STARTED, globalSeq, timestamp);
        snapshotStartedEncoder.round(++snapshotRound);
        return MessageHeaderEncoder.ENCODED_LENGTH + snapshotStartedEncoder.encodedLength();
    }

    /** Whether a bootstrap {@code GatewayActive} has been published; the {@code bootstrap_activated} gauge. */
    public boolean bootstrapActivationEmitted() {
        return bootstrapActivationEmitted;
    }

    /**
     * A cluster session closed. If the designated instance of a gateway was bound to it, promote its standby.
     * @param sessionId session identity
     * @param timestamp now
     * @return whether a {@code GatewayActive} was published; false if this wasn't a designated gateway
     *     instance's session, or it was one with no standby to promote
     */
    public boolean sessionClosed(final long sessionId, final long timestamp) {
        final long closedGatewayId = sessionGateway.remove(sessionId);
        if (closedGatewayId == NO_BINDING || !unbind(rowFor((int)closedGatewayId), sessionId) ||
            !isDesignated((int)closedGatewayId)) {
            return false; // a replaced binding's or a superseded instance's close takes the role from no one
        }
        final int promoted = promotionTarget((int)closedGatewayId);
        if (promoted == NO_GATEWAY_ID) {
            gatewayPromotionFailedCount++;
            Logger.error(Logger.CoreComponent.Sequencer, Logger.CoreEventCode.GatewayPromotionFailed, memberId,
                         "gateway instance %d's session closed with no standby to promote — this logical "
                             + "gateway has no active instance until one starts (globalSeqNo stays %d)",
                         closedGatewayId, globalSeqNo);
            return false;
        }
        promote(promoted, timestamp);
        return true;
    }

    /**
     * Hands the role on from a designated instance that never answered with {@code GatewayStarted} — one
     * that died before binding a session, so {@link #sessionClosed} can never promote past it. Run off the
     * heartbeat's consensus timestamp, so every node decides identically; the promotion arms the same
     * deadline on the instance it names.
     * @param designated the instance whose deadline passed
     * @param timestamp  now
     */
    private void handOverUnansweredActivation(final int designated, final long timestamp) {
        final int promoted = promotionTarget(designated);
        if (promoted == NO_GATEWAY_ID) {
            gatewayPromotionFailedCount++;
            Logger.error(Logger.CoreComponent.Sequencer, Logger.CoreEventCode.GatewayPromotionFailed, memberId,
                         "gateway instance %d never declared itself started within %dms and has no sibling to "
                             + "promote — this logical gateway has no active instance until one starts "
                             + "(globalSeqNo stays %d)",
                         designated, GATEWAY_ACTIVATION_TIMEOUT_MS, globalSeqNo);
            return;
        }
        Logger.error(Logger.CoreComponent.Sequencer, Logger.CoreEventCode.GatewayActivationTimeout, memberId,
                     "gateway instance %d never declared itself started within %dms of being designated — "
                         + "handing the role to instance %d",
                     designated, GATEWAY_ACTIVATION_TIMEOUT_MS, promoted);
        promote(promoted, timestamp);
    }

    /** Publishes the {@code GatewayActive} that hands a logical gateway's role to {@code gatewayId}. */
    private void promote(final int gatewayId, final long timestamp) {
        gatewayPromotionCount++;
        publish(gatewayActive(gatewayId, timestamp));
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
            if (rowFor(pending.gatewayId()).boundSession == NO_BINDING) {
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
            if (row.gatewaySourceId == closed.gatewaySourceId && row.gatewayId != closedGatewayId &&
                (best == null || row.preferenceRank < best.preferenceRank)) {
                best = row;
            }
        }
        return best == null ? NO_GATEWAY_ID : best.gatewayId;
    }

    /** Records a Gateway row, updating any earlier row for the same {@code gatewayId} in place. */
    private void addGatewayRow(final int gatewayId, final int gatewaySourceId, final short preferenceRank) {
        final GatewayRow row = rowFor(gatewayId);
        if (row == null) {
            gatewayRows.add(new GatewayRow(gatewayId, gatewaySourceId, preferenceRank));
            return;
        }
        row.gatewaySourceId = gatewaySourceId;
        row.preferenceRank = preferenceRank;
    }

    /**
     * Removes {@code row}'s binding if {@code sessionId} holds it.
     * @return whether it did
     */
    private static boolean unbind(final GatewayRow row, final long sessionId) {
        if (row.boundSession != sessionId) {
            return false;
        }
        row.boundSession = NO_BINDING;
        return true;
    }

    /**
     * Encodes one {@code GatewayActive} naming {@code gatewayId}, and arms that instance's activation
     * deadline (see {@link #handOverUnansweredActivation}).
     * @param gatewayId gateway identity
     * @param timestamp now
     */
    private int gatewayActive(final int gatewayId, final long timestamp) {
        armActivationDeadline(gatewayId, timestamp);
        final GatewayRow row = rowFor(gatewayId);
        if (row != null) {
            designatedGateway.put(row.gatewaySourceId, gatewayId);
        }
        final long globalSeq = ++globalSeqNo;
        gatewayActiveEncoder.wrapAndApplyHeader(encodeBuffer, 0, headerEncoder);
        stampSynthesized(gatewayActiveEncoder.header(), SystemFrame.GATEWAY_ACTIVE, globalSeq, timestamp);
        gatewayActiveEncoder.gatewayId(gatewayId);
        return MessageHeaderEncoder.ENCODED_LENGTH + gatewayActiveEncoder.encodedLength();
    }

    /** Hands the frame at offset 0 of {@link #encodeBuffer} to the sink. */
    private void publish(final int length) {
        sink.publish(encodeBuffer, length);
    }

    /**
     * Stamps a synthesized frame's header: <b>F-4</b>'s {@code -1} identity, encoded on every node rather
     * than copied through (<b>E-1</b>'s exception). {@code systemEventType} is redundant with the template
     * id and written anyway, so offset 16 discriminates every frame on the queue.
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
            new PendingActivation(row.gatewaySourceId, gatewayId, timestamp + GATEWAY_ACTIVATION_TIMEOUT_NS);
        for (int i = 0; i < pendingActivations.size(); i++) {
            if (pendingActivations.get(i).gatewaySourceId() == row.gatewaySourceId) {
                pendingActivations.set(i, armed);
                return;
            }
        }
        pendingActivations.add(armed);
    }

    /** Whether any list row names {@code sourceId} as its logical gateway (<b>S-6</b> cases 2 and 4). */
    private boolean listClaims(final int sourceId) {
        for (final GatewayRow row : gatewayRows) {
            if (row.gatewaySourceId == sourceId) {
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
        final long boundGatewayId = sessionGateway.get(sessionId);
        if (boundGatewayId == NO_BINDING) {
            return false;
        }
        final GatewayRow row = rowFor((int)boundGatewayId);
        return row != null && row.gatewaySourceId == sourceId;
    }

    /** Whether the last {@code GatewayActive} for {@code gatewayId}'s logical gateway named it. */
    private boolean isDesignated(final int gatewayId) {
        final GatewayRow row = rowFor(gatewayId);
        return row != null && designatedGateway.get(row.gatewaySourceId) == gatewayId;
    }

    /**
     * Whether {@code sessionId} belongs to a gateway instance the log no longer names (<b>S-6</b> case 3): a
     * {@code GatewayStarted} bound it, and since then a sibling was designated or the binding was replaced.
     */
    private boolean deposed(final long sessionId) {
        final long boundGatewayId = sessionGateway.get(sessionId);
        return boundGatewayId != NO_BINDING &&
               (rowFor((int)boundGatewayId).boundSession != sessionId || !isDesignated((int)boundGatewayId));
    }

    /** The Gateway row for {@code gatewayId}, or null if no load has named that instance. */
    private GatewayRow rowFor(final int gatewayId) {
        for (final GatewayRow row : gatewayRows) {
            if (row.gatewayId == gatewayId) {
                return row;
            }
        }
        return null;
    }
}
