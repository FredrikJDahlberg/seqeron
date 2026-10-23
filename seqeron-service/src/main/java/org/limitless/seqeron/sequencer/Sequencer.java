package org.limitless.seqeron.sequencer;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableDirectByteBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Int2IntHashMap;
import org.agrona.collections.IntArrayList;
import org.agrona.collections.Long2LongHashMap;
import org.limitless.seqeron.protocol.FrameLayer;
import org.limitless.seqeron.protocol.SystemFrame;
import org.limitless.seqeron.sbe.frame.ApplicationRegisteredDecoder;
import org.limitless.seqeron.sbe.frame.ClusterHeartbeatEncoder;
import org.limitless.seqeron.sbe.frame.GatewayActivationRequestedDecoder;
import org.limitless.seqeron.sbe.frame.GatewayActiveEncoder;
import org.limitless.seqeron.sbe.frame.GatewayRegisteredDecoder;
import org.limitless.seqeron.sbe.frame.GatewayStartedDecoder;
import org.limitless.seqeron.sbe.frame.GroupsRegisteredDecoder;
import org.limitless.seqeron.sbe.frame.LeadershipChangedEncoder;
import org.limitless.seqeron.sbe.frame.MessageHeaderDecoder;
import org.limitless.seqeron.sbe.frame.MessageHeaderEncoder;
import org.limitless.seqeron.sbe.frame.SequencedEncoder;
import org.limitless.seqeron.sbe.frame.SequencedHeaderEncoder;
import org.limitless.seqeron.sbe.frame.SequencedSystemEncoder;
import org.limitless.seqeron.sbe.frame.SequencedSystemHeaderEncoder;
import org.limitless.seqeron.sbe.frame.SessionClosedEncoder;
import org.limitless.seqeron.sbe.frame.SnapshotPolicyRegisteredDecoder;
import org.limitless.seqeron.sbe.frame.SnapshotStartedEncoder;
import org.limitless.seqeron.sbe.frame.UnsequencedDecoder;
import org.limitless.seqeron.sbe.frame.UnsequencedHeaderDecoder;
import org.limitless.seqeron.sbe.frame.UnsequencedSystemDecoder;
import org.limitless.seqeron.sbe.frame.UnsequencedSystemHeaderDecoder;
import org.limitless.seqeron.util.Logger;

/**
 * The sequencer's replicated state machine, free of every Aeron type: {@code clusterSeqNo}, the gateway
 * list and election, the groups and their queues, the snapshot rounds, and every frame encode. Each event
 * method hands its frames to the {@link FrameSink} in {@code clusterSeqNo} order, the event's own frame first
 * and whatever it triggered behind it, once to each queue the frame is on; {@link SequencerService} publishes
 * them and decides nothing.
 *
 * <p>Sequencing copies the 20-byte ingress header, appends the 24-byte stamp and copies the payload through
 * unopened (<b>E-1</b>); no {@code payloadId} is decoded, only the system payloads state derives from
 * (<b>S-2</b>).
 *
 * <p>Deterministic: no clock reads, randomness or I/O — the consensus timestamp is always passed in — so
 * every node encodes byte-identical frames. Single-threaded: driven only from the cluster's service thread.
 */
public final class Sequencer {
    /** Where every frame goes, in {@code clusterSeqNo} order. */
    public interface FrameSink {
        /**
         * The first {@code GroupsRegistered} activated these groups: open and record each one's queue. Called
         * once, before the first frame is published to any queue (§7.4).
         * @param groupIds in ascending order
         */
        void activate(int[] groupIds);

        /**
         * @param groupId the queue the frame goes to; frames on several queues come once per queue, in
         *                ascending {@code groupId}, each with that queue's {@code groupSeqNo}
         * @param buffer  holding the frame from offset 0; reused for the next frame once this returns
         * @param length  of the frame
         */
        void publish(int groupId, DirectBuffer buffer, int length);
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

    /** {@link #sourceGroup}'s missing value: no source is in group 0. */
    private static final int NO_GROUP = 0;

    /** {@link #topologySession} while no session holds it; no cluster session id equals it. */
    private static final long NO_TOPOLOGY_SESSION = Long.MIN_VALUE;

    /** The bytes of one {@code groupNames} slot. */
    private static final int GROUP_NAME_LENGTH = GroupsRegisteredDecoder.groupNamesLength() / FrameLayer.MAX_GROUPS;

    // The two ingress header composites differ only in the name of the uint16 at offset 16.
    private final MessageHeaderDecoder msgHeaderDecoder = new MessageHeaderDecoder();
    private final UnsequencedHeaderDecoder frameHeaderDecoder = new UnsequencedHeaderDecoder();
    private final UnsequencedSystemHeaderDecoder systemHeaderDecoder = new UnsequencedSystemHeaderDecoder();

    // Only these six system payloads are opened; the rest are matched on systemEventType alone.
    private final GroupsRegisteredDecoder groupsRegisteredDecoder = new GroupsRegisteredDecoder();
    private final ApplicationRegisteredDecoder applicationRegisteredDecoder = new ApplicationRegisteredDecoder();
    private final GatewayRegisteredDecoder gatewayRegisteredDecoder = new GatewayRegisteredDecoder();
    private final GatewayStartedDecoder gatewayStartedDecoder = new GatewayStartedDecoder();
    private final GatewayActivationRequestedDecoder activationRequestedDecoder =
        new GatewayActivationRequestedDecoder();
    private final SnapshotPolicyRegisteredDecoder snapshotPolicyDecoder = new SnapshotPolicyRegisteredDecoder();

    // queueHeaderEncoder writes the four stamp fields, at offsets common to both sequenced composites.
    private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
    private final SequencedHeaderEncoder queueHeaderEncoder = new SequencedHeaderEncoder();
    private final LeadershipChangedEncoder leadershipChangedEncoder = new LeadershipChangedEncoder();
    private final ClusterHeartbeatEncoder clusterHeartbeatEncoder = new ClusterHeartbeatEncoder();
    private final GatewayActiveEncoder gatewayActiveEncoder = new GatewayActiveEncoder();
    private final SnapshotStartedEncoder snapshotStartedEncoder = new SnapshotStartedEncoder();
    private final SessionClosedEncoder sessionClosedEncoder = new SessionClosedEncoder();

    private final MutableDirectBuffer encodeBuffer = new ExpandableDirectByteBuffer(4096);
    private final FrameSink sink;

    /** The latest {@code LeadershipChanged} as sequenced, kept until activation writes it as each prologue. */
    private final MutableDirectBuffer prologue = new ExpandableDirectByteBuffer(64);
    private int prologueLength = 0;

    /** A {@code GroupsRegistered}'s table, decoded by condition 11 for the frame being sequenced. */
    private final byte[] groupNamesBytes = new byte[GroupsRegisteredDecoder.groupNamesLength()];
    private final String[] offeredGroupNames = new String[FrameLayer.MAX_GROUPS];

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
    private long clusterSeqNo = 0;

    /** Each active group's name, slot {@code groupId - 1}; null for a slot that declares none. */
    private final String[] groupNames = new String[FrameLayer.MAX_GROUPS];

    /** Bit {@code g - 1} for every active group {@code g}; 0 before activation, when no frame reaches a queue. */
    private int activeGroupMask = 0;

    /** Each queue's last {@code groupSeqNo}, slot {@code groupId - 1} (<b>F-6</b>). */
    private final long[] groupSeqNos = new long[FrameLayer.MAX_GROUPS];

    /** Every registered source's group: a {@code gatewaySourceId} or {@code applicationSourceId} to its row's. */
    private final Int2IntHashMap sourceGroup = new Int2IntHashMap(NO_GROUP);

    /** The session the latest accepted {@code GroupsRegistered} arrived on, until it closes (<b>S-9</b>). */
    private long topologySession = NO_TOPOLOGY_SESSION;

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

    /** The latest policy's byte threshold; 0 turns the byte trigger off. */
    private long snapshotBytes = 0;

    /** Bytes sequenced since the last {@code SnapshotStarted}, each frame once however many queues it reaches. */
    private long bytesSinceSnapshot = 0;

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
    public long clusterSeqNo() {
        return clusterSeqNo;
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
     * establishes what the next may read — and changes no state until every condition has passed (<b>S-5</b>).
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
        final int bodyOffset = prefixOffset + UnsequencedDecoder.payloadHeaderLength();
        final int systemEventType = system ? systemHeaderDecoder.systemEventType() : 0;
        if (system && !admitSystem(buffer, bodyOffset, systemEventType, sourceId, sessionId)) {
            return false;
        }
        if (system && systemHeaderDecoder.groupMask() != 0) {
            return reject("system frame carries groupMask 0x" + Integer.toHexString(systemHeaderDecoder.groupMask()) +
                          "; system frames reach every queue");
        }
        if (activeGroupMask == 0 && systemEventType != SystemFrame.GROUPS_REGISTERED) {
            return reject("no group is active yet; the first frame must be the topology's GroupsRegistered");
        }
        final int groupMask = system ? 0 : frameHeaderDecoder.groupMask();
        if (!system && !admitGroupMask(groupMask, sessionId)) {
            return false;
        }

        if (system) {
            applySystem(systemEventType, sessionId, timestamp);
        }

        final long clusterSeq = ++clusterSeqNo;
        headerEncoder.wrap(encodeBuffer, 0)
            .blockLength(system ? SequencedSystemEncoder.BLOCK_LENGTH : SequencedEncoder.BLOCK_LENGTH)
            .templateId(system ? SequencedSystemEncoder.TEMPLATE_ID : SequencedEncoder.TEMPLATE_ID)
            .schemaId(MessageHeaderEncoder.SCHEMA_ID)
            .version(MessageHeaderEncoder.SCHEMA_VERSION);
        encodeBuffer.putBytes(MessageHeaderEncoder.ENCODED_LENGTH, buffer, headerOffset,
                              UnsequencedHeaderDecoder.ENCODED_LENGTH);
        queueHeaderEncoder.wrap(encodeBuffer, MessageHeaderEncoder.ENCODED_LENGTH)
            .sessionId(sessionId)
            .clusterSeqNo(clusterSeq)
            .timestamp(timestamp);
        encodeBuffer.putBytes(QUEUE_BODY_PREFIX_OFFSET, buffer, prefixOffset,
                              UnsequencedDecoder.payloadHeaderLength() + bodyLength);
        publish(encodeBuffer, QUEUE_BODY_OFFSET + bodyLength, system ? activeGroupMask : groupMask);

        for (int i = 0; i < designations.size(); i++) {
            publishSynthesized(gatewayActive(designations.getInt(i), timestamp));
        }
        designations.clear();
        if (snapshotRequested) {
            snapshotRequested = false;
            if (snapshotIntervalNs != NO_SNAPSHOT_POLICY) {
                publishSynthesized(snapshotStarted(timestamp));
            }
        }
        return true;
    }

    /**
     * §9.2 conditions 10 and 11 for a system frame: <b>S-6</b>, <b>S-8</b> and <b>S-9</b>. Decodes the payloads
     * state derives from, the only place one is decoded, and changes nothing; {@link #applySystem} does. A
     * system payload has no {@code MessageHeader}, so each decode supplies its own compiled {@code BLOCK_LENGTH}
     * and {@code SCHEMA_VERSION} (<b>V-3</b>); condition 9 has checked the length.
     * @return whether the frame passes both
     */
    private boolean admitSystem(final DirectBuffer buffer, final int bodyOffset, final int systemEventType,
                                final int sourceId, final long sessionId) {
        if (systemEventType != SystemFrame.GATEWAY_STARTED && listClaims(sourceId) &&
            !boundToGateway(sessionId, sourceId)) {
            return reject("systemEventType " + systemEventType + " claims listed sourceId " + sourceId +
                          " on a session no GatewayStarted bound");
        }

        final int version = MessageHeaderDecoder.SCHEMA_VERSION;
        switch (systemEventType) {
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
                return true;
            }
            case SystemFrame.GATEWAY_ACTIVATION_REQUESTED -> {
                activationRequestedDecoder.wrap(buffer, bodyOffset, GatewayActivationRequestedDecoder.BLOCK_LENGTH,
                                                version);
                final int gatewayId = activationRequestedDecoder.gatewayId();
                if (rowFor(gatewayId) == null) {
                    return reject("GatewayActivationRequested names gatewayId " + gatewayId +
                                  ", which no list row does");
                }
                return true;
            }
            case SystemFrame.GROUPS_REGISTERED -> {
                groupsRegisteredDecoder.wrap(buffer, bodyOffset, GroupsRegisteredDecoder.BLOCK_LENGTH, version);
                return admitGroups(sessionId);
            }
            case SystemFrame.GATEWAY_REGISTERED -> {
                gatewayRegisteredDecoder.wrap(buffer, bodyOffset, GatewayRegisteredDecoder.BLOCK_LENGTH, version);
                return admitRow("GatewayRegistered", sessionId, gatewayRegisteredDecoder.gatewaySourceId(),
                                gatewayRegisteredDecoder.groupId());
            }
            case SystemFrame.APPLICATION_REGISTERED -> {
                applicationRegisteredDecoder.wrap(buffer, bodyOffset, ApplicationRegisteredDecoder.BLOCK_LENGTH,
                                                  version);
                return admitRow("ApplicationRegistered", sessionId, applicationRegisteredDecoder.applicationSourceId(),
                                applicationRegisteredDecoder.groupId());
            }
            case SystemFrame.PAYLOAD_ID_REGISTERED -> {
                return admitOnTopologySession("PayloadIdRegistered", sessionId);
            }
            case SystemFrame.SNAPSHOT_POLICY_REGISTERED -> {
                snapshotPolicyDecoder.wrap(buffer, bodyOffset, SnapshotPolicyRegisteredDecoder.BLOCK_LENGTH,
                                           version);
                return admitOnTopologySession("SnapshotPolicyRegistered", sessionId);
            }
            default -> {
                return true; // allocated, ingress-legal and derived from by nothing: forwarded and not opened (S-2)
            }
        }
    }

    /**
     * Condition 11 for a {@code GroupsRegistered}: <b>S-9</b>, then <b>S-8</b> cases 1 and 2. Leaves the table
     * it offers in {@link #offeredGroupNames}.
     */
    private boolean admitGroups(final long sessionId) {
        if (topologySession != NO_TOPOLOGY_SESSION && topologySession != sessionId) {
            return reject("GroupsRegistered arrived while cluster session " + topologySession +
                          " holds the topology session");
        }
        groupsRegisteredDecoder.getGroupNames(groupNamesBytes, 0);
        int declared = 0;
        for (int slot = 0; slot < FrameLayer.MAX_GROUPS; slot++) {
            final String name = groupName(slot);
            for (int earlier = 0; name != null && earlier < slot; earlier++) {
                if (name.equals(offeredGroupNames[earlier])) {
                    return reject("GroupsRegistered names groups " + (earlier + 1) + " and " + (slot + 1) + " \"" +
                                  name + "\"");
                }
            }
            offeredGroupNames[slot] = name;
            declared += name == null ? 0 : 1;
        }
        if (declared == 0) {
            return reject("GroupsRegistered declares no group");
        }
        if (activeGroupMask != 0 && !Arrays.equals(offeredGroupNames, groupNames)) {
            return reject("GroupsRegistered differs from the active groups, which are fixed for the life of the log");
        }
        return true;
    }

    /** {@code groupNames} slot {@code slot}, up to its first 0x00, or null if it declares no group. */
    private String groupName(final int slot) {
        final int start = slot * GROUP_NAME_LENGTH;
        int end = start;
        while (end < start + GROUP_NAME_LENGTH && groupNamesBytes[end] != 0) {
            end++;
        }
        return end == start ? null : new String(groupNamesBytes, start, end - start, StandardCharsets.US_ASCII);
    }

    /** Condition 11 for a gateway or application row: <b>S-9</b>, then <b>S-8</b> cases 3 and 4. */
    private boolean admitRow(final String event, final long sessionId, final int rowSourceId, final int groupId) {
        if (!admitOnTopologySession(event, sessionId)) {
            return false;
        }
        if (groupId < 1 || groupId > FrameLayer.MAX_GROUPS || (activeGroupMask & groupBit(groupId)) == 0) {
            return reject(event + " for sourceId " + rowSourceId + " names group " + groupId + ", which is not active");
        }
        final int registered = sourceGroup.get(rowSourceId);
        if (registered != NO_GROUP && registered != groupId) {
            return reject(event + " moves sourceId " + rowSourceId + " from group " + registered + " to " + groupId +
                          "; a source's group is fixed");
        }
        return true;
    }

    /** <b>S-9</b> for a topology row: only the topology session registers one. */
    private boolean admitOnTopologySession(final String event, final long sessionId) {
        if (topologySession == NO_TOPOLOGY_SESSION || topologySession != sessionId) {
            return reject(event + " arrived on cluster session " + sessionId + ", which is not the topology session");
        }
        return true;
    }

    /** Condition 14: an application frame's {@code groupMask} (§7.4). */
    private boolean admitGroupMask(final int groupMask, final long sessionId) {
        if (groupMask == 0) {
            return reject("groupMask 0 addresses no group");
        }
        if ((groupMask & ~activeGroupMask) != 0) {
            return reject("groupMask 0x" + Integer.toHexString(groupMask) + " names a group that is not active");
        }
        final long boundGatewayId = sessionGateway.get(sessionId);
        if (boundGatewayId != NO_BINDING) {
            final int ownGroup = sourceGroup.get(rowFor((int)boundGatewayId).gatewaySourceId);
            if ((groupMask & groupBit(ownGroup)) == 0) {
                return reject("groupMask 0x" + Integer.toHexString(groupMask) + " lacks group " + ownGroup +
                              ", the bound gateway's own");
            }
        }
        return true;
    }

    /**
     * Derives state from a system payload {@link #admitSystem} has admitted and left decoded: everything a
     * system frame changes, after every reject condition has passed.
     */
    private void applySystem(final int systemEventType, final long sessionId, final long timestamp) {
        switch (systemEventType) {
            case SystemFrame.GROUPS_REGISTERED -> {
                topologySession = sessionId;
                if (activeGroupMask == 0) {
                    activateGroups();
                }
            }
            case SystemFrame.GATEWAY_REGISTERED -> {
                sourceGroup.put(gatewayRegisteredDecoder.gatewaySourceId(), gatewayRegisteredDecoder.groupId());
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
            case SystemFrame.APPLICATION_REGISTERED ->
                sourceGroup.put(applicationRegisteredDecoder.applicationSourceId(),
                                applicationRegisteredDecoder.groupId());
            case SystemFrame.GATEWAY_STARTED -> {
                final int gatewayId = gatewayStartedDecoder.gatewayId();
                final long previous = sessionGateway.put(sessionId, gatewayId);
                if (previous != NO_BINDING && previous != gatewayId) {
                    unbind(rowFor((int)previous), sessionId);
                }
                rowFor(gatewayId).boundSession = sessionId;
            }
            case SystemFrame.GATEWAY_ACTIVATION_REQUESTED -> designations.addInt(activationRequestedDecoder.gatewayId());
            case SystemFrame.SNAPSHOT_POLICY_REGISTERED -> {
                snapshotIntervalNs = TimeUnit.SECONDS.toNanos(snapshotPolicyDecoder.intervalSeconds());
                snapshotBytes = snapshotPolicyDecoder.bytes();
                if (snapshotRound == 0) {
                    lastSnapshotStart = timestamp;
                    bytesSinceSnapshot = 0;
                }
            }
            case SystemFrame.SNAPSHOT_REQUESTED -> snapshotRequested = true;
            default -> {
                // Derived from by nothing.
            }
        }
    }

    /**
     * The first {@code GroupsRegistered} activates its table: the sink opens a queue per group, and each queue
     * opens with the latest {@code LeadershipChanged} as its prologue, before the table itself (§7.4).
     */
    private void activateGroups() {
        final IntArrayList groupIds = new IntArrayList();
        for (int slot = 0; slot < FrameLayer.MAX_GROUPS; slot++) {
            groupNames[slot] = offeredGroupNames[slot];
            if (groupNames[slot] != null) {
                groupIds.addInt(slot + 1);
                activeGroupMask |= groupBit(slot + 1);
            }
        }
        sink.activate(groupIds.toIntArray());
        if (prologueLength > 0) {
            publish(prologue, prologueLength, activeGroupMask);
        }
    }

    /**
     * Refuses an ingress frame (<b>S-7</b>): logged and counted, and {@code clusterSeqNo} is not advanced.
     * @return false, for the caller to return
     */
    private boolean reject(final String reason) {
        rejectedFrameCount++;
        Logger.error(Logger.CoreComponent.Sequencer, Logger.CoreEventCode.MalformedIngressMessage, memberId,
                     "skipping malformed ingress message: %s (clusterSeqNo stays %d)", reason, clusterSeqNo);
        return false;
    }

    /**
     * Publishes the 1 Hz cluster clock frame — consumers read its consensus timestamp while producers are
     * silent — then everything that runs off it: a hand-over for each designated instance that never answered
     * with {@code GatewayStarted}, then a {@code SnapshotStarted} the interval or the byte count calls for.
     * @param timestamp now
     */
    public void clusterHeartbeat(final long timestamp) {
        final long clusterSeq = ++clusterSeqNo;
        clusterHeartbeatEncoder.wrapAndApplyHeader(encodeBuffer, 0, headerEncoder);
        stampSynthesized(clusterHeartbeatEncoder.header(), SystemFrame.CLUSTER_HEARTBEAT, clusterSeq, timestamp);
        publishSynthesized(MessageHeaderEncoder.ENCODED_LENGTH + clusterHeartbeatEncoder.encodedLength());

        int overdue;
        while ((overdue = takeOverdueActivation(timestamp)) != NO_GATEWAY_ID) {
            handOverUnansweredActivation(overdue, timestamp);
        }
        if (snapshotIntervalNs > 0 && timestamp - lastSnapshotStart >= snapshotIntervalNs ||
            snapshotIntervalNs != NO_SNAPSHOT_POLICY && snapshotBytes > 0 && bytesSinceSnapshot >= snapshotBytes) {
            publishSynthesized(snapshotStarted(timestamp));
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
        final long clusterSeq = ++clusterSeqNo;
        leadershipChangedEncoder.wrapAndApplyHeader(encodeBuffer, 0, headerEncoder);
        stampSynthesized(leadershipChangedEncoder.header(), SystemFrame.LEADERSHIP_CHANGED, clusterSeq, timestamp);
        leadershipChangedEncoder.newLeaderMemberId(leaderMemberId);
        leadershipChangedEncoder.leadershipTermId(leadershipTermId);
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + leadershipChangedEncoder.encodedLength();
        if (activeGroupMask == 0) {
            prologue.putBytes(0, encodeBuffer, 0, length);
            prologueLength = length;
        }
        publishSynthesized(length);
    }

    /** The last round started; 0 before the first. */
    public long snapshotRound() {
        return snapshotRound;
    }

    /** Encodes the next round's {@code SnapshotStarted}; its {@code clusterSeqNo} is the round's cut. */
    private int snapshotStarted(final long timestamp) {
        lastSnapshotStart = timestamp;
        bytesSinceSnapshot = 0;
        final long clusterSeq = ++clusterSeqNo;
        snapshotStartedEncoder.wrapAndApplyHeader(encodeBuffer, 0, headerEncoder);
        stampSynthesized(snapshotStartedEncoder.header(), SystemFrame.SNAPSHOT_STARTED, clusterSeq, timestamp);
        snapshotStartedEncoder.round(++snapshotRound);
        return MessageHeaderEncoder.ENCODED_LENGTH + snapshotStartedEncoder.encodedLength();
    }

    /** Whether a bootstrap {@code GatewayActive} has been published; the {@code bootstrap_activated} gauge. */
    public boolean bootstrapActivationEmitted() {
        return bootstrapActivationEmitted;
    }

    /**
     * A cluster session closed. If the designated instance of a gateway was bound to it, promote its standby; then
     * publish {@code SessionClosed}, so a producer that sees it has seen any promotion too.
     * @param sessionId session identity
     * @param timestamp now
     * @return whether a {@code GatewayActive} was published; false if this wasn't a designated gateway
     *     instance's session, or it was one with no standby to promote
     */
    public boolean sessionClosed(final long sessionId, final long timestamp) {
        if (topologySession == sessionId) {
            topologySession = NO_TOPOLOGY_SESSION;
        }
        final boolean promoted = promoteOnClose(sessionId, timestamp);
        final long clusterSeq = ++clusterSeqNo;
        sessionClosedEncoder.wrapAndApplyHeader(encodeBuffer, 0, headerEncoder);
        stampSynthesized(sessionClosedEncoder.header(), SystemFrame.SESSION_CLOSED, clusterSeq, timestamp);
        sessionClosedEncoder.clusterSessionId(sessionId);
        publishSynthesized(MessageHeaderEncoder.ENCODED_LENGTH + sessionClosedEncoder.encodedLength());
        return promoted;
    }

    private boolean promoteOnClose(final long sessionId, final long timestamp) {
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
                             + "gateway has no active instance until one starts (clusterSeqNo stays %d)",
                         closedGatewayId, clusterSeqNo);
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
                             + "(clusterSeqNo stays %d)",
                         designated, GATEWAY_ACTIVATION_TIMEOUT_MS, clusterSeqNo);
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
        publishSynthesized(gatewayActive(gatewayId, timestamp));
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
        final long clusterSeq = ++clusterSeqNo;
        gatewayActiveEncoder.wrapAndApplyHeader(encodeBuffer, 0, headerEncoder);
        stampSynthesized(gatewayActiveEncoder.header(), SystemFrame.GATEWAY_ACTIVE, clusterSeq, timestamp);
        gatewayActiveEncoder.gatewayId(gatewayId);
        return MessageHeaderEncoder.ENCODED_LENGTH + gatewayActiveEncoder.encodedLength();
    }

    /** Hands a synthesized frame at offset 0 of {@link #encodeBuffer} to every queue (§7.4). */
    private void publishSynthesized(final int length) {
        publish(encodeBuffer, length, activeGroupMask);
    }

    /**
     * Hands the frame at offset 0 of {@code buffer} to the queue of each group {@code groupMask} names, in
     * ascending {@code groupId}, stamping each queue's next {@code groupSeqNo} first (§9.5). Before activation
     * the mask is 0 and the frame reaches no queue.
     */
    private void publish(final MutableDirectBuffer buffer, final int length, final int groupMask) {
        if (groupMask != 0) {
            bytesSinceSnapshot += length;
        }
        for (int groupId = 1; groupId <= FrameLayer.MAX_GROUPS; groupId++) {
            if ((groupMask & groupBit(groupId)) != 0) {
                queueHeaderEncoder.wrap(buffer, MessageHeaderEncoder.ENCODED_LENGTH)
                    .groupSeqNo(++groupSeqNos[groupId - 1]);
                sink.publish(groupId, buffer, length);
            }
        }
    }

    /** Bit {@code groupId - 1} of {@code groupMask}. */
    private static int groupBit(final int groupId) {
        return 1 << (groupId - 1);
    }

    /**
     * Stamps a synthesized frame's header: <b>F-4</b>'s {@code -1} identity, encoded on every node rather
     * than copied through (<b>E-1</b>'s exception). {@code systemEventType} is redundant with the template
     * id and written anyway, so offset 16 discriminates every frame on the queue.
     */
    private static void stampSynthesized(final SequencedSystemHeaderEncoder header, final int systemEventType,
                                         final long clusterSeq, final long timestamp) {
        header.sourceId(NO_SOURCE_ID)
            .connectionId(NO_SOURCE_ID)
            .sessionId(NO_SOURCE_ID)
            .systemEventType(systemEventType)
            .groupMask(0)
            .clusterSeqNo(clusterSeq)
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
