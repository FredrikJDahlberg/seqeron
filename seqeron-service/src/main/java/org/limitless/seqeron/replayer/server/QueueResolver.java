package org.limitless.seqeron.replayer.server;

import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Map;
import org.agrona.DirectBuffer;
import org.agrona.collections.Int2IntHashMap;
import org.limitless.seqeron.protocol.FrameLayer;
import org.limitless.seqeron.protocol.SequencedFrameDecoder;
import org.limitless.seqeron.protocol.SystemFrame;
import org.limitless.seqeron.sbe.frame.ApplicationRegisteredDecoder;
import org.limitless.seqeron.sbe.frame.GatewayRegisteredDecoder;
import org.limitless.seqeron.sbe.frame.GroupsRegisteredDecoder;
import org.limitless.seqeron.sbe.frame.MessageHeaderDecoder;
import org.limitless.seqeron.sbe.frame.SequencedSystemDecoder;
import org.limitless.seqeron.sbe.frame.SequencedSystemHeaderDecoder;

/**
 * Which queue an instance reads (spec R-6), from the topology rows on the log: the group table, and each gateway
 * instance's and application's group. Fed the sequenced rows off the default queue, which carries every one of
 * them, on a member as its sequencer publishes them and on a gateway host as its relay copies them. A source's
 * group never changes (S-8), so a resolution holds for the life of the archive.
 *
 * <p>Single-threaded: the replayer's duty cycle.
 */
public final class QueueResolver {
    /** No queue: the row is not on the log yet, or names a group this replayer does not serve. */
    static final int UNRESOLVED = 0;

    /** A member's replayer serves every active group. */
    static final int EVERY_GROUP = (1 << FrameLayer.MAX_GROUPS) - 1;

    private static final int GROUP_NAME_LENGTH = GroupsRegisteredDecoder.groupNamesLength() / FrameLayer.MAX_GROUPS;

    private final int relayedGroups;
    private int activeGroups;
    private final Map<String, Integer> gatewayGroups = new HashMap<>();
    private final Int2IntHashMap applicationGroups = new Int2IntHashMap(UNRESOLVED);

    private final SequencedFrameDecoder frame = new SequencedFrameDecoder();
    private final GroupsRegisteredDecoder groupsRegistered = new GroupsRegisteredDecoder();
    private final GatewayRegisteredDecoder gatewayRegistered = new GatewayRegisteredDecoder();
    private final ApplicationRegisteredDecoder applicationRegistered = new ApplicationRegisteredDecoder();
    private final byte[] groupNames = new byte[GroupsRegisteredDecoder.groupNamesLength()];

    /**
     * @param relayedGroups the groups whose queues this node holds, bit g − 1 for group g: {@link #EVERY_GROUP} on a
     *                      member, {@code replayer.groups} on a gateway host
     */
    QueueResolver(final int relayedGroups) {
        this.relayedGroups = relayedGroups;
    }

    /**
     * Whether a sequenced frame is a row this resolves from. On the default queue's publish path, so it reads two
     * fields and allocates nothing.
     * @param buffer holding the frame
     * @param offset where it starts
     * @param length its length
     * @return true for a {@code GroupsRegistered}, {@code GatewayRegistered} or {@code ApplicationRegistered}
     */
    public static boolean isTopologyRow(final DirectBuffer buffer, final int offset, final int length) {
        if (length < MessageHeaderDecoder.ENCODED_LENGTH + SequencedSystemHeaderDecoder.ENCODED_LENGTH ||
            buffer.getShort(offset + MessageHeaderDecoder.templateIdEncodingOffset(), ByteOrder.LITTLE_ENDIAN) !=
                SequencedSystemDecoder.TEMPLATE_ID) {
            return false;
        }
        final int typeOffset =
            offset + MessageHeaderDecoder.ENCODED_LENGTH + SequencedSystemHeaderDecoder.systemEventTypeEncodingOffset();
        final int systemEventType = buffer.getShort(typeOffset, ByteOrder.LITTLE_ENDIAN) & 0xFFFF;
        return systemEventType == SystemFrame.GROUPS_REGISTERED || systemEventType == SystemFrame.GATEWAY_REGISTERED ||
            systemEventType == SystemFrame.APPLICATION_REGISTERED;
    }

    /**
     * Takes one sequenced row; anything else is ignored.
     * @param buffer holding the frame
     * @param offset where it starts
     * @param length its length
     */
    void onTopologyRow(final DirectBuffer buffer, final int offset, final int length) {
        if (!isTopologyRow(buffer, offset, length) || !frame.wrap(buffer, offset, length)) {
            return;
        }
        final int body = frame.payloadOffset();
        switch (frame.systemEventType()) {
        case SystemFrame.GROUPS_REGISTERED -> {
            groupsRegistered.wrap(buffer, body, GroupsRegisteredDecoder.BLOCK_LENGTH,
                                  GroupsRegisteredDecoder.SCHEMA_VERSION);
            groupsRegistered.getGroupNames(groupNames, 0);
            int groups = 0;
            for (int slot = 0; slot < FrameLayer.MAX_GROUPS; slot++) {
                groups |= groupNames[slot * GROUP_NAME_LENGTH] != 0 ? 1 << slot : 0;
            }
            activeGroups = groups;
        }
        case SystemFrame.GATEWAY_REGISTERED -> {
            gatewayRegistered.wrap(buffer, body, GatewayRegisteredDecoder.BLOCK_LENGTH,
                                   GatewayRegisteredDecoder.SCHEMA_VERSION);
            gatewayGroups.put(gatewayRegistered.gatewayName(), (int)gatewayRegistered.groupId());
        }
        default -> {
            applicationRegistered.wrap(buffer, body, ApplicationRegisteredDecoder.BLOCK_LENGTH,
                                       ApplicationRegisteredDecoder.SCHEMA_VERSION);
            applicationGroups.put(applicationRegistered.applicationSourceId(), (int)applicationRegistered.groupId());
        }
        }
    }

    /** The groups this replayer serves, bit g − 1 for group g: active and held here. 0 until the table arrives. */
    int servedGroups() {
        return activeGroups & relayedGroups;
    }

    /** Whether {@code groupId} is one of {@link #servedGroups()}; false for an id outside 1–16. */
    boolean serves(final int groupId) {
        return groupId >= 1 && groupId <= FrameLayer.MAX_GROUPS && (servedGroups() & (1 << (groupId - 1))) != 0;
    }

    /** The group of the gateway instance its row names, or {@link #UNRESOLVED} if no row names it yet. */
    int gatewayGroup(final String gatewayName) {
        return gatewayGroups.getOrDefault(gatewayName, UNRESOLVED);
    }

    /** The group of the application its row names, or {@link #UNRESOLVED} if no row names it yet. */
    int applicationGroup(final int sourceId) {
        return applicationGroups.get(sourceId);
    }

    /** The lowest group served (§3.1), or {@link #UNRESOLVED} before the table arrives. */
    int defaultGroup() {
        final int served = servedGroups();
        return served == 0 ? UNRESOLVED : Integer.numberOfTrailingZeros(served) + 1;
    }
}
