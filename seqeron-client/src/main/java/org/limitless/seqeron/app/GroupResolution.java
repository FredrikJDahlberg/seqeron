package org.limitless.seqeron.app;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.agrona.DirectBuffer;
import org.limitless.seqeron.protocol.FrameLayer;
import org.limitless.seqeron.protocol.SnapshotHeader;
import org.limitless.seqeron.sbe.frame.GroupsRegisteredDecoder;

/**
 * The groups a producer addresses (spec A-9): the latest group table on its queue, or its snapshot header's when
 * none follows the cut, resolved once when it has caught up, and the mask each frame it publishes then carries.
 * Holds no Aeron runtime; {@link Session} drives it. The C++ twin is {@code app/detail/GroupResolution.hpp}; keep
 * the two in step.
 */
final class GroupResolution {
    /** What {@link #frameMask} returns for a mask naming a group the table lacks. */
    static final int NOT_ADDRESSABLE = -1;

    private static final int NAME_LENGTH = GroupsRegisteredDecoder.groupNamesLength() / FrameLayer.MAX_GROUPS;

    private final List<String> addressedGroups;
    private final GroupsRegisteredDecoder groupsRegistered = new GroupsRegisteredDecoder();
    private final byte[] groupNames = new byte[GroupsRegisteredDecoder.groupNamesLength()];
    private List<SnapshotHeader.Group> table = List.of();
    private GroupDirectory directory;
    private int ownBit;

    /** @param addressedGroups the names of the other groups the producer publishes to */
    GroupResolution(final List<String> addressedGroups) {
        this.addressedGroups = List.copyOf(addressedGroups);
    }

    /** A {@code GroupsRegistered} off the queue; the latest is the table. */
    void onGroupsRegistered(final DirectBuffer buffer, final int offset) {
        groupsRegistered.wrap(buffer, offset, GroupsRegisteredDecoder.BLOCK_LENGTH,
                              GroupsRegisteredDecoder.SCHEMA_VERSION);
        groupsRegistered.getGroupNames(groupNames, 0);
        final List<SnapshotHeader.Group> groups = new ArrayList<>();
        for (int slot = 0; slot < FrameLayer.MAX_GROUPS; slot++) {
            int length = 0;
            while (length < NAME_LENGTH && groupNames[slot * NAME_LENGTH + length] != 0) {
                length++;
            }
            if (length > 0) {
                groups.add(new SnapshotHeader.Group(
                    slot + 1, new String(groupNames, slot * NAME_LENGTH, length, StandardCharsets.US_ASCII)));
            }
        }
        table = List.copyOf(groups);
    }

    /** A restore's header, standing in for the {@code GroupsRegistered} before its cut. */
    void onSnapshotHeader(final List<SnapshotHeader.Group> groups) {
        table = groups;
    }

    /** The table so far, for a snapshot header. */
    List<SnapshotHeader.Group> table() {
        return table;
    }

    /**
     * Resolves the addressed names against the table.
     * @param ownGroupId the group whose queue the producer reads
     * @return the first addressed name the table lacks, which leaves this unresolved, or null
     */
    String resolve(final int ownGroupId) {
        final GroupDirectory resolved = new GroupDirectory(table);
        for (final String name : addressedGroups) {
            if (resolved.mask(name) == 0) {
                return name;
            }
        }
        directory = resolved;
        ownBit = GroupDirectory.bit(ownGroupId);
        return null;
    }

    /** The resolved directory, or null. */
    GroupDirectory directory() {
        return directory;
    }

    /**
     * The mask a frame carries: the groups it addresses and the producer's own, since confirmed ingress watches the
     * own queue. Only once resolved.
     * @param groupMask the groups addressed, 0 for the own group alone
     * @return the frame's mask, or {@link #NOT_ADDRESSABLE}
     */
    int frameMask(final int groupMask) {
        return (groupMask & ~directory.activeMask()) != 0 ? NOT_ADDRESSABLE : groupMask | ownBit;
    }
}
