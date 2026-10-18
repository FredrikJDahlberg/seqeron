package org.limitless.seqeron.app;

import java.util.List;
import org.limitless.seqeron.protocol.FrameLayer;
import org.limitless.seqeron.protocol.SnapshotHeader;

/**
 * The log's group table, as a producer addresses it (spec A-9): a group's name to its bit of {@code groupMask},
 * and a {@code groupId} to its name. Immutable: the groups are fixed by the first topology load (S-8), so one
 * directory holds for the life of the archive. Combine masks with {@code |}.
 *
 * <p>The C++ twin is {@code app/GroupDirectory.hpp}; keep the two in step.
 */
public final class GroupDirectory {
    private final String[] names = new String[FrameLayer.MAX_GROUPS];
    private final int activeMask;

    /** @param groups the table, each group once */
    GroupDirectory(final List<SnapshotHeader.Group> groups) {
        int mask = 0;
        for (final SnapshotHeader.Group group : groups) {
            names[group.groupId() - 1] = group.groupName();
            mask |= bit(group.groupId());
        }
        activeMask = mask;
    }

    /**
     * A group's bit.
     * @param name the group's name in the topology
     * @return its bit of {@code groupMask}, or 0 if the table names no such group
     */
    public int mask(final String name) {
        for (int slot = 0; slot < names.length; slot++) {
            if (name.equals(names[slot])) {
                return 1 << slot;
            }
        }
        return 0;
    }

    /**
     * A group's name, to label it.
     * @param groupId 1 to 16
     * @return its name, or null if the table names no such group
     */
    public String name(final int groupId) {
        return groupId < 1 || groupId > names.length ? null : names[groupId - 1];
    }

    /** Every group's bit. */
    int activeMask() {
        return activeMask;
    }

    static int bit(final int groupId) {
        return 1 << (groupId - 1);
    }
}
