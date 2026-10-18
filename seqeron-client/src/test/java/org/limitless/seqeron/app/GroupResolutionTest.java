package org.limitless.seqeron.app;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.nio.charset.StandardCharsets;
import java.util.List;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.limitless.seqeron.protocol.SnapshotHeader.Group;
import org.limitless.seqeron.sbe.frame.GroupsRegisteredEncoder;

/**
 * The groups a producer addresses (spec A-9), resolved from its queue's table. Case for case with {@code
 * GroupResolutionTest.cpp} and {@code GroupResolutionTest.cs}, in the same order.
 */
class GroupResolutionTest {
    private static final List<Group> TABLE = List.of(new Group(1, "orders"), new Group(3, "risk"));

    @Test
    void theTableIsTheLatestGroupsRegistered() {
        final GroupResolution resolution = new GroupResolution(List.of());
        groupsRegistered(resolution, new Group(2, "audit"));
        groupsRegistered(resolution, TABLE.toArray(Group[] ::new));

        assertEquals(TABLE, resolution.table());
    }

    @Test
    void aRestoredHeaderStandsInForTheGroupsRegistered() {
        final GroupResolution resolution = new GroupResolution(List.of());
        resolution.onSnapshotHeader(TABLE);
        assertEquals(TABLE, resolution.table());

        groupsRegistered(resolution, new Group(1, "orders")); // one after the cut
        assertEquals(List.of(new Group(1, "orders")), resolution.table());
    }

    @Test
    void theDirectoryNamesEveryGroupOfTheTable() {
        final GroupResolution resolution = new GroupResolution(List.of("risk"));
        resolution.onSnapshotHeader(TABLE);

        assertNull(resolution.resolve(1));
        final GroupDirectory directory = resolution.directory();
        assertEquals(1, directory.mask("orders"));
        assertEquals(4, directory.mask("risk"));
        assertEquals(0, directory.mask("audit"));
        assertEquals("risk", directory.name(3));
        assertNull(directory.name(2));
        assertNull(directory.name(0));
        assertNull(directory.name(17));
    }

    @Test
    void anAddressedNameTheTableLacksLeavesTheGroupsUnresolved() {
        final GroupResolution resolution = new GroupResolution(List.of("risk", "audit"));
        resolution.onSnapshotHeader(TABLE);

        assertEquals("audit", resolution.resolve(1));
        assertNull(resolution.directory());
    }

    @Test
    void everyFrameCarriesTheOwnGroupsBit() {
        final GroupResolution resolution = new GroupResolution(List.of());
        resolution.onSnapshotHeader(TABLE);
        resolution.resolve(3);

        assertEquals(4, resolution.frameMask(0));
        assertEquals(5, resolution.frameMask(1));
        assertEquals(4, resolution.frameMask(4));
    }

    @Test
    void aMaskNamingAGroupTheTableLacksIsNotAddressable() {
        final GroupResolution resolution = new GroupResolution(List.of());
        resolution.onSnapshotHeader(TABLE);
        resolution.resolve(1);

        assertEquals(GroupResolution.NOT_ADDRESSABLE, resolution.frameMask(2));
        assertEquals(GroupResolution.NOT_ADDRESSABLE, resolution.frameMask(1 << 15 | 1));
    }

    private static void groupsRegistered(final GroupResolution resolution, final Group... groups) {
        final byte[] names = new byte[GroupsRegisteredEncoder.groupNamesLength()];
        final int slotLength = names.length / 16;
        for (final Group group : groups) {
            final byte[] name = group.groupName().getBytes(StandardCharsets.US_ASCII);
            System.arraycopy(name, 0, names, (group.groupId() - 1) * slotLength, name.length);
        }
        final UnsafeBuffer body = new UnsafeBuffer(new byte[GroupsRegisteredEncoder.BLOCK_LENGTH]);
        new GroupsRegisteredEncoder().wrap(body, 0).putGroupNames(names, 0);
        resolution.onGroupsRegistered(body, 0);
    }
}
