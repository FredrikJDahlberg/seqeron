package org.limitless.seqeron.replayer.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.limitless.seqeron.protocol.FrameLayer;

/** Spec R-6's resolution, fed the topology rows a member hands its replayer (see {@link TopologyLoad}). */
class QueueResolverTest {
    private static final long TIMESTAMP = 1_000_000_000L;

    private final TopologyLoad load = new TopologyLoad();

    @Test
    void nothingResolvesBeforeTheTable() {
        final QueueResolver resolver = new QueueResolver(QueueResolver.EVERY_GROUP);

        assertEquals(0, resolver.servedGroups());
        assertEquals(QueueResolver.UNRESOLVED, resolver.defaultGroup());
        assertFalse(resolver.serves(1));
    }

    @Test
    void theTableServesEveryGroupItDeclaresAndTheLowestIsTheDefault() {
        load.groups(2, "pricing", 5, "risk");

        final QueueResolver resolver = fed(QueueResolver.EVERY_GROUP);

        assertEquals(0b1_0010, resolver.servedGroups());
        assertEquals(2, resolver.defaultGroup(), "no groupId is special: the lowest active one is the default");
        assertTrue(resolver.serves(5));
        assertFalse(resolver.serves(1));
    }

    @Test
    void eachRowResolvesItsSourceToItsGroup() {
        load.groups(1, "pricing", 2, "risk")
            .gatewayRow(5, 9, "GW-A", 2)
            .gatewayRow(6, 9, "GW-B", 2)
            .applicationRow(16, 1);

        final QueueResolver resolver = fed(QueueResolver.EVERY_GROUP);

        assertEquals(2, resolver.gatewayGroup("GW-A"));
        assertEquals(2, resolver.gatewayGroup("GW-B"));
        assertEquals(1, resolver.applicationGroup(16));
        assertEquals(QueueResolver.UNRESOLVED, resolver.gatewayGroup("GW-C"), "no row names it yet");
        assertEquals(QueueResolver.UNRESOLVED, resolver.applicationGroup(17));
    }

    @Test
    void aGatewayHostServesOnlyTheGroupsItRelays() {
        load.groups(1, "pricing", 2, "risk", 3, "audit").gatewayRow(5, 9, "GW-A", 1);

        final QueueResolver resolver = fed(0b110);

        assertEquals(0b110, resolver.servedGroups());
        assertEquals(2, resolver.defaultGroup(), "the lowest queue the host relays");
        assertEquals(1, resolver.gatewayGroup("GW-A"), "resolved, though not to a queue this host serves");
        assertFalse(resolver.serves(1));
    }

    @Test
    void onlyTopologyRowsAreHandedOverAndEachOnce() {
        load.sequencer().leadershipChanged(1, 0, TIMESTAMP); // the prologue, on every queue
        load.groups(1, "pricing", 2, "risk").gatewayRow(5, 9, "GW-A", 2).applicationRow(16, 1);
        load.sequencer().clusterHeartbeat(TIMESTAMP);
        load.payload(0b11);

        assertEquals(3, load.rows().size(), "the table and the two rows, off the default queue alone");
    }

    @Test
    void aGroupIdOutsideTheTableIsNeverServed() {
        load.groups(1, "pricing");

        final QueueResolver resolver = fed(QueueResolver.EVERY_GROUP);

        assertFalse(resolver.serves(0));
        assertFalse(resolver.serves(FrameLayer.MAX_GROUPS + 1));
    }

    private QueueResolver fed(final int relayedGroups) {
        final QueueResolver resolver = new QueueResolver(relayedGroups);
        for (final byte[] row : load.rows()) {
            resolver.onTopologyRow(new UnsafeBuffer(row), 0, row.length);
        }
        return resolver;
    }
}
