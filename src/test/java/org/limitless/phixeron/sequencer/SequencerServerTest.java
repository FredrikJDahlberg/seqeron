package org.limitless.phixeron.sequencer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class SequencerServerTest {
    @Test
    void singleNodeMembersMatchDocumentedLayout() {
        assertEquals("0,localhost:9302,localhost:9303,localhost:9304,localhost:9305,localhost:9301|",
                     SequencerServer.buildClusterMembers(1));
    }

    @Test
    void ingressEndpointMatchesDocumentedLayout() {
        assertEquals("localhost:9302", SequencerServer.ingressEndpoint(0));
        assertEquals("localhost:9312", SequencerServer.ingressEndpoint(1));
    }

    // The reservation is wider than what three members bind, and products check themselves against it
    // (doc/registries.md §2). Pinned here, and in PortLayoutTest, so it cannot quietly narrow to 9325.
    @Test
    void reservedBlockCoversThreeMemberStrides() {
        assertEquals(9300, SequencerServer.CLUSTER_PORT_BLOCK_FIRST);
        assertEquals(9329, SequencerServer.CLUSTER_PORT_BLOCK_LAST);

        assertTrue(SequencerServer.isClusterPort(9300));
        assertTrue(SequencerServer.isClusterPort(9320)); // member 2's base — reserved though unbound
        assertTrue(SequencerServer.isClusterPort(9329));
        assertFalse(SequencerServer.isClusterPort(9299));
        assertFalse(SequencerServer.isClusterPort(9330));
    }

    @Test
    void threeNodeMembersMatchDocumentedLayout() {
        assertEquals("0,localhost:9302,localhost:9303,localhost:9304,localhost:9305,localhost:9301|"
                         + "1,localhost:9312,localhost:9313,localhost:9314,localhost:9315,localhost:9311|"
                         + "2,localhost:9322,localhost:9323,localhost:9324,localhost:9325,localhost:9321|",
                     SequencerServer.buildClusterMembers(3));
    }
}
