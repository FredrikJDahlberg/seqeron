package org.limitless.seqeron.sequencer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.limitless.seqeron.protocol.PortLayout;

class SequencerServerTest {
    @Test
    void singleNodeMembersMatchDocumentedLayout() {
        assertEquals("0,localhost:9302,localhost:9303,localhost:9304,localhost:9305,localhost:9301|",
                     SequencerServer.buildClusterMembers(1));
    }

    @Test
    void ingressEndpointMatchesDocumentedLayout() {
        assertEquals("localhost:9302", PortLayout.ingressEndpoint(0));
        assertEquals("localhost:9312", PortLayout.ingressEndpoint(1));
    }

    // The reservation is wider than what three members bind, and applications check themselves against it
    // (doc/ops.md, "Ports"). Pinned here, and in PortLayoutTest, so it cannot quietly narrow to 9325.
    @Test
    void reservedBlockCoversThreeMemberStrides() {
        assertEquals(9300, PortLayout.CLUSTER_PORT_BLOCK_FIRST);
        assertEquals(9329, PortLayout.CLUSTER_PORT_BLOCK_LAST);

        assertTrue(PortLayout.isClusterPort(9300));
        assertTrue(PortLayout.isClusterPort(9320)); // member 2's base — reserved though unbound
        assertTrue(PortLayout.isClusterPort(9329));
        assertFalse(PortLayout.isClusterPort(9299));
        assertFalse(PortLayout.isClusterPort(9330));
    }

    @Test
    void threeNodeMembersMatchDocumentedLayout() {
        assertEquals("0,localhost:9302,localhost:9303,localhost:9304,localhost:9305,localhost:9301|"
                         + "1,localhost:9312,localhost:9313,localhost:9314,localhost:9315,localhost:9311|"
                         + "2,localhost:9322,localhost:9323,localhost:9324,localhost:9325,localhost:9321|",
                     SequencerServer.buildClusterMembers(3));
    }

    @Test
    void hostListPutsEachMemberOnItsOwnHost() {
        assertEquals("0,h0:9302,h0:9303,h0:9304,h0:9305,h0:9301|"
                         + "1,h1:9312,h1:9313,h1:9314,h1:9315,h1:9311|"
                         + "2,h2:9322,h2:9323,h2:9324,h2:9325,h2:9321|",
                     SequencerServer.buildClusterMembers(List.of("h0", "h1", "h2")));
    }

    @Test
    void hostListMustNameThisMember() {
        assertEquals(List.of("h0", "h1", "h2"), SequencerServer.memberHosts("h0,h1,h2", 2));
        assertThrows(IllegalArgumentException.class, () -> SequencerServer.memberHosts("h0,h1", 2));
        assertThrows(IllegalArgumentException.class, () -> SequencerServer.memberHosts("h0,h1", -1));
    }
}
