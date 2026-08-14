package org.limitless.phixeron.sequencer;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class SequencerNodeTest {
    @Test
    void singleNodeMembersMatchDocumentedLayout() {
        assertEquals("0,localhost:9302,localhost:9303,localhost:9304,localhost:9305,localhost:9301|",
            SequencerNode.buildClusterMembers(1));
    }

    @Test
    void ingressEndpointMatchesDocumentedLayout() {
        assertEquals("localhost:9302", SequencerNode.ingressEndpoint(0));
        assertEquals("localhost:9312", SequencerNode.ingressEndpoint(1));
    }

    @Test
    void threeNodeMembersMatchDocumentedLayout() {
        assertEquals(
            "0,localhost:9302,localhost:9303,localhost:9304,localhost:9305,localhost:9301|"
                + "1,localhost:9312,localhost:9313,localhost:9314,localhost:9315,localhost:9311|"
                + "2,localhost:9322,localhost:9323,localhost:9324,localhost:9325,localhost:9321|",
            SequencerNode.buildClusterMembers(3));
    }
}
