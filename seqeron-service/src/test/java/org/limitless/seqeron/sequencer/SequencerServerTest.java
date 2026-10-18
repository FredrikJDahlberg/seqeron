package org.limitless.seqeron.sequencer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.cluster.client.ClusterEvent;
import java.util.List;
import org.agrona.concurrent.AgentTerminationException;
import org.junit.jupiter.api.Test;
import org.limitless.seqeron.protocol.PortLayout;

class SequencerServerTest {
    /** Aeron 1.53.2's truncation event, its positions filled in. */
    private static ClusterEvent truncation(final long logPosition, final long newPosition) {
        return new ClusterEvent("Truncating Cluster Log - memberId=2 state=CANVASS this.logLeadershipTermId=4 "
                                + "this.leadershipTermId=4 this.candidateTermId=5 this.commitPosition=" + logPosition +
                                " this.logPosition=" + logPosition + " this.appendPosition=192 oldPosition=192"
                                + " newPosition=" + newPosition);
    }

    @Test
    void truncatingBelowTheAppliedPositionIsFatal() {
        assertTrue(SequencerServer.truncatesAppliedLog(truncation(192, 96)));
    }

    @Test
    void truncatingOnlyWhatWasNotAppliedIsNot() {
        assertFalse(SequencerServer.truncatesAppliedLog(truncation(96, 96)));
        assertFalse(
            SequencerServer.truncatesAppliedLog(new ClusterEvent("new leader detected due to commit position")));
        assertFalse(SequencerServer.truncatesAppliedLog(new IllegalStateException(truncation(192, 96).getMessage())));
    }

    @Test
    void anErrorIsLoggedWithEveryCauseBeneathIt() {
        final IllegalStateException why = new IllegalStateException("the co-located archive is unreachable");
        assertEquals("org.agrona.concurrent.AgentTerminationException: failed to start service=0; caused by "
                         + "java.lang.IllegalStateException: the co-located archive is unreachable",
                     SequencerServer.describe(new AgentTerminationException("failed to start service=0", why)));
        assertEquals("java.lang.IllegalStateException: the co-located archive is unreachable",
                     SequencerServer.describe(why));
    }

    @Test
    void aCauseChainThatCyclesIsCutShort() {
        final RuntimeException first = new RuntimeException("first");
        final RuntimeException second = new RuntimeException("second", first);
        first.initCause(second);

        assertEquals(8, SequencerServer.describe(first).split("; caused by ", -1).length - 1);
    }

    @Test
    void singleNodeMembersMatchDocumentedLayout() {
        assertEquals("0,localhost:9302,localhost:9303,localhost:9304,localhost:9305,localhost:9301|",
                     SequencerServer.buildClusterMembers(List.of("localhost")));
    }

    @Test
    void ingressEndpointMatchesDocumentedLayout() {
        assertEquals("localhost:9302", PortLayout.ingressEndpoint(0));
        assertEquals("localhost:9312", PortLayout.ingressEndpoint(1));
    }

    // The reservation is wider than what seven members bind, and applications check themselves against it
    // (doc/ops.md, "Ports"). Pinned here, and in PortLayoutTest, so it cannot quietly narrow to 9365.
    @Test
    void reservedBlockCoversSevenMemberStrides() {
        assertEquals(9300, PortLayout.CLUSTER_PORT_BLOCK_FIRST);
        assertEquals(9369, PortLayout.CLUSTER_PORT_BLOCK_LAST);

        assertTrue(PortLayout.isClusterPort(9300));
        assertTrue(PortLayout.isClusterPort(9360)); // member 6's base — reserved though unbound
        assertTrue(PortLayout.isClusterPort(9369));
        assertFalse(PortLayout.isClusterPort(9299));
        assertFalse(PortLayout.isClusterPort(9370));
    }

    @Test
    void threeNodeMembersMatchDocumentedLayout() {
        assertEquals("0,localhost:9302,localhost:9303,localhost:9304,localhost:9305,localhost:9301|"
                         + "1,localhost:9312,localhost:9313,localhost:9314,localhost:9315,localhost:9311|"
                         + "2,localhost:9322,localhost:9323,localhost:9324,localhost:9325,localhost:9321|",
                     SequencerServer.buildClusterMembers(List.of("localhost", "localhost", "localhost")));
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
        final List<String> three = List.of("h0", "h1", "h2");
        assertEquals(three, SequencerServer.resolveHosts("h0,h1,h2", true, List.of(), 2));
        assertThrows(IllegalArgumentException.class,
                     () -> SequencerServer.resolveHosts("h0,h1", true, List.of(), 2));
        assertThrows(IllegalArgumentException.class,
                     () -> SequencerServer.resolveHosts("h0,h1", true, List.of(), -1));
    }

    @Test
    void propertyWinsOverEnvironmentWhichWinsOverOneLocalMember() {
        final List<String> env = List.of("e0", "e1");
        assertEquals(List.of("h0"), SequencerServer.resolveHosts("h0", true, env, 0));
        assertEquals(env, SequencerServer.resolveHosts(null, true, env, 1));
        assertEquals(List.of("localhost"), SequencerServer.resolveHosts(null, false, List.of(), 0));
        assertThrows(IllegalArgumentException.class,
                     () -> SequencerServer.resolveHosts(null, false, List.of(), 1));
    }

    @Test
    void multiMemberHostListRequiresABaseDir() {
        assertThrows(IllegalArgumentException.class,
                     () -> SequencerServer.resolveHosts("h0,h1", false, List.of(), 0));
        assertThrows(IllegalArgumentException.class,
                     () -> SequencerServer.resolveHosts(null, false, List.of("e0", "e1"), 0));
        assertEquals(List.of("h0"), SequencerServer.resolveHosts("h0", false, List.of(), 0));
    }
}
