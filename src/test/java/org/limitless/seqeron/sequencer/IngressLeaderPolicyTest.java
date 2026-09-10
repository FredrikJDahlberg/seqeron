package org.limitless.seqeron.sequencer;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the one thing a co-located producer must do about a new leader. No Aeron runtime: which
 * member leads and how this client reaches ingress are the two facts the sender reads off egress.
 */
class IngressLeaderPolicyTest {
    private static final int MY_MEMBER = 2;

    private final IngressLeaderPolicy policy = new IngressLeaderPolicy(MY_MEMBER);

    @Test
    @DisplayName("IPC ingress and the leader moves away: the session cannot follow it, so it is replaced")
    void ipcIngressLosesItsLeader() {
        policy.onConnected(true);
        policy.onNewLeader(0);
        assertTrue(policy.reconnectDue());
    }

    @Test
    @DisplayName("IPC ingress and this member is (still) the leader: nothing to do")
    void ipcIngressKeepsItsLeader() {
        policy.onConnected(true);
        policy.onNewLeader(MY_MEMBER);
        assertFalse(policy.reconnectDue());
    }

    @Test
    @DisplayName("UDP ingress never reconnects: the cluster client chases the leader itself")
    void udpIngressChasesItsOwnLeader() {
        policy.onConnected(false);
        policy.onNewLeader(0);
        assertFalse(policy.reconnectDue());
    }

    @Test
    @DisplayName("the reconnect clears the verdict: one leadership move is one replaced session")
    void reconnectingClearsTheVerdict() {
        policy.onConnected(true);
        policy.onNewLeader(0);
        assertTrue(policy.reconnectDue());
        policy.onConnected(false); // what reconnectOverUdp does once the new session is up
        assertFalse(policy.reconnectDue());
    }

    @Test
    @DisplayName("leadership coming back is not chased: swapping back would cost another session")
    void leadershipReturningIsNotChased() {
        policy.onConnected(true);
        policy.onNewLeader(0);
        policy.onConnected(false);
        policy.onNewLeader(MY_MEMBER);
        assertFalse(policy.reconnectDue());
    }

    @Test
    @DisplayName("a producer co-located with no member has nothing to decide")
    void nonColocatedProducerNeverReconnects() {
        final IngressLeaderPolicy remote = new IngressLeaderPolicy(IngressLeaderPolicy.NO_MEMBER);
        remote.onConnected(false);
        remote.onNewLeader(1);
        assertFalse(remote.reconnectDue());
    }
}
