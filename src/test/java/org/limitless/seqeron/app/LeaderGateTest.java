package org.limitless.seqeron.app;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.limitless.seqeron.app.LeaderGate.Transition;

/**
 * Unit tests for the leader gate's edges. {@code (isCaughtUp(), currentLeaderMemberId())} is exactly what a
 * replica reads off its {@code ReplayerStreamReceiver} each duty cycle. Case for case with
 * {@code LeaderGateTest.cpp}.
 */
class LeaderGateTest {
    private static final int SELF = 1;
    private static final int OTHER = 2;
    private static final int NO_LEADER = -1;

    private final LeaderGate gate = new LeaderGate(SELF);

    @Test
    @DisplayName("starts closed")
    void startsClosed() {
        assertFalse(gate.isOpen());
        assertEquals(Transition.NONE, gate.update(false, NO_LEADER));
    }

    @Test
    @DisplayName("opens once when caught up and leader")
    void opensOnce() {
        assertEquals(Transition.OPENED, gate.update(true, SELF));
        assertTrue(gate.isOpen());
        assertEquals(Transition.NONE, gate.update(true, SELF), "an edge, not a level");
    }

    @Test
    @DisplayName("stays closed unless both hold")
    void staysClosedUnlessBothHold() {
        assertEquals(Transition.NONE, gate.update(false, SELF), "leader, but still recovering");
        assertEquals(Transition.NONE, gate.update(true, OTHER));
        assertEquals(Transition.NONE, gate.update(true, NO_LEADER));
        assertFalse(gate.isOpen());
    }

    @Test
    @DisplayName("closes when leadership moves away")
    void closesWhenLeadershipMovesAway() {
        gate.update(true, SELF);

        assertEquals(Transition.CLOSED, gate.update(true, OTHER));
        assertFalse(gate.isOpen());
        assertEquals(Transition.NONE, gate.update(true, OTHER));
    }

    @Test
    @DisplayName("closes when a leader falls behind, and reopens when it catches up")
    void closesWhenFallingBehind() {
        gate.update(true, SELF);

        assertEquals(Transition.CLOSED, gate.update(false, SELF));
        assertEquals(Transition.OPENED, gate.update(true, SELF));
    }

    @Test
    @DisplayName("a leadership flip away and back between two updates closes the gate for one cycle")
    void flipBetweenUpdatesClosesForOneCycle() {
        gate.update(true, SELF);

        gate.onLeadershipChanged(); // SELF -> OTHER
        gate.onLeadershipChanged(); // OTHER -> SELF, both applied within one duty cycle

        assertEquals(Transition.CLOSED, gate.update(true, SELF), "a reply sent during that election may be lost");
        assertEquals(Transition.OPENED, gate.update(true, SELF));
        assertEquals(Transition.NONE, gate.update(true, SELF));
    }

    @Test
    @DisplayName("a leadership change while closed costs nothing")
    void leadershipChangeWhileClosed() {
        gate.onLeadershipChanged();

        assertEquals(Transition.OPENED, gate.update(true, SELF), "nothing was dispatched, so no cycle is lost");
    }
}
