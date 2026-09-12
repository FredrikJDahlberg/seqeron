package org.limitless.seqeron.sequencer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

/**
 * The port base is a deployment knob ({@code SEQERON_PORT_BASE}), and this pins the rules that decide
 * it. Driven through the pure seam rather than the environment: the base is read once into a static
 * field, so a test that set the variable would be testing whichever test ran first. The C++ twin is
 * {@code PortLayoutTest.cpp}'s {@code ParseClusterPortBase*} cases — same rules, same boundaries.
 *
 * <p>The (memberId -&gt; port) pairs the formula produces stay in {@code SequencerServerTest}.
 */
class PortLayoutTest {
    @Test
    void unsetOrBlankFallsBackToTheRegisteredBlock() {
        assertEquals(9300, PortLayout.resolveClusterPortBase(null));
        assertEquals(9300, PortLayout.resolveClusterPortBase(""));
        assertEquals(9300, PortLayout.resolveClusterPortBase("   "));
        assertEquals(PortLayout.DEFAULT_CLUSTER_PORT_BASE, PortLayout.resolveClusterPortBase(null));
    }

    @Test
    void aValidBaseIsTakenAsGiven() {
        assertEquals(20000, PortLayout.resolveClusterPortBase("20000"));
        assertEquals(20000, PortLayout.resolveClusterPortBase("  20000  "));
    }

    @Test
    void nonNumericIsRejectedRatherThanTreatedAsUnset() {
        assertThrows(IllegalArgumentException.class, () -> PortLayout.resolveClusterPortBase("9300x"));
        assertThrows(IllegalArgumentException.class, () -> PortLayout.resolveClusterPortBase("nine"));
    }

    // 1024 is the first unprivileged port, and the block is 30 wide, so 65506 is the last base that
    // fits. Both boundaries are pinned from either side.
    @Test
    void theBaseMustLeaveRoomForTheWholeBlockAndAvoidPrivilegedPorts() {
        assertThrows(IllegalArgumentException.class, () -> PortLayout.resolveClusterPortBase("1023"));
        assertEquals(1024, PortLayout.resolveClusterPortBase("1024"));

        assertEquals(65506, PortLayout.resolveClusterPortBase("65506"));
        assertThrows(IllegalArgumentException.class, () -> PortLayout.resolveClusterPortBase("65507"));
    }
}
