package org.limitless.seqeron.app;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.limitless.seqeron.app.GatewayLifecycle.State;

/**
 * Drives {@link GatewayLifecycle} directly; the races it covers are too narrow for an end-to-end script.
 * Merged from phixeron's acceptor and initiator tests. Topology: {@code GW-A} (id 5) and {@code GW-B} (id 6)
 * under {@code gatewaySourceId} 6, beside another pair (ids 1 and 2) under 0.
 *
 * <p>Case for case with {@code GatewayLifecycleTest.cpp}.
 */
class GatewayLifecycleTest {
    private static final String ME = "GW-A";
    private static final int MY_ID = 5;
    private static final int SIBLING_ID = 6;
    private static final int MY_SOURCE_ID = 6;
    private static final int OTHER_PAIR_ID = 1;
    private static final int OTHER_PAIR_SOURCE_ID = 0;

    private static final class RecordingActions implements GatewayLifecycle.Actions {
        private final List<String> calls = new ArrayList<>();
        private boolean gatewayStartedLands = true;
        private boolean gateOpens = true;

        @Override
        public void identityResolved(final int gatewayId, final int gatewaySourceId, final int preferenceRank) {
            calls.add("identity(" + gatewayId + "," + gatewaySourceId + ")");
        }

        @Override
        public boolean publishGatewayStarted(final int gatewayId) {
            calls.add("GatewayStarted(" + gatewayId + ")");
            return gatewayStartedLands;
        }

        @Override
        public boolean openGate() {
            calls.add("open");
            return gateOpens;
        }

        @Override
        public void closeGate() {
            calls.add("close");
        }

        long count(final String call) {
            return calls.stream().filter(call::equals).count();
        }
    }

    private RecordingActions actions;
    private GatewayLifecycle lifecycle;

    @BeforeEach
    void setUp() {
        actions = new RecordingActions();
        lifecycle = new GatewayLifecycle(ME, actions);
    }

    private void loadTopology() {
        lifecycle.onGatewayRegistered(OTHER_PAIR_ID, OTHER_PAIR_SOURCE_ID, "OTHER-A", 0);
        lifecycle.onGatewayRegistered(2, OTHER_PAIR_SOURCE_ID, "OTHER-B", 1);
        lifecycle.onGatewayRegistered(MY_ID, MY_SOURCE_ID, ME, 0);
        lifecycle.onGatewayRegistered(SIBLING_ID, MY_SOURCE_ID, "GW-B", 1);
    }

    private void becomeServing() {
        loadTopology();
        lifecycle.onGatewayActive(MY_ID);
        lifecycle.onCaughtUp();
        lifecycle.advance();
    }

    @Test
    void resolvesItsIdentityFromTheRowNamingIt() {
        loadTopology();
        assertEquals(MY_ID, lifecycle.gatewayId());
        assertEquals(MY_SOURCE_ID, lifecycle.gatewaySourceId());
        assertEquals(List.of("identity(" + MY_ID + "," + MY_SOURCE_ID + ")"), actions.calls);
    }

    @Test
    void ignoresAnActivationBeforeAnyRowNamesThisInstance() {
        lifecycle.onGatewayActive(MY_ID);
        loadTopology();
        lifecycle.onCaughtUp();
        assertFalse(lifecycle.isActivated());
        assertEquals(0, lifecycle.advance());
    }

    @Test
    void anActivationSeenWhileReplayingOpensOnlyOnceCaughtUp() {
        loadTopology();
        lifecycle.onGatewayActive(MY_ID);
        assertEquals(State.REPLAYING, lifecycle.state());
        assertEquals(0, lifecycle.advance());
        assertEquals(0, actions.count("open"));

        assertTrue(lifecycle.onCaughtUp());
        assertFalse(lifecycle.onCaughtUp());
        assertEquals(1, lifecycle.advance());
        assertEquals(State.SERVING, lifecycle.state());
    }

    @Test
    void staysShutWhenCaughtUpButNotActivated() {
        loadTopology();
        lifecycle.onCaughtUp();
        assertEquals(State.PASSIVE, lifecycle.state());
        assertEquals(0, lifecycle.advance());
        assertFalse(lifecycle.isServing());
        assertEquals(0, actions.count("open"));
    }

    @Test
    void publishesGatewayStartedBeforeOpeningTheGate() {
        becomeServing();
        assertTrue(lifecycle.isServing());
        assertEquals(List.of("GatewayStarted(" + MY_ID + ")", "open"), actions.calls.subList(1, 3));
    }

    @Test
    void doesNotOpenTheGateWhileGatewayStartedIsBackPressured() {
        actions.gatewayStartedLands = false;
        becomeServing();
        assertEquals(State.PASSIVE, lifecycle.state());
        assertEquals(0, actions.count("open"));

        actions.gatewayStartedLands = true;
        assertEquals(1, lifecycle.advance());
        assertEquals(State.SERVING, lifecycle.state());
    }

    @Test
    void retriesAGateThatDidNotOpenWithoutRegisteringAgain() {
        actions.gateOpens = false;
        becomeServing();
        assertEquals(State.PASSIVE, lifecycle.state());

        actions.gateOpens = true;
        assertEquals(1, lifecycle.advance());
        assertEquals(State.SERVING, lifecycle.state());
        assertEquals(1, actions.count("GatewayStarted(" + MY_ID + ")"), "once per activation, not per attempt");
        assertEquals(2, actions.count("open"));
    }

    @Test
    void aGateThatClosesByItselfReopensWithoutRegisteringAgain() {
        becomeServing();
        lifecycle.onGateClosed();
        assertEquals(State.PASSIVE, lifecycle.state());
        assertEquals(0, actions.count("close"), "the gate is already closed");

        assertEquals(1, lifecycle.advance());
        assertEquals(1, actions.count("GatewayStarted(" + MY_ID + ")"));
        assertEquals(2, actions.count("open"));
    }

    @Test
    void standsDownWhenAGatewayActiveNamesTheSibling() {
        becomeServing();
        lifecycle.onGatewayActive(SIBLING_ID);
        assertEquals(State.PASSIVE, lifecycle.state());
        assertFalse(lifecycle.isActivated());
        assertEquals(1, actions.count("close"));
        assertEquals(0, lifecycle.advance());
    }

    @Test
    void reopensOnASecondDesignationAsANewEpoch() {
        becomeServing();
        lifecycle.onGatewayActive(SIBLING_ID);
        lifecycle.onGatewayActive(MY_ID);
        assertEquals(1, lifecycle.advance());
        assertEquals(State.SERVING, lifecycle.state());
        assertEquals(2, actions.count("GatewayStarted(" + MY_ID + ")"));
    }

    @Test
    void ignoresAnotherLogicalGatewaysElection() {
        becomeServing();
        lifecycle.onGatewayActive(OTHER_PAIR_ID);
        assertEquals(State.SERVING, lifecycle.state());
        assertTrue(lifecycle.isActivated());
        assertEquals(0, actions.count("close"));
    }

    @Test
    void ignoresAnActivationForAnUnknownGateway() {
        becomeServing();
        lifecycle.onGatewayActive(99);
        assertEquals(State.SERVING, lifecycle.state());
        assertTrue(lifecycle.isActivated());
    }

    @Test
    void aSupersededActivationReplayedOnRestartLeavesItPassive() {
        loadTopology();
        lifecycle.onGatewayActive(MY_ID); // history: this instance once served
        lifecycle.onGatewayActive(SIBLING_ID); // and was replaced
        lifecycle.onCaughtUp();
        assertEquals(0, lifecycle.advance());
        assertEquals(State.PASSIVE, lifecycle.state());
        assertEquals(0, actions.count("close"), "never opened, so nothing to close");
        assertEquals(0, actions.count("GatewayStarted(" + MY_ID + ")"));
    }

    @Test
    void refusesASessionAcquiredWhileTheGateIsShut() {
        loadTopology();
        lifecycle.onCaughtUp();
        assertFalse(lifecycle.onSessionAcquired());

        lifecycle.onGatewayActive(MY_ID);
        lifecycle.advance();
        assertTrue(lifecycle.onSessionAcquired());

        lifecycle.onGatewayActive(SIBLING_ID); // the gate closed mid-logon
        assertFalse(lifecycle.onSessionAcquired());
    }
}
