package org.limitless.seqeron.app;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.limitless.seqeron.protocol.SystemFrame;

/**
 * Drives {@link GatewayConnections} over a real {@link GatewayLifecycle}: {@code GW-A} (id 5) and {@code GW-B} (id 6)
 * under {@code gatewaySourceId} 6.
 *
 * <p>Case for case with {@code GatewayConnectionsTest.cs}.
 */
class GatewayConnectionsTest {
    private static final int MY_ID = 5;
    private static final int SIBLING_ID = 6;
    private static final int SOURCE_ID = 6;
    private static final int FIRST_ID = 10;

    private static final class Actions implements GatewayLifecycle.Actions {
        GatewayConnections connections;

        @Override
        public boolean publishGatewayStarted(final int gatewayId) {
            connections.resumeAt(FIRST_ID);
            return true;
        }

        @Override
        public boolean openGate() {
            return true;
        }

        @Override
        public void closeGate() {
            connections.onStandby();
        }
    }

    /** Records each frame placed, refusing every one once {@link #lands} is false. */
    private static final class Log implements GatewayConnections.Placer {
        final List<String> placed = new ArrayList<>();
        boolean lands = true;

        @Override
        public boolean place(final int connectionId, final int systemEventType, final byte[] connectionData) {
            if (!lands) {
                return false;
            }
            placed.add((systemEventType == SystemFrame.CONNECTION_OPENED ? "opened " : "closed ") + connectionId);
            return true;
        }
    }

    private GatewayLifecycle lifecycle;
    private GatewayConnections connections;
    private Log log;

    @BeforeEach
    void setUp() {
        final Actions actions = new Actions();
        lifecycle = new GatewayLifecycle("GW-A", actions);
        connections = new GatewayConnections(lifecycle);
        actions.connections = connections;
        log = new Log();
        lifecycle.onGatewayRegistered(MY_ID, SOURCE_ID, "GW-A", 0);
        lifecycle.onGatewayRegistered(SIBLING_ID, SOURCE_ID, "GW-B", 1);
        lifecycle.onCaughtUp();
    }

    private void serve() {
        lifecycle.onGatewayActive(MY_ID);
        assertEquals(1, lifecycle.advance());
    }

    @Test
    void aStandbyPublishesNothingOnAConnectionOrOffOne() {
        assertFalse(connections.mayPublish(Gateway.NO_CONNECTION));
        assertEquals(Gateway.NO_CONNECTION, connections.open(new byte[0]));

        lifecycle.onGatewayActive(SIBLING_ID);
        assertFalse(connections.mayPublish(Gateway.NO_CONNECTION));

        serve();
        assertTrue(connections.mayPublish(Gateway.NO_CONNECTION));

        lifecycle.onGatewayActive(SIBLING_ID);
        assertFalse(connections.mayPublish(Gateway.NO_CONNECTION));
    }

    @Test
    void aPayloadWaitsForItsConnectionsConnectionOpened() {
        serve();
        final int id = connections.open(new byte[0]);
        assertEquals(FIRST_ID, id);
        assertFalse(connections.mayPublish(id));
        assertEquals(1, connections.drain(log));
        assertTrue(connections.mayPublish(id));
        assertEquals(List.of("opened 10"), log.placed);
    }

    @Test
    void aConnectionClosedBeforeItsConnectionOpenedLandedIsNeverAnnounced() {
        serve();
        final int id = connections.open(new byte[0]);
        connections.close(id);
        assertEquals(0, connections.drain(log));
        assertEquals(0, connections.count());
    }

    @Test
    void theDrainStopsAtTheFirstFrameThatDoesNotLandAndResumesInOrder() {
        serve();
        final int first = connections.open(new byte[0]);
        connections.open(new byte[0]);
        log.lands = false;
        assertEquals(0, connections.drain(log));
        log.lands = true;
        connections.close(first);
        assertEquals(1, connections.drain(log), "the unplaced open of the first is dropped with it");
        assertEquals(List.of("opened 11"), log.placed);
    }

    @Test
    void theEdgeClosingClosesEveryConnectionTheLogSawAndAnnouncesNoneItHasNot() {
        serve();
        final int seen = connections.open(new byte[0]);
        final int closed = connections.open(new byte[0]);
        connections.drain(log);
        connections.close(closed);
        final int unseen = connections.open(new byte[0]);

        lifecycle.onGateClosed();
        connections.onEdgeClosed();
        connections.close(seen); // after the edge has gone: nothing more to say
        connections.drain(log);

        assertEquals(List.of("opened " + seen, "opened " + closed, "closed " + closed, "closed " + seen), log.placed);
        assertFalse(connections.mayPublish(seen));
        assertFalse(connections.mayPublish(unseen));
        assertFalse(connections.mayPublish(Gateway.NO_CONNECTION));
        assertEquals(0, connections.count());
    }

    @Test
    void aReopenedEdgeAllocatesPastTheIdsTheClosedOneTook() {
        serve();
        connections.open(new byte[0]);
        lifecycle.onGateClosed();
        connections.onEdgeClosed();

        assertEquals(1, lifecycle.advance(), "the designation stands: the gate reopens without a GatewayStarted");
        assertEquals(FIRST_ID + 1, connections.open(new byte[0]));
    }

    @Test
    void standingDownDropsWhatIsOwed() {
        serve();
        connections.open(new byte[0]);
        lifecycle.onGatewayActive(SIBLING_ID);
        assertEquals(0, connections.drain(log));
        assertEquals(0, connections.count());
    }
}
