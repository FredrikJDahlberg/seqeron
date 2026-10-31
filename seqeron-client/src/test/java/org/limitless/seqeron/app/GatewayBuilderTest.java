package org.limitless.seqeron.app;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.lang.reflect.Proxy;
import java.nio.file.Path;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** The façade's construction rules and what an instance that has not been activated answers. No Aeron is started. */
class GatewayBuilderTest {
    private static final String EGRESS = "aeron:udp?endpoint=localhost:0";

    private static Gateway.Listener listener() {
        return (Gateway.Listener) Proxy.newProxyInstance(
            Gateway.Listener.class.getClassLoader(), new Class<?>[] {Gateway.Listener.class},
            (proxy, method, args) -> boolean.class == method.getReturnType() ? Boolean.FALSE : null);
    }

    private static SnapshotListener snapshotListener() {
        return (SnapshotListener) Proxy.newProxyInstance(
            SnapshotListener.class.getClassLoader(), new Class<?>[] {SnapshotListener.class},
            (proxy, method, args) -> null);
    }

    private static Gateway.Builder complete() {
        return Gateway.builder().gatewayName("GW-A").clientId(1).listener(listener());
    }

    @Test
    @DisplayName("a name, a client id and a listener are required; the egress channel is not")
    void requiresNameClientIdAndListener() {
        assertEquals("gatewayName", assertThrows(NullPointerException.class,
            () -> Gateway.builder().clientId(1).listener(listener()).build()).getMessage());
        assertEquals("clientId", assertThrows(NullPointerException.class,
            () -> Gateway.builder().gatewayName("GW-A").listener(listener()).build()).getMessage());
        assertEquals("listener", assertThrows(NullPointerException.class,
            () -> Gateway.builder().gatewayName("GW-A").clientId(1).build()).getMessage());
        assertEquals("egressChannel", assertThrows(NullPointerException.class,
            () -> complete().egressChannel(null).build()).getMessage());
        assertEquals("ingressEndpoints", assertThrows(NullPointerException.class,
            () -> complete().ingressEndpoints(null).build()).getMessage());
    }

    @Test
    @DisplayName("snapshots need a source id, a listener and a directory")
    void snapshotsNeedSourceId() {
        assertEquals("sourceId is required with snapshots", assertThrows(IllegalStateException.class,
            () -> complete().snapshots(snapshotListener(), Path.of("unused")).build()).getMessage());
        assertEquals("snapshotDirectory", assertThrows(NullPointerException.class,
            () -> complete().snapshots(snapshotListener(), null)).getMessage());
        assertEquals(Gateway.UNRESOLVED,
            complete().snapshots(snapshotListener(), Path.of("unused")).sourceId(4).build().sourceId());
    }

    @Test
    @DisplayName("every option is accepted, and the ids stay unresolved until a topology row names them")
    void acceptsEveryOption() {
        final Gateway gateway = complete().addressedGroups("A").sourceId(4).clientId(2).memberId(1)
            .ingressEndpoints("0=localhost:1").egressChannel(EGRESS)
            .tuning(Tuning.DEFAULT.withPendingCapacity(16).withQueueStallTimeoutMs(2000).withRecoveryStallTimeoutMs(3000))
            .build();
        assertEquals(Gateway.UNRESOLVED, gateway.sourceId());
        assertEquals(Gateway.UNRESOLVED, gateway.gatewayId());
        assertFalse(gateway.isServing());
    }

    @Test
    @DisplayName("an instance that is not serving accepts no connection and has nothing to close")
    void takesNoConnectionUntilServing() {
        final Gateway gateway = complete().build();
        final UnsafeBuffer data = new UnsafeBuffer(new byte[8]);
        assertFalse(gateway.canAccept());
        assertFalse(gateway.isActivated());
        assertFalse(gateway.isCaughtUp());
        assertEquals(Gateway.NO_CONNECTION, gateway.openConnection());
        assertEquals(Gateway.NO_CONNECTION, gateway.openConnection(data, 8));
        gateway.closeConnection(0);
        gateway.gateClosed();
        assertFalse(gateway.isServing());
    }
}
