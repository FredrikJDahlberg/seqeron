package org.limitless.seqeron.app;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.lang.reflect.Proxy;
import java.nio.file.Path;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.limitless.seqeron.protocol.Publish;

/** The façade's construction rules and what a replica that has not connected answers. No Aeron is started. */
class ApplicationBuilderTest {
    private static final int SOURCE_ID = 7;
    private static final String EGRESS = "aeron:udp?endpoint=localhost:0";

    private static Application.Listener listener() {
        return (Application.Listener) Proxy.newProxyInstance(
            Application.Listener.class.getClassLoader(), new Class<?>[] {Application.Listener.class},
            (proxy, method, args) -> null);
    }

    private static Application.Builder complete() {
        return Application.builder().sourceId(SOURCE_ID).egressChannel(EGRESS).listener(listener());
    }

    @Test
    @DisplayName("an egress channel and a listener are required")
    void requiresEgressAndListener() {
        assertEquals("egressChannel", assertThrows(NullPointerException.class,
            () -> Application.builder().listener(listener()).build()).getMessage());
        assertEquals("listener", assertThrows(NullPointerException.class,
            () -> Application.builder().egressChannel(EGRESS).build()).getMessage());
        assertEquals("ingressEndpoints", assertThrows(NullPointerException.class,
            () -> complete().ingressEndpoints(null).build()).getMessage());
    }

    @Test
    @DisplayName("a snapshot listener needs a directory to keep its files in")
    void snapshotListenerNeedsDirectory() {
        final SnapshotListener snapshotListener = (SnapshotListener) Proxy.newProxyInstance(
            SnapshotListener.class.getClassLoader(), new Class<?>[] {SnapshotListener.class},
            (proxy, method, args) -> null);
        final IllegalStateException error = assertThrows(IllegalStateException.class,
            () -> complete().snapshotListener(snapshotListener).build());
        assertEquals("snapshotDirectory is required with a snapshotListener", error.getMessage());
        assertEquals(SOURCE_ID, complete().snapshotDirectory(Path.of("unused")).build().sourceId());
    }

    @Test
    @DisplayName("every option is accepted and the source id is the one given")
    void acceptsEveryOption() {
        final Application application = complete().addressedGroups("A", "B").clientId(3).memberId(1).offCluster(true)
            .egressChannel(EGRESS).ingressEndpoints("0=localhost:1").pendingCapacity(16).queueStallTimeoutMs(2000)
            .recoveryStallTimeoutMs(3000).ipcConnectTimeoutMs(4000).build();
        assertEquals(SOURCE_ID, application.sourceId());
    }

    @Test
    @DisplayName("before it has caught up a replica is not leading, has seen nothing, and declines every publish")
    void declinesWhileTheGateIsShut() {
        final Application application = complete().memberId(1).build();
        final UnsafeBuffer payload = new UnsafeBuffer(new byte[32]);
        assertFalse(application.isLeading());
        assertFalse(application.isCaughtUp());
        assertEquals(Publish.Declined, application.publish(1, 5, payload, 16));
        assertEquals(Publish.Declined, application.reply(2, 9, 1, 5, payload, 16));
    }
}
