package org.limitless.seqeron.app;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
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
        return Application.builder().sourceId(SOURCE_ID).clientId(1).listener(listener());
    }

    @Test
    @DisplayName("a client id and a listener are required; the egress channel is not")
    void requiresClientIdAndListener() {
        assertEquals("clientId", assertThrows(NullPointerException.class,
            () -> Application.builder().listener(listener()).build()).getMessage());
        assertEquals("listener", assertThrows(NullPointerException.class,
            () -> Application.builder().clientId(1).build()).getMessage());
        assertEquals("egressChannel", assertThrows(NullPointerException.class,
            () -> complete().egressChannel(null).build()).getMessage());
        assertEquals("ingressEndpoints", assertThrows(NullPointerException.class,
            () -> complete().ingressEndpoints(null).build()).getMessage());
    }

    @Test
    @DisplayName("snapshots need both a listener and a directory to keep its files in")
    void snapshotsNeedListenerAndDirectory() {
        final SnapshotListener snapshotListener = (SnapshotListener) Proxy.newProxyInstance(
            SnapshotListener.class.getClassLoader(), new Class<?>[] {SnapshotListener.class},
            (proxy, method, args) -> null);
        assertEquals("snapshotDirectory", assertThrows(NullPointerException.class,
            () -> complete().snapshots(snapshotListener, null)).getMessage());
        assertEquals("snapshotListener", assertThrows(NullPointerException.class,
            () -> complete().snapshots(null, Path.of("unused"))).getMessage());
        assertEquals(SOURCE_ID, complete().snapshots(snapshotListener, Path.of("unused")).build().sourceId());
    }

    @Test
    @DisplayName("every option is accepted and the source id is the one given")
    void acceptsEveryOption() {
        final Application application = complete().addressedGroups("A", "B").clientId(3).memberId(1)
            .egressChannel(EGRESS).ingressEndpoints("0=localhost:1")
            .tuning(Tuning.DEFAULT.withPendingCapacity(16).withQueueStallTimeoutMs(2000).withRecoveryStallTimeoutMs(3000))
            .ipcConnectTimeoutMs(4000).outstandingWork(new OutstandingWork<Long, byte[]>())
            .build();
        assertEquals(SOURCE_ID, application.sourceId());
    }

    @Test
    @DisplayName("off the cluster there is no IPC ingress to time, so the two options are rejected together")
    void offClusterRejectsAnIpcTimeout() {
        assertEquals(SOURCE_ID, complete().offCluster(true).memberId(9).build().sourceId());
        assertEquals("ipcConnectTimeoutMs has no meaning with offCluster", assertThrows(IllegalStateException.class,
            () -> complete().offCluster(true).ipcConnectTimeoutMs(4000).build()).getMessage());
        assertEquals(SOURCE_ID, complete().ipcConnectTimeoutMs(4000).build().sourceId());
    }

    @Test
    @DisplayName("a closing gate clears the tracker's dispatches before the listener hears false; an opening one not")
    void closingTheGateDischargesTheTrackerFirst() {
        final OutstandingWork<Long, String> work = new OutstandingWork<>();
        final List<String> seen = new ArrayList<>();
        final Application.Listener recording = (Application.Listener) Proxy.newProxyInstance(
            Application.Listener.class.getClassLoader(), new Class<?>[] {Application.Listener.class},
            (proxy, method, args) -> {
                if (method.getName().equals("onLeadershipChanged")) {
                    seen.add(args[0] + ":" + work.isDispatched(1L));
                }
                return null;
            });
        final Application application = complete().listener(recording).outstandingWork(work).build();
        work.onRequest(1L, "request");
        assertEquals(1, work.dispatchUndispatched((key, request) -> true));

        application.gateEdge(true);
        assertTrue(work.isDispatched(1L));
        application.gateEdge(false);
        assertFalse(work.isDispatched(1L));
        assertEquals(List.of("true:true", "false:false"), seen);
    }

    @Test
    @DisplayName("before it has caught up a replica is not leading, has seen nothing, and declines every publish")
    void declinesWhileTheGateIsShut() {
        final Application application = complete().memberId(1).build();
        final UnsafeBuffer payload = new UnsafeBuffer(new byte[32]);
        assertFalse(application.isLeading());
        assertFalse(application.isCaughtUp());
        assertEquals(Publish.Declined, application.publish(1, 5, payload, 16));
        assertEquals(Publish.Declined, application.reply(new ReplyAddress(2, 9, 1), 5, payload, 16));
    }
}
