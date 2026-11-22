package org.limitless.seqeron.app;

import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import org.limitless.seqeron.protocol.SystemFrame;

/**
 * The connections one gateway instance holds, the lifecycle frames it still owes the log for them, and what a payload
 * may be published on. I/O-free: {@link Gateway} places the frames and asks {@link GatewayLifecycle} nothing more.
 *
 * <p>Single-threaded. The C# twin is {@code App/GatewayConnections.cs}; keep the two in step.
 */
final class GatewayConnections {
    /** Places one connection lifecycle frame. */
    interface Placer {
        /** @return whether it landed; false stops the drain, to be retried in order */
        boolean place(int connectionId, int systemEventType, byte[] connectionData);
    }

    private static final byte[] NO_DATA = new byte[0];

    private final GatewayLifecycle lifecycle;

    /** Lifecycle frames still to be placed, in the order they were asked for. */
    private final ArrayDeque<Entry> backlog = new ArrayDeque<>();

    /** Connections whose {@code ConnectionOpened} has not landed: nothing may be published on one yet. */
    private final Set<Integer> unopened = new HashSet<>();

    /** Connections taken and not yet closed while serving, in the order taken; the connections gauge. */
    private final Set<Integer> held = new LinkedHashSet<>();

    private int nextConnectionId;

    GatewayConnections(final GatewayLifecycle lifecycle) {
        this.lifecycle = lifecycle;
    }

    /** Where allocation resumes for an activation: past every id this logical gateway's history holds. */
    void resumeAt(final int firstConnectionId) {
        nextConnectionId = firstConnectionId;
    }

    /** The id {@link #open} allocates next. */
    int nextConnectionId() {
        return nextConnectionId;
    }

    /**
     * Allocates a connection id and queues its {@code ConnectionOpened}.
     * @return the id, or {@link Gateway#NO_CONNECTION} if this instance is not serving
     */
    int open(final byte[] connectionData) {
        if (!lifecycle.onSessionAcquired()) {
            return Gateway.NO_CONNECTION;
        }
        final int connectionId = nextConnectionId++;
        backlog.add(new Entry(connectionId, SystemFrame.CONNECTION_OPENED, connectionData));
        unopened.add(connectionId);
        held.add(connectionId);
        return connectionId;
    }

    /** Queues a connection's {@code ConnectionClosed}; one the log has not seen opened is dropped instead. */
    void close(final int connectionId) {
        if (!lifecycle.isServing()) {
            // Not serving: the connections already went with the edge.
            return;
        }
        held.remove(connectionId);
        if (unopened.remove(connectionId)) {
            backlog.removeIf(entry -> entry.connectionId() == connectionId);
            return;
        }
        backlog.add(new Entry(connectionId, SystemFrame.CONNECTION_CLOSED, NO_DATA));
    }

    /**
     * The edge closed under a designation that stands. No {@code GatewayStarted} follows to release the connections,
     * so each one the log has seen opened is closed on it, and one it has not is never announced.
     */
    void onEdgeClosed() {
        backlog.removeIf(entry -> unopened.contains(entry.connectionId()));
        for (final int connectionId : held) {
            if (!unopened.contains(connectionId)) {
                backlog.add(new Entry(connectionId, SystemFrame.CONNECTION_CLOSED, NO_DATA));
            }
        }
        unopened.clear();
        held.clear();
    }

    /** Stood down: the connections go with the edge, and a successor's {@code GatewayStarted} releases them. */
    void onStandby() {
        backlog.clear();
        unopened.clear();
        held.clear();
    }

    /**
     * Whether a payload may be published on {@code connectionId} now: serving, and its {@code ConnectionOpened}
     * landed.
     */
    boolean mayPublish(final int connectionId) {
        return lifecycle.isServing() && !unopened.contains(connectionId);
    }

    /** Connections held open. */
    int count() {
        return held.size();
    }

    /**
     * Places what is owed, oldest first, stopping at the first frame that does not land.
     * @return frames placed
     */
    int drain(final Placer placer) {
        int work = 0;
        while (!backlog.isEmpty()) {
            final Entry next = backlog.peek();
            if (!placer.place(next.connectionId(), next.systemEventType(), next.connectionData())) {
                break;
            }
            backlog.poll();
            unopened.remove(next.connectionId());
            work++;
        }
        return work;
    }

    /** One connection lifecycle frame waiting to be placed. */
    private record Entry(int connectionId, int systemEventType, byte[] connectionData) { }
}
