package org.limitless.seqeron.app;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Leader-only request/reply work tracked across a failover. Every replica feeds it the same
 * {@code clusterSeqNo}-ordered stream, so all replicas hold the same outstanding set.
 *
 * <ul>
 *   <li><b>Outstanding is replicated:</b> a request is outstanding from its sequenced request
 *       ({@link #onRequest}) until its sequenced reply ({@link #onReply}).</li>
 *   <li><b>Dispatched is local:</b> "this node is handling it", cleared by {@link #onNotLeader()} and
 *       {@link #onReplyNotEmitted}. Only the leader dispatches.</li>
 * </ul>
 *
 * <p>Emission is at-least-once: a reply lost in an election is dispatched again by the next leader. Key on
 * the request's {@code clusterSeqNo} and make the reply a pure function of the request, so a re-emission is
 * identical to the one it may duplicate and consumers can drop it.
 *
 * <p>Dispatch runs in request order, which is {@code clusterSeqNo} order, so emission order is the same on
 * every replica. The C++ twin is {@code app/OutstandingWork.hpp}; keep the two in step.
 *
 * @param <K> the request key
 * @param <W> the work a dispatch needs
 */
public final class OutstandingWork<K, W> {
    /** A tracker holding no work. */
    public OutstandingWork() {
    }

    /** Takes one request; must not call back into the tracker. */
    @FunctionalInterface
    public interface Dispatcher<K, W> {
        /**
         * Takes one request.
         * @param key  the request's key
         * @param work what dispatching it needs
         * @return true if taken, false to stop the sweep (no capacity); the rest wait for the next call
         */
        boolean dispatch(K key, W work);
    }

    private static final class Entry<W> {
        W work;
        boolean dispatched; // local to this node

        Entry(final W work) {
            this.work = work;
        }
    }

    private final LinkedHashMap<K, Entry<W>> outstanding = new LinkedHashMap<>();

    /** A request seen on the sequenced stream, on every replica. Idempotent on the key, keeping its position. */
    public void onRequest(final K key, final W work) {
        final Entry<W> entry = outstanding.get(key);
        if (entry != null) {
            entry.work = work;
        } else {
            outstanding.put(key, new Entry<>(work));
        }
    }

    /** Its sequenced reply, on every replica: the request is answered. */
    public void onReply(final K key) {
        outstanding.remove(key);
    }

    /** The leader gate closed: forget what this node dispatched, so the next opening re-dispatches it. */
    public void onNotLeader() {
        for (final Entry<W> entry : outstanding.values()) {
            entry.dispatched = false;
        }
    }

    /** The reply offer failed, so no sequenced reply will come. The request stays outstanding. */
    public void onReplyNotEmitted(final K key) {
        final Entry<W> entry = outstanding.get(key);
        if (entry != null) {
            entry.dispatched = false;
        }
    }

    /**
     * Leader only, while the gate is open. Offers each undispatched outstanding request in request order.
     * @return how many were dispatched by this call
     */
    public int dispatchUndispatched(final Dispatcher<K, W> dispatcher) {
        int count = 0;
        for (final Map.Entry<K, Entry<W>> mapping : outstanding.entrySet()) {
            final Entry<W> entry = mapping.getValue();
            if (entry.dispatched) {
                continue;
            }
            if (!dispatcher.dispatch(mapping.getKey(), entry.work)) {
                break;
            }
            entry.dispatched = true;
            ++count;
        }
        return count;
    }

    /** Requests held, dispatched or not. */
    public int size() {
        return outstanding.size();
    }

    /** Whether this request has been dispatched and not yet completed. */
    public boolean isDispatched(final K key) {
        final Entry<W> entry = outstanding.get(key);
        return entry != null && entry.dispatched;
    }
}
