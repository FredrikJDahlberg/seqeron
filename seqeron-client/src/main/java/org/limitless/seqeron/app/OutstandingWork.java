package org.limitless.seqeron.app;

import java.util.Arrays;
import java.util.function.Supplier;

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
 * <p>Emission is at-least-once: a reply lost in an election is dispatched again by the next leader. The key is the
 * request's {@code clusterSeqNo}; make the reply a pure function of the request, so a re-emission is identical to the
 * one it may duplicate and consumers can drop it.
 *
 * <p>Dispatch runs in request order, which is {@code clusterSeqNo} order, so emission order is the same on every
 * replica. Each slot's work is made once by the factory and reused, so a warm tracker allocates nothing per request;
 * the slots double when more requests are outstanding than they hold. The C++ twin is {@code app/OutstandingWork.hpp};
 * keep the two in step.
 *
 * @param <W> the work a dispatch needs, filled in place
 */
public final class OutstandingWork<W> {
    /** Takes one request; must not call back into the tracker. */
    @FunctionalInterface
    public interface Dispatcher<W> {
        /**
         * Takes one request.
         * @param key  the request's {@code clusterSeqNo}
         * @param work what dispatching it needs
         * @return true if taken, false to stop the sweep (no capacity); the rest wait for the next call
         */
        boolean dispatch(long key, W work);
    }

    private static final int INITIAL_SLOTS = 16;
    private static final byte FREE = 0;
    private static final byte OUTSTANDING = 1;
    private static final byte DISPATCHED = 2; // local to this node

    private final Supplier<W> factory;
    // Slots [head, tail) in increasing key order; an answered one stays FREE until compacted away.
    private long[] keys = new long[INITIAL_SLOTS];
    private byte[] states = new byte[INITIAL_SLOTS];
    private Object[] works = new Object[INITIAL_SLOTS];
    private int head;
    private int tail;
    private int size;
    private long newestKey = Long.MIN_VALUE;

    /**
     * A tracker holding no work.
     * @param factory makes the work object of each slot, once
     */
    public OutstandingWork(final Supplier<W> factory) {
        this.factory = factory;
        for (int slot = 0; slot < INITIAL_SLOTS; ++slot) {
            works[slot] = factory.get();
        }
    }

    /**
     * A request seen on the sequenced stream, on every replica. Keys increase; one already outstanding is the same
     * request asserted again, and keeps its place.
     * @param key the request's {@code clusterSeqNo}
     * @return the request's work, to fill with what dispatching it needs; reused once the request is answered
     * @throws IllegalArgumentException for a key no greater than an earlier one that is no longer outstanding
     */
    public W onRequest(final long key) {
        if (key <= newestKey) {
            final int slot = find(key);
            if (slot < 0) {
                throw new IllegalArgumentException("request " + key + " is not outstanding and not after " + newestKey);
            }
            return work(slot);
        }
        if (tail == keys.length) {
            compact();
        }
        newestKey = key;
        keys[tail] = key;
        states[tail] = OUTSTANDING;
        ++size;
        return work(tail++);
    }

    /** Its sequenced reply, on every replica: the request is answered. */
    public void onReply(final long key) {
        final int slot = find(key);
        if (slot < 0) {
            return;
        }
        states[slot] = FREE;
        --size;
        while (head < tail && states[head] == FREE) {
            ++head;
        }
        if (head == tail) {
            head = 0;
            tail = 0;
        }
    }

    /** The leader gate closed: forget what this node dispatched, so the next opening re-dispatches it. */
    public void onNotLeader() {
        for (int slot = head; slot < tail; ++slot) {
            if (states[slot] == DISPATCHED) {
                states[slot] = OUTSTANDING;
            }
        }
    }

    /** The reply offer failed, so no sequenced reply will come. The request stays outstanding. */
    public void onReplyNotEmitted(final long key) {
        final int slot = find(key);
        if (slot >= 0) {
            states[slot] = OUTSTANDING;
        }
    }

    /**
     * Leader only, while the gate is open. Offers each undispatched outstanding request in request order.
     * @return how many were dispatched by this call
     */
    public int dispatchUndispatched(final Dispatcher<W> dispatcher) {
        int count = 0;
        for (int slot = head; slot < tail; ++slot) {
            if (states[slot] != OUTSTANDING) {
                continue;
            }
            if (!dispatcher.dispatch(keys[slot], work(slot))) {
                break;
            }
            states[slot] = DISPATCHED;
            ++count;
        }
        return count;
    }

    /** Requests held, dispatched or not. */
    public int size() {
        return size;
    }

    /** Whether this request has been dispatched and not yet completed. */
    public boolean isDispatched(final long key) {
        final int slot = find(key);
        return slot >= 0 && states[slot] == DISPATCHED;
    }

    @SuppressWarnings("unchecked")
    private W work(final int slot) {
        return (W)works[slot];
    }

    /** The slot holding this outstanding key, or -1. */
    private int find(final long key) {
        int low = head;
        int high = tail - 1;
        while (low <= high) {
            final int mid = (low + high) >>> 1;
            if (keys[mid] < key) {
                low = mid + 1;
            } else if (keys[mid] > key) {
                high = mid - 1;
            } else {
                return states[mid] == FREE ? -1 : mid;
            }
        }
        return -1;
    }

    /** Moves the outstanding slots to the front, swapping their work objects, and doubles if over half are left. */
    private void compact() {
        int to = 0;
        for (int from = head; from < tail; ++from) {
            if (states[from] == FREE) {
                continue;
            }
            if (from != to) {
                keys[to] = keys[from];
                states[to] = states[from];
                states[from] = FREE;
                final Object free = works[to];
                works[to] = works[from];
                works[from] = free;
            }
            ++to;
        }
        head = 0;
        tail = to;
        if (size > keys.length / 2) {
            final int slots = keys.length * 2;
            keys = Arrays.copyOf(keys, slots);
            states = Arrays.copyOf(states, slots);
            works = Arrays.copyOf(works, slots);
            for (int slot = slots / 2; slot < slots; ++slot) {
                works[slot] = factory.get();
            }
        }
    }
}
