package org.limitless.seqeron.replayer.server;

import java.util.ArrayList;
import java.util.List;
import java.util.function.LongPredicate;

/**
 * Admission control and pending-request bookkeeping behind {@link ReplayerService}'s {@code
 * MAX_CONCURRENT_REPLAYS} cap, free of archive and Aeron calls. The {@code token} it is handed is opaque.
 * The pending list holds at most one request per client, since a waiting client resends every ~500ms.
 */
public final class ReplaySlotAllocator {
    /** {@link #supersede} uses this value for "no active slot for that client". */
    public static final long NO_SLOT = -1;

    /** One pending request waiting for a free slot, in request order. */
    public record PendingRequest(long clientId, long requestId, long fromPosition, int queueId) { }

    private record ActiveSlot(long clientId, long token) { }

    private int maxConcurrent;
    private final List<ActiveSlot> active = new ArrayList<>();
    private final List<PendingRequest> pending = new ArrayList<>();

    public ReplaySlotAllocator(final int maxConcurrent) {
        this.maxConcurrent = maxConcurrent;
    }

    public int activeCount() {
        return active.size();
    }

    public int pendingCount() {
        return pending.size();
    }

    public boolean hasCapacity() {
        return active.size() < maxConcurrent;
    }

    /**
     * Sets how many replays may be active at once. Lowering it starts nothing new until enough have ended; it
     * stops none.
     * @param maxConcurrent the new limit
     */
    public void limit(final int maxConcurrent) {
        this.maxConcurrent = maxConcurrent;
    }

    /** Removes clientId's active slot, if any (a re-request supersedes it); returns its token, or {@link #NO_SLOT}. */
    public long supersede(final long clientId) {
        for (int i = 0; i < active.size(); i++) {
            if (active.get(i).clientId() == clientId) {
                return active.remove(i).token();
            }
        }
        return NO_SLOT;
    }

    /** Records a newly-started replay as occupying a slot. */
    public void activate(final long clientId, final long token) {
        active.add(new ActiveSlot(clientId, token));
    }

    /**
     * Holds a request that found no free slot, replacing any the client already has pending in place, so a
     * resend neither accretes entries nor loses the client its turn.
     * @param clientId client identity
     * @param requestId the request to answer when it is finally served
     * @param fromPosition start replay position, or {@code ReplayProtocol.FROM_START}
     * @param queueId the queue it is for
     */
    public void addPending(final long clientId, final long requestId, final long fromPosition, final int queueId) {
        final PendingRequest request = new PendingRequest(clientId, requestId, fromPosition, queueId);
        for (int i = 0; i < pending.size(); i++) {
            if (pending.get(i).clientId() == clientId) {
                pending.set(i, request);
                return;
            }
        }
        pending.add(request);
    }

    /**
     * Drops the client's pending request, if any, as its current one is answered: left behind, the stale
     * one would later take a slot the client never uses.
     * @param clientId client identity
     */
    public void cancelPending(final long clientId) {
        for (int i = 0; i < pending.size(); i++) {
            if (pending.get(i).clientId() == clientId) {
                pending.remove(i);
                return;
            }
        }
    }

    /**
     * Pops the next pending request, or null if none is pending or every slot is taken. Callers bound a
     * drain pass to {@link #pendingCount()}, so a request that is held again is retried once per pass.
     * @return the longest-waiting client's current request, or null
     */
    public PendingRequest pollPending() {
        return hasCapacity() && !pending.isEmpty() ? pending.removeFirst() : null;
    }

    /**
     * Removes every slot whose replay has ended.
     * @param isLive whether the replay a token names is still running
     * @return how many were removed
     */
    public int reclaimEnded(final LongPredicate isLive) {
        int removed = 0;
        for (int i = active.size() - 1; i >= 0; i--) {
            if (!isLive.test(active.get(i).token())) {
                active.remove(i);
                ++removed;
            }
        }
        return removed;
    }

    /** Removes every active slot (shutdown), returning their tokens. */
    public List<Long> clear() {
        final List<Long> tokens = new ArrayList<>(active.size());
        for (final ActiveSlot slot : active) {
            tokens.add(slot.token());
        }
        active.clear();
        return tokens;
    }
}
