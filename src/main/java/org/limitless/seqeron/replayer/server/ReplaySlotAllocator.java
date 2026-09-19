package org.limitless.seqeron.replayer.server;

import java.util.ArrayList;
import java.util.List;

/**
 * Admission control and pending-queue bookkeeping behind {@link ReplayerService}'s {@code
 * MAX_CONCURRENT_REPLAYS} cap, free of archive and Aeron calls. The {@code token} it is handed is opaque.
 * The pending queue holds at most one request per client, since a waiting client resends every ~500ms.
 */
public final class ReplaySlotAllocator {
    /** {@link #supersede} uses this value for "no active slot for that client". */
    public static final long NO_SLOT = -1;

    /** One pending request waiting for a free slot, in request order. */
    public record PendingRequest(int clientId, long requestId, int segmentIndex, long fromPosition) { }

    private record ActiveSlot(int clientId, long token, long lastTouchedMs) { }

    private final int maxConcurrent;
    private final long slotTtlMs;
    private final List<ActiveSlot> active = new ArrayList<>();
    private final List<PendingRequest> pending = new ArrayList<>();

    public ReplaySlotAllocator(final int maxConcurrent, final long slotTtlMs) {
        this.maxConcurrent = maxConcurrent;
        this.slotTtlMs = slotTtlMs;
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

    /** Removes clientId's active slot, if any (a re-request supersedes it); returns its token, or {@link #NO_SLOT}. */
    public long supersede(final int clientId) {
        for (int i = 0; i < active.size(); i++) {
            if (active.get(i).clientId() == clientId) {
                return active.remove(i).token();
            }
        }
        return NO_SLOT;
    }

    /** Records a newly-started replay as occupying a slot. */
    public void activate(final int clientId, final long token, final long nowMs) {
        active.add(new ActiveSlot(clientId, token, nowMs));
    }

    /**
     * Queues a request that found no free slot, replacing any the client already has queued in place, so a
     * resend neither accretes entries nor loses the client its turn.
     * @param clientId client identity
     * @param requestId the request to answer when it is finally served
     * @param segmentIndex segment index
     * @param fromPosition start replay position
     */
    public void enqueue(final int clientId, final long requestId, final int segmentIndex, final long fromPosition) {
        final PendingRequest request = new PendingRequest(clientId, requestId, segmentIndex, fromPosition);
        for (int i = 0; i < pending.size(); i++) {
            if (pending.get(i).clientId() == clientId) {
                pending.set(i, request);
                return;
            }
        }
        pending.add(request);
    }

    /**
     * Drops the client's queued request, if any, as its current one is answered: left behind, the stale
     * one would later take a slot the client never uses.
     * @param clientId client identity
     */
    public void cancelPending(final int clientId) {
        for (int i = 0; i < pending.size(); i++) {
            if (pending.get(i).clientId() == clientId) {
                pending.remove(i);
                return;
            }
        }
    }

    /**
     * Refreshes a client's slot on its {@code ReplayHeartbeat}, so {@link #reclaimIdle} is an idle timeout
     * rather than a max lifetime: a full-log replay can legitimately run long.
     * @param clientId client identity
     * @param nowMs current time
     */
    public void touch(final int clientId, final long nowMs) {
        for (int i = 0; i < active.size(); i++) {
            final ActiveSlot slot = active.get(i);
            if (slot.clientId() == clientId) {
                active.set(i, new ActiveSlot(clientId, slot.token(), nowMs));
                return;
            }
        }
    }

    /**
     * Pops the next pending request, or null if the queue is empty or every slot is taken. Callers bound a
     * drain pass to {@link #pendingCount()}, so a request that re-queues itself is retried once per pass.
     * @return the longest-waiting client's current request, or null
     */
    public PendingRequest pollPending() {
        return hasCapacity() && !pending.isEmpty() ? pending.removeFirst() : null;
    }

    /** Removes and returns the tokens of every slot untouched for longer than the TTL (see {@link #touch}). */
    public List<Long> reclaimIdle(final long nowMs) {
        final List<Long> reclaimed = new ArrayList<>();
        for (int i = active.size() - 1; i >= 0; i--) {
            if (nowMs - active.get(i).lastTouchedMs() > slotTtlMs) {
                reclaimed.add(active.remove(i).token());
            }
        }
        return reclaimed;
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
