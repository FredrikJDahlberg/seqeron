package org.limitless.phixeron.replayer;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/**
 * Pure admission control and pending-queue bookkeeping behind {@link ReplayerService}'s {@code
 * MAX_CONCURRENT_REPLAYS} cap — split out from every archive/Aeron call so it is unit-testable without
 * either, mirroring how {@link org.limitless.phixeron.sequencer.Sequencer} is split from {@code
 * SequencerService}. It never interprets the {@code token} it is handed ({@code replaySessionId} in
 * production) — it only counts, orders, and ages slots by client id.
 */
public final class ReplaySlotAllocator {
    /** {@link #supersede} uses this value for "no active slot for that client". */
    public static final long NO_SLOT = -1;

    /** One pending request waiting for a free slot, in request order. */
    public record PendingRequest(int clientId, int segmentIndex, long fromPosition) {}

    private record ActiveSlot(int clientId, long token, long lastTouchedMs) {}

    private final int maxConcurrent;
    private final long slotTtlMs;
    private final List<ActiveSlot> active = new ArrayList<>();
    private final Deque<PendingRequest> pending = new ArrayDeque<>();

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

    /** Queues a request that found no free slot. */
    public void enqueue(final int clientId, final int segmentIndex, final long fromPosition) {
        pending.addLast(new PendingRequest(clientId, segmentIndex, fromPosition));
    }

    /**
     * Pops the next pending request to retry, or null if the queue is empty or every slot is taken.
     * Callers bound retries to {@link #pendingCount()} at the start of a drain pass, so a request that
     * re-queues itself (no recording yet, etc.) is retried at most once per pass rather than spinning.
     */
    public PendingRequest pollPending() {
        return hasCapacity() ? pending.pollFirst() : null;
    }

    /** Removes and returns the tokens of every slot idle longer than the TTL. */
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
