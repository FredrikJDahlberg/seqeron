package org.limitless.phixeron.replayer.server;

import java.util.ArrayList;
import java.util.List;

/**
 * Pure admission control and pending-queue bookkeeping behind {@link ReplayerService}'s {@code
 * MAX_CONCURRENT_REPLAYS} cap — split out from every archive/Aeron call so it is unit-testable without
 * either, mirroring how {@link org.limitless.phixeron.sequencer.Sequencer} is split from {@code
 * SequencerService}. It never interprets the {@code token} it is handed ({@code replaySessionId} in
 * production) — it only counts, orders, and ages slots by client id.
 *
 * <p>The pending queue holds <b>at most one request per client</b> ({@link #enqueue} replaces, {@link
 * #cancelPending} drops), which is what bounds it: a queued client resends every ~500ms, so appending
 * blindly grew the queue for as long as a node's cold start left more clients than slots — the exact
 * steady state of a three-app node against a cap of two.
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
     * Queues a request that found no free slot, replacing any the client already has queued — one entry
     * per client, so a client resending every ~500ms while it waits does not accrete entries.
     *
     * <p>The replacement keeps the client's place in line rather than moving it to the back: a resend
     * carries no new intent, and re-queueing it behind everyone else would let a client that resends on
     * a timer starve itself for as long as it keeps asking.
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
     * Drops the client's queued request, if any. Called as a client's current request is about to be
     * answered from the archive: whatever it had queued is stale by then, and left behind it is drained
     * later into a replay the client has already moved past — taking a slot it never asked for, answered
     * by a reply it discards on the requestId check, and freed only by the idle TTL.
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
     * Refreshes a client's slot so it ages from its last sign of life rather than from activation —
     * what makes {@link #reclaimIdle} an idle timeout instead of a max lifetime. Callers touch on the
     * client's {@code ReplayHeartbeat}: a replay is unbounded in time (full-log replay over a log that
     * grows all trading day), so a slot aged from activation is reclaimed out from under a perfectly
     * healthy replay, whereas a client that died stops heart-beating and still ages out.
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
     * Pops the next pending request to retry, or null if the queue is empty or every slot is taken.
     * Callers bound retries to {@link #pendingCount()} at the start of a drain pass, so a request that
     * re-queues itself (no recording yet, etc.) is retried at most once per pass rather than spinning.
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
