package org.limitless.seqeron.replayer.server;

import java.util.ArrayList;
import java.util.List;
import java.util.function.LongConsumer;
import java.util.function.LongUnaryOperator;

/**
 * Admission control and pending-request bookkeeping behind {@link ReplayerService}'s {@code
 * MAX_CONCURRENT_REPLAYS} cap, free of archive and Aeron calls. The {@code token} it is handed is opaque.
 * The pending list holds at most one request per client, since a waiting client resends every ~500ms.
 */
public final class ReplaySlotAllocator {
    /** {@link #supersede} uses this value for "no active slot for that client". */
    public static final long NO_SLOT = -1;

    /** One pending request waiting for a free slot, in request order. */
    public record PendingRequest(long clientId, long requestId, long fromPosition, int queueId,
                                 long cutClusterSeqNo) { }

    private static final class ActiveSlot {
        final long clientId;
        final long requestId;
        final long token;
        final long catchUpPosition;
        /** Its replay's progress when last seen to move, and when that was. */
        long progress = Replayer.REPLAY_ENDED;
        long sinceMs;

        ActiveSlot(final long clientId, final long requestId, final long token, final long catchUpPosition) {
            this.clientId = clientId;
            this.requestId = requestId;
            this.token = token;
            this.catchUpPosition = catchUpPosition;
        }
    }

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
            if (active.get(i).clientId == clientId) {
                return active.remove(i).token;
            }
        }
        return NO_SLOT;
    }

    /**
     * Records a newly-started replay as occupying a slot.
     * @param clientId        client identity
     * @param requestId       the request it answers
     * @param token           the replay
     * @param catchUpPosition where it ends
     */
    public void activate(final long clientId, final long requestId, final long token, final long catchUpPosition) {
        active.add(new ActiveSlot(clientId, requestId, token, catchUpPosition));
    }

    /**
     * The replay that answers a client's request, which a resend of it repeats.
     * @param clientId  client identity
     * @param requestId the request
     * @return its token, or {@link #NO_SLOT} if no active replay answers it
     */
    public long answering(final long clientId, final long requestId) {
        for (int i = 0; i < active.size(); i++) {
            final ActiveSlot slot = active.get(i);
            if (slot.clientId == clientId && slot.requestId == requestId) {
                return slot.token;
            }
        }
        return NO_SLOT;
    }

    /**
     * Where the replay a token names ends.
     * @param token an active replay
     * @return its catch-up position, or {@link #NO_SLOT} if it is not active
     */
    public long catchUpPosition(final long token) {
        for (int i = 0; i < active.size(); i++) {
            if (active.get(i).token == token) {
                return active.get(i).catchUpPosition;
            }
        }
        return NO_SLOT;
    }

    /**
     * Holds a request that found no free slot, replacing any the client already has pending in place, so a
     * resend neither accretes entries nor loses the client its turn.
     * @param clientId client identity
     * @param requestId the request to answer when it is finally served
     * @param fromPosition start replay position, or {@code ReplayProtocol.FROM_START}
     * @param queueId the queue it is for
     * @param cutClusterSeqNo the snapshot cut it restores, or {@code ReplayProtocol.NO_CUT}
     */
    public void addPending(final long clientId, final long requestId, final long fromPosition, final int queueId,
                           final long cutClusterSeqNo) {
        final PendingRequest request = new PendingRequest(clientId, requestId, fromPosition, queueId, cutClusterSeqNo);
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
     * drain pass to {@link #pendingCount()}.
     * @return the longest-waiting client's current request, or null
     */
    public PendingRequest pollPending() {
        return hasCapacity() && !pending.isEmpty() ? pending.removeFirst() : null;
    }

    /**
     * Removes every slot whose replay has ended, or has made no progress for {@code stallMs}.
     * @param progress the progress of the replay a token names, {@link Replayer#REPLAY_ENDED} once it has ended
     * @param nowMs    the time now
     * @param stallMs  how long a replay may go without progress
     * @param stalled  given the token of each stalled replay, which is still running
     * @return how many were removed
     */
    public int reclaim(final LongUnaryOperator progress, final long nowMs, final long stallMs,
                       final LongConsumer stalled) {
        int removed = 0;
        for (int i = active.size() - 1; i >= 0; i--) {
            final ActiveSlot slot = active.get(i);
            final long now = progress.applyAsLong(slot.token);
            if (now == Replayer.REPLAY_ENDED) {
                active.remove(i);
                ++removed;
            } else if (now != slot.progress) {
                slot.progress = now;
                slot.sinceMs = nowMs;
            } else if (nowMs - slot.sinceMs >= stallMs) {
                active.remove(i);
                ++removed;
                stalled.accept(slot.token);
            }
        }
        return removed;
    }

    /** Removes every active slot (shutdown), returning their tokens. */
    public List<Long> clear() {
        final List<Long> tokens = new ArrayList<>(active.size());
        for (final ActiveSlot slot : active) {
            tokens.add(slot.token);
        }
        active.clear();
        return tokens;
    }
}
