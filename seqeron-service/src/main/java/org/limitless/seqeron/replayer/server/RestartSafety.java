package org.limitless.seqeron.replayer.server;

import java.util.HashMap;
import java.util.Map;

/**
 * When a cluster snapshot may be restarted from (doc/snapshot.md §10.8), free of Aeron and of the clock. A member's
 * queue restarts at the cut, so a co-located application whose newest file is older would be fenced: the cut is
 * restart-safe once every application that joined its round has confirmed its file for it. The ones that join are
 * those that do within a window of the cut, so an application that is down is not waited for.
 *
 * <p>One round is tracked at a time; a later one replaces it, and an unfinished one stays unsafe. Single-threaded:
 * the Replayer's duty cycle.
 */
final class RestartSafety {
    private static final long NONE = Long.MIN_VALUE;

    private final long joinWindowMs;
    /** Each replica that joined this round by its sourceId, and whether it has confirmed. */
    private final Map<Integer, Boolean> joined = new HashMap<>();

    private long round = NONE;
    private long logPosition;
    private long windowEndsMs;
    private boolean decided;

    /** @param joinWindowMs how long after a cut an application may join its round */
    RestartSafety(final long joinWindowMs) {
        this.joinWindowMs = joinWindowMs;
    }

    /**
     * A cut was taken; the round before it, if still open, stays unsafe.
     * @param newRound     its round
     * @param newPosition  the snapshot's log position
     * @param nowMs        now
     */
    void onCut(final long newRound, final long newPosition, final long nowMs) {
        round = newRound;
        logPosition = newPosition;
        windowEndsMs = nowMs + joinWindowMs;
        decided = false;
        joined.clear();
    }

    /** An application replica says it takes part in {@code forRound}. */
    void onJoined(final long forRound, final int sourceId) {
        if (forRound == round) {
            joined.putIfAbsent(sourceId, false);
        }
    }

    /** An application replica says its file for {@code forRound} is confirmed; that also says it took part. */
    void onConfirmed(final long forRound, final int sourceId) {
        if (forRound == round) {
            joined.put(sourceId, true);
        }
    }

    /**
     * Whether the round in flight has become safe.
     * @param nowMs now
     * @return its snapshot's log position, once and once only, else {@link #NO_POSITION}
     */
    long poll(final long nowMs) {
        if (round == NONE || decided || nowMs < windowEndsMs) {
            return NO_POSITION;
        }
        if (joined.containsValue(false)) {
            return NO_POSITION;
        }
        decided = true;
        return logPosition;
    }

    /** {@link #poll}'s answer while the round is not safe. */
    static final long NO_POSITION = -1;
}
