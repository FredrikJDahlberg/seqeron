package org.limitless.seqeron.replayer.server;

import java.util.HashMap;
import java.util.Map;

/**
 * When a cluster snapshot may be restarted from (doc/snapshot.md §10.8), free of Aeron and of the clock. A member's
 * queue restarts at the cut, so a co-located application whose newest file is older would be fenced: the cut is
 * restart-safe once every application and gateway that joined its round has confirmed its file for it. The ones that
 * join are those that do within a window of the cut, so one that is down is not waited for. A source is its
 * {@code sourceId}, which a gateway's two instances share, so two {@code clientId}s joining for one leave the round
 * unsafe: the pair is on one node, or an instance restarted mid-round.
 *
 * <p>One round is tracked at a time; a later one replaces it, and an unfinished one stays unsafe. A confirmation is
 * signalled once, so one for the round after the one tracked, which the Replayer has not yet seen cut, is kept for it. Single-threaded:
 * the Replayer's duty cycle.
 */
final class RestartSafety {
    private static final long NONE = Long.MIN_VALUE;

    private final long joinWindowMs;
    /** Each source that joined this round by its sourceId, and whether it has confirmed. */
    private final Map<Integer, Boolean> joined = new HashMap<>();
    /** The clientId each source joined or confirmed under in this round. */
    private final Map<Integer, Long> clients = new HashMap<>();

    /** The clientId each source confirmed under for {@link #earlyRound}, the round after the one tracked. */
    private final Map<Integer, Long> early = new HashMap<>();

    private long earlyRound = NONE;
    private long round = NONE;
    private long logPosition;
    private long windowEndsMs;
    private boolean decided;
    private boolean conflicted;

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
        conflicted = false;
        joined.clear();
        clients.clear();
        if (newRound == earlyRound) {
            early.forEach((sourceId, clientId) -> {
                joined.put(sourceId, true);
                clients.put(sourceId, clientId);
            });
        }
        if (newRound >= earlyRound) {
            early.clear();
            earlyRound = NONE;
        }
    }

    /**
     * An instance says it takes part in {@code forRound}.
     * @return whether this made the round unsafe, a second {@code clientId} joining for the source
     */
    boolean onJoined(final long forRound, final int sourceId, final long clientId) {
        if (forRound != round) {
            return false;
        }
        joined.putIfAbsent(sourceId, false);
        return admit(sourceId, clientId);
    }

    /**
     * An instance says its file for {@code forRound} is confirmed; that also says it took part.
     * @return whether this made the round unsafe, as {@link #onJoined}
     */
    boolean onConfirmed(final long forRound, final int sourceId, final long clientId) {
        if (forRound > round) {
            if (forRound != earlyRound) {
                early.clear();
                earlyRound = forRound;
            }
            early.putIfAbsent(sourceId, clientId);
            return false;
        }
        if (forRound != round) {
            return false;
        }
        joined.put(sourceId, true);
        return admit(sourceId, clientId);
    }

    private boolean admit(final int sourceId, final long clientId) {
        final Long known = clients.putIfAbsent(sourceId, clientId);
        if (known == null || known == clientId || conflicted) {
            return false;
        }
        conflicted = true;
        return true;
    }

    /**
     * Whether the round in flight has become safe.
     * @param nowMs now
     * @return its snapshot's log position, once and once only, else {@link #NO_POSITION}
     */
    long poll(final long nowMs) {
        if (round == NONE || decided || conflicted || nowMs < windowEndsMs) {
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
