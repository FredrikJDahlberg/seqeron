package org.limitless.seqeron.replayer.server;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * What a Replayer is given to track its cluster snapshots (doc/snapshot.md §10.8): the cuts a member's service hands
 * over from its own thread, or the markers a gateway host's relays report, the member's record of which snapshots may
 * be restarted from, and how long an application has to join a round.
 *
 * @param cuts         each cut, as a member's service takes it
 * @param log          the member's {@code restart-safe.log}; null on a gateway host, which has none
 * @param joinWindowMs how long after a cut an application or gateway may join its round
 * @param relayed      each marker a gateway host's relays publish; empty on a member
 */
public record CutIntake(Queue<CutHandover> cuts, RestartSafeLog log, long joinWindowMs, Queue<RelayedCut> relayed) {
    /** A member's: its service hands over the cuts. */
    public CutIntake(final Queue<CutHandover> cuts, final RestartSafeLog log, final long joinWindowMs) {
        this(cuts, log, joinWindowMs, new ConcurrentLinkedQueue<>());
    }

    /** A gateway host's: its relays report the markers. */
    static CutIntake ofHost(final Queue<RelayedCut> relayed, final long joinWindowMs) {
        return new CutIntake(new ConcurrentLinkedQueue<>(), null, joinWindowMs, relayed);
    }
}
