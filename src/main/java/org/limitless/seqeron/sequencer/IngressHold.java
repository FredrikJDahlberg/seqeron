package org.limitless.seqeron.sequencer;

/**
 * What a sender tells, and asks, about a leader change. A send spinning through an election would land in
 * the new term ahead of older frames that election lost, so the sender gives it up while the hold is on.
 * {@code app.PendingSends} is the implementation; the C++ twin is {@code sequencer/IngressHold.hpp}.
 */
public interface IngressHold {
    /** Egress named a new leader, for this term. */
    void onNewLeader(long leadershipTermId);

    /** Whether frames from an earlier term may still need resending ahead of any new one. */
    boolean isHolding();
}
