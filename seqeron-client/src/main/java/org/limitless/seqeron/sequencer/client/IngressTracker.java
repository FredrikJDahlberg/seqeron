package org.limitless.seqeron.sequencer.client;

import org.agrona.DirectBuffer;

/**
 * What {@link IngressPublisher} tells, and asks, about each frame it places, so a producer confirms its
 * ingress on the tap without handling a frame itself; and what the sender tells, and asks, about a leader
 * change. A send spinning through an election would land in the new term ahead of older frames that election
 * lost, so the sender gives it up while the hold is on. {@code app.PendingSends} is the implementation; the
 * C++ twin is {@code sequencer/client/IngressTracker.hpp}.
 */
public interface IngressTracker {
    /** Whether another frame can be tracked; one that cannot must not be sent. */
    boolean isFull();

    /** A frame just placed, with the sender's {@code clusterSessionId()} and {@code leadershipTermId()}. */
    void track(DirectBuffer frame, int length, long clusterSessionId, long leadershipTermId);

    /** Egress named a new leader, for this term. */
    void onNewLeader(long leadershipTermId);

    /** Whether frames from an earlier term may still need resending ahead of any new one. */
    boolean isHolding();
}
