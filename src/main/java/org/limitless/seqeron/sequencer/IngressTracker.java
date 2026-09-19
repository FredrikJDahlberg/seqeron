package org.limitless.seqeron.sequencer;

import org.agrona.DirectBuffer;

/**
 * What {@link IngressPublisher} tells, and asks, about each frame it places, so a producer confirms its
 * ingress on the tap without handling a frame itself. {@code app.PendingSends} is the implementation; the
 * C++ twin is {@code sequencer/IngressTracker.hpp}.
 */
public interface IngressTracker extends IngressHold {
    /** Whether another frame can be tracked; one that cannot must not be sent. */
    boolean isFull();

    /** A frame just placed, with the sender's {@code clusterSessionId()} and {@code leadershipTermId()}. */
    void track(DirectBuffer frame, int length, long clusterSessionId, long leadershipTermId);
}
