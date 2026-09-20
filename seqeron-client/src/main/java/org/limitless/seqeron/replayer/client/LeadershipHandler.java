package org.limitless.seqeron.replayer.client;

/** Receives each {@code LeadershipChanged} as it is dispatched, in log order. */
@FunctionalInterface
public interface LeadershipHandler {
    /**
     * Called once per term, whether or not the leader changed with it.
     * @param newLeaderMemberId  the member leading from this frame onward
     * @param leadershipTermId   the term that begins here
     * @param globalSeqNo        this frame's own sequence number, which counts toward continuity
     */
    void onLeadershipChanged(int newLeaderMemberId, long leadershipTermId, long globalSeqNo);
}
