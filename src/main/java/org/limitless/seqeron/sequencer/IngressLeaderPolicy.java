package org.limitless.seqeron.sequencer;

/**
 * Pure decision logic behind {@link ClusterStreamSender#pollEgress}'s one reaction to a new leader:
 * whether this producer's ingress must move. Split off and unit-tested the way {@link TapStallPolicy} is.
 *
 * <p>It matters only to a client connected over the co-located member's {@code aeron:ipc}. That channel
 * reaches exactly one member, and only while that member leads — no endpoint set to chase, so
 * {@code AeronCluster} would re-add the publication on the same channel and wait there forever. On UDP
 * there is nothing to decide: the cluster client chases the leader itself.
 *
 * <p>Leadership coming <i>back</i> to the co-located member is deliberately not chased, unlike the C++
 * twin's re-chase: swapping back costs a session here, where there it costs a publication.
 */
public final class IngressLeaderPolicy {
    /** No co-located member: this producer reaches the cluster over the network. */
    public static final int NO_MEMBER = -1;

    private final int colocatedMemberId;
    private boolean overIpc;
    private boolean reconnectDue;

    /** @param colocatedMemberId the member sharing this producer's media driver, or {@link #NO_MEMBER} */
    public IngressLeaderPolicy(final int colocatedMemberId) {
        this.colocatedMemberId = colocatedMemberId;
    }

    /**
     * Records how the session that has just been established reaches the cluster, and clears any pending
     * verdict — a reconnect that has happened is not still due.
     * @param overIpc whether ingress is the co-located member's own {@code aeron:ipc}
     */
    public void onConnected(final boolean overIpc) {
        this.overIpc = overIpc;
        reconnectDue = false;
    }

    /** Records the leader the cluster has just named. */
    public void onNewLeader(final int leaderMemberId) {
        reconnectDue = overIpc && leaderMemberId != colocatedMemberId;
    }

    /** Whether ingress can no longer reach the leader and the session must be replaced. */
    public boolean reconnectDue() {
        return reconnectDue;
    }
}
