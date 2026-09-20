package org.limitless.seqeron.app;

/**
 * Whether this replica may do leader-only work: caught up, and its node is the leader. Gives edges rather
 * than a boolean, because {@link OutstandingWork#onNotLeader()} belongs on the closing edge.
 *
 * <p><b>Every leadership change closes an open gate</b>, not only one that ends with another leader.
 * {@code update} runs once per duty cycle, so a flip away and back applied within one cycle reads the same
 * member before and after. A reply this node sent during that election may have been lost with it, so the
 * gate closes for one cycle and the tracker re-dispatches. Call {@link #onLeadershipChanged()} from the
 * receiver's leadership callback.
 *
 * <p>The C++ twin is {@code app/LeaderGate.hpp}; keep the two in step.
 */
public final class LeaderGate {
    /** An edge crossed by one {@link #update}. */
    public enum Transition {
        /** The gate stands where it stood. */
        NONE,
        /** This replica may now do leader-only work. */
        OPENED,
        /** It may not: re-dispatch on the next open rather than assume anything completed. */
        CLOSED
    }

    private final int memberId;

    private boolean open;
    private boolean leadershipChanged;

    /**
     * A gate that starts shut.
     *
     * @param memberId this node's cluster member id
     */
    public LeaderGate(final int memberId) {
        this.memberId = memberId;
    }

    /** A {@code LeadershipChanged} frame was applied; an open gate closes on the next {@link #update}. */
    public void onLeadershipChanged() {
        leadershipChanged = true;
    }

    /**
     * Evaluates the gate once per duty cycle.
     * @param caughtUp              the receiver's {@code isCaughtUp()}
     * @param currentLeaderMemberId the receiver's {@code currentLeaderMemberId()}
     * @return the edge crossed, if any
     */
    public Transition update(final boolean caughtUp, final int currentLeaderMemberId) {
        final boolean wasOpen = open;
        final boolean restart = leadershipChanged;
        leadershipChanged = false;
        open = !(wasOpen && restart) && caughtUp && currentLeaderMemberId == memberId;
        if (open == wasOpen) {
            return Transition.NONE;
        }
        return open ? Transition.OPENED : Transition.CLOSED;
    }

    /** Whether leader-only work may run, without waiting for the next {@link #update}. */
    public boolean isOpen() {
        return open;
    }
}
