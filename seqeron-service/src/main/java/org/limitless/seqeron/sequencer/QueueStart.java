package org.limitless.seqeron.sequencer;

/**
 * Where a queue's recording starts when a member starts (doc/snapshot.md §10.3), free of Aeron: at its first frame
 * with no snapshot, at the position its marker started at when the node's term length and MTU are the ones the
 * snapshot names, so that the positions agree with the recording it replaces and with every peer's, and at
 * position 0 otherwise, the cut being found by its {@code clusterSeqNo} instead.
 */
final class QueueStart {
    /** What a start does. */
    enum Kind {
        /** A new recording from {@code groupSeqNo} 1, with no snapshot to restore. */
        GENESIS,
        /** A new recording from the position the restored marker started at. */
        AT_POSITION,
        /** A new recording from position 0, the restored marker being its first frame. */
        AT_ZERO
    }

    /**
     * A decision.
     * @param kind     what to do
     * @param position where the queue publication starts: the marker's for {@link Kind#AT_POSITION}, else 0
     */
    record Decision(Kind kind, long position) { }

    private QueueStart() {
    }

    /**
     * Decides where one queue starts.
     *
     * @param restored           whether the start restores a snapshot
     * @param markerPosition     where the snapshot says the queue's marker started
     * @param snapshotTermLength the term length the snapshot says the queue was published with
     * @param snapshotMtu        the MTU the snapshot says the queue was published with
     * @param nodeTermLength     the term length the node publishes its queues with now
     * @param nodeMtu            the MTU the node publishes its queues with now
     * @return the decision
     */
    static Decision decide(final boolean restored, final long markerPosition, final int snapshotTermLength,
                           final int snapshotMtu, final int nodeTermLength, final int nodeMtu) {
        if (!restored) {
            return new Decision(Kind.GENESIS, 0);
        }
        if (snapshotTermLength == nodeTermLength && snapshotMtu == nodeMtu && markerPosition >= 0) {
            return new Decision(Kind.AT_POSITION, markerPosition);
        }
        return new Decision(Kind.AT_ZERO, 0);
    }
}
