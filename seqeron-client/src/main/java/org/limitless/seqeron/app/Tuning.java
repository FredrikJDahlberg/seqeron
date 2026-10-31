package org.limitless.seqeron.app;

/**
 * The policy knobs a producer rarely changes, kept off its builder so that holds only identity and wiring.
 *
 * @param pendingCapacity        messages tracked by confirmed ingress; beyond it {@code publish} returns {@code Declined}
 * @param queueStallTimeoutMs    no {@code ClusterHeartbeat} for this long fences an instance that acts with
 *                               {@link ClusterError#QUEUE_STALLED}; any other logs it
 * @param recoveryStallTimeoutMs recovery dispatching nothing for this long fences, as above, with {@link
 *                               ClusterError#RECOVERY_STALLED}
 */
public record Tuning(int pendingCapacity, long queueStallTimeoutMs, long recoveryStallTimeoutMs) {
    /** 1,024 messages, 20 heartbeat periods and three times that. */
    public static final Tuning DEFAULT = new Tuning(Session.DEFAULT_PENDING_CAPACITY,
                                                    Session.DEFAULT_QUEUE_STALL_TIMEOUT_MS,
                                                    Session.DEFAULT_RECOVERY_STALL_TIMEOUT_MS);

    public Tuning withPendingCapacity(final int pendingCapacity) {
        return new Tuning(pendingCapacity, queueStallTimeoutMs, recoveryStallTimeoutMs);
    }

    public Tuning withQueueStallTimeoutMs(final long queueStallTimeoutMs) {
        return new Tuning(pendingCapacity, queueStallTimeoutMs, recoveryStallTimeoutMs);
    }

    public Tuning withRecoveryStallTimeoutMs(final long recoveryStallTimeoutMs) {
        return new Tuning(pendingCapacity, queueStallTimeoutMs, recoveryStallTimeoutMs);
    }
}
