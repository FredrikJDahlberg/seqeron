package org.limitless.seqeron.app;

/**
 * Why a producer may no longer act. Each is terminal and latched: the process releases its cluster session
 * so a standby can take over, rather than carrying on behind a view of the log it cannot trust.
 */
public enum Fence {
    /** The cluster closed this session, or a new leader never arrived. This instance can never be promoted again. */
    CLUSTER_SESSION_LOST,
    /** An own frame came back differing from the oldest pending one, so what reached the log cannot be counted. */
    INGRESS_CONFIRM_FAULTED,
    /** Recovery has dispatched nothing for the deadline, on an instance that had caught up before. */
    RECOVERY_STALLED,
    /** No {@code ClusterHeartbeat} for the deadline: this process has stopped seeing its node's tap. */
    TAP_STALLED
}
