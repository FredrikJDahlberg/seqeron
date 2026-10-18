package org.limitless.seqeron.app;

/**
 * A producer's queue-liveness fence: no {@code ClusterHeartbeat} for a deadline, on an instance that has
 * caught up. The heartbeat is the one frame that keeps arriving while every producer is silent, so its
 * absence — and nothing else — separates a quiet deployment from a queue this process has stopped seeing.
 *
 * <p><b>Armed by catching up, and re-armed by each re-convergence.</b> A cold start replays history, from
 * {@code clusterSeqNo} 1 or a snapshot's cut, and a re-walk dispatches history rather than live frames; neither
 * has a heartbeat cadence to measure, so timing either would fence the very path recovery takes.
 * {@link RecoveryStallFence} is the fence covering that side.
 *
 * <p>Single-threaded, like every block here. The C++ twin is {@code app/QueueStallFence.hpp}; keep the two in
 * step.
 */
final class QueueStallFence {
    private final long deadlineMs;

    private boolean armed;
    private long lastProgressMs;

    /**
     * A fence that is not armed until the first catch-up.
     *
     * @param deadlineMs how long the queue may be silent before this instance can no longer be trusted to be
     *                   seeing it; generous against the 1 Hz heartbeat
     */
    public QueueStallFence(final long deadlineMs) {
        this.deadlineMs = deadlineMs;
    }

    /** Caught up: arms the fence and anchors the deadline, so a healed gap is never timed against it. */
    public void onCaughtUp(final long nowMs) {
        armed = true;
        lastProgressMs = nowMs;
    }

    /** A {@code ClusterHeartbeat} off the queue. */
    public void onClusterHeartbeat(final long nowMs) {
        lastProgressMs = nowMs;
    }

    /**
     * @param nowMs monotonic clock reading
     * @return true once the queue has been silent for the deadline; always false before the first catch-up
     */
    public boolean isStalled(final long nowMs) {
        return armed && (nowMs - lastProgressMs) >= deadlineMs;
    }
}
