#pragma once

#include <cstdint>

namespace org::limitless::seqeron::app::detail {

// A producer's queue-liveness fence: no ClusterHeartbeat for a deadline, on an instance that has caught up.
// The heartbeat is the one frame that keeps arriving while every producer is silent, so its absence — and
// nothing else — separates a quiet deployment from a queue this process has stopped seeing. Armed by catching
// up and re-armed by each re-convergence, because a replay has no heartbeat cadence to measure and timing
// one would fence the very path recovery takes; RecoveryStallFence covers that side. The Java twin is
// app/QueueStallFence.java, which carries the full rationale; keep the two in step.
class QueueStallFence
{
  public:
    // deadlineMs: how long the queue may be silent before this instance can no longer be trusted to be seeing
    // it; generous against the 1 Hz heartbeat.
    explicit QueueStallFence(std::int64_t deadlineMs) : m_deadlineMs{ deadlineMs }
    {}

    // Caught up: arms the fence and anchors the deadline, so a healed gap is never timed against it.
    void onCaughtUp(const std::int64_t nowMs)
    {
        m_armed = true;
        m_lastProgressMs = nowMs;
    }

    // A ClusterHeartbeat off the queue.
    void onClusterHeartbeat(const std::int64_t nowMs)
    {
        m_lastProgressMs = nowMs;
    }

    // True once the queue has been silent for the deadline; always false before the first catch-up.
    [[nodiscard]] bool isStalled(const std::int64_t nowMs) const noexcept
    {
        return m_armed && (nowMs - m_lastProgressMs) >= m_deadlineMs;
    }

  private:
    std::int64_t m_deadlineMs;
    bool m_armed = false;
    std::int64_t m_lastProgressMs = 0;
};

} // namespace org::limitless::seqeron::app::detail
