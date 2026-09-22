#pragma once

#include <cstdint>

namespace org::limitless::seqeron::app::detail {

// A gateway's recovery-stall fence: recovery dispatching nothing for a deadline, once this instance has been
// caught up at least once. Progress, not elapsed recovery — a converging re-walk always advances the
// globalSeqNo it has dispatched, and a cold start (never caught up) is never fenced. The Java twin is
// app/RecoveryStallFence.java, which carries the full rationale; keep the two in step.
class RecoveryStallFence
{
  public:
    // deadlineMs: how long recovery may run without dispatching a single frame, once this instance has been
    // caught up before, before it is declared unconvergent.
    explicit RecoveryStallFence(std::int64_t deadlineMs) : m_deadlineMs{ deadlineMs }
    {}

    // Caught up: latches everCaughtUp forever and clears the recovery clock, so the next episode re-arms.
    void onCaughtUp()
    {
        m_everCaughtUp = true;
        m_recoveryStartMs = 0;
    }

    // Evaluates one !isCaughtUp() observation, given the highest globalSeqNo dispatched so far. Returns true
    // once recovery has dispatched nothing for >= deadlineMs on an instance that has been caught up before.
    bool onNotCaughtUp(const std::int64_t nowMs, const std::int64_t globalSeqNo)
    {
        if (!m_everCaughtUp)
        {
            return false;
        }
        if (m_recoveryStartMs == 0 || globalSeqNo != m_lastGlobalSeqNo)
        {
            m_lastGlobalSeqNo = globalSeqNo;
            m_recoveryStartMs = nowMs;
            return false;
        }
        return (nowMs - m_recoveryStartMs) >= m_deadlineMs;
    }

  private:
    std::int64_t m_deadlineMs;
    bool m_everCaughtUp = false;
    std::int64_t m_recoveryStartMs = 0; // 0 = no recovery episode currently timed
    std::int64_t m_lastGlobalSeqNo = 0; // frontier at the last observation; only meaningful while timing
};

} // namespace org::limitless::seqeron::app::detail
