#pragma once

#include <cstdint>

namespace org::limitless::phixeron::replayer::client {

class RecoveryProgressPolicy
{
  public:
    explicit RecoveryProgressPolicy(std::int64_t stallMs) : m_stallMs{ stallMs }
    {}

    bool onProgress()
    {
        const bool wasReported = m_reported;
        m_sinceMs = 0;
        m_reported = false;
        return wasReported;
    }

    bool onNoProgress(const std::int64_t nowMs)
    {
        if (m_sinceMs == 0)
        {
            m_sinceMs = nowMs;
            return false;
        }
        if (m_reported || (nowMs - m_sinceMs) < m_stallMs)
        {
            return false;
        }
        m_reported = true;
        return true;
    }

  private:
    std::int64_t m_stallMs;
    std::int64_t m_sinceMs = 0; // 0 = no episode currently timed
    bool m_reported = false;
};

} // namespace org::limitless::phixeron::replayer::client
