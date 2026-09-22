#pragma once

#include <cstdint>

namespace org::limitless::seqeron::app {

// Observes how far behind the leader a co-located tap is, from each live ClusterHeartbeat's arrival
// lateness (receiveTimeNs - clusterTimestampNs). Observation only; it raises no fence. The Java twin is
// app/TapLagMonitor.java, which carries the full rationale; keep the two in step.
enum class TapLag : std::uint8_t
{
    None,          // no edge crossed
    BecameStale,   // lag reached the threshold, having been under it
    BecameFresh,   // lag fell back under the threshold, having been over it
    SkewSuspected, // a heartbeat arrived a threshold or more before its own timestamp; reported once, then latched
};

class TapLagMonitor
{
  public:
    // thresholdNs: the lag at which the tap is called stale; no tighter than the caller's tap-silence timeout.
    explicit TapLagMonitor(std::int64_t thresholdNs) : m_thresholdNs{ thresholdNs }
    {}

    // Evaluates one live ClusterHeartbeat; the caller gates on isCaughtUp(). Returns only edges.
    TapLag onClusterHeartbeat(const std::int64_t clusterTimestampNs, const std::int64_t receiveTimeNs)
    {
        m_lastLagNs = receiveTimeNs - clusterTimestampNs;
        ++m_sampleCount;
        if (m_lastLagNs > m_peakLagNs)
        {
            m_peakLagNs = m_lastLagNs;
        }
        // Impossible on one clock: this host is behind the leader by at least the threshold. Latched.
        if (m_lastLagNs <= -m_thresholdNs)
        {
            if (m_skewReported)
            {
                return TapLag::None;
            }
            m_skewReported = true;
            return TapLag::SkewSuspected;
        }
        if (m_lastLagNs >= m_thresholdNs)
        {
            if (m_stale)
            {
                return TapLag::None;
            }
            m_stale = true;
            return TapLag::BecameStale;
        }
        if (m_stale)
        {
            m_stale = false;
            return TapLag::BecameFresh;
        }
        return TapLag::None;
    }

    std::int64_t lastLagNs() const
    {
        return m_lastLagNs;
    }

    // Worst lag since start. Report it with sampleCount(): a client that never caught up also peaks at 0.
    std::int64_t peakLagNs() const
    {
        return m_peakLagNs;
    }

    std::int64_t sampleCount() const
    {
        return m_sampleCount;
    }

    bool isStale() const
    {
        return m_stale;
    }

    // Latched once a heartbeat arrives a threshold or more before its own timestamp: this host's clock
    // trails the leader's, so isStale() cannot be trusted until the clocks are synchronised.
    bool isSkewSuspected() const
    {
        return m_skewReported;
    }

  private:
    std::int64_t m_thresholdNs;
    std::int64_t m_lastLagNs = 0;
    std::int64_t m_peakLagNs = 0;
    std::int64_t m_sampleCount = 0;
    bool m_stale = false;
    bool m_skewReported = false;
};

} // namespace org::limitless::seqeron::app
