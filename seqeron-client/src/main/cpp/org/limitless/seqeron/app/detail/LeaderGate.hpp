#pragma once

#include <cstdint>

namespace org::limitless::seqeron::app::detail {

// Whether this replica may do leader-only work: caught up, and its node is the leader — or, off the cluster,
// caught up alone. Gives edges, and
// every leadership change closes an open gate for one duty cycle. The Java twin is app/LeaderGate.java,
// which carries the rationale; keep the two in step.
class LeaderGate
{
  public:
    enum class Transition : std::uint8_t
    {
        None,
        Opened,
        Closed,
    };

    // offCluster: this node runs no member, so no leadership is its own and the gate opens whoever leads.
    explicit LeaderGate(std::int32_t memberId, bool offCluster = false) :
      m_memberId{ memberId },
      m_offCluster{ offCluster }
    {}

    // A LeadershipChanged frame was applied; an open gate closes on the next update.
    void onLeadershipChanged()
    {
        m_leadershipChanged = true;
    }

    // Once per duty cycle, with the receiver's isCaughtUp() and currentLeaderMemberId().
    Transition update(const bool caughtUp, const std::int32_t currentLeaderMemberId)
    {
        const bool wasOpen = m_open;
        const bool restart = m_leadershipChanged;
        m_leadershipChanged = false;
        m_open = !(wasOpen && restart) && caughtUp && (m_offCluster || currentLeaderMemberId == m_memberId);
        if (m_open == wasOpen)
        {
            return Transition::None;
        }
        return m_open ? Transition::Opened : Transition::Closed;
    }

    [[nodiscard]] bool isOpen() const noexcept
    {
        return m_open;
    }

  private:
    std::int32_t m_memberId;
    bool m_offCluster;
    bool m_open = false;
    bool m_leadershipChanged = false;
};

} // namespace org::limitless::seqeron::app::detail
