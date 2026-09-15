#pragma once

#include <cstdint>

namespace org::limitless::seqeron::app {

// Whether this replica may do leader-only work: caught up, and its node is the leader. Gives edges, and
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

    explicit LeaderGate(std::int32_t memberId) : m_memberId{ memberId }
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
        m_open = !(wasOpen && restart) && caughtUp && currentLeaderMemberId == m_memberId;
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
    bool m_open = false;
    bool m_leadershipChanged = false;
};

} // namespace org::limitless::seqeron::app
