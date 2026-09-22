#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>

namespace org::limitless::seqeron::app::detail {

// The election lifecycle of one gateway instance: which instance of a logical gateway opens its gate, and
// when it stops. The Java twin is app/GatewayLifecycle.java, which carries the rationale; keep the two in step.
//
// Actions provides:
//   void identityResolved(std::int32_t gatewayId, std::int32_t gatewaySourceId, std::int32_t preferenceRank)
//   bool publishGatewayStarted(std::int32_t gatewayId) // false is back-pressure, retried on advance()
//   bool openGate()                                    // accept or dial; false is retried on advance()
//   void closeGate()                                   // drops every session it let in; publishes nothing
template<typename Actions>
class GatewayLifecycle
{
  public:
    static constexpr std::int32_t UNRESOLVED = -1;

    enum class State : std::uint8_t
    {
        Replaying,
        Passive,
        Serving,
    };

    GatewayLifecycle(std::string gatewayName, Actions& actions) :
      m_gatewayName{ std::move(gatewayName) },
      m_actions{ actions }
    {}

    // The tap reached the live frontier for the first time. Returns whether this left Replaying.
    bool onCaughtUp()
    {
        if (m_state != State::Replaying)
        {
            return false;
        }
        m_state = State::Passive;
        return true;
    }

    // A GatewayRegistered row. Only the one carrying this instance's name resolves its identity.
    void onGatewayRegistered(const std::int32_t rowGatewayId, const std::int32_t rowGatewaySourceId,
                             const std::string_view rowName, const std::int32_t preferenceRank)
    {
        m_gatewaySourceIds.insert_or_assign(rowGatewayId, rowGatewaySourceId);
        if (rowName != m_gatewayName)
        {
            return;
        }
        m_gatewayId = rowGatewayId;
        m_gatewaySourceId = rowGatewaySourceId;
        m_actions.identityResolved(m_gatewayId, m_gatewaySourceId, preferenceRank);
    }

    // A GatewayActive. One naming a sibling stands this instance down; another pair's is ignored.
    void onGatewayActive(const std::int32_t targetGatewayId)
    {
        if (m_gatewayId == UNRESOLVED)
        {
            return;
        }
        const auto target = m_gatewaySourceIds.find(targetGatewayId);
        if (target == m_gatewaySourceIds.end() || target->second != m_gatewaySourceId)
        {
            return;
        }
        const bool wasActivated = m_activated;
        m_activated = targetGatewayId == m_gatewayId;
        if (m_activated || !wasActivated)
        {
            return;
        }
        m_registered = false; // being asked back is a new epoch
        if (m_state == State::Serving)
        {
            m_state = State::Passive;
            m_actions.closeGate();
        }
    }

    // The gate closed without being asked to. The activation stands, so advance() reopens it without a
    // second GatewayStarted.
    void onGateClosed()
    {
        if (m_state == State::Serving)
        {
            m_state = State::Passive;
        }
    }

    // A session arrived through the gate. False means the gate closed while it was in flight: drop it
    // without publishing anything about it.
    [[nodiscard]] bool onSessionAcquired() const noexcept
    {
        return m_state == State::Serving;
    }

    // One duty cycle: an activated, caught-up instance publishes GatewayStarted, then opens the gate.
    int advance()
    {
        if (m_state != State::Passive || !m_activated)
        {
            return 0;
        }
        if (!m_registered)
        {
            if (!m_actions.publishGatewayStarted(m_gatewayId))
            {
                return 0;
            }
            m_registered = true;
        }
        if (!m_actions.openGate())
        {
            return 0;
        }
        m_state = State::Serving;
        return 1;
    }

    [[nodiscard]] State state() const noexcept
    {
        return m_state;
    }

    [[nodiscard]] bool isServing() const noexcept
    {
        return m_state == State::Serving;
    }

    [[nodiscard]] bool isActivated() const noexcept
    {
        return m_activated;
    }

    [[nodiscard]] std::int32_t gatewayId() const noexcept
    {
        return m_gatewayId;
    }

    [[nodiscard]] std::int32_t gatewaySourceId() const noexcept
    {
        return m_gatewaySourceId;
    }

  private:
    std::string m_gatewayName;
    Actions& m_actions;
    std::unordered_map<std::int32_t, std::int32_t> m_gatewaySourceIds; // a GatewayActive carries only the id
    State m_state = State::Replaying;
    std::int32_t m_gatewayId = UNRESOLVED;
    std::int32_t m_gatewaySourceId = UNRESOLVED;
    bool m_activated = false;
    bool m_registered = false;
};

} // namespace org::limitless::seqeron::app::detail
