#pragma once

#include <algorithm>
#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "org/limitless/seqeron/protocol/Snapshot.hpp"

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
        Row row{ rowGatewayId, rowGatewaySourceId, preferenceRank, std::string(rowName) };
        if (const auto existing = findRow(rowGatewayId); existing != m_rows.end())
        {
            *existing = std::move(row);
        }
        else
        {
            m_rows.push_back(std::move(row));
        }
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
        const auto target = findRow(targetGatewayId);
        if (target == m_rows.end() || target->gatewaySourceId != m_gatewaySourceId)
        {
            return;
        }
        m_activeGatewayId = targetGatewayId;
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

    // Takes the election state a snapshot header holds in place of the frames before its cut (doc/snapshot.md §6):
    // the pair's rows, which resolve this instance's identity by name, and the instance its GatewayActive names.
    // Before the gate has opened, so nothing is published or closed.
    void onSnapshotHeader(const protocol::SnapshotGatewayState& state)
    {
        m_rows.clear();
        for (const protocol::SnapshotGatewayRow& row : state.rows)
        {
            onGatewayRegistered(row.gatewayId, state.gatewaySourceId, row.gatewayName, row.preferenceRank);
        }
        m_activeGatewayId = state.activeGatewayId;
        m_activated = m_gatewayId != UNRESOLVED && m_activeGatewayId == m_gatewayId;
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

    // Whether this activation's GatewayStarted has been placed, binding the session to the pair's frames.
    [[nodiscard]] bool isAnnounced() const noexcept
    {
        return m_activated && m_registered;
    }

    // The instance of this pair the last GatewayActive named, or SnapshotHeader::NO_GATEWAY.
    [[nodiscard]] std::int32_t activeGatewayId() const noexcept
    {
        return m_activeGatewayId;
    }

    // Every row of this instance's pair, in list order: what a snapshot header carries.
    [[nodiscard]] std::vector<protocol::SnapshotGatewayRow> pairRows() const
    {
        std::vector<protocol::SnapshotGatewayRow> pair;
        for (const Row& row : m_rows)
        {
            if (row.gatewaySourceId == m_gatewaySourceId)
            {
                pair.push_back({ row.gatewayId, static_cast<std::uint8_t>(row.preferenceRank), row.gatewayName });
            }
        }
        return pair;
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
    struct Row
    {
        std::int32_t gatewayId;
        std::int32_t gatewaySourceId;
        std::int32_t preferenceRank;
        std::string gatewayName;
    };

    typename std::vector<Row>::iterator findRow(const std::int32_t gatewayId)
    {
        return std::ranges::find(m_rows, gatewayId, &Row::gatewayId);
    }

    std::string m_gatewayName;
    Actions& m_actions;
    // Every row, in list order, a re-published row replacing its earlier one in place as the sequencer's list does: a
    // GatewayActive carries only the id, and a snapshot header every row of the pair.
    std::vector<Row> m_rows;
    State m_state = State::Replaying;
    std::int32_t m_gatewayId = UNRESOLVED;
    std::int32_t m_gatewaySourceId = UNRESOLVED;
    bool m_activated = false;
    bool m_registered = false;
    std::int32_t m_activeGatewayId = protocol::SnapshotHeader::NO_GATEWAY;
};

} // namespace org::limitless::seqeron::app::detail
