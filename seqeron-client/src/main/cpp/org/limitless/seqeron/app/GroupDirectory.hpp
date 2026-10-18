#pragma once

#include <array>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "org/limitless/seqeron/protocol/SequencedFrame.hpp"
#include "org/limitless/seqeron/protocol/Snapshot.hpp"

namespace org::limitless::seqeron::app {

namespace detail {
class GroupResolution;
} // namespace detail

/**
 * The log's group table, as a producer addresses it (spec A-9): a group's name to its bit of groupMask, and a
 * groupId to its name. Immutable: the groups are fixed by the first topology load (S-8), so one directory holds for
 * the life of the archive. Combine masks with |.
 *
 * The Java twin is app/GroupDirectory.java; keep the two in step.
 */
class GroupDirectory
{
  public:
    /**
     * A group's bit.
     *
     * @param name the group's name in the topology
     * @return its bit of groupMask, or 0 if the table names no such group
     */
    [[nodiscard]] std::uint16_t mask(const std::string_view name) const
    {
        for (std::size_t slot = 0; slot < m_names.size(); ++slot)
        {
            if (!m_names[slot].empty() && m_names[slot] == name)
            {
                return static_cast<std::uint16_t>(1U << slot);
            }
        }
        return 0;
    }

    /**
     * A group's name, to label it.
     *
     * @param groupId 1 to 16
     * @return its name, or empty if the table names no such group
     */
    [[nodiscard]] std::string_view name(const std::int32_t groupId) const
    {
        return groupId < 1 || groupId > protocol::MAX_GROUPS ? std::string_view{} : m_names[groupId - 1];
    }

  private:
    friend class detail::GroupResolution;

    explicit GroupDirectory(const std::vector<protocol::SnapshotGroup>& groups)
    {
        for (const protocol::SnapshotGroup& group : groups)
        {
            m_names[group.groupId - 1] = group.groupName;
            m_activeMask |= bit(group.groupId);
        }
    }

    static std::uint16_t bit(const std::int32_t groupId)
    {
        return static_cast<std::uint16_t>(1U << (groupId - 1));
    }

    std::array<std::string, protocol::MAX_GROUPS> m_names;
    std::uint16_t m_activeMask = 0;
};

} // namespace org::limitless::seqeron::app
