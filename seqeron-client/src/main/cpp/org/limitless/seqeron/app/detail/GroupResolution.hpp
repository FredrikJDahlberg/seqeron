#pragma once

#include <algorithm>
#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "org/limitless/seqeron/app/GroupDirectory.hpp"
#include "org/limitless/seqeron/protocol/SequencedFrame.hpp"
#include "org/limitless/seqeron/protocol/Snapshot.hpp"

#include "org_limitless_seqeron_sbe_frame/GroupsRegistered.h"

namespace org::limitless::seqeron::app::detail {

/**
 * The groups a producer addresses (spec A-9): the latest group table on its queue, or its snapshot header's when
 * none follows the cut, resolved once when it has caught up, and the mask each frame it publishes then carries.
 * Holds no Aeron runtime; Session drives it. The Java twin is app/GroupResolution.java; keep the two in step.
 */
class GroupResolution
{
  public:
    // What frameMask returns for a mask naming a group the table lacks.
    static constexpr std::int32_t NOT_ADDRESSABLE = -1;

    // @param addressedGroups the names of the other groups the producer publishes to
    explicit GroupResolution(std::vector<std::string> addressedGroups) : m_addressedGroups{ std::move(addressedGroups) }
    {}

    /**
     * A GroupsRegistered off the queue; the latest is the table.
     *
     * @param message the message's first byte
     * @param length  its length
     */
    void onGroupsRegistered(const char* const message, const std::uint64_t length)
    {
        const auto groupsRegistered = protocol::decodeSystem<sbe::frame::GroupsRegistered>(message, length);
        const char* const names = groupsRegistered.groupNames();
        std::vector<protocol::SnapshotGroup> groups;
        for (std::int32_t slot = 0; slot < protocol::MAX_GROUPS; ++slot)
        {
            const char* const name = names + slot * NAME_LENGTH;
            const std::size_t nameLength = std::find(name, name + NAME_LENGTH, '\0') - name;
            if (nameLength > 0)
            {
                groups.push_back(protocol::SnapshotGroup{ slot + 1, std::string(name, nameLength) });
            }
        }
        m_table = std::move(groups);
    }

    // A restore's header, standing in for the GroupsRegistered before its cut.
    void onSnapshotHeader(const std::vector<protocol::SnapshotGroup>& groups)
    {
        m_table = groups;
    }

    // The table so far, for a snapshot header.
    [[nodiscard]] const std::vector<protocol::SnapshotGroup>& table() const
    {
        return m_table;
    }

    /**
     * Resolves the addressed names against the table.
     *
     * @param ownGroupId the group whose queue the producer reads
     * @return the first addressed name the table lacks, which leaves this unresolved, or empty
     */
    std::optional<std::string> resolve(const std::int32_t ownGroupId)
    {
        GroupDirectory resolved{ m_table };
        for (const std::string& name : m_addressedGroups)
        {
            if (resolved.mask(name) == 0)
            {
                return name;
            }
        }
        m_directory = std::move(resolved);
        m_ownBit = GroupDirectory::bit(ownGroupId);
        return std::nullopt;
    }

    // The resolved directory, or null.
    [[nodiscard]] const GroupDirectory* directory() const
    {
        return m_directory ? &*m_directory : nullptr;
    }

    /**
     * The mask a frame carries: the groups it addresses and the producer's own, since confirmed ingress watches the
     * own queue. Only once resolved.
     *
     * @param groupMask the groups addressed, 0 for the own group alone
     * @return the frame's mask, or NOT_ADDRESSABLE
     */
    [[nodiscard]] std::int32_t frameMask(const std::uint16_t groupMask) const
    {
        return (groupMask & ~m_directory->m_activeMask) != 0 ? NOT_ADDRESSABLE : groupMask | m_ownBit;
    }

  private:
    static constexpr std::size_t NAME_LENGTH = sbe::frame::GroupsRegistered::groupNamesLength() / protocol::MAX_GROUPS;

    std::vector<std::string> m_addressedGroups;
    std::vector<protocol::SnapshotGroup> m_table;
    std::optional<GroupDirectory> m_directory;
    std::uint16_t m_ownBit = 0;
};

} // namespace org::limitless::seqeron::app::detail
