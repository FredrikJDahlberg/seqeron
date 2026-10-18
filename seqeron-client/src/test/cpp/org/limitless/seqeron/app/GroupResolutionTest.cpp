// The groups a producer addresses (spec A-9), resolved from its queue's table. Case for case with
// GroupResolutionTest.java and GroupResolutionTest.cs, in the same order.

#include <algorithm>
#include <array>
#include <vector>

#include <gtest/gtest.h>

#include "org/limitless/seqeron/app/detail/GroupResolution.hpp"

namespace org::limitless::seqeron::app::detail {
namespace {

using Groups = std::vector<protocol::SnapshotGroup>;

const Groups TABLE{ { 1, "orders" }, { 3, "risk" } };

void groupsRegistered(GroupResolution& resolution, const Groups& groups)
{
    std::array<char, sbe::frame::GroupsRegistered::sbeBlockLength()> body{};
    constexpr std::size_t slotLength = sbe::frame::GroupsRegistered::groupNamesLength() / protocol::MAX_GROUPS;
    for (const protocol::SnapshotGroup& group : groups)
    {
        std::copy(group.groupName.begin(), group.groupName.end(), body.begin() + (group.groupId - 1) * slotLength);
    }
    resolution.onGroupsRegistered(body.data(), body.size());
}

TEST(GroupResolution, TheTableIsTheLatestGroupsRegistered)
{
    GroupResolution resolution{ {} };
    groupsRegistered(resolution, { { 2, "audit" } });
    groupsRegistered(resolution, TABLE);

    EXPECT_EQ(TABLE, resolution.table());
}

TEST(GroupResolution, ARestoredHeaderStandsInForTheGroupsRegistered)
{
    GroupResolution resolution{ {} };
    resolution.onSnapshotHeader(TABLE);
    EXPECT_EQ(TABLE, resolution.table());

    groupsRegistered(resolution, { { 1, "orders" } }); // one after the cut
    EXPECT_EQ((Groups{ { 1, "orders" } }), resolution.table());
}

TEST(GroupResolution, TheDirectoryNamesEveryGroupOfTheTable)
{
    GroupResolution resolution{ { "risk" } };
    resolution.onSnapshotHeader(TABLE);

    EXPECT_FALSE(resolution.resolve(1));
    const GroupDirectory& directory = *resolution.directory();
    EXPECT_EQ(1, directory.mask("orders"));
    EXPECT_EQ(4, directory.mask("risk"));
    EXPECT_EQ(0, directory.mask("audit"));
    EXPECT_EQ("risk", directory.name(3));
    EXPECT_TRUE(directory.name(2).empty());
    EXPECT_TRUE(directory.name(0).empty());
    EXPECT_TRUE(directory.name(17).empty());
}

TEST(GroupResolution, AnAddressedNameTheTableLacksLeavesTheGroupsUnresolved)
{
    GroupResolution resolution{ { "risk", "audit" } };
    resolution.onSnapshotHeader(TABLE);

    EXPECT_EQ("audit", resolution.resolve(1));
    EXPECT_EQ(nullptr, resolution.directory());
}

TEST(GroupResolution, EveryFrameCarriesTheOwnGroupsBit)
{
    GroupResolution resolution{ {} };
    resolution.onSnapshotHeader(TABLE);
    resolution.resolve(3);

    EXPECT_EQ(4, resolution.frameMask(0));
    EXPECT_EQ(5, resolution.frameMask(1));
    EXPECT_EQ(4, resolution.frameMask(4));
}

TEST(GroupResolution, AMaskNamingAGroupTheTableLacksIsNotAddressable)
{
    GroupResolution resolution{ {} };
    resolution.onSnapshotHeader(TABLE);
    resolution.resolve(1);

    EXPECT_EQ(GroupResolution::NOT_ADDRESSABLE, resolution.frameMask(2));
    EXPECT_EQ(GroupResolution::NOT_ADDRESSABLE, resolution.frameMask(1U << 15 | 1U));
}

} // namespace
} // namespace org::limitless::seqeron::app::detail
