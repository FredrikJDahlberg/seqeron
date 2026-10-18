// The groups a producer addresses (spec A-9), resolved from its queue's table. Case for case with
// GroupResolutionTest.java and GroupResolutionTest.cs, in the same order.

#include <algorithm>
#include <array>
#include <string>
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

    EXPECT_EQ(GroupResolution::Outcome::Resolved, resolution.onCaughtUp(1));
    const GroupDirectory& directory = *resolution.directory();
    EXPECT_EQ(1, directory.mask("orders"));
    EXPECT_EQ(4, directory.mask("risk"));
    EXPECT_EQ(0, directory.mask("audit"));
    EXPECT_EQ(0, directory.mask("")) << "no slot's name is empty";
    EXPECT_EQ("risk", directory.name(3));
    EXPECT_TRUE(directory.name(2).empty());
    EXPECT_TRUE(directory.name(0).empty());
    EXPECT_TRUE(directory.name(17).empty());
}

TEST(GroupResolution, AnAddressedNameTheTableLacksLeavesTheGroupsUnresolved)
{
    GroupResolution resolution{ { "risk", "audit" } };
    resolution.onSnapshotHeader(TABLE);

    EXPECT_EQ(GroupResolution::Outcome::GroupUnknown, resolution.onCaughtUp(1));
    EXPECT_EQ("audit", resolution.unknownGroup());
    EXPECT_EQ(nullptr, resolution.directory());
    EXPECT_EQ(GroupResolution::UNRESOLVED, resolution.frameMask(0)) << "nothing is published";
}

TEST(GroupResolution, EveryFrameCarriesTheOwnGroupsBit)
{
    GroupResolution resolution{ {} };
    resolution.onSnapshotHeader(TABLE);
    resolution.onCaughtUp(3);

    EXPECT_EQ(4, resolution.frameMask(0));
    EXPECT_EQ(5, resolution.frameMask(1));
    EXPECT_EQ(4, resolution.frameMask(4));
}

TEST(GroupResolution, AMaskNamingAGroupTheTableLacksIsNotAddressable)
{
    GroupResolution resolution{ {} };
    resolution.onSnapshotHeader(TABLE);
    resolution.onCaughtUp(1);

    EXPECT_EQ(GroupResolution::NOT_ADDRESSABLE, resolution.frameMask(2));
    EXPECT_EQ(GroupResolution::NOT_ADDRESSABLE, resolution.frameMask(1U << 15 | 1U));
}

TEST(GroupResolution, NothingIsAddressableBeforeTheFirstCatchUp)
{
    GroupResolution resolution{ {} };
    resolution.onSnapshotHeader(TABLE);

    EXPECT_EQ(GroupResolution::UNRESOLVED, resolution.frameMask(0));
    EXPECT_EQ(GroupResolution::UNRESOLVED, resolution.frameMask(1));
}

TEST(GroupResolution, OnlyTheFirstCatchUpResolves)
{
    GroupResolution resolution{ {} };
    resolution.onSnapshotHeader(TABLE);
    EXPECT_EQ(GroupResolution::Outcome::Resolved, resolution.onCaughtUp(1));
    const GroupDirectory* const directory = resolution.directory();

    groupsRegistered(resolution, { { 2, "audit" } });
    EXPECT_EQ(GroupResolution::Outcome::Unchanged, resolution.onCaughtUp(1));
    EXPECT_EQ(directory, resolution.directory());
    EXPECT_EQ(GroupResolution::NOT_ADDRESSABLE, resolution.frameMask(2)) << "the mask resolved once still holds";
}

TEST(GroupResolution, TheHighestGroupWithANameThatFillsItsSlotResolves)
{
    const std::string name(32, 'g');
    GroupResolution resolution{ { name } };
    groupsRegistered(resolution, { { 1, "orders" }, { 16, name } });

    EXPECT_EQ(GroupResolution::Outcome::Resolved, resolution.onCaughtUp(16));
    EXPECT_EQ(1U << 15, resolution.directory()->mask(name));
    EXPECT_EQ(name, resolution.directory()->name(16));
    EXPECT_EQ(1 << 15, resolution.frameMask(0));
    EXPECT_EQ(1 << 15 | 1, resolution.frameMask(1));
}

} // namespace
} // namespace org::limitless::seqeron::app::detail
