// Pins PortLayout.hpp's cluster port-layout formula against the same (memberId -> port) pairs
// SequencerServerTest (Java) checks, so a change to one side without the other fails a build
// instead of drifting silently. The applications' own bases are AppPortsTest's, over
// src/main/cpp/.../AppPorts.hpp.

#include <gtest/gtest.h>

#include "org/limitless/phixeron/sequencer/PortLayout.hpp"

namespace {

using namespace org::limitless::phixeron::sequencer;

TEST(PortLayout, ClusterMemberPortsMatchDocumentedLayout)
{
    EXPECT_EQ(9301, clusterArchivePort(0));
    EXPECT_EQ(9302, clusterIngressPort(0));
    EXPECT_EQ(9303, clusterConsensusPort(0));
    EXPECT_EQ(9304, clusterLogPort(0));
    EXPECT_EQ(9305, clusterTransferPort(0));

    EXPECT_EQ(9311, clusterArchivePort(1));
    EXPECT_EQ(9312, clusterIngressPort(1));

    EXPECT_EQ(9321, clusterArchivePort(2));
    EXPECT_EQ(9322, clusterIngressPort(2));
}

// The reservation is wider than what three members bind, and products check themselves against it
// (doc/registries.md §2). Pinned here so it cannot quietly narrow back to 9325.
TEST(PortLayout, ReservedBlockCoversThreeMemberStrides)
{
    EXPECT_EQ(9300, CLUSTER_PORT_BLOCK_FIRST);
    EXPECT_EQ(9329, CLUSTER_PORT_BLOCK_LAST);

    EXPECT_TRUE(isClusterPort(9300));
    EXPECT_TRUE(isClusterPort(clusterTransferPort(2)));
    EXPECT_TRUE(isClusterPort(9320)); // member 2's base — reserved though no member binds it
    EXPECT_TRUE(isClusterPort(9329));
    EXPECT_FALSE(isClusterPort(9299));
    EXPECT_FALSE(isClusterPort(9330));
}

TEST(PortLayout, ArchiveEndpointsCsvMatchesThreeNodeLayout)
{
    EXPECT_EQ("localhost:9301,localhost:9311,localhost:9321", archiveEndpointsCsv(3));
    EXPECT_EQ("localhost:9301", archiveEndpointsCsv(1));
}

} // namespace
