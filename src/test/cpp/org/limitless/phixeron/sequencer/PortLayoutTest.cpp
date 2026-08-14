// Pins PortLayout.hpp's port-layout formula against the same (memberId -> port) pairs
// SequencerNodeTest (Java) checks, so a change to one side without the other fails a build
// instead of drifting silently — see PortLayout.hpp's comment for how that drift happened
// before (FixGateway's and BasicDataClient's egress ports both defaulting to 9340+memberId).

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

TEST(PortLayout, ArchiveEndpointsCsvMatchesThreeNodeLayout)
{
    EXPECT_EQ("localhost:9301,localhost:9311,localhost:9321", archiveEndpointsCsv(3));
    EXPECT_EQ("localhost:9301", archiveEndpointsCsv(1));
}

TEST(PortLayout, SatellitePortsAreDistinctPerRole)
{
    EXPECT_EQ(9000, fixTcpPort(0));
    EXPECT_EQ(9001, fixTcpPort(1));
    EXPECT_EQ(9330, orderExecEgressPort(0));
    EXPECT_EQ(9340, fixGatewayEgressPort(0));
    EXPECT_EQ(9350, basicDataEgressPort(0));

    // The bug this header fixed: OrderExecClient/FixGateway/BasicDataClient co-located egress
    // ports must never collide for the same memberId.
    for (int memberId = 0; memberId < 3; ++memberId)
    {
        EXPECT_NE(orderExecEgressPort(memberId), fixGatewayEgressPort(memberId));
        EXPECT_NE(orderExecEgressPort(memberId), basicDataEgressPort(memberId));
        EXPECT_NE(fixGatewayEgressPort(memberId), basicDataEgressPort(memberId));
    }
}

} // namespace
