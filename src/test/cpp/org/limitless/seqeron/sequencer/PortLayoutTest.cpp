// Pins PortLayout.hpp's cluster port-layout formula against the same (memberId -> port) pairs
// SequencerServerTest (Java) checks, so a change to one side without the other fails a build
// instead of drifting silently. The applications' own bases are AppPortsTest's, over
// src/main/cpp/.../AppPorts.hpp.

#include <gtest/gtest.h>

#include "org/limitless/seqeron/sequencer/PortLayout.hpp"

namespace {

using namespace org::limitless::seqeron::sequencer;

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
    EXPECT_EQ(9300, clusterPortBlockFirst());
    EXPECT_EQ(9329, clusterPortBlockLast());

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

// The base is a deployment knob (SEQERON_PORT_BASE), and these pin the rules that decide it. Driven
// through the pure seam rather than the environment, which clusterPortBase() reads once into a local
// static. The Java twin is PortLayoutTest.java — same rules, same boundaries.
TEST(PortLayout, ParseClusterPortBaseFallsBackWhenUnset)
{
    EXPECT_EQ(DEFAULT_CLUSTER_PORT_BASE, parseClusterPortBase(nullptr));
    EXPECT_EQ(9300, parseClusterPortBase(""));
}

TEST(PortLayout, ParseClusterPortBaseTakesAValidBaseAsGiven)
{
    EXPECT_EQ(20000, parseClusterPortBase("20000"));
}

TEST(PortLayout, ParseClusterPortBaseRejectsNonNumeric)
{
    EXPECT_THROW(parseClusterPortBase("9300x"), std::invalid_argument);
    EXPECT_THROW(parseClusterPortBase("nine"), std::invalid_argument);
}

// 1024 is the first unprivileged port and the block is 30 wide, so 65506 is the last base that fits.
TEST(PortLayout, ParseClusterPortBaseRequiresRoomForTheWholeBlock)
{
    EXPECT_THROW(parseClusterPortBase("1023"), std::invalid_argument);
    EXPECT_EQ(1024, parseClusterPortBase("1024"));

    EXPECT_EQ(65506, parseClusterPortBase("65506"));
    EXPECT_THROW(parseClusterPortBase("65507"), std::invalid_argument);
}

} // namespace
