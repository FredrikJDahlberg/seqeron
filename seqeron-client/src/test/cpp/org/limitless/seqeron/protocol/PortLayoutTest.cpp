// Pins PortLayout.hpp's cluster port-layout formula against the same (memberId -> port) pairs
// SequencerServerTest (Java) checks, so a change to one side without the other fails a build
// instead of drifting silently. Applications' own bases are their own to test.

#include <gtest/gtest.h>

#include "org/limitless/seqeron/protocol/PortLayout.hpp"

namespace {

using namespace org::limitless::seqeron::protocol;

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

// The reservation is wider than what seven members bind, and applications check themselves against it
// (doc/ops.md, "Ports"). Pinned here so it cannot quietly narrow back to 9365.
TEST(PortLayout, ReservedBlockCoversSevenMemberStrides)
{
    EXPECT_EQ(9300, clusterPortBlockFirst());
    EXPECT_EQ(9369, clusterPortBlockLast());

    EXPECT_TRUE(isClusterPort(9300));
    EXPECT_TRUE(isClusterPort(clusterTransferPort(6)));
    EXPECT_TRUE(isClusterPort(9360)); // member 6's base — reserved though no member binds it
    EXPECT_TRUE(isClusterPort(9369));
    EXPECT_FALSE(isClusterPort(9299));
    EXPECT_FALSE(isClusterPort(9370));
}

TEST(PortLayout, ArchiveEndpointsCsvMatchesThreeNodeLayout)
{
    EXPECT_EQ("localhost:9301,localhost:9311,localhost:9321", archiveEndpointsCsv(3));
    EXPECT_EQ("localhost:9301", archiveEndpointsCsv(1));
}

// The same string PortLayout.ingressEndpoints builds, so one deployment's set reads the same in both languages.
TEST(PortLayout, IngressEndpointsCsvMatchesThreeNodeLayout)
{
    EXPECT_EQ("0=localhost:9302,1=localhost:9312,2=localhost:9322", ingressEndpointsCsv(3));
    EXPECT_EQ("0=localhost:9302", ingressEndpointsCsv(1));
    EXPECT_EQ(ingressEndpointsCsv(DEFAULT_MEMBER_COUNT), ingressEndpointsCsv());
    EXPECT_EQ("0=host0:9302,1=host0:9312", ingressEndpointsCsv(2, "host0"));
}

// Member i of a host list runs on entry i; the Java twin is PortLayoutTest.java's hostList* cases.
TEST(PortLayout, HostListEndpointSet)
{
    EXPECT_EQ("0=h0:9302,1=h1:9312,2=h2:9322", ingressEndpointsCsv(parseHosts("h0, h1 ,h2")));
    EXPECT_EQ(ingressEndpointsCsv(1), ingressEndpointsCsv(std::vector<std::string>{ "localhost" }));
}

// SEQERON_HOSTS unset or blank names no host; set, it is parsed like any host list.
TEST(PortLayout, ParseClusterHostsFromTheEnvironment)
{
    EXPECT_TRUE(parseClusterHosts(nullptr).empty());
    EXPECT_TRUE(parseClusterHosts("  ").empty());
    EXPECT_EQ((std::vector<std::string>{ "h0", "h1" }), parseClusterHosts("h0,h1"));
    EXPECT_THROW(parseClusterHosts("h0,,h1"), std::invalid_argument);
}

TEST(PortLayout, HostListRules)
{
    EXPECT_EQ(std::vector<std::string>{ "h0" }, parseHosts("h0"));
    EXPECT_THROW(parseHosts(""), std::invalid_argument);
    EXPECT_THROW(parseHosts("h0,,h2"), std::invalid_argument);
    EXPECT_THROW(parseHosts("h0,h1,"), std::invalid_argument);
    EXPECT_EQ(7U, parseHosts("h0,h1,h2,h3,h4,h5,h6").size());
    EXPECT_THROW(parseHosts("h0,h1,h2,h3,h4,h5,h6,h7"), std::invalid_argument);
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

// 1024 is the first unprivileged port and the block is 70 wide, so 65466 is the last base that fits.
TEST(PortLayout, ParseClusterPortBaseRequiresRoomForTheWholeBlock)
{
    EXPECT_THROW(parseClusterPortBase("1023"), std::invalid_argument);
    EXPECT_EQ(1024, parseClusterPortBase("1024"));

    EXPECT_EQ(65466, parseClusterPortBase("65466"));
    EXPECT_THROW(parseClusterPortBase("65467"), std::invalid_argument);
}

} // namespace
