#pragma once

// Canonical port-layout formula shared by every phixeron process — the C++ mirror of
// SequencerNode's PORT_BASE + memberId*10 + offset scheme (see SequencerNode.java's class
// Javadoc and doc/design.md's "Port layout" section, the source of truth both sides cite).
// Every other port constant used to be an independently hand-typed literal restating this
// formula (or a satellite base "known" to sit outside it) — that drift is how FixGateway's and
// BasicDataClient's co-located egress ports ended up both defaulting to 9340+memberId. Kept in
// sync deliberately now: SequencerNodeTest (Java) and PortLayoutTest (here) each pin the same
// (memberId -> port) pairs so a change to one side without the other fails a build.

#include <cstdint>
#include <string>

namespace org::limitless::phixeron::sequencer {

// ── Aeron Cluster member ports: archive / ingress / consensus / log / transfer ───────────────
inline constexpr int CLUSTER_PORT_BASE = 9300;
inline constexpr int CLUSTER_PORT_STRIDE = 10;

constexpr int
clusterMemberPortBase(int memberId)
{
    return CLUSTER_PORT_BASE + memberId * CLUSTER_PORT_STRIDE;
}
constexpr std::uint16_t
clusterArchivePort(int memberId)
{
    return static_cast<std::uint16_t>(clusterMemberPortBase(memberId) + 1);
}
constexpr std::uint16_t
clusterIngressPort(int memberId)
{
    return static_cast<std::uint16_t>(clusterMemberPortBase(memberId) + 2);
}
constexpr std::uint16_t
clusterConsensusPort(int memberId)
{
    return static_cast<std::uint16_t>(clusterMemberPortBase(memberId) + 3);
}
constexpr std::uint16_t
clusterLogPort(int memberId)
{
    return static_cast<std::uint16_t>(clusterMemberPortBase(memberId) + 4);
}
constexpr std::uint16_t
clusterTransferPort(int memberId)
{
    return static_cast<std::uint16_t>(clusterMemberPortBase(memberId) + 5);
}

// Builds the "host:port,host:port,..." archive-endpoint CSV for a nodeCount-member cluster, all
// on one host — the C++ mirror of SequencerNode.buildClusterMembers's archive column. Every
// member's co-located archive independently holds a complete recording of the tap (see
// SequencerNode's class Javadoc), so any reachable member's endpoint here works equally well.
inline std::string
archiveEndpointsCsv(int nodeCount, const char* host = "localhost")
{
    std::string csv;
    for (int id = 0; id < nodeCount; ++id)
    {
        if (id > 0)
        {
            csv += ',';
        }
        csv += host;
        csv += ':';
        csv += std::to_string(clusterArchivePort(id));
    }
    return csv;
}

// ── Satellite ports: one dedicated base per client role, deliberately outside the cluster's own
//    9300-9325 (3-node) block; offset by memberId for a role that runs one co-located replica per
//    node. Two independent Aeron media-driver processes on one host cannot bind the same UDP
//    port, which is why every co-located client needs its own port here in the first place. ────
inline constexpr std::uint16_t FIX_TCP_PORT_BASE = 9000; // FixGateway TCP listen port
inline constexpr std::uint16_t FIX_TEST_CLIENT_EGRESS_PORT =
    9320; // fix_test_server's own (non-colocated) cluster egress
inline constexpr std::uint16_t ORDER_EXEC_EGRESS_PORT_BASE = 9330;   // OrderExecClient co-located egress
inline constexpr std::uint16_t FIX_GATEWAY_EGRESS_PORT_BASE = 9340;  // FixGateway co-located egress
inline constexpr std::uint16_t BASICDATA_EGRESS_PORT_BASE = 9350;    // BasicDataClient co-located egress
inline constexpr std::uint16_t RISK_TEST_REPLAY_PORT_DEFAULT = 9400; // fix_test_server risk-test replay
inline constexpr std::uint16_t RESEND_REPLAY_PORT_DEFAULT = 9401;    // FixGateway resend-recovery replay

constexpr std::uint16_t
fixTcpPort(int gatewayIndex = 0)
{
    return static_cast<std::uint16_t>(FIX_TCP_PORT_BASE + gatewayIndex);
}
constexpr std::uint16_t
orderExecEgressPort(int memberId)
{
    return static_cast<std::uint16_t>(ORDER_EXEC_EGRESS_PORT_BASE + memberId);
}
constexpr std::uint16_t
fixGatewayEgressPort(int memberId)
{
    return static_cast<std::uint16_t>(FIX_GATEWAY_EGRESS_PORT_BASE + memberId);
}
constexpr std::uint16_t
basicDataEgressPort(int memberId)
{
    return static_cast<std::uint16_t>(BASICDATA_EGRESS_PORT_BASE + memberId);
}

} // namespace org::limitless::phixeron::sequencer
