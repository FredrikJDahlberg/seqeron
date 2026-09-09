#pragma once

// Canonical cluster port-layout formula — the C++ mirror of SequencerServer's
// PORT_BASE + memberId*10 + offset scheme (see SequencerServer.java's class Javadoc and
// doc/design.md's "Port layout" section, the source of truth both sides cite). Cluster
// member ports only: the applications' own bases live in src/main/cpp/.../AppPorts.hpp,
// because a reusable sequencer must not name the processes that talk to it. Which block
// each product owns is doc/registries.md's; the only thing core states in code is its own
// reservation, below.
// Every port constant used to be an independently hand-typed literal restating this
// formula (or a satellite base "known" to sit outside it) — that drift is how FixGateway's
// and BasicDataServer's co-located egress ports ended up both defaulting to 9340+memberId.
// Kept in sync deliberately now: SequencerServerTest (Java) and PortLayoutTest (here) each
// pin the same (memberId -> port) pairs so a change to one side without the other fails a
// build.

#include <cstdint>
#include <string>

namespace org::limitless::seqeron::sequencer {

// ── Aeron Cluster member ports: archive / ingress / consensus / log / transfer ───────────────
inline constexpr int CLUSTER_PORT_BASE = 9300;
inline constexpr int CLUSTER_PORT_STRIDE = 10;

// Core's reserved block (doc/registries.md §2). Three members wide, one stride each — NOT the
// 9301-9325 a three-node cluster happens to bind, which is what every restatement of this
// boundary used to say and how an application port ended up squatting on 9320.
inline constexpr int CLUSTER_PORT_BLOCK_FIRST = CLUSTER_PORT_BASE;
inline constexpr int CLUSTER_PORT_BLOCK_LAST = CLUSTER_PORT_BASE + 3 * CLUSTER_PORT_STRIDE - 1;

// For a product asserting its own bases sit outside core's block, so the boundary is read from
// here rather than copied.
constexpr bool isClusterPort(int port)
{
    return port >= CLUSTER_PORT_BLOCK_FIRST && port <= CLUSTER_PORT_BLOCK_LAST;
}

constexpr int clusterMemberPortBase(int memberId)
{
    return CLUSTER_PORT_BASE + memberId * CLUSTER_PORT_STRIDE;
}
constexpr std::uint16_t clusterArchivePort(int memberId)
{
    return static_cast<std::uint16_t>(clusterMemberPortBase(memberId) + 1);
}
constexpr std::uint16_t clusterIngressPort(int memberId)
{
    return static_cast<std::uint16_t>(clusterMemberPortBase(memberId) + 2);
}
constexpr std::uint16_t clusterConsensusPort(int memberId)
{
    return static_cast<std::uint16_t>(clusterMemberPortBase(memberId) + 3);
}
constexpr std::uint16_t clusterLogPort(int memberId)
{
    return static_cast<std::uint16_t>(clusterMemberPortBase(memberId) + 4);
}
constexpr std::uint16_t clusterTransferPort(int memberId)
{
    return static_cast<std::uint16_t>(clusterMemberPortBase(memberId) + 5);
}

// Builds the "host:port,host:port,..." archive-endpoint CSV for a nodeCount-member cluster, all
// on one host — the C++ mirror of SequencerServer.buildClusterMembers's archive column. Every
// member's co-located archive independently holds a complete recording of the tap (see
// SequencerServer's class Javadoc), so any reachable member's endpoint here works equally well.
inline std::string archiveEndpointsCsv(int nodeCount, const char* host = "localhost")
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

} // namespace org::limitless::seqeron::sequencer
