#pragma once

// Canonical cluster port-layout formula — the C++ mirror of SequencerServer's
// PORT_BASE + memberId*10 + offset scheme (see SequencerServer.java's class Javadoc, the
// source of truth both sides cite). Cluster
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

#include <charconv>
#include <cstdint>
#include <cstdlib>
#include <stdexcept>
#include <string>
#include <string_view>

namespace org::limitless::seqeron::sequencer {

// ── Aeron Cluster member ports: archive / ingress / consensus / log / transfer ───────────────
// The stride is fixed; the BASE is a deployment knob, SEQERON_PORT_BASE, read by all three mirrors
// (this header, PortLayout.java, ports.sh). Set it identically for every seqeron process on every
// host: a node that disagrees with a client about the base binds and dials different ports, and the
// symptom is a connection that never completes rather than an error naming the cause.
//
// Configurable and constexpr are mutually exclusive, so these are functions rather than the constants
// they used to be. Nothing in either repo static_asserts on them; a consumer that did has to move the
// assertion to a runtime check.
inline constexpr int CLUSTER_PORT_STRIDE = 10;
inline constexpr int DEFAULT_CLUSTER_PORT_BASE = 9300;
inline constexpr int CLUSTER_PORT_BLOCK_WIDTH = 3 * CLUSTER_PORT_STRIDE;
inline constexpr const char* ENV_PORT_BASE = "SEQERON_PORT_BASE";

// Pure seam over the environment read — the lookup is the caller's, so the rules are testable without
// touching the environment (the C++ twin of PortLayout.resolveClusterPortBase). A bad value throws
// here rather than surfacing later as a bind error on a port nobody chose.
inline int parseClusterPortBase(const char* raw)
{
    if (raw == nullptr || *raw == '\0')
    {
        return DEFAULT_CLUSTER_PORT_BASE;
    }

    const std::string_view text{ raw };
    int base = 0;
    const auto [end, ec] = std::from_chars(text.data(), text.data() + text.size(), base);
    if (ec != std::errc{} || end != text.data() + text.size())
    {
        throw std::invalid_argument(std::string{ ENV_PORT_BASE } + " is not a number: '" + raw + "'");
    }
    if (base < 1024)
    {
        throw std::invalid_argument(std::string{ ENV_PORT_BASE } + "=" + std::to_string(base) +
                                    " is below 1024 (privileged ports)");
    }
    if (base + CLUSTER_PORT_BLOCK_WIDTH - 1 > 65535)
    {
        throw std::invalid_argument(std::string{ ENV_PORT_BASE } + "=" + std::to_string(base) +
                                    " leaves no room for the " + std::to_string(CLUSTER_PORT_BLOCK_WIDTH) +
                                    "-port cluster block below 65535");
    }
    return base;
}

// Read once: the environment cannot change under a running process, and this sits on every port
// computation.
inline int clusterPortBase()
{
    static const int base = parseClusterPortBase(std::getenv(ENV_PORT_BASE));
    return base;
}

// Core's reserved block (doc/registries.md §2). Three members wide, one stride each — NOT the
// base+1..base+25 a three-node cluster happens to bind, which is what every restatement of this
// boundary used to say and how an application port ended up squatting on the third member's base.
inline int clusterPortBlockFirst()
{
    return clusterPortBase();
}
inline int clusterPortBlockLast()
{
    return clusterPortBase() + CLUSTER_PORT_BLOCK_WIDTH - 1;
}

// For a product asserting its own bases sit outside core's block, so the boundary is read from
// here rather than copied.
inline bool isClusterPort(int port)
{
    return port >= clusterPortBlockFirst() && port <= clusterPortBlockLast();
}

inline int clusterMemberPortBase(int memberId)
{
    return clusterPortBase() + memberId * CLUSTER_PORT_STRIDE;
}
inline std::uint16_t clusterArchivePort(int memberId)
{
    return static_cast<std::uint16_t>(clusterMemberPortBase(memberId) + 1);
}
inline std::uint16_t clusterIngressPort(int memberId)
{
    return static_cast<std::uint16_t>(clusterMemberPortBase(memberId) + 2);
}
inline std::uint16_t clusterConsensusPort(int memberId)
{
    return static_cast<std::uint16_t>(clusterMemberPortBase(memberId) + 3);
}
inline std::uint16_t clusterLogPort(int memberId)
{
    return static_cast<std::uint16_t>(clusterMemberPortBase(memberId) + 4);
}
inline std::uint16_t clusterTransferPort(int memberId)
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
