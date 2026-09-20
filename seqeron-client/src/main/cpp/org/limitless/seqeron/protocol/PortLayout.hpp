#pragma once

// The cluster port-layout formula — the C++ mirror of PortLayout.java and ports.sh; SequencerServerTest
// and PortLayoutTest pin the same (memberId -> port) pairs. Cluster member ports only: which block each
// product owns is doc/registries.md §2's.

#include <charconv>
#include <cstdint>
#include <cstdlib>
#include <stdexcept>
#include <string>
#include <string_view>

namespace org::limitless::seqeron::protocol {

// ── Aeron Cluster member ports: archive / ingress / consensus / log / transfer ───────────────
// The base is SEQERON_PORT_BASE, read by all three mirrors; set it identically for every seqeron process.
// Functions rather than constexpr constants, since the base is configurable.
inline constexpr int CLUSTER_PORT_STRIDE = 10;
inline constexpr int DEFAULT_CLUSTER_PORT_BASE = 9300;
inline constexpr int CLUSTER_MEMBER_COUNT = 3; // a cluster is bounded at the block's width in strides
inline constexpr int CLUSTER_PORT_BLOCK_WIDTH = CLUSTER_MEMBER_COUNT * CLUSTER_PORT_STRIDE;
inline constexpr const char* ENV_PORT_BASE = "SEQERON_PORT_BASE";

// Pure seam over the environment read, so the rules are testable. A bad value throws here rather than
// surfacing later as a bind error.
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

// Read once: the environment cannot change under a running process.
inline int clusterPortBase()
{
    static const int base = parseClusterPortBase(std::getenv(ENV_PORT_BASE));
    return base;
}

// Core's reserved block (doc/registries.md §2): three members wide, one stride each — wider than the
// base+1..base+25 three members bind.
inline int clusterPortBlockFirst()
{
    return clusterPortBase();
}
inline int clusterPortBlockLast()
{
    return clusterPortBase() + CLUSTER_PORT_BLOCK_WIDTH - 1;
}

// For a product checking that its own bases sit outside core's block.
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

// The "host:port,..." archive-endpoint CSV for a nodeCount-member cluster on one host. Every member's
// archive holds a complete recording, so any reachable one will do.
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

} // namespace org::limitless::seqeron::protocol
