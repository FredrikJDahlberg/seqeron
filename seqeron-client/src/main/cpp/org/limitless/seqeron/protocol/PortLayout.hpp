#pragma once

// The cluster port-layout formula — the C++ mirror of PortLayout.java and ports.sh; SequencerServerTest
// and PortLayoutTest pin the same (memberId -> port) pairs. Cluster member ports only (doc/ops.md, "Ports").

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

// How a process reaches the archive in its own Aeron directory, and that link's control stream. The Java
// twin is PortLayout's ARCHIVE_CONTROL_CHANNEL/ARCHIVE_CONTROL_STREAM_ID; both sides must name one id.
inline constexpr const char* ARCHIVE_CONTROL_CHANNEL = "aeron:ipc";
inline constexpr std::int32_t ARCHIVE_CONTROL_STREAM_ID = 100;

/**
 * Parses and validates a cluster port base. The pure seam over the environment read, so the rules are
 * testable; a bad value throws here rather than surfacing later as a bind error.
 *
 * @param raw the SEQERON_PORT_BASE value, or nullptr when unset
 * @return the base, DEFAULT_CLUSTER_PORT_BASE when raw is unset or empty
 * @throws std::invalid_argument if raw is not a number, is below 1024, or leaves no room for the block
 */
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

// Core's reserved block (doc/ops.md, "Ports"): three members wide, one stride each — wider than the
// base+1..base+25 three members bind.
inline int clusterPortBlockFirst()
{
    return clusterPortBase();
}
inline int clusterPortBlockLast()
{
    return clusterPortBase() + CLUSTER_PORT_BLOCK_WIDTH - 1;
}

/**
 * Whether a port is inside core's reserved block; for a product checking that its own bases sit outside it.
 *
 * @param port the port to check
 */
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

/**
 * Builds the "host:port,..." archive-endpoint CSV for a cluster on one host. Every member's archive holds a
 * complete recording, so any reachable one will do.
 *
 * @param nodeCount the cluster's member count
 * @param host      the host every member runs on
 */
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

/**
 * Builds a UDP channel URI. The Java twin is SequencerServer's own udp(host, port).
 *
 * @param endpoint the channel's endpoint, "host:port"
 */
inline std::string udpChannel(const std::string& endpoint)
{
    return "aeron:udp?endpoint=" + endpoint;
}

} // namespace org::limitless::seqeron::protocol
