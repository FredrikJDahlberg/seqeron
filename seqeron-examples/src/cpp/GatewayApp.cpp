// One instance of an elected active/standby pair, written against the hosted stub, hosted::Gateway: the gateway twin
// of ColocatedApp.cpp, with the same short include list. The election, the connection id space, confirmed ingress and
// the fences are in the host process, org.limitless.seqeron.proxy.ClientProxy, which serves the pair's row by name.
//
// A gateway is the producer kind the cluster elects: the topology names both instances, the cluster designates one,
// and only that one serves. What is left here is the edge. A real gateway opens a socket in onActivated; this one takes
// a single simulated client connection instead, and pings the cluster on it once a second, reading each ping back off
// its own queue.
//
// Start a node, load the pair, start a host for the instance, then run it (in the seqeron repo, after ./gradlew
// uberJar; doc/running-a-cluster.md, "Client proxy", has the host's properties). The standby has a host and a client
// of its own, under the other name:
//   seqeron-service/src/main/scripts/start-cluster.sh
//   seqeron-service/src/main/scripts/clusterctl.sh load-topology seqeron-examples/topology.xml
//   seqeron-service/src/main/scripts/start-client-proxy.sh -Dproxy.hostName=GW-EX-A -Dproxy.gatewayName=GW-EX-A \
//        -Dproxy.sourceId=13 -Dproxy.clientId=15 -Dproxy.snapshotDir=/tmp/gw-ex-a
//   SEQERON_EXAMPLE_GATEWAY_NAME=GW-EX-A ./gateway_app
// and the same under GW-EX-B with -Dproxy.clientId=16; stop A and B takes over.
// Environment: SEQERON_EXAMPLE_GATEWAY_NAME (default GW-EX-A; the stub attaches to the host of that name),
// SEQERON_NODE_MEMBER_ID (default 0), SEQERON_AERON_DIR, SEQERON_IDLE_STRATEGY.

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <memory>
#include <string>

#include "Aeron.h"

#include "org/limitless/seqeron/app/ClusterError.hpp"
#include "org/limitless/seqeron/app/Payload.hpp"
#include "org/limitless/seqeron/hosted/Gateway.hpp"
#include "org/limitless/seqeron/protocol/Publish.hpp"
#include "org/limitless/seqeron/util/Env.hpp"
#include "org/limitless/seqeron/util/IdleStrategy.hpp"

namespace {

namespace app = org::limitless::seqeron::app;
namespace hosted = org::limitless::seqeron::hosted;
namespace protocol = org::limitless::seqeron::protocol;

// The examples' payloadId: eight raw bytes, no schema, which spec §13.2 admits.
constexpr std::uint16_t PING_PAYLOAD_ID = 6;

// The pair's sourceId (spec §5), the one GW-EX-A and GW-EX-B share in topology.xml.
constexpr std::int32_t SOURCE_ID = 13;

constexpr std::int64_t PING_INTERVAL_NS = 1'000'000'000;

constexpr const char* CLIENT_LABEL = "example-client";

// What Gateway::openConnection answers when there is none; Edge cannot name Gateway<Edge> before it is complete.
constexpr std::int32_t NO_CONNECTION = -1;

// Fenced or host lost, restart me: this repository's convention (doc/ops.md). 78: the host refused the attach, which a
// restart does not cure.
constexpr int EXIT_RESTART = 70;
constexpr int EXIT_REFUSED = 78;

std::atomic_bool running{ true };
std::string fault;

std::int64_t nowNs()
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

void onSignal(int)
{
    running.store(false, std::memory_order_relaxed);
}

// The edge: what a gateway does that the stub cannot do for it. Here, one simulated client.
struct Edge
{
    // Designated: open the edge. A real gateway binds its listen socket here; false would be retried.
    bool onActivated(const std::int32_t firstConnectionId)
    {
        std::printf("# designated, serving, connection ids from %d\n", firstConnectionId);
        open = true;
        return true;
    }

    // Stood down or fenced: close the edge and drop every connection it let in.
    void onStandby()
    {
        std::puts("# standing by");
        open = false;
        connectionId = NO_CONNECTION;
        pingSentNs = 0;
    }

    // Every application payload on this node's queue, in clusterSeqNo order, history and live alike.
    void onSequenced(const app::Payload& payload)
    {
        std::int64_t sentNs = 0;
        if (pingSentNs != 0 && payload.connectionId() == connectionId && payload.payloadId() == PING_PAYLOAD_ID &&
            payload.payloadLength() == sizeof sentNs)
        {
            std::memcpy(&sentNs, payload.payload(), sizeof sentNs);
        }
        if (sentNs != 0 && sentNs == pingSentNs)
        {
            std::printf("%lld ping echoed on connection %d, round trip %lldus\n",
                        static_cast<long long>(payload.clusterSeqNo()), payload.connectionId(),
                        static_cast<long long>((nowNs() - pingSentNs) / 1000));
        }
    }

    // This logical gateway's connections, whichever instance opened them: how a standby that keeps per-connection
    // state rebuilds it while it replays.
    void onConnectionOpened(const std::int32_t id, const char* data, const std::size_t length)
    {
        std::printf("# connection %d opened (%.*s)\n", id, static_cast<int>(length), data);
    }

    void onConnectionClosed(const std::int32_t id)
    {
        std::printf("# connection %d closed\n", id);
    }

    void onCaughtUp(const std::int64_t clusterSeqNo)
    {
        std::printf("# caught up at %lld, following the queue live\n", static_cast<long long>(clusterSeqNo));
    }

    // Latched, once: the host fenced and has exited. Exiting lets the supervisor restart both, and the standby takes
    // over meanwhile.
    void onFenced(const app::ClusterError fence, const std::string& detail)
    {
        fault = std::string(app::clusterErrorName(fence)) + ": " + detail;
    }

    bool open = false;
    std::int32_t connectionId = NO_CONNECTION;
    std::int64_t pingSentNs = 0;
};

static_assert(NO_CONNECTION == hosted::Gateway<Edge>::NO_CONNECTION);

} // namespace

int main()
{
    namespace util = org::limitless::seqeron::util;

    const std::string gatewayName = util::envString("SEQERON_EXAMPLE_GATEWAY_NAME", "GW-EX-A");
    const std::int32_t memberId = util::envInt("SEQERON_NODE_MEMBER_ID", 0);
    const std::string aeronDir = util::resolveAeronDir("SEQERON_AERON_DIR", memberId);

    // A line at a time even into a file or a pipe, as the Java and C# twins print.
    std::setvbuf(stdout, nullptr, _IOLBF, 0);
    std::signal(SIGINT, onSignal);
    std::signal(SIGTERM, onSignal);

    // The stub connects to the host's media driver and to nothing else: no ports, no cluster session.
    aeron::Context context;
    context.aeronDir(aeronDir);
    const std::shared_ptr<aeron::Aeron> aeron = aeron::Aeron::connect(context);

    Edge edge;
    hosted::Gateway<Edge>::Config config;
    config.sourceId = SOURCE_ID;
    config.hostName = gatewayName;
    hosted::Gateway<Edge> gateway{ config, edge };
    gateway.start(aeron);
    std::printf("# %s on member %d via %s\n", gatewayName.c_str(), memberId, aeronDir.c_str());

    // The one duty cycle. Every method on the stub belongs to this thread, callbacks included.
    auto idle = util::resolveIdleStrategy();
    std::int64_t nextPingNs = 0;
    while (running.load(std::memory_order_relaxed) && fault.empty() && gateway.failure() == hosted::Failure::None)
    {
        const int work = gateway.doWork();
        // canAccept() mirrors the host's last Gate: serving and not held behind a failover's resend. The moment a
        // real gateway accepts.
        if (edge.open && edge.connectionId == NO_CONNECTION && gateway.canAccept())
        {
            edge.connectionId = gateway.openConnection(CLIENT_LABEL, std::strlen(CLIENT_LABEL));
        }
        const std::int64_t now = nowNs();
        if (edge.connectionId != NO_CONNECTION && now >= nextPingNs)
        {
            // Declined until the connection's ConnectionOpened has landed, and while the window is full or the
            // stream is back-pressured. Next second's ping is the retry.
            edge.pingSentNs = now;
            if (gateway.publish(edge.connectionId, PING_PAYLOAD_ID, reinterpret_cast<const std::uint8_t*>(&now),
                                sizeof now) != protocol::Publish::Published)
            {
                edge.pingSentNs = 0;
            }
            nextPingNs = now + PING_INTERVAL_NS;
        }
        idle.idle(work);
    }
    gateway.close();

    std::fflush(stdout);
    if (!fault.empty())
    {
        std::fprintf(stderr, "# %s\n", fault.c_str());
        return EXIT_RESTART;
    }
    switch (gateway.failure())
    {
        case hosted::Failure::AttachRefused:
            std::fputs("# the host refused the attach\n", stderr);
            return EXIT_REFUSED;
        case hosted::Failure::AttachTimedOut:
            std::fputs("# no host answered the attach\n", stderr);
            return EXIT_RESTART;
        case hosted::Failure::HostLost:
            std::fputs("# HOST_LOST\n", stderr);
            return EXIT_RESTART;
        case hosted::Failure::None:
            break;
    }
    return 0;
}
