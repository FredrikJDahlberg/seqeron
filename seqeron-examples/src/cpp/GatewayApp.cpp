// One instance of an elected active/standby pair, written against app::Gateway alone — the gateway twin of
// ColocatedApp.cpp. The includes are the same short list: app, protocol/Publish, and util.
//
// A gateway is the producer kind the cluster elects: the topology names both instances, the cluster
// designates one, and only that one serves. What is left here is the edge. A real gateway opens a socket in
// onActivated; this one takes a single simulated client connection instead, and pings the cluster on it
// once a second, reading each ping back off its own tap.
//
// Start a node, load the pair, then run one instance or both (in the seqeron repo):
//   seqeron-service/src/main/scripts/start-cluster.sh
//   seqeron-service/src/main/scripts/clusterctl.sh load-topology seqeron-examples/topology.xml
//   SEQERON_EXAMPLE_GATEWAY_NAME=GW-EX-A ./gateway_app
//   SEQERON_EXAMPLE_GATEWAY_NAME=GW-EX-B ./gateway_app   # the standby; stop A and it takes over
// Environment: SEQERON_EXAMPLE_GATEWAY_NAME (default GW-EX-A), SEQERON_NODE_MEMBER_ID (default 0),
// SEQERON_REPLAYER_CLIENT_ID (default 15 for A, 16 for B), SEQERON_AERON_DIR, SEQERON_IDLE_STRATEGY,
// SEQERON_EXAMPLE_EGRESS_PORT (default 9205 for A, 9206 for B).

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <memory>
#include <string>

#include "Aeron.h"

#include "org/limitless/seqeron/app/Gateway.hpp"
#include "org/limitless/seqeron/protocol/Publish.hpp"
#include "org/limitless/seqeron/util/Env.hpp"
#include "org/limitless/seqeron/util/IdleStrategy.hpp"

namespace {

namespace app = org::limitless::seqeron::app;
namespace protocol = org::limitless::seqeron::protocol;

// The examples' payloadId — eight raw bytes, no schema, which spec §13.2 admits.
constexpr std::uint16_t PING_PAYLOAD_ID = 6;

constexpr std::int64_t PING_INTERVAL_NS = 1'000'000'000;

constexpr const char* CLIENT_LABEL = "example-client";

// What Gateway::openConnection answers when there is none; Edge cannot name Gateway<Edge> before it is complete.
constexpr std::int32_t NO_CONNECTION = -1;

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

// The edge: what a gateway does that the façade cannot do for it. Here, one simulated client.
struct Edge
{
    // Designated: open the edge. A real gateway binds its listen socket here; false would be retried.
    bool onActivated(const std::int32_t firstConnectionId)
    {
        std::printf("# designated — serving, connection ids from %d\n", firstConnectionId);
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

    // Every application payload on this node's tap, in globalSeqNo order, history and live alike.
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
                        static_cast<long long>(payload.globalSeqNo()), payload.connectionId(),
                        static_cast<long long>((nowNs() - pingSentNs) / 1000));
        }
    }

    // This logical gateway's connections, whichever instance opened them: how a standby that keeps
    // per-connection state rebuilds it while it replays.
    void onConnectionOpened(const std::int32_t id, const char* data, const std::size_t length)
    {
        std::printf("# connection %d opened (%.*s)\n", id, static_cast<int>(length), data);
    }

    void onConnectionClosed(const std::int32_t id)
    {
        std::printf("# connection %d closed\n", id);
    }

    void onCaughtUp(const std::int64_t globalSeqNo)
    {
        std::printf("# caught up at %lld — following the tap live\n", static_cast<long long>(globalSeqNo));
    }

    void onClusterHeartbeat(std::int64_t, std::int64_t)
    {}

    // Latched, once: this instance may no longer act. Exiting releases its session, so the standby takes over.
    void onFenced(const app::ClusterError fence, const std::string& detail)
    {
        fault = std::string(app::clusterErrorName(fence)) + ": " + detail;
    }

    bool open = false;
    std::int32_t connectionId = NO_CONNECTION;
    std::int64_t pingSentNs = 0;
};

static_assert(NO_CONNECTION == app::Gateway<Edge>::NO_CONNECTION);

} // namespace

int main()
{
    namespace util = org::limitless::seqeron::util;

    const std::string gatewayName = util::envString("SEQERON_EXAMPLE_GATEWAY_NAME", "GW-EX-A");
    const std::int32_t instance = gatewayName == "GW-EX-B" ? 1 : 0;
    const std::int32_t memberId = util::envInt("SEQERON_NODE_MEMBER_ID", 0);
    const std::int32_t clientId = util::envInt("SEQERON_REPLAYER_CLIENT_ID", 15 + instance);
    const std::string aeronDir = util::resolveAeronDir("SEQERON_AERON_DIR", memberId);

    std::signal(SIGINT, onSignal);
    std::signal(SIGTERM, onSignal);

    aeron::Context context;
    context.aeronDir(aeronDir);
    const std::shared_ptr<aeron::Aeron> aeron = aeron::Aeron::connect(context);

    Edge edge;
    app::Gateway<Edge> gateway{ { .gatewayName = gatewayName,
                                  .clientId = clientId,
                                  .memberId = memberId,
                                  .egressChannel =
                                      "aeron:udp?endpoint=localhost:" +
                                      std::to_string(util::envInt("SEQERON_EXAMPLE_EGRESS_PORT", 9205 + instance)) },
                                edge };
    gateway.start(aeron);
    std::printf("# %s on member %d via %s\n", gatewayName.c_str(), memberId, aeronDir.c_str());

    // The one duty cycle. Every method on the façade belongs to this thread, callbacks included.
    auto idle = util::resolveIdleStrategy();
    std::int64_t nextPingNs = 0;
    while (running.load(std::memory_order_relaxed) && fault.empty())
    {
        const int work = gateway.doWork();
        // canAccept() is serving and not held behind a failover's resend: the moment a real gateway accepts.
        if (edge.open && edge.connectionId == NO_CONNECTION && gateway.canAccept())
        {
            edge.connectionId = gateway.openConnection(CLIENT_LABEL, std::strlen(CLIENT_LABEL));
        }
        const std::int64_t now = nowNs();
        if (edge.connectionId != NO_CONNECTION && now >= nextPingNs)
        {
            // Declined until the connection's ConnectionOpened has landed, and while ingress is held or
            // back-pressured. Next second's ping is the retry.
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
        return 1;
    }
    return 0;
}
