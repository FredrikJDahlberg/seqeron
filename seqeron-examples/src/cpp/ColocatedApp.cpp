// A co-located application written against the hosted stub, hosted::Application. The includes are the whole
// import list: app, hosted, protocol/Publish, and util for the two things Java gets from its own standard library.
// The cluster session, the queue, confirmed ingress across a failover, the fences and the leader gate are in the
// host process, org.limitless.seqeron.proxy.ClientProxy; this file hands it a duty cycle and gets Payloads back.
//
// A co-located application is the producer kind nothing elects: one replica per node, publishing only while its own
// node leads. onLeadershipChanged is the whole election.
//
// Start a node, load the examples' topology, start the host, then run this (in the seqeron repo, after
// ./gradlew uberJar; doc/running-a-cluster.md, "Client proxy", has the host's properties):
//   seqeron-service/src/main/scripts/start-cluster.sh
//   seqeron-service/src/main/scripts/clusterctl.sh load-topology seqeron-examples/topology.xml
//   seqeron-service/src/main/scripts/start-client-proxy.sh -Dproxy.sourceId=12 -Dproxy.clientId=14 \
//        -Dproxy.snapshotDir=/tmp/colocated-example
//   ./colocated_app
// Environment: SEQERON_NODE_MEMBER_ID (default 0), SEQERON_AERON_DIR, SEQERON_IDLE_STRATEGY, SEQERON_HOST_NAME (the
// host's proxy.hostName; default hosted).

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
#include "org/limitless/seqeron/hosted/Application.hpp"
#include "org/limitless/seqeron/protocol/Publish.hpp"
#include "org/limitless/seqeron/util/Env.hpp"
#include "org/limitless/seqeron/util/IdleStrategy.hpp"

namespace {

namespace app = org::limitless::seqeron::app;
namespace hosted = org::limitless::seqeron::hosted;
namespace protocol = org::limitless::seqeron::protocol;

// The examples' payloadId: eight raw bytes, no schema, which spec §13.2 admits.
constexpr std::uint16_t PING_PAYLOAD_ID = 6;

// This example's own sourceId (spec §5), the row COLO-EX-C in topology.xml.
constexpr std::int32_t SOURCE_ID = 12;

constexpr std::int64_t PING_INTERVAL_NS = 1'000'000'000;

// Fenced or host lost, restart me: this repository's convention (doc/ops.md). 78: the host refused the attach, which a
// restart does not cure.
constexpr int EXIT_RESTART = 70;
constexpr int EXIT_REFUSED = 78;

std::atomic_bool running{ true };
std::int64_t pingSentNs = 0;
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

// This process's own ping, told from the other examples' by the timestamp it carries.
bool isOwnPing(const app::Payload& payload)
{
    if (pingSentNs == 0 || payload.sourceId() != SOURCE_ID || payload.payloadId() != PING_PAYLOAD_ID ||
        payload.payloadLength() != sizeof(std::int64_t))
    {
        return false;
    }
    std::int64_t sentNs = 0;
    std::memcpy(&sentNs, payload.payload(), sizeof sentNs);
    return sentNs == pingSentNs;
}

// What an application does that the stub cannot do for it: the whole of it, for this example.
struct PingListener
{
    // The gate crossed an edge, in stream order. Every leadership change shuts an open gate, and a payload submitted
    // during the election may have gone with it.
    void onLeadershipChanged(const bool leading) const
    {
        std::puts(leading ? "# leading, publishing" : "# not leading, silent");
    }

    // Every application payload on this node's queue, in clusterSeqNo order, history and live alike. System frames
    // never arrive here; the host acts on them.
    void onSequenced(const app::Payload& payload) const
    {
        if (isOwnPing(payload))
        {
            std::printf("%lld ping echoed, round trip %lldus\n", static_cast<long long>(payload.clusterSeqNo()),
                        static_cast<long long>((nowNs() - pingSentNs) / 1000));
        }
        else
        {
            std::printf("%lld sourceId=%d payloadId=%u length=%llu\n", static_cast<long long>(payload.clusterSeqNo()),
                        payload.sourceId(), payload.payloadId(),
                        static_cast<unsigned long long>(payload.payloadLength()));
        }
    }

    void onCaughtUp(const std::int64_t clusterSeqNo) const
    {
        std::printf("# caught up at %lld, following the queue live\n", static_cast<long long>(clusterSeqNo));
    }

    // Latched, once: the host fenced and has exited. Exiting lets the supervisor restart both.
    void onFenced(const app::ClusterError fence, const std::string& detail) const
    {
        fault = std::string(app::clusterErrorName(fence)) + ": " + detail;
    }
};

} // namespace

int main()
{
    namespace util = org::limitless::seqeron::util;

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

    PingListener listener;
    hosted::Application<PingListener>::Config config;
    config.sourceId = SOURCE_ID;
    config.hostName = util::envString("SEQERON_HOST_NAME", config.hostName);
    hosted::Application<PingListener> application{ config, listener };
    application.start(aeron);
    std::printf("# host %s via %s: replica of application sourceId %d\n", config.hostName.c_str(), aeronDir.c_str(),
                SOURCE_ID);

    // The one duty cycle. Every method on the stub belongs to this thread, callbacks included.
    auto idle = util::resolveIdleStrategy();
    std::int64_t nextPingNs = 0;
    while (running.load(std::memory_order_relaxed) && fault.empty() && application.failure() == hosted::Failure::None)
    {
        const int work = application.doWork();
        const std::int64_t now = nowNs();
        // canPublish() mirrors the host's last Gate: the leader gate and the failover hold in one, a hop behind.
        if (application.canPublish() && now >= nextPingNs)
        {
            // Eight raw bytes, so the publish taking already-encoded bytes is the one this reaches for.
            pingSentNs = now;
            if (application.publish(PING_PAYLOAD_ID, reinterpret_cast<const std::uint8_t*>(&pingSentNs),
                                    sizeof pingSentNs) != protocol::Publish::Published)
            {
                // Declined: the gate shut, the command window is full or the stream is back-pressured. Next
                // second's ping is the retry.
                pingSentNs = 0;
            }
            nextPingNs = now + PING_INTERVAL_NS;
        }
        idle.idle(work);
    }
    application.close();

    std::fflush(stdout);
    if (!fault.empty())
    {
        std::fprintf(stderr, "# %s\n", fault.c_str());
        return EXIT_RESTART;
    }
    switch (application.failure())
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
