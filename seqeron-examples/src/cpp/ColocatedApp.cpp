// The same flow as FollowStream.cpp, written against the front door instead of the tiers under it, and the
// C++ twin of src/java's ColocatedApp. Look at the includes: app, protocol/Publish, and util for the two
// things Java gets from its own standard library. No receiver, no sender, no envelope, no systemEventType —
// app::ColocatedApplication assembles the cluster session, the tap, confirmed ingress across a failover, the
// fences and the leader gate, and hands this file Payloads.
//
// A co-located application is the producer kind nothing elects: one replica per node, publishing only while
// its own node leads. That is why it needs no topology document and no clusterctl step — LeadershipChanged
// already picks the replica that submits, so onLeadershipChanged is the whole election.
//
// Start a node first (seqeron-service/src/main/scripts/start-cluster.sh in the seqeron repo), then run this.
// Environment: SEQERON_NODE_MEMBER_ID (default 0), SEQERON_REPLAYER_CLIENT_ID (default 14),
// SEQERON_AERON_DIR, SEQERON_IDLE_STRATEGY, SEQERON_EXAMPLE_EGRESS_PORT.

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <memory>
#include <string>

#include "Aeron.h"

#include "org/limitless/seqeron/app/ColocatedApplication.hpp"
#include "org/limitless/seqeron/protocol/Publish.hpp"
#include "org/limitless/seqeron/util/Env.hpp"
#include "org/limitless/seqeron/util/IdleStrategy.hpp"

namespace {

namespace app = org::limitless::seqeron::app;
namespace protocol = org::limitless::seqeron::protocol;

// The examples' payloadId — eight raw bytes, no schema, which spec §13.2 admits.
constexpr std::uint16_t PING_PAYLOAD_ID = 6;

// This example's own sourceId (spec §5); the low-level example is 10, the Java façade one 11.
constexpr std::int32_t SOURCE_ID = 12;

constexpr std::int64_t PING_INTERVAL_NS = 1'000'000'000;

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

// What an application does that the façade cannot do for it — the whole of it, for this example.
struct PingListener
{
    // The whole election, for the producer kind nothing elects. False is also where a replica keeping
    // outstanding work calls OutstandingWork::onNotLeader(): every leadership change shuts an open gate, and
    // a payload submitted during the election may have gone with it.
    void onLeadershipChanged(const bool leading) const
    {
        std::puts(leading ? "# leading — publishing" : "# not leading — silent");
    }

    // Every application payload on this node's tap, in globalSeqNo order, history and live alike — every
    // replica sees the same ones and so holds the same state. System frames never arrive here; the façade
    // acts on them and reports only what an application has a decision to make about.
    void onSequenced(const app::Payload& payload) const
    {
        if (isOwnPing(payload))
        {
            std::printf("%lld ping echoed, round trip %lldus\n", static_cast<long long>(payload.globalSeqNo()),
                        static_cast<long long>((nowNs() - pingSentNs) / 1000));
        }
        else
        {
            std::printf("%lld sourceId=%d payloadId=%u length=%llu\n", static_cast<long long>(payload.globalSeqNo()),
                        payload.sourceId(), payload.payloadId(),
                        static_cast<unsigned long long>(payload.payloadLength()));
        }
    }

    void onCaughtUp(const std::int64_t globalSeqNo) const
    {
        std::printf("# caught up at %lld — following the tap live\n", static_cast<long long>(globalSeqNo));
    }

    // The cluster clock, once a second: the one time source that keeps advancing while every producer is
    // silent, and identical on every node. A deadline belongs on this rather than on a local clock.
    void onClusterHeartbeat(std::int64_t, std::int64_t) const
    {}

    // Latched, once: this replica may no longer act, and exiting lets its restart re-walk the log.
    void onFenced(const app::Fence fence, const std::string& detail) const
    {
        fault = std::string(app::fenceName(fence)) + ": " + detail;
    }
};

} // namespace

int main()
{
    namespace util = org::limitless::seqeron::util;

    const std::int32_t memberId = util::envInt("SEQERON_NODE_MEMBER_ID", 0);
    const std::int32_t clientId = util::envInt("SEQERON_REPLAYER_CLIENT_ID", 14);
    const std::string aeronDir = util::resolveAeronDir("SEQERON_AERON_DIR", memberId);

    std::signal(SIGINT, onSignal);
    std::signal(SIGTERM, onSignal);

    aeron::Context context;
    context.aeronDir(aeronDir);
    const std::shared_ptr<aeron::Aeron> aeron = aeron::Aeron::connect(context);

    PingListener listener;
    app::ColocatedApplication<PingListener> application{
        { .sourceId = SOURCE_ID,
          .clientId = clientId,
          .memberId = memberId,
          .egressChannel = "aeron:udp?endpoint=localhost:" +
                           std::to_string(util::envInt("SEQERON_EXAMPLE_EGRESS_PORT", 9202 + memberId)) },
        listener
    };
    application.start(aeron);
    std::printf("# member %d via %s — replica of application sourceId %d\n", memberId, aeronDir.c_str(), SOURCE_ID);

    // The one duty cycle. Every method on the façade belongs to this thread, callbacks included.
    auto idle = util::resolveIdleStrategy();
    std::int64_t nextPingNs = 0;
    while (running.load(std::memory_order_relaxed) && fault.empty())
    {
        const int work = application.doWork();
        const std::int64_t now = nowNs();
        // canPublish() is the gate and the failover hold in one: shut while this node does not lead, and shut
        // while PendingSends is still resending what the last leader change lost.
        if (application.canPublish() && now >= nextPingNs)
        {
            // Eight raw bytes, so the publish taking already-encoded bytes is the one this reaches for; the
            // other is templated on an SBE encoder and a Fill.
            pingSentNs = now;
            if (application.publish(PING_PAYLOAD_ID, reinterpret_cast<const std::uint8_t*>(&pingSentNs),
                                    sizeof pingSentNs) != protocol::Publish::Published)
            {
                // Declined: the gate shut, ingress is held behind a resend, or the transport is
                // back-pressured. Next second's ping is the retry.
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
        return 1;
    }
    return 0;
}
