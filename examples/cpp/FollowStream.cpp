// Follows one node's ordered stream end to end: history replayed through that node's co-located
// ReplayerService, then the live tap, with the switch between them handled by the receiver. The C++ twin
// of examples/java's FollowStream — same flow, same output.
//
// Once caught up it also produces — one ping a second at cluster ingress, whose echo comes back through
// onSequenced with everything else.
//
// Start a node first (src/main/scripts/start-cluster.sh in the seqeron repo), then run this.
// Environment: SEQERON_NODE_MEMBER_ID (default 0), SEQERON_REPLAYER_CLIENT_ID (default 8),
// SEQERON_AERON_DIR, SEQERON_IDLE_STRATEGY, SEQERON_EXAMPLE_EGRESS_PORT.

#include <array>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <exception>
#include <memory>
#include <string>

#include "Aeron.h"

#include "org/limitless/seqeron/replayer/client/ReplayerStreamReceiver.hpp"
#include "org/limitless/seqeron/sequencer/ClusterStreamSender.hpp"
#include "org/limitless/seqeron/util/Env.hpp"
#include "org/limitless/seqeron/util/IdleStrategy.hpp"
#include "org_limitless_seqeron_sbe_frame/Unsequenced.h"

namespace {

namespace frame_sbe = org::limitless::seqeron::sbe::frame;

using org::limitless::seqeron::sequencer::SequencedEvent;

// The examples' own payloadId and sourceId, allocated in the spec's §6.1 and §5 tables.
constexpr std::uint16_t PING_PAYLOAD_ID = 6;
constexpr std::int32_t PING_SOURCE_ID = 10;

// The ping belongs to no connection of its own.
constexpr std::int32_t NO_ID = -1;

// How long ingress is tried over the co-located member's IPC before falling back to its UDP endpoint,
// which a follower answers with a REDIRECT to the real leader.
constexpr std::int64_t IPC_CONNECT_TIMEOUT_MS = 500;

constexpr std::int64_t PING_INTERVAL_NS = 1'000'000'000;

std::atomic_bool running{ true };
std::int64_t lastGlobalSeqNo = 0;
std::string fault;

std::int64_t pingSentNs = 0;

std::int64_t nowNs()
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

// This process's own ping, told from any other producer's by the timestamp it carries.
bool isOwnPing(const SequencedEvent& event)
{
    if (pingSentNs == 0 || event.payloadId != PING_PAYLOAD_ID || event.payloadLength != sizeof(std::int64_t))
    {
        return false;
    }
    std::int64_t sentNs = 0;
    std::memcpy(&sentNs, event.payload, sizeof sentNs);
    return sentNs == pingSentNs;
}

// One ping at cluster ingress: the examples' own payloadId, and a body of eight raw bytes holding the
// clock reading it left on. Not SBE, and it need not be — the cluster tier decodes no payloadId at all, so
// a payload is copied through unopened whatever it holds.
//
// Nothing waits here for the echo: the consumer this process already is picks it up off the tap like every
// other frame.
void ping(org::limitless::seqeron::sequencer::ClusterStreamSender& sender)
{
    alignas(16) std::array<std::uint8_t, 64> buffer{};
    frame_sbe::Unsequenced frame;
    frame.wrapAndApplyHeader(reinterpret_cast<char*>(buffer.data()), 0, buffer.size());
    frame.header()
        .sourceId(PING_SOURCE_ID)
        .connectionId(NO_ID)
        .sessionId(sender.clusterSessionId()) // advisory; the sequencer overwrites it with the true one
        .payloadId(PING_PAYLOAD_ID);
    pingSentNs = nowNs();
    // Little-endian on the wire, as every SBE field is, so the Java example reads the same eight bytes.
    frame.putPayload(reinterpret_cast<const char*>(&pingSentNs), sizeof pingSentNs);
    const auto length = static_cast<std::uint16_t>(frame_sbe::MessageHeader::encodedLength() + frame.encodedLength());
    if (!sender.send(buffer.data(), length))
    {
        // No session to take it — an election, or one that closed. Next second's ping is the retry.
        pingSentNs = 0;
    }
}

void onSignal(int)
{
    running.store(false, std::memory_order_relaxed);
}

// The invariant the whole tier exists for: one frame per globalSeqNo, no holes, replay and live alike.
// Recorded rather than thrown — this runs inside a fragment handler, and Image::poll advances the
// subscriber position regardless of what a handler raises, so the duty cycle raises it instead.
bool inOrder(const std::int64_t globalSeqNo)
{
    if (!fault.empty())
    {
        return false;
    }
    if (globalSeqNo != lastGlobalSeqNo + 1)
    {
        fault = "gap: " + std::to_string(lastGlobalSeqNo) + " -> " + std::to_string(globalSeqNo);
        return false;
    }
    lastGlobalSeqNo = globalSeqNo;
    return true;
}

// Every other frame arrives here in globalSeqNo order, history and live alike — the receiver requests a
// replay for anything the live tap dropped and dispatches nothing out of order in the meantime.
void onSequenced(const SequencedEvent& event)
{
    if (!inOrder(event.globalSeqNo))
    {
        return;
    }
    if (event.system)
    {
        std::printf("%lld system eventType=%u\n", static_cast<long long>(event.globalSeqNo), event.systemEventType);
    }
    else if (isOwnPing(event))
    {
        std::printf("%lld ping echoed, round trip %lldus\n", static_cast<long long>(event.globalSeqNo),
                    static_cast<long long>((nowNs() - pingSentNs) / 1000));
    }
    else
    {
        std::printf("%lld payloadId=%u template=%u length=%llu\n", static_cast<long long>(event.globalSeqNo),
                    event.payloadId, event.templateId, static_cast<unsigned long long>(event.payloadLength));
    }
}

// The one frame family that reaches a consumer here instead of through onSequenced.
void onLeadershipChanged(const std::int32_t newLeaderMemberId, const std::int64_t globalSeqNo)
{
    if (!inOrder(globalSeqNo))
    {
        return;
    }
    std::printf("%lld leader=member %d\n", static_cast<long long>(globalSeqNo), newLeaderMemberId);
}

} // namespace

int main()
{
    namespace util = org::limitless::seqeron::util;
    namespace client = org::limitless::seqeron::replayer::client;

    const std::int32_t memberId = util::envInt("SEQERON_NODE_MEMBER_ID", 0);
    const std::int32_t clientId = util::envInt("SEQERON_REPLAYER_CLIENT_ID", 8);
    const std::string aeronDir = util::resolveAeronDir("SEQERON_AERON_DIR", memberId);

    std::signal(SIGINT, onSignal);
    std::signal(SIGTERM, onSignal);

    aeron::Context context;
    context.aeronDir(aeronDir);
    const std::shared_ptr<aeron::Aeron> aeron = aeron::Aeron::connect(context);

    const std::string egressChannel =
        "aeron:udp?endpoint=localhost:" + std::to_string(util::envInt("SEQERON_EXAMPLE_EGRESS_PORT", 9202 + memberId));
    org::limitless::seqeron::sequencer::ClusterStreamSender sender;
    sender.connectColocated(aeron, memberId, IPC_CONNECT_TIMEOUT_MS, egressChannel);

    client::ReplayerStreamReceiver receiver(clientId, onSequenced, {}, {}, onLeadershipChanged,
                                            [] { std::puts("# caught up — following the tap live"); });
    receiver.start(aeron, memberId);
    std::printf("# following member %d via %s\n", memberId, aeronDir.c_str());

    // The one duty cycle. Every receiver and sender method belongs to this thread.
    auto idle = util::resolveIdleStrategy();
    std::int64_t nextPingNs = 0;
    while (running.load(std::memory_order_relaxed) && fault.empty())
    {
        const int work = receiver.poll();
        sender.pollEgress([](const std::uint8_t*, std::int32_t) {}); // session events; the ping has no reply
        // Self-throttling: the sender decides when a keep-alive is due, so this just says when it had the
        // chance to send one.
        sender.keepAlive();
        // Only once caught up: a ping submitted during the replay walk would be echoed behind the history
        // still being read, and the round trip would measure the walk rather than the path.
        const std::int64_t now = nowNs();
        if (receiver.isCaughtUp() && now >= nextPingNs)
        {
            ping(sender);
            nextPingNs = now + PING_INTERVAL_NS;
        }
        idle.idle(work);
    }
    sender.close();

    std::fflush(stdout);
    if (!fault.empty())
    {
        std::fprintf(stderr, "# %s\n", fault.c_str());
        return 1;
    }
    return 0;
}
