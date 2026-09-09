// Follows one node's ordered stream end to end: history replayed through that node's co-located
// ReplayerService, then the live tap, with the switch between them handled by the receiver. The C++ twin
// of examples/java's FollowStream — same flow, same output.
//
// Start a node first (src/main/scripts/start-cluster.sh in the seqeron repo), then run this.
// Environment: SEQERON_NODE_MEMBER_ID (default 0), SEQERON_REPLAYER_CLIENT_ID (default 8),
// SEQERON_AERON_DIR, SEQERON_IDLE_STRATEGY.

#include <atomic>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <string>

#include "Aeron.h"

#include "org/limitless/seqeron/replayer/client/ReplayerStreamReceiver.hpp"
#include "org/limitless/seqeron/util/Env.hpp"
#include "org/limitless/seqeron/util/IdleStrategy.hpp"

namespace {

using org::limitless::seqeron::sequencer::SequencedEvent;

std::atomic_bool running{ true };
std::int64_t lastGlobalSeqNo = 0;
std::string fault;

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
    // Split by family first. A system event is seqeron's own vocabulary, named by systemEventType (the
    // table is in SequencedFrame.hpp); an application payload stays opaque here, as it is to the cluster
    // tier itself. Never dispatch on a bare templateId — it is unique only within one schema.
    if (event.system)
    {
        std::printf("%lld system eventType=%u\n", static_cast<long long>(event.globalSeqNo), event.systemEventType);
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

    // Both handlers, not just the sequenced one: a LeadershipChanged frame is delivered on its own
    // callback and on no other, so a consumer that passes {} there sees a hole in globalSeqNo wherever
    // the cluster changed leader — including at globalSeqNo 1, which always is one.
    //
    // clientId must be unique among the Replayer's co-located apps: two sharing one supersede each
    // other's replays and neither ever catches up.
    client::ReplayerStreamReceiver receiver(clientId, onSequenced, {}, {}, onLeadershipChanged,
                                            [] { std::puts("# caught up — following the tap live"); });
    receiver.start(aeron, memberId);
    std::printf("# following member %d via %s\n", memberId, aeronDir.c_str());

    // The one duty cycle. Every receiver method belongs to this thread.
    auto idle = util::resolveIdleStrategy();
    while (running.load(std::memory_order_relaxed) && fault.empty())
    {
        idle.idle(receiver.poll());
    }

    std::fflush(stdout);
    if (!fault.empty())
    {
        std::fprintf(stderr, "# %s\n", fault.c_str());
        return 1;
    }
    return 0;
}
