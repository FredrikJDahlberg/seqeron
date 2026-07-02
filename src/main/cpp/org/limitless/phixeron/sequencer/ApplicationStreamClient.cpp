/*
 * ApplicationStreamClient — replays the global stream then follows the live
 * recording, delivering only application messages to a handler.
 *
 * Startup sequence:
 *   1. Connect to the Aeron Archive at the SequencerNode (localhost:9301).
 *   2. Locate the global stream recording (channel 224.0.1.1:9200, stream 1).
 *   3. Start an archive replay with NULL_POSITION length so the same image
 *      follows the live recording seamlessly after the history is consumed.
 *   4. Dispatch each SequencedMessage payload (AppMessage SBE prefix stripped)
 *      to the application handler.  Lifecycle events (SourceConnected /
 *      SourceDisconnected) are silently filtered out.
 *   5. onCaughtUp fires once the replay image reaches the recording stop
 *      position that was observed at startup.
 */

#include <atomic>
#include <cinttypes>
#include <csignal>
#include <cstdio>
#include <memory>
#include <thread>

#include "Aeron.h"

#include "org/limitless/phixeron/sequencer/ApplicationStreamClient.hpp"

using namespace org::limitless::phixeron::sequencer;

static std::atomic<bool> g_running{true};
static void sigintHandler(int) { g_running = false; }

int main()
{
    signal(SIGINT, sigintHandler);

    aeron::Context aeronCtx;
    auto aeron = aeron::Aeron::connect(aeronCtx);
    std::puts("[ApplicationStreamClient] Connected to Aeron media driver");

    ApplicationStreamClient client(
        [](const ApplicationEvent& e)
        {
            std::printf("[Message] globalSeq=%" PRId64
                        "  source=%" PRId64
                        "  appSeq=%" PRId64
                        "  clusterTs=%" PRId64
                        "  receiveNs=%" PRId64
                        "  bytes=%" PRIu64 "\n",
                        e.globalSeqNo,
                        e.sourceSessionId,
                        e.appSeqNo,
                        e.clusterTimestamp,
                        e.receiveTimeNs,
                        e.payloadLength);
        },
        []()
        {
            std::puts("[ApplicationStreamClient] Caught up — now receiving live messages");
        }
    );

    client.start(std::move(aeron));
    std::puts("[ApplicationStreamClient] Replay started");

    while (g_running)
    {
        if (client.poll() == 0)
            std::this_thread::yield();
    }

    std::puts("[ApplicationStreamClient] Shutting down");
    return 0;
}
