/*
 * ApplicationStreamClient — replays the global stream then follows the live
 * recording, delivering only application messages to a handler.
 *
 * Startup sequence:
 *   1. Connect to the Aeron Archive at the SequencerNode (localhost:9301).
 *   2. Locate the global stream recording (channel 224.0.1.1:9200, stream 1).
 *   3. Start an archive replay with NULL_POSITION length so the same image
 *      follows the live recording seamlessly after the history is consumed.
 *   4. Dispatch each global-stream fragment (a sbe-sequenced.xml message,
 *      schemaId=202, verbatim) to the application handler.  Lifecycle events
 *      (ClientConnected / ClientDisconnected) are silently filtered out.
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

// Generated SBE codecs (sbe-sequenced.xml via GenerateSequencedSbeCodecs)
#include "org_limitless_phixeron_sbe_sequenced/MessageHeader.h"
#include "org_limitless_phixeron_sbe_sequenced/Header.h"
#include "org_limitless_phixeron_sbe_sequenced/ExecutionReport.h"
#include "org_limitless_phixeron_sbe_sequenced/NewOrderSingle.h"

using namespace org::limitless::phixeron::sequencer;
namespace seq = org::limitless::phixeron::sbe::sequenced;

static std::atomic<bool> g_running{true};
static void sigintHandler(int) { g_running = false; }

static double toPrice(const std::int64_t mantissa)
{
    return static_cast<double>(mantissa) / 100000000.0;
}

static void printExecutionReport(seq::ExecutionReport& m)
{
    std::printf("  ExecutionReport sourceId=%d sessionId=%" PRId64
                " orderID=%s clOrdID=%s execID=%s execType=%s"
                " ordStatus=%s symbol=%s side=%s leavesQty=%u cumQty=%u avgPx=%.8f\n",
                m.header().sourceId(), m.header().sessionId(),
                m.getOrderIDAsString().c_str(),
                m.getClOrdIDAsString().c_str(),
                m.getExecIDAsString().c_str(),
                seq::ExecType::c_str(m.execType()),
                seq::OrdStatus::c_str(m.ordStatus()),
                m.getSymbolAsString().c_str(),
                seq::Side::c_str(m.side()),
                m.leavesQty(),
                m.cumQty(),
                toPrice(m.avgPx()));
}

static void printNewOrderSingle(seq::NewOrderSingle& m)
{
    std::printf("  NewOrderSingle sourceId=%d sessionId=%" PRId64
                " clOrdID=%s symbol=%s side=%s ordType=%s orderQty=%u\n",
                m.header().sourceId(), m.header().sessionId(),
                m.getClOrdIDAsString().c_str(),
                m.getSymbolAsString().c_str(),
                seq::Side::c_str(m.side()),
                seq::OrdType::c_str(m.ordType()),
                m.orderQty());
}

// Decodes ApplicationEvent::payload as a sbe-sequenced.xml message and
// prints the decoded fields for the two application message types
// (ExecutionReport / NewOrderSingle); admin messages (Logon, Heartbeat, …)
// are reported but not decoded — this stream is application-focused.
static void decodeApplicationPayload(const ApplicationEvent& e)
{
    if (e.payloadLength < seq::MessageHeader::encodedLength()) {
        std::printf("  (payload too short for SBE header: %" PRIu64 " bytes)\n",
                    e.payloadLength);
        return;
    }

    auto* buffer = const_cast<char*>(reinterpret_cast<const char*>(e.payload));

    seq::MessageHeader header;
    header.wrap(buffer, 0, seq::MessageHeader::sbeSchemaVersion(), e.payloadLength);

    if (header.schemaId() != seq::MessageHeader::sbeSchemaId()) {
        std::printf("  (not a sbe-sequenced payload: schemaId=%u)\n", header.schemaId());
        return;
    }

    switch (header.templateId())
    {
    case seq::ExecutionReport::sbeTemplateId():
    {
        seq::ExecutionReport msg;
        msg.wrapForDecode(buffer, seq::MessageHeader::encodedLength(),
                          header.blockLength(), header.version(), e.payloadLength);
        printExecutionReport(msg);
        break;
    }
    case seq::NewOrderSingle::sbeTemplateId():
    {
        seq::NewOrderSingle msg;
        msg.wrapForDecode(buffer, seq::MessageHeader::encodedLength(),
                          header.blockLength(), header.version(), e.payloadLength);
        printNewOrderSingle(msg);
        break;
    }
    default:
        std::printf("  (admin sbe-sequenced templateId=%u; not an application message)\n",
                    header.templateId());
        break;
    }
}

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
                        "  clusterTs=%" PRId64
                        "  receiveNs=%" PRId64
                        "  bytes=%" PRIu64 "\n",
                        e.globalSeqNo,
                        e.sourceSessionId,
                        e.clusterTimestamp,
                        e.receiveTimeNs,
                        e.payloadLength);
            decodeApplicationPayload(e);
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
        {
            std::this_thread::yield();
        }
    }

    std::puts("[ApplicationStreamClient] Shutting down");
    return 0;
}
