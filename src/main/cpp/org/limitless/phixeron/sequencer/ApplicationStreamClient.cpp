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

// Generated SBE codecs (sbe-application.xml via GenerateApplicationSbeCodecs)
#include "org_limitless_phixeron_sbe_application/MessageHeader.h"
#include "org_limitless_phixeron_sbe_application/ExecutionReport.h"
#include "org_limitless_phixeron_sbe_application/NewOrderSingle.h"

using namespace org::limitless::phixeron::sequencer;
namespace sbeapp = org::limitless::phixeron::sbe::application;

static std::atomic<bool> g_running{true};
static void sigintHandler(int) { g_running = false; }

static double toPrice(const std::int64_t mantissa)
{
    return static_cast<double>(mantissa) / 100000000.0;
}

static void printExecutionReport(sbeapp::ExecutionReport& m)
{
    std::printf("  ExecutionReport orderID=%s clOrdID=%s execID=%s execType=%s"
                " ordStatus=%s symbol=%s side=%s leavesQty=%u cumQty=%u avgPx=%.8f\n",
                m.getOrderIDAsString().c_str(),
                m.getClOrdIDAsString().c_str(),
                m.getExecIDAsString().c_str(),
                sbeapp::ExecType::c_str(m.execType()),
                sbeapp::OrdStatus::c_str(m.ordStatus()),
                m.getSymbolAsString().c_str(),
                sbeapp::Side::c_str(m.side()),
                m.leavesQty(),
                m.cumQty(),
                toPrice(m.avgPx()));
}

static void printNewOrderSingle(sbeapp::NewOrderSingle& m)
{
    std::printf("  NewOrderSingle clOrdID=%s symbol=%s side=%s ordType=%s orderQty=%u\n",
                m.getClOrdIDAsString().c_str(),
                m.getSymbolAsString().c_str(),
                sbeapp::Side::c_str(m.side()),
                sbeapp::OrdType::c_str(m.ordType()),
                m.orderQty());
}

// Decodes ApplicationEvent::payload as an sbe-application.xml message
// (ExecutionReport / NewOrderSingle) and prints the decoded fields.
static void decodeApplicationPayload(const ApplicationEvent& e)
{
    if (e.payloadLength < sbeapp::MessageHeader::encodedLength()) {
        std::printf("  (payload too short for SBE header: %" PRIu64 " bytes)\n",
                    e.payloadLength);
        return;
    }

    auto* buffer = const_cast<char*>(reinterpret_cast<const char*>(e.payload));

    sbeapp::MessageHeader header;
    header.wrap(buffer, 0, sbeapp::MessageHeader::sbeSchemaVersion(), e.payloadLength);

    if (header.schemaId() != sbeapp::MessageHeader::sbeSchemaId()) {
        std::printf("  (not an sbe-application payload: schemaId=%u)\n", header.schemaId());
        return;
    }

    switch (header.templateId())
    {
    case sbeapp::ExecutionReport::sbeTemplateId():
    {
        sbeapp::ExecutionReport msg;
        msg.wrapForDecode(buffer, sbeapp::MessageHeader::encodedLength(),
                          header.blockLength(), header.version(), e.payloadLength);
        printExecutionReport(msg);
        break;
    }
    case sbeapp::NewOrderSingle::sbeTemplateId():
    {
        sbeapp::NewOrderSingle msg;
        msg.wrapForDecode(buffer, sbeapp::MessageHeader::encodedLength(),
                          header.blockLength(), header.version(), e.payloadLength);
        printNewOrderSingle(msg);
        break;
    }
    default:
        std::printf("  (unknown sbe-application templateId=%u)\n", header.templateId());
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
