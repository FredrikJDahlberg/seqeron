/*
 * OrderExecClient — Aeron Cluster client that watches the global stream for order
 * execution activity and answers PortfolioQueryRequests with a risk assessment
 * computed by an external (mocked) risk engine. Combines what used to be two
 * separate binaries:
 *   - application_stream_client: replays the global stream then follows it live,
 *     printing every NewOrderSingle / ExecutionReport it sees.
 *   - the C++ RiskEngineClient: tracks per-account positions from those same
 *     fills and answers PortfolioQueryRequest.
 * They shared an identical startup sequence (connect to the Archive at the
 * SequencerNode, locate the global stream recording, replay it into a
 * GlobalStreamClient) and consumed the same NewOrderSingle/ExecutionReport
 * traffic, so folding them into one process removes that duplication and the
 * extra archive replay port (see GlobalStreamClient.hpp).
 *
 * Read side — replays the global stream from the Archive (following live once
 * caught up) and, purely on the poll thread:
 *   - prints every NewOrderSingle / ExecutionReport it decodes
 *   - maintains clOrdID -> account (from NewOrderSingle) and
 *     account -> symbol -> position (from ExecutionReport fills, execType ==
 *     Trade), looked up via that clOrdID map
 *
 * Write side — on a PortfolioQueryRequest, snapshots the matching account's
 * positions and hands them to MockRiskEngine on a throttled worker thread, then
 * submits a PortfolioQueryReply to cluster ingress with header.sourceId copied
 * from the request so the FIX gateway can route the answer back to the
 * originating TCP connection.
 *
 * Backpressure — MockRiskEngine is synchronous and slow, and only
 * MAX_CONCURRENT_RISK_QUERIES requests may be outstanding at once. The Java
 * version leaves an over-limit PortfolioQueryRequest fragment unconsumed
 * (ControlledFragmentHandler::Action::ABORT) so Aeron redelivers it, which
 * naturally pauses global-stream consumption until a worker slot frees up.
 * GlobalStreamClient here only exposes a plain (uncontrolled) poll, so instead a
 * request that arrives when every worker slot is busy is queued locally
 * (pendingQueries) and drained as slots free up on each poll() — global stream
 * consumption itself is never paused, but the effective query throttling is the
 * same.
 */

#include <array>
#include <atomic>
#include <cinttypes>
#include <csignal>
#include <cstdio>
#include <deque>
#include <limits>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "Aeron.h"
#include "concurrent/YieldingIdleStrategy.h"
#include "client/archive/AeronArchive.h"

#include "org/limitless/phixeron/sequencer/GlobalStreamClient.hpp"
#include "org/limitless/phixeron/sequencer/ClusterIngressSender.hpp"
#include "org/limitless/phixeron/risk/MockRiskEngine.hpp"

// Generated SBE C++ codecs (sbe-sequenced.xml / sbe-unsequenced.xml)
#include "org_limitless_phixeron_sbe_sequenced/MessageHeader.h"
#include "org_limitless_phixeron_sbe_sequenced/NewOrderSingle.h"
#include "org_limitless_phixeron_sbe_sequenced/ExecutionReport.h"
#include "org_limitless_phixeron_sbe_sequenced/PortfolioQueryRequest.h"
#include "org_limitless_phixeron_sbe_unsequenced/MessageHeader.h"
#include "org_limitless_phixeron_sbe_unsequenced/PortfolioQueryReply.h"

using namespace org::limitless::phixeron::sequencer;
using org::limitless::phixeron::risk::MockRiskEngine;
namespace seq = org::limitless::phixeron::sbe::sequenced;
namespace usq = org::limitless::phixeron::sbe::unsequenced;

namespace {

// ── Configuration — single-node SequencerNode layout ─────────────────────────
constexpr const char*  ARCHIVE_CONTROL_CHANNEL  = "aeron:udp?endpoint=localhost:9301";
constexpr std::int32_t ARCHIVE_CONTROL_STREAM   = 100;
constexpr const char*  ARCHIVE_RESPONSE_CHANNEL = "aeron:udp?endpoint=localhost:0";
// Distinct port from FixSessionClient (9310) so their archive replay
// publications don't conflict. Overridable via PHIXERON_ORDER_EXEC_REPLAY_PORT
// so two OrderExecClient instances can run on one host without clashing.
constexpr std::uint16_t DEFAULT_REPLAY_PORT = 9311;

constexpr int          MAX_CONCURRENT_RISK_QUERIES = 5;
constexpr std::int64_t RISK_ENGINE_LATENCY_MILLIS  = 250;

std::atomic<bool> g_running{true};
void sigintHandler(int) { g_running = false; }

// ── Portfolio state ───────────────────────────────────────────────────────────

// One account's running position in one symbol; mutated only from the poll thread.
struct Position
{
    std::int64_t netQty        = 0;
    std::int64_t avgPxMantissa = 0;

    // Extending a position (opening, or adding in the same direction) re-averages
    // the cost basis; reducing without flipping sides leaves the average price
    // unchanged (a partial realization); crossing through zero resets the basis to
    // the fill price.
    void applyFill(std::int64_t signedQty, std::int64_t fillPxMantissa)
    {
        const std::int64_t newQty = netQty + signedQty;
        if (netQty == 0 || sgn(signedQty) == sgn(netQty)) {
            avgPxMantissa = (netQty * avgPxMantissa + signedQty * fillPxMantissa) / newQty;
        } else if (newQty == 0) {
            avgPxMantissa = 0;
        } else if (sgn(newQty) != sgn(netQty)) {
            avgPxMantissa = fillPxMantissa;
        }
        netQty = newQty;
    }

private:
    static int sgn(std::int64_t v) { return (v > 0) - (v < 0); }
};

// A completed (or short-circuited) query result awaiting publication to cluster ingress.
struct PendingReply
{
    std::int32_t                 sourceId;
    std::int64_t                 correlationId;
    std::string                  account;
    usq::RiskQueryStatus::Value  status;
    std::uint32_t                riskScore              = 0;
    std::int64_t                 grossExposureMantissa  = 0;
    std::int64_t                 netExposureMantissa    = 0;
    std::uint32_t                positionCount          = 0;
    std::string                  errorText;
};

// A PortfolioQueryRequest waiting for a free risk-query worker slot.
struct PendingQuery
{
    std::int32_t                          sourceId;
    std::int64_t                          correlationId;
    std::string                           account;
    std::vector<MockRiskEngine::Position> snapshot;
};

// Thread-safe hand-off from risk-query worker threads (up to
// MAX_CONCURRENT_RISK_QUERIES concurrently) to the single poll thread.
class ResultsQueue
{
public:
    void push(PendingReply reply)
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_queue.push_back(std::move(reply));
    }

    std::optional<PendingReply> pop()
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        if (m_queue.empty()) { return std::nullopt; }
        PendingReply reply = std::move(m_queue.front());
        m_queue.pop_front();
        return reply;
    }

private:
    std::mutex               m_mutex;
    std::deque<PendingReply> m_queue;
};

// Models "corePoolSize == maxPoolSize == MAX_CONCURRENT_RISK_QUERIES with a
// zero-capacity queue" (the Java version's ThreadPoolExecutor +
// SynchronousQueue): a query only starts if a slot is immediately free.
// tryReserveSlot()/execute() are split so a caller can find out whether a slot
// was available before committing to (moving) the query it would run.
class RiskQueryExecutor
{
public:
    bool tryReserveSlot()
    {
        const int current = m_inFlight.fetch_add(1) + 1;
        if (current > MAX_CONCURRENT_RISK_QUERIES) {
            m_inFlight.fetch_sub(1);
            return false;
        }
        return true;
    }

    // Runs fn on a new detached thread; must only be called after a successful
    // tryReserveSlot(). Releases the slot when fn returns.
    template <typename Fn>
    void execute(Fn&& fn)
    {
        std::thread([this, fn = std::forward<Fn>(fn)]() mutable
        {
            fn();
            m_inFlight.fetch_sub(1);
        }).detach();
    }

private:
    std::atomic<int> m_inFlight{0};
};

// ── Archive helpers ────────────────────────────────────────────────────────

std::int64_t findGlobalStreamRecording(
    aeron::archive::client::AeronArchive& archive, std::int64_t& catchUpPos)
{
    // Prefer the active (live) recording over any stopped one. When multiple
    // stopped recordings exist (e.g., after leader failover), pick the one with
    // the largest stop position (holds the most committed data).
    std::int64_t activeId   = -1;
    std::int64_t stoppedId  = -1;
    std::int64_t stoppedPos = std::numeric_limits<std::int64_t>::min();

    archive.listRecordingsForUri(
        0, std::numeric_limits<std::int32_t>::max(),
        GLOBAL_STREAM_CHANNEL, GLOBAL_STREAM_ID,
        [&](aeron::archive::client::RecordingDescriptor& desc) {
            if (desc.m_stopPosition == aeron::archive::client::NULL_POSITION) {
                activeId = desc.m_recordingId;
            } else if (desc.m_stopPosition > stoppedPos) {
                stoppedId  = desc.m_recordingId;
                stoppedPos = desc.m_stopPosition;
            }
        });

    if (activeId < 0 && stoppedId < 0) {
        throw std::runtime_error(
            "[OrderExecClient] No global stream recording found on "
            + std::string(GLOBAL_STREAM_CHANNEL));
    }

    if (activeId >= 0) {
        catchUpPos = archive.getRecordingPosition(activeId);
        if (catchUpPos == aeron::archive::client::NULL_POSITION) { catchUpPos = 0; }
        return activeId;
    }

    catchUpPos = stoppedPos;
    return stoppedId;
}

// ── Printing (observability — mirrors the old application_stream_client) ────

double toPrice(const std::int64_t mantissa)
{
    return static_cast<double>(mantissa) / 100000000.0;
}

void printExecutionReport(seq::ExecutionReport& m)
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

void printNewOrderSingle(seq::NewOrderSingle& m)
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

} // namespace

int main()
{
    signal(SIGINT, sigintHandler);

    const std::string replayChannel =
        resolveReplayChannel("PHIXERON_ORDER_EXEC_REPLAY_PORT", DEFAULT_REPLAY_PORT);

    // ── Aeron ────────────────────────────────────────────────────────────────
    aeron::Context aeronCtx;
    auto aeron = aeron::Aeron::connect(aeronCtx);
    std::puts("[OrderExecClient] Connected to Aeron media driver");

    // ── Archive ──────────────────────────────────────────────────────────────
    aeron::archive::client::Context archiveCtx;
    archiveCtx.aeron(aeron)
              .controlRequestChannel(ARCHIVE_CONTROL_CHANNEL)
              .controlRequestStreamId(ARCHIVE_CONTROL_STREAM)
              .controlResponseChannel(ARCHIVE_RESPONSE_CHANNEL);

    auto archive = aeron::archive::client::AeronArchive::connect(archiveCtx);
    std::puts("[OrderExecClient] Connected to Aeron Archive");

    // ── Locate global stream recording ───────────────────────────────────────
    std::int64_t catchUpPosition = 0;
    const std::int64_t recordingId = findGlobalStreamRecording(*archive, catchUpPosition);
    std::printf("[OrderExecClient] Recording %" PRId64
                "  catchUpPosition=%" PRId64 "\n", recordingId, catchUpPosition);

    // ── Start replay (only when there is historical data to replay) ──────────
    std::int64_t replaySessionId = -1;
    if (catchUpPosition > 0) {
        aeron::archive::client::ReplayParams replayParams;
        replayParams.position(0).length(aeron::archive::client::NULL_LENGTH);
        replaySessionId = archive->startReplay(
            recordingId, replayChannel.c_str(), REPLAY_STREAM_ID, replayParams);
        std::printf("[OrderExecClient] Replay started  replaySessionId=%" PRId64 "\n",
                    replaySessionId);
    } else {
        std::puts("[OrderExecClient] No historical data — subscribing to live stream");
    }

    // ── Cluster ingress ───────────────────────────────────────────────────────
    ClusterIngressSender ingressSender;
    ingressSender.connect(aeron);

    // ── Portfolio state — single-threaded, mutated/read only from poll thread ──
    std::unordered_map<std::string, std::string> orderAccounts; // clOrdID -> account
    std::unordered_map<std::string, std::unordered_map<std::string, Position>> portfolios;

    // ── External risk engine + query throttling ──────────────────────────────
    MockRiskEngine           mockRiskEngine(MAX_CONCURRENT_RISK_QUERIES, RISK_ENGINE_LATENCY_MILLIS);
    RiskQueryExecutor        riskQueryExecutor;
    ResultsQueue             resultsQueue;
    std::deque<PendingQuery> pendingQueries;

    // Reply awaiting a successful cluster.offer after a prior attempt was back-pressured.
    std::optional<PendingReply> pendingReply;

    // ── SBE encode buffer for outgoing PortfolioQueryReply ────────────────────
    alignas(16) std::array<std::uint8_t, 512> sendBuffer{};

    auto offerReply = [&](const PendingReply& reply)
    {
        usq::PortfolioQueryReply replyEncoder;
        replyEncoder.wrapAndApplyHeader(reinterpret_cast<char*>(sendBuffer.data()), 0, sendBuffer.size());
        replyEncoder.header()
            .sourceId(reply.sourceId)
            .sessionId(ingressSender.clusterSessionId());
        replyEncoder.correlationId(reply.correlationId)
                    .putAccount(reply.account)
                    .status(reply.status)
                    .riskScore(reply.riskScore)
                    .grossExposure(reply.grossExposureMantissa)
                    .netExposure(reply.netExposureMantissa)
                    .positionCount(reply.positionCount)
                    .putText(reply.errorText);

        const auto length = static_cast<std::uint16_t>(
            usq::MessageHeader::encodedLength() + replyEncoder.encodedLength());
        ingressSender.send(sendBuffer.data(), length);
    };

    // ── Global stream subscription ────────────────────────────────────────────
    // e.payload is directly a sbe-sequenced.xml message (schemaId=202) — no
    // envelope to strip; GlobalStreamClient already decoded the outer
    // messageHeader and the header composite generically.
    GlobalStreamClient globalStream(
        [&](const SequencedEvent& e)
        {
            auto* body = const_cast<char*>(e.payload);

            if (e.templateId == seq::NewOrderSingle::sbeTemplateId())
            {
                seq::NewOrderSingle msg;
                msg.wrapForDecode(body, seq::MessageHeader::encodedLength(),
                                  e.blockLength, e.version, e.payloadLength);
                printNewOrderSingle(msg);

                const std::string account = msg.getAccountAsString();
                const std::string clOrdId = msg.getClOrdIDAsString();
                if (!account.empty() && !clOrdId.empty()) {
                    orderAccounts[clOrdId] = account;
                }
                return;
            }

            if (e.templateId == seq::ExecutionReport::sbeTemplateId())
            {
                seq::ExecutionReport msg;
                msg.wrapForDecode(body, seq::MessageHeader::encodedLength(),
                                  e.blockLength, e.version, e.payloadLength);
                printExecutionReport(msg);

                if (msg.execType() != seq::ExecType::Trade) { return; }

                const auto it = orderAccounts.find(msg.getClOrdIDAsString());
                if (it == orderAccounts.end()) {
                    // Fill for an order this client never saw the NewOrderSingle
                    // for (e.g. replay started mid-history without the full
                    // record); nothing to attribute it to.
                    return;
                }

                const std::string  symbol  = msg.getSymbolAsString();
                const std::int64_t lastQty = static_cast<std::int64_t>(msg.lastQty());
                const std::int64_t lastPx  = msg.lastPx();
                const auto         side    = msg.side();
                const std::int64_t signedQty =
                    (side == seq::Side::Sell || side == seq::Side::SellPlus
                     || side == seq::Side::SellShort) ? -lastQty : lastQty;

                portfolios[it->second][symbol].applyFill(signedQty, lastPx);
                return;
            }

            if (e.templateId == seq::PortfolioQueryRequest::sbeTemplateId())
            {
                seq::PortfolioQueryRequest msg;
                msg.wrapForDecode(body, seq::MessageHeader::encodedLength(),
                                  e.blockLength, e.version, e.payloadLength);

                const std::int32_t sourceId      = msg.header().sourceId();
                const std::int64_t correlationId = msg.correlationId();
                const std::string  account       = msg.getAccountAsString();

                const auto accountIt = portfolios.find(account);
                if (accountIt == portfolios.end() || accountIt->second.empty()) {
                    resultsQueue.push(PendingReply{
                        sourceId, correlationId, account,
                        usq::RiskQueryStatus::NoPositions, 0, 0, 0, 0, ""});
                    return;
                }

                std::vector<MockRiskEngine::Position> snapshot;
                snapshot.reserve(accountIt->second.size());
                for (const auto& [symbol, position] : accountIt->second) {
                    snapshot.push_back(MockRiskEngine::Position{
                        symbol, position.netQty, position.avgPxMantissa});
                }

                pendingQueries.push_back(PendingQuery{
                    sourceId, correlationId, account, std::move(snapshot)});
                return;
            }
            // Lifecycle events, admin FIX messages, and our own PortfolioQueryReply
            // looping back on the global stream carry nothing this client needs.
        });

    globalStream.start(aeron, replaySessionId, catchUpPosition, replayChannel.c_str());
    std::puts(catchUpPosition > 0
        ? "[OrderExecClient] Replaying history…"
        : "[OrderExecClient] Live from start");

    // ── Duty cycle ────────────────────────────────────────────────────────────
    aeron::concurrent::YieldingIdleStrategy idleStrategy;
    while (g_running)
    {
        int workCount = globalStream.poll();

        ingressSender.keepAlive();
        ingressSender.pollEgress([](const std::uint8_t*, std::int32_t) {});

        // Dispatch as many queued risk queries as there are free worker slots.
        while (!pendingQueries.empty() && riskQueryExecutor.tryReserveSlot()) {
            PendingQuery query = std::move(pendingQueries.front());
            pendingQueries.pop_front();
            riskQueryExecutor.execute(
                [&resultsQueue, &mockRiskEngine, query = std::move(query)]() mutable
                {
                    try {
                        const auto assessment = mockRiskEngine.assess(query.snapshot);
                        resultsQueue.push(PendingReply{
                            query.sourceId, query.correlationId, query.account,
                            usq::RiskQueryStatus::Ok,
                            static_cast<std::uint32_t>(assessment.riskScore),
                            assessment.grossExposureMantissa, assessment.netExposureMantissa,
                            static_cast<std::uint32_t>(query.snapshot.size()), ""});
                    } catch (const std::exception& e) {
                        resultsQueue.push(PendingReply{
                            query.sourceId, query.correlationId, query.account,
                            usq::RiskQueryStatus::Error, 0, 0, 0,
                            static_cast<std::uint32_t>(query.snapshot.size()), e.what()});
                    }
                });
            ++workCount;
        }

        // Drain one completed assessment per iteration and publish it.
        if (!pendingReply) {
            pendingReply = resultsQueue.pop();
        }
        if (pendingReply) {
            offerReply(*pendingReply);
            pendingReply.reset();
            ++workCount;
        }

        idleStrategy.idle(workCount);
    }

    std::puts("[OrderExecClient] Shutting down");
    ingressSender.close();
    return 0;
}
