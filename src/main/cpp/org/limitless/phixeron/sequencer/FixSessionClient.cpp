/*
 * FixSessionClient — TCP FIX gateway bridging FIX clients to the Aeron Cluster sequencer.
 *
 * Deterministic design invariant:
 *   Session state is updated ONLY by messages received from the cluster global
 *   stream. The cluster provides total ordering and consensus timestamps; these
 *   are used as the authoritative clock for all session timers.
 *
 * Inbound path (TCP → Cluster):
 *   1. Accepts FIX sessions on TCP port 9000.
 *   2. Decodes each message with simdfix PayloadDecoder<FIXT_1_1>.
 *   3. Every message — admin (Logon, Heartbeat, …) and application
 *      (NewOrderSingle, ExecutionReport) — is re-encoded as the matching
 *      sbe-unsequenced.xml message (schemaId=200) and offered to the cluster
 *      ingress directly, with header.sourceId/sessionId identifying the
 *      submitting connection. See ClusterIngressHandler.hpp.
 *
 * Outbound path (Global stream → TCP):
 *   On startup the client replays the archive (NULL_POSITION length → live
 *   follow-through on the same image). Each fragment on the global stream is
 *   a complete sbe-sequenced.xml message (schemaId=202); its templateId picks
 *   the branch:
 *   - Admin templates: decoded, and the corresponding session (looked up by
 *     header.sourceId) is driven with the cluster consensus timestamp. The
 *     session FSM then writes the FIX response to TCP.
 *   - ExecutionReport: decoded and re-encoded as FIX wire text (its MsgSeqNum
 *     was already reserved and stamped by ClusterIngressHandler at TCP-inbound
 *     submission time — see ClusterIngressHandler::sendExecutionReport — so
 *     re-encoding here reuses that value rather than assigning a new one) and
 *     delivered to TCP.
 *   - NewOrderSingle: the client's own submission echoed back; ignored here.
 *
 * Channels / ports (must match SequencerNode defaults):
 *   Archive control  aeron:udp?endpoint=localhost:9301  stream 100
 *   Cluster ingress  aeron:udp?endpoint=localhost:9302  stream 101
 *   Cluster egress   aeron:udp?endpoint=localhost:9320  stream 102
 *   Global stream    aeron:udp?endpoint=224.0.1.1:9200|interface=localhost  stream 1
 *   FIX TCP          0.0.0.0:9000
 */

#include <array>
#include <atomic>
#include <chrono>
#include <cinttypes>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <functional>
#include <limits>
#include <memory>
#include <span>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

// POSIX TCP / poll
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

// Aeron
#include "Aeron.h"
#include "FragmentAssembler.h"
#include "concurrent/AtomicBuffer.h"
#include "client/archive/AeronArchive.h"

// phixeron — session FSM + codec
#include "org/limitless/phixeron/session/ServerSession.hpp"
#include "org/limitless/phixeron/session/ResendCache.hpp"
#include "org/limitless/fix/decoder/PayloadDecoder.hpp"

// Generated FIX types (fix-application.xml via GenerateAppMessages)
#include "org/limitless/fix/generated/messages/FixMessageHandler.hpp"
#include "org/limitless/fix/generated/messages/FixMessageDecoders.hpp"
#include "org/limitless/fix/generated/messages/FixMessageEncoders.hpp"
#include "org/limitless/fix/generated/config/FixEngine.hpp"

// Global stream subscription + event types
#include "org/limitless/phixeron/sequencer/GlobalStreamClient.hpp"

// Cluster ingress session state machine (SessionConnectRequest → SessionEvent(OK)
// → send/keep-alive → SessionCloseRequest), talking to the cluster wire protocol
// generated from sbe-cluster.xml (trimmed mirror of io.aeron.cluster.codecs,
// schemaId=111).
#include "org/limitless/phixeron/sequencer/ClusterIngressSender.hpp"

// FIX-session-facing bridge into the cluster: byte-level FIX parsing helpers,
// CapturingTransport, FixSession alias and ClusterIngressHandler (admin SBE
// encoding + application-message routing). Split out so this logic can be
// unit tested against ClusterIngressSender's in-memory fakes (see
// ClusterIngressHandlerTest.cpp) without a real media driver or TCP socket.
#include "org/limitless/phixeron/sequencer/ClusterIngressHandler.hpp"

// SBE codecs for the sequencer → global stream egress schema (sbe-sequenced.xml
// schemaId=202) — used on this egress path to decode what comes back off the
// global stream. Ingress encoding (sbe-unsequenced.xml) is fully encapsulated
// in ClusterIngressHandler.hpp and does not need to be decoded here.
#include "org_limitless_phixeron_sbe_sequenced/MessageHeader.h"
#include "org_limitless_phixeron_sbe_sequenced/Header.h"
#include "org_limitless_phixeron_sbe_sequenced/Logon.h"
#include "org_limitless_phixeron_sbe_sequenced/Logout.h"
#include "org_limitless_phixeron_sbe_sequenced/Heartbeat.h"
#include "org_limitless_phixeron_sbe_sequenced/TestRequest.h"
#include "org_limitless_phixeron_sbe_sequenced/ResendRequest.h"
#include "org_limitless_phixeron_sbe_sequenced/SequenceReset.h"
#include "org_limitless_phixeron_sbe_sequenced/ExecutionReport.h"
#include "org_limitless_phixeron_sbe_sequenced/NewOrderSingle.h"

// ── Namespace aliases ──────────────────────────────────────────────────────────

namespace fix    = org::limitless::fix;
namespace sess   = org::limitless::phixeron::session;
namespace msg    = fix::generated::messages;
namespace cfg    = fix::generated::config;
namespace sequencer = org::limitless::phixeron::sequencer;
namespace seq = org::limitless::phixeron::sbe::sequenced;

using namespace aeron;
using namespace aeron::concurrent;
using namespace fix::generated::config;   // FIXT_1_1, MaxMessageSize, …
using namespace fix::generated::messages; // FixMessageHandler, LogonDecoder, …
using sequencer::nowMs;

// ── Constants ─────────────────────────────────────────────────────────────────

static constexpr uint16_t    FIX_TCP_PORT              = 9000;
static constexpr int         FIX_TCP_BACKLOG            = 8;
static constexpr const char* ARCHIVE_CONTROL_CHANNEL   = "aeron:udp?endpoint=localhost:9301";
static constexpr int32_t     ARCHIVE_CONTROL_STREAM    = 100;
static constexpr const char* ARCHIVE_RESPONSE_CHANNEL  = "aeron:udp?endpoint=localhost:0";
static constexpr const char* FIX_REPLAY_CHANNEL        = "aeron:udp?endpoint=localhost:9310";

// FIX byte-level helpers, sendRaw, CapturingTransport, FixSession and
// ClusterIngressHandler now live in ClusterIngressHandler.hpp so they can be
// unit tested against ClusterIngressSender's in-memory fakes (see
// ClusterIngressHandlerTest.cpp) without a real media driver or TCP socket.
using sequencer::ClusterIngressSender;
using sequencer::findFixMessageEnd;
using sequencer::fixTagRange;
using sequencer::patchResendFlags;
using sequencer::sendRaw;
using sequencer::CapturingTransport;
using sequencer::FixSession;
using sequencer::ClusterIngressHandler;

// ── SBE enum → FIX enum mapping (egress: sbe-sequenced.xml → simdfix) ────────
//
// Inverse of ClusterIngressHandler.hpp's toSbeX() maps; both mirror the same
// FIX values by construction (see fix-application.xml / sbe-unsequenced.xml /
// sbe-sequenced.xml — all three keep identical enum valid values).

static msg::Side fromSbeSide(seq::Side::Value v)
{
    switch (v) {
        case seq::Side::Value::Buy:       return msg::Side::Buy;
        case seq::Side::Value::Sell:      return msg::Side::Sell;
        case seq::Side::Value::BuyMinus:  return msg::Side::BuyMinus;
        case seq::Side::Value::SellPlus:  return msg::Side::SellPlus;
        case seq::Side::Value::SellShort: return msg::Side::SellShort;
        default: return msg::Side::Buy;
    }
}

static msg::ExecType fromSbeExecType(seq::ExecType::Value v)
{
    switch (v) {
        case seq::ExecType::Value::New:            return msg::ExecType::New;
        case seq::ExecType::Value::DoneForDay:     return msg::ExecType::DoneForDay;
        case seq::ExecType::Value::Canceled:       return msg::ExecType::Canceled;
        case seq::ExecType::Value::Replaced:       return msg::ExecType::Replaced;
        case seq::ExecType::Value::PendingCancel:  return msg::ExecType::PendingCancel;
        case seq::ExecType::Value::Stopped:        return msg::ExecType::Stopped;
        case seq::ExecType::Value::Rejected:       return msg::ExecType::Rejected;
        case seq::ExecType::Value::Suspended:      return msg::ExecType::Suspended;
        case seq::ExecType::Value::PendingNew:     return msg::ExecType::PendingNew;
        case seq::ExecType::Value::Calculated:     return msg::ExecType::Calculated;
        case seq::ExecType::Value::Expired:        return msg::ExecType::Expired;
        case seq::ExecType::Value::Restated:       return msg::ExecType::Restated;
        case seq::ExecType::Value::PendingReplace: return msg::ExecType::PendingReplace;
        case seq::ExecType::Value::Trade:          return msg::ExecType::Trade;
        case seq::ExecType::Value::TradeCorrect:   return msg::ExecType::TradeCorrect;
        case seq::ExecType::Value::TradeCancel:    return msg::ExecType::TradeCancel;
        case seq::ExecType::Value::OrderStatus:    return msg::ExecType::OrderStatus;
        default: return msg::ExecType::New;
    }
}

static msg::OrdStatus fromSbeOrdStatus(seq::OrdStatus::Value v)
{
    switch (v) {
        case seq::OrdStatus::Value::New:             return msg::OrdStatus::New;
        case seq::OrdStatus::Value::PartiallyFilled: return msg::OrdStatus::PartiallyFilled;
        case seq::OrdStatus::Value::Filled:          return msg::OrdStatus::Filled;
        case seq::OrdStatus::Value::DoneForDay:      return msg::OrdStatus::DoneForDay;
        case seq::OrdStatus::Value::Canceled:        return msg::OrdStatus::Canceled;
        case seq::OrdStatus::Value::Replaced:        return msg::OrdStatus::Replaced;
        case seq::OrdStatus::Value::PendingCancel:   return msg::OrdStatus::PendingCancel;
        case seq::OrdStatus::Value::Stopped:         return msg::OrdStatus::Stopped;
        case seq::OrdStatus::Value::Rejected:        return msg::OrdStatus::Rejected;
        case seq::OrdStatus::Value::Suspended:       return msg::OrdStatus::Suspended;
        case seq::OrdStatus::Value::PendingNew:      return msg::OrdStatus::PendingNew;
        case seq::OrdStatus::Value::Calculated:      return msg::OrdStatus::Calculated;
        case seq::OrdStatus::Value::Expired:         return msg::OrdStatus::Expired;
        case seq::OrdStatus::Value::PendingReplace:  return msg::OrdStatus::PendingReplace;
        default: return msg::OrdStatus::New;
    }
}

// Re-encodes a cluster-received sbe-sequenced.xml ExecutionReport as FIX
// wire text, using its own embedded seqNum/sendingTimeMs. Uses a standalone
// FixPayloadEncoder (independent of any live FixSession's outgoing counter)
// because ExecutionReport's MsgSeqNum was already reserved and stamped by
// ClusterIngressHandler at TCP-inbound submission time (see
// ClusterIngressHandler::sendExecutionReport) — this just reproduces that
// exact, already-assigned message, whether for normal delivery or a resend.
static std::vector<uint8_t> reencodeExecutionReportToFix(const seq::ExecutionReport& er)
{
    msg::FixPayloadEncoder<cfg::FIXT_1_1, "CLIENT", "SEQUENCER"> encoder;
    alignas(16) std::array<uint8_t, 512> buf{};
    encoder.wrap(0, std::span<uint8_t>(buf.data(), buf.size()));

    msg::ExecutionReportEncoder enc;
    encoder.wrapHeader(enc, er.seqNum(), std::chrono::milliseconds(er.sendingTimeMs()));

    enc.orderID(er.getOrderIDAsString())
       .clOrdID(er.getClOrdIDAsString())
       .execID(er.getExecIDAsString())
       .execType(fromSbeExecType(er.execType()))
       .ordStatus(fromSbeOrdStatus(er.ordStatus()))
       .symbol(er.getSymbolAsString())
       .side(fromSbeSide(er.side()))
       .orderQty(er.orderQty())
       .leavesQty(er.leavesQty())
       .cumQty(er.cumQty())
       .avgPx(fix::utils::FixedDecimal{er.avgPx()})
       .transactTime(std::chrono::milliseconds(er.transactTime()));
    if (er.price() != seq::ExecutionReport::priceNullValue()) {
        enc.price(fix::utils::FixedDecimal{er.price()});
    }
    if (er.lastQty() != seq::ExecutionReport::lastQtyNullValue()) {
        enc.lastQty(er.lastQty());
    }
    if (er.lastPx() != seq::ExecutionReport::lastPxNullValue()) {
        enc.lastPx(fix::utils::FixedDecimal{er.lastPx()});
    }
    if (er.text()[0] != '\0') {
        enc.text(er.getTextAsString());
    }

    const auto len = encoder.encode(enc);
    return std::vector<uint8_t>(buf.data(), buf.data() + len);
}

// ── Archive-backed resend fallback ────────────────────────────────────────────

// Shared handle to the archive connection and the global stream recording,
// used on-demand by FixConnection::handleResendRequest when the ResendCache
// doesn't hold everything a ResendRequest asks for. Owned by main() and
// outlives every FixConnection.
struct ArchiveResendContext
{
    std::shared_ptr<Aeron>                                aeron;
    std::shared_ptr<aeron::archive::client::AeronArchive> archive;
    std::int64_t                                          recordingId;
};

// On a resend-cache miss, scans the archived global stream from the beginning
// up to the current recording position, filtering for sbe-sequenced.xml
// ExecutionReport messages (schemaId=202) that this connId submitted
// (header.sourceId) and whose seqNum (tag 34) is in `missing`. Found entries
// are stored as the original SBE bytes verbatim — the caller re-decodes and
// re-encodes them to FIX text via reencodeExecutionReportToFix, same as a
// live cluster echo. This is a synchronous, bounded, occasional slow-path
// operation — resends are rare, so a full scan-and-filter is preferred here
// over maintaining a seqNum→archive-position index.
// Archive-backed resend recovery is a best-effort, occasional slow path
// (see comment above). The shared control-plane AeronArchive connection can
// go stale after being idle between resend calls (e.g. an archive-side
// control-session timeout), which surfaces as an ArchiveException from
// getRecordingPosition()/startReplay(). That must degrade to "nothing
// recovered" — the caller already turns any unfound sequence number into a
// GapFill — rather than take down the whole FIX gateway process.
static std::unordered_map<uint32_t, std::vector<uint8_t>> replayMissingAppMessages(
    ArchiveResendContext& ctx, int32_t connId, const std::unordered_set<uint32_t>& missing)
{
    std::unordered_map<uint32_t, std::vector<uint8_t>> found;
    if (missing.empty() || !ctx.archive) { return found; }

    try
    {
        const std::int64_t upToPosition = ctx.archive->getRecordingPosition(ctx.recordingId);
        if (upToPosition <= 0) { return found; }

        static constexpr const char* RESEND_REPLAY_CHANNEL = "aeron:udp?endpoint=localhost:9312";

        aeron::archive::client::ReplayParams replayParams;
        replayParams.position(0).length(upToPosition);
        const std::int64_t replaySessionId = ctx.archive->startReplay(
            ctx.recordingId, RESEND_REPLAY_CHANNEL, sequencer::REPLAY_STREAM_ID, replayParams);

        bool done = false;
        sequencer::GlobalStreamClient scan(
            [&](const sequencer::SequencedEvent& e)
            {
                if (e.templateId != seq::ExecutionReport::sbeTemplateId()) { return; }
                if (e.sourceId != connId) { return; }

                const auto* payload = reinterpret_cast<const uint8_t*>(e.payload);
                seq::ExecutionReport er;
                er.wrapForDecode(const_cast<char*>(reinterpret_cast<const char*>(payload)),
                                 seq::MessageHeader::encodedLength(),
                                 e.blockLength, e.version, e.payloadLength);
                const uint32_t seqNum = er.seqNum();

                if (missing.contains(seqNum) && !found.contains(seqNum)) {
                    found.emplace(seqNum, std::vector<uint8_t>(payload, payload + e.payloadLength));
                }
            },
            nullptr, nullptr,
            [&] { done = true; });

        scan.start(ctx.aeron, replaySessionId, upToPosition, RESEND_REPLAY_CHANNEL);

        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (!done && found.size() < missing.size() && std::chrono::steady_clock::now() < deadline)
        {
            if (scan.poll() == 0) { std::this_thread::yield(); }
        }
    }
    catch (const std::exception& e)
    {
        std::fprintf(stderr, "[Resend] archive recovery failed for connId=%d: %s\n",
                    connId, e.what());
    }

    return found;
}

// ── FixConnection ─────────────────────────────────────────────────────────────
//
// TCP receive path:  ClusterIngressHandler encodes every FIX message (admin
//                   and application) as sbe-unsequenced.xml and forwards it
//                   to the cluster. Session state is NOT updated here.
//
// Global stream path: FixConnection::onClusterAdmin() and
//                     onClusterExecutionReport() are called by the main loop
//                     when a sbe-sequenced.xml message arrives that was
//                     sourced by this connection (matched by header.sourceId). They
//                     advance the session clock to the cluster timestamp;
//                     application messages are also delivered to TCP and
//                     cached here (see ResendCache), never at the point they
//                     are locally encoded.

// Session only needs a transport; it no longer handles inbound application messages.

static constexpr std::size_t MAX_RECV_BUF = 1u * 1024u * 1024u;  // 1 MB

struct FixConnection
{
    int                     fd;
    bool                    m_dead{false};
    ArchiveResendContext*   archiveCtx;
    // Latest outbound FIX messages, keyed by MsgSeqNum, used to satisfy
    // ResendRequest replay without consulting the archive on every request.
    sess::ResendCache       m_resendCache{2500};
    FixSession              session;
    ClusterIngressHandler   ingressHandler;
    fix::decoder::PayloadDecoder<FIXT_1_1> decoder;
    std::vector<uint8_t>                   recvBuf;

    FixConnection(int fd_, ClusterIngressSender* ingress, ArchiveResendContext* archiveCtx_)
        : fd(fd_)
        , archiveCtx(archiveCtx_)
        , session(FixSession::Builder{}
                      .transport(CapturingTransport{fd_})
                      .build())
        , ingressHandler(ingress, fd_, &session)
    {
        session.onTcpConnected();
        recvBuf.reserve(8192);
    }

    [[nodiscard]] bool isDead() const noexcept { return m_dead; }

    // Handles a ResendRequest from the cluster global stream.
    //
    // Walks [begin, limit) against the ResendCache. Whatever the cache misses
    // (evicted, or never cached because it predates this cache instance) is
    // looked up in one batched archive replay pass. Anything still missing
    // after that must never have been an application message at all (the FIX
    // seqnum space is shared and gapless across admin/app messages), so it is
    // covered by a SequenceReset GapFill instead. The session's outgoing
    // sequence counter is restored to savedNext afterwards.
    void handleResendRequest(uint32_t begin, uint32_t end, int64_t clusterTs)
    {
        session.setNowMs(clusterTs);
        if (!session.isActive()) { return; }

        const uint32_t savedNext = session.nextOutgoingSeqNum();
        const uint32_t limit     = (end == 0 || end + 1 >= savedNext)
                                   ? savedNext : end + 1;

        std::printf("[Resend] fd=%d begin=%u end=%u limit=%u cacheSize=%zu\n",
                    fd, begin, end, limit, m_resendCache.size());

        std::unordered_set<uint32_t> missing;
        for (uint32_t seq = begin; seq < limit; ++seq) {
            if (m_resendCache.get(seq) == nullptr) { missing.insert(seq); }
        }

        std::unordered_map<uint32_t, std::vector<uint8_t>> recovered;
        if (!missing.empty() && archiveCtx)
        {
            std::printf("[Resend] fd=%d %zu seq missing from cache; consulting archive\n",
                        fd, missing.size());
            recovered = replayMissingAppMessages(*archiveCtx, fd, missing);
            std::printf("[Resend] fd=%d archive recovered %zu of %zu missing\n",
                        fd, recovered.size(), missing.size());
        }

        uint32_t gapStart = 0;

        for (uint32_t seq = begin; seq < limit; ++seq)
        {
            const std::vector<uint8_t>* bytes = m_resendCache.get(seq);
            std::vector<uint8_t> archiveCopy;
            if (!bytes)
            {
                const auto it = recovered.find(seq);
                if (it != recovered.end())
                {
                    archiveCopy = it->second;
                    bytes = &archiveCopy;
                }
            }

            if (!bytes)
            {
                // Neither cache nor archive holds an app message for this
                // seq — it was an admin message. Accumulate into the gap.
                if (gapStart == 0) { gapStart = seq; }
                continue;
            }

            if (gapStart != 0)
            {
                std::printf("[Resend] fd=%d GapFill [%u, %u)\n", fd, gapStart, seq);
                session.sendGapFill(gapStart, seq, clusterTs);
                gapStart = 0;
            }

            // Decode the cached/recovered sbe-sequenced ExecutionReport,
            // re-encode it as FIX text, then patch PossDupFlag +
            // OrigSendingTime into a copy and send raw.
            seq::MessageHeader hdr;
            hdr.wrap(reinterpret_cast<char*>(const_cast<uint8_t*>(bytes->data())), 0,
                     seq::MessageHeader::sbeSchemaVersion(), bytes->size());
            seq::ExecutionReport er;
            er.wrapForDecode(reinterpret_cast<char*>(const_cast<uint8_t*>(bytes->data())),
                             seq::MessageHeader::encodedLength(),
                             hdr.blockLength(), hdr.version(), bytes->size());
            std::vector<uint8_t> patched = reencodeExecutionReportToFix(er);
            patchResendFlags(patched);

            std::printf("[Resend] fd=%d AppMsg seq=%u bytes=%zu\n",
                        fd, seq, patched.size());
            sendRaw(fd, patched.data(), patched.size());

            session.setNextOutgoingSeqNum(seq + 1);
        }

        // Flush any trailing admin gap
        if (gapStart != 0)
        {
            std::printf("[Resend] fd=%d GapFill [%u, %u)\n", fd, gapStart, limit);
            session.sendGapFill(gapStart, limit, clusterTs);
        }

        // Restore the outgoing counter (matters when end < savedNext)
        session.setNextOutgoingSeqNum(savedNext);
    }

    // TCP receive: forward every complete FIX message to the cluster as SBE.
    void onRecv(const uint8_t* data, std::size_t len)
    {
        if (recvBuf.size() + len > MAX_RECV_BUF)
        {
            std::fprintf(stderr, "[FixConnection] fd=%d recv buffer exceeded 1 MB; dropping\n", fd);
            m_dead = true;
            return;
        }
        recvBuf.insert(recvBuf.end(), data, data + len);

        std::size_t consumed = 0;
        while (consumed < recvBuf.size()) {
            const auto remaining = std::span<const uint8_t>(
                recvBuf.data() + consumed, recvBuf.size() - consumed);

            const std::size_t msgLen = findFixMessageEnd(remaining);
            if (msgLen == 0) { break; }

            const auto msgSpan = remaining.subspan(0, msgLen);
            ingressHandler.setRawBytes(msgSpan);
            const auto parseResult = decoder.parse(msgSpan, ingressHandler);
            std::printf("[FixConnection] fd=%d parse status=%d processed=%zu msgLen=%zu\n",
                        fd, static_cast<int>(parseResult.m_value),
                        static_cast<std::size_t>(parseResult.m_processed), msgLen);
            consumed += msgLen;
        }

        if (consumed > 0) {
            recvBuf.erase(recvBuf.begin(),
                          recvBuf.begin() + static_cast<std::ptrdiff_t>(consumed));
        }
    }

    // Global stream: drive session state from decoded SBE admin message with
    // the cluster consensus timestamp as the authoritative clock.
    void onClusterAdmin(uint16_t templateId, const char* sbeBody,
                        uint64_t sbeBodyLen, uint16_t blockLen, uint16_t version,
                        int64_t clusterTimestampMs)
    {
        session.setNowMs(clusterTimestampMs);
        switch (templateId)
        {
        case seq::Logon::sbeTemplateId():
        {
            seq::Logon msg;
            msg.wrapForDecode(const_cast<char*>(sbeBody), seq::MessageHeader::encodedLength(),
                              blockLen, version, sbeBodyLen);
            session.handleClusterLogon(msg.heartbeatInterval(), clusterTimestampMs);
            break;
        }
        case seq::Logout::sbeTemplateId():
            session.handleClusterLogout(clusterTimestampMs);
            break;

        case seq::Heartbeat::sbeTemplateId():
        {
            seq::Heartbeat msg;
            msg.wrapForDecode(const_cast<char*>(sbeBody), seq::MessageHeader::encodedLength(),
                              blockLen, version, sbeBodyLen);
            session.handleClusterHeartbeat(clusterTimestampMs);
            break;
        }
        case seq::TestRequest::sbeTemplateId():
        {
            seq::TestRequest msg;
            msg.wrapForDecode(const_cast<char*>(sbeBody), seq::MessageHeader::encodedLength(),
                              blockLen, version, sbeBodyLen);
            session.handleClusterTestRequest(msg.testReqID(), clusterTimestampMs);
            break;
        }
        case seq::ResendRequest::sbeTemplateId():
        {
            seq::ResendRequest msg;
            msg.wrapForDecode(const_cast<char*>(sbeBody), seq::MessageHeader::encodedLength(),
                              blockLen, version, sbeBodyLen);
            handleResendRequest(msg.beginSeqNo(), msg.endSeqNo(), clusterTimestampMs);
            break;
        }
        case seq::SequenceReset::sbeTemplateId():
        {
            seq::SequenceReset msg;
            msg.wrapForDecode(const_cast<char*>(sbeBody), seq::MessageHeader::encodedLength(),
                              blockLen, version, sbeBodyLen);
            session.handleClusterSequenceReset(msg.newSeqNo(), clusterTimestampMs);
            break;
        }
        default:
            std::fprintf(stderr, "[FixConnection] fd=%d unknown SBE templateId=%u; ignored\n",
                         fd, templateId);
            break;
        }
    }

    // Global stream: an outbound application message (ExecutionReport) this
    // connection submitted to the cluster has been sequenced and echoed back.
    // sbeBody/sbeBodyLen is the full sbe-sequenced.xml ExecutionReport
    // payload (starting at its own 8-byte messageHeader), matching what
    // replayMissingAppMessages stores in the archive-recovery map — the
    // resend cache stores exactly the same shape so handleResendRequest can
    // treat a cache hit and an archive hit identically. ExecutionReport's
    // MsgSeqNum was already reserved and stamped by ClusterIngressHandler at
    // TCP-inbound submission time, so it's read here, not assigned.
    void onClusterExecutionReport(const char* sbeBody, uint64_t sbeBodyLen,
                                  uint16_t blockLen, uint16_t version,
                                  int64_t clusterTimestampMs)
    {
        session.setNowMs(clusterTimestampMs);

        seq::ExecutionReport er;
        er.wrapForDecode(const_cast<char*>(sbeBody), seq::MessageHeader::encodedLength(),
                         blockLen, version, sbeBodyLen);
        const uint32_t seqNum = er.seqNum();
        if (seqNum == 0) { return; }

        m_resendCache.put(seqNum, std::span<const uint8_t>(
            reinterpret_cast<const uint8_t*>(sbeBody), sbeBodyLen));

        const std::vector<uint8_t> fixBytes = reencodeExecutionReportToFix(er);
        sendRaw(fd, fixBytes.data(), fixBytes.size());
    }
};

// ── TcpServer ─────────────────────────────────────────────────────────────────

class TcpServer
{
public:
    void start(uint16_t port)
    {
        m_listenFd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (m_listenFd < 0) {
            throw std::runtime_error("socket() failed: " + std::string(std::strerror(errno)));
        }

        const int one = 1;
        ::setsockopt(m_listenFd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

        sockaddr_in addr{};
        addr.sin_family      = AF_INET;
        addr.sin_addr.s_addr = INADDR_ANY;
        addr.sin_port        = htons(port);

        if (::bind(m_listenFd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
            throw std::runtime_error("bind() failed: " + std::string(std::strerror(errno)));
        }
        if (::listen(m_listenFd, FIX_TCP_BACKLOG) < 0) {
            throw std::runtime_error("listen() failed: " + std::string(std::strerror(errno)));
        }

        const int flags = ::fcntl(m_listenFd, F_GETFL, 0);
        ::fcntl(m_listenFd, F_SETFL, flags | O_NONBLOCK);

        std::printf("[TCP] Listening on port %d\n", port);
    }

    // Returns a new non-blocking fd, or -1 if no connection is pending.
    int acceptNewConnection()
    {
        sockaddr_in addr{};
        socklen_t   addrLen = sizeof(addr);
        const int fd = ::accept(m_listenFd, reinterpret_cast<sockaddr*>(&addr), &addrLen);
        if (fd < 0) { return -1; }

        const int flags = ::fcntl(fd, F_GETFL, 0);
        ::fcntl(fd, F_SETFL, flags | O_NONBLOCK);

        char ip[INET_ADDRSTRLEN];
        ::inet_ntop(AF_INET, &addr.sin_addr, ip, sizeof(ip));
        std::printf("[TCP] Accepted fd=%d from %s:%d\n", fd, ip, ntohs(addr.sin_port));
        return fd;
    }

    int  listenFd() const { return m_listenFd; }
    void shutdown()
    {
        if (m_listenFd >= 0) { ::close(m_listenFd); m_listenFd = -1; }
    }

private:
    int m_listenFd{-1};
};

// ── Signal handling ───────────────────────────────────────────────────────────

static std::atomic<bool> g_running{true};
static void sigintHandler(int) { g_running = false; }

// ── Archive helpers ───────────────────────────────────────────────────────────

static std::int64_t findGlobalStreamRecording(
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
        sequencer::GLOBAL_STREAM_CHANNEL, sequencer::GLOBAL_STREAM_ID,
        [&](aeron::archive::client::RecordingDescriptor& desc) {
            if (desc.m_stopPosition == aeron::archive::client::NULL_POSITION) {
                activeId = desc.m_recordingId;  // active recording wins
            } else if (desc.m_stopPosition > stoppedPos) {
                stoppedId  = desc.m_recordingId;
                stoppedPos = desc.m_stopPosition;
            }
        });

    if (activeId < 0 && stoppedId < 0) {
        throw std::runtime_error(
            "[FixSessionClient] No global stream recording found on " +
            std::string(sequencer::GLOBAL_STREAM_CHANNEL));
    }

    if (activeId >= 0) {
        // Resolve actual write position so the catch-up check is accurate.
        catchUpPos = archive.getRecordingPosition(activeId);
        if (catchUpPos == aeron::archive::client::NULL_POSITION) { catchUpPos = 0; }
        return activeId;
    }

    catchUpPos = stoppedPos;
    return stoppedId;
}

// ── main ──────────────────────────────────────────────────────────────────────

int main()
{
    signal(SIGINT, sigintHandler);

    // ── Aeron ────────────────────────────────────────────────────────────────
    aeron::Context aeronCtx;
    auto aeron = Aeron::connect(aeronCtx);
    std::puts("[FixSessionClient] Connected to Aeron media driver");

    // ── Archive ──────────────────────────────────────────────────────────────
    aeron::archive::client::Context archiveCtx;
    archiveCtx.aeron(aeron)
              .controlRequestChannel(ARCHIVE_CONTROL_CHANNEL)
              .controlRequestStreamId(ARCHIVE_CONTROL_STREAM)
              .controlResponseChannel(ARCHIVE_RESPONSE_CHANNEL);

    auto archive = aeron::archive::client::AeronArchive::connect(archiveCtx);
    std::puts("[FixSessionClient] Connected to Aeron Archive");

    // ── Locate global stream recording ───────────────────────────────────────
    std::int64_t catchUpPosition = 0;
    const std::int64_t recordingId =
        findGlobalStreamRecording(*archive, catchUpPosition);
    std::printf("[FixSessionClient] Recording %" PRId64
                "  catchUpPosition=%" PRId64 "\n", recordingId, catchUpPosition);

    // Shared with every FixConnection for the on-demand resend archive fallback.
    ArchiveResendContext resendArchiveCtx{aeron, archive, recordingId};

    // ── Start replay (only when there is historical data to replay) ──────────
    std::int64_t replaySessionId = -1;
    if (catchUpPosition > 0) {
        aeron::archive::client::ReplayParams replayParams;
        replayParams.position(0).length(aeron::archive::client::NULL_LENGTH);
        replaySessionId = archive->startReplay(
            recordingId, FIX_REPLAY_CHANNEL, sequencer::REPLAY_STREAM_ID, replayParams);
        std::printf("[FixSessionClient] Replay started  replaySessionId=%" PRId64 "\n",
                    replaySessionId);
    } else {
        std::puts("[FixSessionClient] No historical data — subscribing to live stream");
    }

    // ── Cluster ingress ───────────────────────────────────────────────────────
    ClusterIngressSender ingressSender;
    ingressSender.connect(aeron);

    // ── TCP server ────────────────────────────────────────────────────────────
    TcpServer tcpServer;
    tcpServer.start(FIX_TCP_PORT);

    // ── Per-connection state ──────────────────────────────────────────────────
    std::unordered_map<int, std::unique_ptr<FixConnection>> connections;

    // ── Global stream subscription ────────────────────────────────────────────
    // e.payload is directly a sbe-sequenced.xml message (schemaId=202) — no
    // envelope to strip. GlobalStreamClient already decoded the outer
    // messageHeader and the header composite generically, exposing
    // sourceId/templateId/blockLength/version on the event, so this lambda
    // just routes to the right FixConnection and picks the specific decode.
    sequencer::GlobalStreamClient globalStream(
        [&](const sequencer::SequencedEvent& e)
        {
            std::printf("[Global] SequencedEvent globalSeq=%" PRId64 " payloadLen=%" PRIu64 "\n",
                        e.globalSeqNo, static_cast<uint64_t>(e.payloadLength));

            auto it = connections.find(e.sourceId);
            if (it == connections.end()) { return; }

            const auto* body    = reinterpret_cast<const char*>(e.payload);
            const uint64_t bodyLen = e.payloadLength;

            if (e.templateId == seq::ExecutionReport::sbeTemplateId())
            {
                it->second->onClusterExecutionReport(body, bodyLen, e.blockLength,
                                                     e.version, e.clusterTimestamp);
                return;
            }
            if (e.templateId == seq::NewOrderSingle::sbeTemplateId())
            {
                // The client's own submission, echoed back for ordering; not
                // ours to deliver or cache (see onClusterExecutionReport).
                return;
            }

            std::printf("[Global] Admin SBE connId=%d connections.size=%zu\n",
                        e.sourceId, connections.size());
            it->second->onClusterAdmin(e.templateId, body, bodyLen,
                                       e.blockLength, e.version, e.clusterTimestamp);
        },
        [](const sequencer::LifecycleEvent& e)
        {
            std::printf("[Global] Source connected    id=%" PRId64 "\n",
                        e.sourceSessionId);
        },
        [](const sequencer::LifecycleEvent& e)
        {
            std::printf("[Global] Source disconnected id=%" PRId64 "\n",
                        e.sourceSessionId);
        },
        []() { std::puts("[Global] Caught up to live stream"); }
    );

    globalStream.start(aeron, replaySessionId, catchUpPosition, FIX_REPLAY_CHANNEL);
    std::puts(catchUpPosition > 0
        ? "[FixSessionClient] Replaying history…"
        : "[FixSessionClient] Live from start");

    // ── Duty cycle ────────────────────────────────────────────────────────────
    alignas(16) std::array<uint8_t, 8192> recvBuf{};

    while (g_running)
    {
        // Build poll set: listen fd + all client fds.
        std::vector<pollfd> pfds;
        pfds.reserve(1 + connections.size());
        pfds.push_back({tcpServer.listenFd(), POLLIN, 0});
        for (const auto& [fd, _] : connections) {
            pfds.push_back({fd, POLLIN, 0});
        }

        ::poll(pfds.data(), static_cast<nfds_t>(pfds.size()), 0);

        // New TCP connections.
        if (pfds[0].revents & POLLIN) {
            const int clientFd = tcpServer.acceptNewConnection();
            if (clientFd >= 0) {
                try {
                    connections.emplace(clientFd,
                        std::make_unique<FixConnection>(clientFd, &ingressSender, &resendArchiveCtx));
                } catch (...) {
                    ::close(clientFd);
                    throw;
                }
            }
        }

        // Inbound data from established connections.
        std::vector<int> toClose;
        for (std::size_t i = 1; i < pfds.size(); ++i) {
            if (!(pfds[i].revents & (POLLIN | POLLHUP | POLLERR))) { continue; }
            const int fd = pfds[i].fd;

            const ssize_t n = ::recv(fd, recvBuf.data(), recvBuf.size(), 0);
            if (n <= 0) {
                std::printf("[TCP] fd=%d closed (%s)\n", fd,
                            n == 0 ? "EOF" : std::strerror(errno));
                toClose.push_back(fd);
            } else {
                auto it = connections.find(fd);
                if (it != connections.end()) {
                    it->second->onRecv(recvBuf.data(), static_cast<std::size_t>(n));
                    if (it->second->isDead() || it->second->session.isPendingClose()) {
                        toClose.push_back(fd);
                    }
                }
            }
        }

        for (const int fd : toClose) {
            connections.erase(fd);
            ::close(fd);
        }

        // Heartbeat keep-alive: use cluster-driven m_nowMs (last set from
        // clusterTimestamp when a global-stream message was processed) so that
        // outbound heartbeats are timed by cluster consensus, not wall-clock.
        for (auto& [_, conn] : connections) {
            conn->session.keepAlive();
        }

        // Aeron: global stream + cluster egress + session keep-alive.
        const int aeronWork = globalStream.poll();
        ingressSender.keepAlive();
        ingressSender.pollEgress([](const uint8_t* /*data*/, int32_t /*len*/) {});

        if (aeronWork == 0 && pfds[0].revents == 0) {
            std::this_thread::yield();
        }
    }

    std::printf("[FixSessionClient] Shutting down. Active connections: %zu\n",
                connections.size());
    for (const auto& [fd, _] : connections) { ::close(fd); }
    tcpServer.shutdown();
    ingressSender.close();
    return 0;
}
