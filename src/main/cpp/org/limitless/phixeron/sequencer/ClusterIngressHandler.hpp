#pragma once

// ClusterIngressHandler — FIX-session-facing bridge into the Aeron Cluster.
//
// Everything here is pure logic (byte-level FIX parsing helpers, SBE encoding,
// application-message routing) built on top of ClusterIngressSender's
// IngressTransport/EgressTransport seam. Nothing in this file touches a raw
// TCP socket fd for anything observable in a test: CapturingTransport calls
// sendRaw(fd, ...), but that is a fire-and-forget guarded no-op when fd < 0,
// and ClusterIngressHandler's own encoders never inspect their output — they
// only push bytes through ClusterIngressSender, which tests drive with the
// same in-memory IngressTransport/EgressTransport fakes as
// ClusterIngressSenderTest.cpp (see ClusterIngressHandlerTest.cpp).

#include <array>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <span>
#include <string_view>

#include <errno.h>
#include <sys/socket.h>
#include <unistd.h>

#include "org/limitless/simdifx/generated/messages/FixMessageHandler.hpp"
#include "org/limitless/simdifx/generated/messages/FixMessageDecoders.hpp"
#include "org/limitless/simdifx/generated/config/FixEngine.hpp"

#include "org/limitless/phixeron/fix/ServerSession.hpp"
#include "org/limitless/phixeron/sequencer/ClusterIngressSender.hpp"
#include "org/limitless/phixeron/fix/Conversions.hpp"

#include "org_limitless_phixeron_sbe_unsequenced/MessageHeader.h"
#include "org_limitless_phixeron_sbe_unsequenced/Header.h"
#include "org_limitless_phixeron_sbe_unsequenced/Logon.h"
#include "org_limitless_phixeron_sbe_unsequenced/Logout.h"
#include "org_limitless_phixeron_sbe_unsequenced/Heartbeat.h"
#include "org_limitless_phixeron_sbe_unsequenced/TestRequest.h"
#include "org_limitless_phixeron_sbe_unsequenced/ResendRequest.h"
#include "org_limitless_phixeron_sbe_unsequenced/SequenceReset.h"
#include "org_limitless_phixeron_sbe_unsequenced/ExecutionReport.h"
#include "org_limitless_phixeron_sbe_unsequenced/NewOrderSingle.h"

namespace org::limitless::phixeron::sequencer
{

namespace fix = limitless::simdifx;
namespace sess = phixeron::fix;
namespace msg = fix::generated::messages;
namespace cfg = fix::generated::config;
namespace usq = sbe::unsequenced;

using sess::toSbeSide;
using sess::toSbeOrdType;
using sess::toSbeHandlInst;
using sess::toSbeTimeInForce;

// ── send FIX message ────────────────────────────────────────────────────────────

// Blocking TCP send of a complete byte range; shared by CapturingTransport,
// FixConnection::handleResendRequest and FixConnection's ExecutionReport delivery.
inline void sendMessage(const int fd, const std::uint8_t* data, std::size_t length)
{
    if (fd < 0 || length == 0)
    {
        return;
    }

    std::size_t sent = 0;
    while (sent < length)
    {
        const ssize_t n = ::send(fd, data + sent, length - sent, MSG_NOSIGNAL);
        if (n < 0)
        {
            if (errno == EINTR)
            {
                continue;
            }
            std::fprintf(stderr, "[TcpTransport] send fd=%d failed: %s\n", fd, std::strerror(errno));
            return;
        }
        sent += static_cast<std::size_t>(n);
    }
}

// ── CapturingTransport ────────────────────────────────────────────────────────

struct CapturingTransport
{
    int fd{-1};

    void operator()(std::span<const std::uint8_t> bytes) const
    {
        sendMessage(fd, bytes.data(), bytes.size());
    }
};

using FixSession = sess::ServerSession<cfg::FIXT_1_1, "SEQUENCER", "CLIENT", CapturingTransport>;

// Reserved SenderCompID (tag 49) stamped on the gateway's own chunk-boundary
// SequenceReset when a large ResendRequest is served in 1000-message chunks
// (see FixConnection::handleResendRequest). Every client-originated message on
// the global stream carries sender="CLIENT" (hardcoded below), so this value —
// which no real client uses — lets FixConnection recognize its own checkpoint
// reset when it round-trips back on the sequenced stream, without an SBE schema
// change. Exactly 8 chars to fill the fixed compId field with no padding.
inline constexpr std::string_view RESEND_CHUNK_COMPID = "RSNDCHNK";

// ── ClusterIngressHandler ─────────────────────────────────────────────────────

// Receives decoded FIX messages on the TCP receive path and forwards them to the cluster WITHOUT updating
// any session state.
class ClusterIngressHandler : public msg::FixMessageHandler<ClusterIngressHandler>
{
    ClusterIngressSender*         m_ingress{};
    std::int32_t                  m_connectionId{-1};
    FixSession*                   m_session{nullptr};

    alignas(16) std::array<std::uint8_t, 8192> m_buffer{};
    std::span<const std::uint8_t> m_rawFixBytes;

    usq::NewOrderSingle m_newOrderSingle;

    [[nodiscard]] char* buffer()
    {
        return reinterpret_cast<char*>(m_buffer.data());
    }

    [[nodiscard]] uint64_t bufferLength() const
    {
        return m_buffer.size();
    }

    template <typename SbeMsg>
    void sendUnsequenced(SbeMsg& msg)
    {
        if (!m_ingress)
        {
            return;
        }
        m_ingress->send(m_buffer.data(), static_cast<std::uint16_t>(msg.sbePosition()));
    }

public:
    using FixMessageHandler::handle;

    ClusterIngressHandler() = default;
    ClusterIngressHandler(ClusterIngressSender* ingress, int32_t connId, FixSession* session = nullptr)
        : m_ingress(ingress), m_connectionId(connId), m_session(session) {}

    // Test-only: lets ClusterIngressHandlerTest feed the exact raw FIX bytes a
    // message was parsed from, independent of decoder.parse()'s own buffer.
    // Not used by any handle() overload in production.
    void setRawBytes(std::span<const std::uint8_t> bytes) { m_rawFixBytes = bytes; }

    usq::Logon m_logon;
    usq::Logout m_logout;
    usq::Heartbeat m_heartbeat;
    usq::TestRequest m_testRequest;
    usq::ResendRequest m_resendRequest;
    usq::SequenceReset m_sequenceReset;
    usq::ExecutionReport m_executionReport;

    // Real SenderCompID (tag 49) from the TCP-received Logon, captured here for
    // FixConnection's one-session-per-SenderCompID check when the cluster
    // confirms this Logon (see FixConnection::onClusterAdmin). Not itself sent
    // to the cluster — Sender/Target on the wire to the cluster are still the
    // hardcoded "CLIENT"/"SEQNCR" literals below (todo.md item 10).
    std::string m_senderCompId;

    [[nodiscard]] const std::string& senderCompId() const
    {
        return m_senderCompId;
    }

    // Verifies an inbound message's SenderCompID (tag 49) / TargetCompID
    // (tag 56) against this session's expected identity:
    //   - TargetCompID must always equal our own compId (FixSession::senderCompId()) —
    //     the client must be addressing this gateway, not some other identity.
    //   - SenderCompID must equal whatever was captured at Logon (m_senderCompId) —
    //     an authenticated session's identity can't change mid-session. Not
    //     checked for the Logon itself, since m_senderCompId is still empty at
    //     that point (it's being established by this very message).
    // Callers distinguish "on Logon" (Logout+disconnect) from "mid-session"
    // (Reject, session stays up) per FIX 4.4 — see handle(LogonDecoder&) vs.
    // the other handle() overloads below.
    template <typename Decoder>
    [[nodiscard]] bool verifyCompIds(const Decoder& message) const
    {
        const auto sender = message.sender();
        const auto target = message.target();
        if (!sender || !target)
        {
            return false;
        }
        if (*target != FixSession::senderCompId())
        {
            return false;
        }
        if (!m_senderCompId.empty() && *sender != m_senderCompId)
        {
            return false;
        }
        return true;
    }

    fix::Result handle(const msg::LogonDecoder& logon)
    {
        if (!verifyCompIds(logon))
        {
            std::fprintf(stderr, "[Ingress] Logon fd=%d rejected: Invalid CompId\n", m_connectionId);
            if (m_session)
            {
                m_session->rejectLogon("Invalid CompId");
            }
            return fix::Result::Success;
        }
        const std::uint32_t hbSecs = logon.heartbeatInterval().value_or(30u);
        if (const auto sender = logon.sender())
        {
            m_senderCompId.assign(sender->data(), sender->size());
        }
        std::printf("[Ingress] Logon from fd=%d hbSecs=%u → encoding sbe-unsequenced\n",
                    m_connectionId, hbSecs);
        m_logon.wrapAndApplyHeader(buffer(), 0, bufferLength());
        m_logon.header().sourceId(m_ingress ? m_ingress->sourceId() : 0).connectionId(m_connectionId).sessionId(m_ingress ? m_ingress->clusterSessionId() : -1);
        m_logon.putSender(static_cast<const char*>("CLIENT  ")).putTarget(static_cast<const char*>("SEQNCR  "))
                .seqNum(logon.sequenceNumber().value_or(0u)).sendingTimeMs(nowMs())
                .encryptMethod(usq::EncryptMethod::Value::None)
                .heartbeatInterval(hbSecs)
                .putXmlData(nullptr, 0);
        sendUnsequenced(m_logon);
        std::printf("[Ingress] Logon sent to cluster (fd=%d sbePos=%llu)\n",
                    m_connectionId, static_cast<unsigned long long>(m_logon.sbePosition()));
        return fix::Result::Success;
    }

    fix::Result handle(const msg::LogoutDecoder& logout)
    {
        if (!verifyCompIds(logout))
        {
            std::fprintf(stderr, "[Ingress] Logout fd=%d rejected: Invalid CompId\n", m_connectionId);
            if (m_session)
            {
                m_session->sendReject(logout.sequenceNumber().value_or(0u),
                                       msg::SessionRejectReason::CompIDProblem, "Invalid CompId");
            }
            return fix::Result::Success;
        }
        m_logout.wrapAndApplyHeader(buffer(), 0, bufferLength());
        m_logout.header().sourceId(m_ingress ? m_ingress->sourceId() : 0).connectionId(m_connectionId).sessionId(m_ingress ? m_ingress->clusterSessionId() : -1);
        m_logout.putSender(static_cast<const char*>("CLIENT  ")).putTarget(static_cast<const char*>("SEQNCR  "))
                .seqNum(logout.sequenceNumber().value_or(0u)).sendingTimeMs(nowMs())
                .putText(nullptr, 0);
        sendUnsequenced(m_logout);
        return fix::Result::Success;
    }

    fix::Result handle(const msg::HeartbeatDecoder& heartbeat)
    {
        if (!verifyCompIds(heartbeat))
        {
            std::fprintf(stderr, "[Ingress] Heartbeat fd=%d rejected: Invalid CompId\n", m_connectionId);
            if (m_session)
            {
                m_session->sendReject(heartbeat.sequenceNumber().value_or(0u),
                                       msg::SessionRejectReason::CompIDProblem, "Invalid CompId");
            }
            return fix::Result::Success;
        }
        m_heartbeat.wrapAndApplyHeader(buffer(), 0, bufferLength());
        m_heartbeat.header().sourceId(m_ingress ? m_ingress->sourceId() : 0).connectionId(m_connectionId).sessionId(m_ingress ? m_ingress->clusterSessionId() : -1);
        m_heartbeat.putSender(static_cast<const char*>("CLIENT  ")).putTarget(static_cast<const char*>("SEQNCR  "))
                .seqNum(heartbeat.sequenceNumber().value_or(0u)).sendingTimeMs(nowMs());
        if (const auto id = heartbeat.testReqID()) {
            const auto sv = *id;
            const std::size_t n = std::min(sv.size(), static_cast<std::size_t>(32));
            std::memcpy(m_heartbeat.testReqID(), sv.data(), n);
            if (n < 32) { m_heartbeat.testReqID()[n] = '\0'; }
        } else {
            m_heartbeat.testReqID()[0] = '\0';
        }
        sendUnsequenced(m_heartbeat);
        return fix::Result::Success;
    }

    fix::Result handle(const msg::TestRequestDecoder& testRequest)
    {
        if (!verifyCompIds(testRequest))
        {
            std::fprintf(stderr, "[Ingress] TestRequest fd=%d rejected: Invalid CompId\n", m_connectionId);
            if (m_session)
            {
                m_session->sendReject(testRequest.sequenceNumber().value_or(0u),
                                       msg::SessionRejectReason::CompIDProblem, "Invalid CompId");
            }
            return fix::Result::Success;
        }
        m_testRequest.wrapAndApplyHeader(buffer(), 0, bufferLength());
        m_testRequest.header().sourceId(m_ingress ? m_ingress->sourceId() : 0).connectionId(m_connectionId).sessionId(m_ingress ? m_ingress->clusterSessionId() : -1);
        m_testRequest.putSender(static_cast<const char*>("CLIENT  ")).putTarget(static_cast<const char*>("SEQNCR  "))
                .seqNum(testRequest.sequenceNumber().value_or(0u)).sendingTimeMs(nowMs());
        if (const auto id = testRequest.testReqID())
        {
            const auto sv = *id;
            const std::size_t n = std::min(sv.size(), static_cast<std::size_t>(32));
            std::memcpy(m_testRequest.testReqID(), sv.data(), n);
            if (n < 32)
            {
                m_testRequest.testReqID()[n] = '\0';
            }
        }
        else
        {
            m_testRequest.testReqID()[0] = '\0';
        }
        sendUnsequenced(m_testRequest);
        return fix::Result::Success;
    }

    fix::Result handle(const msg::ResendRequestDecoder& resendRequest)
    {
        if (!verifyCompIds(resendRequest))
        {
            std::fprintf(stderr, "[Ingress] ResendRequest fd=%d rejected: Invalid CompId\n", m_connectionId);
            if (m_session)
            {
                m_session->sendReject(resendRequest.sequenceNumber().value_or(0u),
                                       msg::SessionRejectReason::CompIDProblem, "Invalid CompId");
            }
            return fix::Result::Success;
        }
        m_resendRequest.wrapAndApplyHeader(buffer(), 0, bufferLength());
        m_resendRequest.header().sourceId(m_ingress ? m_ingress->sourceId() : 0).connectionId(m_connectionId).sessionId(m_ingress ? m_ingress->clusterSessionId() : -1);
        m_resendRequest.putSender(static_cast<const char*>("CLIENT  ")).putTarget(static_cast<const char*>("SEQNCR  "))
         .seqNum(resendRequest.sequenceNumber().value_or(0u)).sendingTimeMs(nowMs())
         .beginSeqNo(resendRequest.beginSeqNo().value_or(1u))
         .endSeqNo(resendRequest.endSeqNo().value_or(0u));
        sendUnsequenced(m_resendRequest);
        return fix::Result::Success;
    }

    fix::Result handle(const msg::SequenceResetDecoder& sequenceReset)
    {
        if (!verifyCompIds(sequenceReset))
        {
            std::fprintf(stderr, "[Ingress] SequenceReset fd=%d rejected: Invalid CompId\n", m_connectionId);
            if (m_session)
            {
                m_session->sendReject(sequenceReset.sequenceNumber().value_or(0u),
                                       msg::SessionRejectReason::CompIDProblem, "Invalid CompId");
            }
            return fix::Result::Success;
        }
        m_sequenceReset.wrapAndApplyHeader(buffer(), 0, bufferLength());
        m_sequenceReset.header().sourceId(m_ingress ? m_ingress->sourceId() : 0).connectionId(m_connectionId).sessionId(m_ingress ? m_ingress->clusterSessionId() : -1);
        m_sequenceReset.putSender(static_cast<const char*>("CLIENT  ")).putTarget(static_cast<const char*>("SEQNCR  "))
         .seqNum(sequenceReset.sequenceNumber().value_or(0u)).sendingTimeMs(nowMs())
         .gapFillFlag(usq::GapFillFlag::Value::NULL_VALUE)
         .newSeqNo(sequenceReset.newSeqNo().value_or(1u));
        sendUnsequenced(m_sequenceReset);
        return fix::Result::Success;
    }

    // Sends the gateway's own chunk-boundary SequenceReset to the cluster while
    // serving a large ResendRequest in 1000-message chunks (see
    // FixConnection::handleResendRequest). Unlike every other message this
    // handler forwards, this one does NOT originate from a TCP-received client
    // message — the gateway synthesizes it — so it is stamped with the reserved
    // RESEND_CHUNK_COMPID sender (rather than the "CLIENT" literal) so
    // FixConnection can identify it when it round-trips on the global stream.
    // Both seqNum (tag 34) and newSeqNo (tag 36) are the block-end+1 value: the
    // reset sits at the outbound sequence position immediately after the last
    // message of the just-replayed block (blockEndPlusOne == chunkEnd, the
    // exclusive end of [chunkBegin, chunkEnd)).
    void sendResendChunkReset(const std::uint32_t blockEndPlusOne, const std::int64_t sendingTimeMs)
    {
        if (!m_ingress)
        {
            return;
        }
        m_sequenceReset.wrapAndApplyHeader(buffer(), 0, bufferLength());
        m_sequenceReset.header().sourceId(m_ingress->sourceId()).connectionId(m_connectionId).sessionId(m_ingress->clusterSessionId());
        m_sequenceReset.putSender(RESEND_CHUNK_COMPID).putTarget(static_cast<const char*>("SEQNCR  "))
         .seqNum(blockEndPlusOne).sendingTimeMs(sendingTimeMs)
         .gapFillFlag(usq::GapFillFlag::Value::GapFillMessage)
         .newSeqNo(blockEndPlusOne);
        sendUnsequenced(m_sequenceReset);
    }

    fix::Result handle(const msg::NewOrderSingleDecoder& newOrderSingle)
    {
        if (!verifyCompIds(newOrderSingle))
        {
            std::fprintf(stderr, "[Ingress] NewOrderSingle fd=%d rejected: Invalid CompId\n", m_connectionId);
            if (m_session)
            {
                m_session->sendReject(newOrderSingle.sequenceNumber().value_or(0u),
                                       msg::SessionRejectReason::CompIDProblem, "Invalid CompId");
            }
            return fix::Result::Success;
        }
        const auto clOrdId  = newOrderSingle.clOrdID().value_or(std::string_view{});
        const auto symbol   = newOrderSingle.symbol().value_or(std::string_view{});
        const msg::Side side = newOrderSingle.side().value_or(msg::Side::Buy);
        const auto leavesQuantity      = newOrderSingle.orderQty().value_or(0u);
        const auto ordType  = newOrderSingle.ordType().value_or(msg::OrdType::Market);
        const auto priceOpt = newOrderSingle.price();

        std::printf("[App] NewOrderSingle clOrdID=%.*s symbol=%.*s side=%s qty=%u ordType=%s\n",
                    static_cast<int>(clOrdId.size()), clOrdId.data(),
                    static_cast<int>(symbol.size()), symbol.data(),
                    msg::name(side).data(), leavesQuantity, msg::name(ordType).data());
        // Business validation
        const char* rejectReason = nullptr;
        if (clOrdId.empty())
        {
            rejectReason = "ClOrdID is empty";
        }
        else if (symbol.empty())
        {
            rejectReason = "Symbol is empty";
        }
        else if (leavesQuantity == 0)
        {
            rejectReason = "OrderQty must be > 0";
        }
        else if (ordType == msg::OrdType::Limit && !priceOpt)
        {
            rejectReason = "Price required for Limit order";
        }
        else if (priceOpt && *priceOpt <= fix::utils::FixedDecimal{0})
        {
            rejectReason = "Price must be positive";
        }
        if (rejectReason)
        {
            std::fprintf(stderr, "[App] Rejected clOrdID=%.*s: %s\n", static_cast<int>(clOrdId.size()), clOrdId.data(),
                         rejectReason);
            if (m_session)
            {
                sendExecutionReport("NONE", clOrdId, "EXEC-REJ",
                                     usq::ExecType::Value::Rejected,
                                     usq::OrdStatus::Value::Rejected,
                                     symbol.empty() ? std::string_view{"?"} : symbol,
                                     toSbeSide(side), leavesQuantity, /*price*/ nullptr,
                                     /*leavesQty*/ 0, /*cumQty*/ 0, rejectReason);
            }
            return fix::Result::Success;
        }
        if (m_ingress)
        {
            m_newOrderSingle.wrapAndApplyHeader(buffer(), 0, bufferLength());
            m_newOrderSingle.header().sourceId(m_ingress->sourceId()).connectionId(m_connectionId).sessionId(m_ingress->clusterSessionId());
            m_newOrderSingle.putSender(static_cast<const char*>("CLIENT  ")).putTarget(static_cast<const char*>("SEQNCR  "))
                    .seqNum(newOrderSingle.sequenceNumber().value_or(0u)).sendingTimeMs(nowMs());
            m_newOrderSingle.putAccount(newOrderSingle.account().value_or(std::string_view{}));
            m_newOrderSingle.putClOrdID(clOrdId);
            m_newOrderSingle.handlInst(toSbeHandlInst(newOrderSingle.handlInst().value_or(msg::HandlInst::AutoPrivate)));
            m_newOrderSingle.putSymbol(symbol);
            m_newOrderSingle.side(toSbeSide(side));
            m_newOrderSingle.transactTime(newOrderSingle.transactTime().value_or(std::chrono::milliseconds(nowMs())).count());
            m_newOrderSingle.orderQty(leavesQuantity);
            m_newOrderSingle.ordType(toSbeOrdType(ordType));
            m_newOrderSingle.price(priceOpt ? priceOpt->mantissa() : usq::NewOrderSingle::priceNullValue());
            if (const auto tif = newOrderSingle.timeInForce())
            {
                m_newOrderSingle.timeInForce(toSbeTimeInForce(*tif));
            }
            else
            {
                m_newOrderSingle.timeInForce(usq::TimeInForce::Value::NULL_VALUE);
            }
            m_newOrderSingle.putText(std::string_view{});
            m_newOrderSingle.tradeDate(usq::NewOrderSingle::tradeDateNullValue());
            m_newOrderSingle.maturityTime(usq::NewOrderSingle::maturityTimeNullValue());
            sendUnsequenced(m_newOrderSingle);
        }
        if (m_session)
        {
            sendExecutionReport("ORD-0001", clOrdId, "EXEC-0001", usq::ExecType::Value::New,
                                 usq::OrdStatus::Value::New, symbol, toSbeSide(side), leavesQuantity,
                                 priceOpt ? &*priceOpt : nullptr, leavesQuantity, 0, nullptr);
            std::printf("[App] Sent ExecutionReport (New) for clOrdID=%.*s\n",
                        static_cast<int>(clOrdId.size()), clOrdId.data());
        }
        return fix::Result::Success;
    }

private:

    void sendExecutionReport(const std::string_view orderId,
                             const std::string_view clOrdId,
                             const std::string_view execId,
                             const usq::ExecType::Value execType,
                             const usq::OrdStatus::Value ordStatus,
                             const std::string_view symbol,
                             const usq::Side::Value side,
                             const std::uint32_t orderQty,
                             const fix::utils::FixedDecimal* price,
                             const std::uint32_t leavesQty,
                             const std::uint32_t cumQty,
                             const char* text)
    {
        if (!m_ingress || !m_session)
        {
            return;
        }

        const std::uint32_t seq = m_session->nextOutgoingSeqNum();
        m_session->setNextOutgoingSeqNum(seq + 1);

        m_executionReport.wrapAndApplyHeader(buffer(), 0, bufferLength());
        m_executionReport.header().sourceId(m_ingress->sourceId()).connectionId(m_connectionId).sessionId(m_ingress->clusterSessionId());
        m_executionReport.putSender(static_cast<const char*>("CLIENT  ")).putTarget(static_cast<const char*>("SEQNCR  "))
         .seqNum(seq).sendingTimeMs(nowMs());
        m_executionReport.putOrderID(orderId);
        m_executionReport.putClOrdID(clOrdId);
        m_executionReport.putExecID(execId);
        m_executionReport.execType(execType);
        m_executionReport.ordStatus(ordStatus);
        m_executionReport.putSymbol(symbol);
        m_executionReport.side(side);
        m_executionReport.orderQty(orderQty);
        m_executionReport.price(price ? price->mantissa() : usq::ExecutionReport::priceNullValue());
        m_executionReport.lastQty(usq::ExecutionReport::lastQtyNullValue());
        m_executionReport.lastPx(usq::ExecutionReport::lastPxNullValue());
        m_executionReport.leavesQty(leavesQty);
        m_executionReport.cumQty(cumQty);
        m_executionReport.avgPx(0);
        m_executionReport.transactTime(nowMs());
        m_executionReport.putText(text ? std::string_view(text) : std::string_view{});
        sendUnsequenced(m_executionReport);
    }

};

} // namespace org::limitless::phixeron::sequencer
