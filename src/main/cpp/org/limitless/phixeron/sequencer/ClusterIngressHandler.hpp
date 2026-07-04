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
#include <limits>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <errno.h>
#include <sys/socket.h>
#include <unistd.h>

#include "org/limitless/fix/generated/messages/FixMessageHandler.hpp"
#include "org/limitless/fix/generated/messages/FixMessageDecoders.hpp"
#include "org/limitless/fix/generated/config/FixEngine.hpp"

#include "org/limitless/phixeron/session/ServerSession.hpp"
#include "org/limitless/phixeron/sequencer/ClusterIngressSender.hpp"

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

namespace fix      = org::limitless::fix;
namespace sess     = org::limitless::phixeron::session;
namespace msg      = fix::generated::messages;
namespace cfg      = fix::generated::config;
namespace sbeunseq = org::limitless::phixeron::sbe::unsequenced;

// ── FIX byte-level helpers ────────────────────────────────────────────────────

// Scans a TCP recv buffer for the end of one FIX message (tag 10 + trailing SOH).
// Returns byte count of the complete message, or 0 if not yet complete.
inline std::size_t findFixMessageEnd(std::span<const std::uint8_t> buf)
{
    for (std::size_t i = 0; i + 5 <= buf.size(); ++i) {
        // Tag 10 must be preceded by a field separator (SOH) to avoid matching
        // values that contain the substring "10=".
        if ((i == 0 || buf[i - 1] == '\x01')
            && buf[i] == '1' && buf[i + 1] == '0' && buf[i + 2] == '=')
        {
            for (std::size_t j = i + 3; j < buf.size(); ++j) {
                if (buf[j] == '\x01') return j + 1;
            }
        }
    }
    return 0;
}

// Returns the byte offset of "tag=" preceded by SOH (or at offset 0), else npos.
inline std::size_t fixFindTag(const std::uint8_t* data, std::size_t len, std::string_view tag)
{
    const std::size_t tlen = tag.size();
    if (len < tlen + 1) return std::string::npos;
    for (std::size_t i = 0; i + tlen + 1 <= len; ++i)
    {
        if ((i == 0 || data[i - 1] == '\x01')
            && std::memcmp(data + i, tag.data(), tlen) == 0
            && data[i + tlen] == '=')
            return i;
    }
    return std::string::npos;
}

// Returns {valueStart, valueEnd} where valueEnd points to the trailing SOH.
// Returns {npos, npos} when the tag is absent.
inline std::pair<std::size_t, std::size_t>
fixTagRange(const std::uint8_t* data, std::size_t len, std::string_view tag)
{
    const std::size_t pos = fixFindTag(data, len, tag);
    if (pos == std::string::npos) return {std::string::npos, std::string::npos};
    const std::size_t vs = pos + tag.size() + 1;
    std::size_t ve = vs;
    while (ve < len && data[ve] != '\x01') ++ve;
    return {vs, ve};
}

// Patches a stored outbound FIX message for retransmission:
//   • inserts 43=Y and 122=<SendingTime> after tag 52
//   • updates tag 9 (BodyLength)
//   • recalculates tag 10 (CheckSum)
inline void patchResendFlags(std::vector<std::uint8_t>& msg)
{
    // ── 1. Extract SendingTime value and build the insertion string ────────────
    const auto [t52s, t52e] = fixTagRange(msg.data(), msg.size(), "52");
    if (t52s == std::string::npos) return;

    std::string ins;
    ins.reserve(32);
    ins += "43=Y\x01" "122=";
    ins.append(reinterpret_cast<const char*>(msg.data() + t52s), t52e - t52s);
    ins += '\x01';

    // Insert immediately after the SOH of tag 52
    const std::size_t insPos = t52e + 1;
    msg.insert(msg.begin() + static_cast<std::ptrdiff_t>(insPos),
               reinterpret_cast<const std::uint8_t*>(ins.data()),
               reinterpret_cast<const std::uint8_t*>(ins.data() + ins.size()));

    // ── 2. Update BodyLength (tag 9) ─────────────────────────────────────────
    {
        const auto [t9s, t9e] = fixTagRange(msg.data(), msg.size(), "9");
        if (t9s == std::string::npos) return;

        std::uint32_t bodyLen = 0;
        for (std::size_t i = t9s; i < t9e; ++i) bodyLen = bodyLen * 10 + (msg[i] - '0');
        const std::uint32_t newBodyLen = bodyLen + static_cast<std::uint32_t>(ins.size());

        const std::size_t oldDigits = t9e - t9s;
        char newVal[12];
        const int written = std::snprintf(newVal, sizeof(newVal),
                                          "%0*u", static_cast<int>(oldDigits), newBodyLen);
        msg.erase(msg.begin() + static_cast<std::ptrdiff_t>(t9s),
                  msg.begin() + static_cast<std::ptrdiff_t>(t9e));
        msg.insert(msg.begin() + static_cast<std::ptrdiff_t>(t9s),
                   reinterpret_cast<const std::uint8_t*>(newVal),
                   reinterpret_cast<const std::uint8_t*>(newVal + written));
    }

    // ── 3. Recalculate CheckSum (tag 10) ─────────────────────────────────────
    {
        const std::size_t t10pos = fixFindTag(msg.data(), msg.size(), "10");
        if (t10pos == std::string::npos) return;

        std::uint32_t sum = 0;
        for (std::size_t i = 0; i < t10pos; ++i) sum += msg[i];
        sum %= 256;

        const auto [t10s, t10e] = fixTagRange(msg.data(), msg.size(), "10");
        if (t10s == std::string::npos) return;

        char chk[4];
        std::snprintf(chk, sizeof(chk), "%03u", sum);
        // FIX standard: CheckSum is always exactly 3 digits
        if (t10e - t10s == 3)
            std::memcpy(msg.data() + t10s, chk, 3);
    }
}

// ── sendRaw ───────────────────────────────────────────────────────────────────

// Blocking TCP send of a complete byte range; shared by CapturingTransport,
// FixConnection::handleResendRequest and FixConnection's ExecutionReport delivery.
inline void sendRaw(int fd, const std::uint8_t* data, std::size_t len)
{
    if (fd < 0 || len == 0) return;
    std::size_t sent = 0;
    while (sent < len)
    {
        const ssize_t n = ::send(fd, data + sent, len - sent, MSG_NOSIGNAL);
        if (n < 0)
        {
            if (errno == EINTR) continue;
            std::fprintf(stderr, "[TcpTransport] send fd=%d failed: %s\n",
                         fd, std::strerror(errno));
            return;
        }
        sent += static_cast<std::size_t>(n);
    }
}

// ── CapturingTransport ────────────────────────────────────────────────────────

// The FixSession's outbound transport: every message the session encodes
// (cluster-driven admin replies, SequenceReset gap-fills, and — on the
// FixSessionClient egress side — FIX-text ExecutionReports re-encoded from
// the cluster's sbe-unsequenced.xml payload) goes straight to TCP. The
// session is never used to FIX-encode-and-forward bytes to the cluster
// anymore: ClusterIngressHandler encodes and submits application messages
// as sbe-unsequenced.xml directly (see handle(NewOrderSingleDecoder&) and
// sendExecutionReport below), reserving ExecutionReport's MsgSeqNum from the
// same session (see sendExecutionReport's comment) without touching its
// transport.
struct CapturingTransport
{
    int fd{-1};

    void operator()(std::span<const std::uint8_t> bytes) const
    {
        sendRaw(fd, bytes.data(), bytes.size());
    }
};

using FixSession = sess::ServerSession<cfg::FIXT_1_1, "SEQUENCER", "CLIENT", CapturingTransport>;

// ── Enum mapping: simdfix FIX enums → sbe-unsequenced.xml enums ──────────────
//
// Both enum sets mirror the same FIX values by construction (see
// fix-application.xml / sbe-unsequenced.xml), so these are straight 1:1 maps.

inline sbeunseq::Side::Value toSbeSide(msg::Side v)
{
    switch (v) {
        case msg::Side::Buy:       return sbeunseq::Side::Value::Buy;
        case msg::Side::Sell:      return sbeunseq::Side::Value::Sell;
        case msg::Side::BuyMinus:  return sbeunseq::Side::Value::BuyMinus;
        case msg::Side::SellPlus:  return sbeunseq::Side::Value::SellPlus;
        case msg::Side::SellShort: return sbeunseq::Side::Value::SellShort;
        case msg::Side::Null:      break;
    }
    return sbeunseq::Side::Value::Buy;
}

inline sbeunseq::OrdType::Value toSbeOrdType(msg::OrdType v)
{
    switch (v) {
        case msg::OrdType::Market:    return sbeunseq::OrdType::Value::Market;
        case msg::OrdType::Limit:     return sbeunseq::OrdType::Value::Limit;
        case msg::OrdType::Stop:      return sbeunseq::OrdType::Value::Stop;
        case msg::OrdType::StopLimit: return sbeunseq::OrdType::Value::StopLimit;
        case msg::OrdType::Null:      break;
    }
    return sbeunseq::OrdType::Value::Market;
}

inline sbeunseq::HandlInst::Value toSbeHandlInst(msg::HandlInst v)
{
    switch (v) {
        case msg::HandlInst::AutoPrivate: return sbeunseq::HandlInst::Value::AutoPrivate;
        case msg::HandlInst::AutoPublic:  return sbeunseq::HandlInst::Value::AutoPublic;
        case msg::HandlInst::Manual:      return sbeunseq::HandlInst::Value::Manual;
        case msg::HandlInst::Null:        break;
    }
    return sbeunseq::HandlInst::Value::AutoPrivate;
}

inline sbeunseq::TimeInForce::Value toSbeTimeInForce(msg::TimeInForce v)
{
    switch (v) {
        case msg::TimeInForce::Day:              return sbeunseq::TimeInForce::Value::Day;
        case msg::TimeInForce::GoodTillCancel:    return sbeunseq::TimeInForce::Value::GoodTillCancel;
        case msg::TimeInForce::AtTheOpening:      return sbeunseq::TimeInForce::Value::AtTheOpening;
        case msg::TimeInForce::ImmediateOrCancel: return sbeunseq::TimeInForce::Value::ImmediateOrCancel;
        case msg::TimeInForce::FillOrKill:        return sbeunseq::TimeInForce::Value::FillOrKill;
        case msg::TimeInForce::Null:              break;
    }
    return sbeunseq::TimeInForce::Value::Day;
}

// ── ClusterIngressHandler ─────────────────────────────────────────────────────

// Receives decoded FIX messages on the TCP receive path and forwards them to
// the cluster WITHOUT updating any session state. Every message — admin
// (Logon, Heartbeat, …) and application (NewOrderSingle, ExecutionReport) —
// is re-encoded as the matching sbe-unsequenced.xml message, with
// header.sourceId/sessionId identifying the submitting connection.
class ClusterIngressHandler : public msg::FixMessageHandler<ClusterIngressHandler>
{
    ClusterIngressSender*         m_ingress{};
    std::int32_t                  m_connectionId{-1};
    FixSession*                   m_session{nullptr};
    std::span<const std::uint8_t> m_rawFixBytes;

    alignas(16) std::array<std::uint8_t, 512> m_sbeBuf{};

    char*    sbeBufBody() { return reinterpret_cast<char*>(m_sbeBuf.data()); }
    uint64_t sbeBufLen()  { return m_sbeBuf.size(); }

    template <typename SbeMsg>
    void sendUnsequenced(SbeMsg& msg)
    {
        if (!m_ingress) return;
        m_ingress->send(m_sbeBuf.data(), static_cast<std::uint16_t>(msg.sbePosition()));
    }

public:
    using FixMessageHandler::handle;

    ClusterIngressHandler() = default;
    ClusterIngressHandler(ClusterIngressSender* ingress, int32_t connId, FixSession* session = nullptr)
        : m_ingress(ingress), m_connectionId(connId), m_session(session) {}

    void setRawBytes(std::span<const std::uint8_t> bytes) { m_rawFixBytes = bytes; }

    fix::Result handle(msg::LogonDecoder& logon)
    {
        const std::uint32_t hbSecs = logon.heartbeatInterval().value_or(30u);
        std::printf("[Ingress] Logon from fd=%d hbSecs=%u → encoding sbe-unsequenced\n",
                    m_connectionId, hbSecs);
        sbeunseq::Logon m;
        m.wrapAndApplyHeader(sbeBufBody(), 0, sbeBufLen());
        m.header().sourceId(m_connectionId).sessionId(m_ingress ? m_ingress->clusterSessionId() : -1);
        m.putSender(static_cast<const char*>("CLIENT  ")).putTarget(static_cast<const char*>("SEQNCR  "))
         .seqNum(0).sendingTimeMs(nowMs())
         .encryptMethod(sbeunseq::EncryptMethod::Value::None)
         .heartbeatInterval(hbSecs)
         .putXmlData(nullptr, 0);
        sendUnsequenced(m);
        std::printf("[Ingress] Logon sent to cluster (fd=%d sbePos=%llu)\n",
                    m_connectionId, static_cast<unsigned long long>(m.sbePosition()));
        return fix::Result::Success;
    }

    fix::Result handle(msg::LogoutDecoder& /*logout*/)
    {
        sbeunseq::Logout m;
        m.wrapAndApplyHeader(sbeBufBody(), 0, sbeBufLen());
        m.header().sourceId(m_connectionId).sessionId(m_ingress ? m_ingress->clusterSessionId() : -1);
        m.putSender(static_cast<const char*>("CLIENT  ")).putTarget(static_cast<const char*>("SEQNCR  "))
         .seqNum(0).sendingTimeMs(nowMs())
         .putText(nullptr, 0);
        sendUnsequenced(m);
        return fix::Result::Success;
    }

    fix::Result handle(msg::HeartbeatDecoder& heartbeat)
    {
        sbeunseq::Heartbeat m;
        m.wrapAndApplyHeader(sbeBufBody(), 0, sbeBufLen());
        m.header().sourceId(m_connectionId).sessionId(m_ingress ? m_ingress->clusterSessionId() : -1);
        m.putSender(static_cast<const char*>("CLIENT  ")).putTarget(static_cast<const char*>("SEQNCR  "))
         .seqNum(0).sendingTimeMs(nowMs());
        if (const auto id = heartbeat.testReqID()) {
            const auto sv = *id;
            const std::size_t n = std::min(sv.size(), static_cast<std::size_t>(32));
            std::memcpy(m.testReqID(), sv.data(), n);
            if (n < 32) m.testReqID()[n] = '\0';
        } else {
            m.testReqID()[0] = '\0';
        }
        sendUnsequenced(m);
        return fix::Result::Success;
    }

    fix::Result handle(msg::TestRequestDecoder& testRequest)
    {
        sbeunseq::TestRequest m;
        m.wrapAndApplyHeader(sbeBufBody(), 0, sbeBufLen());
        m.header().sourceId(m_connectionId).sessionId(m_ingress ? m_ingress->clusterSessionId() : -1);
        m.putSender(static_cast<const char*>("CLIENT  ")).putTarget(static_cast<const char*>("SEQNCR  "))
         .seqNum(0).sendingTimeMs(nowMs());
        if (const auto id = testRequest.testReqID()) {
            const auto sv = *id;
            const std::size_t n = std::min(sv.size(), static_cast<std::size_t>(32));
            std::memcpy(m.testReqID(), sv.data(), n);
            if (n < 32) m.testReqID()[n] = '\0';
        } else {
            m.testReqID()[0] = '\0';
        }
        sendUnsequenced(m);
        return fix::Result::Success;
    }

    fix::Result handle(msg::ResendRequestDecoder& rr)
    {
        sbeunseq::ResendRequest m;
        m.wrapAndApplyHeader(sbeBufBody(), 0, sbeBufLen());
        m.header().sourceId(m_connectionId).sessionId(m_ingress ? m_ingress->clusterSessionId() : -1);
        m.putSender(static_cast<const char*>("CLIENT  ")).putTarget(static_cast<const char*>("SEQNCR  "))
         .seqNum(0).sendingTimeMs(nowMs())
         .beginSeqNo(rr.beginSeqNo().value_or(1u))
         .endSeqNo(rr.endSeqNo().value_or(0u));
        sendUnsequenced(m);
        return fix::Result::Success;
    }

    fix::Result handle(msg::SequenceResetDecoder& sr)
    {
        sbeunseq::SequenceReset m;
        m.wrapAndApplyHeader(sbeBufBody(), 0, sbeBufLen());
        m.header().sourceId(m_connectionId).sessionId(m_ingress ? m_ingress->clusterSessionId() : -1);
        m.putSender(static_cast<const char*>("CLIENT  ")).putTarget(static_cast<const char*>("SEQNCR  "))
         .seqNum(0).sendingTimeMs(nowMs())
         .gapFillFlag(sbeunseq::GapFillFlag::Value::NULL_VALUE)
         .newSeqNo(sr.newSeqNo().value_or(1u));
        sendUnsequenced(m);
        return fix::Result::Success;
    }

    fix::Result handle(msg::NewOrderSingleDecoder& nos)
    {
        const auto clOrdId  = nos.clOrdID().value_or(std::string_view{});
        const auto symbol   = nos.symbol().value_or(std::string_view{});
        const msg::Side side     = nos.side().value_or(msg::Side::Buy);
        const auto qty      = nos.orderQty().value_or(0u);
        const auto ordType  = nos.ordType().value_or(msg::OrdType::Market);
        const auto priceOpt = nos.price();

        std::printf("[App] NewOrderSingle clOrdID=%.*s symbol=%.*s side=%s qty=%u ordType=%s\n",
                    static_cast<int>(clOrdId.size()), clOrdId.data(),
                    static_cast<int>(symbol.size()), symbol.data(),
                    msg::name(side).data(), qty, msg::name(ordType).data());

        // Business validation
        const char* rejectReason = nullptr;
        if (clOrdId.empty())
            rejectReason = "ClOrdID is empty";
        else if (symbol.empty())
            rejectReason = "Symbol is empty";
        else if (qty == 0)
            rejectReason = "OrderQty must be > 0";
        else if (ordType == msg::OrdType::Limit && !priceOpt)
            rejectReason = "Price required for Limit order";
        else if (priceOpt && *priceOpt <= fix::utils::FixedDecimal{0})
            rejectReason = "Price must be positive";

        if (rejectReason) {
            std::fprintf(stderr, "[App] Rejected clOrdID=%.*s: %s\n",
                         static_cast<int>(clOrdId.size()), clOrdId.data(), rejectReason);
            if (m_session) {
                sendExecutionReport("NONE", clOrdId, "EXEC-REJ",
                                     sbeunseq::ExecType::Value::Rejected,
                                     sbeunseq::OrdStatus::Value::Rejected,
                                     symbol.empty() ? std::string_view{"?"} : symbol,
                                     toSbeSide(side), qty, /*price*/ nullptr,
                                     /*leavesQty*/ 0, /*cumQty*/ 0, rejectReason);
            }
            return fix::Result::Success;
        }

        if (m_ingress) {
            sbeunseq::NewOrderSingle m;
            m.wrapAndApplyHeader(sbeBufBody(), 0, sbeBufLen());
            m.header().sourceId(m_connectionId).sessionId(m_ingress->clusterSessionId());
            m.putSender(static_cast<const char*>("CLIENT  ")).putTarget(static_cast<const char*>("SEQNCR  "))
             .seqNum(0).sendingTimeMs(nowMs());
            m.putAccount(nos.account().value_or(std::string_view{}));
            m.putClOrdID(clOrdId);
            m.handlInst(toSbeHandlInst(nos.handlInst().value_or(msg::HandlInst::AutoPrivate)));
            m.putSymbol(symbol);
            m.side(toSbeSide(side));
            m.transactTime(nos.transactTime().value_or(std::chrono::milliseconds(nowMs())).count());
            m.orderQty(qty);
            m.ordType(toSbeOrdType(ordType));
            m.price(priceOpt ? priceOpt->mantissa() : sbeunseq::NewOrderSingle::priceNullValue());
            if (const auto tif = nos.timeInForce())
                m.timeInForce(toSbeTimeInForce(*tif));
            else
                m.timeInForce(sbeunseq::TimeInForce::Value::NULL_VALUE);
            m.putText(std::string_view{});
            m.tradeDate(sbeunseq::NewOrderSingle::tradeDateNullValue());
            m.maturityTime(sbeunseq::NewOrderSingle::maturityTimeNullValue());
            sendUnsequenced(m);
        }

        if (m_session) {
            sendExecutionReport("ORD-0001", clOrdId, "EXEC-0001",
                                 sbeunseq::ExecType::Value::New,
                                 sbeunseq::OrdStatus::Value::New,
                                 symbol, toSbeSide(side), qty,
                                 priceOpt ? &*priceOpt : nullptr,
                                 /*leavesQty*/ qty, /*cumQty*/ 0, /*text*/ nullptr);
            std::printf("[App] Sent ExecutionReport (New) for clOrdID=%.*s\n",
                        static_cast<int>(clOrdId.size()), clOrdId.data());
        }
        return fix::Result::Success;
    }

private:
    // Builds and submits one sbe-unsequenced.xml ExecutionReport to the
    // cluster. Used for both the reject path and the accepted-order path in
    // handle(NewOrderSingleDecoder&) above.
    void sendExecutionReport(std::string_view orderId, std::string_view clOrdId,
                             std::string_view execId,
                             sbeunseq::ExecType::Value execType,
                             sbeunseq::OrdStatus::Value ordStatus,
                             std::string_view symbol, sbeunseq::Side::Value side,
                             std::uint32_t orderQty, const fix::utils::FixedDecimal* price,
                             std::uint32_t leavesQty, std::uint32_t cumQty,
                             const char* text)
    {
        if (!m_ingress || !m_session) return;

        // ExecutionReport is the gateway's own outgoing FIX message (unlike
        // admin messages here, whose real reply is built later on the egress
        // side by FixConnection::onClusterAdmin). Its MsgSeqNum is therefore
        // reserved synchronously, now, from the shared session counter — the
        // same counter FixConnection's cluster-driven admin replies advance
        // via session.send() — so the two interleave into one gapless FIX
        // sequence. It travels through the cluster (and archive) embedded in
        // seqNum below; FixConnection re-derives the FIX-wire MsgSeqNum from
        // this field rather than reassigning one on echo.
        const std::uint32_t seq = m_session->nextOutgoingSeqNum();
        m_session->setNextOutgoingSeqNum(seq + 1);

        sbeunseq::ExecutionReport m;
        m.wrapAndApplyHeader(sbeBufBody(), 0, sbeBufLen());
        m.header().sourceId(m_connectionId).sessionId(m_ingress->clusterSessionId());
        m.putSender(static_cast<const char*>("CLIENT  ")).putTarget(static_cast<const char*>("SEQNCR  "))
         .seqNum(seq).sendingTimeMs(nowMs());
        m.putOrderID(orderId);
        m.putClOrdID(clOrdId);
        m.putExecID(execId);
        m.execType(execType);
        m.ordStatus(ordStatus);
        m.putSymbol(symbol);
        m.side(side);
        m.orderQty(orderQty);
        m.price(price ? price->mantissa() : sbeunseq::ExecutionReport::priceNullValue());
        m.lastQty(sbeunseq::ExecutionReport::lastQtyNullValue());
        m.lastPx(sbeunseq::ExecutionReport::lastPxNullValue());
        m.leavesQty(leavesQty);
        m.cumQty(cumQty);
        m.avgPx(0);
        m.transactTime(nowMs());
        m.putText(text ? std::string_view(text) : std::string_view{});
        sendUnsequenced(m);
    }
};

} // namespace org::limitless::phixeron::sequencer
