#pragma once

// ClusterIngressHandler — FIX-session-facing bridge into the Aeron Cluster.
//
// Everything here is pure logic (byte-level FIX parsing helpers, SBE admin
// encoding, application-message routing) built on top of ClusterIngressSender's
// IngressTransport/EgressTransport seam. Nothing in this file touches a raw
// TCP socket fd for anything observable in a test: CapturingTransport's admin
// branch calls sendRaw(fd, ...), but that is a fire-and-forget guarded no-op
// when fd < 0, and ClusterIngressHandler's admin/app encoders never inspect
// their own output — they only push bytes through ClusterIngressSender, which
// tests drive with the same in-memory IngressTransport/EgressTransport fakes
// as ClusterIngressSenderTest.cpp (see ClusterIngressHandlerTest.cpp).

#include <array>
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

#include "org/limitless/phixeron/session/ServerSession.hpp"
#include "org/limitless/fix/generated/messages/FixMessageHandler.hpp"
#include "org/limitless/fix/generated/messages/FixMessageDecoders.hpp"
#include "org/limitless/fix/generated/messages/FixMessageEncoders.hpp"
#include "org/limitless/fix/generated/config/FixEngine.hpp"

#include "org/limitless/phixeron/sequencer/ClusterIngressSender.hpp"

#include "org_limitless_phixeron_sbe/MessageHeader.h"
#include "org_limitless_phixeron_sbe/Logon.h"
#include "org_limitless_phixeron_sbe/Logout.h"
#include "org_limitless_phixeron_sbe/Heartbeat.h"
#include "org_limitless_phixeron_sbe/TestRequest.h"
#include "org_limitless_phixeron_sbe/ResendRequest.h"
#include "org_limitless_phixeron_sbe/SequenceReset.h"

namespace org::limitless::phixeron::sequencer
{

namespace fix     = org::limitless::fix;
namespace sess    = org::limitless::phixeron::session;
namespace msg     = fix::generated::messages;
namespace cfg     = fix::generated::config;
namespace sbesess = org::limitless::phixeron::sbe;

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

// Returns true when tag 35 carries a session-layer MsgType.
inline bool isAdminMsgType(const std::uint8_t* data, std::size_t len) noexcept
{
    const auto [vs, ve] = fixTagRange(data, len, "35");
    if (vs == std::string::npos || vs == ve) return true;
    const char t = static_cast<char>(data[vs]);
    return t == '0' || t == '1' || t == '2' || t == '3'
        || t == '4' || t == '5' || t == 'A';
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
// FixConnection::handleResendRequest and FixConnection::onClusterAppMessage.
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

// The session's outbound transport policy. Admin (session-layer) messages are
// still delivered straight to TCP: their timing/state transitions are already
// driven exclusively by the cluster global stream (see FixConnection::
// onClusterAdmin), so there is nothing to gain by round-tripping them again.
//
// Application messages (ExecutionReport) are instead handed to the cluster for
// sequencing; per the file's deterministic design invariant, they only reach
// the TCP client once echoed back on the global stream, at which point
// FixConnection::onClusterAppMessage delivers them and populates the resend
// cache. This is what makes the cache genuinely "populated from the cluster
// global stream" rather than from a local side-channel capture.
struct CapturingTransport
{
    int                   fd{-1};
    ClusterIngressSender* ingress{nullptr};
    int32_t               connId{-1};

    void operator()(std::span<const std::uint8_t> bytes) const
    {
        if (bytes.empty()) return;

        if (isAdminMsgType(bytes.data(), bytes.size()))
        {
            sendRaw(fd, bytes.data(), bytes.size());
            return;
        }

        if (ingress && bytes.size() <= std::numeric_limits<std::uint16_t>::max())
        {
            ingress->send(connId, bytes.data(), static_cast<std::uint16_t>(bytes.size()));
        }
    }
};

using FixSession = sess::ServerSession<cfg::FIXT_1_1, "SEQUENCER", "CLIENT",
                                       sess::NullStorage, CapturingTransport>;

// ── ClusterIngressHandler ─────────────────────────────────────────────────────

// Receives decoded FIX messages on the TCP receive path and forwards them to
// the cluster WITHOUT updating any session state. Admin messages are encoded
// as SBE (sbe-session.xml, schemaId=100) prefixed with a 4-byte connection ID
// for routing replies. Application messages (NewOrderSingle) are forwarded as
// raw FIX bytes wrapped in the AppMessage SBE envelope.
class ClusterIngressHandler : public msg::FixMessageHandler<ClusterIngressHandler>
{
    ClusterIngressSender*    m_ingress{};
    int32_t                  m_connectionId{-1};
    std::span<const std::uint8_t> m_rawFixBytes;
    FixSession*              m_session{nullptr};

    // SBE encode buffer: [8-byte SBE header][SBE body]. The connId prefix is
    // added uniformly by ClusterIngressSender::send/encodeAppMessage.
    alignas(16) std::array<std::uint8_t, 512> m_sbeBuf{};

    char*    sbeBufBody()   { return reinterpret_cast<char*>(m_sbeBuf.data()); }
    uint64_t sbeBufLen()    { return m_sbeBuf.size(); }

    template <typename SbeMsg>
    void sendAdmin(SbeMsg& msg)
    {
        if (!m_ingress) return;
        m_ingress->send(m_connectionId, m_sbeBuf.data(), static_cast<std::uint16_t>(msg.sbePosition()));
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
        std::printf("[Ingress] Logon from fd=%d hbSecs=%u → encoding SBE\n",
                    m_connectionId, hbSecs);
        sbesess::Logon m;
        m.wrapAndApplyHeader(sbeBufBody(), 0, sbeBufLen());
        m.putSender(static_cast<const char*>("CLIENT  ")).putTarget(static_cast<const char*>("SEQNCR  "))
         .seqNum(0).sendingTimeMs(nowMs())
         .encryptMethod(sbesess::EncryptMethod::Value::None)
         .heartbeatInterval(hbSecs)
         .putXmlData(nullptr, 0);
        sendAdmin(m);
        std::printf("[Ingress] Logon sent to cluster (fd=%d sbePos=%llu)\n",
                    m_connectionId, static_cast<unsigned long long>(m.sbePosition()));
        return fix::Result::Success;
    }

    fix::Result handle(msg::LogoutDecoder& /*logout*/)
    {
        sbesess::Logout m;
        m.wrapAndApplyHeader(sbeBufBody(), 0, sbeBufLen());
        m.putSender(static_cast<const char*>("CLIENT  ")).putTarget(static_cast<const char*>("SEQNCR  "))
         .seqNum(0).sendingTimeMs(nowMs())
         .putText(nullptr, 0);
        sendAdmin(m);
        return fix::Result::Success;
    }

    fix::Result handle(msg::HeartbeatDecoder& heartbeat)
    {
        sbesess::Heartbeat m;
        m.wrapAndApplyHeader(sbeBufBody(), 0, sbeBufLen());
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
        sendAdmin(m);
        return fix::Result::Success;
    }

    fix::Result handle(msg::TestRequestDecoder& testRequest)
    {
        sbesess::TestRequest m;
        m.wrapAndApplyHeader(sbeBufBody(), 0, sbeBufLen());
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
        sendAdmin(m);
        return fix::Result::Success;
    }

    fix::Result handle(msg::ResendRequestDecoder& rr)
    {
        sbesess::ResendRequest m;
        m.wrapAndApplyHeader(sbeBufBody(), 0, sbeBufLen());
        m.putSender(static_cast<const char*>("CLIENT  ")).putTarget(static_cast<const char*>("SEQNCR  "))
         .seqNum(0).sendingTimeMs(nowMs())
         .beginSeqNo(rr.beginSeqNo().value_or(1u))
         .endSeqNo(rr.endSeqNo().value_or(0u));
        sendAdmin(m);
        return fix::Result::Success;
    }

    fix::Result handle(msg::SequenceResetDecoder& sr)
    {
        sbesess::SequenceReset m;
        m.wrapAndApplyHeader(sbeBufBody(), 0, sbeBufLen());
        m.putSender(static_cast<const char*>("CLIENT  ")).putTarget(static_cast<const char*>("SEQNCR  "))
         .seqNum(0).sendingTimeMs(nowMs())
         .gapFillFlag(sbesess::GapFillFlag::Value::NULL_VALUE)
         .newSeqNo(sr.newSeqNo().value_or(1u));
        sendAdmin(m);
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
                m_session->setNowMs(nowMs());
                msg::ExecutionReportEncoder er;
                m_session->wrapHeader(er);
                er.orderID("NONE")
                  .clOrdID(clOrdId)
                  .execID("EXEC-REJ")
                  .execType(msg::ExecType::Rejected)
                  .ordStatus(msg::OrdStatus::Rejected)
                  .symbol(symbol.empty() ? std::string_view{"?"} : symbol)
                  .side(side)
                  .orderQty(qty)
                  .leavesQty(0)
                  .cumQty(0)
                  .avgPx(fix::utils::FixedDecimal{0})
                  .transactTime(std::chrono::milliseconds(nowMs()))
                  .text(rejectReason);
                m_session->send(er);
            }
            return fix::Result::Success;
        }

        if (m_ingress && !m_rawFixBytes.empty()) {
            const std::size_t sz = m_rawFixBytes.size();
            if (sz > 4096) {
                std::fprintf(stderr, "[App] FIX message too large (%zu bytes); dropped\n", sz);
            } else {
                m_ingress->send(m_connectionId, m_rawFixBytes.data(), static_cast<std::uint16_t>(sz));
            }
        }

        if (m_session) {
            m_session->setNowMs(nowMs());
            msg::ExecutionReportEncoder er;
            m_session->wrapHeader(er);
            er.orderID("ORD-0001")
              .clOrdID(clOrdId)
              .execID("EXEC-0001")
              .execType(msg::ExecType::New)
              .ordStatus(msg::OrdStatus::New)
              .symbol(symbol)
              .side(side)
              .orderQty(qty)
              .leavesQty(qty)
              .cumQty(0)
              .avgPx(fix::utils::FixedDecimal{0});
            if (priceOpt) er.price(*priceOpt);
            er.transactTime(std::chrono::milliseconds(nowMs()));
            m_session->send(er);
            std::printf("[App] Sent ExecutionReport (New) for clOrdID=%.*s\n",
                        static_cast<int>(clOrdId.size()), clOrdId.data());
        }
        return fix::Result::Success;
    }
};

} // namespace org::limitless::phixeron::sequencer
