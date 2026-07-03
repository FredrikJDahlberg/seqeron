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
 *   3. Admin messages (Logon, Heartbeat, …) are re-encoded as SBE
 *      (sbe-session.xml schemaId=100) with a 4-byte connection ID prefix and
 *      sent to the cluster ingress. Session state is NOT touched here.
 *   4. Application messages (NewOrderSingle) are wrapped in an AppMessage SBE
 *      envelope (schemaId=201) and offered to the cluster ingress.
 *
 * Outbound path (Global stream → TCP):
 *   On startup the client replays the archive (NULL_POSITION length → live
 *   follow-through on the same image). For each SequencedMessage:
 *   - Admin SBE payloads (schemaId==100): decoded, and the corresponding
 *     session (looked up by embedded connection ID) is driven with the cluster
 *     consensus timestamp. The session FSM then writes the FIX response to TCP.
 *   - Raw FIX payloads (byte[0]=='8'): decoded as application messages
 *     (ExecutionReport etc.) and broadcast to TCP clients.
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
#include <map>
#include <memory>
#include <span>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
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
#include "org/limitless/fix/decoder/PayloadDecoder.hpp"

// Generated FIX types (application.xml via GenerateAppMessages)
#include "org/limitless/fix/generated/messages/FixMessageHandler.hpp"
#include "org/limitless/fix/generated/messages/FixMessageDecoders.hpp"
#include "org/limitless/fix/generated/messages/FixMessageEncoders.hpp"
#include "org/limitless/fix/generated/config/FixEngine.hpp"

// Cluster wire protocol
#include "org/limitless/phixeron/cluster/ClusterProtocol.hpp"

// Global stream subscription + event types
#include "org/limitless/phixeron/sequencer/GlobalStreamClient.hpp"

// SBE codecs for session admin messages (sbe-session.xml schemaId=100)
#include "org_limitless_phixeron_sbe/MessageHeader.h"
#include "org_limitless_phixeron_sbe/Logon.h"
#include "org_limitless_phixeron_sbe/Logout.h"
#include "org_limitless_phixeron_sbe/Heartbeat.h"
#include "org_limitless_phixeron_sbe/TestRequest.h"
#include "org_limitless_phixeron_sbe/ResendRequest.h"
#include "org_limitless_phixeron_sbe/SequenceReset.h"

// ── Namespace aliases ──────────────────────────────────────────────────────────

namespace fix     = org::limitless::fix;
namespace sess    = org::limitless::phixeron::session;
namespace msg     = fix::generated::messages;
namespace cfg     = fix::generated::config;
namespace cl      = org::limitless::phixeron::cluster;
namespace seq     = org::limitless::phixeron::sequencer;
namespace sbesess = org::limitless::phixeron::sbe;

using namespace aeron;
using namespace aeron::concurrent;
using namespace fix::generated::config;   // FIXT_1_1, MaxMessageSize, …
using namespace fix::generated::messages; // FixMessageHandler, LogonDecoder, …

// ── Constants ─────────────────────────────────────────────────────────────────

static constexpr uint16_t    FIX_TCP_PORT              = 9000;
static constexpr int         FIX_TCP_BACKLOG            = 8;
static constexpr const char* ARCHIVE_CONTROL_CHANNEL   = "aeron:udp?endpoint=localhost:9301";
static constexpr int32_t     ARCHIVE_CONTROL_STREAM    = 100;
static constexpr const char* ARCHIVE_RESPONSE_CHANNEL  = "aeron:udp?endpoint=localhost:0";
static constexpr const char* CLUSTER_INGRESS_CHANNEL   = "aeron:udp?endpoint=localhost:9302";
static constexpr const char* CLUSTER_EGRESS_CHANNEL    = "aeron:udp?endpoint=localhost:9320";
static constexpr const char* CLIENT_INFO               = "FixSessionClient";
static constexpr int64_t     CLUSTER_CONNECT_TIMEOUT_MS = 10'000;
static constexpr const char* FIX_REPLAY_CHANNEL        = "aeron:udp?endpoint=localhost:9310";

// AppMessage SBE constants (sequencer.xml schemaId=201, templateId=1, blockLength=0)
static constexpr uint16_t APP_MSG_TEMPLATE_ID = 1;
static constexpr uint16_t APP_MSG_SCHEMA_ID   = 201;

// Session admin SBE schema (sbe-session.xml schemaId=100).
// Admin AppMessage payloads are prefixed with a 4-byte connection ID so the
// global-stream receiver can route responses to the correct TCP socket.
static constexpr uint16_t SESSION_SCHEMA_ID = 100;
static constexpr uint32_t CONN_ID_PREFIX    = 4;   // bytes before SBE header in admin payload

// ── Low-level helpers ─────────────────────────────────────────────────────────

static int64_t nowMs()
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

static void putU16LE(uint8_t* buf, uint16_t v)
{
    buf[0] = static_cast<uint8_t>(v & 0xFFu);
    buf[1] = static_cast<uint8_t>(v >> 8u);
}

// Encodes one AppMessage SBE frame: 8-byte SBE header + 2-byte varData length + payload.
// Returns total bytes written (10 + fixLen).
static int32_t encodeAppMessage(uint8_t* buf, const uint8_t* fixBytes, uint16_t fixLen)
{
    putU16LE(buf + 0, 0);                   // blockLength  = 0
    putU16LE(buf + 2, APP_MSG_TEMPLATE_ID); // templateId   = 1
    putU16LE(buf + 4, APP_MSG_SCHEMA_ID);   // schemaId     = 201
    putU16LE(buf + 6, 0);                   // version      = 0
    putU16LE(buf + 8, fixLen);              // payload length (uint16 per varDataEncoding)
    std::memcpy(buf + 10, fixBytes, fixLen);
    return 10 + static_cast<int32_t>(fixLen);
}

// Scans a TCP recv buffer for the end of one FIX message (tag 10 + trailing SOH).
// Returns byte count of the complete message, or 0 if not yet complete.
static std::size_t findFixMessageEnd(std::span<const uint8_t> buf)
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

// ── FIX byte-level helpers ────────────────────────────────────────────────────

// Returns the byte offset of "tag=" preceded by SOH (or at offset 0), else npos.
static std::size_t fixFindTag(const uint8_t* data, std::size_t len, std::string_view tag)
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
static std::pair<std::size_t, std::size_t>
fixTagRange(const uint8_t* data, std::size_t len, std::string_view tag)
{
    const std::size_t pos = fixFindTag(data, len, tag);
    if (pos == std::string::npos) return {std::string::npos, std::string::npos};
    const std::size_t vs = pos + tag.size() + 1;
    std::size_t ve = vs;
    while (ve < len && data[ve] != '\x01') ++ve;
    return {vs, ve};
}

// Returns true when tag 35 carries a session-layer MsgType.
static bool isAdminMsgType(const uint8_t* data, std::size_t len) noexcept
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
static void patchResendFlags(std::vector<uint8_t>& msg)
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
               reinterpret_cast<const uint8_t*>(ins.data()),
               reinterpret_cast<const uint8_t*>(ins.data() + ins.size()));

    // ── 2. Update BodyLength (tag 9) ─────────────────────────────────────────
    {
        const auto [t9s, t9e] = fixTagRange(msg.data(), msg.size(), "9");
        if (t9s == std::string::npos) return;

        uint32_t bodyLen = 0;
        for (std::size_t i = t9s; i < t9e; ++i) bodyLen = bodyLen * 10 + (msg[i] - '0');
        const uint32_t newBodyLen = bodyLen + static_cast<uint32_t>(ins.size());

        const std::size_t oldDigits = t9e - t9s;
        char newVal[12];
        const int written = std::snprintf(newVal, sizeof(newVal),
                                          "%0*u", static_cast<int>(oldDigits), newBodyLen);
        msg.erase(msg.begin() + static_cast<std::ptrdiff_t>(t9s),
                  msg.begin() + static_cast<std::ptrdiff_t>(t9e));
        msg.insert(msg.begin() + static_cast<std::ptrdiff_t>(t9s),
                   reinterpret_cast<const uint8_t*>(newVal),
                   reinterpret_cast<const uint8_t*>(newVal + written));
    }

    // ── 3. Recalculate CheckSum (tag 10) ─────────────────────────────────────
    {
        const std::size_t t10pos = fixFindTag(msg.data(), msg.size(), "10");
        if (t10pos == std::string::npos) return;

        uint32_t sum = 0;
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

// ── AppRecord ─────────────────────────────────────────────────────────────────

struct AppRecord
{
    std::vector<uint8_t> fixBytes;  // raw outbound FIX bytes captured from the session
};

// ── CapturingTransport ────────────────────────────────────────────────────────

// Sends FIX bytes over TCP and stores copies of application messages (non-admin)
// in the per-connection store keyed by FIX MsgSeqNum (tag 34).  The store is
// used by FixConnection::handleResendRequest to replay app messages on demand.
struct CapturingTransport
{
    int                           fd{-1};
    std::map<uint32_t, AppRecord>* store{nullptr};

    void operator()(std::span<const uint8_t> bytes) const
    {
        if (fd < 0 || bytes.empty()) return;

        // ── Send via TCP ──────────────────────────────────────────────────────
        std::size_t sent = 0;
        while (sent < bytes.size())
        {
            const ssize_t n = ::send(fd, bytes.data() + sent,
                                     bytes.size() - sent, MSG_NOSIGNAL);
            if (n < 0)
            {
                if (errno == EINTR) continue;
                std::fprintf(stderr, "[TcpTransport] send fd=%d failed: %s\n",
                             fd, std::strerror(errno));
                return;
            }
            sent += static_cast<std::size_t>(n);
        }

        // ── Capture application messages ──────────────────────────────────────
        if (!store || isAdminMsgType(bytes.data(), bytes.size())) return;

        const auto [vs, ve] = fixTagRange(bytes.data(), bytes.size(), "34");
        if (vs == std::string::npos) return;

        uint32_t seq = 0;
        for (std::size_t i = vs; i < ve; ++i) seq = seq * 10 + (bytes[i] - '0');
        if (seq == 0) return;

        (*store)[seq].fixBytes.assign(bytes.begin(), bytes.end());
    }
};

// ── ClusterIngressSender ──────────────────────────────────────────────────────

// Manages the Aeron Cluster session (SessionConnectRequest → SessionEvent(OK))
// and sends AppMessage SBE frames to the cluster ingress.
class ClusterIngressSender
{
public:
    // Connects to the cluster ingress, polls egress until SessionEvent(OK).
    void connect(std::shared_ptr<Aeron> aeron)
    {
        m_aeron = std::move(aeron);

        const auto subId = m_aeron->addSubscription(CLUSTER_EGRESS_CHANNEL, cl::EGRESS_STREAM_ID);
        while (!(m_egressSub = m_aeron->findSubscription(subId)))
            std::this_thread::yield();

        const auto pubId = m_aeron->addPublication(CLUSTER_INGRESS_CHANNEL, cl::INGRESS_STREAM_ID);
        while (!(m_ingressPub = m_aeron->findPublication(pubId)))
            std::this_thread::yield();
        while (!m_ingressPub->isConnected())
            std::this_thread::yield();

        // Send SessionConnectRequest; response channel ephemeral (port 0).
        alignas(16) std::array<uint8_t, 512> connBuf{};
        const int32_t connLen = cl::encodeSessionConnectRequest(
            connBuf.data(), static_cast<int32_t>(connBuf.size()),
            m_correlationId, cl::EGRESS_STREAM_ID,
            CLUSTER_EGRESS_CHANNEL, CLIENT_INFO);

        AtomicBuffer connAtom(connBuf.data(), connBuf.size());
        while (m_ingressPub->offer(connAtom, 0, connLen) < 0)
            std::this_thread::yield();

        // Poll egress for SessionEvent(OK).
        auto deadline = std::chrono::steady_clock::now() +
                        std::chrono::milliseconds(CLUSTER_CONNECT_TIMEOUT_MS);

        auto onEgress = [&](AtomicBuffer& buf, util::index_t off,
                            util::index_t /*len*/, Header&)
        {
            if (cl::decodeTemplateId(buf.buffer(), off) != cl::TEMPLATE_SESSION_EVENT)
                return;
            const auto f = cl::decodeSessionEvent(buf.buffer(), off + cl::HDR_LENGTH);
            if (f.code == cl::EventCode::OK) {
                m_clusterSessionId = f.clusterSessionId;
                m_leadershipTermId = f.leadershipTermId;
                std::printf("[Cluster] Session opened  sessionId=%" PRId64
                            "  termId=%" PRId64 "  leader=%d\n",
                            m_clusterSessionId, m_leadershipTermId, f.leaderMemberId);
            } else {
                std::fprintf(stderr, "[Cluster] SessionEvent error code=%d\n",
                             static_cast<int>(f.code));
            }
        };

        FragmentAssembler fa(onEgress);
        while (m_clusterSessionId < 0 && std::chrono::steady_clock::now() < deadline) {
            if (m_egressSub->poll(fa.handler(), 10) == 0)
                std::this_thread::yield();
        }

        if (m_clusterSessionId < 0)
            throw std::runtime_error("[FixSessionClient] Timed out waiting for cluster session");
    }

    bool isConnected() const { return m_clusterSessionId >= 0; }

    // Send a keep-alive to the cluster ingress if the interval has elapsed.
    // Must be called regularly (e.g. every duty-cycle iteration) to prevent session timeout.
    void keepAlive()
    {
        if (!m_ingressPub || m_clusterSessionId < 0) return;
        const int64_t now = nowMs();
        if (now - m_lastKeepAliveMs < KEEP_ALIVE_INTERVAL_MS) return;
        m_lastKeepAliveMs = now;

        alignas(16) std::array<uint8_t, 64> kaBuf{};
        const int32_t kaLen = cl::encodeSessionKeepAlive(
            kaBuf.data(), static_cast<int32_t>(kaBuf.size()),
            m_leadershipTermId, m_clusterSessionId);
        AtomicBuffer kaAb(kaBuf.data(), kaBuf.size());
        if (m_ingressPub->offer(kaAb, 0, kaLen) < 0)
            std::fprintf(stderr, "[Cluster] keep-alive offer failed\n");
    }

    // Drain cluster egress; calls onAppMessage for each application-layer response.
    void pollEgress(const std::function<void(const uint8_t*, int32_t)>& onAppMessage)
    {
        if (!m_egressSub) return;
        m_onAppMessage = &onAppMessage;
        m_egressSub->poll(m_fa.handler(), 10);
        m_onAppMessage = nullptr;
    }

    // Wraps fixBytes in AppMessage SBE + SessionMessageHeader and offers to the cluster.
    void send(const uint8_t* fixBytes, uint16_t fixLen)
    {
        if (!m_ingressPub || m_clusterSessionId < 0 || fixLen == 0) return;

        alignas(16) std::array<uint8_t, 4096 + 42> buf{};
        const int32_t hdrLen = cl::encodeSessionMessageHeader(
            buf.data(), static_cast<int32_t>(buf.size()),
            m_leadershipTermId, m_clusterSessionId, nowMs());
        const int32_t appLen = encodeAppMessage(buf.data() + hdrLen, fixBytes, fixLen);

        AtomicBuffer ab(buf.data(), buf.size());
        if (m_ingressPub->offer(ab, 0, hdrLen + appLen) < 0)
            std::fprintf(stderr, "[Cluster] ingress offer failed\n");
    }

private:
    static constexpr int64_t KEEP_ALIVE_INTERVAL_MS = 1000;

    std::shared_ptr<Aeron>           m_aeron;
    std::shared_ptr<Publication>     m_ingressPub;
    std::shared_ptr<Subscription>    m_egressSub;
    int64_t  m_clusterSessionId = -1;
    int64_t  m_leadershipTermId = -1;
    int64_t  m_lastKeepAliveMs  = 0;
    const int64_t m_correlationId = 1;

    // Per-call callback set in pollEgress; null outside of that call.
    const std::function<void(const uint8_t*, int32_t)>* m_onAppMessage{nullptr};

    // Persistent across poll() calls so multi-fragment messages reassemble correctly.
    FragmentAssembler m_fa{[this](AtomicBuffer& buf, util::index_t off,
                                  util::index_t len, Header&)
    {
        if (static_cast<int32_t>(len) < cl::HDR_LENGTH) return;
        const uint16_t templateId = cl::decodeTemplateId(buf.buffer(), off);
        if (templateId == cl::TEMPLATE_NEW_LEADER_EVENT)
        {
            m_leadershipTermId = cl::detail::get64(buf.buffer(), off + cl::HDR_LENGTH);
            std::printf("[Cluster] New leader  termId=%" PRId64 "\n", m_leadershipTermId);
            return;
        }
        if (templateId != cl::TEMPLATE_SESSION_MESSAGE_HEADER) return;
        const int32_t appOff = off + cl::HDR_LENGTH + cl::SESSION_MESSAGE_HEADER_BLOCK_LENGTH;
        const int32_t appLen = static_cast<int32_t>(len)
                             - cl::HDR_LENGTH - cl::SESSION_MESSAGE_HEADER_BLOCK_LENGTH;
        if (appLen > 0 && m_onAppMessage)
            (*m_onAppMessage)(buf.buffer() + appOff, appLen);
    }};
};

// Forward-declared here so ClusterIngressHandler can hold a pointer to it.
using FixSession = sess::ServerSession<FIXT_1_1, "SEQUENCER", "CLIENT",
                                       sess::NullStorage, CapturingTransport>;

// ── ClusterIngressHandler ─────────────────────────────────────────────────────

// Receives decoded FIX messages on the TCP receive path and forwards them to
// the cluster WITHOUT updating any session state. Admin messages are encoded
// as SBE (sbe-session.xml, schemaId=100) prefixed with a 4-byte connection ID
// for routing replies. Application messages (NewOrderSingle) are forwarded as
// raw FIX bytes wrapped in the AppMessage SBE envelope.
class ClusterIngressHandler : public FixMessageHandler<ClusterIngressHandler>
{
    ClusterIngressSender*    m_ingress{};
    int32_t                  m_connectionId{-1};
    std::span<const uint8_t> m_rawFixBytes;
    FixSession*              m_session{nullptr};

    // SBE encode buffer: [4-byte connId][8-byte SBE header][SBE body]
    alignas(16) std::array<uint8_t, 512> m_sbeBuf{};

    char*    sbeBufBody()   { return reinterpret_cast<char*>(m_sbeBuf.data() + CONN_ID_PREFIX); }
    uint64_t sbeBufLen()    { return m_sbeBuf.size() - CONN_ID_PREFIX; }

    // Writes connId at offset 0 and sends the SBE bytes to the cluster.
    template <typename SbeMsg>
    void sendAdmin(SbeMsg& msg)
    {
        if (!m_ingress) return;
        std::memcpy(m_sbeBuf.data(), &m_connectionId, CONN_ID_PREFIX);
        const uint16_t total = static_cast<uint16_t>(CONN_ID_PREFIX + msg.sbePosition());
        m_ingress->send(m_sbeBuf.data(), total);
    }

public:
    using FixMessageHandler::handle;

    ClusterIngressHandler() = default;
    ClusterIngressHandler(ClusterIngressSender* ingress, int32_t connId, FixSession* session = nullptr)
        : m_ingress(ingress), m_connectionId(connId), m_session(session) {}

    void setRawBytes(std::span<const uint8_t> bytes) { m_rawFixBytes = bytes; }

    fix::Result handle(LogonDecoder& logon)
    {
        const uint32_t hbSecs = logon.heartbeatInterval().value_or(30u);
        std::printf("[Ingress] Logon from fd=%d hbSecs=%u → encoding SBE\n",
                    m_connectionId, hbSecs);
        sbesess::Logon msg;
        msg.wrapAndApplyHeader(sbeBufBody(), 0, sbeBufLen());
        msg.putSender(static_cast<const char*>("CLIENT  ")).putTarget(static_cast<const char*>("SEQNCR  "))
           .seqNum(0).sendingTimeMs(nowMs())
           .encryptMethod(sbesess::EncryptMethod::Value::None)
           .heartbeatInterval(hbSecs)
           .putXmlData(nullptr, 0);
        sendAdmin(msg);
        std::printf("[Ingress] Logon sent to cluster (fd=%d sbePos=%llu)\n",
                    m_connectionId, static_cast<unsigned long long>(msg.sbePosition()));
        return fix::Result::Success;
    }

    fix::Result handle(LogoutDecoder& /*logout*/)
    {
        sbesess::Logout msg;
        msg.wrapAndApplyHeader(sbeBufBody(), 0, sbeBufLen());
        msg.putSender(static_cast<const char*>("CLIENT  ")).putTarget(static_cast<const char*>("SEQNCR  "))
           .seqNum(0).sendingTimeMs(nowMs())
           .putText(nullptr, 0);
        sendAdmin(msg);
        return fix::Result::Success;
    }

    fix::Result handle(HeartbeatDecoder& heartbeat)
    {
        sbesess::Heartbeat msg;
        msg.wrapAndApplyHeader(sbeBufBody(), 0, sbeBufLen());
        msg.putSender(static_cast<const char*>("CLIENT  ")).putTarget(static_cast<const char*>("SEQNCR  "))
           .seqNum(0).sendingTimeMs(nowMs());
        if (const auto id = heartbeat.testReqID()) {
            const auto sv = *id;
            const std::size_t n = std::min(sv.size(), static_cast<std::size_t>(32));
            std::memcpy(msg.testReqID(), sv.data(), n);
            if (n < 32) msg.testReqID()[n] = '\0';
        } else {
            msg.testReqID()[0] = '\0';
        }
        sendAdmin(msg);
        return fix::Result::Success;
    }

    fix::Result handle(TestRequestDecoder& testRequest)
    {
        sbesess::TestRequest msg;
        msg.wrapAndApplyHeader(sbeBufBody(), 0, sbeBufLen());
        msg.putSender(static_cast<const char*>("CLIENT  ")).putTarget(static_cast<const char*>("SEQNCR  "))
           .seqNum(0).sendingTimeMs(nowMs());
        if (const auto id = testRequest.testReqID()) {
            const auto sv = *id;
            const std::size_t n = std::min(sv.size(), static_cast<std::size_t>(32));
            std::memcpy(msg.testReqID(), sv.data(), n);
            if (n < 32) msg.testReqID()[n] = '\0';
        } else {
            msg.testReqID()[0] = '\0';
        }
        sendAdmin(msg);
        return fix::Result::Success;
    }

    fix::Result handle(ResendRequestDecoder& rr)
    {
        sbesess::ResendRequest msg;
        msg.wrapAndApplyHeader(sbeBufBody(), 0, sbeBufLen());
        msg.putSender(static_cast<const char*>("CLIENT  ")).putTarget(static_cast<const char*>("SEQNCR  "))
           .seqNum(0).sendingTimeMs(nowMs())
           .beginSeqNo(rr.beginSeqNo().value_or(1u))
           .endSeqNo(rr.endSeqNo().value_or(0u));
        sendAdmin(msg);
        return fix::Result::Success;
    }

    fix::Result handle(SequenceResetDecoder& sr)
    {
        sbesess::SequenceReset msg;
        msg.wrapAndApplyHeader(sbeBufBody(), 0, sbeBufLen());
        msg.putSender(static_cast<const char*>("CLIENT  ")).putTarget(static_cast<const char*>("SEQNCR  "))
           .seqNum(0).sendingTimeMs(nowMs())
           .gapFillFlag(sbesess::GapFillFlag::Value::NULL_VALUE)
           .newSeqNo(sr.newSeqNo().value_or(1u));
        sendAdmin(msg);
        return fix::Result::Success;
    }

    fix::Result handle(NewOrderSingleDecoder& nos)
    {
        const auto clOrdId  = nos.clOrdID().value_or(std::string_view{});
        const auto symbol   = nos.symbol().value_or(std::string_view{});
        const Side side     = nos.side().value_or(Side::Buy);
        const auto qty      = nos.orderQty().value_or(0u);
        const auto ordType  = nos.ordType().value_or(OrdType::Market);
        const auto priceOpt = nos.price();

        std::printf("[App] NewOrderSingle clOrdID=%.*s symbol=%.*s side=%s qty=%u ordType=%s\n",
                    static_cast<int>(clOrdId.size()), clOrdId.data(),
                    static_cast<int>(symbol.size()), symbol.data(),
                    name(side).data(), qty, name(ordType).data());

        // Business validation
        const char* rejectReason = nullptr;
        if (clOrdId.empty())
            rejectReason = "ClOrdID is empty";
        else if (symbol.empty())
            rejectReason = "Symbol is empty";
        else if (qty == 0)
            rejectReason = "OrderQty must be > 0";
        else if (ordType == OrdType::Limit && !priceOpt)
            rejectReason = "Price required for Limit order";
        else if (priceOpt && *priceOpt <= fix::utils::FixedDecimal{0})
            rejectReason = "Price must be positive";

        if (rejectReason) {
            std::fprintf(stderr, "[App] Rejected clOrdID=%.*s: %s\n",
                         static_cast<int>(clOrdId.size()), clOrdId.data(), rejectReason);
            if (m_session) {
                m_session->setNowMs(nowMs());
                ExecutionReportEncoder er;
                m_session->wrapHeader(er);
                er.orderID("NONE")
                  .clOrdID(clOrdId)
                  .execID("EXEC-REJ")
                  .execType(ExecType::Rejected)
                  .ordStatus(OrdStatus::Rejected)
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
                m_ingress->send(m_rawFixBytes.data(), static_cast<uint16_t>(sz));
            }
        }

        if (m_session) {
            m_session->setNowMs(nowMs());
            ExecutionReportEncoder er;
            m_session->wrapHeader(er);
            er.orderID("ORD-0001")
              .clOrdID(clOrdId)
              .execID("EXEC-0001")
              .execType(ExecType::New)
              .ordStatus(OrdStatus::New)
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

// ── FixConnection ─────────────────────────────────────────────────────────────
//
// TCP receive path:  ClusterIngressHandler encodes every FIX message as SBE
//                   (admin) or raw FIX (app) and forwards to the cluster.
//                   Session state is NOT updated here.
//
// Global stream path: FixConnection::onClusterMessage() is called by the main
//                     loop when a SequencedMessage arrives that was sourced by
//                     this connection. It advances the session clock to the
//                     cluster timestamp and drives the session state machine,
//                     which then writes the FIX response to the TCP socket.

// Session only needs a transport; it no longer handles inbound application messages.

static constexpr std::size_t MAX_RECV_BUF = 1u * 1024u * 1024u;  // 1 MB

struct FixConnection
{
    int  fd;
    bool m_dead{false};
    // m_appStore must be declared before session so its address is stable when
    // CapturingTransport (stored inside session by value) is constructed.
    std::map<uint32_t, AppRecord>          m_appStore;
    FixSession                             session;
    ClusterIngressHandler                  ingressHandler;
    fix::decoder::PayloadDecoder<FIXT_1_1> decoder;
    std::vector<uint8_t>                   recvBuf;

    FixConnection(int fd_, ClusterIngressSender* ingress)
        : fd(fd_)
        , session(FixSession::Builder{sess::NullStorage{}}
                      .transport(CapturingTransport{fd_, &m_appStore})
                      .build())
        , ingressHandler(ingress, fd_, &session)
    {
        session.onTcpConnected();
        recvBuf.reserve(8192);
    }

    [[nodiscard]] bool isDead() const noexcept { return m_dead; }

    // Handles a ResendRequest from the cluster global stream.
    //
    // Walks [begin, limit) using the in-memory app-message store populated by
    // CapturingTransport.  For each contiguous run of admin seqNums it emits one
    // SequenceReset GapFill; for each stored app-message seqNum it patches
    // PossDupFlag=Y + OrigSendingTime into the raw bytes and sends them directly.
    // The session's outgoing sequence counter is restored to savedNext afterwards.
    void handleResendRequest(uint32_t begin, uint32_t end, int64_t clusterTs)
    {
        session.setNowMs(clusterTs);
        if (!session.isActive()) return;

        const uint32_t savedNext = session.nextOutgoingSeqNum();
        const uint32_t limit     = (end == 0 || end + 1 >= savedNext)
                                   ? savedNext : end + 1;

        std::printf("[Resend] fd=%d begin=%u end=%u limit=%u store=%zu\n",
                    fd, begin, end, limit, m_appStore.size());

        uint32_t gapStart = 0;

        for (uint32_t seq = begin; seq < limit; ++seq)
        {
            const auto it = m_appStore.find(seq);

            if (it == m_appStore.end())
            {
                // Admin message — accumulate into the current gap range
                if (gapStart == 0) gapStart = seq;
            }
            else
            {
                // App message — flush any pending gap-fill first
                if (gapStart != 0)
                {
                    std::printf("[Resend] fd=%d GapFill [%u, %u)\n", fd, gapStart, seq);
                    session.sendGapFill(gapStart, seq, clusterTs);
                    gapStart = 0;
                }

                // Patch PossDupFlag + OrigSendingTime into a copy and send raw
                std::vector<uint8_t> patched = it->second.fixBytes;
                patchResendFlags(patched);

                std::printf("[Resend] fd=%d AppMsg seq=%u bytes=%zu\n",
                            fd, seq, patched.size());

                std::size_t sent = 0;
                while (sent < patched.size())
                {
                    const ssize_t n = ::send(fd, patched.data() + sent,
                                             patched.size() - sent, MSG_NOSIGNAL);
                    if (n < 0)
                    {
                        if (errno == EINTR) continue;
                        std::fprintf(stderr, "[Resend] fd=%d send failed: %s\n",
                                     fd, std::strerror(errno));
                        break;
                    }
                    sent += static_cast<std::size_t>(n);
                }

                session.setNextOutgoingSeqNum(seq + 1);
            }
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
            if (msgLen == 0) break;

            const auto msgSpan = remaining.subspan(0, msgLen);
            ingressHandler.setRawBytes(msgSpan);
            const auto parseResult = decoder.parse(msgSpan, ingressHandler);
            std::printf("[FixConnection] fd=%d parse status=%d processed=%zu msgLen=%zu\n",
                        fd, static_cast<int>(parseResult.m_value),
                        static_cast<std::size_t>(parseResult.m_processed), msgLen);
            consumed += msgLen;
        }

        if (consumed > 0)
            recvBuf.erase(recvBuf.begin(),
                          recvBuf.begin() + static_cast<std::ptrdiff_t>(consumed));
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
        case sbesess::Logon::sbeTemplateId():
        {
            sbesess::Logon msg;
            msg.wrapForDecode(const_cast<char*>(sbeBody), sbesess::MessageHeader::encodedLength(),
                              blockLen, version, sbeBodyLen);
            session.handleClusterLogon(msg.heartbeatInterval(), clusterTimestampMs);
            break;
        }
        case sbesess::Logout::sbeTemplateId():
            session.handleClusterLogout(clusterTimestampMs);
            break;

        case sbesess::Heartbeat::sbeTemplateId():
        {
            sbesess::Heartbeat msg;
            msg.wrapForDecode(const_cast<char*>(sbeBody), sbesess::MessageHeader::encodedLength(),
                              blockLen, version, sbeBodyLen);
            session.handleClusterHeartbeat(clusterTimestampMs);
            break;
        }
        case sbesess::TestRequest::sbeTemplateId():
        {
            sbesess::TestRequest msg;
            msg.wrapForDecode(const_cast<char*>(sbeBody), sbesess::MessageHeader::encodedLength(),
                              blockLen, version, sbeBodyLen);
            session.handleClusterTestRequest(msg.testReqID(), clusterTimestampMs);
            break;
        }
        case sbesess::ResendRequest::sbeTemplateId():
        {
            sbesess::ResendRequest msg;
            msg.wrapForDecode(const_cast<char*>(sbeBody), sbesess::MessageHeader::encodedLength(),
                              blockLen, version, sbeBodyLen);
            handleResendRequest(msg.beginSeqNo(), msg.endSeqNo(), clusterTimestampMs);
            break;
        }
        case sbesess::SequenceReset::sbeTemplateId():
        {
            sbesess::SequenceReset msg;
            msg.wrapForDecode(const_cast<char*>(sbeBody), sbesess::MessageHeader::encodedLength(),
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
};

// ── GlobalStreamFixHandler ────────────────────────────────────────────────────

// Decodes FIX messages arriving on the global stream (SequencedMessage payloads).
// Currently routes ExecutionReports to the provided callback for broadcast to TCP clients.
class GlobalStreamFixHandler : public FixMessageHandler<GlobalStreamFixHandler>
{
    std::function<void(ExecutionReportDecoder&)> m_onExecReport;

public:
    using FixMessageHandler::handle;

    explicit GlobalStreamFixHandler(std::function<void(ExecutionReportDecoder&)> onExecReport)
        : m_onExecReport(std::move(onExecReport)) {}

    fix::Result handle(ExecutionReportDecoder& er)
    {
        if (m_onExecReport) m_onExecReport(er);
        return fix::Result::Success;
    }
};

// ── TcpServer ─────────────────────────────────────────────────────────────────

class TcpServer
{
public:
    void start(uint16_t port)
    {
        m_listenFd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (m_listenFd < 0)
            throw std::runtime_error("socket() failed: " + std::string(std::strerror(errno)));

        const int one = 1;
        ::setsockopt(m_listenFd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

        sockaddr_in addr{};
        addr.sin_family      = AF_INET;
        addr.sin_addr.s_addr = INADDR_ANY;
        addr.sin_port        = htons(port);

        if (::bind(m_listenFd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0)
            throw std::runtime_error("bind() failed: " + std::string(std::strerror(errno)));
        if (::listen(m_listenFd, FIX_TCP_BACKLOG) < 0)
            throw std::runtime_error("listen() failed: " + std::string(std::strerror(errno)));

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
        if (fd < 0) return -1;

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
        seq::GLOBAL_STREAM_CHANNEL, seq::GLOBAL_STREAM_ID,
        [&](aeron::archive::client::RecordingDescriptor& desc) {
            if (desc.m_stopPosition == aeron::archive::client::NULL_POSITION) {
                activeId = desc.m_recordingId;  // active recording wins
            } else if (desc.m_stopPosition > stoppedPos) {
                stoppedId  = desc.m_recordingId;
                stoppedPos = desc.m_stopPosition;
            }
        });

    if (activeId < 0 && stoppedId < 0)
        throw std::runtime_error(
            "[FixSessionClient] No global stream recording found on " +
            std::string(seq::GLOBAL_STREAM_CHANNEL));

    if (activeId >= 0) {
        // Resolve actual write position so the catch-up check is accurate.
        catchUpPos = archive.getRecordingPosition(activeId);
        if (catchUpPos == aeron::archive::client::NULL_POSITION) catchUpPos = 0;
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

    // ── Start replay (only when there is historical data to replay) ──────────
    std::int64_t replaySessionId = -1;
    if (catchUpPosition > 0) {
        aeron::archive::client::ReplayParams replayParams;
        replayParams.position(0).length(aeron::archive::client::NULL_LENGTH);
        replaySessionId = archive->startReplay(
            recordingId, FIX_REPLAY_CHANNEL, seq::REPLAY_STREAM_ID, replayParams);
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

    // ── Global stream FIX decoder (for application messages, e.g. ExecutionReport) ─
    fix::decoder::PayloadDecoder<FIXT_1_1> globalDecoder;

    GlobalStreamFixHandler globalFixHandler([&](ExecutionReportDecoder& er)
    {
        const auto clOrdId   = er.clOrdID().value_or(std::string_view{});
        const auto execType  = er.execType();
        const auto ordStatus = er.ordStatus();
        std::printf("[Global] ExecutionReport clOrdID=%.*s"
                    " execType=%d ordStatus=%d\n",
                    static_cast<int>(clOrdId.size()), clOrdId.data(),
                    execType ? static_cast<int>(*execType) : -1,
                    ordStatus ? static_cast<int>(*ordStatus) : -1);

        // TODO: encode ExecutionReport as FIX and send to matching TCP client(s).
        (void)er;
    });

    // ── Global stream subscription ────────────────────────────────────────────
    // Payload layout after the 10-byte AppMessage SBE prefix:
    //   Admin SBE:  [4-byte connId LE][8-byte SBE MessageHeader][SBE body]
    //               Identified by schemaId==SESSION_SCHEMA_ID at payload[8..9].
    //   Raw FIX:    bytes starting with '8' (BeginString).
    seq::GlobalStreamClient globalStream(
        [&](const seq::SequencedEvent& e)
        {
            std::printf("[Global] SequencedEvent globalSeq=%" PRId64 " payloadLen=%" PRIu64 "\n",
                        e.globalSeqNo, static_cast<uint64_t>(e.payloadLength));
            if (e.payloadLength <= seq::APP_MSG_SBE_PREFIX) return;
            const auto* payload = reinterpret_cast<const uint8_t*>(e.payload)
                                  + seq::APP_MSG_SBE_PREFIX;
            const uint64_t payloadLen = e.payloadLength - seq::APP_MSG_SBE_PREFIX;

            // Detect admin SBE: connId prefix (4 bytes) + SBE header (8 bytes).
            // schemaId is at payload[CONN_ID_PREFIX + 4] (bytes 4-5 within SBE header).
            if (payloadLen >= CONN_ID_PREFIX + sbesess::MessageHeader::encodedLength())
            {
                uint16_t schemaId;
                std::memcpy(&schemaId,
                            payload + CONN_ID_PREFIX + 4,  // offset within SBE header
                            sizeof(uint16_t));
                schemaId = SBE_LITTLE_ENDIAN_ENCODE_16(schemaId);  // no-op on LE; bswap on BE

                std::printf("[Global] schemaId=%u (expected SESSION=%u)\n",
                            schemaId, SESSION_SCHEMA_ID);
                if (schemaId == SESSION_SCHEMA_ID)
                {
                    int32_t connId;
                    std::memcpy(&connId, payload, CONN_ID_PREFIX);
                    std::printf("[Global] Admin SBE connId=%d connections.size=%zu\n",
                                connId, connections.size());

                    const char* sbeHeader = reinterpret_cast<const char*>(payload) + CONN_ID_PREFIX;
                    const uint64_t sbeLen = payloadLen - CONN_ID_PREFIX;

                    // Read SBE MessageHeader fields.
                    uint16_t blockLen, templateId, version;
                    std::memcpy(&blockLen,   sbeHeader + 0, 2);
                    std::memcpy(&templateId, sbeHeader + 2, 2);
                    std::memcpy(&version,    sbeHeader + 6, 2);
                    blockLen   = SBE_LITTLE_ENDIAN_ENCODE_16(blockLen);
                    templateId = SBE_LITTLE_ENDIAN_ENCODE_16(templateId);
                    version    = SBE_LITTLE_ENDIAN_ENCODE_16(version);

                    auto it = connections.find(connId);
                    if (it != connections.end())
                    {
                        it->second->onClusterAdmin(templateId, sbeHeader, sbeLen,
                                                   blockLen, version,
                                                   e.clusterTimestamp);
                    }
                    return;
                }
            }

            // Raw FIX application message.
            if (payloadLen > 0 && payload[0] == '8')
            {
                globalDecoder.parse(std::span<const uint8_t>(payload, payloadLen),
                                    globalFixHandler);
            }
        },
        [](const seq::LifecycleEvent& e)
        {
            std::printf("[Global] Source connected    id=%" PRId64 "\n",
                        e.sourceSessionId);
        },
        [](const seq::LifecycleEvent& e)
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
        for (const auto& [fd, _] : connections)
            pfds.push_back({fd, POLLIN, 0});

        ::poll(pfds.data(), static_cast<nfds_t>(pfds.size()), 0);

        // New TCP connections.
        if (pfds[0].revents & POLLIN) {
            const int clientFd = tcpServer.acceptNewConnection();
            if (clientFd >= 0) {
                try {
                    connections.emplace(clientFd,
                        std::make_unique<FixConnection>(clientFd, &ingressSender));
                } catch (...) {
                    ::close(clientFd);
                    throw;
                }
            }
        }

        // Inbound data from established connections.
        std::vector<int> toClose;
        for (std::size_t i = 1; i < pfds.size(); ++i) {
            if (!(pfds[i].revents & (POLLIN | POLLHUP | POLLERR))) continue;
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
                    if (it->second->isDead() || it->second->session.isPendingClose())
                        toClose.push_back(fd);
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
        for (auto& [_, conn] : connections)
            conn->session.keepAlive();

        // Aeron: global stream + cluster egress + session keep-alive.
        const int aeronWork = globalStream.poll();
        ingressSender.keepAlive();
        ingressSender.pollEgress([](const uint8_t* /*data*/, int32_t /*len*/) {});

        if (aeronWork == 0 && pfds[0].revents == 0)
            std::this_thread::yield();
    }

    std::printf("[FixSessionClient] Shutting down. Active connections: %zu\n",
                connections.size());
    for (const auto& [fd, _] : connections) ::close(fd);
    tcpServer.shutdown();
    return 0;
}
