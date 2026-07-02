/*
 * FixSessionClient — TCP FIX gateway bridging FIX clients to the Aeron Cluster sequencer.
 *
 * Inbound path (TCP → Cluster):
 *   1. Accepts FIX sessions on TCP port 9000.
 *   2. Decodes each message with simdfix PayloadDecoder<FIXT_1_1>.
 *   3. Admin messages (Logon, Heartbeat, …) are handled by the per-connection
 *      ServerSession state machine, which sends responses back on TCP.
 *   4. Application messages (NewOrderSingle) are wrapped in an AppMessage SBE
 *      envelope and offered to the Aeron Cluster ingress publication.
 *
 * Outbound path (Global stream → TCP):
 *   On startup the client replays the archive (NULL_POSITION length → live
 *   follow-through on the same image), then decodes each SequencedMessage's
 *   embedded FIX payload.  ExecutionReports arriving on the global stream are
 *   broadcast to all connected TCP FIX clients.
 *
 * Channels / ports (must match SequencerNode defaults):
 *   Archive control  aeron:udp?endpoint=localhost:9301  stream 100
 *   Cluster ingress  aeron:udp?endpoint=localhost:9302  stream 101
 *   Cluster egress   aeron:udp?endpoint=localhost:0     stream 102
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
#include "AeronArchive.h"

// simdfix — session FSM + codec
#include "org/limitless/fix/session/ServerSession.hpp"
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

// ── Namespace aliases ──────────────────────────────────────────────────────────

namespace fix  = org::limitless::fix;
namespace sess = fix::session;
namespace msg  = fix::generated::messages;
namespace cfg  = fix::generated::config;
namespace cl   = org::limitless::phixeron::cluster;
namespace seq  = org::limitless::phixeron::sequencer;

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
static constexpr const char* CLUSTER_EGRESS_CHANNEL    = "aeron:udp?endpoint=localhost:0";
static constexpr const char* CLIENT_INFO               = "FixSessionClient";
static constexpr int64_t     CLUSTER_CONNECT_TIMEOUT_MS = 10'000;

// AppMessage SBE constants (sequencer.xml schemaId=201, templateId=1, blockLength=0)
static constexpr uint16_t APP_MSG_TEMPLATE_ID = 1;
static constexpr uint16_t APP_MSG_SCHEMA_ID   = 201;

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

// ── TcpTransport ──────────────────────────────────────────────────────────────

// Outbound FIX transport for one TCP connection.  Stored by value in
// ServerSession; the session calls operator() for each encoded admin message.
struct TcpTransport
{
    int fd{-1};

    void operator()(std::span<const uint8_t> bytes) const
    {
        if (fd < 0 || bytes.empty()) return;
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
    std::shared_ptr<Aeron>           m_aeron;
    std::shared_ptr<Publication>     m_ingressPub;
    std::shared_ptr<Subscription>    m_egressSub;
    int64_t  m_clusterSessionId = -1;
    int64_t  m_leadershipTermId = -1;
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

// ── OrderGatewayApplication ───────────────────────────────────────────────────

// Application handler injected into each ServerSession.  Receives
// NewOrderSingle messages decoded from TCP and forwards the raw FIX bytes to
// the cluster ingress.
class OrderGatewayApplication : public FixMessageHandler<OrderGatewayApplication>
{
    ClusterIngressSender* m_ingress{};
    // Raw FIX bytes of the message currently being dispatched; set by FixConnection
    // before each PayloadDecoder::parse() call so handle() can send the exact wire bytes.
    std::span<const uint8_t> m_rawFixBytes;

public:
    using FixMessageHandler::handle;  // keep base overloads in scope

    OrderGatewayApplication() = default;

    explicit OrderGatewayApplication(ClusterIngressSender* ingress)
        : m_ingress(ingress) {}

    void setRawBytes(std::span<const uint8_t> bytes) { m_rawFixBytes = bytes; }

    fix::Result handle(NewOrderSingleDecoder& nos)
    {
        const auto clOrdId = nos.clOrdID().value_or(std::string_view{});
        const auto symbol  = nos.symbol().value_or(std::string_view{});
        std::printf("[App] NewOrderSingle clOrdID=%.*s symbol=%.*s\n",
                    static_cast<int>(clOrdId.size()), clOrdId.data(),
                    static_cast<int>(symbol.size()), symbol.data());

        if (m_ingress && !m_rawFixBytes.empty()) {
            const std::size_t sz = m_rawFixBytes.size();
            if (sz > 4096) {
                std::fprintf(stderr, "[App] FIX message too large (%zu bytes); dropped\n", sz);
                return fix::Result::Success;
            }
            m_ingress->send(m_rawFixBytes.data(), static_cast<uint16_t>(sz));
        }
        return fix::Result::Success;
    }
};

// ── FixConnection ──────────────────────────────────────────────────────────────

using FixSession = sess::ServerSession<FIXT_1_1, "SEQUENCER", "CLIENT",
                                       sess::NullStorage, TcpTransport,
                                       OrderGatewayApplication>;

static constexpr std::size_t MAX_RECV_BUF = 1u * 1024u * 1024u;  // 1 MB

// One FixConnection per accepted TCP socket.  Owns the ServerSession, a byte
// accumulator for split TCP reads, and a PayloadDecoder.
struct FixConnection
{
    int fd;
    bool m_dead{false};
    FixSession                          session;
    fix::decoder::PayloadDecoder<FIXT_1_1> decoder;
    std::vector<uint8_t>                recvBuf;

    FixConnection(int fd_, ClusterIngressSender* ingress)
        : fd(fd_)
        , session(FixSession::Builder{sess::NullStorage{}}
                      .transport(TcpTransport{fd_})
                      .application(OrderGatewayApplication{ingress})
                      .build())
    {
        session.onTcpConnected();
        recvBuf.reserve(8192);
    }

    [[nodiscard]] bool isDead() const noexcept { return m_dead; }

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
            if (msgLen == 0) break;  // incomplete message — wait for more bytes

            const auto msgSpan = remaining.subspan(0, msgLen);
            session.application().setRawBytes(msgSpan);
            decoder.parse(msgSpan, session);
            consumed += msgLen;
        }

        if (consumed > 0)
            recvBuf.erase(recvBuf.begin(),
                          recvBuf.begin() + static_cast<std::ptrdiff_t>(consumed));
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
    aeron::archive::AeronArchive& archive, std::int64_t& catchUpPos)
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
        [&](const aeron::archive::RecordingDescriptor& desc) {
            if (desc.stopPosition == aeron::archive::NULL_POSITION) {
                activeId = desc.recordingId;  // active recording wins
            } else if (desc.stopPosition > stoppedPos) {
                stoppedId  = desc.recordingId;
                stoppedPos = desc.stopPosition;
            }
        });

    if (activeId < 0 && stoppedId < 0)
        throw std::runtime_error(
            "[FixSessionClient] No global stream recording found on " +
            std::string(seq::GLOBAL_STREAM_CHANNEL));

    if (activeId >= 0) {
        // Resolve actual write position so the catch-up check is accurate.
        catchUpPos = archive.getRecordingPosition(activeId);
        if (catchUpPos == aeron::archive::NULL_POSITION) catchUpPos = 0;
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
    aeron::archive::AeronArchive::Context archiveCtx;
    archiveCtx.aeron(aeron)
              .controlRequestChannel(ARCHIVE_CONTROL_CHANNEL)
              .controlRequestStreamId(ARCHIVE_CONTROL_STREAM)
              .controlResponseChannel(ARCHIVE_RESPONSE_CHANNEL);

    auto archive = aeron::archive::AeronArchive::connect(archiveCtx);
    std::puts("[FixSessionClient] Connected to Aeron Archive");

    // ── Locate global stream recording ───────────────────────────────────────
    std::int64_t catchUpPosition = 0;
    const std::int64_t recordingId =
        findGlobalStreamRecording(*archive, catchUpPosition);
    std::printf("[FixSessionClient] Recording %" PRId64
                "  catchUpPosition=%" PRId64 "\n", recordingId, catchUpPosition);

    // ── Start replay (NULL_POSITION → follow live recording) ─────────────────
    const std::int64_t replaySessionId = archive->startReplay(
        recordingId, /*position=*/0,
        aeron::archive::AeronArchive::NULL_POSITION,
        seq::REPLAY_CHANNEL, seq::REPLAY_STREAM_ID);
    std::printf("[FixSessionClient] Replay started  replaySessionId=%" PRId64 "\n",
                replaySessionId);

    // ── Cluster ingress ───────────────────────────────────────────────────────
    ClusterIngressSender ingressSender;
    ingressSender.connect(aeron);

    // ── TCP server ────────────────────────────────────────────────────────────
    TcpServer tcpServer;
    tcpServer.start(FIX_TCP_PORT);

    // ── Per-connection state ──────────────────────────────────────────────────
    std::unordered_map<int, std::unique_ptr<FixConnection>> connections;

    // ── Global stream FIX decoder (for ExecutionReports) ─────────────────────
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

        // Route by TargetCompID (= the originating client's SenderCompID).
        // For now broadcast to all TCP clients; a routing table can be added
        // here once clients identify themselves with distinct CompIDs.
        //
        // TODO: encode ExecutionReport as FIX and send to TCP client(s).
        (void)er;
    });

    // ── Global stream subscription ────────────────────────────────────────────
    seq::GlobalStreamClient globalStream(
        [&](const seq::SequencedEvent& e)
        {
            if (e.payloadLength <= seq::APP_MSG_SBE_PREFIX) return;
            const auto* fixBytes = reinterpret_cast<const uint8_t*>(e.payload)
                                   + seq::APP_MSG_SBE_PREFIX;
            const std::size_t fixLen = e.payloadLength - seq::APP_MSG_SBE_PREFIX;
            globalDecoder.parse(std::span<const uint8_t>(fixBytes, fixLen),
                                globalFixHandler);
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

    globalStream.start(aeron, replaySessionId, catchUpPosition);
    std::puts("[FixSessionClient] Replay started — replaying history…");

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

        // Heartbeat keep-alive for all active sessions.
        const int64_t tsMs = nowMs();
        for (auto& [_, conn] : connections)
            conn->session.keepAlive(milliseconds{tsMs});

        // Aeron: global stream + cluster egress.
        const int aeronWork = globalStream.poll();
        ingressSender.pollEgress([](const uint8_t* /*data*/, int32_t /*len*/) {
            // Cluster echo / keepalive — not expected in normal operation.
        });

        if (aeronWork == 0 && pfds[0].revents == 0)
            std::this_thread::yield();
    }

    std::printf("[FixSessionClient] Shutting down. Active connections: %zu\n",
                connections.size());
    for (const auto& [fd, _] : connections) ::close(fd);
    tcpServer.shutdown();
    return 0;
}
