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

// Generated FIX types (application.xml via GenerateAppMessages)
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
namespace seq     = org::limitless::phixeron::sequencer;
namespace sbesess = org::limitless::phixeron::sbe;

using namespace aeron;
using namespace aeron::concurrent;
using namespace fix::generated::config;   // FIXT_1_1, MaxMessageSize, …
using namespace fix::generated::messages; // FixMessageHandler, LogonDecoder, …
using seq::nowMs;
using seq::CONN_ID_PREFIX;

// ── Constants ─────────────────────────────────────────────────────────────────

static constexpr uint16_t    FIX_TCP_PORT              = 9000;
static constexpr int         FIX_TCP_BACKLOG            = 8;
static constexpr const char* ARCHIVE_CONTROL_CHANNEL   = "aeron:udp?endpoint=localhost:9301";
static constexpr int32_t     ARCHIVE_CONTROL_STREAM    = 100;
static constexpr const char* ARCHIVE_RESPONSE_CHANNEL  = "aeron:udp?endpoint=localhost:0";
static constexpr const char* FIX_REPLAY_CHANNEL        = "aeron:udp?endpoint=localhost:9310";

// Session admin SBE schema (sbe-session.xml schemaId=100).
static constexpr uint16_t SESSION_SCHEMA_ID = 100;

// FIX byte-level helpers, sendRaw, CapturingTransport, FixSession and
// ClusterIngressHandler now live in ClusterIngressHandler.hpp so they can be
// unit tested against ClusterIngressSender's in-memory fakes (see
// ClusterIngressHandlerTest.cpp) without a real media driver or TCP socket.
using seq::ClusterIngressSender;
using seq::findFixMessageEnd;
using seq::fixTagRange;
using seq::patchResendFlags;
using seq::sendRaw;
using seq::CapturingTransport;
using seq::FixSession;
using seq::ClusterIngressHandler;

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
// up to the current recording position, filtering for application messages
// (MsgType=ExecutionReport) that this connId submitted and whose MsgSeqNum
// (tag 34) is in `missing`. This is a synchronous, bounded, occasional
// slow-path operation — resends are rare, so a full scan-and-filter is
// preferred here over maintaining a seqNum→archive-position index.
static std::unordered_map<uint32_t, std::vector<uint8_t>> replayMissingAppMessages(
    ArchiveResendContext& ctx, int32_t connId, const std::unordered_set<uint32_t>& missing)
{
    std::unordered_map<uint32_t, std::vector<uint8_t>> found;
    if (missing.empty() || !ctx.archive) return found;

    const std::int64_t upToPosition = ctx.archive->getRecordingPosition(ctx.recordingId);
    if (upToPosition <= 0) return found;

    static constexpr const char* RESEND_REPLAY_CHANNEL = "aeron:udp?endpoint=localhost:9312";

    aeron::archive::client::ReplayParams replayParams;
    replayParams.position(0).length(upToPosition);
    const std::int64_t replaySessionId = ctx.archive->startReplay(
        ctx.recordingId, RESEND_REPLAY_CHANNEL, seq::REPLAY_STREAM_ID, replayParams);

    bool done = false;
    seq::GlobalStreamClient scan(
        [&](const seq::SequencedEvent& e)
        {
            if (e.payloadLength <= seq::APP_MSG_SBE_PREFIX + CONN_ID_PREFIX) return;
            const auto* p = reinterpret_cast<const uint8_t*>(e.payload) + seq::APP_MSG_SBE_PREFIX;

            int32_t msgConnId;
            std::memcpy(&msgConnId, p, CONN_ID_PREFIX);
            if (msgConnId != connId) return;

            const uint8_t* fixBytes = p + CONN_ID_PREFIX;
            const auto     fixLen   = static_cast<std::size_t>(
                e.payloadLength - seq::APP_MSG_SBE_PREFIX - CONN_ID_PREFIX);
            if (fixLen == 0) return;

            const auto [mts, mte] = fixTagRange(fixBytes, fixLen, "35");
            if (mts == std::string::npos || mte - mts != 1 || fixBytes[mts] != '8') return;

            const auto [vs, ve] = fixTagRange(fixBytes, fixLen, "34");
            if (vs == std::string::npos) return;
            uint32_t seqNum = 0;
            for (std::size_t i = vs; i < ve; ++i) seqNum = seqNum * 10 + (fixBytes[i] - '0');

            if (missing.contains(seqNum) && !found.contains(seqNum))
                found.emplace(seqNum, std::vector<uint8_t>(fixBytes, fixBytes + fixLen));
        },
        nullptr, nullptr,
        [&] { done = true; });

    scan.start(ctx.aeron, replaySessionId, upToPosition, RESEND_REPLAY_CHANNEL);

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (!done && found.size() < missing.size() && std::chrono::steady_clock::now() < deadline)
    {
        if (scan.poll() == 0) std::this_thread::yield();
    }

    return found;
}

// ── FixConnection ─────────────────────────────────────────────────────────────
//
// TCP receive path:  ClusterIngressHandler encodes every FIX message as SBE
//                   (admin) or raw FIX (app) and forwards to the cluster.
//                   Session state is NOT updated here.
//
// Global stream path: FixConnection::onClusterAdmin() and onClusterAppMessage()
//                     are called by the main loop when a SequencedMessage
//                     arrives that was sourced by this connection (matched by
//                     the embedded connId). They advance the session clock to
//                     the cluster timestamp; application messages are also
//                     delivered to TCP and cached here (see ResendCache),
//                     never at the point they are locally encoded.

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
                      .transport(CapturingTransport{fd_, ingress, fd_})
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
        if (!session.isActive()) return;

        const uint32_t savedNext = session.nextOutgoingSeqNum();
        const uint32_t limit     = (end == 0 || end + 1 >= savedNext)
                                   ? savedNext : end + 1;

        std::printf("[Resend] fd=%d begin=%u end=%u limit=%u cacheSize=%zu\n",
                    fd, begin, end, limit, m_resendCache.size());

        std::unordered_set<uint32_t> missing;
        for (uint32_t seq = begin; seq < limit; ++seq)
            if (m_resendCache.get(seq) == nullptr) missing.insert(seq);

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
                if (gapStart == 0) gapStart = seq;
                continue;
            }

            if (gapStart != 0)
            {
                std::printf("[Resend] fd=%d GapFill [%u, %u)\n", fd, gapStart, seq);
                session.sendGapFill(gapStart, seq, clusterTs);
                gapStart = 0;
            }

            // Patch PossDupFlag + OrigSendingTime into a copy and send raw
            std::vector<uint8_t> patched = *bytes;
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

    // Global stream: an outbound application message (ExecutionReport) this
    // connection submitted to the cluster has been sequenced and echoed back.
    // Echoes of inbound client messages (e.g. NewOrderSingle, also submitted to
    // the cluster for total ordering) share the same connId but are not ours to
    // deliver or cache, so anything other than MsgType=ExecutionReport ('8') is
    // ignored here.
    void onClusterAppMessage(const uint8_t* fixBytes, std::size_t len, int64_t clusterTimestampMs)
    {
        const auto [mts, mte] = fixTagRange(fixBytes, len, "35");
        if (mts == std::string::npos || mte - mts != 1 || fixBytes[mts] != '8') return;

        session.setNowMs(clusterTimestampMs);

        const auto [vs, ve] = fixTagRange(fixBytes, len, "34");
        if (vs == std::string::npos) return;
        uint32_t seqNum = 0;
        for (std::size_t i = vs; i < ve; ++i) seqNum = seqNum * 10 + (fixBytes[i] - '0');
        if (seqNum == 0) return;

        m_resendCache.put(seqNum, std::span<const uint8_t>(fixBytes, len));
        sendRaw(fd, fixBytes, len);
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

    // Shared with every FixConnection for the on-demand resend archive fallback.
    ArchiveResendContext resendArchiveCtx{aeron, archive, recordingId};

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

    // ── Global stream subscription ────────────────────────────────────────────
    // Payload layout after the 10-byte AppMessage SBE prefix:
    //   [4-byte connId LE][admin SBE body | raw FIX bytes]
    //   Every message this gateway submits (admin or application) is prefixed
    //   with the 4-byte connId of the TCP connection that originated it (see
    //   ClusterIngressSender::send), so the echo can always be routed back to
    //   the right FixConnection. Admin vs. application is then distinguished
    //   by schemaId==SESSION_SCHEMA_ID at the start of the remaining bytes.
    seq::GlobalStreamClient globalStream(
        [&](const seq::SequencedEvent& e)
        {
            std::printf("[Global] SequencedEvent globalSeq=%" PRId64 " payloadLen=%" PRIu64 "\n",
                        e.globalSeqNo, static_cast<uint64_t>(e.payloadLength));
            if (e.payloadLength <= seq::APP_MSG_SBE_PREFIX + CONN_ID_PREFIX) return;
            const auto* payload = reinterpret_cast<const uint8_t*>(e.payload)
                                  + seq::APP_MSG_SBE_PREFIX;
            const uint64_t payloadLen = e.payloadLength - seq::APP_MSG_SBE_PREFIX;

            int32_t connId;
            std::memcpy(&connId, payload, CONN_ID_PREFIX);
            const uint8_t* rest    = payload + CONN_ID_PREFIX;
            const uint64_t restLen = payloadLen - CONN_ID_PREFIX;

            auto it = connections.find(connId);
            if (it == connections.end()) return;

            // Detect admin SBE: schemaId is at rest[4..5] (bytes 4-5 of the SBE
            // MessageHeader that opens the remaining bytes).
            if (restLen >= sbesess::MessageHeader::encodedLength())
            {
                uint16_t schemaId;
                std::memcpy(&schemaId, rest + 4, sizeof(uint16_t));
                schemaId = SBE_LITTLE_ENDIAN_ENCODE_16(schemaId);  // no-op on LE; bswap on BE

                if (schemaId == SESSION_SCHEMA_ID)
                {
                    std::printf("[Global] Admin SBE connId=%d connections.size=%zu\n",
                                connId, connections.size());

                    // Read SBE MessageHeader fields.
                    uint16_t blockLen, templateId, version;
                    std::memcpy(&blockLen,   rest + 0, 2);
                    std::memcpy(&templateId, rest + 2, 2);
                    std::memcpy(&version,    rest + 6, 2);
                    blockLen   = SBE_LITTLE_ENDIAN_ENCODE_16(blockLen);
                    templateId = SBE_LITTLE_ENDIAN_ENCODE_16(templateId);
                    version    = SBE_LITTLE_ENDIAN_ENCODE_16(version);

                    it->second->onClusterAdmin(templateId, reinterpret_cast<const char*>(rest),
                                               restLen, blockLen, version, e.clusterTimestamp);
                    return;
                }
            }

            // Application-layer FIX bytes: our own ExecutionReport echoed back
            // (see FixConnection::onClusterAppMessage), or the client's own
            // NewOrderSingle echoed back (ignored there).
            if (restLen > 0 && rest[0] == '8')
            {
                it->second->onClusterAppMessage(rest, restLen, e.clusterTimestamp);
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
    ingressSender.close();
    return 0;
}
