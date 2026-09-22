#pragma once

// ClusterStreamSender — Aeron Cluster client session state machine
// (SessionConnectRequest → SessionEvent(OK) → send/keep-alive → SessionCloseRequest).
//
// The session logic talks to the cluster only through IngressTransport/EgressTransport, so a test drives
// it with in-memory fakes; connect(aeron) acquires the Aeron resources and delegates to the
// transport-agnostic connect(). On REDIRECT or NewLeaderEvent it resolves the leader's endpoint from the
// wire's "memberId=host:port,..." CSV and swaps its ingress publication; the session id is unchanged.
// Reconnection needs a real Aeron client, so the test seam leaves it disabled.

#include <array>
#include <charconv>
#include <chrono>
#include <cinttypes>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <functional>
#include <memory>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>

#include "Aeron.h"
#include "FragmentAssembler.h"
#include "concurrent/AtomicBuffer.h"
#include "concurrent/YieldingIdleStrategy.h"
#include "org/limitless/seqeron/protocol/PortLayout.hpp"
#include "org/limitless/seqeron/sequencer/client/IngressTracker.hpp"
#include "org/limitless/seqeron/util/Logger.hpp"
#include "org_limitless_seqeron_cluster_sbe/MessageHeader.h"
#include "org_limitless_seqeron_cluster_sbe/NewLeaderEvent.h"
#include "org_limitless_seqeron_cluster_sbe/SessionCloseRequest.h"
#include "org_limitless_seqeron_cluster_sbe/SessionConnectRequest.h"
#include "org_limitless_seqeron_cluster_sbe/SessionEvent.h"
#include "org_limitless_seqeron_cluster_sbe/SessionKeepAlive.h"
#include "org_limitless_seqeron_cluster_sbe/SessionMessageHeader.h"

namespace org::limitless::seqeron::sequencer::client {

// ── Constants — cluster channels, stream ids and client protocol semver, per io.aeron.cluster.codecs
//    and AeronCluster.Configuration. Ports come from PortLayout.hpp. Member 0's ingress endpoint is only
//    a non-colocated client's first guess; the wire CSV names the real leader afterwards. ────
inline const std::string CLUSTER_INGRESS_ENDPOINT = "localhost:" + std::to_string(protocol::clusterIngressPort(0));
inline const std::string CLUSTER_INGRESS_CHANNEL = protocol::udpChannel(CLUSTER_INGRESS_ENDPOINT);
inline constexpr const char* CLUSTER_INGRESS_CHANNEL_IPC = "aeron:ipc";
inline constexpr std::int32_t CLUSTER_INGRESS_STREAM_ID = 101;
inline constexpr std::int32_t CLUSTER_EGRESS_STREAM_ID = 102;
inline constexpr std::int32_t CLUSTER_PROTOCOL_VERSION = (0 << 16) | (3 << 8) | 0; // 0.3.0
inline constexpr const char* CLUSTER_CLIENT_INFO = "seqeron";
inline constexpr std::int64_t CLUSTER_CONNECT_TIMEOUT_MS = 10'000;

inline std::int64_t nowMs()
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
        .count();
}

// A member's ingress endpoint by id, for SessionEvent(OK), which names the leader but carries no CSV.
// Assumes a single host, as PortLayout does.
inline std::string memberIngressEndpoint(const std::int32_t memberId)
{
    return "localhost:" + std::to_string(protocol::clusterIngressPort(memberId));
}

// Finds `memberId`'s endpoint in a "memberId=host:port,memberId=host:port,..." CSV, the wire
// format both SessionEvent.detail (on REDIRECT) and NewLeaderEvent.ingressEndpoints use.
// Returns false (leaving `out` untouched) if the CSV has no entry for that member.
inline bool findIngressEndpoint(std::string_view endpoints, std::int32_t memberId, std::string& out)
{
    std::size_t start = 0;
    while (start <= endpoints.size())
    {
        const std::size_t comma = endpoints.find(',', start);
        const std::string_view entry =
            endpoints.substr(start, comma == std::string_view::npos ? std::string_view::npos : comma - start);

        const std::size_t eq = entry.find('=');
        if (eq != std::string_view::npos)
        {
            std::int32_t parsedId = -1;
            const std::string_view idPart = entry.substr(0, eq);
            const auto res = std::from_chars(idPart.data(), idPart.data() + idPart.size(), parsedId);
            if (res.ec == std::errc() && parsedId == memberId)
            {
                out.assign(entry.substr(eq + 1));
                return true;
            }
        }

        if (comma == std::string_view::npos)
        {
            break;
        }
        start = comma + 1;
    }
    return false;
}

// ── Transport interfaces ──────────────────────────────────────────────────────

// Outbound half: offers raw bytes to the cluster ingress; `false` means not yet accepted.
class IngressTransport
{
  public:
    virtual ~IngressTransport() = default;
    virtual bool offer(std::span<const std::uint8_t> bytes) = 0;
};

// Inbound half: polls whole (reassembled) egress messages; returns fragments processed.
class EgressTransport
{
  public:
    using FragmentHandler = std::function<void(std::span<const std::uint8_t>)>;

    virtual ~EgressTransport() = default;
    virtual int poll(const FragmentHandler& handler) = 0;
};

// ── Real Aeron-backed transports ──────────────────────────────────────────────

class AeronIngressTransport : public IngressTransport
{
  public:
    explicit AeronIngressTransport(std::shared_ptr<aeron::Publication> pub) : m_pub(std::move(pub))
    {}

    bool offer(std::span<const std::uint8_t> bytes) override
    {
        const aeron::concurrent::AtomicBuffer buffer(const_cast<std::uint8_t*>(bytes.data()),
                                                     static_cast<aeron::util::index_t>(bytes.size()));
        return m_pub->offer(buffer, 0, static_cast<aeron::util::index_t>(bytes.size())) >= 0;
    }

  private:
    std::shared_ptr<aeron::Publication> m_pub;
};

class AeronEgressTransport : public EgressTransport
{
  public:
    explicit AeronEgressTransport(std::shared_ptr<aeron::Subscription> sub) : m_sub(std::move(sub))
    {}

    int poll(const FragmentHandler& handler) override
    {
        m_handler = &handler;
        const int n = m_sub->poll(m_poll, 10);
        m_handler = nullptr;
        return n;
    }

  private:
    std::shared_ptr<aeron::Subscription> m_sub;

    // Per-call callback set in poll(); null outside of that call.
    const FragmentHandler* m_handler{ nullptr };

    // Persistent across poll() calls so multi-fragment messages reassemble correctly.
    aeron::FragmentAssembler m_fa{ [this](aeron::concurrent::AtomicBuffer& buf, aeron::util::index_t off,
                                          aeron::util::index_t len, aeron::Header&) {
        if (m_handler)
        {
            (*m_handler)(std::span<const std::uint8_t>(reinterpret_cast<const std::uint8_t*>(buf.buffer()) + off,
                                                       static_cast<std::size_t>(len)));
        }
    } };

    // Composed once rather than per poll(): FragmentAssembler::handler() returns a fresh std::function.
    // Declared after m_fa, which it is built from.
    aeron::fragment_handler_t m_poll{ m_fa.handler() };
};

// ── ClusterStreamSender ──────────────────────────────────────────────────────

// Manages the Aeron Cluster session and sends pre-encoded frames to the cluster ingress. Each frame
// carries its own header composite, sourceId included.
class ClusterStreamSender
{
  public:
    static constexpr std::size_t MAX_PAYLOAD_LEN = 8192;

    // Acquires the ingress publication and egress subscription, then runs the handshake below. Storing
    // `aeron` is what enables reconnection on REDIRECT/NewLeaderEvent.
    void connect(std::shared_ptr<aeron::Aeron> aeron, const std::string& egressChannel)
    {
        m_aeron = std::move(aeron);
        m_ingressEndpoint = CLUSTER_INGRESS_ENDPOINT;
        m_egressChannel = egressChannel;

        auto egress = std::make_unique<AeronEgressTransport>(awaitEgressSubscription());
        connect(
            std::make_unique<AeronIngressTransport>(createIngressPublication(m_ingressEndpoint, m_connectTimeoutMs)),
            std::move(egress), m_egressChannel);
    }

    // For a client sharing a cluster member's Aeron directory. Ingress tries that member's IPC first: a
    // follower never opens IPC ingress, so the request goes unanswered, hence the short
    // ipcConnectTimeoutMs before falling back to UDP, where a follower redirects to the leader. Leadership
    // later moving away degrades to UDP; moving back to `memberId` is re-chased onto IPC (see onFragment).
    void connectColocated(std::shared_ptr<aeron::Aeron> aeron, std::int32_t memberId, std::int64_t ipcConnectTimeoutMs,
                          const std::string& egressChannel)
    {
        m_aeron = std::move(aeron);
        m_coLocatedMemberId = memberId;
        m_ipcConnectTimeoutMs = ipcConnectTimeoutMs;
        // Must be a distinct UDP endpoint per co-located client: two drivers on one host cannot both
        // bind one egress port.
        m_egressChannel = egressChannel;

        auto egress = std::make_unique<AeronEgressTransport>(awaitEgressSubscription());

        // A follower never connects the IPC publication, so building it is bounded by the short
        // ipcConnectTimeoutMs; a timeout there is treated like a failed handshake on it.
        m_ingressEndpoint = "ipc";
        std::unique_ptr<IngressTransport> primary;
        std::string primaryFailureReason;
        try
        {
            primary = std::make_unique<AeronIngressTransport>(createIpcIngressPublication(ipcConnectTimeoutMs));
        }
        catch (const std::exception& ex)
        {
            primaryFailureReason = ex.what();
        }

        connectColocated(
            std::move(primary),
            [this] {
                m_ingressEndpoint = CLUSTER_INGRESS_ENDPOINT;
                return std::make_unique<AeronIngressTransport>(
                    createIngressPublication(m_ingressEndpoint, m_connectTimeoutMs));
            },
            std::move(egress), ipcConnectTimeoutMs, primaryFailureReason.c_str(), memberId);
    }

    // Test seam for connectColocated: try `primaryIngress` with the short timeout, fall back to
    // `buildFallbackIngress` with the full one. `primaryIngress` may be null to go straight to the
    // fallback; `egress` is shared by both attempts. Without m_aeron the NewLeaderEvent re-chase stays a
    // no-op, so `memberId` only sets the state a test inspects.
    void connectColocated(std::unique_ptr<IngressTransport> primaryIngress,
                          std::function<std::unique_ptr<IngressTransport>()> buildFallbackIngress,
                          std::unique_ptr<EgressTransport> egress, std::int64_t primaryConnectTimeoutMs,
                          const char* primaryFailureReason, std::int32_t memberId = -1)
    {
        m_coLocatedMemberId = memberId;
        const std::int64_t fullTimeoutMs = m_connectTimeoutMs;
        std::string reasonStorage; // outlives the catch block, unlike ex.what()'s pointer

        if (primaryIngress)
        {
            m_connectTimeoutMs = primaryConnectTimeoutMs;
            try
            {
                connect(std::move(primaryIngress), std::move(egress), m_egressChannel);
                m_connectTimeoutMs = fullTimeoutMs;
                return;
            }
            catch (const std::exception& ex)
            {
                reasonStorage = ex.what();
                primaryFailureReason = reasonStorage.c_str();
                egress = std::move(m_egress); // connect() already stashed it in m_egress before failing
            }
        }

        util::Logger::info(util::component::Cluster, "Co-located member not leader (%s) — falling back to UDP ingress",
                           primaryFailureReason != nullptr ? primaryFailureReason : "unknown");
        m_connectTimeoutMs = fullTimeoutMs;
        connect(buildFallbackIngress(), std::move(egress), m_egressChannel);
    }

    // Test seam: drives the handshake against any transport pair, synchronously and without Aeron.
    // Redirect/reconnect is skipped here (no Aeron client to build a publication with). `egressChannel`
    // is the responseChannel the cluster publishes egress on.
    void connect(std::unique_ptr<IngressTransport> ingress, std::unique_ptr<EgressTransport> egress,
                 const std::string& egressChannel = "")
    {
        m_ingress = std::move(ingress);
        m_egress = std::move(egress);
        m_egressChannel = egressChannel;

        sendConnectRequest();

        const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(m_connectTimeoutMs);

        auto onEgress = [this](std::span<const std::uint8_t> bytes) {
            if (bytes.size() < cluster::sbe::MessageHeader::encodedLength())
            {
                return;
            }
            cluster::sbe::MessageHeader hdr;
            hdr.wrap(reinterpret_cast<char*>(const_cast<std::uint8_t*>(bytes.data())), 0, 0, bytes.size());
            if (hdr.templateId() != cluster::sbe::SessionEvent::sbeTemplateId())
            {
                return;
            }

            cluster::sbe::SessionEvent evt;
            evt.wrapForDecode(reinterpret_cast<char*>(const_cast<std::uint8_t*>(bytes.data())),
                              cluster::sbe::MessageHeader::encodedLength(), hdr.blockLength(), hdr.version(),
                              bytes.size());
            if (evt.code() == cluster::sbe::EventCode::Value::OK)
            {
                m_clusterSessionId = evt.clusterSessionId();
                m_leadershipTermId = evt.leadershipTermId();
                util::Logger::info(
                    util::component::Cluster,
                    "Session opened  sessionId=%" PRId64 "  termId=%" PRId64 "  leader=%d  via ingress %s",
                    m_clusterSessionId, m_leadershipTermId, evt.leaderMemberId(), m_ingressEndpoint.c_str());
                ensureIngressTargetsLeader(evt.leaderMemberId());
            }
            else if (evt.code() == cluster::sbe::EventCode::Value::REDIRECT)
            {
                handleRedirect(evt);
            }
            else
            {
                util::Logger::error(util::component::Cluster, util::eventCode::ClusterSessionError,
                                    "SessionEvent error code=%d", static_cast<int>(evt.code()));
                if (m_clusterSessionId >= 0 && evt.clusterSessionId() == m_clusterSessionId)
                {
                    m_clusterSessionId = -1;
                }
            }
        };

        while (m_clusterSessionId < 0 && std::chrono::steady_clock::now() < deadline)
        {
            const int fragments = m_egress->poll(onEgress);
            applyPendingIngressSwitch(); // a REDIRECT in that batch; never build inside poll()
            m_idleStrategy.idle(fragments);
        }

        if (m_clusterSessionId < 0)
        {
            throw std::runtime_error("[ClusterStreamSender] Timed out waiting for cluster session");
        }
    }

    // Overrides the connect handshake timeout (default 10s), for tests.
    void setConnectTimeoutMs(std::int64_t ms)
    {
        m_connectTimeoutMs = ms;
    }

    // Overrides send()'s give-up bound (default INGRESS_STALL_FATAL_TIMEOUT_MS), for tests.
    void setIngressStallTimeoutMs(std::int64_t ms)
    {
        m_ingressStallFatalTimeoutMs = ms;
    }

    // Hears every NewLeaderEvent, and gives up a send that met one while it holds. Not owned.
    void setIngressHold(IngressTracker* hold)
    {
        m_hold = hold;
    }

    bool isConnected() const
    {
        return m_clusterSessionId >= 0;
    }

    // This connection's cluster session id, or -1 if not connected. Stamped into a frame's header.sessionId.
    std::int64_t clusterSessionId() const
    {
        return m_clusterSessionId;
    }

    // The leadership term ingress is stamped with, or -1 if not yet connected. Read straight after a
    // send() that returned true, it is the term that frame carried: send() re-stamps on every retry.
    std::int64_t leadershipTermId() const
    {
        return m_leadershipTermId;
    }

    // True once the cluster has closed this session, as opposed to never having opened one. Latched:
    // there is no re-handshake.
    [[nodiscard]] bool isSessionLost() const noexcept
    {
        return m_sessionLost;
    }

    // Sends a keep-alive if the interval has elapsed; call it every duty-cycle iteration.
    void keepAlive()
    {
        if (!m_ingress || m_clusterSessionId < 0)
        {
            return;
        }
        const std::int64_t now = nowMs();
        if (now - m_lastKeepAliveMs < KEEP_ALIVE_INTERVAL_MS)
        {
            return;
        }
        m_lastKeepAliveMs = now;

        alignas(16) std::array<std::uint8_t, 64> kaBuf{};
        cluster::sbe::SessionKeepAlive ka;
        ka.wrapAndApplyHeader(reinterpret_cast<char*>(kaBuf.data()), 0, kaBuf.size())
            .leadershipTermId(m_leadershipTermId)
            .clusterSessionId(m_clusterSessionId);
        if (!m_ingress->offer(std::span<const std::uint8_t>(kaBuf.data(), static_cast<std::size_t>(ka.sbePosition()))))
        {
            util::Logger::error(util::component::Cluster, util::eventCode::ClusterOfferFailed,
                                "keep-alive offer failed");
        }
    }

    // Sends a SessionCloseRequest and locally forgets the session. Best-effort:
    // the cluster also expires unresponsive sessions via keep-alive timeout.
    void close()
    {
        if (!m_ingress || m_clusterSessionId < 0)
        {
            return;
        }

        alignas(16) std::array<std::uint8_t, 64> buf{};
        cluster::sbe::SessionCloseRequest req;
        req.wrapAndApplyHeader(reinterpret_cast<char*>(buf.data()), 0, buf.size())
            .leadershipTermId(m_leadershipTermId)
            .clusterSessionId(m_clusterSessionId);
        if (!m_ingress->offer(std::span<const std::uint8_t>(buf.data(), static_cast<std::size_t>(req.sbePosition()))))
        {
            util::Logger::error(util::component::Cluster, util::eventCode::ClusterOfferFailed, "close offer failed");
        }

        m_clusterSessionId = -1;
    }

    // Drain cluster egress; calls onAppMessage for each application-layer response.
    void pollEgress(const std::function<void(const std::uint8_t*, std::int32_t)>& onAppMessage)
    {
        if (!m_egress)
        {
            return;
        }
        m_egress->poll([this, &onAppMessage](std::span<const std::uint8_t> bytes) { onFragment(bytes, onAppMessage); });
        applyPendingIngressSwitch(); // a NewLeaderEvent/REDIRECT in that batch; never build inside poll()
    }

    // Wraps a pre-encoded frame in a SessionMessageHeader and offers it, spinning until it lands. The spin
    // pumps egress itself: a leader change leaves the publication not-connected until a
    // NewLeaderEvent/REDIRECT swaps it, and that swap runs on this thread. The term and timestamp are
    // re-stamped before each retry, as AeronCluster.offer() does, since the new leader drops a stale term.
    //
    // Returns false with no session, or when a NewLeaderEvent arrived mid-spin while the IngressTracker
    // holds: nothing was placed, and the frame goes again once it releases.
    [[nodiscard]] bool send(const std::uint8_t* bytes, std::uint16_t len)
    {
        if (!m_ingress || m_clusterSessionId < 0 || len == 0)
        {
            return false;
        }
        // buf is sized from MAX_PAYLOAD_LEN like every caller's encode buffer, so this is a programming
        // error; dropping the frame silently would open a hole.
        if (len > MAX_PAYLOAD_LEN)
        {
            throw std::runtime_error("[ClusterStreamSender] ingress payload " + std::to_string(len) +
                                     " exceeds MAX_PAYLOAD_LEN " + std::to_string(MAX_PAYLOAD_LEN));
        }

        alignas(16) std::array<std::uint8_t, INGRESS_FRAME_LEN> buf{};
        cluster::sbe::SessionMessageHeader hdr;
        hdr.wrapAndApplyHeader(reinterpret_cast<char*>(buf.data()), 0, buf.size())
            .leadershipTermId(m_leadershipTermId)
            .clusterSessionId(m_clusterSessionId)
            .timestamp(nowMs());
        const std::int32_t hdrLen = static_cast<std::int32_t>(hdr.sbePosition());
        std::memcpy(buf.data() + hdrLen, bytes, len);
        const std::size_t frameLen = static_cast<std::size_t>(hdrLen) + len;

        // Bounded: both other exits need a leader, so losing quorum would otherwise stop the caller's whole
        // duty cycle silently. Past the bound the session is called lost and the caller's isSessionLost()
        // fence takes over. No keep-alive from in here: it would offer on the same stuck publication.
        std::chrono::steady_clock::time_point blockedSince{};
        std::chrono::steady_clock::time_point nextAlert{};
        m_newLeaderDuringSend = false;
        while (!m_ingress->offer(std::span<const std::uint8_t>(buf.data(), frameLen)))
        {
            pumpEgressControl();
            if (m_clusterSessionId < 0)
            {
                return false;
            }
            if (m_newLeaderDuringSend && m_hold && m_hold->isHolding())
            {
                return false;
            }
            const auto now = std::chrono::steady_clock::now();
            if (blockedSince == std::chrono::steady_clock::time_point{})
            {
                blockedSince = now; // first refusal only anchors the period; the common case never gets here
                nextAlert = now + INGRESS_BACKPRESSURE_ALERT_INTERVAL;
            }
            const auto blockedMs = static_cast<std::int64_t>(
                std::chrono::duration_cast<std::chrono::milliseconds>(now - blockedSince).count());
            if (blockedMs >= m_ingressStallFatalTimeoutMs)
            {
                util::Logger::error(util::component::Cluster, util::eventCode::ClusterSessionError,
                                    "no leader has accepted cluster ingress for %" PRId64 "ms — giving the session "
                                    "up rather than spin on with the duty cycle stopped",
                                    blockedMs);
                m_clusterSessionId = -1;
                m_sessionLost = true;
                return false;
            }
            if (now >= nextAlert)
            {
                nextAlert = now + INGRESS_BACKPRESSURE_ALERT_INTERVAL;
                util::Logger::error(util::component::Cluster, util::eventCode::ClusterOfferFailed,
                                    "cluster ingress has refused this frame for %" PRId64 "ms — still retrying",
                                    blockedMs);
            }
            m_idleStrategy.idle();
            hdr.leadershipTermId(m_leadershipTermId).timestamp(nowMs()); // re-stamp for the (possibly new) leader
        }
        return true;
    }

  private:
    // Paired with the cluster's sessionTimeoutNs (1s) at a 5x margin; raising this alone reaps healthy
    // sessions.
    static constexpr std::int64_t KEEP_ALIVE_INTERVAL_MS = 200;

    // How long send() tolerates continuous refusal before calling the session lost: 2x the slowest election
    // timeout SequencerServer configures, while still bounding an outage no election will end.
    static constexpr std::int64_t INGRESS_STALL_FATAL_TIMEOUT_MS = 10'000;

    // How often a continuously refused offer is alerted on.
    static constexpr auto INGRESS_BACKPRESSURE_ALERT_INTERVAL = std::chrono::milliseconds(1'000);

    // send()'s framing buffer: the largest payload plus the SessionMessageHeader envelope it goes in.
    static constexpr std::size_t INGRESS_FRAME_LEN =
        MAX_PAYLOAD_LEN + cluster::sbe::SessionMessageHeader::sbeBlockAndHeaderLength();

    // Polls egress for session-control frames only (SessionEvent, NewLeaderEvent, REDIRECT), discarding
    // application payloads, so a failover completes mid-send. Safe: cluster egress carries nothing else here.
    void pumpEgressControl()
    {
        if (m_egress)
        {
            m_egress->poll([this](std::span<const std::uint8_t> bytes) {
                onFragment(bytes, [](const std::uint8_t*, std::int32_t) {});
            });
            // Let send()'s spin pick up the new leader; the swap must happen after poll() returns.
            applyPendingIngressSwitch();
        }
    }

    void onFragment(std::span<const std::uint8_t> bytes,
                    const std::function<void(const std::uint8_t*, std::int32_t)>& onAppMessage)
    {
        if (bytes.size() < cluster::sbe::MessageHeader::encodedLength())
        {
            return;
        }
        cluster::sbe::MessageHeader hdr;
        hdr.wrap(reinterpret_cast<char*>(const_cast<std::uint8_t*>(bytes.data())), 0, 0, bytes.size());

        if (hdr.templateId() == cluster::sbe::SessionEvent::sbeTemplateId())
        {
            cluster::sbe::SessionEvent evt;
            evt.wrapForDecode(reinterpret_cast<char*>(const_cast<std::uint8_t*>(bytes.data())),
                              cluster::sbe::MessageHeader::encodedLength(), hdr.blockLength(), hdr.version(),
                              bytes.size());
            // The cluster has closed our session
            if (m_clusterSessionId >= 0 && evt.clusterSessionId() == m_clusterSessionId &&
                evt.code() != cluster::sbe::EventCode::Value::OK)
            {
                // Every close reason is trusted, TIMEOUT included.
                const std::string detail = evt.getDetailAsString();
                util::Logger::error(util::component::Cluster, util::eventCode::ClusterSessionError,
                                    "Cluster closed session %" PRId64 " (code=%d, %s)", m_clusterSessionId,
                                    static_cast<int>(evt.code()), detail.c_str());
                m_clusterSessionId = -1;
                m_sessionLost = true;
            }
            return;
        }
        if (hdr.templateId() == cluster::sbe::NewLeaderEvent::sbeTemplateId())
        {
            cluster::sbe::NewLeaderEvent evt;
            evt.wrapForDecode(reinterpret_cast<char*>(const_cast<std::uint8_t*>(bytes.data())),
                              cluster::sbe::MessageHeader::encodedLength(), hdr.blockLength(), hdr.version(),
                              bytes.size());
            m_leadershipTermId = evt.leadershipTermId();
            m_newLeaderDuringSend = true;
            if (m_hold)
            {
                m_hold->onNewLeader(m_leadershipTermId);
            }
            const std::int32_t leaderMemberId = evt.leaderMemberId();
            const std::string ingressEndpoints = evt.getIngressEndpointsAsString();

            // Leadership has returned to our own co-located member while we're parked on UDP
            if (m_aeron && m_coLocatedMemberId >= 0 && leaderMemberId == m_coLocatedMemberId &&
                m_ingressEndpoint != "ipc")
            {
                util::Logger::info(util::component::Cluster,
                                   "New leader  termId=%" PRId64
                                   "  member=%d is co-located — switching back to IPC ingress",
                                   m_leadershipTermId, leaderMemberId);
                m_pendingIngress = PendingIngressSwitch{
                    .pending = true, .endpoint = "ipc", .timeoutMs = m_ipcConnectTimeoutMs, .keepCurrentOnFailure = true
                };
                return;
            }

            std::string endpoint;
            if (m_aeron && findIngressEndpoint(ingressEndpoints, leaderMemberId, endpoint) &&
                endpoint != m_ingressEndpoint)
            {
                util::Logger::info(util::component::Cluster, "New leader  termId=%" PRId64 "  member=%d  endpoint=%s",
                                   m_leadershipTermId, leaderMemberId, endpoint.c_str());
                m_pendingIngress =
                    PendingIngressSwitch{ .pending = true, .endpoint = endpoint, .timeoutMs = m_connectTimeoutMs };
            }
            else
            {
                util::Logger::info(util::component::Cluster, "New leader  termId=%" PRId64, m_leadershipTermId);
            }
            return;
        }
        if (hdr.templateId() != cluster::sbe::SessionMessageHeader::sbeTemplateId())
        {
            return;
        }

        const std::size_t appOff =
            cluster::sbe::MessageHeader::encodedLength() + static_cast<std::size_t>(hdr.blockLength());
        if (bytes.size() <= appOff)
        {
            return;
        }
        onAppMessage(bytes.data() + appOff, static_cast<std::int32_t>(bytes.size() - appOff));
    }

    // Encodes and offers a SessionConnectRequest: the initial handshake, and the re-announce after a REDIRECT.
    void sendConnectRequest()
    {
        alignas(16) std::array<std::uint8_t, 512> connBuf{};
        cluster::sbe::SessionConnectRequest req;
        req.wrapAndApplyHeader(reinterpret_cast<char*>(connBuf.data()), 0, connBuf.size());
        req.correlationId(m_correlationId).responseStreamId(CLUSTER_EGRESS_STREAM_ID).version(CLUSTER_PROTOCOL_VERSION);
        req.putResponseChannel(std::string_view(m_egressChannel));
        req.putEncodedCredentials(nullptr, 0);
        req.putClientInfo(std::string_view(CLUSTER_CLIENT_INFO));

        while (!m_ingress->offer(
            std::span<const std::uint8_t>(connBuf.data(), static_cast<std::size_t>(req.sbePosition()))))
        {
            m_idleStrategy.idle();
        }
    }

    // A follower rejected our SessionConnectRequest, pointing us at the real leader. Swap the
    // ingress Publication to the leader's endpoint and re-announce.
    void ensureIngressTargetsLeader(const std::int32_t leaderMemberId)
    {
        // m_aeron is null in the test seam: nothing to build a publication with.
        if (!m_aeron || leaderMemberId < 0)
        {
            return;
        }
        // IPC ingress is Aeron's leader-only listener (SequencerServer's isIpcIngressAllowed), so being on
        // it at all already proves the co-located member leads.
        if (m_ingressEndpoint == "ipc")
        {
            return;
        }
        const std::string endpoint = memberIngressEndpoint(leaderMemberId);
        if (endpoint == m_ingressEndpoint)
        {
            return;
        }

        util::Logger::info(util::component::Cluster,
                           "Session opened through non-leader ingress %s — leader is member=%d, switching to %s",
                           m_ingressEndpoint.c_str(), leaderMemberId, endpoint.c_str());
        m_pendingIngress =
            PendingIngressSwitch{ .pending = true, .endpoint = endpoint, .timeoutMs = m_connectTimeoutMs };
    }

    void handleRedirect(cluster::sbe::SessionEvent& evt)
    {
        const std::int32_t leaderMemberId = evt.leaderMemberId();
        const std::string detail = evt.getDetailAsString();

        std::string endpoint;
        if (!m_aeron || !findIngressEndpoint(detail, leaderMemberId, endpoint) || endpoint == m_ingressEndpoint)
        {
            util::Logger::error(util::component::Cluster, util::eventCode::ClusterRedirectUnresolved,
                                "Redirected to member=%d but could not resolve a new "
                                "ingress endpoint from \"%s\"",
                                leaderMemberId, detail.c_str());
            return;
        }

        util::Logger::info(util::component::Cluster, "Redirected to leader  member=%d  endpoint=%s", leaderMemberId,
                           endpoint.c_str());
        m_pendingIngress = PendingIngressSwitch{
            .pending = true, .endpoint = endpoint, .timeoutMs = m_connectTimeoutMs, .resendConnectRequest = true
        };
    }

    // Performs the ingress swap a fragment handler asked for. MUST run outside EgressTransport::poll.
    void applyPendingIngressSwitch()
    {
        if (!m_pendingIngress.pending)
        {
            return;
        }
        // Copy and clear first: a throw below must not leave the request armed to retry forever.
        const PendingIngressSwitch req = m_pendingIngress;
        m_pendingIngress = PendingIngressSwitch{};

        try
        {
            auto pub = req.endpoint == "ipc" ? createIpcIngressPublication(req.timeoutMs)
                                             : createIngressPublication(req.endpoint, req.timeoutMs);
            m_ingress = std::make_unique<AeronIngressTransport>(std::move(pub));
            m_ingressEndpoint = req.endpoint;
            util::Logger::info(util::component::Cluster, "Ingress switched to %s", req.endpoint.c_str());
            if (req.resendConnectRequest || m_clusterSessionId < 0)
            {
                sendConnectRequest();
            }
        }
        catch (const std::exception& ex)
        {
            if (req.keepCurrentOnFailure)
            {
                util::Logger::error(util::component::Cluster, util::eventCode::ClusterIpcFallback,
                                    "Ingress %s not ready yet (%s) — staying on %s", req.endpoint.c_str(), ex.what(),
                                    m_ingressEndpoint.c_str());
            }
            else
            {
                util::Logger::error(util::component::Cluster, util::eventCode::ClusterRedirectUnresolved,
                                    "Could not build ingress publication to %s (%s)", req.endpoint.c_str(), ex.what());
            }
        }
    }

    // Adds this client's egress subscription on m_egressChannel and blocks until it is resolved.
    std::shared_ptr<aeron::Subscription> awaitEgressSubscription()
    {
        const auto subId = m_aeron->addSubscription(m_egressChannel, CLUSTER_EGRESS_STREAM_ID);
        std::shared_ptr<aeron::Subscription> egressSub;
        while (!(egressSub = m_aeron->findSubscription(subId)))
        {
            m_idleStrategy.idle();
        }
        return egressSub;
    }

    // Adds a cluster-ingress publication on `channel` and blocks until it is both resolved and
    // connected, or timeoutMs elapses. `description` names it in the timeout messages.
    // Only valid when connect(aeron) was used — m_aeron is null in the transport-agnostic test seam.
    std::shared_ptr<aeron::Publication> awaitIngressPublication(const std::string& channel,
                                                                const std::string& description, std::int64_t timeoutMs)
    {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);

        const auto pubId = m_aeron->addPublication(channel, CLUSTER_INGRESS_STREAM_ID);
        std::shared_ptr<aeron::Publication> pub;
        while (!(pub = m_aeron->findPublication(pubId)))
        {
            if (std::chrono::steady_clock::now() >= deadline)
            {
                throw std::runtime_error("[ClusterStreamSender] Timed out creating " + description);
            }
            m_idleStrategy.idle();
        }
        while (!pub->isConnected())
        {
            if (std::chrono::steady_clock::now() >= deadline)
            {
                throw std::runtime_error("[ClusterStreamSender] Timed out connecting " + description);
            }
            m_idleStrategy.idle();
        }
        return pub;
    }

    // The cluster ingress at `endpoint` ("host:port").
    std::shared_ptr<aeron::Publication> createIngressPublication(const std::string& endpoint, std::int64_t timeoutMs)
    {
        return awaitIngressPublication(protocol::udpChannel(endpoint), "ingress publication to " + endpoint, timeoutMs);
    }

    // Same over IPC, which only a leading co-located member listens on, so it may never connect; the
    // deadline is what triggers connectColocated's UDP fallback.
    std::shared_ptr<aeron::Publication> createIpcIngressPublication(std::int64_t timeoutMs)
    {
        return awaitIngressPublication(CLUSTER_INGRESS_CHANNEL_IPC, "IPC ingress publication", timeoutMs);
    }

    std::shared_ptr<aeron::Aeron> m_aeron;
    std::string m_ingressEndpoint;
    std::int32_t m_coLocatedMemberId = -1;
    std::int64_t m_ipcConnectTimeoutMs = 1500;
    std::string m_egressChannel;
    std::unique_ptr<IngressTransport> m_ingress;
    std::unique_ptr<EgressTransport> m_egress;
    aeron::concurrent::YieldingIdleStrategy m_idleStrategy;

    // An ingress-publication swap requested from inside an egress fragment handler, performed later
    // by applyPendingIngressSwitch(). Never build the publication in the handler itself — see there.
    struct PendingIngressSwitch
    {
        bool pending = false;
        std::string endpoint;              // "ipc", or "host:port"
        std::int64_t timeoutMs = 0;        // deadline for building it
        bool resendConnectRequest = false; // REDIRECT re-handshakes; NewLeaderEvent keeps the session
        bool keepCurrentOnFailure = false; // NewLeaderEvent→IPC: stay on the current UDP leg if IPC isn't up
    };
    PendingIngressSwitch m_pendingIngress;

    std::int64_t m_clusterSessionId = -1;
    // Latched once the cluster closes this client's session; see onFragment and isSessionLost().
    bool m_sessionLost = false;
    std::int64_t m_ingressStallFatalTimeoutMs = INGRESS_STALL_FATAL_TIMEOUT_MS;
    std::int64_t m_leadershipTermId = -1;
    IngressTracker* m_hold = nullptr;
    bool m_newLeaderDuringSend = false;
    std::int64_t m_lastKeepAliveMs = 0;
    std::int64_t m_connectTimeoutMs = CLUSTER_CONNECT_TIMEOUT_MS;
    const std::int64_t m_correlationId = 1;
};

} // namespace org::limitless::seqeron::sequencer::client
