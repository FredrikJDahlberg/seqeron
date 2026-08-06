#pragma once

// ClusterStreamSender — Aeron Cluster client session state machine
// (SessionConnectRequest → SessionEvent(OK) → send/keep-alive → SessionCloseRequest).
//
// The session logic below never touches Aeron types directly: it talks to the
// cluster purely through IngressTransport::offer(bytes) and
// EgressTransport::poll(handler). AeronIngressTransport/AeronEgressTransport
// are the only place Aeron Publication/Subscription appear, so a test can
// satisfy the same two interfaces with an in-memory fake and exercise
// connect/send/keepAlive/close synchronously — no media driver, no threads,
// no polling loops. See connect(std::unique_ptr<IngressTransport>, ...) below,
// which is the transport-agnostic entry point used by tests; connect(aeron)
// is the real entry point and only does Aeron resource acquisition before
// delegating to it.
//
// Leader failover: a follower answers SessionConnectRequest with
// SessionEvent(REDIRECT), and an established session gets a NewLeaderEvent when
// the cluster elects a new leader — both carry a "memberId=host:port,..." CSV of
// ingress endpoints (io.aeron.cluster.client.AeronCluster's own wire format).
// ClusterStreamSender resolves its own new endpoint out of that CSV and swaps
// its ingress Publication to it; the cluster session id is unaffected, only the
// leadershipTermId and the publication endpoint change. Reconnection only runs
// when connect(aeron) supplied a real Aeron client (m_aeron); the transport-
// agnostic connect() overload used by unit tests leaves it disabled.

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
#include "org_limitless_phixeron_cluster_sbe/MessageHeader.h"
#include "org_limitless_phixeron_cluster_sbe/NewLeaderEvent.h"
#include "org_limitless_phixeron_cluster_sbe/SessionCloseRequest.h"
#include "org_limitless_phixeron_cluster_sbe/SessionConnectRequest.h"
#include "org_limitless_phixeron_cluster_sbe/SessionEvent.h"
#include "org_limitless_phixeron_cluster_sbe/SessionKeepAlive.h"
#include "org_limitless_phixeron_cluster_sbe/SessionMessageHeader.h"
#include "org/limitless/phixeron/sequencer/PortLayout.hpp"
#include "org/limitless/phixeron/util/Logger.hpp"

namespace org::limitless::phixeron::sequencer {

namespace cluster_sbe = org::limitless::phixeron::cluster::sbe;
namespace diag = org::limitless::phixeron::util;

// ── Constants — Aeron Cluster ingress/egress channels, stream ids and client
//    protocol semver, per io.aeron.cluster.codecs / AeronCluster.Configuration
//    defaults. Ports are derived from PortLayout.hpp's shared formula rather than
//    restated as literals — must match SequencerNode's cluster listener configuration. ────
// CLUSTER_INGRESS_CHANNEL must stay "aeron:udp?endpoint=" + CLUSTER_INGRESS_ENDPOINT — the
// endpoint alone is also this client's initial value for the reconnect-on-failover tracking
// in ClusterStreamSender (m_ingressEndpoint). Member 0's ingress port is only the *initial*
// guess for a non-colocated client: handleRedirect/onFragment resolve the real leader's
// endpoint from the wire CSV afterward.
inline const std::string CLUSTER_INGRESS_ENDPOINT = "localhost:" + std::to_string(clusterIngressPort(0));
inline const std::string CLUSTER_INGRESS_CHANNEL = "aeron:udp?endpoint=" + CLUSTER_INGRESS_ENDPOINT;
inline const std::string CLUSTER_EGRESS_CHANNEL =
    "aeron:udp?endpoint=localhost:" + std::to_string(FIX_TEST_CLIENT_EGRESS_PORT);
// Distinct egress port for a co-located client (see connectColocated): it attaches to its own
// SequencerNode member's embedded media driver rather than the shared standalone aeronmd that
// FixGateway/fix_test_server use, so it needs its own port here too — two independent
// media driver processes can't both bind the same UDP port on localhost.
inline const std::string CLUSTER_EGRESS_CHANNEL_COLOCATED =
    "aeron:udp?endpoint=localhost:" + std::to_string(orderExecEgressPort(0));
// Ingress channel for a client co-located with (sharing the Aeron directory of) a cluster
// member — only reachable while that member is the current leader, see
// ClusterStreamSender::connectColocated.
inline constexpr const char* CLUSTER_INGRESS_CHANNEL_IPC = "aeron:ipc";
inline constexpr std::int32_t CLUSTER_INGRESS_STREAM_ID = 101;
inline constexpr std::int32_t CLUSTER_EGRESS_STREAM_ID = 102;
inline constexpr std::int32_t CLUSTER_PROTOCOL_VERSION = (0 << 16) | (3 << 8) | 0;  // 0.3.0
inline constexpr const char* CLUSTER_CLIENT_INFO = "FixGateway";
inline constexpr std::int64_t CLUSTER_CONNECT_TIMEOUT_MS = 10'000;

inline std::int64_t nowMs()
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
        .count();
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

// Outbound half: offers raw bytes to the cluster ingress. Implementations
// retry/back-pressure as they see fit; ClusterStreamSender treats a `false`
// return as "not yet accepted" and spins.
class IngressTransport {
   public:
    virtual ~IngressTransport() = default;
    virtual bool offer(std::span<const std::uint8_t> bytes) = 0;
};

// Inbound half: polls for whole (already reassembled) messages from the
// cluster egress, invoking the handler once per message. Returns the number
// of fragments processed (0 means nothing was available).
class EgressTransport {
   public:
    using FragmentHandler = std::function<void(std::span<const std::uint8_t>)>;

    virtual ~EgressTransport() = default;
    virtual int poll(const FragmentHandler& handler) = 0;
};

// ── Real Aeron-backed transports ──────────────────────────────────────────────

class AeronIngressTransport : public IngressTransport {
   public:
    explicit AeronIngressTransport(std::shared_ptr<aeron::Publication> pub) : m_pub(std::move(pub))
    {}

    bool offer(std::span<const std::uint8_t> bytes) override
    {
        aeron::concurrent::AtomicBuffer ab(const_cast<std::uint8_t*>(bytes.data()),
                                           static_cast<aeron::util::index_t>(bytes.size()));
        return m_pub->offer(ab, 0, static_cast<aeron::util::index_t>(bytes.size())) >= 0;
    }

   private:
    std::shared_ptr<aeron::Publication> m_pub;
};

class AeronEgressTransport : public EgressTransport {
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
    const FragmentHandler* m_handler{nullptr};

    // Persistent across poll() calls so multi-fragment messages reassemble correctly.
    aeron::FragmentAssembler m_fa{[this](aeron::concurrent::AtomicBuffer& buf, aeron::util::index_t off,
                                         aeron::util::index_t len, aeron::Header&) {
        if (m_handler)
            (*m_handler)(std::span<const std::uint8_t>(reinterpret_cast<const std::uint8_t*>(buf.buffer()) + off,
                                                       static_cast<std::size_t>(len)));
    }};

    // Composed once rather than per poll(): FragmentAssembler::handler() returns a fresh
    // std::function by value, and this is polled every duty-cycle iteration. Subscription::poll
    // takes its handler by forwarding reference, so passing the member costs nothing to begin with.
    // Declared after m_fa — it is built from it.
    aeron::fragment_handler_t m_poll{m_fa.handler()};
};

// ── ClusterStreamSender ──────────────────────────────────────────────────────

// Manages the Aeron Cluster session (SessionConnectRequest → SessionEvent(OK))
// and sends pre-encoded sbe-unsequenced.xml messages to the cluster ingress.
// Every message in that schema carries its own header composite (sourceId,
// connectionId, sessionId), so unlike the old AppMessage scheme, send() needs
// no connection id of its own — the caller bakes it into the message before
// calling send(). sourceId (this process's fixed identity) is held here
// instead, since it's the same for every message this sender ever submits.
class ClusterStreamSender {
   public:
    // Largest sbe-unsequenced.xml payload send() can frame. Every caller sizes its encode buffer from
    // this (see FixIngressHandler::m_buffer), so a caller's buffer and send()'s framing buffer cannot
    // disagree — the two used to, at 8192 against 4096, leaving an unchecked memcpy of a
    // network-derived length into a smaller stack array (doc/review-2026-07-25.md #4).
    static constexpr std::size_t MAX_PAYLOAD_LEN = 8192;

    // Real entry point: acquires the ingress publication + egress subscription
    // from Aeron (inherently async — driver IPC via addPublication/addSubscription
    // and find*), then hands off to the transport-agnostic handshake below.
    // Storing `aeron` (used by createIngressPublication) is what enables automatic
    // ingress reconnection on SessionEvent(REDIRECT)/NewLeaderEvent below.
    void connect(std::shared_ptr<aeron::Aeron> aeron)
    {
        m_aeron = std::move(aeron);
        m_ingressEndpoint = CLUSTER_INGRESS_ENDPOINT;

        const auto subId = m_aeron->addSubscription(CLUSTER_EGRESS_CHANNEL, CLUSTER_EGRESS_STREAM_ID);
        std::shared_ptr<aeron::Subscription> egressSub;
        while (!(egressSub = m_aeron->findSubscription(subId)))
            m_idleStrategy.idle();

        connect(std::make_unique<AeronIngressTransport>(createIngressPublication(m_ingressEndpoint)),
                std::make_unique<AeronEgressTransport>(egressSub));
    }

    // Real entry point for a client deployed co-located with one cluster member — sharing
    // that member's own Aeron directory (see OrderExecClient's PHIXERON_ORDER_EXEC_AERON_DIR).
    // Egress uses the given UDP egressChannel (default CLUSTER_EGRESS_CHANNEL_COLOCATED; a
    // per-node replica passes a member-specific endpoint so co-located replicas on one host don't
    // collide — see the parameter note below). It is unaffected by which member is leader: the
    // leader publishes to whatever responseChannel the client requests, over UDP loopback here
    // regardless of which host/process is currently leader. Ingress tries
    // CLUSTER_INGRESS_CHANNEL_IPC first, on the theory that the co-located member usually is
    // (or will shortly become) leader; a co-located member that is a follower never opens the
    // IPC ingress subscription at all (see SequencerNode's isIpcIngressAllowed — leader-only),
    // so an IPC SessionConnectRequest to a follower simply goes unanswered rather than being
    // rejected — hence the short ipcConnectTimeoutMs before falling back to the normal UDP
    // ingress endpoint, where a follower answers with a proper REDIRECT to the real leader.
    //
    // Once connected (by either path), everything else is unchanged: NewLeaderEvent/REDIRECT
    // handling already resolves UDP endpoints from the wire CSV and swaps m_ingress, so
    // leadership later moving away from the co-located member degrades to UDP ingress
    // automatically. Leadership later moving *back* to it is handled too (see onFragment's
    // NewLeaderEvent branch): `memberId` is this client's own co-located cluster member id, so a
    // NewLeaderEvent naming it can be recognised and re-chased back onto IPC.
    void connectColocated(std::shared_ptr<aeron::Aeron> aeron, std::int32_t memberId,
                          std::int64_t ipcConnectTimeoutMs = 1500,
                          const std::string& egressChannel = CLUSTER_EGRESS_CHANNEL_COLOCATED)
    {
        m_aeron = std::move(aeron);
        m_coLocatedMemberId = memberId;
        m_ipcConnectTimeoutMs = ipcConnectTimeoutMs;
        // egressChannel must be a distinct UDP endpoint per co-located client: when a replica runs
        // on every cluster node, each one attaches to its own member's media driver, and two driver
        // processes on one host cannot both bind the same egress UDP port. Callers pass
        // localhost:(9330 + memberId) or similar; the default keeps the single-replica behaviour.
        m_egressChannel = egressChannel;

        const auto subId = m_aeron->addSubscription(m_egressChannel, CLUSTER_EGRESS_STREAM_ID);
        std::shared_ptr<aeron::Subscription> egressSub;
        while (!(egressSub = m_aeron->findSubscription(subId)))
            m_idleStrategy.idle();

        m_ingressEndpoint = "ipc";
        std::unique_ptr<IngressTransport> primary;
        try
        {
            primary = std::make_unique<AeronIngressTransport>(createIpcIngressPublication());
        }
        catch (const std::exception& ex)
        {
            // Building the IPC publication itself timed out (createIpcIngressPublication's own
            // deadline) — treat exactly like a failed handshake attempt below.
            connectColocated(
                nullptr,
                [this] {
                    m_ingressEndpoint = CLUSTER_INGRESS_ENDPOINT;
                    return std::make_unique<AeronIngressTransport>(createIngressPublication(m_ingressEndpoint));
                },
                std::make_unique<AeronEgressTransport>(egressSub), ipcConnectTimeoutMs, ex.what());
            return;
        }

        connectColocated(
            std::move(primary),
            [this] {
                m_ingressEndpoint = CLUSTER_INGRESS_ENDPOINT;
                return std::make_unique<AeronIngressTransport>(createIngressPublication(m_ingressEndpoint));
            },
            std::make_unique<AeronEgressTransport>(egressSub), ipcConnectTimeoutMs, nullptr);
    }

    // Test seam for connectColocated: exercises the same "try the primary ingress transport
    // with a short timeout, fall back to a freshly-built one with the normal timeout on
    // failure" logic against fake transports, without a real Aeron client. `primaryIngress`
    // may be null to skip straight to the fallback (mirrors createIpcIngressPublication()
    // itself throwing before a transport ever exists). `buildFallbackIngress` is only invoked
    // if the primary attempt fails; `egress` is shared by both attempts (reused via
    // ClusterStreamSender::connect's `m_egress` after a failed first attempt). `memberId`
    // (default -1, i.e. "no co-located member") lets a test set m_coLocatedMemberId without a
    // real Aeron client, to exercise onFragment's NewLeaderEvent guard — the guard also requires
    // m_aeron, so it stays a no-op here regardless; there is nothing to reconnect to without a
    // real Aeron client, same reasoning as the plain endpoint-reconnect path above it.
    void connectColocated(std::unique_ptr<IngressTransport> primaryIngress,
                          std::function<std::unique_ptr<IngressTransport>()> buildFallbackIngress,
                          std::unique_ptr<EgressTransport> egress, std::int64_t primaryConnectTimeoutMs,
                          const char* primaryFailureReason, std::int32_t memberId = -1)
    {
        m_coLocatedMemberId = memberId;
        const std::int64_t fullTimeoutMs = m_connectTimeoutMs;
        std::string reasonStorage;  // outlives the catch block, unlike ex.what()'s pointer

        if (primaryIngress)
        {
            m_connectTimeoutMs = primaryConnectTimeoutMs;
            try
            {
                connect(std::move(primaryIngress), std::move(egress));
                m_connectTimeoutMs = fullTimeoutMs;
                return;
            }
            catch (const std::exception& ex)
            {
                reasonStorage = ex.what();
                primaryFailureReason = reasonStorage.c_str();
                egress = std::move(m_egress);  // connect() already stashed it in m_egress before failing
            }
        }

        diag::Logger::info(diag::Component::Cluster,
                               "Co-located member not leader (%s) — falling back to UDP ingress",
                               primaryFailureReason != nullptr ? primaryFailureReason : "unknown");
        m_connectTimeoutMs = fullTimeoutMs;
        connect(buildFallbackIngress(), std::move(egress));
    }

    // Test seam: drives the SessionConnectRequest → SessionEvent(OK) handshake
    // against any IngressTransport/EgressTransport pair, synchronously and
    // without Aeron. A fake whose poll() answers immediately with a
    // SessionEvent(OK) makes this deterministic in a unit test. Redirect/reconnect
    // is skipped in this path since it has no Aeron client to build a new
    // Publication with (see the m_aeron guard in handleRedirect/onFragment).
    void connect(std::unique_ptr<IngressTransport> ingress, std::unique_ptr<EgressTransport> egress)
    {
        m_ingress = std::move(ingress);
        m_egress = std::move(egress);

        sendConnectRequest();

        const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(m_connectTimeoutMs);

        auto onEgress = [this](std::span<const std::uint8_t> bytes) {
            if (bytes.size() < cluster_sbe::MessageHeader::encodedLength())
                return;
            cluster_sbe::MessageHeader hdr;
            hdr.wrap(reinterpret_cast<char*>(const_cast<std::uint8_t*>(bytes.data())), 0, 0, bytes.size());
            if (hdr.templateId() != cluster_sbe::SessionEvent::sbeTemplateId())
                return;

            cluster_sbe::SessionEvent evt;
            evt.wrapForDecode(reinterpret_cast<char*>(const_cast<std::uint8_t*>(bytes.data())),
                              cluster_sbe::MessageHeader::encodedLength(), hdr.blockLength(), hdr.version(),
                              bytes.size());
            if (evt.code() == cluster_sbe::EventCode::Value::OK)
            {
                m_clusterSessionId = evt.clusterSessionId();
                m_leadershipTermId = evt.leadershipTermId();
                diag::Logger::info(diag::Component::Cluster,
                                       "Session opened  sessionId=%" PRId64 "  termId=%" PRId64 "  leader=%d",
                                       m_clusterSessionId, m_leadershipTermId, evt.leaderMemberId());
            }
            else if (evt.code() == cluster_sbe::EventCode::Value::REDIRECT)
            {
                handleRedirect(evt);
            }
            else
            {
                diag::Logger::error(diag::Component::Cluster, diag::EventCode::ClusterSessionError,
                                        "SessionEvent error code=%d", static_cast<int>(evt.code()));
            }
        };

        while (m_clusterSessionId < 0 && std::chrono::steady_clock::now() < deadline)
        {
            m_idleStrategy.idle(m_egress->poll(onEgress));
        }

        if (m_clusterSessionId < 0)
            throw std::runtime_error("[ClusterStreamSender] Timed out waiting for cluster session");
    }

    // Overrides the connect handshake timeout (default 10s). Exposed so tests
    // exercising the "cluster never answers" path don't have to wait 10s.
    void setConnectTimeoutMs(std::int64_t ms)
    {
        m_connectTimeoutMs = ms;
    }

    bool isConnected() const
    {
        return m_clusterSessionId >= 0;
    }

    // Aeron Cluster client session id of this connection, or -1 if not yet
    // connected. Callers embed this into a message's header.sessionId field
    // before encoding it for send().
    std::int64_t clusterSessionId() const
    {
        return m_clusterSessionId;
    }

    // True once the cluster has closed this client's session (see onFragment) — as opposed to never
    // having connected one, which leaves clusterSessionId() at -1 just the same. Latched: there is no
    // re-handshake, so a caller whose work is only valid with a session checks this and stops.
    [[nodiscard]] bool isSessionLost() const noexcept
    {
        return m_sessionLost;
    }

    // Send a keep-alive to the cluster ingress if the interval has elapsed.
    // Must be called regularly (e.g. every duty-cycle iteration) to prevent session timeout.
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
        cluster_sbe::SessionKeepAlive ka;
        ka.wrapAndApplyHeader(reinterpret_cast<char*>(kaBuf.data()), 0, kaBuf.size())
            .leadershipTermId(m_leadershipTermId)
            .clusterSessionId(m_clusterSessionId);
        if (!m_ingress->offer(std::span<const std::uint8_t>(kaBuf.data(), static_cast<std::size_t>(ka.sbePosition()))))
        {
            diag::Logger::error(diag::Component::Cluster, diag::EventCode::ClusterOfferFailed,
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
        cluster_sbe::SessionCloseRequest req;
        req.wrapAndApplyHeader(reinterpret_cast<char*>(buf.data()), 0, buf.size())
            .leadershipTermId(m_leadershipTermId)
            .clusterSessionId(m_clusterSessionId);
        if (!m_ingress->offer(std::span<const std::uint8_t>(buf.data(), static_cast<std::size_t>(req.sbePosition()))))
        {
            diag::Logger::error(diag::Component::Cluster, diag::EventCode::ClusterOfferFailed,
                                    "close offer failed");
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
    }

    // Wraps a pre-encoded sbe-unsequenced.xml message in a SessionMessageHeader
    // (the Aeron Cluster ingress envelope) and offers it to the cluster, spinning
    // until the offer lands. This is reliable by the same contract as the sequencer's
    // tap emit: a dropped ingress frame is unrecoverable — it tears a hole in the
    // gateway's outbound MsgSeqNum stream that no resend can fill (the ResendCache only
    // ever holds frames that actually round-tripped) — so send() never returns having
    // failed to place the frame *while there is a cluster session to place it on*.
    //
    // Returns false in the one case the spin cannot resolve: there is no session (not yet
    // connected, or already closed), so no amount of waiting would help — a leader failover,
    // which the spin does handle, keeps the session id. The caller must not have committed
    // anything on the assumption the frame went out; that is why this reports rather than
    // returning void, and why Session::publishOutbound only advances the outbound MsgSeqNum
    // once the frame is placed (doc/review-2026-07-25.md #4).
    //
    // Two reasons an offer is rejected, both handled by the spin:
    //   • transient back-pressure — the ingress subscriber is briefly behind;
    //     re-offering succeeds once it drains. This is the common case even in steady
    //     state and was the frame-dropping bug before this became a spin.
    //   • the ingress publication went not-connected because the leader changed — the
    //     retry must first let a NewLeaderEvent/REDIRECT swap m_ingress to the new
    //     leader. That swap is driven by egress polling, which runs on *this* thread
    //     (the single duty cycle), so the spin pumps egress itself (pumpEgressControl).
    //     A plain while(!offer) idle() would DEADLOCK here: it would spin forever on the
    //     dead leader's publication while the very poll that revives it never runs.
    // After a swap the leadership term has moved on, so leadershipTermId (and the
    // cluster-overwritten timestamp) are re-stamped before each retry — the new leader
    // rejects a frame still carrying the previous term. This mirrors what Aeron's own
    // AeronCluster.offer() does internally.
    [[nodiscard]] bool send(const std::uint8_t* bytes, std::uint16_t len)
    {
        if (!m_ingress || m_clusterSessionId < 0 || len == 0)
        {
            return false;
        }
        // buf is sized from MAX_PAYLOAD_LEN, as every caller's encode buffer is, so this cannot fire
        // without a code change that broke that pairing. Throwing rather than truncating or dropping:
        // a frame too large to place is a programming error no runtime handling can repair, and
        // silently dropping it would tear the outbound MsgSeqNum hole this function exists to prevent.
        if (len > MAX_PAYLOAD_LEN)
        {
            throw std::runtime_error("[ClusterStreamSender] ingress payload " + std::to_string(len) +
                                     " exceeds MAX_PAYLOAD_LEN " + std::to_string(MAX_PAYLOAD_LEN));
        }

        alignas(16) std::array<std::uint8_t, INGRESS_FRAME_LEN> buf{};
        cluster_sbe::SessionMessageHeader hdr;
        hdr.wrapAndApplyHeader(reinterpret_cast<char*>(buf.data()), 0, buf.size())
            .leadershipTermId(m_leadershipTermId)
            .clusterSessionId(m_clusterSessionId)
            .timestamp(nowMs());
        const std::int32_t hdrLen = static_cast<std::int32_t>(hdr.sbePosition());
        std::memcpy(buf.data() + hdrLen, bytes, len);
        const std::size_t frameLen = static_cast<std::size_t>(hdrLen) + len;

        while (!m_ingress->offer(std::span<const std::uint8_t>(buf.data(), frameLen)))
        {
            pumpEgressControl();  // let a NewLeaderEvent/REDIRECT swap m_ingress to the new leader
            m_idleStrategy.idle();
            hdr.leadershipTermId(m_leadershipTermId).timestamp(nowMs());  // re-stamp for the (possibly new) leader
        }
        return true;
    }

   private:
    static constexpr std::int64_t KEEP_ALIVE_INTERVAL_MS = 1000;

    // send()'s framing buffer: the largest payload plus the SessionMessageHeader envelope it goes in.
    static constexpr std::size_t INGRESS_FRAME_LEN =
        MAX_PAYLOAD_LEN + cluster_sbe::SessionMessageHeader::sbeBlockAndHeaderLength();

    // Drives the egress subscription for cluster session-control frames only —
    // SessionEvent / NewLeaderEvent / REDIRECT, all handled inside onFragment (e.g.
    // swapping m_ingress to a new leader) — discarding any application payload. Used
    // by send()'s reliable-offer spin so a leader failover can complete mid-send.
    // Discarding the payload is safe because every ClusterStreamSender caller already
    // polls egress with a no-op application handler: in this system the data round-trip
    // is the node-local tap, and cluster egress carries only session-control frames.
    void pumpEgressControl()
    {
        if (m_egress)
        {
            m_egress->poll([this](std::span<const std::uint8_t> bytes) {
                onFragment(bytes, [](const std::uint8_t*, std::int32_t) {});
            });
        }
    }

    void onFragment(std::span<const std::uint8_t> bytes,
                    const std::function<void(const std::uint8_t*, std::int32_t)>& onAppMessage)
    {
        if (bytes.size() < cluster_sbe::MessageHeader::encodedLength())
        {
            return;
        }
        cluster_sbe::MessageHeader hdr;
        hdr.wrap(reinterpret_cast<char*>(const_cast<std::uint8_t*>(bytes.data())), 0, 0, bytes.size());

        if (hdr.templateId() == cluster_sbe::SessionEvent::sbeTemplateId())
        {
            cluster_sbe::SessionEvent evt;
            evt.wrapForDecode(reinterpret_cast<char*>(const_cast<std::uint8_t*>(bytes.data())),
                              cluster_sbe::MessageHeader::encodedLength(), hdr.blockLength(), hdr.version(),
                              bytes.size());
            // The cluster has closed our session — on request, on leader shutdown, or by keep-alive
            // timeout, all of which it reports here the same way (io.aeron.cluster.SessionManager sends
            // EventCode.CLOSED carrying the CloseReason as detail, timeouts included). A steady-state
            // SessionEvent used to fall off the end of this handler, so a session the cluster had already
            // forgotten still looked healthy to this client and send() went on framing into a dead id.
            // Forgetting it here makes send()/keepAlive() report the truth instead; what to do about it is
            // the caller's call (FixGateway fences its TCP clients — it must not answer FIX traffic it can
            // no longer sequence). Latched, because there is no re-handshake to clear it.
            if (m_clusterSessionId >= 0 && evt.clusterSessionId() == m_clusterSessionId &&
                evt.code() != cluster_sbe::EventCode::Value::OK)
            {
                diag::Logger::error(diag::Component::Cluster, diag::EventCode::ClusterSessionError,
                                        "Cluster closed session %" PRId64 " (code=%d, %s)", m_clusterSessionId,
                                        static_cast<int>(evt.code()), evt.getDetailAsString().c_str());
                m_clusterSessionId = -1;
                m_sessionLost = true;
            }
            return;
        }
        if (hdr.templateId() == cluster_sbe::NewLeaderEvent::sbeTemplateId())
        {
            cluster_sbe::NewLeaderEvent evt;
            evt.wrapForDecode(reinterpret_cast<char*>(const_cast<std::uint8_t*>(bytes.data())),
                              cluster_sbe::MessageHeader::encodedLength(), hdr.blockLength(), hdr.version(),
                              bytes.size());
            m_leadershipTermId = evt.leadershipTermId();
            const std::int32_t leaderMemberId = evt.leaderMemberId();
            const std::string ingressEndpoints = evt.getIngressEndpointsAsString();

            // Leadership has returned to our own co-located member while we're parked on UDP
            // (having failed over from IPC earlier) — re-chase the cheaper IPC path rather than
            // staying on UDP until the next restart. Bounded by the same short timeout
            // connectColocated uses for its own first IPC attempt: the co-located member's IPC
            // ingress subscription is Aeron's own leader-only listener (isIpcIngressAllowed), so
            // there is a startup race identical to the one connectColocated already tolerates, not
            // a reason to block the duty cycle waiting on the full connect timeout.
            if (m_aeron && m_coLocatedMemberId >= 0 && leaderMemberId == m_coLocatedMemberId &&
                m_ingressEndpoint != "ipc")
            {
                const std::int64_t fullTimeoutMs = m_connectTimeoutMs;
                m_connectTimeoutMs = m_ipcConnectTimeoutMs;
                try
                {
                    auto ipcPub = createIpcIngressPublication();
                    m_connectTimeoutMs = fullTimeoutMs;
                    m_ingress = std::make_unique<AeronIngressTransport>(std::move(ipcPub));
                    m_ingressEndpoint = "ipc";
                    diag::Logger::info(diag::Component::Cluster, "New leader  termId=%" PRId64
                                           "  member=%d is co-located — switched back to IPC ingress",
                                          m_leadershipTermId, leaderMemberId);
                    return;
                }
                catch (const std::exception& ex)
                {
                    m_connectTimeoutMs = fullTimeoutMs;
                    diag::Logger::error(diag::Component::Cluster,diag::EventCode::ClusterIpcFallback,
                                            "Co-located member=%d is new leader but IPC ingress not ready yet "
                                            "(%s) — staying on UDP", leaderMemberId, ex.what());
                }
            }

            std::string endpoint;
            if (m_aeron && findIngressEndpoint(ingressEndpoints, leaderMemberId, endpoint) &&
                endpoint != m_ingressEndpoint)
            {
                diag::Logger::info(diag::Component::Cluster,
                                       "New leader  termId=%" PRId64 "  member=%d  endpoint=%s",
                                       m_leadershipTermId, leaderMemberId, endpoint.c_str());
                m_ingress = std::make_unique<AeronIngressTransport>(createIngressPublication(endpoint));
                m_ingressEndpoint = endpoint;
            }
            else
            {
                diag::Logger::info(diag::Component::Cluster, "New leader  termId=%" PRId64, m_leadershipTermId);
            }
            return;
        }
        if (hdr.templateId() != cluster_sbe::SessionMessageHeader::sbeTemplateId())
        {
            return;
        }

        const std::size_t appOff =
            cluster_sbe::MessageHeader::encodedLength() + static_cast<std::size_t>(hdr.blockLength());
        if (bytes.size() <= appOff)
        {
            return;
        }
        onAppMessage(bytes.data() + appOff, static_cast<std::int32_t>(bytes.size() - appOff));
    }

    // Encodes and offers a SessionConnectRequest on the current m_ingress. Used both for the
    // initial handshake and to re-announce the session after a REDIRECT swaps m_ingress to the
    // new leader's endpoint.
    void sendConnectRequest()
    {
        alignas(16) std::array<std::uint8_t, 512> connBuf{};
        cluster_sbe::SessionConnectRequest req;
        req.wrapAndApplyHeader(reinterpret_cast<char*>(connBuf.data()), 0, connBuf.size());
        req.correlationId(m_correlationId).responseStreamId(CLUSTER_EGRESS_STREAM_ID).version(CLUSTER_PROTOCOL_VERSION);
        req.putResponseChannel(std::string_view(m_egressChannel));
        req.putEncodedCredentials(nullptr, 0);
        req.putClientInfo(std::string_view(CLUSTER_CLIENT_INFO));

        while (!m_ingress->offer(
            std::span<const std::uint8_t>(connBuf.data(), static_cast<std::size_t>(req.sbePosition()))))
            m_idleStrategy.idle();
    }

    // A follower rejected our SessionConnectRequest, pointing us at the real leader. Swap the
    // ingress Publication to the leader's endpoint (if we have a real Aeron client to build one
    // with) and re-announce. No-op if the endpoint is already the one we're using.
    void handleRedirect(cluster_sbe::SessionEvent& evt)
    {
        const std::int32_t leaderMemberId = evt.leaderMemberId();
        const std::string detail = evt.getDetailAsString();

        std::string endpoint;
        if (!m_aeron || !findIngressEndpoint(detail, leaderMemberId, endpoint) || endpoint == m_ingressEndpoint)
        {
            diag::Logger::error(diag::Component::Cluster, diag::EventCode::ClusterRedirectUnresolved,
                                    "Redirected to member=%d but could not resolve a new "
                                    "ingress endpoint from \"%s\"", leaderMemberId, detail.c_str());
            return;
        }

        diag::Logger::info(diag::Component::Cluster, "Redirected to leader  member=%d  endpoint=%s",
                               leaderMemberId, endpoint.c_str());
        m_ingress = std::make_unique<AeronIngressTransport>(createIngressPublication(endpoint));
        m_ingressEndpoint = endpoint;
        sendConnectRequest();
    }

    // Creates and blocks (up to m_connectTimeoutMs) until connected to a Publication for the
    // cluster ingress at `endpoint` ("host:port"). Only valid when connect(aeron) was used —
    // m_aeron is null in the transport-agnostic test seam.
    std::shared_ptr<aeron::Publication> createIngressPublication(const std::string& endpoint)
    {
        const std::string channel = "aeron:udp?endpoint=" + endpoint;
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(m_connectTimeoutMs);

        const auto pubId = m_aeron->addPublication(channel, CLUSTER_INGRESS_STREAM_ID);
        std::shared_ptr<aeron::Publication> pub;
        while (!(pub = m_aeron->findPublication(pubId)))
        {
            if (std::chrono::steady_clock::now() >= deadline)
                throw std::runtime_error("[ClusterStreamSender] Timed out creating ingress publication to " +
                                         endpoint);
            m_idleStrategy.idle();
        }
        while (!pub->isConnected())
        {
            if (std::chrono::steady_clock::now() >= deadline)
                throw std::runtime_error("[ClusterStreamSender] Timed out connecting ingress publication to " +
                                         endpoint);
            m_idleStrategy.idle();
        }
        return pub;
    }

    // Same as createIngressPublication(endpoint), but over CLUSTER_INGRESS_CHANNEL_IPC —
    // only ever reachable by an ingress subscription the co-located member opens while it is
    // leader (see SequencerNode's isIpcIngressAllowed), so isConnected() may simply never
    // become true when it isn't; the m_connectTimeoutMs deadline here is what bounds that,
    // same as the UDP case, and connectColocated relies on it to trigger the UDP fallback.
    std::shared_ptr<aeron::Publication> createIpcIngressPublication()
    {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(m_connectTimeoutMs);

        const auto pubId = m_aeron->addPublication(CLUSTER_INGRESS_CHANNEL_IPC, CLUSTER_INGRESS_STREAM_ID);
        std::shared_ptr<aeron::Publication> pub;
        while (!(pub = m_aeron->findPublication(pubId)))
        {
            if (std::chrono::steady_clock::now() >= deadline)
                throw std::runtime_error("[ClusterStreamSender] Timed out creating IPC ingress publication");
            m_idleStrategy.idle();
        }
        while (!pub->isConnected())
        {
            if (std::chrono::steady_clock::now() >= deadline)
                throw std::runtime_error("[ClusterStreamSender] Timed out connecting IPC ingress publication");
            m_idleStrategy.idle();
        }
        return pub;
    }

    std::shared_ptr<aeron::Aeron> m_aeron;
    std::string m_ingressEndpoint;
    // This client's own co-located cluster member id, or -1 if not co-located (plain connect()).
    // Set by connectColocated; lets onFragment's NewLeaderEvent handling recognise "leadership
    // came back to me" and re-chase IPC instead of resolving yet another UDP endpoint.
    std::int32_t m_coLocatedMemberId = -1;
    // Short bounded timeout reused for every IPC (re)connect attempt, not just the first —
    // set once by connectColocated alongside m_coLocatedMemberId.
    std::int64_t m_ipcConnectTimeoutMs = 1500;
    // Response channel sendConnectRequest() advertises to the cluster; CLUSTER_EGRESS_CHANNEL
    // for connect(aeron), CLUSTER_EGRESS_CHANNEL_COLOCATED for connectColocated(aeron, ...).
    std::string m_egressChannel = CLUSTER_EGRESS_CHANNEL;
    std::unique_ptr<IngressTransport> m_ingress;
    std::unique_ptr<EgressTransport> m_egress;
    aeron::concurrent::YieldingIdleStrategy m_idleStrategy;

    std::int64_t m_clusterSessionId = -1;
    // Latched once the cluster closes this client's session; see onFragment and isSessionLost().
    bool m_sessionLost = false;
    std::int64_t m_leadershipTermId = -1;
    std::int64_t m_lastKeepAliveMs = 0;
    std::int64_t m_connectTimeoutMs = CLUSTER_CONNECT_TIMEOUT_MS;
    const std::int64_t m_correlationId = 1;
    std::int32_t m_sourceId = 0;

   public:
    // Fixed constant identifying this gateway *process* to the cluster (header.sourceId),
    // as opposed to header.connectionId which identifies one TCP connection within it.
    // Set once at startup (see PHIXERON_*_SOURCE_ID env vars in each binary's main()) so
    // it stays stable across restarts and unique across every gateway instance sharing
    // this cluster — unlike a per-connection counter, which starts back at 1 on every
    // process and would otherwise collide with another gateway's connection ids.
    void setSourceId(std::int32_t sourceId)
    {
        m_sourceId = sourceId;
    }
    std::int32_t sourceId() const
    {
        return m_sourceId;
    }
};

}  // namespace org::limitless::phixeron::sequencer
