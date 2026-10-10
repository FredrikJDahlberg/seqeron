#pragma once

// ClusterStreamSender — Aeron Cluster client session state machine
// (SessionConnectRequest → SessionEvent(OK) → send/keep-alive → SessionCloseRequest).
//
// The session logic talks to the cluster only through IngressTransport/EgressTransport
// (detail/Transport.hpp), so a test drives it with in-memory fakes; connect(aeron) acquires the Aeron
// resources and delegates to the transport-agnostic connect(). On REDIRECT or NewLeaderEvent it resolves
// the leader's endpoint from the wire's "memberId=host:port,..." CSV and swaps its ingress publication;
// the session id is unchanged. The first dial goes to every member of the configured set at once, and the
// first to answer takes the handshake: a follower redirects to the leader.
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
#include <utility>
#include <vector>

#include "Aeron.h"
#include "FragmentAssembler.h"
#include "concurrent/AtomicBuffer.h"
#include "concurrent/YieldingIdleStrategy.h"
#include "org/limitless/seqeron/protocol/PortLayout.hpp"
#include "org/limitless/seqeron/sequencer/client/IngressTracker.hpp"
#include "org/limitless/seqeron/sequencer/client/detail/Transport.hpp"
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
//    and AeronCluster.Configuration. ────
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

/**
 * Finds a member's endpoint in an ingress-endpoint CSV.
 *
 * @param endpoints "memberId=host:port,memberId=host:port,...", the wire format both SessionEvent.detail
 *                  (on REDIRECT) and NewLeaderEvent.ingressEndpoints use
 * @param memberId  the member to find
 * @param[out] out  its "host:port"; untouched if not found
 * @return false if the CSV has no entry for that member
 */
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

/**
 * Lists the endpoints of an ingress-endpoint CSV, in order.
 *
 * @param endpoints "memberId=host:port,memberId=host:port,..."
 * @return each entry's "host:port"
 * @throws std::invalid_argument if an entry has no memberId, or there are none
 */
inline std::vector<std::string> ingressEndpointList(std::string_view endpoints)
{
    std::vector<std::string> list;
    std::size_t start = 0;
    while (start < endpoints.size())
    {
        const std::size_t comma = endpoints.find(',', start);
        const std::string_view entry =
            endpoints.substr(start, comma == std::string_view::npos ? std::string_view::npos : comma - start);
        const std::size_t eq = entry.find('=');
        if (eq == std::string_view::npos || eq == 0 || eq + 1 == entry.size())
        {
            throw std::invalid_argument("[ClusterStreamSender] ingress endpoint '" + std::string{ entry } +
                                        "' is not memberId=host:port");
        }
        list.emplace_back(entry.substr(eq + 1));
        if (comma == std::string_view::npos)
        {
            break;
        }
        start = comma + 1;
    }
    if (list.empty())
    {
        throw std::invalid_argument("[ClusterStreamSender] no ingress endpoints");
    }
    return list;
}

// ── ClusterStreamSender ──────────────────────────────────────────────────────

// Manages the Aeron Cluster session and sends pre-encoded frames to the cluster ingress. Each frame
// carries its own header composite, sourceId included.
class ClusterStreamSender
{
  public:
    static constexpr std::size_t MAX_PAYLOAD_LEN = 16384;

    /**
     * Opens a cluster session over UDP ingress: acquires the ingress publication and egress subscription,
     * then runs the handshake. Blocks until the session opens.
     *
     * @param aeron            the client to build publications on; storing it is what enables reconnection
     *                         on REDIRECT/NewLeaderEvent
     * @param egressChannel    this client's own egress channel, the responseChannel the cluster answers on
     * @param ingressEndpoints the members to reach, "memberId=host:port,..."
     * @throws std::runtime_error if no session opens within the connect timeout
     * @throws std::invalid_argument if ingressEndpoints is malformed
     */
    void connect(std::shared_ptr<aeron::Aeron> aeron, const std::string& egressChannel,
                 const std::string& ingressEndpoints = protocol::ingressEndpointsCsv())
    {
        m_aeron = std::move(aeron);
        m_ingressEndpoints = ingressEndpoints;
        m_egressChannel = egressChannel;

        auto egress = std::make_unique<detail::AeronEgressTransport>(awaitEgressSubscription());
        connect(dialIngress(), std::move(egress), m_egressChannel);
    }

    /**
     * Opens a cluster session for a client sharing a cluster member's Aeron directory. Ingress tries that
     * member's IPC first: a follower never opens IPC ingress, so the request goes unanswered, hence a short
     * timeout before falling back to UDP, where a follower redirects to the leader. Leadership later moving
     * away degrades to UDP; moving back to the member is re-chased onto IPC (see onFragment).
     *
     * @param aeron               the client to build publications on, sharing the member's directory
     * @param memberId            the co-located member
     * @param ipcConnectTimeoutMs how long to wait for IPC ingress before falling back to UDP
     * @param egressChannel       this client's own egress channel; a distinct UDP endpoint per co-located
     *                            client, since two drivers on one host cannot both bind one port
     * @param ingressEndpoints    the members to reach over UDP, "memberId=host:port,..."
     * @throws std::runtime_error if no session opens over either ingress
     * @throws std::invalid_argument if ingressEndpoints is malformed
     */
    void connectColocated(std::shared_ptr<aeron::Aeron> aeron, std::int32_t memberId, std::int64_t ipcConnectTimeoutMs,
                          const std::string& egressChannel,
                          const std::string& ingressEndpoints = protocol::ingressEndpointsCsv())
    {
        ingressEndpointList(ingressEndpoints); // refuse a malformed set before the IPC attempt, not after it
        m_aeron = std::move(aeron);
        m_ingressEndpoints = ingressEndpoints;
        m_coLocatedMemberId = memberId;
        m_ipcConnectTimeoutMs = ipcConnectTimeoutMs;
        // Must be a distinct UDP endpoint per co-located client: two drivers on one host cannot both
        // bind one egress port.
        m_egressChannel = egressChannel;

        openColocated(std::make_unique<detail::AeronEgressTransport>(awaitEgressSubscription()));
    }

    /**
     * Replaces a lost session with a new one, opened as the first was and on the same egress subscription, but
     * one step per call, so the caller's duty cycle never waits on an unreachable cluster: the co-located
     * member's IPC ingress, then the dial to every member, then the handshake, each within its own timeout.
     * What the old one had not committed is not carried over: the caller's tracker decides about it.
     *
     * @return whether a session opened; until then isReconnecting() says whether to call again, and a failed
     *         attempt is logged and ends, for the caller to begin another later
     */
    bool reconnect()
    {
        if (!m_aeron || !m_egress)
        {
            return false; // the test seam has no Aeron client to build a publication with
        }
        try
        {
            switch (m_reconnect.stage)
            {
                case Reconnect::Stage::Idle:
                    m_clusterSessionId = -1;
                    m_pendingIngress = PendingIngressSwitch{};
                    m_ingress.reset();
                    if (m_coLocatedMemberId < 0)
                    {
                        beginReconnectDial();
                    }
                    else
                    {
                        beginReconnectIpc();
                    }
                    return false;
                case Reconnect::Stage::Ipc:
                    stepReconnectIpc();
                    return false;
                case Reconnect::Stage::Dial:
                    stepReconnectDial();
                    return false;
                case Reconnect::Stage::Handshake:
                    return stepReconnectHandshake();
            }
        }
        catch (const std::exception& ex)
        {
            m_reconnect = Reconnect{};
            util::Logger::error(util::component::Cluster, util::eventCode::ClusterSessionError,
                                "could not replace the lost cluster session (%s)", ex.what());
        }
        return false;
    }

    // Whether reconnect() has an attempt under way, which only further calls advance.
    [[nodiscard]] bool isReconnecting() const noexcept
    {
        return m_reconnect.stage != Reconnect::Stage::Idle;
    }

    /**
     * Test seam for connectColocated: tries the primary ingress with the short timeout, then falls back.
     * Without m_aeron the NewLeaderEvent re-chase stays a no-op.
     *
     * @param primaryIngress         tried first; null goes straight to the fallback
     * @param buildFallbackIngress   builds the fallback, tried with the full timeout
     * @param egress                 shared by both attempts
     * @param primaryConnectTimeoutMs the primary attempt's timeout
     * @param primaryFailureReason   why building the primary failed, when it is null; logged
     * @param memberId               the co-located member; only sets the state a test inspects
     */
    void connectColocated(std::unique_ptr<detail::IngressTransport> primaryIngress,
                          std::function<std::unique_ptr<detail::IngressTransport>()> buildFallbackIngress,
                          std::unique_ptr<detail::EgressTransport> egress, std::int64_t primaryConnectTimeoutMs,
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
        m_egress = std::move(egress); // kept for a reconnect if the fallback cannot be built either
        auto fallback = buildFallbackIngress();
        connect(std::move(fallback), std::move(m_egress), m_egressChannel);
    }

    /**
     * Test seam: drives the handshake against any transport pair, synchronously and without Aeron.
     * Redirect/reconnect is skipped here (no Aeron client to build a publication with).
     *
     * @param ingress       the transport the handshake and frames go out on
     * @param egress        the transport the cluster's answers come in on
     * @param egressChannel the responseChannel the cluster publishes egress on
     * @throws std::runtime_error if no session opens within the connect timeout
     */
    void connect(std::unique_ptr<detail::IngressTransport> ingress, std::unique_ptr<detail::EgressTransport> egress,
                 const std::string& egressChannel = "")
    {
        m_ingress = std::move(ingress);
        m_egress = std::move(egress);
        m_egressChannel = egressChannel;

        sendConnectRequest();

        const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(m_connectTimeoutMs);
        while (m_clusterSessionId < 0 && std::chrono::steady_clock::now() < deadline)
        {
            m_idleStrategy.idle(pollHandshake());
        }

        if (m_clusterSessionId < 0)
        {
            throw std::runtime_error("[ClusterStreamSender] Timed out waiting for cluster session");
        }
    }

    /**
     * Overrides the connect handshake timeout, for tests.
     *
     * @param ms the timeout; the default is 10s
     */
    void setConnectTimeoutMs(std::int64_t ms)
    {
        m_connectTimeoutMs = ms;
    }

    /**
     * Overrides send()'s give-up bound, for tests.
     *
     * @param ms the bound; the default is INGRESS_STALL_FATAL_TIMEOUT_MS
     */
    void setIngressStallTimeoutMs(std::int64_t ms)
    {
        m_ingressStallFatalTimeoutMs = ms;
    }

    /**
     * Sets the tracker that hears every NewLeaderEvent and gives up a send that met one while it holds.
     *
     * @param hold the tracker, normally the producer's PendingSends; not owned, nullptr for none
     */
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

    // True once the cluster has closed this session, as opposed to never having opened one. Latched until
    // reconnect() opens another.
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
            if (m_keepAliveFailures++ == 0)
            {
                util::Logger::error(util::component::Cluster, util::eventCode::ClusterOfferFailed,
                                    "keep-alive offer failed — the rest of the run is counted, not logged");
            }
        }
        else if (m_keepAliveFailures > 0)
        {
            util::Logger::info(util::component::Cluster, "keep-alive accepted after %d refused", m_keepAliveFailures);
            m_keepAliveFailures = 0;
        }
    }

    // Sends a SessionCloseRequest and locally forgets the session. Best-effort:
    // the cluster also expires unresponsive sessions via keep-alive timeout.
    void close()
    {
        m_reconnect = Reconnect{};
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

    /**
     * Drains cluster egress. Call it every duty cycle.
     *
     * @param onAppMessage called with the bytes and length of each application-layer response
     */
    void pollEgress(const std::function<void(const std::uint8_t*, std::int32_t)>& onAppMessage)
    {
        if (!m_egress || isReconnecting()) // a replacement's handshake takes its answer off the same egress
        {
            return;
        }
        m_egress->poll([this, &onAppMessage](std::span<const std::uint8_t> bytes) { onFragment(bytes, onAppMessage); });
        applyPendingIngressSwitch(); // a NewLeaderEvent/REDIRECT in that batch; never build inside poll()
    }

    /**
     * Wraps a pre-encoded frame in a SessionMessageHeader and offers it, spinning until it lands. The spin
     * pumps egress itself: a leader change leaves the publication not-connected until a
     * NewLeaderEvent/REDIRECT swaps it, and that swap runs on this thread. The term and timestamp are
     * re-stamped before each retry, as AeronCluster.offer() does, since the new leader drops a stale term.
     *
     * @param bytes the frame's first byte, its messageHeader included
     * @param len   the frame's length, at most MAX_PAYLOAD_LEN
     * @return true once the frame is placed; false with no session, when the spin outlasts the stall bound
     *         (the session is then lost), or when a NewLeaderEvent arrived mid-spin while the IngressTracker
     *         holds — nothing was placed, and the frame goes again once it releases
     * @throws std::runtime_error if len exceeds MAX_PAYLOAD_LEN
     */
    [[nodiscard]] bool send(const std::uint8_t* bytes, std::uint16_t len)
    {
        if (!m_ingress || m_clusterSessionId < 0 || len == 0)
        {
            return false;
        }
        // m_sendBuffer is sized from MAX_PAYLOAD_LEN like every caller's encode buffer, so this is a programming
        // error; dropping the frame silently would open a hole.
        if (len > MAX_PAYLOAD_LEN)
        {
            throw std::runtime_error("[ClusterStreamSender] ingress payload " + std::to_string(len) +
                                     " exceeds MAX_PAYLOAD_LEN " + std::to_string(MAX_PAYLOAD_LEN));
        }

        std::uint8_t* const buf = m_sendBuffer.data();
        cluster::sbe::SessionMessageHeader hdr;
        hdr.wrapAndApplyHeader(reinterpret_cast<char*>(buf), 0, m_sendBuffer.size())
            .leadershipTermId(m_leadershipTermId)
            .clusterSessionId(m_clusterSessionId)
            .timestamp(nowMs());
        const std::int32_t hdrLen = static_cast<std::int32_t>(hdr.sbePosition());
        std::memcpy(buf + hdrLen, bytes, len);
        const std::size_t frameLen = static_cast<std::size_t>(hdrLen) + len;

        // Bounded: both other exits need a leader, so losing quorum would otherwise stop the caller's whole
        // duty cycle silently. Past the bound the session is called lost and the caller's isSessionLost()
        // fence takes over. No keep-alive from in here: it would offer on the same stuck publication.
        std::chrono::steady_clock::time_point blockedSince{};
        std::chrono::steady_clock::time_point nextAlert{};
        m_newLeaderDuringSend = false;
        while (!m_ingress->offer(std::span<const std::uint8_t>(buf, frameLen)))
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

    // Polls egress for the handshake's answer, then makes any ingress swap a REDIRECT in it asked for. Returns the
    // fragments read.
    int pollHandshake()
    {
        const int fragments =
            m_egress->poll([this](std::span<const std::uint8_t> bytes) { onHandshakeFragment(bytes); });
        applyPendingIngressSwitch(); // a REDIRECT in that batch; never build inside poll()
        return fragments;
    }

    void onHandshakeFragment(std::span<const std::uint8_t> bytes)
    {
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
                          cluster::sbe::MessageHeader::encodedLength(), hdr.blockLength(), hdr.version(), bytes.size());
        if (evt.code() == cluster::sbe::EventCode::Value::OK)
        {
            m_clusterSessionId = evt.clusterSessionId();
            m_leadershipTermId = evt.leadershipTermId();
            util::Logger::info(util::component::Cluster,
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
    }

    // reconnect()'s first stage when co-located: the member's IPC ingress, which only a leader listens on.
    void beginReconnectIpc()
    {
        m_reconnect =
            Reconnect{ .stage = Reconnect::Stage::Ipc,
                       .deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(m_ipcConnectTimeoutMs),
                       .ipcAdd = PendingPublication(*m_aeron, CLUSTER_INGRESS_CHANNEL_IPC, CLUSTER_INGRESS_STREAM_ID) };
    }

    // Any failure on IPC is a follower's silence, as in connectColocated: the dial to every member follows.
    void stepReconnectIpc()
    {
        std::string failure = "timed out";
        try
        {
            if (!m_reconnect.ipc)
            {
                m_reconnect.ipc = m_reconnect.ipcAdd.find();
            }
            if (m_reconnect.ipc && m_reconnect.ipc->isConnected())
            {
                m_ingress = std::make_unique<detail::AeronIngressTransport>(std::move(m_reconnect.ipc));
                m_ingressEndpoint = "ipc";
                beginReconnectHandshake(m_ipcConnectTimeoutMs);
                return;
            }
            if (std::chrono::steady_clock::now() < m_reconnect.deadline)
            {
                return;
            }
        }
        catch (const std::exception& ex)
        {
            failure = ex.what();
        }
        reconnectOverUdp(failure);
    }

    void reconnectOverUdp(const std::string& ipcFailure)
    {
        util::Logger::info(util::component::Cluster,
                           "Co-located member did not answer IPC ingress (%s) — replacing the session over UDP",
                           ipcFailure.c_str());
        m_reconnect = Reconnect{};
        beginReconnectDial();
    }

    void beginReconnectDial()
    {
        m_reconnect =
            Reconnect{ .stage = Reconnect::Stage::Dial,
                       .deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(m_connectTimeoutMs),
                       .dials = beginDial() };
    }

    void stepReconnectDial()
    {
        std::shared_ptr<aeron::Publication> chosen = stepDial(m_reconnect.dials);
        if (!chosen && std::chrono::steady_clock::now() < m_reconnect.deadline)
        {
            return;
        }
        if (!chosen)
        {
            throw std::runtime_error("[ClusterStreamSender] Timed out connecting ingress publication to any of " +
                                     m_ingressEndpoints);
        }
        m_ingress = std::make_unique<detail::AeronIngressTransport>(std::move(chosen));
        beginReconnectHandshake(m_connectTimeoutMs);
    }

    void beginReconnectHandshake(const std::int64_t timeoutMs)
    {
        const bool overIpc = m_reconnect.stage == Reconnect::Stage::Ipc;
        m_reconnect = Reconnect{ .stage = Reconnect::Stage::Handshake,
                                 .deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs),
                                 .handshakeOverIpc = overIpc };
        sendConnectRequest();
    }

    // An unanswered handshake over IPC falls back to the dial, as connectColocated's does.
    bool stepReconnectHandshake()
    {
        pollHandshake();
        if (m_clusterSessionId >= 0)
        {
            m_reconnect = Reconnect{};
            m_sessionLost = false;
            util::Logger::info(util::component::Cluster, "cluster session %" PRId64 " replaces the lost one",
                               m_clusterSessionId);
            return true;
        }
        if (std::chrono::steady_clock::now() < m_reconnect.deadline)
        {
            return false;
        }
        if (m_reconnect.handshakeOverIpc)
        {
            reconnectOverUdp("no answer to the session request");
            return false;
        }
        throw std::runtime_error("[ClusterStreamSender] Timed out waiting for cluster session");
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
        // An OK carries no endpoint CSV, so the leader is looked up in the set this client was given.
        std::string endpoint;
        if (!findIngressEndpoint(m_ingressEndpoints, leaderMemberId, endpoint))
        {
            util::Logger::error(util::component::Cluster, util::eventCode::ClusterRedirectUnresolved,
                                "Session opened through non-leader ingress %s — leader member=%d is not in \"%s\"",
                                m_ingressEndpoint.c_str(), leaderMemberId, m_ingressEndpoints.c_str());
            return;
        }
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
            m_ingress = std::make_unique<detail::AeronIngressTransport>(std::move(pub));
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

    // The co-located member's IPC ingress first: a follower never connects that publication, so building it is
    // bounded by the short m_ipcConnectTimeoutMs, and a timeout there is treated like a failed handshake on it.
    void openColocated(std::unique_ptr<detail::EgressTransport> egress)
    {
        m_ingressEndpoint = "ipc";
        std::unique_ptr<detail::IngressTransport> primary;
        std::string primaryFailureReason;
        try
        {
            primary =
                std::make_unique<detail::AeronIngressTransport>(createIpcIngressPublication(m_ipcConnectTimeoutMs));
        }
        catch (const std::exception& ex)
        {
            primaryFailureReason = ex.what();
        }

        connectColocated(
            std::move(primary), [this] { return dialIngress(); }, std::move(egress), m_ipcConnectTimeoutMs,
            primaryFailureReason.c_str(), m_coLocatedMemberId);
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

        PendingPublication add(*m_aeron, channel, CLUSTER_INGRESS_STREAM_ID);
        std::shared_ptr<aeron::Publication> pub;
        while (!(pub = add.find()))
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

    // A UDP ingress publication to every member of m_ingressEndpoints at once, keeping the first to connect:
    // any member answers a SessionConnectRequest, a follower with a REDIRECT, so the first live one will do.
    std::unique_ptr<detail::IngressTransport> dialIngress()
    {
        std::vector<IngressDial> dials = beginDial();
        std::shared_ptr<aeron::Publication> chosen;
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(m_connectTimeoutMs);
        while (!(chosen = stepDial(dials)) && std::chrono::steady_clock::now() < deadline)
        {
            m_idleStrategy.idle();
        }
        if (!chosen)
        {
            throw std::runtime_error("[ClusterStreamSender] Timed out connecting ingress publication to any of " +
                                     m_ingressEndpoints);
        }
        return std::make_unique<detail::AeronIngressTransport>(std::move(chosen));
    }

    // A publication added and not yet found. Dropping it unfound cancels the registration, so an attempt abandoned
    // at any stage leaves nothing with the driver.
    class PendingPublication
    {
      public:
        PendingPublication() = default;

        PendingPublication(aeron::Aeron& aeron, const std::string& channel, std::int32_t streamId) :
          m_aeron(&aeron),
          m_add(aeron.addPublicationAsync(channel, streamId))
        {}

        PendingPublication(PendingPublication&& other) noexcept :
          m_aeron(other.m_aeron),
          m_add(std::exchange(other.m_add, nullptr))
        {}

        PendingPublication& operator=(PendingPublication&& other) noexcept
        {
            if (this != &other)
            {
                cancel();
                m_aeron = other.m_aeron;
                m_add = std::exchange(other.m_add, nullptr);
            }
            return *this;
        }

        PendingPublication(const PendingPublication&) = delete;
        PendingPublication& operator=(const PendingPublication&) = delete;

        ~PendingPublication()
        {
            cancel();
        }

        // The publication once the driver has added it, else null; throws the driver's error. A result or an error
        // ends the registration's pending state.
        std::shared_ptr<aeron::Publication> find()
        {
            aeron::AsyncAddPublication* add = std::exchange(m_add, nullptr);
            std::shared_ptr<aeron::Publication> publication = m_aeron->findPublication(add);
            if (!publication)
            {
                m_add = add;
            }
            return publication;
        }

      private:
        void cancel() noexcept
        {
            if (m_add)
            {
                aeron_async_add_publication_cancel(m_aeron->aeron(), m_add);
                m_add = nullptr;
            }
        }

        aeron::Aeron* m_aeron = nullptr;
        aeron::AsyncAddPublication* m_add = nullptr;
    };

    struct IngressDial
    {
        std::string endpoint;
        PendingPublication add;
        std::shared_ptr<aeron::Publication> publication = nullptr;
        bool resolved = false;
    };

    std::vector<IngressDial> beginDial()
    {
        std::vector<IngressDial> dials;
        for (std::string& endpoint : ingressEndpointList(m_ingressEndpoints))
        {
            PendingPublication add(*m_aeron, protocol::udpChannel(endpoint), CLUSTER_INGRESS_STREAM_ID);
            dials.push_back(IngressDial{ .endpoint = std::move(endpoint), .add = std::move(add) });
        }
        return dials;
    }

    // A driver error (an unresolvable host, say) leaves that one member out rather than failing the dial.
    void resolveDial(IngressDial& dial)
    {
        try
        {
            dial.publication = dial.add.find();
            dial.resolved = dial.publication != nullptr;
        }
        catch (const std::exception& ex)
        {
            dial.resolved = true;
            util::Logger::error(util::component::Cluster, util::eventCode::ClusterRedirectUnresolved,
                                "Could not build ingress publication to %s (%s)", dial.endpoint.c_str(), ex.what());
        }
    }

    // One pass over the dials; returns the first connected, and points m_ingressEndpoint at it, or null.
    std::shared_ptr<aeron::Publication> stepDial(std::vector<IngressDial>& dials)
    {
        for (IngressDial& dial : dials)
        {
            if (!dial.resolved)
            {
                resolveDial(dial);
            }
            if (dial.publication && dial.publication->isConnected())
            {
                m_ingressEndpoint = dial.endpoint;
                return std::move(dial.publication);
            }
        }
        return nullptr;
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
    std::string m_ingressEndpoints; // "memberId=host:port,...", as given to connect/connectColocated
    std::string m_ingressEndpoint;
    std::int32_t m_coLocatedMemberId = -1;
    std::int64_t m_ipcConnectTimeoutMs = 1500;
    std::string m_egressChannel;
    std::unique_ptr<detail::IngressTransport> m_ingress;
    std::unique_ptr<detail::EgressTransport> m_egress;
    // send()'s framing buffer, allocated once: a frame-sized stack array, zeroed per call, costs every send.
    std::vector<std::uint8_t> m_sendBuffer = std::vector<std::uint8_t>(INGRESS_FRAME_LEN);
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

    // A session replacement under way, one step per reconnect() call.
    struct Reconnect
    {
        enum class Stage
        {
            Idle,
            Ipc,
            Dial,
            Handshake
        };
        Stage stage = Stage::Idle;
        std::chrono::steady_clock::time_point deadline{};
        PendingPublication ipcAdd;
        std::shared_ptr<aeron::Publication> ipc;
        std::vector<IngressDial> dials;
        bool handshakeOverIpc = false;
    };
    Reconnect m_reconnect;

    std::int64_t m_clusterSessionId = -1;
    // Latched once the cluster closes this client's session; see onFragment and isSessionLost().
    bool m_sessionLost = false;
    std::int64_t m_ingressStallFatalTimeoutMs = INGRESS_STALL_FATAL_TIMEOUT_MS;
    std::int64_t m_leadershipTermId = -1;
    IngressTracker* m_hold = nullptr;
    bool m_newLeaderDuringSend = false;
    std::int64_t m_lastKeepAliveMs = 0;
    // Keep-alives refused in a row: the first is logged, and the run when one is accepted again.
    int m_keepAliveFailures = 0;
    std::int64_t m_connectTimeoutMs = CLUSTER_CONNECT_TIMEOUT_MS;
    const std::int64_t m_correlationId = 1;
};

} // namespace org::limitless::seqeron::sequencer::client
