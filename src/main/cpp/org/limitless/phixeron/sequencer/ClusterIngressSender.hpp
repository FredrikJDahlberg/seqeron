#pragma once

// ClusterIngressSender — Aeron Cluster client session state machine
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

#include <array>
#include <chrono>
#include <cinttypes>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <functional>
#include <memory>
#include <span>
#include <stdexcept>
#include <string_view>
#include <thread>

#include "Aeron.h"
#include "FragmentAssembler.h"
#include "concurrent/AtomicBuffer.h"

#include "org_limitless_phixeron_cluster_sbe/MessageHeader.h"
#include "org_limitless_phixeron_cluster_sbe/SessionConnectRequest.h"
#include "org_limitless_phixeron_cluster_sbe/SessionCloseRequest.h"
#include "org_limitless_phixeron_cluster_sbe/SessionEvent.h"
#include "org_limitless_phixeron_cluster_sbe/SessionKeepAlive.h"
#include "org_limitless_phixeron_cluster_sbe/SessionMessageHeader.h"
#include "org_limitless_phixeron_cluster_sbe/NewLeaderEvent.h"

namespace org::limitless::phixeron::sequencer
{

namespace cluster_sbe = org::limitless::phixeron::cluster::sbe;

// ── Constants — Aeron Cluster ingress/egress channels, stream ids and client
//    protocol semver, per io.aeron.cluster.codecs / AeronCluster.Configuration
//    defaults. Must match SequencerNode's cluster listener configuration. ────
inline constexpr const char*    CLUSTER_INGRESS_CHANNEL    = "aeron:udp?endpoint=localhost:9302";
inline constexpr const char*    CLUSTER_EGRESS_CHANNEL     = "aeron:udp?endpoint=localhost:9320";
inline constexpr std::int32_t   CLUSTER_INGRESS_STREAM_ID  = 101;
inline constexpr std::int32_t   CLUSTER_EGRESS_STREAM_ID   = 102;
inline constexpr std::int32_t   CLUSTER_PROTOCOL_VERSION   = (0 << 16) | (3 << 8) | 0; // 0.3.0
inline constexpr const char*    CLUSTER_CLIENT_INFO        = "FixSessionClient";
inline constexpr std::int64_t   CLUSTER_CONNECT_TIMEOUT_MS = 10'000;

inline std::int64_t nowMs()
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

// ── Transport interfaces ──────────────────────────────────────────────────────

// Outbound half: offers raw bytes to the cluster ingress. Implementations
// retry/back-pressure as they see fit; ClusterIngressSender treats a `false`
// return as "not yet accepted" and spins.
class IngressTransport
{
public:
    virtual ~IngressTransport() = default;
    virtual bool offer(std::span<const std::uint8_t> bytes) = 0;
};

// Inbound half: polls for whole (already reassembled) messages from the
// cluster egress, invoking the handler once per message. Returns the number
// of fragments processed (0 means nothing was available).
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
    explicit AeronIngressTransport(std::shared_ptr<aeron::Publication> pub) : m_pub(std::move(pub)) {}

    bool offer(std::span<const std::uint8_t> bytes) override
    {
        aeron::concurrent::AtomicBuffer ab(const_cast<std::uint8_t*>(bytes.data()),
                                            static_cast<aeron::util::index_t>(bytes.size()));
        return m_pub->offer(ab, 0, static_cast<aeron::util::index_t>(bytes.size())) >= 0;
    }

private:
    std::shared_ptr<aeron::Publication> m_pub;
};

class AeronEgressTransport : public EgressTransport
{
public:
    explicit AeronEgressTransport(std::shared_ptr<aeron::Subscription> sub) : m_sub(std::move(sub)) {}

    int poll(const FragmentHandler& handler) override
    {
        m_handler = &handler;
        const int n = m_sub->poll(m_fa.handler(), 10);
        m_handler = nullptr;
        return n;
    }

private:
    std::shared_ptr<aeron::Subscription> m_sub;

    // Per-call callback set in poll(); null outside of that call.
    const FragmentHandler* m_handler{nullptr};

    // Persistent across poll() calls so multi-fragment messages reassemble correctly.
    aeron::FragmentAssembler m_fa{
        [this](aeron::concurrent::AtomicBuffer& buf, aeron::util::index_t off,
               aeron::util::index_t len, aeron::Header&)
        {
            if (m_handler)
                (*m_handler)(std::span<const std::uint8_t>(
                    reinterpret_cast<const std::uint8_t*>(buf.buffer()) + off,
                    static_cast<std::size_t>(len)));
        }};
};

// ── ClusterIngressSender ──────────────────────────────────────────────────────

// Manages the Aeron Cluster session (SessionConnectRequest → SessionEvent(OK))
// and sends pre-encoded sbe-unsequenced.xml messages to the cluster ingress.
// Every message in that schema carries its own header composite (sourceId,
// sessionId), so unlike the old AppMessage scheme, send() needs no connection
// id of its own — the caller bakes it into the message before calling send().
class ClusterIngressSender
{
public:
    // Real entry point: acquires the ingress publication + egress subscription
    // from Aeron (inherently async — driver IPC via addPublication/addSubscription
    // and find*), then hands off to the transport-agnostic handshake below.
    void connect(std::shared_ptr<aeron::Aeron> aeron)
    {
        const auto subId = aeron->addSubscription(CLUSTER_EGRESS_CHANNEL, CLUSTER_EGRESS_STREAM_ID);
        std::shared_ptr<aeron::Subscription> egressSub;
        while (!(egressSub = aeron->findSubscription(subId)))
            std::this_thread::yield();

        const auto pubId = aeron->addPublication(CLUSTER_INGRESS_CHANNEL, CLUSTER_INGRESS_STREAM_ID);
        std::shared_ptr<aeron::Publication> ingressPub;
        while (!(ingressPub = aeron->findPublication(pubId)))
            std::this_thread::yield();
        while (!ingressPub->isConnected())
            std::this_thread::yield();

        connect(std::make_unique<AeronIngressTransport>(ingressPub),
                std::make_unique<AeronEgressTransport>(egressSub));
    }

    // Test seam: drives the SessionConnectRequest → SessionEvent(OK) handshake
    // against any IngressTransport/EgressTransport pair, synchronously and
    // without Aeron. A fake whose poll() answers immediately with a
    // SessionEvent(OK) makes this deterministic in a unit test.
    void connect(std::unique_ptr<IngressTransport> ingress, std::unique_ptr<EgressTransport> egress)
    {
        m_ingress = std::move(ingress);
        m_egress  = std::move(egress);

        alignas(16) std::array<std::uint8_t, 512> connBuf{};
        cluster_sbe::SessionConnectRequest req;
        req.wrapAndApplyHeader(reinterpret_cast<char*>(connBuf.data()), 0, connBuf.size());
        req.correlationId(m_correlationId)
           .responseStreamId(CLUSTER_EGRESS_STREAM_ID)
           .version(CLUSTER_PROTOCOL_VERSION);
        req.putResponseChannel(std::string_view(CLUSTER_EGRESS_CHANNEL));
        req.putEncodedCredentials(nullptr, 0);
        req.putClientInfo(std::string_view(CLUSTER_CLIENT_INFO));

        while (!m_ingress->offer(std::span<const std::uint8_t>(
                   connBuf.data(), static_cast<std::size_t>(req.sbePosition()))))
            std::this_thread::yield();

        const auto deadline = std::chrono::steady_clock::now()
                             + std::chrono::milliseconds(m_connectTimeoutMs);

        auto onEgress = [this](std::span<const std::uint8_t> bytes)
        {
            if (bytes.size() < cluster_sbe::MessageHeader::encodedLength()) return;
            cluster_sbe::MessageHeader hdr;
            hdr.wrap(reinterpret_cast<char*>(const_cast<std::uint8_t*>(bytes.data())),
                     0, 0, bytes.size());
            if (hdr.templateId() != cluster_sbe::SessionEvent::sbeTemplateId()) return;

            cluster_sbe::SessionEvent evt;
            evt.wrapForDecode(reinterpret_cast<char*>(const_cast<std::uint8_t*>(bytes.data())),
                               cluster_sbe::MessageHeader::encodedLength(),
                               hdr.blockLength(), hdr.version(), bytes.size());
            if (evt.code() == cluster_sbe::EventCode::Value::OK) {
                m_clusterSessionId = evt.clusterSessionId();
                m_leadershipTermId = evt.leadershipTermId();
                std::printf("[Cluster] Session opened  sessionId=%" PRId64
                            "  termId=%" PRId64 "  leader=%d\n",
                            m_clusterSessionId, m_leadershipTermId, evt.leaderMemberId());
            } else {
                std::fprintf(stderr, "[Cluster] SessionEvent error code=%d\n",
                             static_cast<int>(evt.code()));
            }
        };

        while (m_clusterSessionId < 0 && std::chrono::steady_clock::now() < deadline) {
            if (m_egress->poll(onEgress) == 0)
                std::this_thread::yield();
        }

        if (m_clusterSessionId < 0)
            throw std::runtime_error("[ClusterIngressSender] Timed out waiting for cluster session");
    }

    // Overrides the connect handshake timeout (default 10s). Exposed so tests
    // exercising the "cluster never answers" path don't have to wait 10s.
    void setConnectTimeoutMs(std::int64_t ms) { m_connectTimeoutMs = ms; }

    bool isConnected() const { return m_clusterSessionId >= 0; }

    // Aeron Cluster client session id of this connection, or -1 if not yet
    // connected. Callers embed this into a message's header.sessionId field
    // before encoding it for send().
    std::int64_t clusterSessionId() const { return m_clusterSessionId; }

    // Send a keep-alive to the cluster ingress if the interval has elapsed.
    // Must be called regularly (e.g. every duty-cycle iteration) to prevent session timeout.
    void keepAlive()
    {
        if (!m_ingress || m_clusterSessionId < 0) return;
        const std::int64_t now = nowMs();
        if (now - m_lastKeepAliveMs < KEEP_ALIVE_INTERVAL_MS) return;
        m_lastKeepAliveMs = now;

        alignas(16) std::array<std::uint8_t, 64> kaBuf{};
        cluster_sbe::SessionKeepAlive ka;
        ka.wrapAndApplyHeader(reinterpret_cast<char*>(kaBuf.data()), 0, kaBuf.size())
          .leadershipTermId(m_leadershipTermId)
          .clusterSessionId(m_clusterSessionId);
        if (!m_ingress->offer(std::span<const std::uint8_t>(
                kaBuf.data(), static_cast<std::size_t>(ka.sbePosition()))))
            std::fprintf(stderr, "[Cluster] keep-alive offer failed\n");
    }

    // Sends a SessionCloseRequest and locally forgets the session. Best-effort:
    // the cluster also expires unresponsive sessions via keep-alive timeout.
    void close()
    {
        if (!m_ingress || m_clusterSessionId < 0) return;

        alignas(16) std::array<std::uint8_t, 64> buf{};
        cluster_sbe::SessionCloseRequest req;
        req.wrapAndApplyHeader(reinterpret_cast<char*>(buf.data()), 0, buf.size())
           .leadershipTermId(m_leadershipTermId)
           .clusterSessionId(m_clusterSessionId);
        if (!m_ingress->offer(std::span<const std::uint8_t>(
                buf.data(), static_cast<std::size_t>(req.sbePosition()))))
            std::fprintf(stderr, "[Cluster] close offer failed\n");

        m_clusterSessionId = -1;
    }

    // Drain cluster egress; calls onAppMessage for each application-layer response.
    void pollEgress(const std::function<void(const std::uint8_t*, std::int32_t)>& onAppMessage)
    {
        if (!m_egress) return;
        m_egress->poll([this, &onAppMessage](std::span<const std::uint8_t> bytes)
        {
            onFragment(bytes, onAppMessage);
        });
    }

    // Wraps a pre-encoded sbe-unsequenced.xml message in a SessionMessageHeader
    // (the Aeron Cluster ingress envelope) and offers it to the cluster.
    void send(const std::uint8_t* bytes, std::uint16_t len)
    {
        if (!m_ingress || m_clusterSessionId < 0 || len == 0) return;

        alignas(16) std::array<std::uint8_t, 4096 + 42> buf{};
        cluster_sbe::SessionMessageHeader hdr;
        hdr.wrapAndApplyHeader(reinterpret_cast<char*>(buf.data()), 0, buf.size())
           .leadershipTermId(m_leadershipTermId)
           .clusterSessionId(m_clusterSessionId)
           .timestamp(nowMs());
        const std::int32_t hdrLen = static_cast<std::int32_t>(hdr.sbePosition());
        std::memcpy(buf.data() + hdrLen, bytes, len);

        if (!m_ingress->offer(std::span<const std::uint8_t>(
                buf.data(), static_cast<std::size_t>(hdrLen) + len)))
            std::fprintf(stderr, "[Cluster] ingress offer failed\n");
    }

private:
    static constexpr std::int64_t KEEP_ALIVE_INTERVAL_MS = 1000;

    void onFragment(std::span<const std::uint8_t> bytes,
                     const std::function<void(const std::uint8_t*, std::int32_t)>& onAppMessage)
    {
        if (bytes.size() < cluster_sbe::MessageHeader::encodedLength()) return;
        cluster_sbe::MessageHeader hdr;
        hdr.wrap(reinterpret_cast<char*>(const_cast<std::uint8_t*>(bytes.data())), 0, 0, bytes.size());

        if (hdr.templateId() == cluster_sbe::NewLeaderEvent::sbeTemplateId())
        {
            cluster_sbe::NewLeaderEvent evt;
            evt.wrapForDecode(reinterpret_cast<char*>(const_cast<std::uint8_t*>(bytes.data())),
                               cluster_sbe::MessageHeader::encodedLength(),
                               hdr.blockLength(), hdr.version(), bytes.size());
            m_leadershipTermId = evt.leadershipTermId();
            std::printf("[Cluster] New leader  termId=%" PRId64 "\n", m_leadershipTermId);
            return;
        }
        if (hdr.templateId() != cluster_sbe::SessionMessageHeader::sbeTemplateId()) return;

        const std::size_t appOff = cluster_sbe::MessageHeader::encodedLength()
                                  + static_cast<std::size_t>(hdr.blockLength());
        if (bytes.size() <= appOff) return;
        onAppMessage(bytes.data() + appOff, static_cast<std::int32_t>(bytes.size() - appOff));
    }

    std::unique_ptr<IngressTransport> m_ingress;
    std::unique_ptr<EgressTransport>  m_egress;

    std::int64_t  m_clusterSessionId  = -1;
    std::int64_t  m_leadershipTermId  = -1;
    std::int64_t  m_lastKeepAliveMs   = 0;
    std::int64_t  m_connectTimeoutMs  = CLUSTER_CONNECT_TIMEOUT_MS;
    const std::int64_t m_correlationId = 1;
};

} // namespace org::limitless::phixeron::sequencer
