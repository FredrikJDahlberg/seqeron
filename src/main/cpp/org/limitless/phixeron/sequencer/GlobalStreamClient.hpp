#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "Aeron.h"
#include "FragmentAssembler.h"
#include "client/archive/AeronArchive.h"

// Generated SBE C++ codecs from sbe-sequenced.xml (via GenerateSequencedSbeCodecs)
#include "org_limitless_phixeron_sbe_sequenced/MessageHeader.h"
#include "org_limitless_phixeron_sbe_sequenced/Header.h"

namespace org::limitless::phixeron::sequencer
{

// ── Constants matching SequencerService / SequencerNode ──────────────────────

// Multi-destination-cast (dynamic control mode). This is the publisher/archive-recording
// channel — matches SequencerService.GLOBAL_STREAM_CHANNEL, used here only for archive
// recording lookups (listRecordingsForUri), never to open a local subscription directly.
inline constexpr const char* GLOBAL_STREAM_CHANNEL =
    "aeron:udp?control-mode=dynamic|control=localhost:9200";

// Subscriber-side channel: same control address, plus an ephemeral local data endpoint
// that the publisher discovers and adds as a destination automatically. Matches
// SequencerService.GLOBAL_STREAM_SUBSCRIBER_CHANNEL. Used for the live (post-replay) fallback
// subscription below.
inline constexpr const char* GLOBAL_STREAM_SUBSCRIBER_CHANNEL =
    "aeron:udp?control-mode=dynamic|control=localhost:9200|endpoint=localhost:0";

inline constexpr std::int32_t GLOBAL_STREAM_ID = 1;

// Each binary uses a distinct port so their archive replay publications don't conflict.
// FixSessionClient  → 9310 (env PHIXERON_FIX_REPLAY_PORT)
// OrderExecClient   → 9311 (env PHIXERON_ORDER_EXEC_REPLAY_PORT)
// fix_test_server   → 9312 (env PHIXERON_RISK_TEST_REPLAY_PORT)
// FixSessionClient's resend-recovery replay → 9313 (env PHIXERON_RESEND_REPLAY_PORT)
inline constexpr std::int32_t REPLAY_STREAM_ID = 110;

/**
 * Resolves a replay channel's port from the environment (so two instances of the same
 * binary can run on one host without a port clash — see todo.md item 8), falling back to
 * the given default. Returns a full "aeron:udp?endpoint=localhost:<port>" channel string.
 */
inline std::string resolveReplayChannel(const char* envVar, std::uint16_t defaultPort)
{
    std::uint16_t port = defaultPort;
    if (const char* value = std::getenv(envVar); value != nullptr && *value != '\0')
    {
        port = static_cast<std::uint16_t>(std::strtoul(value, nullptr, 10));
    }
    return "aeron:udp?endpoint=localhost:" + std::to_string(port);
}

// Default 3-node cluster archive control endpoints, one per member, following the
// SequencerNode.PORT_BASE + memberId*10 + 1 formula (see three-node-cluster.sh's
// CLUSTER_MEMBERS): member 0 → 9301, member 1 → 9311, member 2 → 9321. Every
// member's co-located archive holds an identical recording of the global stream,
// so any reachable one works equally well — there's no leader-affinity requirement
// here, unlike cluster ingress.
inline constexpr const char* DEFAULT_ARCHIVE_ENDPOINTS = "localhost:9301,localhost:9311,localhost:9321";

/**
 * Splits a comma-separated "host:port,host:port,..." list from the given
 * environment variable, falling back to defaultCsv (same format) when unset
 * or empty.
 */
inline std::vector<std::string> resolveArchiveEndpoints(const char* envVar, const char* defaultCsv)
{
    const char* value = std::getenv(envVar);
    const std::string csv = (value != nullptr && *value != '\0') ? value : defaultCsv;

    std::vector<std::string> endpoints;
    std::size_t start = 0;
    while (start <= csv.size())
    {
        const std::size_t comma = csv.find(',', start);
        const std::size_t end   = (comma == std::string::npos) ? csv.size() : comma;
        if (end > start)
        {
            endpoints.push_back(csv.substr(start, end - start));
        }
        if (comma == std::string::npos)
        {
            break;
        }
        start = comma + 1;
    }
    return endpoints;
}

/**
 * Connects to the first reachable archive among controlEndpoints, in order.
 *
 * Archive recordings of the global stream are identical across every cluster
 * member (each node's co-located archive records the same replicated stream),
 * so — unlike cluster ingress, which must track the current Raft leader via
 * REDIRECT/NewLeaderEvent — any reachable member's archive is an equally valid
 * replay source. This is a one-time bootstrap choice, not something that needs
 * to react to leadership changes afterward.
 *
 * @throws std::runtime_error if every candidate endpoint fails to connect.
 */
inline std::shared_ptr<aeron::archive::client::AeronArchive> connectToAnyArchive(
    std::shared_ptr<aeron::Aeron>    aeron,
    const std::vector<std::string>& controlEndpoints,
    std::int32_t                     controlStreamId,
    const char*                      controlResponseChannel,
    const char*                      logPrefix)
{
    std::string lastError = "no candidate endpoints given";
    for (const auto& endpoint : controlEndpoints)
    {
        try
        {
            aeron::archive::client::Context archiveCtx;
            archiveCtx.aeron(aeron)
                      .controlRequestChannel("aeron:udp?endpoint=" + endpoint)
                      .controlRequestStreamId(controlStreamId)
                      .controlResponseChannel(controlResponseChannel);

            auto archive = aeron::archive::client::AeronArchive::connect(archiveCtx);
            std::printf("%s Connected to Aeron Archive at %s\n", logPrefix, endpoint.c_str());
            return archive;
        }
        catch (const std::exception& ex)
        {
            std::fprintf(stderr, "%s Archive connect to %s failed: %s\n",
                         logPrefix, endpoint.c_str(), ex.what());
            lastError = ex.what();
        }
    }

    throw std::runtime_error(
        std::string(logPrefix) + " Could not connect to any archive endpoint (tried " +
        std::to_string(controlEndpoints.size()) + "); last error: " + lastError);
}

// ClientConnected/ClientDisconnected aren't FIX messages, so sbe-sequenced.xml
// (like sbe-unsequenced.xml) gives them small, non-ASCII-derived template ids,
// clear of the FIX-MsgType-derived range used by every other message.
inline constexpr std::uint16_t CLIENT_CONNECTED_TEMPLATE_ID    = 1;
inline constexpr std::uint16_t CLIENT_DISCONNECTED_TEMPLATE_ID = 2;

// ── Event types delivered to the application ─────────────────────────────────

/**
 * Carries one sbe-sequenced.xml message from the global stream.
 *
 * Every raw fragment on the wire *is* a complete sbe-sequenced.xml message
 * (schemaId=202) — no envelope to strip. Every message in that schema
 * declares `header` (sourceId, sessionId, globalSeqNo, timestamp) as its
 * first field, at the same fixed offset regardless of templateId, so this
 * client decodes it generically and exposes the fields here — callers don't
 * need to re-decode it themselves before dispatching on templateId.
 *
 * payload/payloadLength point into the Aeron fragment buffer and are valid
 * only for the duration of the callback; payload addresses the start of the
 * full message (its own 8-byte messageHeader included). Copy the data before
 * returning if it must survive.
 */
struct SequencedEvent
{
    std::int64_t  globalSeqNo;
    std::int32_t  sourceId;         ///< TCP connection id at the FIX gateway (header.sourceId)
    std::int64_t  sourceSessionId;  ///< Aeron Cluster client session id (header.sessionId)
    std::int64_t  clusterTimestamp; ///< cluster consensus time (ms) when message was committed
    std::int64_t  receiveTimeNs;    ///< wall-clock ns at receipt by this client
    std::uint16_t templateId;       ///< outer messageHeader templateId; picks the specific decode
    std::uint16_t blockLength;      ///< outer messageHeader blockLength; pass straight to wrapForDecode
    std::uint16_t version;          ///< outer messageHeader version; pass straight to wrapForDecode
    const char*   payload;          ///< raw sbe-sequenced.xml message bytes (see struct comment)
    std::uint64_t payloadLength;    ///< total byte count
    std::int64_t  position;         ///< recording/stream position of this frame's first byte;
                                     ///< pass to ReplayParams::position() to replay from here
};

struct LifecycleEvent
{
    std::int64_t globalSeqNo;
    std::int64_t sourceSessionId;
    std::int64_t clusterTimestamp;
    std::int64_t receiveTimeNs;
};

// ── GlobalStreamClient ────────────────────────────────────────────────────────

/**
 * Subscribes to the SequencerService global stream, decoding and dispatching
 * SBE messages to the caller.
 *
 * Startup sequence (caller is responsible for the archive connection):
 *   1. Caller uses AeronArchive to find the global stream recording and call
 *      aeronArchive.startReplay(recordingId, startPosition, NULL_POSITION,
 *                               REPLAY_CHANNEL, REPLAY_STREAM_ID)
 *      keeping the returned replaySessionId and the recording's stop position.
 *   2. Call start(aeron, replaySessionId, catchUpPosition).
 *   3. Call poll() in a duty-cycle loop.
 *
 * The replay image uses NULL_POSITION as length so it follows the live
 * recording seamlessly — the same image delivers both historical and live
 * messages without a subscription switch. If the image closes (leader failover)
 * the client falls back to a direct MDC subscription.
 *
 * Every message is stamped with receiveTimeNs (std::chrono::system_clock).
 */
class GlobalStreamClient
{
public:
    using OnSequenced    = std::function<void(const SequencedEvent&)>;
    using OnConnected    = std::function<void(const LifecycleEvent&)>;
    using OnDisconnected = std::function<void(const LifecycleEvent&)>;
    using OnCaughtUp     = std::function<void()>;

    explicit GlobalStreamClient(OnSequenced    onSequenced,
                                OnConnected    onConnected    = {},
                                OnDisconnected onDisconnected = {},
                                OnCaughtUp     onCaughtUp     = {})
        : m_onSequenced(std::move(onSequenced))
        , m_onConnected(std::move(onConnected))
        , m_onDisconnected(std::move(onDisconnected))
        , m_onCaughtUp(std::move(onCaughtUp))
        , m_fragmentHandler([this](auto& buf, auto off, auto len, auto& hdr) {
              onFragment(buf, off, len, hdr);
          })
    {}

    /**
     * Attaches to an already-started archive replay image and adds a live
     * MDC fallback subscription.
     *
     * @param aeron           connected Aeron instance
     * @param replaySessionId session ID returned by AeronArchive::startReplay(),
     *                        or -1 when there is no historical data to replay
     * @param catchUpPosition recording stop position observed at startup;
     *                        onCaughtUp fires once the replay image reaches it
     * @param replayChannel   channel the archive publishes the replay on;
     *                        ignored when replaySessionId < 0
     */
    void start(std::shared_ptr<aeron::Aeron> aeron,
               std::int64_t                  replaySessionId,
               std::int64_t                  catchUpPosition,
               const char*                   replayChannel = nullptr)
    {
        m_aeron           = std::move(aeron);
        m_replaySessionId = replaySessionId;
        m_catchUpPosition = catchUpPosition;

        if (replaySessionId >= 0 && replayChannel != nullptr) {
            m_replaySubRegId = m_aeron->addSubscription(replayChannel, REPLAY_STREAM_ID);
        }

        // Live MDC fallback — always subscribed; used when replay image closes.
        m_liveSubRegId = m_aeron->addSubscription(GLOBAL_STREAM_SUBSCRIBER_CHANNEL, GLOBAL_STREAM_ID);

        if (replaySessionId < 0) {
            // No historical data — already at live.
            notifyCaughtUp();
        }
    }

    /**
     * Polls the active image for up to FRAGMENT_LIMIT fragments.
     * @return number of fragments consumed
     */
    int poll()
    {
        // Lazily resolve subscriptions once they become available.
        if (!m_replaySub && m_replaySubRegId >= 0) {
            m_replaySub = m_aeron->findSubscription(m_replaySubRegId);
        }
        if (!m_liveSub && m_liveSubRegId >= 0) {
            m_liveSub = m_aeron->findSubscription(m_liveSubRegId);
        }

        // Lazily resolve the replay image once it becomes available.
        if (!m_replayImage && m_replaySub) {
            m_replayImage = m_replaySub->imageBySessionId(
                static_cast<std::int32_t>(m_replaySessionId));
        }

        if (m_replayImage) {
            if (!m_replayImage->isClosed()) {
                const int work = m_replayImage->poll(m_fragmentHandler, FRAGMENT_LIMIT);
                if (!m_caughtUp && m_replayImage->position() >= m_catchUpPosition) {
                    notifyCaughtUp();
                }
                return work;
            }
            // Archive recording stopped (e.g. leader failover). Discard and fall through.
            m_replayImage.reset();
            m_replaySub.reset();
        }

        // Live MDC fallback — poll the subscription directly.
        if (m_liveSub) {
            return m_liveSub->poll(m_fragmentHandler, FRAGMENT_LIMIT);
        }
        return 0;
    }

    bool isCaughtUp() const { return m_caughtUp; }

private:
    static constexpr int FRAGMENT_LIMIT = 10;

    using HdrSbe = org::limitless::phixeron::sbe::sequenced::MessageHeader;
    using HeaderComposite = org::limitless::phixeron::sbe::sequenced::Header;

    void onFragment(aeron::concurrent::AtomicBuffer& buffer,
                    aeron::util::index_t              offset,
                    aeron::util::index_t              length,
                    aeron::Header&                   header)
    {
        const std::int64_t receiveNs = nowNs();

        // header.position() is the position the image has advanced to *after*
        // consuming this fragment; subtracting frameLength() gives the position
        // of the frame's first byte, which is what ReplayParams::position() needs
        // to replay starting at (and including) this exact message.
        const std::int64_t framePosition = header.position() - header.frameLength();

        char* const         raw = reinterpret_cast<char*>(buffer.buffer());
        const std::uint64_t cap = static_cast<std::uint64_t>(buffer.capacity());
        const std::uint64_t off = static_cast<std::uint64_t>(offset);
        const std::uint64_t len = static_cast<std::uint64_t>(length);

        if (len < HdrSbe::encodedLength() + HeaderComposite::encodedLength()) {
            std::fprintf(stderr, "[GlobalStreamClient] fragment too short: %" PRIu64 " bytes\n", len);
            return;
        }

        m_hdr.wrap(raw, off, 0U, cap);
        if (m_hdr.schemaId() != HdrSbe::sbeSchemaId()) {
            std::fprintf(stderr, "[GlobalStreamClient] unexpected schemaId=%u; ignored\n", m_hdr.schemaId());
            return;
        }

        const std::uint16_t templateId = m_hdr.templateId();
        const std::uint64_t bodyOff    = off + HdrSbe::encodedLength();

        // `header` is every message's first field, at a fixed offset right
        // after the 8-byte messageHeader — safe to decode before knowing the
        // rest of the message shape.
        m_header.wrap(raw, bodyOff, 0U, cap);
        const auto gseq  = m_header.globalSeqNo();
        const auto srcId = m_header.sourceId();
        const auto sessId = m_header.sessionId();
        const auto ts    = m_header.timestamp();

        if (templateId == CLIENT_CONNECTED_TEMPLATE_ID) {
            if (m_onConnected) {
                m_onConnected(LifecycleEvent{
                    .globalSeqNo      = gseq,
                    .sourceSessionId  = sessId,
                    .clusterTimestamp = ts,
                    .receiveTimeNs    = receiveNs
                });
            }
            return;
        }
        if (templateId == CLIENT_DISCONNECTED_TEMPLATE_ID) {
            if (m_onDisconnected) {
                m_onDisconnected(LifecycleEvent{
                    .globalSeqNo      = gseq,
                    .sourceSessionId  = sessId,
                    .clusterTimestamp = ts,
                    .receiveTimeNs    = receiveNs
                });
            }
            return;
        }

        if (m_onSequenced) {
            m_onSequenced(SequencedEvent{
                .globalSeqNo      = gseq,
                .sourceId         = srcId,
                .sourceSessionId  = sessId,
                .clusterTimestamp = ts,
                .receiveTimeNs    = receiveNs,
                .templateId       = templateId,
                .blockLength      = m_hdr.blockLength(),
                .version          = m_hdr.version(),
                .payload          = raw + off,
                .payloadLength    = len,
                .position         = framePosition
            });
        }
    }

    void notifyCaughtUp()
    {
        m_caughtUp = true;
        if (m_onCaughtUp) { m_onCaughtUp(); }
    }

    static std::int64_t nowNs()
    {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
    }

    // ── Callbacks ─────────────────────────────────────────────────────────────
    OnSequenced    m_onSequenced;
    OnConnected    m_onConnected;
    OnDisconnected m_onDisconnected;
    OnCaughtUp     m_onCaughtUp;

    // ── Aeron ─────────────────────────────────────────────────────────────────
    std::shared_ptr<aeron::Aeron>        m_aeron;
    std::int64_t                         m_replaySubRegId = -1;
    std::int64_t                         m_liveSubRegId   = -1;
    std::shared_ptr<aeron::Subscription> m_replaySub;
    std::shared_ptr<aeron::Subscription> m_liveSub;
    std::shared_ptr<aeron::Image>        m_replayImage; // null until resolved

    std::int64_t m_replaySessionId = -1;
    std::int64_t m_catchUpPosition = 0;
    bool         m_caughtUp        = false;

    aeron::fragment_handler_t m_fragmentHandler;

    // ── SBE decoders — single-threaded, reused per fragment ──────────────────
    HdrSbe          m_hdr;
    HeaderComposite m_header;
};

} // namespace org::limitless::phixeron::sequencer
