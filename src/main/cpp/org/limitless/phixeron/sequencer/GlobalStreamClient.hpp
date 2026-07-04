#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <functional>
#include <memory>
#include <stdexcept>
#include <string>

#include "Aeron.h"
#include "FragmentAssembler.h"

// Generated SBE C++ codecs from sbe-sequenced.xml (via GenerateSequencedSbeCodecs)
#include "org_limitless_phixeron_sbe_sequenced/MessageHeader.h"
#include "org_limitless_phixeron_sbe_sequenced/Header.h"

namespace org::limitless::phixeron::sequencer
{

// ── Constants matching SequencerService / SequencerNode ──────────────────────

inline constexpr const char* GLOBAL_STREAM_CHANNEL =
    "aeron:udp?endpoint=224.0.1.1:9200|interface=localhost";
inline constexpr std::int32_t GLOBAL_STREAM_ID = 1;

// Each binary uses a distinct port so their archive replay publications don't conflict.
// fix_session_client      → 9310
// application_stream_client → 9311
inline constexpr std::int32_t REPLAY_STREAM_ID = 110;

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
 * the client falls back to a direct multicast subscription.
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
     * multicast fallback subscription.
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

        // Live multicast fallback — always subscribed; used when replay image closes.
        m_liveSubRegId = m_aeron->addSubscription(GLOBAL_STREAM_CHANNEL, GLOBAL_STREAM_ID);

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

        // Live multicast fallback — poll the subscription directly.
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
                    aeron::Header&                   /*header*/)
    {
        const std::int64_t receiveNs = nowNs();

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
                .payloadLength    = len
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
