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

// Generated SBE C++ codecs from sequencer.xml (via GenerateSeqSbeCodecs)
#include "org/limitless/phixeron/sbe/sequencer/MessageHeader.h"
#include "org/limitless/phixeron/sbe/sequencer/SequencedMessage.h"
#include "org/limitless/phixeron/sbe/sequencer/SourceConnected.h"
#include "org/limitless/phixeron/sbe/sequencer/SourceDisconnected.h"

namespace org::limitless::phixeron::sequencer
{

// ── Constants matching SequencerService / SequencerNode ──────────────────────

inline constexpr const char* GLOBAL_STREAM_CHANNEL =
    "aeron:udp?endpoint=224.0.1.1:9200|interface=localhost";
inline constexpr std::int32_t GLOBAL_STREAM_ID = 1;

inline constexpr const char* REPLAY_CHANNEL   = "aeron:udp?endpoint=localhost:0";
inline constexpr std::int32_t REPLAY_STREAM_ID = 110;

// ── Event types delivered to the application ─────────────────────────────────

/**
 * Carries one SequencedMessage from the global stream.
 *
 * payload/payloadLength point into the Aeron fragment buffer and are valid
 * only for the duration of the callback. Copy the data before returning if
 * it must survive.
 *
 * The payload bytes are the raw AppMessage SBE envelope written by
 * SequencerClient::send().  Layout:
 *   [0-7]  SBE MessageHeader  (8 bytes, schemaId=201, templateId=1)
 *   [8-9]  varData length     (uint16 LE, 2 bytes)
 *   [10..] actual FIX content
 * Use APP_MSG_SBE_PREFIX (10) to skip to the FIX bytes.
 */
struct SequencedEvent
{
    std::int64_t  globalSeqNo;
    std::int64_t  sourceSessionId;
    std::int64_t  appSeqNo;
    std::int64_t  clusterTimestamp;  ///< cluster consensus time (ms) when message was committed
    std::int64_t  receiveTimeNs;     ///< wall-clock ns at receipt by this client
    const char*   payload;           ///< raw AppMessage SBE bytes (see struct comment)
    std::uint64_t payloadLength;     ///< total byte count including the 10-byte prefix
};

/** Offset past the AppMessage SBE prefix to the embedded FIX content. */
inline constexpr std::uint64_t APP_MSG_SBE_PREFIX = 10U;

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
     * @param replaySessionId session ID returned by AeronArchive::startReplay()
     * @param catchUpPosition recording stop position observed at startup;
     *                        onCaughtUp fires once the replay image reaches it
     */
    void start(std::shared_ptr<aeron::Aeron> aeron,
               std::int64_t                  replaySessionId,
               std::int64_t                  catchUpPosition)
    {
        m_aeron           = std::move(aeron);
        m_replaySessionId = replaySessionId;
        m_catchUpPosition = catchUpPosition;

        m_replaySub = m_aeron->addSubscription(REPLAY_CHANNEL, REPLAY_STREAM_ID);

        // Fallback for when the replay image closes (leader failover).
        m_liveSub = m_aeron->addSubscription(GLOBAL_STREAM_CHANNEL, GLOBAL_STREAM_ID);

        if (catchUpPosition <= 0) {
            // Nothing to replay; already live.
            notifyCaughtUp();
        }
    }

    /**
     * Polls the active image for up to FRAGMENT_LIMIT fragments.
     * @return number of fragments consumed
     */
    int poll()
    {
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

    using SeqSbe   = org::limitless::phixeron::sbe::sequencer::SequencedMessage;
    using ConnSbe  = org::limitless::phixeron::sbe::sequencer::SourceConnected;
    using DiscSbe  = org::limitless::phixeron::sbe::sequencer::SourceDisconnected;
    using HdrSbe   = org::limitless::phixeron::sbe::sequencer::MessageHeader;

    void onFragment(aeron::concurrent::AtomicBuffer& buffer,
                    aeron::util::index_t              offset,
                    aeron::util::index_t             /*length*/,
                    aeron::Header&                   /*header*/)
    {
        const std::int64_t receiveNs = nowNs();

        char* const         raw = reinterpret_cast<char*>(buffer.buffer());
        const std::uint64_t cap = static_cast<std::uint64_t>(buffer.capacity());
        const std::uint64_t off = static_cast<std::uint64_t>(offset);

        m_hdr.wrap(raw, off, 0U, cap);

        const std::uint16_t templateId = m_hdr.templateId();
        const std::uint16_t blockLen   = m_hdr.blockLength();
        const std::uint16_t version    = m_hdr.version();
        const std::uint64_t bodyOff    = off + HdrSbe::encodedLength();

        switch (templateId)
        {
        case SeqSbe::sbeTemplateId():
            m_seqMsg.wrap(raw, bodyOff, blockLen, version, cap);
            if (m_onSequenced) {
                const auto gseq  = m_seqMsg.globalSeqNo();
                const auto srcId = m_seqMsg.sourceSessionId();
                const auto aseq  = m_seqMsg.appSeqNo();
                const auto ts    = m_seqMsg.clusterTimestamp();
                const auto plen  = m_seqMsg.payloadLength();
                const auto pdata = m_seqMsg.payload();
                m_onSequenced(SequencedEvent{
                    .globalSeqNo      = gseq,
                    .sourceSessionId  = srcId,
                    .appSeqNo         = aseq,
                    .clusterTimestamp = ts,
                    .receiveTimeNs    = receiveNs,
                    .payload          = pdata,
                    .payloadLength    = plen
                });
            }
            break;

        case ConnSbe::sbeTemplateId():
            m_srcConn.wrap(raw, bodyOff, blockLen, version, cap);
            if (m_onConnected) {
                m_onConnected(LifecycleEvent{
                    .globalSeqNo      = m_srcConn.globalSeqNo(),
                    .sourceSessionId  = m_srcConn.sourceSessionId(),
                    .clusterTimestamp = m_srcConn.clusterTimestamp(),
                    .receiveTimeNs    = receiveNs
                });
            }
            break;

        case DiscSbe::sbeTemplateId():
            m_srcDisc.wrap(raw, bodyOff, blockLen, version, cap);
            if (m_onDisconnected) {
                m_onDisconnected(LifecycleEvent{
                    .globalSeqNo      = m_srcDisc.globalSeqNo(),
                    .sourceSessionId  = m_srcDisc.sourceSessionId(),
                    .clusterTimestamp = m_srcDisc.clusterTimestamp(),
                    .receiveTimeNs    = receiveNs
                });
            }
            break;

        default:
            std::fprintf(stderr,
                "[GlobalStreamClient] Unknown SBE templateId=%u; ignored\n",
                templateId);
            break;
        }
    }

    void notifyCaughtUp()
    {
        m_caughtUp = true;
        if (m_onCaughtUp) m_onCaughtUp();
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
    std::shared_ptr<aeron::Subscription> m_replaySub;
    std::shared_ptr<aeron::Subscription> m_liveSub;
    std::shared_ptr<aeron::Image>        m_replayImage; // null until resolved

    std::int64_t m_replaySessionId = -1;
    std::int64_t m_catchUpPosition = 0;
    bool         m_caughtUp        = false;

    aeron::fragment_handler_t m_fragmentHandler;

    // ── SBE decoders — single-threaded, reused per fragment ──────────────────
    HdrSbe  m_hdr;
    SeqSbe  m_seqMsg;
    ConnSbe m_srcConn;
    DiscSbe m_srcDisc;
};

} // namespace org::limitless::phixeron::sequencer
