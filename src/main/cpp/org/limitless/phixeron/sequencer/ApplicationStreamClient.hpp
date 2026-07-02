#pragma once

#include <cstdint>
#include <cstdio>
#include <functional>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>

#include "Aeron.h"
#include "client/archive/AeronArchive.h"

#include "org/limitless/phixeron/sequencer/GlobalStreamClient.hpp"

namespace org::limitless::phixeron::sequencer
{

// ── ApplicationEvent ──────────────────────────────────────────────────────────

/**
 * Carries one application-layer message from the global stream.
 *
 * Lifecycle events (SourceConnected, SourceDisconnected) are filtered out;
 * only SequencedMessages with non-empty payloads reach this struct.
 *
 * payload/payloadLength address the raw application bytes that were originally
 * sent into the cluster — i.e. the AppMessage SBE prefix (10 bytes) has
 * already been stripped.  The pointer is valid only for the duration of the
 * callback; copy the data if it must survive.
 */
struct ApplicationEvent
{
    std::int64_t  globalSeqNo;       ///< monotonically increasing across all sources
    std::int64_t  sourceSessionId;   ///< cluster session that submitted this message
    std::int64_t  appSeqNo;          ///< per-source application sequence number
    std::int64_t  clusterTimestamp;  ///< consensus time (ms) when committed by the cluster
    std::int64_t  receiveTimeNs;     ///< wall-clock ns at receipt by this client
    const uint8_t* payload;          ///< raw application bytes (AppMessage SBE prefix removed)
    std::uint64_t  payloadLength;    ///< byte count of payload
};

// ── ApplicationStreamClient ───────────────────────────────────────────────────

/**
 * Self-contained client for the SequencerService global stream.
 *
 * Connects to the Aeron Archive, locates the global stream recording, starts
 * an archive replay (NULL_POSITION length → live follow-through on the same
 * image), and delivers only application messages to the caller.
 *
 * Lifecycle events (SourceConnected / SourceDisconnected) are filtered out;
 * the AppMessage SBE 10-byte prefix is stripped before the callback fires.
 *
 * Typical usage:
 * @code
 *   ApplicationStreamClient client(
 *       [](const ApplicationEvent& e) { ... },
 *       []() { printf("caught up to live stream\n"); });
 *
 *   client.start(aeron);
 *   while (running) client.poll();
 * @endcode
 */
class ApplicationStreamClient
{
public:
    // ── Configuration ─────────────────────────────────────────────────────────

    struct Config
    {
        const char* archiveControlChannel  = "aeron:udp?endpoint=localhost:9301";
        int32_t     archiveControlStream   = 100;
        const char* archiveResponseChannel = "aeron:udp?endpoint=localhost:0";
        const char* replayChannel          = "aeron:udp?endpoint=localhost:9311";
    };

    // ── Callbacks ─────────────────────────────────────────────────────────────

    using OnMessage  = std::function<void(const ApplicationEvent&)>;
    using OnCaughtUp = std::function<void()>;

    // ── Construction ──────────────────────────────────────────────────────────

    /**
     * @param onMessage  called for each application message (never for lifecycle events)
     * @param onCaughtUp called once when the replay image catches up to the live stream
     */
    explicit ApplicationStreamClient(OnMessage  onMessage,
                                     OnCaughtUp onCaughtUp = {})
        : m_onMessage(std::move(onMessage))
        , m_onCaughtUp(std::move(onCaughtUp))
    {}

    // ── Lifecycle ─────────────────────────────────────────────────────────────

    /**
     * Connects to the Aeron Archive, locates the global stream recording, and
     * starts the archive replay.  Blocks until the archive connection is
     * established and the recording is found; throws on failure.
     *
     * @param aeron  connected Aeron instance
     * @param config archive connection parameters (defaults match SequencerNode)
     */
    void start(std::shared_ptr<aeron::Aeron> aeron)
    {
        start(std::move(aeron), Config{});
    }

    void start(std::shared_ptr<aeron::Aeron> aeron,
               const Config&                 config)
    {
        m_aeron = std::move(aeron);

        // ── Connect to the Aeron Archive ─────────────────────────────────────
        aeron::archive::client::Context archiveCtx;
        archiveCtx.aeron(m_aeron)
                  .controlRequestChannel(config.archiveControlChannel)
                  .controlRequestStreamId(config.archiveControlStream)
                  .controlResponseChannel(config.archiveResponseChannel);

        m_archive = aeron::archive::client::AeronArchive::connect(archiveCtx);

        // ── Find the global stream recording ─────────────────────────────────
        std::int64_t catchUpPosition = 0;
        const std::int64_t recordingId = findRecording(*m_archive, catchUpPosition);
        std::printf("[ApplicationStreamClient] Recording %" PRId64
                    "  catchUpPosition=%" PRId64 "\n",
                    recordingId, catchUpPosition);

        // ── Start replay only when there is historical data to replay ─────────
        std::int64_t replaySessionId = -1;
        if (catchUpPosition > 0) {
            aeron::archive::client::ReplayParams replayParams;
            replayParams.position(0).length(aeron::archive::client::NULL_LENGTH);
            replaySessionId = m_archive->startReplay(
                recordingId, config.replayChannel, REPLAY_STREAM_ID, replayParams);
            std::printf("[ApplicationStreamClient] Replay started"
                        "  replaySessionId=%" PRId64 "\n", replaySessionId);
        } else {
            std::puts("[ApplicationStreamClient] No historical data — subscribing to live stream");
        }

        // ── Wire up GlobalStreamClient (application messages only) ────────────
        m_globalStream = std::make_unique<GlobalStreamClient>(
            [this](const SequencedEvent& e) { onSequenced(e); },
            /*onConnected=*/nullptr,         // lifecycle events are filtered out
            /*onDisconnected=*/nullptr,
            [this]() { if (m_onCaughtUp) m_onCaughtUp(); }
        );

        m_globalStream->start(m_aeron, replaySessionId, catchUpPosition, config.replayChannel);
    }

    /**
     * Polls the active image for up to FRAGMENT_LIMIT fragments.
     * Must be called regularly from the application's duty-cycle loop.
     * @return number of fragments consumed
     */
    int poll()
    {
        return m_globalStream ? m_globalStream->poll() : 0;
    }

    bool isCaughtUp() const
    {
        return m_globalStream && m_globalStream->isCaughtUp();
    }

private:
    // ── Archive helpers ───────────────────────────────────────────────────────

    static std::int64_t findRecording(aeron::archive::client::AeronArchive& archive,
                                      std::int64_t&                          catchUpPos)
    {
        // Prefer the active (live) recording; fall back to the stopped one with
        // the highest stop position. NULL_POSITION means the recording is active.
        std::int64_t activeId   = -1;
        std::int64_t stoppedId  = -1;
        std::int64_t stoppedPos = std::numeric_limits<std::int64_t>::min();

        archive.listRecordingsForUri(
            0, std::numeric_limits<std::int32_t>::max(),
            GLOBAL_STREAM_CHANNEL, GLOBAL_STREAM_ID,
            [&](aeron::archive::client::RecordingDescriptor& desc) {
                if (desc.m_stopPosition == aeron::archive::client::NULL_POSITION) {
                    activeId = desc.m_recordingId;
                } else if (desc.m_stopPosition > stoppedPos) {
                    stoppedId  = desc.m_recordingId;
                    stoppedPos = desc.m_stopPosition;
                }
            });

        if (activeId < 0 && stoppedId < 0)
            throw std::runtime_error(
                std::string("[ApplicationStreamClient] No global stream recording on ")
                + GLOBAL_STREAM_CHANNEL);

        if (activeId >= 0) {
            catchUpPos = archive.getRecordingPosition(activeId);
            if (catchUpPos == aeron::archive::client::NULL_POSITION) catchUpPos = 0;
            return activeId;
        }

        catchUpPos = stoppedPos;
        return stoppedId;
    }

    // ── Fragment handler ──────────────────────────────────────────────────────

    void onSequenced(const SequencedEvent& e)
    {
        // Filter: drop messages whose payload is too short to contain application bytes.
        if (e.payloadLength <= APP_MSG_SBE_PREFIX || !m_onMessage) return;

        // Strip the 10-byte AppMessage SBE prefix (8-byte SBE header + 2-byte length field).
        const auto* appBytes = reinterpret_cast<const uint8_t*>(e.payload)
                               + APP_MSG_SBE_PREFIX;
        const std::uint64_t appLen = e.payloadLength - APP_MSG_SBE_PREFIX;

        m_onMessage(ApplicationEvent{
            .globalSeqNo      = e.globalSeqNo,
            .sourceSessionId  = e.sourceSessionId,
            .appSeqNo         = e.appSeqNo,
            .clusterTimestamp = e.clusterTimestamp,
            .receiveTimeNs    = e.receiveTimeNs,
            .payload          = appBytes,
            .payloadLength    = appLen
        });
    }

    // ── State ─────────────────────────────────────────────────────────────────

    OnMessage  m_onMessage;
    OnCaughtUp m_onCaughtUp;

    std::shared_ptr<aeron::Aeron>                       m_aeron;
    std::shared_ptr<aeron::archive::client::AeronArchive> m_archive;
    std::unique_ptr<GlobalStreamClient>                 m_globalStream;
};

} // namespace org::limitless::phixeron::sequencer
