#pragma once

#include <algorithm>
#include <cinttypes> // PRIu64
#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "Aeron.h"
#include "FragmentAssembler.h"
#include "client/archive/AeronArchive.h"

#include "org/limitless/seqeron/protocol/PortLayout.hpp"
#include "org/limitless/seqeron/protocol/SequencedFrame.hpp"
#include "org/limitless/seqeron/util/Env.hpp"
#include "org/limitless/seqeron/util/Logger.hpp"

// Reading the sequenced stream back out of an Aeron Archive: finding the recordings, connecting to the
// archive that holds them, and walking them in order. Not the live path — consumers follow the tap
// through ReplayerStreamReceiver; this serves bounded scans. Frame decoding alone is SequencedFrame.hpp.

namespace org::limitless::seqeron::sequencer::client {

// A UDP-replaying binary uses a port of its own, outside core's reserved block (doc/ops.md, "Ports");
// a client co-located with the archive replays over REPLAY_CHANNEL_IPC and needs none.
inline constexpr std::int32_t ARCHIVE_REPLAY_STREAM_ID = 110;

// Replay channel for a client co-located with the archive it's replaying from.
inline constexpr const char* REPLAY_CHANNEL_IPC = "aeron:ipc";

/**
 * Resolves a UDP replay channel on localhost, its port from the environment so two instances of the same
 * binary can run on one host without a port clash.
 *
 * @param envVar      the variable holding the port
 * @param defaultPort the port when envVar is unset or empty
 * @return the channel, "aeron:udp?endpoint=localhost:<port>"
 */
inline std::string resolveReplayChannel(const char* envVar, std::uint16_t defaultPort)
{
    return protocol::udpChannel("localhost:" + std::to_string(util::envInt(envVar, defaultPort)));
}

// Default 3-node cluster archive control endpoints, from PortLayout.hpp. Every member's archive holds an
// identical recording, so any reachable one will do.
inline const std::string DEFAULT_ARCHIVE_ENDPOINTS = protocol::archiveEndpointsCsv(protocol::CLUSTER_MEMBER_COUNT);

/**
 * Resolves the archive control endpoints to try, from the environment.
 *
 * @param envVar     the variable holding a comma-separated "host:port,host:port,..." list
 * @param defaultCsv the list, in the same format, when envVar is unset or empty
 * @return the endpoints, in list order
 */
inline std::vector<std::string> resolveArchiveEndpoints(const char* envVar, const std::string& defaultCsv)
{
    const std::string csv = util::envString(envVar, defaultCsv);

    std::vector<std::string> endpoints;
    std::size_t start = 0;
    while (start <= csv.size())
    {
        const std::size_t comma = csv.find(',', start);
        const std::size_t end = (comma == std::string::npos) ? csv.size() : comma;
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
 * Looks up the cluster stream recording on an already-connected archive, preferring the
 * active (live) recording over any stopped one; among stopped recordings prefers the
 * largest stop position (holds the most committed data).
 *
 * @param archive              a connected archive client to search
 * @param[out] recordingId    recording id of the cluster stream found on the archive
 * @param[out] catchUpPosition recording position to replay/catch up to
 * @return false if the archive holds no FEEDER_STREAM_ID recording at all (leaving both
 *         out-parameters untouched).
 */
inline bool findClusterStreamRecording(const std::shared_ptr<aeron::archive::client::AeronArchive>& archive,
                                       std::int64_t& recordingId, std::int64_t& catchUpPosition)
{
    std::int64_t activeId = -1;
    std::int64_t stoppedId = -1;
    std::int64_t stoppedPosition = std::numeric_limits<std::int64_t>::min();
    archive->listRecordingsForUri(0, std::numeric_limits<std::int32_t>::max(), "", protocol::FEEDER_STREAM_ID,
                                  [&](aeron::archive::client::RecordingDescriptor& recording) {
                                      if (recording.m_stopPosition == aeron::archive::client::NULL_POSITION)
                                      {
                                          activeId = recording.m_recordingId;
                                      }
                                      else if (recording.m_stopPosition > stoppedPosition)
                                      {
                                          stoppedId = recording.m_recordingId;
                                          stoppedPosition = recording.m_stopPosition;
                                      }
                                  });

    if (activeId < 0 && stoppedId < 0)
    {
        return false;
    }

    if (activeId >= 0)
    {
        catchUpPosition = archive->getRecordingPosition(activeId);
        if (catchUpPosition == aeron::archive::client::NULL_POSITION)
        {
            catchUpPosition = 0;
        }
        recordingId = activeId;
    }
    else
    {
        catchUpPosition = stoppedPosition;
        recordingId = stoppedId;
    }
    return true;
}

/**
 * Connects to each candidate archive endpoint in turn until one both connects
 * and holds a recording of the sequenced stream (matched by FEEDER_STREAM_ID
 * alone). Every member records its own node-local tap, so any reachable
 * member's archive holds a full copy; trying more than one endpoint is just
 * defense in depth against an individual member being down or still starting up
 * (its recording not yet active). This connects to a remote archive over UDP
 * control and replays over a UDP replay channel — the recording's aeron:ipc
 * source is irrelevant to replay.
 *
 * @param aeron                the client the archive connection is made on
 * @param controlEndpoints     candidate archive control endpoints, "host:port", tried in order
 * @param controlStreamId      the archives' control request stream id
 * @param controlResponseChannel this client's own channel for the archive's control responses
 * @param logPrefix            prepended to every log line and to the exception message
 * @param[out] recordingId    recording id of the sequenced stream found on the
 *                            connected archive
 * @param[out] catchUpPosition recording position to replay/catch up to
 * @throws std::runtime_error if no candidate endpoint both connects and holds
 *         a sequenced-stream recording.
 */
inline std::shared_ptr<aeron::archive::client::AeronArchive> connectToArchiveWithClusterStream(
    std::shared_ptr<aeron::Aeron> aeron, const std::vector<std::string>& controlEndpoints, std::int32_t controlStreamId,
    const char* controlResponseChannel, const char* logPrefix, std::int64_t& recordingId, std::int64_t& catchUpPosition)
{
    std::string lastError = "no candidate endpoints given";
    for (const auto& endpoint : controlEndpoints)
    {
        std::shared_ptr<aeron::archive::client::AeronArchive> archive;
        try
        {
            aeron::archive::client::Context archiveCtx;
            archiveCtx.aeron(aeron)
                .controlRequestChannel(protocol::udpChannel(endpoint))
                .controlRequestStreamId(controlStreamId)
                .controlResponseChannel(controlResponseChannel);
            archive = aeron::archive::client::AeronArchive::connect(archiveCtx);
        }
        catch (const std::exception& ex)
        {
            util::Logger::warn(util::component::ClusterStreamClient, util::eventCode::ArchiveConnectFailed,
                               "%s Archive connect to %s failed: %s", logPrefix, endpoint.c_str(), ex.what());
            lastError = ex.what();
            continue;
        }

        if (!findClusterStreamRecording(archive, recordingId, catchUpPosition))
        {
            util::Logger::info(util::component::ClusterStreamClient,
                               "%s Connected to %s but it has no cluster stream recording"
                               " (not currently/recently leader) — trying next endpoint",
                               logPrefix, endpoint.c_str());
            lastError = "connected but no cluster stream recording found on " + endpoint;
            continue;
        }

        util::Logger::info(util::component::ClusterStreamClient,
                           "%s Connected to Aeron Archive at %s (holds the cluster stream recording)", logPrefix,
                           endpoint.c_str());
        return archive;
    }

    throw std::runtime_error(std::string(logPrefix) + " Could not find the cluster stream recording on any of " +
                             std::to_string(controlEndpoints.size()) +
                             " archive endpoint(s); last error: " + lastError);
}

/**
 * Connects to the archive co-located with this process over "aeron:ipc" — used by clients
 * deliberately deployed sharing a single SequencerServer member's own
 * Aeron directory (see ClusterStreamSender::connectColocated's doc comment for the ingress
 * half of that deployment). Unlike connectToArchiveWithClusterStream, there is exactly one
 * candidate archive here, and — because every member records its own node-local tap — every
 * member's archive holds a full copy of the sequenced stream regardless of current leadership,
 * so a missing recording here is a real error, not just "wrong member to ask".
 *
 * @param aeron                the client the archive connection is made on
 * @param controlStreamId      the local archive's control request stream id
 * @param logPrefix            prepended to the log line and to the exception message
 * @param[out] recordingId    recording id of the cluster stream found on the local archive
 * @param[out] catchUpPosition recording position to replay/catch up to
 * @throws std::runtime_error if the local archive can't be reached, or holds no cluster
 *         stream recording at all.
 */
inline std::shared_ptr<aeron::archive::client::AeronArchive> connectLocalArchive(std::shared_ptr<aeron::Aeron> aeron,
                                                                                 std::int32_t controlStreamId,
                                                                                 const char* logPrefix,
                                                                                 std::int64_t& recordingId,
                                                                                 std::int64_t& catchUpPosition)
{
    aeron::archive::client::Context archiveCtx;
    archiveCtx.aeron(aeron)
        .controlRequestChannel(protocol::ARCHIVE_CONTROL_CHANNEL)
        .controlRequestStreamId(controlStreamId)
        .controlResponseChannel(protocol::ARCHIVE_CONTROL_CHANNEL);
    auto archive = aeron::archive::client::AeronArchive::connect(archiveCtx);

    if (!findClusterStreamRecording(archive, recordingId, catchUpPosition))
    {
        throw std::runtime_error(std::string(logPrefix) + " Co-located archive has no cluster stream recording");
    }

    util::Logger::info(util::component::ClusterStreamClient,
                       "%s Connected to co-located Aeron Archive via IPC (holds the cluster stream recording)",
                       logPrefix);
    return archive;
}

/**
 * One recording of the cluster stream, as found in a single archive's catalog.
 * stopPosition is NULL_POSITION when the recording is still active (only
 * possible for the last segment in a resolveClusterStreamSegments() result).
 */
struct RecordingSegment
{
    std::int64_t recordingId;
    std::int64_t stopPosition;
};

/**
 * Lists every FEEDER_STREAM_ID recording on an already-connected archive,
 * ordered oldest-to-newest by recordingId — each one is a prior leader's
 * tenure, so replaying them in this order and concatenating reproduces full
 * history. The current leader's own archive holds every earlier tenure's
 * segment too, because every follower continuously replicates the leader's
 * recording into its own archive the whole time it isn't leader.
 * recordingId is monotone as the archive creates recordings; startTimestamp
 * is archive wall clock, which a backward clock step can invert.
 *
 * The abrupt-leader-death race in that same area can
 * leave two segments both reporting stopPosition == NULL_POSITION (active);
 * since the newer holds the older's content, only the most recent is kept and
 * any earlier "active" duplicate is dropped rather than replayed twice.
 *
 * @param archive a connected archive client to list
 * @return the segments, oldest first; empty if the archive holds none
 */
inline std::vector<RecordingSegment> resolveClusterStreamSegments(
    const std::shared_ptr<aeron::archive::client::AeronArchive>& archive)
{
    struct Entry
    {
        std::int64_t recordingId;
        std::int64_t stopPosition;
    };
    std::vector<Entry> entries;
    archive->listRecordingsForUri(0, std::numeric_limits<std::int32_t>::max(), "", protocol::FEEDER_STREAM_ID,
                                  [&](aeron::archive::client::RecordingDescriptor& recording) {
                                      entries.push_back({ recording.m_recordingId, recording.m_stopPosition });
                                  });

    std::ranges::sort(entries, [](const Entry& a, const Entry& b) { return a.recordingId < b.recordingId; });

    std::int64_t newestActiveId = -1;
    for (const auto& e : entries)
    {
        if (e.stopPosition == aeron::archive::client::NULL_POSITION)
        {
            newestActiveId = e.recordingId;
        }
    }

    std::vector<RecordingSegment> segments;
    for (const auto& e : entries)
    {
        const bool stale =
            (e.stopPosition == aeron::archive::client::NULL_POSITION) && (e.recordingId != newestActiveId);
        if (!stale)
        {
            segments.push_back({ e.recordingId, e.stopPosition });
        }
    }
    return segments;
}

// ── ClusterStreamClient ───────────────────────────────────────────────────────

/**
 * Replays the recorded sequenced stream from an archive, decoding and dispatching
 * SBE messages to the caller.
 *
 * Startup sequence (caller is responsible for the archive connection):
 *   1. Caller uses resolveClusterStreamSegments(archive) to list every
 *      FEEDER_STREAM_ID recording on the connected archive, oldest first.
 *   2. Call start(aeron, archive, segments, replayChannel).
 *   3. Call poll() in a duty-cycle loop.
 *
 * Each historical (stopped) segment is replayed in full before moving on to
 * the next; the last segment (which may still be actively recording) is
 * replayed with NULL_LENGTH so it follows the recording's growth seamlessly
 * once caught up — the same image delivers both historical and live messages
 * without a subscription switch. Since every node records its own continuous
 * tap, that last recording spans every leader failover and keeps growing as
 * long as its member is up, so there is no live network fallback: the open-ended
 * replay is the live feed. Segments share one underlying replay subscription
 * (same channel/stream id), so moving from one segment's replay to the next only
 * requires starting a new archive replay session, not a new local subscription.
 *
 * Every message is stamped with receiveTimeNs (std::chrono::system_clock).
 */
class ClusterStreamClient
{
  public:
    using OnSequenced = std::function<void(const protocol::SequencedEvent&)>;
    using OnConnected = std::function<void(const protocol::LifecycleEvent&)>;
    using OnDisconnected = std::function<void(const protocol::LifecycleEvent&)>;
    using OnCaughtUp = std::function<void()>;
    // Fired (single-image mode only) if the replay image closes before catchUpPosition — an invalid range
    // or a truncated recording — so a caller need not wait out a stall timeout.
    using OnReplayEnded = std::function<void()>;

    /**
     * Creates a client that has not started.
     *
     * @param onSequenced    every frame but the connection lifecycle pair, in globalSeqNo order
     * @param onConnected    each ConnectionOpened; empty drops them, leaving a hole in globalSeqNo
     * @param onDisconnected each ConnectionClosed; empty drops them, leaving a hole in globalSeqNo
     * @param onCaughtUp     once the replay reaches its catch-up position
     * @param onReplayEnded  single-image mode only: the replay image closed before its catch-up position
     */
    explicit ClusterStreamClient(OnSequenced onSequenced, OnConnected onConnected = {},
                                 OnDisconnected onDisconnected = {}, OnCaughtUp onCaughtUp = {},
                                 OnReplayEnded onReplayEnded = {}) :
      m_onSequenced(std::move(onSequenced)),
      m_onConnected(std::move(onConnected)),
      m_onDisconnected(std::move(onDisconnected)),
      m_onCaughtUp(std::move(onCaughtUp)),
      m_onReplayEnded(std::move(onReplayEnded)),
      m_fragmentHandler([this](auto& buf, auto off, auto len, auto& hdr) { onFragment(buf, off, len, hdr); }),
      m_assembler(std::make_unique<aeron::FragmentAssembler>(m_fragmentHandler)),
      m_poll(m_assembler->handler())
    {}

    /**
     * When segments is non-empty, starts replaying them in order (see class doc comment); the last
     * (active) segment is replayed open-ended so it follows the recording's growth as the live feed.
     *
     * @param aeron         connected Aeron instance
     * @param archive       connected AeronArchive the segments were resolved
     *                      from; kept alive to start each segment's replay
     * @param segments      result of resolveClusterStreamSegments(archive),
     *                      oldest first; empty means no historical data
     * @param replayChannel channel the archive publishes replays on;
     *                      ignored when segments is empty
     */
    void start(std::shared_ptr<aeron::Aeron> aeron, std::shared_ptr<aeron::archive::client::AeronArchive> archive,
               std::vector<RecordingSegment> segments, const char* replayChannel = nullptr)
    {
        m_aeron = std::move(aeron);
        m_archive = std::move(archive);
        m_segments = std::move(segments);

        if (replayChannel != nullptr)
        {
            m_replayChannel = replayChannel;
        }

        if (m_segments.empty())
        {
            // No historical data — already at live.
            notifyCaughtUp();
            return;
        }

        const auto& last = m_segments.back();
        if (last.stopPosition == aeron::archive::client::NULL_POSITION)
        {
            m_catchUpPosition = m_archive->getRecordingPosition(last.recordingId);
            if (m_catchUpPosition == aeron::archive::client::NULL_POSITION)
            {
                m_catchUpPosition = 0;
            }
        }
        else
        {
            m_catchUpPosition = last.stopPosition;
        }

        m_replaySubRegId = m_aeron->addSubscription(m_replayChannel, ARCHIVE_REPLAY_STREAM_ID);
        startSegmentReplay(0);
    }

    /**
     * Attaches to a single already-started archive replay image — for a bounded scan of one
     * already-known recording, not the multi-segment bootstrap walk above. Used by
     * FixConnection::replayMissingAppMessages's resend-recovery scan, which replays a specific
     * position range within one recording (not from position 0, and not following live once it ends).
     * Completion is detected by position (catchUpPosition), so there is no live subscription.
     *
     * @param aeron           connected Aeron instance
     * @param replaySessionId session ID returned by AeronArchive::startReplay(),
     *                        or -1 when there is no historical data to replay
     * @param catchUpPosition recording position onCaughtUp fires once reached
     * @param replayChannel   channel the archive publishes the replay on;
     *                        ignored when replaySessionId < 0
     */
    void start(std::shared_ptr<aeron::Aeron> aeron, std::int64_t replaySessionId, std::int64_t catchUpPosition,
               const char* replayChannel = nullptr)
    {
        m_aeron = std::move(aeron);
        m_singleImageMode = true;
        m_replaySessionId = replaySessionId;
        m_catchUpPosition = catchUpPosition;

        if (replaySessionId >= 0 && replayChannel != nullptr)
        {
            m_replaySubRegId = m_aeron->addSubscription(replayChannel, ARCHIVE_REPLAY_STREAM_ID);
        }

        if (replaySessionId < 0)
        {
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
        // Lazily resolve the replay subscription once it becomes available.
        if (!m_replaySub && m_replaySubRegId >= 0)
        {
            m_replaySub = m_aeron->findSubscription(m_replaySubRegId);
        }

        // Lazily resolve the current segment's replay image once it becomes available.
        if (!m_replayImage && m_replaySub)
        {
            m_replayImage = m_replaySub->imageBySessionId(static_cast<std::int32_t>(m_replaySessionId));
        }

        if (m_replayImage)
        {
            if (!m_replayImage->isClosed())
            {
                const int work = m_replayImage->poll(m_poll, FRAGMENT_LIMIT);
                if (!m_caughtUp && isOnLastSegment() && m_replayImage->position() >= m_catchUpPosition)
                {
                    notifyCaughtUp();
                }
                return work;
            }
            if (m_singleImageMode && !m_caughtUp)
            {
                if (m_onReplayEnded)
                {
                    m_onReplayEnded();
                }
            }
            m_replayImage.reset();
            ++m_segmentIndex;
            if (m_segmentIndex < m_segments.size())
            {
                startSegmentReplay(m_segmentIndex);
            }
        }
        return 0;
    }

    bool isCaughtUp() const
    {
        return m_caughtUp;
    }

    // Current replay image position, or -1 if no image has resolved yet.
    [[nodiscard]] std::int64_t replayImagePosition() const
    {
        return m_replayImage ? m_replayImage->position() : -1;
    }

  private:
    static constexpr int FRAGMENT_LIMIT = 10;

    bool isOnLastSegment() const
    {
        return m_singleImageMode || m_segmentIndex + 1 == m_segments.size();
    }

    void startSegmentReplay(std::size_t index)
    {
        const auto& segment = m_segments[index];
        const bool isLast = (index + 1 == m_segments.size());

        aeron::archive::client::ReplayParams replayParams;
        replayParams.position(0).length(isLast ? aeron::archive::client::NULL_LENGTH : segment.stopPosition);
        m_replaySessionId =
            m_archive->startReplay(segment.recordingId, m_replayChannel, ARCHIVE_REPLAY_STREAM_ID, replayParams);
    }

    void onFragment(const aeron::concurrent::AtomicBuffer& buffer, const aeron::util::index_t offset,
                    const aeron::util::index_t length, const aeron::Header& header)
    {
        const std::int64_t receiveNs = protocol::nowNs();
        const std::int64_t framePosition = protocol::frameStartPosition(header);
        char* const raw = reinterpret_cast<char*>(buffer.buffer());
        const std::uint64_t off = static_cast<std::uint64_t>(offset);
        const std::uint64_t len = static_cast<std::uint64_t>(length);
        const protocol::FrameView view = protocol::unwrapFrame(raw + off, len);
        if (!view.valid)
        {
            util::Logger::error(util::component::ClusterStreamClient, util::eventCode::FragmentTooShort,
                                "unreadable frame of %" PRIu64 " bytes; ignored", len);
            return;
        }

        const auto gseq = view.globalSeqNo;
        if (!m_singleImageMode)
        {
            if (m_lastGlobalSeqNo != 0 && gseq <= m_lastGlobalSeqNo)
            {
                return; // already delivered (overlapping recording after a restart)
            }
            m_lastGlobalSeqNo = gseq;
        }
        const bool isSystem = view.system;
        if (isSystem && view.systemEventType == protocol::CONNECTION_OPENED)
        {
            if (m_onConnected)
            {
                m_onConnected(protocol::lifecycleEventOf(view, receiveNs));
            }
            return;
        }
        if (isSystem && view.systemEventType == protocol::CONNECTION_CLOSED)
        {
            if (m_onDisconnected)
            {
                m_onDisconnected(protocol::lifecycleEventOf(view, receiveNs));
            }
            return;
        }
        if (m_onSequenced)
        {
            m_onSequenced(protocol::sequencedEventOf(view, receiveNs, framePosition));
        }
    }

    void notifyCaughtUp()
    {
        m_caughtUp = true;
        if (m_onCaughtUp)
        {
            m_onCaughtUp();
        }
    }

    OnSequenced m_onSequenced;
    OnConnected m_onConnected;
    OnDisconnected m_onDisconnected;
    OnCaughtUp m_onCaughtUp;
    OnReplayEnded m_onReplayEnded;

    std::shared_ptr<aeron::Aeron> m_aeron;
    std::shared_ptr<aeron::archive::client::AeronArchive> m_archive;
    std::int64_t m_replaySubRegId = -1;
    std::shared_ptr<aeron::Subscription> m_replaySub;
    std::shared_ptr<aeron::Image> m_replayImage;

    std::vector<RecordingSegment> m_segments;
    std::size_t m_segmentIndex = 0;
    bool m_singleImageMode = false;
    std::string m_replayChannel;
    std::int64_t m_replaySessionId = -1;
    std::int64_t m_catchUpPosition = 0;
    bool m_caughtUp = false;

    std::int64_t m_lastGlobalSeqNo = 0; // highest globalSeqNo delivered; 0 = none yet (overlap de-dup)

    aeron::fragment_handler_t m_fragmentHandler;

    // Reassembly. A sequenced frame can exceed the IPC MTU.
    std::unique_ptr<aeron::FragmentAssembler> m_assembler;

    // Composed once: FragmentAssembler::handler() builds a fresh std::function per call, and this is
    // polled every duty-cycle iteration.
    aeron::fragment_handler_t m_poll;
};

} // namespace org::limitless::seqeron::sequencer::client
