#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "Aeron.h"
#include "client/archive/AeronArchive.h"

// Generated SBE C++ codecs from sbe-sequenced.xml (via GenerateSequencedSbeCodecs)
#include "org_limitless_phixeron_sbe_sequenced/Header.h"
#include "org_limitless_phixeron_sbe_sequenced/MessageHeader.h"

namespace org::limitless::phixeron::sequencer {

// ── Constants matching SequencerService / SequencerNode ──────────────────────

// Multi-destination-cast (dynamic control mode). This is the publisher/archive-recording
// channel — matches SequencerService.GLOBAL_STREAM_CHANNEL, used here only for archive
// recording lookups (listRecordingsForUri), never to open a local subscription directly.
inline constexpr const char* GLOBAL_STREAM_CHANNEL = "aeron:udp?control-mode=dynamic|control=localhost:9200";

// Subscriber-side channel: same control address, plus an ephemeral local data endpoint
// that the publisher discovers and adds as a destination automatically. Matches
// SequencerService.GLOBAL_STREAM_SUBSCRIBER_CHANNEL. Used for the live (post-replay) fallback
// subscription below.
//
// tether=false is the audit S4 fix: an untethered subscriber that falls behind the
// publisher's window is moved to "resting" rather than back-pressuring the publisher, so a
// slow or stalled global-stream consumer can never wedge the sequencer's single
// ClusteredService thread (which spins in SequencerService.offerToGlobalStream until the
// offer lands). The trade-off is that a rested subscriber loses the messages it fell behind
// on and rejoins live past them — GlobalStreamClient detects that hole from the gap-free
// globalSeqNo run and re-bootstraps the missing range from the archive (see onFragment /
// beginRecovery below), which is the intended "let it fall behind and recover via archive
// replay" posture.
inline constexpr const char* GLOBAL_STREAM_SUBSCRIBER_CHANNEL =
    "aeron:udp?control-mode=dynamic|control=localhost:9200|endpoint=localhost:0|tether=false";

inline constexpr std::int32_t GLOBAL_STREAM_ID = 1;

// Each UDP-replaying binary uses a distinct port so their archive replay publications
// don't conflict.
// FixSessionClient  → 9310 (env PHIXERON_FIX_REPLAY_PORT)
// fix_test_server   → 9400 (env PHIXERON_RISK_TEST_REPLAY_PORT; kept outside the
//                     9300-9325 cluster port block — see SequencerNode's port layout —
//                     since 9312 used to alias member 1's cluster ingress port)
// FixSessionClient's resend-recovery replay → 9313 (env PHIXERON_RESEND_REPLAY_PORT)
// OrderExecClient, deployed co-located with one SequencerNode member (see
// connectLocalArchive/ClusterIngressSender::connectColocated), replays over
// REPLAY_CHANNEL_IPC below instead — no port needed.
inline constexpr std::int32_t REPLAY_STREAM_ID = 110;

// Replay channel for a client co-located with the archive it's replaying from.
inline constexpr const char* REPLAY_CHANNEL_IPC = "aeron:ipc";

/**
 * Resolves a replay channel's port from the environment (so two instances of the same
 * binary can run on one host without a port clash the given default.
 * Returns a full "aeron:udp?endpoint=localhost:<port>" channel string.
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
 * Looks up the global stream recording on an already-connected archive, preferring the
 * active (live) recording over any stopped one; among stopped recordings prefers the
 * largest stop position (holds the most committed data).
 *
 * @param[out] recordingId    recording id of the global stream found on the archive
 * @param[out] catchUpPosition recording position to replay/catch up to
 * @return false if the archive holds no GLOBAL_STREAM_ID recording at all (leaving both
 *         out-parameters untouched).
 */
inline bool findGlobalStreamRecording(const std::shared_ptr<aeron::archive::client::AeronArchive>& archive,
                                      std::int64_t& recordingId, std::int64_t& catchUpPosition)
{
    std::int64_t activeId = -1;
    std::int64_t stoppedId = -1;
    std::int64_t stoppedPosition = std::numeric_limits<std::int64_t>::min();
    archive->listRecordingsForUri(0, std::numeric_limits<std::int32_t>::max(), "", GLOBAL_STREAM_ID,
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
 * and holds a recording of the global stream (matched by GLOBAL_STREAM_ID
 * alone: the recorded originalChannel is Aeron's resolved form of the
 * control-mode=dynamic channel, e.g. an assigned multicast endpoint, which
 * never contains the literal GLOBAL_STREAM_CHANNEL constant as a substring).
 *
 * Trying more than one endpoint is necessary — not just defense in depth —
 * because SequencerService.applyLeadership() creates the global-stream
 * ExclusivePublication and its recording lazily, only on the node that is
 * currently (or was most recently) leader, rather than on every node
 * unconditionally at startup (see todo.md's "Global-stream control port
 * collision" entry for why: every node eagerly pre-creating it collided on
 * the shared control-mode=dynamic port when co-located on one host). So an
 * arbitrary reachable member's archive may simply have no matching recording
 * at all — this must keep trying candidates until it finds the one that does.
 *
 * Note this does not chase a *later* leadership change once connected: if
 * leadership moves on mid-session, this connected archive's recording stops
 * advancing (a new one starts on the new leader) — see todo.md's
 * "Cross-failover global-stream recording continuity" entry.
 *
 * @param[out] recordingId    recording id of the global stream found on the
 *                            connected archive
 * @param[out] catchUpPosition recording position to replay/catch up to
 * @throws std::runtime_error if no candidate endpoint both connects and holds
 *         a global stream recording.
 */
inline std::shared_ptr<aeron::archive::client::AeronArchive> connectToArchiveWithGlobalStream(
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
                .controlRequestChannel("aeron:udp?endpoint=" + endpoint)
                .controlRequestStreamId(controlStreamId)
                .controlResponseChannel(controlResponseChannel);
            archive = aeron::archive::client::AeronArchive::connect(archiveCtx);
        }
        catch (const std::exception& ex)
        {
            std::fprintf(stderr, "%s Archive connect to %s failed: %s\n", logPrefix, endpoint.c_str(), ex.what());
            lastError = ex.what();
            continue;
        }

        if (!findGlobalStreamRecording(archive, recordingId, catchUpPosition))
        {
            std::printf(
                "%s Connected to %s but it has no global stream recording"
                " (not currently/recently leader) — trying next endpoint\n",
                logPrefix, endpoint.c_str());
            lastError = "connected but no global stream recording found on " + endpoint;
            continue;
        }

        std::printf("%s Connected to Aeron Archive at %s (holds the global stream recording)\n", logPrefix,
                    endpoint.c_str());
        return archive;
    }

    throw std::runtime_error(std::string(logPrefix) + " Could not find the global stream recording on any of " +
                             std::to_string(controlEndpoints.size()) +
                             " archive endpoint(s); last error: " + lastError);
}

/**
 * Connects to the archive co-located with this process over "aeron:ipc" — used by clients
 * (e.g. OrderExecClient) deliberately deployed sharing a single SequencerNode member's own
 * Aeron directory (see ClusterIngressSender::connectColocated's doc comment for the ingress
 * half of that deployment). Unlike connectToArchiveWithGlobalStream, there is exactly one
 * candidate archive here, and — thanks to SequencerService's standby-follow replication —
 * every member's archive holds a full copy of the global stream regardless of current
 * leadership, so a missing recording here is a real error, not just "wrong member to ask".
 *
 * @param[out] recordingId    recording id of the global stream found on the local archive
 * @param[out] catchUpPosition recording position to replay/catch up to
 * @throws std::runtime_error if the local archive can't be reached, or holds no global
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
        .controlRequestChannel("aeron:ipc")
        .controlRequestStreamId(controlStreamId)
        .controlResponseChannel("aeron:ipc");
    auto archive = aeron::archive::client::AeronArchive::connect(archiveCtx);

    if (!findGlobalStreamRecording(archive, recordingId, catchUpPosition))
    {
        throw std::runtime_error(std::string(logPrefix) + " Co-located archive has no global stream recording");
    }

    std::printf("%s Connected to co-located Aeron Archive via IPC (holds the global stream recording)\n", logPrefix);
    return archive;
}

/**
 * One recording of the global stream, as found in a single archive's catalog.
 * stopPosition is NULL_POSITION when the recording is still active (only
 * possible for the last segment in a resolveGlobalStreamSegments() result).
 */
struct RecordingSegment {
    std::int64_t recordingId;
    std::int64_t stopPosition;
};

/**
 * Lists every GLOBAL_STREAM_ID recording on an already-connected archive,
 * ordered oldest-to-newest by startTimestamp — each one is a prior leader's
 * tenure (see todo.md's "Cross-failover global-stream recording continuity"
 * entry), so replaying them in this order and concatenating reproduces full
 * history. The current leader's own archive holds every earlier tenure's
 * segment too, because every follower continuously replicates the leader's
 * recording into its own archive the whole time it isn't leader.
 *
 * The abrupt-leader-death race documented in the same todo.md entry can
 * leave two segments both reporting stopPosition == NULL_POSITION (active);
 * since they hold identical content, only the most recent is kept and any
 * earlier "active" duplicate is dropped rather than replayed twice.
 */
inline std::vector<RecordingSegment> resolveGlobalStreamSegments(
    const std::shared_ptr<aeron::archive::client::AeronArchive>& archive)
{
    struct Entry {
        std::int64_t recordingId;
        std::int64_t startTimestamp;
        std::int64_t stopPosition;
    };
    std::vector<Entry> entries;
    archive->listRecordingsForUri(
        0, std::numeric_limits<std::int32_t>::max(), "", GLOBAL_STREAM_ID,
        [&](aeron::archive::client::RecordingDescriptor& recording) {
            entries.push_back({recording.m_recordingId, recording.m_startTimestamp, recording.m_stopPosition});
        });

    std::sort(entries.begin(), entries.end(),
              [](const Entry& a, const Entry& b) { return a.startTimestamp < b.startTimestamp; });

    std::vector<RecordingSegment> segments;
    bool keptActive = false;
    for (const auto& e : entries)
    {
        const bool active = (e.stopPosition == aeron::archive::client::NULL_POSITION);
        if (active && keptActive)
        {
            continue;
        }
        if (active)
        {
            keptActive = true;
        }
        segments.push_back({e.recordingId, e.stopPosition});
    }
    return segments;
}

// ClientConnected/ClientDisconnected aren't FIX messages, so sbe-sequenced.xml
// (like sbe-unsequenced.xml) gives them small, non-ASCII-derived template ids,
// clear of the FIX-MsgType-derived range used by every other message.
inline constexpr std::uint16_t CLIENT_CONNECTED_TEMPLATE_ID = 1;
inline constexpr std::uint16_t CLIENT_DISCONNECTED_TEMPLATE_ID = 2;

// ── Event types delivered to the application ─────────────────────────────────

/**
 * Carries one sbe-sequenced.xml message from the global stream.
 *
 * Every raw fragment on the wire *is* a complete sbe-sequenced.xml message
 * (schemaId=202) — no envelope to strip. Every message in that schema
 * declares `header` (sourceId, connectionId, sessionId, globalSeqNo,
 * timestamp) as its first field, at the same fixed offset regardless of
 * templateId, so this client decodes it generically and exposes the fields
 * here — callers don't need to re-decode it themselves before dispatching
 * on templateId.
 *
 * payload/payloadLength point into the Aeron fragment buffer and are valid
 * only for the duration of the callback; payload addresses the start of the
 * full message (its own 8-byte messageHeader included). Copy the data before
 * returning if it must survive.
 */
struct SequencedEvent {
    std::int64_t globalSeqNo;
    std::int32_t sourceId;          ///< Fixed constant identifying the submitting gateway process (header.sourceId)
    std::int32_t connectionId;      ///< TCP connection id at that gateway; routes the reply (header.connectionId)
    std::int64_t sourceSessionId;   ///< Aeron Cluster client session id (header.sessionId)
    std::int64_t clusterTimestamp;  ///< cluster consensus time (ms) when message was committed
    std::int64_t receiveTimeNs;     ///< wall-clock ns at receipt by this client
    std::uint16_t templateId;       ///< outer messageHeader templateId; picks the specific decode
    std::uint16_t blockLength;      ///< outer messageHeader blockLength; pass straight to wrapForDecode
    std::uint16_t version;          ///< outer messageHeader version; pass straight to wrapForDecode
    const char* payload;            ///< raw sbe-sequenced.xml message bytes (see struct comment)
    std::uint64_t payloadLength;    ///< total byte count
    std::int64_t position;          ///< recording/stream position of this frame's first byte;
                                    ///< pass to ReplayParams::position() to replay from here
};

struct LifecycleEvent {
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
 *   1. Caller uses resolveGlobalStreamSegments(archive) to list every
 *      GLOBAL_STREAM_ID recording on the connected archive, oldest first.
 *   2. Call start(aeron, archive, segments, replayChannel).
 *   3. Call poll() in a duty-cycle loop.
 *
 * Each historical (stopped) segment is replayed in full before moving on to
 * the next; the last segment (which may still be actively recording) is
 * replayed with NULL_LENGTH so it follows live seamlessly once caught up —
 * the same image delivers both historical and live messages for that segment
 * without a subscription switch. If that last segment's image closes (leader
 * failover), the client falls back to a direct MDC subscription. Segments
 * share one underlying replay subscription (same channel/stream id), so
 * moving from one segment's replay to the next only requires starting a new
 * archive replay session, not a new local subscription.
 *
 * Every message is stamped with receiveTimeNs (std::chrono::system_clock).
 */
class GlobalStreamClient {
   public:
    using OnSequenced = std::function<void(const SequencedEvent&)>;
    using OnConnected = std::function<void(const LifecycleEvent&)>;
    using OnDisconnected = std::function<void(const LifecycleEvent&)>;
    using OnCaughtUp = std::function<void()>;

    explicit GlobalStreamClient(OnSequenced onSequenced, OnConnected onConnected = {},
                                OnDisconnected onDisconnected = {}, OnCaughtUp onCaughtUp = {})
        : m_onSequenced(std::move(onSequenced)),
          m_onConnected(std::move(onConnected)),
          m_onDisconnected(std::move(onDisconnected)),
          m_onCaughtUp(std::move(onCaughtUp)),
          m_fragmentHandler([this](auto& buf, auto off, auto len, auto& hdr) { onFragment(buf, off, len, hdr); })
    {}

    /**
     * Adds a live MDC fallback subscription and, when segments is non-empty,
     * starts replaying its segments in order (see class doc comment).
     *
     * @param aeron         connected Aeron instance
     * @param archive       connected AeronArchive the segments were resolved
     *                      from; kept alive to start each segment's replay
     * @param segments      result of resolveGlobalStreamSegments(archive),
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

        // Live MDC fallback — always subscribed; used when the last segment's replay image closes.
        m_liveSubRegId = m_aeron->addSubscription(GLOBAL_STREAM_SUBSCRIBER_CHANNEL, GLOBAL_STREAM_ID);

        // Stored unconditionally (even with no segments yet) so beginRecovery can replay from the
        // archive if the untethered live sub later gaps — including when there was no history at start.
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

        m_replaySubRegId = m_aeron->addSubscription(m_replayChannel, REPLAY_STREAM_ID);
        startSegmentReplay(0);
    }

    /**
     * Attaches to a single already-started archive replay image and adds a
     * live MDC fallback subscription — for a bounded scan of one already-known
     * recording, not the multi-segment bootstrap walk above. Used by
     * FixConnection::replayMissingAppMessages's resend-recovery scan, which
     * replays a specific position range within one recording (not from
     * position 0, and not following live once it ends).
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
            m_replaySubRegId = m_aeron->addSubscription(replayChannel, REPLAY_STREAM_ID);
        }

        // Live MDC fallback — always subscribed; used when replay image closes.
        m_liveSubRegId = m_aeron->addSubscription(GLOBAL_STREAM_SUBSCRIBER_CHANNEL, GLOBAL_STREAM_ID);
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
        // Lazily resolve subscriptions once they become available.
        if (!m_replaySub && m_replaySubRegId >= 0)
        {
            m_replaySub = m_aeron->findSubscription(m_replaySubRegId);
        }
        if (!m_liveSub && m_liveSubRegId >= 0)
        {
            m_liveSub = m_aeron->findSubscription(m_liveSubRegId);
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
                m_pollingLive = false;
                const int work = m_replayImage->poll(m_fragmentHandler, FRAGMENT_LIMIT);
                if (!m_caughtUp && isOnLastSegment() && m_replayImage->position() >= m_catchUpPosition)
                {
                    notifyCaughtUp();
                }
                return work;
            }
            // This segment's replay finished (historical segment fully replayed) or, if it was
            // the last segment, its recording stopped growing (e.g. leader failover).
            m_replayImage.reset();
            ++m_segmentIndex;
            if (m_segmentIndex < m_segments.size())
            {
                startSegmentReplay(m_segmentIndex);
                return 0;
            }
            // All segments replayed and the last one's image closed — fall through to live MDC.
        }

        // Live MDC fallback — poll the subscription directly. Reached only once the
        // open-ended last-segment replay image closes (leader failover); in steady state the
        // client follows live through that replay image and never gets here. A gap can appear
        // on this untethered subscription (it may have rested while replay ran, or reconnected
        // past a failover boundary) — onFragment detects it and triggers beginRecovery.
        if (m_liveSub)
        {
            m_pollingLive = true;
            return m_liveSub->poll(m_fragmentHandler, FRAGMENT_LIMIT);
        }
        return 0;
    }

    bool isCaughtUp() const
    {
        return m_caughtUp;
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
            m_archive->startReplay(segment.recordingId, m_replayChannel, REPLAY_STREAM_ID, replayParams);
    }

    // Re-bootstrap the segment-replay walk from the archive after a live-stream gap (see
    // onFragment). Re-lists the recording segments (the active recording may have grown, or a
    // new leader's tenure appeared since start()) and restarts the replay from the oldest
    // segment; onFragment's globalSeqNo filter drops everything already delivered, so only the
    // missing tail is re-emitted, in order, before live-following resumes. O(history) like the
    // initial bootstrap (cf. audit.md S1), but only reached on a real gap — a consumer that fell
    // far behind, or a leader failover — which is rare. Runs its archive control round-trip on
    // the duty-cycle thread, like start() does; this blocks only the caller's own liveness, never
    // the sequencer's (cf. the S3 resend fix, which likewise keeps ms-scale archive control calls
    // synchronous). m_caughtUp is left set, so onCaughtUp does not re-fire.
    void beginRecovery()
    {
        if (!m_archive || m_replayChannel.empty())
        {
            return;  // no archive / replay channel to recover from (cannot happen in the live-following clients)
        }
        // The replay subscription is normally created in start(); create it here too for the
        // "no history at start, then a live gap" path, which returned before start() added it.
        if (m_replaySubRegId < 0)
        {
            m_replaySubRegId = m_aeron->addSubscription(m_replayChannel, REPLAY_STREAM_ID);
        }
        m_recovering = true;
        m_segments = resolveGlobalStreamSegments(m_archive);
        m_segmentIndex = 0;
        m_replayImage.reset();
        if (m_segments.empty())
        {
            return;
        }
        startSegmentReplay(0);
    }

    using HdrSbe = org::limitless::phixeron::sbe::sequenced::MessageHeader;
    using HeaderComposite = org::limitless::phixeron::sbe::sequenced::Header;

    void onFragment(const aeron::concurrent::AtomicBuffer& buffer, const aeron::util::index_t offset,
                    const aeron::util::index_t length, const aeron::Header& header)
    {
        const std::int64_t receiveNs = nowNs();

        // header.position() is the position the image has advanced to *after*
        // consuming this fragment; subtracting frameLength() gives the position
        // of the frame's first byte, which is what ReplayParams::position() needs
        // to replay starting at (and including) this exact message.
        const std::int64_t framePosition = header.position() - header.frameLength();

        char* const raw = reinterpret_cast<char*>(buffer.buffer());
        const std::uint64_t cap = static_cast<std::uint64_t>(buffer.capacity());
        const std::uint64_t off = static_cast<std::uint64_t>(offset);
        const std::uint64_t len = static_cast<std::uint64_t>(length);
        if (len < HdrSbe::encodedLength() + HeaderComposite::encodedLength())
        {
            std::fprintf(stderr, "[GlobalStreamClient] fragment too short: %" PRIu64 " bytes\n", len);
            return;
        }

        m_hdr.wrap(raw, off, 0U, cap);
        if (m_hdr.schemaId() != HdrSbe::sbeSchemaId())
        {
            std::fprintf(stderr, "[GlobalStreamClient] unexpected schemaId=%u; ignored\n", m_hdr.schemaId());
            return;
        }

        const std::uint16_t templateId = m_hdr.templateId();
        const std::uint64_t bodyOff = off + HdrSbe::encodedLength();

        m_header.wrap(raw, bodyOff, 0U, cap);
        const auto gseq = m_header.globalSeqNo();
        const auto srcId = m_header.sourceId();
        const auto connId = m_header.connectionId();
        const auto sessId = m_header.sessionId();
        const auto ts = m_header.timestamp();

        // Contiguity / de-duplication guard for the live-following modes (skipped for the bounded
        // single-image resend scan, which replays and delivers an exact range verbatim). The
        // sequencer stamps a cluster-wide globalSeqNo that increments by exactly one per published
        // event (message or lifecycle), so the global stream is gap-free by construction: any
        // forward jump means the current source dropped messages — an untethered live subscription
        // that rested after falling behind (see GLOBAL_STREAM_SUBSCRIBER_CHANNEL), or a
        // post-failover live sub that reconnected past the gap. The archive holds every sequenced
        // message, so heal it by re-bootstrapping from the archive; drop the out-of-order fragment
        // — the recovery replay re-delivers it, and everything after it, in order.
        if (!m_singleImageMode)
        {
            if (m_lastGlobalSeqNo != 0)
            {
                if (gseq <= m_lastGlobalSeqNo)
                {
                    return;  // already delivered (e.g. a recovery replay re-covering seen ground)
                }
                if (gseq > m_lastGlobalSeqNo + 1)
                {
                    if (m_pollingLive && !m_recovering)
                    {
                        std::fprintf(stderr,
                                     "[GlobalStreamClient] live-stream gap: expected globalSeqNo=%" PRId64
                                     ", got %" PRId64 " — re-bootstrapping from archive\n",
                                     static_cast<std::int64_t>(m_lastGlobalSeqNo + 1), static_cast<std::int64_t>(gseq));
                        beginRecovery();
                    }
                    return;
                }
            }
            m_recovering = false;
            m_lastGlobalSeqNo = gseq;
        }
        if (templateId == CLIENT_CONNECTED_TEMPLATE_ID)
        {
            if (m_onConnected)
            {
                m_onConnected(LifecycleEvent{.globalSeqNo = gseq,
                                             .sourceSessionId = sessId,
                                             .clusterTimestamp = ts,
                                             .receiveTimeNs = receiveNs});
            }
            return;
        }
        if (templateId == CLIENT_DISCONNECTED_TEMPLATE_ID)
        {
            if (m_onDisconnected)
            {
                m_onDisconnected(LifecycleEvent{.globalSeqNo = gseq,
                                                .sourceSessionId = sessId,
                                                .clusterTimestamp = ts,
                                                .receiveTimeNs = receiveNs});
            }
            return;
        }
        if (m_onSequenced)
        {
            m_onSequenced(SequencedEvent{.globalSeqNo = gseq,
                                         .sourceId = srcId,
                                         .connectionId = connId,
                                         .sourceSessionId = sessId,
                                         .clusterTimestamp = ts,
                                         .receiveTimeNs = receiveNs,
                                         .templateId = templateId,
                                         .blockLength = m_hdr.blockLength(),
                                         .version = m_hdr.version(),
                                         .payload = raw + off,
                                         .payloadLength = len,
                                         .position = framePosition});
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

    static std::int64_t nowNs()
    {
        using namespace std::chrono;
        return duration_cast<nanoseconds>(system_clock::now().time_since_epoch()).count();
    }

    OnSequenced m_onSequenced;
    OnConnected m_onConnected;
    OnDisconnected m_onDisconnected;
    OnCaughtUp m_onCaughtUp;

    std::shared_ptr<aeron::Aeron> m_aeron;
    std::shared_ptr<aeron::archive::client::AeronArchive> m_archive;
    std::int64_t m_replaySubRegId = -1;
    std::int64_t m_liveSubRegId = -1;
    std::shared_ptr<aeron::Subscription> m_replaySub;
    std::shared_ptr<aeron::Subscription> m_liveSub;
    std::shared_ptr<aeron::Image> m_replayImage;

    std::vector<RecordingSegment> m_segments;
    std::size_t m_segmentIndex = 0;
    bool m_singleImageMode = false;
    std::string m_replayChannel;
    std::int64_t m_replaySessionId = -1;
    std::int64_t m_catchUpPosition = 0;
    bool m_caughtUp = false;

    std::int64_t m_lastGlobalSeqNo = 0;  // highest globalSeqNo delivered; 0 = none yet (gap detection)
    bool m_pollingLive = false;          // true while poll() is draining the live MDC sub (vs. a replay)
    bool m_recovering = false;           // a re-bootstrap replay is in flight; suppresses repeat triggers

    aeron::fragment_handler_t m_fragmentHandler;

    HdrSbe m_hdr;
    HeaderComposite m_header;
};

}  // namespace org::limitless::phixeron::sequencer
