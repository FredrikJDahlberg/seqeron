#pragma once

// ReplayerRecovery — the walk/resume/gap decision state machine behind ReplayerStreamReceiver.
//
#include <array>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <functional>
#include <limits>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <vector>

#include "org/limitless/seqeron/protocol/ReplayProtocol.hpp"
#include "org/limitless/seqeron/protocol/SequencedFrame.hpp"
#include "org/limitless/seqeron/protocol/Snapshot.hpp"
#include "org/limitless/seqeron/replayer/client/SnapshotRestoreHandler.hpp"
#include "org/limitless/seqeron/replayer/client/SnapshotStore.hpp"
#include "org/limitless/seqeron/util/Logger.hpp"

// Replay-protocol control codecs (sbe-replay.xml) + LeadershipChanged (sbe-frame.xml)
#include "org_limitless_seqeron_sbe_frame/LeadershipChanged.h"
#include "org_limitless_seqeron_sbe_replay/MessageHeader.h"
#include "org_limitless_seqeron_sbe_replay/ReplayClientIdInUse.h"
#include "org_limitless_seqeron_sbe_replay/ReplayPending.h"
#include "org_limitless_seqeron_sbe_replay/ReplayUnavailable.h"
#include "org_limitless_seqeron_sbe_replay/Replaying.h"
#include "org_limitless_seqeron_sbe_replay/SnapshotLocation.h"

namespace org::limitless::seqeron::replayer::client::detail {

/**
 * Everything ReplayerRecovery cannot do itself: the sends, the replay subscription, and the gauge.
 * ReplayerStreamReceiver implements it against Aeron; the unit suite substitutes a recorder.
 */
class ReplayerRecoveryActions
{
  public:
    virtual void sendReplayRequest(std::int64_t requestId, std::int32_t segmentIndex, std::int64_t fromPosition) = 0;
    virtual void sendSnapshotQuery(std::int64_t requestId, std::int32_t sourceId, std::int64_t round) = 0;
    virtual bool sendReplayComplete() = 0;
    virtual bool sendReplayHeartbeat() = 0;
    virtual void openReplay(std::int64_t replaySessionId) = 0;
    virtual void closeReplay() = 0;
    virtual void recoveryStalled(bool stalled) = 0;
    virtual std::int64_t nowMs() = 0;

  protected:
    ~ReplayerRecoveryActions() = default;
};

// Fixed-size block of the retained FIFO; a record never spans two blocks, and the largest frame fits one.
struct RetainBlock
{
    static constexpr std::size_t SIZE = 16 * 1024;
    std::array<std::uint8_t, SIZE> bytes;
    std::size_t used = 0;
};

// Per-record framing within a block. Carries the receive stamp and position from when the frame
// arrived, so a consumer's delivery-latency stats measure the tap, not the drain.
struct RetainRecordHeader
{
    std::int64_t globalSeqNo;
    std::int64_t position;
    std::int64_t receiveNs;
    std::uint32_t length;
};

class RetainBlockPool
{
  public:
    RetainBlockPool() = default;
    RetainBlockPool(const RetainBlockPool&) = delete;
    RetainBlockPool& operator=(const RetainBlockPool&) = delete;

    ~RetainBlockPool()
    {
        for (auto* block : m_freeList)
        {
            delete block;
        }
    }

    RetainBlock* acquire()
    {
        RetainBlock* block;
        if (m_freeList.empty())
        {
            block = new RetainBlock();
        }
        else
        {
            block = m_freeList.back();
            m_freeList.pop_back();
            block->used = 0;
        }
        return block;
    }

    void release(RetainBlock* const block)
    {
        m_freeList.push_back(block);
    }

  private:
    std::vector<RetainBlock*> m_freeList;
};

// Given a source to restore (restoreFrom), a cold start first restores the newest snapshot of its own that the log
// confirms (doc/snapshot.md §7): it asks the Replayer for that round's sequenced SnapshotEnd, reads the file's
// records into the handler if they match it, dispatching nothing, then resumes at the round's SnapshotStarted with
// the cut as the anchor; every later fall-back to a walk from segment 0 resumes there instead. A file the log does
// not confirm gives way to the next older one, and the last to a walk.
class ReplayerRecovery
{
  public:
    using OnSequenced = std::function<void(const protocol::SequencedEvent&)>;
    using OnConnected = std::function<void(const protocol::LifecycleEvent&)>;
    using OnDisconnected = std::function<void(const protocol::LifecycleEvent&)>;
    using OnLeadershipChanged =
        std::function<void(std::int32_t newLeaderMemberId, std::int64_t leadershipTermId, std::int64_t globalSeqNo)>;
    using OnCaughtUp = std::function<void()>;

  private:
    static constexpr std::int64_t RESEND_INTERVAL_MS = 500;

    // How long an established replay may deliver nothing before it is re-requested.
    static constexpr std::int64_t REPLAY_STALL_TIMEOUT_MS = 5'000;

    // How long recovery may run without dispatching a single frame before it is reported as unconvergent.
    static constexpr std::int64_t RECOVERY_PROGRESS_TIMEOUT_MS = 30'000;
    static constexpr std::int32_t RESUME_SEGMENT_INDEX = -1;

    // Caps on frames retained ahead of a hole (see retainFrame) — enough to cover a walk over a normal
    // recording, not a whole log; past either bound recovery falls back to re-walking.
    static constexpr std::size_t MAX_RETAINED_FRAMES = 65536;
    static constexpr std::size_t MAX_RETAINED_BYTES = 16UL * 1024 * 1024;

  public:
    // Records one duty cycle restores at most, so a large snapshot does not stall the session.
    static constexpr int MAX_RESTORE_RECORDS_PER_CYCLE = 1024;

  private:
    const std::int32_t m_clientId;
    ReplayerRecoveryActions& m_actions;
    OnSequenced m_onSequenced;
    OnConnected m_onConnected;
    OnDisconnected m_onDisconnected;
    OnLeadershipChanged m_onLeadershipChanged;
    OnCaughtUp m_onCaughtUp;

    bool m_awaitingReplay = false;
    std::int64_t m_replaySessionId = -1;
    std::int64_t m_catchUpPosition = 0;  // bounded replay's end position; segment done once the image reaches it
    std::int32_t m_walkSegmentIndex = 0; // cold-start walk position; -1 once caught up (steady/resume mode)
    std::int64_t m_walkRecordingId = -1; // recordingId last served for m_walkSegmentIndex, or -1 if not yet known
    std::int64_t m_reqFromPosition = 0;  // fromPosition of the current request, for idempotent resend
    std::int64_t m_lastRequestMs = 0;
    std::int64_t m_requestId = 0; // advances per send; replies not carrying it are stale (see onControl)
    std::int64_t m_lastHeartbeatMs = 0;
    bool m_completePending = false;
    std::int64_t m_lastReplayPosition = -1;  // last replay-image position seen; -1 = not attached yet
    std::int64_t m_lastReplayProgressMs = 0; // when it last changed — the stall watchdog's clock
    bool m_replayerUnavailable = false;
    bool m_clientIdInUse = false;

    std::int64_t m_lastGlobalSeqNo = 0;            // highest globalSeqNo delivered; 0 = none yet
    std::int64_t m_lastFramePosition = 0;          // where that frame starts in the recording; requestResume's anchor
    std::int64_t m_resumeAnchorSequenceNumber = 0; // globalSeqNo a resume replay must open at, or 0 if not resuming
    bool m_replayGapLogged = false;                // report a hole in replayed history once per episode, not per frame
    bool m_caughtUp = false;                       // following live; revoked on a tap gap, re-established at the seam

    std::int64_t m_noProgressSinceMs = 0; // when the current no-progress episode started; 0 = none timed
    bool m_recoveryStallReported = false;

    // Live tap frames from beyond the current hole, retained in arrival order (see retainFrame).
    RetainBlockPool m_retainPool;
    std::deque<RetainBlock*> m_retained;
    std::size_t m_retainReadOffset = 0;          // offset of the next unconsumed record within m_retained.front()
    std::int64_t m_retainTailSequenceNumber = 0; // globalSeqNo of the most recently retained frame; dedups redelivery
    std::size_t m_retainFrameCount = 0;
    std::size_t m_retainBytes = 0;
    bool m_retainOverflowed = false;
    bool m_retainOverflowLogged = false; // that overflow reported once per episode, not per frame

    std::int32_t m_currentLeaderMemberId = -1;

    std::int32_t m_restoreSourceId = -1; // the source restored from its snapshot, or -1 for none
    SnapshotRestoreHandler* m_restoreHandler = nullptr;
    SnapshotStore* m_store = nullptr;
    bool m_querying = false;          // a SnapshotQuery is out and unanswered
    std::int64_t m_queriedRound = -1; // the round of the local file the query is about
    bool m_restoring = false;         // reading the chosen file's records; nothing is dispatched until its last
    std::optional<SnapshotStore::Reader> m_reader;
    std::int32_t m_restoreRecordIndex = 0;
    // The chosen snapshot: its round, its cut, and where its SnapshotStarted starts. 0 cut = none.
    std::int64_t m_snapshotRound = 0;
    std::int64_t m_snapshotGlobalSeqNo = 0;
    std::int64_t m_snapshotPosition = 0;
    std::optional<protocol::SnapshotHeader> m_restoredHeader; // of the restore in progress, empty before record 0
    std::optional<std::string> m_restoreFailure;              // why the restore cannot proceed; latched

  public:
    ReplayerRecovery(const std::int32_t clientId, ReplayerRecoveryActions& actions, OnSequenced onSequenced,
                     OnConnected onConnected = {}, OnDisconnected onDisconnected = {},
                     OnLeadershipChanged onLeadershipChanged = {}, OnCaughtUp onCaughtUp = {}) :
      m_clientId(clientId),
      m_actions(actions),
      m_onSequenced(std::move(onSequenced)),
      m_onConnected(std::move(onConnected)),
      m_onDisconnected(std::move(onDisconnected)),
      m_onLeadershipChanged(std::move(onLeadershipChanged)),
      m_onCaughtUp(std::move(onCaughtUp))
    {
        if (!m_onSequenced)
        {
            throw std::invalid_argument("[ReplayerRecovery] onSequenced is required");
        }
    }

    // Owns its retained blocks, and is referenced by the actions it calls back into.
    ReplayerRecovery(const ReplayerRecovery&) = delete;
    ReplayerRecovery& operator=(const ReplayerRecovery&) = delete;

    ~ReplayerRecovery()
    {
        for (auto* block : m_retained)
        {
            delete block;
        }
    }

    /**
     * Restores a source's newest snapshot in a store that the log confirms on start(), before anything is
     * dispatched. Call before start().
     *
     * @param sourceId the source whose snapshot to restore
     * @param store    this instance's own snapshots; must outlive this object
     * @param handler  takes the snapshot's records; must outlive this object
     */
    void restoreFrom(const std::int32_t sourceId, SnapshotStore& store, SnapshotRestoreHandler& handler)
    {
        m_restoreSourceId = sourceId;
        m_store = &store;
        m_restoreHandler = &handler;
    }

    // Cold start: restore the newest local snapshot if restoring one, else walk the recording chain from segment 0.
    void start()
    {
        if (m_restoreSourceId >= 0)
        {
            queryBelow(std::numeric_limits<std::int64_t>::max());
        }
        else
        {
            requestReplay(0, 0);
        }
    }

    // Starts over as a cold start on a client that has dispatched frames already: a passive gateway instance's
    // activation (doc/snapshot.md §4). It restores the source's latest snapshot again, or walks from segment 0
    // without one, and dispatches every frame after that once more, and nothing before. Call only once caught up:
    // no query, restore or retained frame is in flight then, so the state reset here is all there is.
    void restart()
    {
        m_actions.closeReplay();
        m_replaySessionId = -1;
        m_awaitingReplay = false;
        m_resumeAnchorSequenceNumber = 0;
        m_caughtUp = false;
        m_lastGlobalSeqNo = 0;
        m_lastFramePosition = 0;
        m_snapshotGlobalSeqNo = 0;
        start();
    }

    void onFrame(char* const frame, const std::uint64_t length, const std::int64_t framePosition,
                 const std::int64_t receiveNs, const bool fromReplay)
    {
        // The envelope is stripped once, here: both frame families carry globalSeqNo, at different offsets.
        const protocol::FrameView entry = protocol::unwrapFrame(frame, length);
        if (m_restoreFailure || !entry.valid)
        {
            return;
        }
        const auto sequenceNumber = entry.globalSeqNo;
        if (fromReplay && m_resumeAnchorSequenceNumber != 0)
        {
            const std::int64_t anchor = m_resumeAnchorSequenceNumber;
            m_resumeAnchorSequenceNumber = 0;
            if (sequenceNumber != anchor)
            {
                util::Logger::warn(util::component::ReplayerStreamReceiver, util::eventCode::TapGap,
                                   "resume replay opened at globalSeqNo=%lld, expected %lld — the active "
                                   "recording rotated under us; replaying history from its start (segment 0, or "
                                   "the restored snapshot)",
                                   static_cast<long long>(sequenceNumber), static_cast<long long>(anchor));
                rewalk();
                return;
            }
        }
        if (m_lastGlobalSeqNo != 0)
        {
            if (sequenceNumber <= m_lastGlobalSeqNo)
            {
                return;
            }
            if (sequenceNumber > m_lastGlobalSeqNo + 1)
            {
                if (!fromReplay && !isRecovering())
                {
                    util::Logger::warn(util::component::ReplayerStreamReceiver, util::eventCode::TapGap,
                                       "tap gap: expected globalSeqNo=%lld got %lld — "
                                       "resuming the recording at globalSeqNo=%lld",
                                       static_cast<long long>(m_lastGlobalSeqNo + 1),
                                       static_cast<long long>(sequenceNumber),
                                       static_cast<long long>(m_lastGlobalSeqNo));
                    m_caughtUp = false;
                    requestResume();
                }
                if (!fromReplay)
                {
                    retainFrame(sequenceNumber, frame, length, framePosition, receiveNs);
                }
                else if (!m_replayGapLogged)
                {
                    m_replayGapLogged = true;
                    util::Logger::warn(util::component::ReplayerStreamReceiver, util::eventCode::TapGap,
                                       "gap in REPLAYED history: expected globalSeqNo=%lld got %lld — this "
                                       "node's recording chain does not cover the hole; recovery cannot "
                                       "converge until it does",
                                       static_cast<long long>(m_lastGlobalSeqNo + 1),
                                       static_cast<long long>(sequenceNumber));
                }
                return;
            }
        }
        else if (sequenceNumber != 1)
        {
            if (!fromReplay && isRecovering())
            {
                retainFrame(sequenceNumber, frame, length, framePosition, receiveNs);
                return;
            }
            util::Logger::fault(util::component::ReplayerStreamReceiver, util::eventCode::FirstFrameNotOne,
                                "FATAL: first frame observed has globalSeqNo=%lld, expected 1 — "
                                "this node's recording does not reach the start of the log; aborting",
                                static_cast<long long>(sequenceNumber));
            std::abort();
        }
        dispatchFrame(frame, length, sequenceNumber, framePosition, receiveNs, fromReplay);
        drainRetained();
    }

    // One message off the Replayer's control stream (Replaying / ReplayPending / ReplayUnavailable /
    // ReplayClientIdInUse / SnapshotLocation).
    void onControl(char* const message, const std::uint64_t length)
    {
        if (length < sbe::replay::MessageHeader::encodedLength())
        {
            return;
        }

        sbe::replay::MessageHeader mh;
        mh.wrap(message, 0U, 0U, length);
        const std::uint64_t bodyOff = sbe::replay::MessageHeader::encodedLength();

        if (mh.templateId() == sbe::replay::Replaying::sbeTemplateId())
        {
            sbe::replay::Replaying replaying;
            replaying.wrapForDecode(message, bodyOff, mh.blockLength(), mh.version(), length);
            if (replaying.clientId() != m_clientId || replaying.requestId() != m_requestId)
            {
                return;
            }
            onReplaying(replaying.replaySessionId(), replaying.catchUpPosition(), replaying.recordingId());
        }
        else if (mh.templateId() == sbe::replay::SnapshotLocation::sbeTemplateId())
        {
            sbe::replay::SnapshotLocation location;
            location.wrapForDecode(message, bodyOff, mh.blockLength(), mh.version(), length);
            if (m_querying && location.clientId() == m_clientId && location.requestId() == m_requestId)
            {
                onSnapshotLocation(location);
            }
        }
        else if (mh.templateId() == sbe::replay::ReplayPending::sbeTemplateId())
        {
            sbe::replay::ReplayPending replayPending;
            replayPending.wrapForDecode(message, bodyOff, mh.blockLength(), mh.version(), length);
            if (replayPending.clientId() == m_clientId && replayPending.requestId() == m_requestId)
            {
                m_lastRequestMs = m_actions.nowMs();
                m_replayerUnavailable = false; // queued, not refused — the episode ended (see Replaying)
            }
        }
        else if (mh.templateId() == sbe::replay::ReplayUnavailable::sbeTemplateId())
        {
            sbe::replay::ReplayUnavailable replayUnavailable;
            replayUnavailable.wrapForDecode(message, bodyOff, mh.blockLength(), mh.version(), length);
            if (replayUnavailable.clientId() == m_clientId && replayUnavailable.requestId() == m_requestId)
            {
                onReplayUnavailable();
            }
        }
        else if (mh.templateId() == sbe::replay::ReplayClientIdInUse::sbeTemplateId())
        {
            sbe::replay::ReplayClientIdInUse replayClientIdInUse;
            replayClientIdInUse.wrapForDecode(message, bodyOff, mh.blockLength(), mh.version(), length);
            if (replayClientIdInUse.clientId() == m_clientId)
            {
                onClientIdInUse();
            }
        }
    }

    // Whether this node's Replayer has seen another process requesting under this client's id. Latched: the
    // receiver raises it from its duty cycle, since a throw from inside a fragment handler is swallowed.
    bool isClientIdInUse() const
    {
        return m_clientIdInUse;
    }

    void onReplayPosition(const std::int64_t position)
    {
        if (position >= m_catchUpPosition)
        {
            onReplaySegmentComplete();
        }
        else if (position != m_lastReplayPosition)
        {
            m_lastReplayPosition = position;
            m_lastReplayProgressMs = m_actions.nowMs();
        }
    }

    void onReplayImageClosed(const std::int64_t finalPosition)
    {
        if (finalPosition >= m_catchUpPosition)
        {
            onReplaySegmentComplete();
        }
        else
        {
            util::Logger::warn(util::component::ReplayerStreamReceiver, util::eventCode::TapGap,
                               "replay image closed at position %lld, short of catchUpPosition %lld — the replay was "
                               "stopped under us; re-requesting the same segment (index %d)",
                               static_cast<long long>(finalPosition), static_cast<long long>(m_catchUpPosition),
                               static_cast<int>(m_walkSegmentIndex));
            reRequestCurrent();
        }
    }

    void doTimers(const bool requestPublicationPending)
    {
        if (m_restoring)
        {
            restoreRecords();
        }
        const std::int64_t nowMs = m_actions.nowMs();
        if (m_querying && (requestPublicationPending || (nowMs - m_lastRequestMs) > RESEND_INTERVAL_MS))
        {
            sendSnapshotQuery();
        }
        if (m_awaitingReplay && (requestPublicationPending || (nowMs - m_lastRequestMs) > RESEND_INTERVAL_MS))
        {
            requestReplay(m_walkSegmentIndex, m_reqFromPosition); // re-send the same request verbatim
        }
        if (m_completePending)
        {
            sendReplayComplete();
        }
        // >= 0: 0 is an ordinary replaySessionId, and -1 alone means none.
        if (m_replaySessionId >= 0)
        {
            if ((nowMs - m_lastHeartbeatMs) > RESEND_INTERVAL_MS)
            {
                sendHeartbeat();
            }
            if ((nowMs - m_lastReplayProgressMs) > REPLAY_STALL_TIMEOUT_MS)
            {
                onReplayStalled();
            }
        }
    }

    bool checkRecoveryProgress()
    {
        if (m_caughtUp)
        {
            return false;
        }
        const std::int64_t nowMs = m_actions.nowMs();
        if (m_noProgressSinceMs == 0)
        {
            m_noProgressSinceMs = nowMs; // the first observation only anchors the clock
            return false;
        }
        if (m_recoveryStallReported || nowMs - m_noProgressSinceMs < RECOVERY_PROGRESS_TIMEOUT_MS)
        {
            return false;
        }
        m_recoveryStallReported = true;
        util::Logger::fault(util::component::ReplayerStreamReceiver, util::eventCode::RecoveryStalled,
                            "recovery has dispatched nothing for >%lldms: lastGlobalSeqNo=%lld segment=%d "
                            "awaitingReplay=%d replaySession=%lld replayerUnavailable=%d querying=%d restoring=%d "
                            "— holding; check this node's Replayer and its recording chain",
                            static_cast<long long>(RECOVERY_PROGRESS_TIMEOUT_MS),
                            static_cast<long long>(m_lastGlobalSeqNo), static_cast<int>(m_walkSegmentIndex),
                            m_awaitingReplay ? 1 : 0, static_cast<long long>(m_replaySessionId),
                            m_replayerUnavailable ? 1 : 0, m_querying ? 1 : 0, m_restoring ? 1 : 0);
        m_actions.recoveryStalled(true);
        return true;
    }

    bool isCaughtUp() const
    {
        return m_caughtUp;
    }

    std::int64_t lastGlobalSeqNo() const
    {
        return m_lastGlobalSeqNo;
    }

    std::int32_t currentLeaderMemberId() const
    {
        return m_currentLeaderMemberId;
    }

    bool isAwaitingReplay() const
    {
        return m_awaitingReplay;
    }

    bool isRecovering() const
    {
        return m_replaySessionId >= 0 || m_awaitingReplay || m_querying || m_restoring;
    }

    // Reading a snapshot's records, before anything after its cut is dispatched.
    bool isRestoring() const
    {
        return m_restoring;
    }

    // Why the snapshot cannot be restored, or empty: this instance can no longer recover. Latched.
    const std::optional<std::string>& restoreFailure() const
    {
        return m_restoreFailure;
    }

    std::int64_t replaySessionId() const
    {
        return m_replaySessionId;
    }

    std::int64_t catchUpPosition() const
    {
        return m_catchUpPosition;
    }

    std::int32_t walkSegmentIndex() const
    {
        return m_walkSegmentIndex;
    }

    std::int64_t walkRecordingId() const
    {
        return m_walkRecordingId;
    }

    std::int64_t requestId() const
    {
        return m_requestId;
    }

    std::int64_t requestFromPosition() const
    {
        return m_reqFromPosition;
    }

    bool completePending() const
    {
        return m_completePending;
    }

  private:
    void requestReplay(const std::int32_t segmentIndex, const std::int64_t fromPosition)
    {
        if (segmentIndex >= 0)
        {
            m_resumeAnchorSequenceNumber = 0; // a walk supersedes any resume in flight
            if (segmentIndex != m_walkSegmentIndex)
            {
                m_walkRecordingId = -1;
            }
        }
        m_walkSegmentIndex = segmentIndex;
        m_reqFromPosition = fromPosition;
        m_awaitingReplay = true;
        m_replaySessionId = -1;
        m_completePending = false;
        m_actions.closeReplay();
        m_lastRequestMs = m_actions.nowMs();
        ++m_requestId;
        m_actions.sendReplayRequest(m_requestId, segmentIndex, fromPosition);
    }

    void requestResume()
    {
        requestReplay(RESUME_SEGMENT_INDEX, m_lastFramePosition);
        m_resumeAnchorSequenceNumber = m_lastGlobalSeqNo;
    }

    // Replays history from its start again: segment 0 of the chain, or, once a snapshot is restored, the active
    // recording at its SnapshotStarted, with the cut as the anchor.
    void rewalk()
    {
        if (m_snapshotGlobalSeqNo == 0)
        {
            requestReplay(0, 0);
            return;
        }
        requestReplay(RESUME_SEGMENT_INDEX, m_snapshotPosition);
        m_resumeAnchorSequenceNumber = m_snapshotGlobalSeqNo;
    }

    // Asks about the newest local snapshot below a round, or walks from segment 0 when there is none.
    void queryBelow(const std::int64_t belowRound)
    {
        m_queriedRound = m_store->latestRound(belowRound);
        if (m_queriedRound < 0)
        {
            requestReplay(0, 0);
            return;
        }
        sendSnapshotQuery();
    }

    // Asks this node's Replayer for the sequenced end of the queried round; resent until answered.
    void sendSnapshotQuery()
    {
        m_querying = true;
        m_lastRequestMs = m_actions.nowMs();
        ++m_requestId;
        m_actions.sendSnapshotQuery(m_requestId, m_restoreSourceId, m_queriedRound);
    }

    // Restores the queried round's file if the log confirms it; otherwise moves on to the next older one. A format
    // this build does not read stops recovery.
    void onSnapshotLocation(sbe::replay::SnapshotLocation& location)
    {
        m_querying = false;
        m_replayerUnavailable = false;
        if (location.round() != m_queriedRound)
        {
            util::Logger::info(util::component::ReplayerStreamReceiver,
                               "round %lld of source %d has no sequenced SnapshotEnd in this node's index; trying an "
                               "older snapshot",
                               static_cast<long long>(m_queriedRound), static_cast<int>(m_restoreSourceId));
            queryBelow(m_queriedRound);
            return;
        }
        if (!m_restoreHandler->supportsFormatVersion(location.formatVersion()))
        {
            failRestore("round " + std::to_string(m_queriedRound) + " has formatVersion " +
                        std::to_string(location.formatVersion()) + ", which this build does not read");
            return;
        }
        m_reader = m_store->open(m_queriedRound);
        if (!m_reader || m_reader->recordCount() != location.recordCount() ||
            m_reader->length() != static_cast<std::uint64_t>(location.length()) ||
            m_reader->crc32c() != location.crc32c())
        {
            util::Logger::warn(util::component::ReplayerStreamReceiver, util::eventCode::SnapshotStoreFailed,
                               "round %lld's snapshot file is %s; trying an older snapshot",
                               static_cast<long long>(m_queriedRound),
                               m_reader ? "not the one sequenced" : "missing or cut short");
            m_reader.reset();
            queryBelow(m_queriedRound);
            return;
        }
        m_snapshotRound = m_queriedRound;
        m_snapshotGlobalSeqNo = location.asOfGlobalSeqNo();
        m_snapshotPosition = location.asOfPosition();
        m_restoring = true;
        m_restoreRecordIndex = 0;
        m_restoredHeader.reset();
        util::Logger::info(util::component::ReplayerStreamReceiver,
                           "restoring source %d from round %lld, cut at globalSeqNo=%lld position=%lld",
                           static_cast<int>(m_restoreSourceId), static_cast<long long>(m_snapshotRound),
                           static_cast<long long>(m_snapshotGlobalSeqNo), static_cast<long long>(m_snapshotPosition));
    }

    // The next records of the restore, at most MAX_RESTORE_RECORDS_PER_CYCLE. They match the end the log holds by
    // the file's trailer, so records that fail it are a damaged file.
    void restoreRecords()
    {
        m_noProgressSinceMs = 0;
        std::span<const std::uint8_t> record;
        for (int i = 0; i < MAX_RESTORE_RECORDS_PER_CYCLE; ++i)
        {
            const std::int32_t length = m_reader->next(record);
            if (length == SnapshotStore::Reader::END)
            {
                completeRestore();
                return;
            }
            if (length == SnapshotStore::Reader::DAMAGED)
            {
                failRestore("round " + std::to_string(m_snapshotRound) +
                            "'s snapshot file is damaged: its records do not match the SnapshotEnd sequenced for it");
                return;
            }
            if (m_restoreRecordIndex == 0)
            {
                m_restoredHeader = protocol::SnapshotHeader::decode(record.data(), record.size());
                if (!m_restoredHeader)
                {
                    failRestore("round " + std::to_string(m_snapshotRound) + " has a header of version " +
                                std::to_string(protocol::SnapshotHeader::version(record.data(), record.size())) +
                                ", which this build does not read");
                    return;
                }
                m_restoreHandler->onSnapshotHeader(*m_restoredHeader);
            }
            else
            {
                m_restoreHandler->onSnapshotRecord(record, m_restoreRecordIndex - 1);
            }
            ++m_restoreRecordIndex;
        }
    }

    // The snapshot is restored: the state is that after the cut, and so is the leadership its header carries.
    // Resumes at the cut, which is dropped as already dispatched.
    void completeRestore()
    {
        m_restoring = false;
        m_reader.reset();
        m_lastGlobalSeqNo = m_snapshotGlobalSeqNo;
        m_lastFramePosition = m_snapshotPosition;
        m_currentLeaderMemberId = m_restoredHeader->leaderMemberId;
        util::Logger::info(util::component::ReplayerStreamReceiver,
                           "restored source %d from round %lld; resuming after globalSeqNo=%lld",
                           static_cast<int>(m_restoreSourceId), static_cast<long long>(m_snapshotRound),
                           static_cast<long long>(m_snapshotGlobalSeqNo));
        if (m_onLeadershipChanged && m_restoredHeader->leadershipTermId >= 0)
        {
            m_onLeadershipChanged(m_currentLeaderMemberId, m_restoredHeader->leadershipTermId, m_snapshotGlobalSeqNo);
        }
        requestResume();
    }

    // Stops recovering for good: what is left cannot be restored, and nothing else may be dispatched in its place.
    void failRestore(std::string reason)
    {
        m_querying = false;
        m_restoring = false;
        m_reader.reset();
        m_awaitingReplay = false;
        m_replaySessionId = -1;
        m_actions.closeReplay();
        util::Logger::fault(util::component::ReplayerStreamReceiver, util::eventCode::SnapshotRestoreFailed,
                            "cannot restore source %d: %s", static_cast<int>(m_restoreSourceId), reason.c_str());
        m_restoreFailure = std::move(reason);
    }

    void reRequestCurrent()
    {
        if (m_walkSegmentIndex < 0)
        {
            requestResume();
        }
        else
        {
            requestReplay(m_walkSegmentIndex, m_reqFromPosition); // same request verbatim, new requestId
        }
    }

    void onReplaying(const std::int64_t session, const std::int64_t replayCatchUpPosition,
                     const std::int64_t recordingId)
    {
        m_awaitingReplay = false;
        m_replayerUnavailable = false;
        if (session == protocol::REPLAYER_NO_REPLAY_NEEDED)
        {
            if (m_walkSegmentIndex < 0)
            {
                util::Logger::warn(util::component::ReplayerStreamReceiver, util::eventCode::TapGap,
                                   "resume at position %lld answered 'nothing to replay' while a hole is open "
                                   "above globalSeqNo=%lld — the active recording rotated under us; replaying "
                                   "history from its start (segment 0, or the restored snapshot)",
                                   static_cast<long long>(m_reqFromPosition),
                                   static_cast<long long>(m_lastGlobalSeqNo));
                rewalk();
                return;
            }
            if (recordingId >= 0)
            {
                requestReplay(m_walkSegmentIndex + 1, 0);
                return;
            }
            m_replaySessionId = -1;  // already at the tip — follow the live tap
            m_walkSegmentIndex = -1; // chain exhausted (or never a walk) → steady/resume mode
            if (reachedTip())
            {
                notifyCaughtUp();
            }
            return;
        }

        if (m_walkSegmentIndex >= 0)
        {
            if (m_walkRecordingId >= 0 && recordingId != m_walkRecordingId)
            {
                util::Logger::warn(util::component::ReplayerStreamReceiver, util::eventCode::TapGap,
                                   "walk segment %d now resolves to recording %lld, previously %lld — the "
                                   "recording chain shifted under us; re-walking from segment 0",
                                   static_cast<int>(m_walkSegmentIndex), static_cast<long long>(recordingId),
                                   static_cast<long long>(m_walkRecordingId));
                requestReplay(0, 0);
                return;
            }
            m_walkRecordingId = recordingId;
        }
        m_replaySessionId = session;
        m_catchUpPosition = replayCatchUpPosition;
        m_actions.openReplay(session);
        m_lastReplayPosition = -1;
        m_lastReplayProgressMs = m_actions.nowMs();
    }

    void sendReplayComplete()
    {
        m_completePending = !m_actions.sendReplayComplete();
    }

    void sendHeartbeat()
    {
        if (m_actions.sendReplayHeartbeat())
        {
            m_lastHeartbeatMs = m_actions.nowMs();
        }
    }

    void onClientIdInUse()
    {
        if (!m_clientIdInUse)
        {
            m_clientIdInUse = true;
            util::Logger::fault(util::component::ReplayerStreamReceiver, util::eventCode::ReplayClientIdCollision,
                                "this node's Replayer reports another process requesting under clientId=%d — "
                                "neither can catch up; give each replica on the node its own id",
                                m_clientId);
        }
    }

    void onReplayUnavailable()
    {
        if (!m_replayerUnavailable)
        {
            m_replayerUnavailable = true;
            util::Logger::fault(util::component::ReplayerStreamReceiver, util::eventCode::ReplayUnavailable,
                                "this node's Replayer has no valid history to serve (its archive failed the "
                                "globalSeqNo-1 integrity check) — holding, not dispatching; repair the node's "
                                "archive and restart its Replayer");
        }
        m_lastRequestMs = m_actions.nowMs();
    }

    // An attached (or expected) replay stopped delivering — see the watchdog in doTimers.
    void onReplayStalled()
    {
        util::Logger::warn(util::component::ReplayerStreamReceiver, util::eventCode::TapGap,
                           "replay session %lld made no progress for %lldms at position %lld of "
                           "catchUpPosition %lld — re-requesting segment %d",
                           static_cast<long long>(m_replaySessionId), static_cast<long long>(REPLAY_STALL_TIMEOUT_MS),
                           static_cast<long long>(m_lastReplayPosition), static_cast<long long>(m_catchUpPosition),
                           static_cast<int>(m_walkSegmentIndex));
        reRequestCurrent();
    }

    // A replay segment finished (reached its bounded tip, or its image closed for a stopped segment).
    void onReplaySegmentComplete()
    {
        m_actions.closeReplay();
        m_replaySessionId = -1;
        if (m_walkSegmentIndex < 0)
        {
            if (reachedTip())
            {
                sendReplayComplete();
                notifyCaughtUp();
            }
        }
        else
        {
            requestReplay(m_walkSegmentIndex + 1, 0); // advance the walk to the next segment
        }
    }

    void dispatchFrame(char* const frame, const std::uint64_t length, const std::int64_t sequenceNumber,
                       const std::int64_t framePosition, const std::int64_t receiveNs, const bool fromReplay)
    {
        m_noProgressSinceMs = 0;
        if (m_recoveryStallReported)
        {
            m_recoveryStallReported = false;
            m_actions.recoveryStalled(false);
        }
        // onFrame unwrapped and validated this frame already, and the retained FIFO holds only frames that
        // passed there — the unwrap here re-addresses the caller's bytes, it does not re-check them.
        const protocol::FrameView view = protocol::unwrapFrame(frame, length);

        m_lastGlobalSeqNo = sequenceNumber;
        m_replayGapLogged = false;
        m_lastFramePosition = framePosition;
        if (!fromReplay && !m_caughtUp && !m_retainOverflowed)
        {
            notifyCaughtUp();
        }

        // A systemEventType is only a systemEventType on a system frame: an application payload's own 1
        // and 2 sit at the same offset, so both halves have to match before a frame is read as a
        // lifecycle event.
        const bool isSystem = view.system;
        const std::uint16_t eventType = view.systemEventType;
        if (isSystem && (eventType == protocol::CONNECTION_OPENED || eventType == protocol::CONNECTION_CLOSED))
        {
            // Both carry the same LifecycleEvent; only the callback differs.
            const OnConnected& callback = eventType == protocol::CONNECTION_OPENED ? m_onConnected : m_onDisconnected;
            if (callback)
            {
                callback(protocol::lifecycleEventOf(view, receiveNs));
                return;
            }
            // No lifecycle callback: the frame goes to onSequenced like any other rather than being
            // dropped. A consumer that confirms its own ingress, which must see every frame it placed,
            // takes them there.
        }
        if (isSystem && eventType == protocol::LEADERSHIP_CHANGED)
        {
            // Synthesized, so its fields are inline in the frame's own block, which is what
            // view.payload addresses for these three.
            auto leadershipChanged =
                protocol::decodeSystem<sbe::frame::LeadershipChanged>(view.payload, view.payloadLength);
            m_currentLeaderMemberId = leadershipChanged.newLeaderMemberId();
            if (m_onLeadershipChanged)
            {
                m_onLeadershipChanged(m_currentLeaderMemberId, leadershipChanged.leadershipTermId(), sequenceNumber);
                return;
            }
        }
        m_onSequenced(protocol::sequencedEventOf(view, receiveNs, framePosition));
    }

    void retainFrame(const std::int64_t sequenceNumber, const char* const frame, const std::uint64_t len,
                     const std::int64_t framePosition, const std::int64_t receiveNs)
    {
        const std::size_t recordSize = sizeof(RetainRecordHeader) + len;
        if (m_retainFrameCount >= MAX_RETAINED_FRAMES || (m_retainBytes + len) > MAX_RETAINED_BYTES ||
            recordSize > RetainBlock::SIZE)
        {
            m_retainOverflowed = true;
            if (!m_retainOverflowLogged)
            {
                m_retainOverflowLogged = true;
                util::Logger::warn(util::component::ReplayerStreamReceiver, util::eventCode::TapGap,
                                   "retained-frame buffer full at globalSeqNo=%lld (%zu frames, %zu bytes) — "
                                   "dropping ahead-of-hole frames; recovery falls back to re-walking",
                                   static_cast<long long>(sequenceNumber), m_retainFrameCount, m_retainBytes);
            }
        }
        else if (m_retainFrameCount <= 0 || sequenceNumber > m_retainTailSequenceNumber)
        {
            if (m_retained.empty() || m_retained.back()->used + recordSize > RetainBlock::SIZE)
            {
                m_retained.push_back(m_retainPool.acquire());
            }
            RetainBlock* const tail = m_retained.back();
            const RetainRecordHeader header{ sequenceNumber, framePosition, receiveNs,
                                             static_cast<std::uint32_t>(len) };
            std::memcpy(tail->bytes.data() + tail->used, &header, sizeof(header));
            std::memcpy(tail->bytes.data() + tail->used + sizeof(header), frame, len);
            tail->used += recordSize;
            ++m_retainFrameCount;
            m_retainBytes += len;
            m_retainTailSequenceNumber = sequenceNumber;
        }
    }

    // Reads the oldest retained record's header, recycling any fully-read blocks first. False when the FIFO is empty
    bool peekRetained(RetainRecordHeader& header)
    {
        while (!m_retained.empty() && m_retainReadOffset >= m_retained.front()->used)
        {
            m_retainPool.release(m_retained.front());
            m_retained.pop_front();
            m_retainReadOffset = 0;
        }

        const bool empty = m_retained.empty();
        if (!empty)
        {
            std::memcpy(&header, m_retained.front()->bytes.data() + m_retainReadOffset, sizeof(header));
        }
        return !empty;
    }

    // Exits when the FIFO runs dry, or on a hole below the oldest retained frame.
    void drainRetained()
    {
        RetainRecordHeader header;
        while (peekRetained(header) && header.globalSeqNo <= m_lastGlobalSeqNo + 1)
        {
            std::uint8_t* const payload = m_retained.front()->bytes.data() + m_retainReadOffset + sizeof(header);
            m_retainReadOffset += sizeof(header) + header.length;
            --m_retainFrameCount;
            m_retainBytes -= header.length;
            if (header.globalSeqNo > m_lastGlobalSeqNo) // otherwise the replay already covered it
            {
                dispatchFrame(reinterpret_cast<char*>(payload), header.length, header.globalSeqNo, header.position,
                              header.receiveNs, /*fromReplay=*/false);
            }
        }
    }

    bool reachedTip()
    {
        drainRetained();
        const bool success = m_retained.empty() && !m_retainOverflowed;
        if (!success)
        {
            endOverflowEpisode();
            rewalk();
        }
        return success;
    }

    void endOverflowEpisode()
    {
        m_retainOverflowed = false;
        m_retainOverflowLogged = false;
    }

    void notifyCaughtUp()
    {
        if (!m_caughtUp)
        {
            m_caughtUp = true;
            if (m_onCaughtUp)
            {
                m_onCaughtUp();
            }
        }
    }
};

} // namespace org::limitless::seqeron::replayer::client::detail
