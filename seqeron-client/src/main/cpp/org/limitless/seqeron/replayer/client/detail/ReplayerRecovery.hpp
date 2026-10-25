#pragma once

// ReplayerRecovery — the walk/resume/gap decision state machine behind ReplayerStreamReceiver.
//
#include <array>
#include <cstdint>
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
#include "org_limitless_seqeron_sbe_replay/ReplayPending.h"
#include "org_limitless_seqeron_sbe_replay/Replaying.h"

namespace org::limitless::seqeron::replayer::client::detail {

/**
 * Everything ReplayerRecovery cannot do itself: the sends, the replay subscription, and the gauge.
 * ReplayerStreamReceiver implements it against Aeron; the unit suite substitutes a recorder.
 */
class ReplayerRecoveryActions
{
  public:
    // fromPosition: where to resume the active recording, or protocol::REPLAYER_FROM_START.
    // cutClusterSeqNo: the cut of the snapshot a restore checks, else protocol::REPLAYER_NO_CUT.
    virtual void sendReplayRequest(std::int64_t requestId, std::int64_t fromPosition, std::int64_t cutClusterSeqNo) = 0;
    virtual void openReplay(std::int64_t replaySessionId) = 0;
    virtual void closeReplay() = 0;
    virtual void recoveryStalled(bool stalled) = 0;
    // The id the Replayer addresses this client's replies to, unique on its media driver; -1 before start.
    virtual std::int64_t clientId() = 0;
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
// arrived, so a consumer's delivery-latency stats measure the queue, not the drain.
struct RetainRecordHeader
{
    std::int64_t groupSeqNo;
    std::int64_t clusterSeqNo;
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

// A cold start walks the node's active recording of its queue from its start, which must be the queue's: a recording
// that starts at a cut or was purged stops the walk at its first frame. A gap in groupSeqNo is repaired by resuming
// it at the last dispatched frame.
//
// Given a snapshot store (restoreFrom), a cold start first restores the newest snapshot it holds (doc/snapshot.md
// §7): it replays the recording from the snapshot's cut, and if the frame there is the round's SnapshotStarted the
// file recorded, reads the file's records into the handler, dispatching nothing, then resumes at the cut with it as
// the anchor. A file whose cut the recording does not hold, or whose records do not match its trailer, gives way to
// the next older one, and the last to a walk.
class ReplayerRecovery
{
  public:
    using OnSequenced = std::function<void(const protocol::SequencedEvent&)>;
    using OnConnected = std::function<void(const protocol::LifecycleEvent&)>;
    using OnDisconnected = std::function<void(const protocol::LifecycleEvent&)>;
    using OnLeadershipChanged =
        std::function<void(std::int32_t newLeaderMemberId, std::int64_t leadershipTermId, std::int64_t clusterSeqNo)>;
    using OnCaughtUp = std::function<void()>;

  private:
    static constexpr std::int64_t RESEND_INTERVAL_MS = 500;

    // How long an established replay may deliver nothing before it is re-requested.
    static constexpr std::int64_t REPLAY_STALL_TIMEOUT_MS = 5'000;

    // How long recovery may run without dispatching a single frame before it is reported as unconvergent.
    static constexpr std::int64_t RECOVERY_PROGRESS_TIMEOUT_MS = 30'000;

  public:
    // Caps on frames retained ahead of a hole (see retainFrame) — enough to cover a walk over a normal
    // recording, not a whole log; past either bound recovery resumes at the last dispatched frame.
    static constexpr std::size_t MAX_RETAINED_FRAMES = 65536;
    static constexpr std::size_t MAX_RETAINED_BYTES = 16UL * 1024 * 1024;

    // Records one duty cycle restores at most, so a large snapshot does not stall the session.
    static constexpr int MAX_RESTORE_RECORDS_PER_CYCLE = 1024;

  private:
    ReplayerRecoveryActions& m_actions;
    OnSequenced m_onSequenced;
    OnConnected m_onConnected;
    OnDisconnected m_onDisconnected;
    OnLeadershipChanged m_onLeadershipChanged;
    OnCaughtUp m_onCaughtUp;

    bool m_awaitingReplay = false;
    std::int64_t m_replaySessionId = -1;
    std::int64_t m_catchUpPosition = 0; // bounded replay's end position; replay done once the image reaches it
    // fromPosition of the current request, for idempotent resend; REPLAYER_FROM_START for a walk
    std::int64_t m_reqFromPosition = protocol::REPLAYER_FROM_START;
    // cutClusterSeqNo of the current request: the snapshot a restore is checking, else REPLAYER_NO_CUT
    std::int64_t m_reqCutClusterSeqNo = protocol::REPLAYER_NO_CUT;
    std::int64_t m_lastRequestMs = 0;
    std::int64_t m_requestId = 0; // advances per request, and a resend keeps it; replies not carrying it are stale
    std::int64_t m_answeredRequestId = -1;   // the request whose Replaying was taken: each resend draws it again
    std::int64_t m_lastReplayPosition = -1;  // last replay-image position seen; -1 = not attached yet
    std::int64_t m_lastReplayProgressMs = 0; // when it last changed — the stall watchdog's clock
    std::optional<std::string> m_fault;      // why recovery cannot go on; latched, raised by the receiver

    std::int64_t m_lastClusterSeqNo = 0;           // highest clusterSeqNo delivered; 0 = none yet
    std::int64_t m_lastGroupSeqNo = 0;             // groupSeqNo of that frame: the queue's continuity (F-6)
    std::int64_t m_lastFramePosition = 0;          // where that frame starts in the recording; requestResume's anchor
    std::int64_t m_resumeAnchorSequenceNumber = 0; // clusterSeqNo a resume replay must open at, or 0 if not resuming
    bool m_replayGapLogged = false;                // report a hole in replayed history once per episode, not per frame
    bool m_caughtUp = false;  // following live; revoked on a queue gap or an eviction, re-established at the seam
    bool m_queueLost = false; // evicted from the queue and not yet rejoined: nothing live reaches it

    std::int64_t m_noProgressSinceMs = 0; // when the current no-progress episode started; 0 = none timed
    bool m_recoveryStallReported = false;

    // Live queue frames from beyond the current hole, retained in arrival order (see retainFrame).
    RetainBlockPool m_retainPool;
    std::deque<RetainBlock*> m_retained;
    std::size_t m_retainReadOffset = 0;          // offset of the next unconsumed record within m_retained.front()
    std::int64_t m_retainTailSequenceNumber = 0; // clusterSeqNo of the most recently retained frame; dedups redelivery
    std::size_t m_retainFrameCount = 0;
    std::size_t m_retainBytes = 0;
    bool m_retainOverflowed = false;
    bool m_retainOverflowLogged = false; // that overflow reported once per episode, not per frame

    std::int32_t m_currentLeaderMemberId = -1;

    SnapshotRestoreHandler* m_restoreHandler = nullptr;
    SnapshotStore* m_store = nullptr; // where the snapshot to restore is, or nullptr for none
    bool m_checkingCut = false;       // replaying from the chosen file's cut, to see that the recording holds it
    bool m_restoring = false;         // reading the chosen file's records; nothing is dispatched until its last
    bool m_recordsChecked = false;    // they match its trailer, so the restore reads them again, into the handler
    std::optional<SnapshotStore::Reader> m_reader;
    std::int32_t m_restoreRecordIndex = 0;
    std::int64_t m_snapshotRound = 0;                         // the chosen snapshot's round
    std::int64_t m_cutGroupSeqNo = 0;                         // of the frame at its cut, off the replay that found it
    std::int64_t m_cutFramePosition = 0;                      // where that frame starts here; the file's is a hint
    std::optional<protocol::SnapshotHeader> m_restoredHeader; // of the restore in progress, empty before record 0
    std::optional<std::string> m_restoreFailure;              // why the restore cannot proceed; latched

  public:
    explicit ReplayerRecovery(ReplayerRecoveryActions& actions, OnSequenced onSequenced, OnConnected onConnected = {},
                              OnDisconnected onDisconnected = {}, OnLeadershipChanged onLeadershipChanged = {},
                              OnCaughtUp onCaughtUp = {}) :
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
     * Restores the newest snapshot in a store on start(), before anything is dispatched. Call before start().
     *
     * @param store   this instance's own snapshots; must outlive this object
     * @param handler takes the snapshot's records; must outlive this object
     */
    void restoreFrom(SnapshotStore& store, SnapshotRestoreHandler& handler)
    {
        m_store = &store;
        m_restoreHandler = &handler;
    }

    // Cold start: restore the newest local snapshot if restoring one, else walk the recording from its start.
    void start()
    {
        if (m_store != nullptr)
        {
            chooseBelow(std::numeric_limits<std::int64_t>::max());
        }
        else
        {
            requestReplay(protocol::REPLAYER_FROM_START);
        }
    }

    // Releases the snapshot file a restore in progress holds open. Nothing may be called after.
    void close()
    {
        m_reader.reset();
    }

    void onFrame(char* const frame, const std::uint64_t length, const std::int64_t framePosition,
                 const std::int64_t receiveNs, const bool fromReplay)
    {
        // The envelope is stripped once, here. Continuity is groupSeqNo's (F-6); de-duplication and a resume's anchor
        // are clusterSeqNo's, which increases along a queue but jumps (P-3).
        const protocol::FrameView entry = protocol::unwrapFrame(frame, length);
        if (m_restoreFailure || m_fault || !entry.valid)
        {
            return;
        }
        const auto sequenceNumber = entry.clusterSeqNo;
        const auto groupSeqNo = entry.groupSeqNo;
        // First frame off a resume replay: it must be the frame whose position requested. Positions follow from
        // the frames, and the recording changes only with the node's driver, so a mismatch is a fault.
        if (fromReplay && m_resumeAnchorSequenceNumber != 0)
        {
            const std::int64_t anchor = m_resumeAnchorSequenceNumber;
            m_resumeAnchorSequenceNumber = 0;
            if (m_checkingCut)
            {
                m_cutGroupSeqNo = groupSeqNo;
                m_cutFramePosition = framePosition;
                onCutReplayed(sequenceNumber == anchor && entry.clusterTimestampNs == m_reader->cutTimestampNs());
                return;
            }
            if (sequenceNumber != anchor)
            {
                fail("resume replay at position " + std::to_string(m_reqFromPosition) + " opened at clusterSeqNo=" +
                     std::to_string(sequenceNumber) + ", expected " + std::to_string(anchor));
                return;
            }
        }
        if (m_lastGroupSeqNo != 0)
        {
            if (sequenceNumber <= m_lastClusterSeqNo)
            {
                return;
            }
            if (groupSeqNo > m_lastGroupSeqNo + 1)
            {
                if (!fromReplay && !isRecovering())
                {
                    util::Logger::warn(util::component::ReplayerStreamReceiver, util::eventCode::QueueGap,
                                       "queue gap: expected groupSeqNo=%lld got %lld — "
                                       "resuming the recording at clusterSeqNo=%lld",
                                       static_cast<long long>(m_lastGroupSeqNo + 1), static_cast<long long>(groupSeqNo),
                                       static_cast<long long>(m_lastClusterSeqNo));
                    m_caughtUp = false;
                    requestResume();
                }
                if (!fromReplay)
                {
                    retainFrame(sequenceNumber, groupSeqNo, frame, length, framePosition, receiveNs);
                }
                else if (!m_replayGapLogged)
                {
                    m_replayGapLogged = true;
                    util::Logger::warn(util::component::ReplayerStreamReceiver, util::eventCode::QueueGap,
                                       "gap in REPLAYED history: expected groupSeqNo=%lld got %lld — this "
                                       "node's recording does not cover the hole; recovery cannot converge "
                                       "until it does",
                                       static_cast<long long>(m_lastGroupSeqNo + 1),
                                       static_cast<long long>(groupSeqNo));
                }
                return;
            }
        }
        else if (groupSeqNo != 1)
        {
            if (!fromReplay && isRecovering())
            {
                retainFrame(sequenceNumber, groupSeqNo, frame, length, framePosition, receiveNs);
                return;
            }
            util::Logger::fault(util::component::ReplayerStreamReceiver, util::eventCode::FirstFrameNotOne,
                                "FATAL: first frame observed has groupSeqNo=%lld, expected 1 — "
                                "this node's recording does not reach the start of the queue",
                                static_cast<long long>(groupSeqNo));
            latchFault("first frame observed has groupSeqNo=" + std::to_string(groupSeqNo) +
                       ", expected 1 — this node's recording does not reach the start of the queue");
            return;
        }
        dispatchFrame(frame, length, sequenceNumber, framePosition, receiveNs, fromReplay);
        drainRetained();
    }

    // One message off the Replayer's control stream (Replaying / ReplayPending).
    void onControl(char* const message, const std::uint64_t length)
    {
        if (length < sbe::replay::MessageHeader::encodedLength())
        {
            return;
        }

        sbe::replay::MessageHeader mh;
        mh.wrap(message, 0U, 0U, length);
        const std::uint64_t bodyOff = sbe::replay::MessageHeader::encodedLength();
        const std::int64_t clientId = m_actions.clientId();

        if (mh.templateId() == sbe::replay::Replaying::sbeTemplateId())
        {
            sbe::replay::Replaying replaying;
            replaying.wrapForDecode(message, bodyOff, mh.blockLength(), mh.version(), length);
            if (replaying.clientId() != clientId || replaying.requestId() != m_requestId ||
                m_requestId == m_answeredRequestId)
            {
                return; // a superseded request's, or a resend's repeat of the answer already taken
            }
            m_answeredRequestId = m_requestId;
            onReplaying(replaying.replaySessionId(), replaying.catchUpPosition());
        }
        else if (mh.templateId() == sbe::replay::ReplayPending::sbeTemplateId())
        {
            sbe::replay::ReplayPending replayPending;
            replayPending.wrapForDecode(message, bodyOff, mh.blockLength(), mh.version(), length);
            if (replayPending.clientId() == clientId && replayPending.requestId() == m_requestId)
            {
                m_lastRequestMs = m_actions.nowMs();
            }
        }
    }

    // Why recovery cannot go on, or empty. Latched: the receiver raises it from its duty cycle, since a throw from
    // inside a fragment handler is swallowed.
    const std::optional<std::string>& fault() const
    {
        return m_fault;
    }

    // The queue's image went away or came back. Its driver evicts an untethered subscriber that stops polling, and
    // rejoins it at the tip once rested, so until then the client is not following live whatever its replay reached.
    // Nothing is requested: the first live frame after the rejoin shows the hole. A queue that closes, as its node
    // stops, takes the image too.
    void onQueueImage(const bool available)
    {
        m_queueLost = !available;
        if (m_queueLost)
        {
            m_caughtUp = false;
            util::Logger::warn(
                util::component::ReplayerStreamReceiver, util::eventCode::QueueGap,
                "evicted from the queue, or it closed, at clusterSeqNo=%lld — not caught up until it is back",
                static_cast<long long>(m_lastClusterSeqNo));
        }
    }

    void onReplayPosition(const std::int64_t position)
    {
        if (position >= m_catchUpPosition)
        {
            onReplayReachedBound();
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
            onReplayReachedBound();
        }
        else
        {
            util::Logger::warn(util::component::ReplayerStreamReceiver, util::eventCode::QueueGap,
                               "replay image closed at position %lld, short of catchUpPosition %lld — the replay was "
                               "stopped under us; re-requesting it",
                               static_cast<long long>(finalPosition), static_cast<long long>(m_catchUpPosition));
            reRequestCurrent();
        }
    }

    // Returns the records the restore read, the duty cycle's work; timers are none.
    std::int32_t doTimers(const bool requestPublicationPending)
    {
        const std::int32_t records = m_restoring ? restoreRecords() : 0;
        const std::int64_t nowMs = m_actions.nowMs();
        if (m_awaitingReplay && (requestPublicationPending || (nowMs - m_lastRequestMs) > RESEND_INTERVAL_MS))
        {
            resendRequest();
        }
        // >= 0: 0 is an ordinary replaySessionId, and -1 alone means none.
        if (m_replaySessionId >= 0 && (nowMs - m_lastReplayProgressMs) > REPLAY_STALL_TIMEOUT_MS)
        {
            onReplayStalled();
        }
        return records;
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
                            "recovery has dispatched nothing for >%lldms: lastClusterSeqNo=%lld fromPosition=%lld "
                            "awaitingReplay=%d replaySession=%lld checkingCut=%d restoring=%d — holding; check this "
                            "node's Replayer and its recording",
                            static_cast<long long>(RECOVERY_PROGRESS_TIMEOUT_MS),
                            static_cast<long long>(m_lastClusterSeqNo), static_cast<long long>(m_reqFromPosition),
                            m_awaitingReplay ? 1 : 0, static_cast<long long>(m_replaySessionId), m_checkingCut ? 1 : 0,
                            m_restoring ? 1 : 0);
        m_actions.recoveryStalled(true);
        return true;
    }

    bool isCaughtUp() const
    {
        return m_caughtUp;
    }

    std::int64_t lastClusterSeqNo() const
    {
        return m_lastClusterSeqNo;
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
        return m_replaySessionId >= 0 || m_awaitingReplay || m_restoring;
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

    std::int64_t requestId() const
    {
        return m_requestId;
    }

    std::int64_t requestFromPosition() const
    {
        return m_reqFromPosition;
    }

    /** Frames held ahead of the current hole. */
    std::size_t retainedFrameCount() const
    {
        return m_retainFrameCount;
    }

  private:
    void requestReplay(const std::int64_t fromPosition, const std::int64_t cutClusterSeqNo = protocol::REPLAYER_NO_CUT)
    {
        if (fromPosition == protocol::REPLAYER_FROM_START)
        {
            m_resumeAnchorSequenceNumber = 0; // a walk supersedes any resume in flight
        }
        m_reqFromPosition = fromPosition;
        m_reqCutClusterSeqNo = cutClusterSeqNo;
        m_awaitingReplay = true;
        m_replaySessionId = -1;
        m_actions.closeReplay();
        m_lastRequestMs = m_actions.nowMs();
        ++m_requestId;
        m_actions.sendReplayRequest(m_requestId, fromPosition, cutClusterSeqNo);
    }

    // Sends the current request again, under its requestId (spec R-1): the Replayer answers a resend of the request a
    // running replay answers with that replay, so a slow answer costs no replay.
    void resendRequest()
    {
        m_lastRequestMs = m_actions.nowMs();
        m_actions.sendReplayRequest(m_requestId, m_reqFromPosition, m_reqCutClusterSeqNo);
    }

    void requestResume()
    {
        requestReplay(m_lastFramePosition);
        m_resumeAnchorSequenceNumber = m_lastClusterSeqNo;
    }

    // Opens the newest snapshot file below a round and replays the recording from its cut, or walks the recording
    // when there is none. A format this build does not read stops recovery.
    void chooseBelow(const std::int64_t belowRound)
    {
        m_reader.reset();
        for (std::int64_t round = m_store->latestRound(belowRound); round >= 0; round = m_store->latestRound(round))
        {
            m_reader = m_store->open(round);
            if (!m_reader)
            {
                util::Logger::warn(util::component::ReplayerStreamReceiver, util::eventCode::SnapshotStoreFailed,
                                   "round %lld's snapshot file is cut short; trying an older snapshot",
                                   static_cast<long long>(round));
                continue;
            }
            m_snapshotRound = round;
            if (!m_restoreHandler->supportsFormatVersion(m_reader->formatVersion()))
            {
                failRestore("round " + std::to_string(round) + " has formatVersion " +
                            std::to_string(m_reader->formatVersion()) + ", which this build does not read");
                return;
            }
            requestCut();
            return;
        }
        requestReplay(protocol::REPLAYER_FROM_START);
    }

    // Replays the recording from the chosen snapshot's cut; its first frame decides the restore.
    void requestCut()
    {
        m_checkingCut = true;
        requestReplay(m_reader->cutPosition(), m_reader->cutClusterSeqNo());
        m_resumeAnchorSequenceNumber = m_reader->cutClusterSeqNo();
    }

    // The replay from the cut opened: on the round's SnapshotStarted, the file's records are read; on anything else,
    // or nothing, this recording is not the log the file was cut from, and an older file is tried.
    void onCutReplayed(const bool matches)
    {
        m_checkingCut = false;
        m_replaySessionId = -1;
        m_actions.closeReplay();
        if (!matches)
        {
            util::Logger::warn(util::component::ReplayerStreamReceiver, util::eventCode::SnapshotStoreFailed,
                               "round %lld's cut, clusterSeqNo=%lld at position %lld, is not in this node's "
                               "recording; trying an older snapshot",
                               static_cast<long long>(m_snapshotRound),
                               static_cast<long long>(m_reader->cutClusterSeqNo()),
                               static_cast<long long>(m_reader->cutPosition()));
            chooseBelow(m_snapshotRound);
            return;
        }
        m_restoring = true;
        m_recordsChecked = false;
        m_restoreRecordIndex = 0;
        m_restoredHeader.reset();
        util::Logger::info(util::component::ReplayerStreamReceiver,
                           "restoring round %lld, cut at clusterSeqNo=%lld position=%lld",
                           static_cast<long long>(m_snapshotRound), static_cast<long long>(m_reader->cutClusterSeqNo()),
                           static_cast<long long>(m_reader->cutPosition()));
    }

    // The next records of the restore, at most MAX_RESTORE_RECORDS_PER_CYCLE, once checkRecords has matched them to
    // the file's trailer; records that fail it now changed underneath the restore. Returns how many it passed on.
    std::int32_t restoreRecords()
    {
        m_noProgressSinceMs = 0;
        if (!m_recordsChecked)
        {
            return checkRecords();
        }
        std::span<const std::uint8_t> record;
        for (int i = 0; i < MAX_RESTORE_RECORDS_PER_CYCLE; ++i)
        {
            const std::int32_t length = m_reader->next(record);
            if (length == SnapshotStore::Reader::END)
            {
                completeRestore();
                return i;
            }
            if (length == SnapshotStore::Reader::DAMAGED)
            {
                failRestore("round " + std::to_string(m_snapshotRound) +
                            "'s snapshot file is damaged: its records do not match its trailer");
                return i;
            }
            if (m_restoreRecordIndex == 0)
            {
                m_restoredHeader = protocol::SnapshotHeader::decode(record.data(), record.size());
                if (!m_restoredHeader)
                {
                    failRestore("round " + std::to_string(m_snapshotRound) + " has a header of version " +
                                std::to_string(protocol::SnapshotHeader::version(record.data(), record.size())) +
                                ", which this build does not read");
                    return i;
                }
                m_restoreHandler->onSnapshotHeader(*m_restoredHeader);
            }
            else
            {
                m_restoreHandler->onSnapshotRecord(record, m_restoreRecordIndex - 1, m_reader->formatVersion());
            }
            ++m_restoreRecordIndex;
        }
        return MAX_RESTORE_RECORDS_PER_CYCLE;
    }

    // Reads the next records of the chosen file without passing them on, at most MAX_RESTORE_RECORDS_PER_CYCLE, so a
    // damaged file gives way to an older one before the handler has seen any. Returns how many it read.
    std::int32_t checkRecords()
    {
        std::span<const std::uint8_t> record;
        std::int32_t records = 0;
        std::int32_t length = 0;
        while (records < MAX_RESTORE_RECORDS_PER_CYCLE && (length = m_reader->next(record)) >= 0)
        {
            ++records;
        }
        if (length >= 0)
        {
            return records;
        }
        m_reader.reset();
        if (length == SnapshotStore::Reader::END && (m_reader = m_store->open(m_snapshotRound)))
        {
            m_recordsChecked = true;
            return records;
        }
        m_restoring = false;
        util::Logger::warn(util::component::ReplayerStreamReceiver, util::eventCode::SnapshotStoreFailed,
                           "round %lld's snapshot file is damaged: its records do not match its trailer, or it "
                           "cannot be opened again; trying an older snapshot",
                           static_cast<long long>(m_snapshotRound));
        chooseBelow(m_snapshotRound);
        return records;
    }

    // The snapshot is restored: the state is that after the cut, and so is the leadership its header carries.
    // Resumes at the cut, which is dropped as already dispatched.
    void completeRestore()
    {
        m_restoring = false;
        m_lastClusterSeqNo = m_reader->cutClusterSeqNo();
        m_lastGroupSeqNo = m_cutGroupSeqNo;
        m_lastFramePosition = m_cutFramePosition;
        m_reader.reset();
        m_currentLeaderMemberId = m_restoredHeader->leaderMemberId;
        util::Logger::info(util::component::ReplayerStreamReceiver,
                           "restored round %lld; resuming after clusterSeqNo=%lld",
                           static_cast<long long>(m_snapshotRound), static_cast<long long>(m_lastClusterSeqNo));
        if (m_onLeadershipChanged && m_restoredHeader->leadershipTermId >= 0)
        {
            m_onLeadershipChanged(m_currentLeaderMemberId, m_restoredHeader->leadershipTermId, m_lastClusterSeqNo);
        }
        requestResume();
    }

    // Stops recovering for good: what is left cannot be restored, and nothing else may be dispatched in its place.
    void failRestore(std::string reason)
    {
        m_checkingCut = false;
        m_restoring = false;
        m_reader.reset();
        m_awaitingReplay = false;
        m_replaySessionId = -1;
        m_actions.closeReplay();
        util::Logger::fault(util::component::ReplayerStreamReceiver, util::eventCode::SnapshotRestoreFailed,
                            "cannot restore a snapshot: %s", reason.c_str());
        m_restoreFailure = std::move(reason);
    }

    // Re-asks for whatever is in flight; a resume goes back through requestResume for a fresh anchor, and so does a
    // walk that has dispatched a frame.
    void reRequestCurrent()
    {
        if (m_checkingCut)
        {
            requestCut();
        }
        else if (m_reqFromPosition == protocol::REPLAYER_FROM_START && m_lastGroupSeqNo == 0)
        {
            requestReplay(protocol::REPLAYER_FROM_START); // same request verbatim, new requestId
        }
        else
        {
            requestResume();
        }
    }

    void onReplaying(const std::int64_t session, const std::int64_t replayCatchUpPosition)
    {
        m_awaitingReplay = false;
        if (session == protocol::REPLAYER_NO_REPLAY_NEEDED)
        {
            if (m_checkingCut)
            {
                onCutReplayed(false); // the cut is not a frame of this recording
                return;
            }
            if (m_reqFromPosition != protocol::REPLAYER_FROM_START)
            {
                fail("the Replayer refused a resume at position " + std::to_string(m_reqFromPosition) +
                     " above clusterSeqNo=" + std::to_string(m_lastClusterSeqNo));
                return;
            }
            m_replaySessionId = -1; // the recording holds nothing yet — follow the live queue
            if (reachedTip())
            {
                notifyCaughtUp();
            }
            return;
        }
        m_replaySessionId = session;
        m_catchUpPosition = replayCatchUpPosition;
        m_actions.openReplay(session);
        m_lastReplayPosition = -1;
        m_lastReplayProgressMs = m_actions.nowMs();
    }

    // Stops recovering: the recording does not hold what the request expected of it.
    void fail(std::string reason)
    {
        util::Logger::fault(util::component::ReplayerStreamReceiver, util::eventCode::QueueGap,
                            "FATAL: %s — this node's recording does not match the positions this client holds",
                            reason.c_str());
        latchFault(std::move(reason));
    }

    // Stops recovering, for the receiver to raise the reason from its duty cycle.
    void latchFault(std::string reason)
    {
        m_awaitingReplay = false;
        m_replaySessionId = -1;
        m_actions.closeReplay();
        m_fault = std::move(reason);
    }

    // An attached (or expected) replay stopped delivering — see the watchdog in doTimers.
    void onReplayStalled()
    {
        util::Logger::warn(util::component::ReplayerStreamReceiver, util::eventCode::QueueGap,
                           "replay session %lld made no progress for %lldms at position %lld of "
                           "catchUpPosition %lld — re-requesting it",
                           static_cast<long long>(m_replaySessionId), static_cast<long long>(REPLAY_STALL_TIMEOUT_MS),
                           static_cast<long long>(m_lastReplayPosition), static_cast<long long>(m_catchUpPosition));
        reRequestCurrent();
    }

    // The replay reached its bound, or its image closed there. Closing the image frees the Replayer's slot.
    void onReplayReachedBound()
    {
        m_actions.closeReplay();
        m_replaySessionId = -1;
        if (m_replayGapLogged)
        {
            m_replayGapLogged = false; // the replay skipped a hole, so it did not reach the tip: resume from before it
            requestResume();
            return;
        }
        if (reachedTip())
        {
            notifyCaughtUp();
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

        m_lastClusterSeqNo = sequenceNumber;
        m_lastGroupSeqNo = view.groupSeqNo;
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

    void retainFrame(const std::int64_t sequenceNumber, const std::int64_t groupSeqNo, const char* const frame,
                     const std::uint64_t len, const std::int64_t framePosition, const std::int64_t receiveNs)
    {
        const std::size_t recordSize = sizeof(RetainRecordHeader) + len;
        if (m_retainFrameCount >= MAX_RETAINED_FRAMES || (m_retainBytes + len) > MAX_RETAINED_BYTES ||
            recordSize > RetainBlock::SIZE)
        {
            m_retainOverflowed = true;
            if (!m_retainOverflowLogged)
            {
                m_retainOverflowLogged = true;
                util::Logger::warn(util::component::ReplayerStreamReceiver, util::eventCode::QueueGap,
                                   "retained-frame buffer full at clusterSeqNo=%lld (%zu frames, %zu bytes) — "
                                   "dropping ahead-of-hole frames; recovery resumes at the last dispatched frame",
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
            const RetainRecordHeader header{ groupSeqNo, sequenceNumber, framePosition, receiveNs,
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
        while (peekRetained(header) && header.groupSeqNo <= m_lastGroupSeqNo + 1)
        {
            std::uint8_t* const payload = m_retained.front()->bytes.data() + m_retainReadOffset + sizeof(header);
            m_retainReadOffset += sizeof(header) + header.length;
            --m_retainFrameCount;
            m_retainBytes -= header.length;
            if (header.clusterSeqNo > m_lastClusterSeqNo) // otherwise the replay already covered it
            {
                dispatchFrame(reinterpret_cast<char*>(payload), header.length, header.clusterSeqNo, header.position,
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
            requestResume();
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
        if (!m_caughtUp && !m_queueLost)
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
