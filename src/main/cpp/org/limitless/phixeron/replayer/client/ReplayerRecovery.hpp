#pragma once

// ReplayerRecovery — the walk/resume/gap decision state machine behind ReplayerStreamReceiver.
//
#include <array>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <functional>
#include <vector>

// Reuses SequencedEvent / LifecycleEvent and the CLIENT_CONNECTED/DISCONNECTED template-id constants
// from the other sequenced-stream client — the two deliver the same frames off the same stream.
#include "org/limitless/phixeron/replayer/client/RecoveryProgressPolicy.hpp"
#include "org/limitless/phixeron/sequencer/ClusterStreamReceiver.hpp"
#include "org/limitless/phixeron/util/Logger.hpp"

// Replay-protocol control codecs (sbe-unsequenced.xml) + LeadershipChanged (sbe-sequenced.xml)
#include "org_limitless_phixeron_sbe_sequenced/LeadershipChanged.h"
#include "org_limitless_phixeron_sbe_unsequenced/MessageHeader.h"
#include "org_limitless_phixeron_sbe_unsequenced/ReplayPending.h"
#include "org_limitless_phixeron_sbe_unsequenced/ReplayUnavailable.h"
#include "org_limitless_phixeron_sbe_unsequenced/Replaying.h"

namespace org::limitless::phixeron::replayer::client {

namespace usq = org::limitless::phixeron::sbe::unsequenced;
namespace diag = org::limitless::phixeron::util;

using org::limitless::phixeron::sequencer::CLIENT_CONNECTED_TEMPLATE_ID;
using org::limitless::phixeron::sequencer::CLIENT_DISCONNECTED_TEMPLATE_ID;
using org::limitless::phixeron::sequencer::LifecycleEvent;
using org::limitless::phixeron::sequencer::SequencedEvent;

// Replaying.replaySessionId sentinel: "nothing to replay, you are at the tip — follow the live tap".
inline constexpr std::int64_t REPLAYER_NO_REPLAY_NEEDED = -1;

// LeadershipChanged (sbe-sequenced.xml template 5)
inline constexpr std::uint16_t LEADERSHIP_CHANGED_TEMPLATE_ID = 5;

/**
 * Everything ReplayerRecovery cannot do itself: the sends, the replay subscription, and the gauge.
 * ReplayerStreamReceiver implements it against Aeron; the unit suite substitutes a recorder.
 */
class ReplayerRecoveryActions
{
  public:
    virtual void sendReplayRequest(std::int64_t requestId, std::int32_t segmentIndex, std::int64_t fromPosition) = 0;
    virtual bool sendReplayComplete() = 0;
    virtual bool sendReplayHeartbeat() = 0;
    virtual void openReplay(std::int64_t replaySessionId) = 0;
    virtual void closeReplay() = 0;
    virtual void recoveryStalled(bool stalled) = 0;
    virtual std::int64_t nowMs() = 0;

  protected:
    ~ReplayerRecoveryActions() = default;
};

// Fixed-size block backing the retained-ahead FIFO (see retainMessages/drainRetained). Records are
// appended length-prefixed and never split across a block boundary — every message here is well
// under 512 bytes, so the wasted tail per boundary is bounded and negligible against SIZE.
struct MessagesBlock
{
    static constexpr std::size_t SIZE = 4096;
    std::array<std::uint8_t, SIZE> bytes;
    std::size_t used = 0;
};

// Per-record framing within a block. Carries the receive stamp and position from when the frame
// arrived, so a consumer's delivery-latency stats measure the tap, not the drain.
struct MessagesRecordHeader
{
    std::int64_t globalSeqNo;
    std::int64_t position;
    std::int64_t receiveNs;
    std::uint32_t length;
};

class MessagesBlockPool
{
public:
    ~MessagesBlockPool()
    {
        for (auto* block : m_freeList)
        {
            delete block;
        }
    }

    MessagesBlock* acquire()
    {
        MessagesBlock* block;
        if (m_freeList.empty())
        {
            block = new MessagesBlock();
        }
        else
        {
            block = m_freeList.back();
            m_freeList.pop_back();
            block->used = 0;
        }
        return block;
    }

    void release(MessagesBlock* const block)
    {
        m_freeList.push_back(block);
    }

private:
    std::vector<MessagesBlock*> m_freeList;
};

class ReplayerRecovery
{
  public:
    using OnSequenced = std::function<void(const SequencedEvent&)>;
    using OnConnected = std::function<void(const LifecycleEvent&)>;
    using OnDisconnected = std::function<void(const LifecycleEvent&)>;
    using OnLeadershipChanged = std::function<void(std::int32_t newLeaderMemberId, std::int64_t globalSeqNo)>;
    using OnCaughtUp = std::function<void()>;

  private:
    static constexpr std::int64_t RESEND_INTERVAL_MS = 500;

    // How long an established replay may deliver nothing before it is re-requested.
    static constexpr std::int64_t REPLAY_STALL_TIMEOUT_MS = 5'000;

    // How long recovery may run without dispatching a single frame before it is reported as unconvergent.
    static constexpr std::int64_t RECOVERY_PROGRESS_TIMEOUT_MS = 30'000;
    static constexpr std::int32_t RESUME_SEGMENT_INDEX = -1;

    // Caps on frames retained ahead of a hole (see retainMessages) — enough to cover a walk over a normal
    // recording, not a whole trading day; past either bound recovery falls back to re-walking.
    static constexpr std::size_t MAX_MESSAGES_FRAMES = 65536;
    static constexpr std::size_t MAX_MESSAGES_BYTES = 16UL * 1024 * 1024;

    using HdrSbe = org::limitless::phixeron::sbe::sequenced::MessageHeader;
    using HeaderComposite = org::limitless::phixeron::sbe::sequenced::Header;

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

    std::int64_t m_lastGlobalSeqNo = 0;   // highest globalSeqNo delivered; 0 = none yet
    std::int64_t m_lastFramePosition = 0; // where that frame starts in the recording; requestResume's anchor
    std::int64_t m_resumeAnchorSequenceNumber = 0;  // globalSeqNo a resume replay must open at, or 0 if not resuming
    bool m_replayGapLogged = false;       // report a hole in replayed history once per episode, not per frame
    bool m_caughtUp = false;              // following live; revoked on a tap gap, re-established at the seam

    RecoveryProgressPolicy m_recoveryProgress{ RECOVERY_PROGRESS_TIMEOUT_MS };

    // Live tap frames from beyond the current hole, retained in arrival order (see retainMessages).
    MessagesBlockPool m_messagesBlockPool;
    std::deque<MessagesBlock*> m_messagesBlocks;
    std::size_t m_messagesReadOffset = 0; // offset of the next unconsumed record within m_messagesBlocks.front()
    std::int64_t m_messagesTailSequenceNumber = 0;  // globalSeqNo of the most recently retained frame; dedups redelivery
    std::size_t m_messagesFrameCount = 0;
    std::size_t m_messagesBytes = 0;
    bool m_messagesOverflowed = false;
    bool m_messagesOverflowLogged = false; // that overflow reported once per episode, not per frame

    std::int32_t m_currentLeaderMemberId = -1;

    HdrSbe m_hdr;
    HeaderComposite m_header;

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
    {}

    ~ReplayerRecovery()
    {
        for (auto* block : m_messagesBlocks)
        {
            delete block;
        }
    }

    // Cold start: walk the recording chain from segment 0.
    void start()
    {
        requestReplay(0, 0);
    }

    void onFrame(char* const frame, const std::uint64_t length, const std::int64_t framePosition,
                 const std::int64_t receiveNs, const bool fromReplay)
    {
        if (length < HdrSbe::encodedLength() + HeaderComposite::encodedLength())
        {
            return;
        }

        m_hdr.wrap(frame, 0U, 0U, length);
        if (m_hdr.schemaId() != HdrSbe::sbeSchemaId())
        {
            return;
        }

        m_header.wrap(frame, HdrSbe::encodedLength(), 0U, length);
        const auto sequenceNumber = m_header.globalSeqNo();
        if (fromReplay && m_resumeAnchorSequenceNumber != 0)
        {
            const std::int64_t anchor = m_resumeAnchorSequenceNumber;
            m_resumeAnchorSequenceNumber = 0;
            if (sequenceNumber != anchor)
            {
                diag::Logger::warn(diag::Component::ReplayerStreamReceiver, diag::EventCode::TapGap,
                                   "resume replay opened at globalSeqNo=%lld, expected %lld — the active "
                                   "recording rotated under us; re-walking the recording chain from segment 0",
                                   static_cast<long long>(sequenceNumber), static_cast<long long>(anchor));
                requestReplay(0, 0);
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
                    diag::Logger::warn(diag::Component::ReplayerStreamReceiver, diag::EventCode::TapGap,
                                       "tap gap: expected globalSeqNo=%lld got %lld — "
                                       "resuming the recording at globalSeqNo=%lld",
                                       static_cast<long long>(m_lastGlobalSeqNo + 1), static_cast<long long>(sequenceNumber),
                                       static_cast<long long>(m_lastGlobalSeqNo));
                    m_caughtUp = false;
                    requestResume();
                }
                if (!fromReplay)
                {
                    retainMessages(sequenceNumber, frame, length, framePosition, receiveNs);
                }
                else if (!m_replayGapLogged)
                {
                    m_replayGapLogged = true;
                    diag::Logger::warn(diag::Component::ReplayerStreamReceiver, diag::EventCode::TapGap,
                                       "gap in REPLAYED history: expected globalSeqNo=%lld got %lld — this "
                                       "node's recording chain does not cover the hole; recovery cannot "
                                       "converge until it does",
                                       static_cast<long long>(m_lastGlobalSeqNo + 1), static_cast<long long>(sequenceNumber));
                }
                return;
            }
        }
        else if (sequenceNumber != 1)
        {
            if (!fromReplay && isRecovering())
            {
                retainMessages(sequenceNumber, frame, length, framePosition, receiveNs);
                return;
            }
            diag::Logger::fault(diag::Component::ReplayerStreamReceiver, diag::EventCode::FirstFrameNotOne,
                                "FATAL: first frame observed has globalSeqNo=%lld, expected 1 — "
                                "this node's recording does not reach the start of the log; aborting",
                                static_cast<long long>(sequenceNumber));
            std::abort();
        }
        dispatchFrame(frame, length, sequenceNumber, framePosition, receiveNs, fromReplay);
        drainRetained();
    }

    // One message off the Replayer's control stream (Replaying / ReplayPending / ReplayUnavailable).
    void onControl(char* const message, const std::uint64_t length)
    {
        if (length < usq::MessageHeader::encodedLength())
        {
            return;
        }

        usq::MessageHeader mh;
        mh.wrap(message, 0U, 0U, length);
        const std::uint64_t bodyOff = usq::MessageHeader::encodedLength();

        if (mh.templateId() == usq::Replaying::sbeTemplateId())
        {
            usq::Replaying replaying;
            replaying.wrapForDecode(message, bodyOff, mh.blockLength(), mh.version(), length);
            if (replaying.clientId() != m_clientId || replaying.requestId() != m_requestId)
            {
                return;
            }
            onReplaying(replaying.replaySessionId(), replaying.catchUpPosition(), replaying.recordingId());
        }
        else if (mh.templateId() == usq::ReplayPending::sbeTemplateId())
        {
            usq::ReplayPending replayPending;
            replayPending.wrapForDecode(message, bodyOff, mh.blockLength(), mh.version(), length);
            if (replayPending.clientId() == m_clientId && replayPending.requestId() == m_requestId)
            {
                m_lastRequestMs = m_actions.nowMs();
                m_replayerUnavailable = false; // queued, not refused — the episode ended (see Replaying)
            }
        }
        else if (mh.templateId() == usq::ReplayUnavailable::sbeTemplateId())
        {
            usq::ReplayUnavailable replayUnavailable;
            replayUnavailable.wrapForDecode(message, bodyOff, mh.blockLength(), mh.version(), length);
            if (replayUnavailable.clientId() == m_clientId && replayUnavailable.requestId() == m_requestId)
            {
                onReplayUnavailable();
            }
        }
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
            diag::Logger::warn(diag::Component::ReplayerStreamReceiver, diag::EventCode::TapGap,
                               "replay image closed at position %lld, short of catchUpPosition %lld — the replay was "
                               "stopped under us; re-requesting the same segment (index %d)",
                               static_cast<long long>(finalPosition), static_cast<long long>(m_catchUpPosition),
                               static_cast<int>(m_walkSegmentIndex));
            reRequestCurrent();
        }
    }

    void doTimers(const bool requestPublicationPending)
    {
        const std::int64_t nowMs = m_actions.nowMs();
        if (m_awaitingReplay && (requestPublicationPending || (nowMs - m_lastRequestMs) > RESEND_INTERVAL_MS))
        {
            requestReplay(m_walkSegmentIndex, m_reqFromPosition); // re-send the same request verbatim
        }
        if (m_completePending)
        {
            sendReplayComplete();
        }
        if (m_replaySessionId >= 1)
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
        if (m_caughtUp || !m_recoveryProgress.onNoProgress(m_actions.nowMs()))
        {
            return false;
        }
        diag::Logger::fault(diag::Component::ReplayerStreamReceiver, diag::EventCode::RecoveryStalled,
                            "recovery has dispatched nothing for >%lldms: lastGlobalSeqNo=%lld segment=%d "
                            "awaitingReplay=%d replaySession=%lld replayerUnavailable=%d — holding; check "
                            "this node's Replayer and its recording chain",
                            static_cast<long long>(RECOVERY_PROGRESS_TIMEOUT_MS),
                            static_cast<long long>(m_lastGlobalSeqNo), static_cast<int>(m_walkSegmentIndex),
                            m_awaitingReplay ? 1 : 0, static_cast<long long>(m_replaySessionId),
                            m_replayerUnavailable ? 1 : 0);
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
        return m_replaySessionId >= 0 || m_awaitingReplay;
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
        if (session == REPLAYER_NO_REPLAY_NEEDED)
        {
            if (m_walkSegmentIndex < 0)
            {
                diag::Logger::warn(diag::Component::ReplayerStreamReceiver, diag::EventCode::TapGap,
                                   "resume at position %lld answered 'nothing to replay' while a hole is open "
                                   "above globalSeqNo=%lld — the active recording rotated under us; re-walking "
                                   "the recording chain from segment 0",
                                   static_cast<long long>(m_reqFromPosition),
                                   static_cast<long long>(m_lastGlobalSeqNo));
                requestReplay(0, 0);
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
                diag::Logger::warn(diag::Component::ReplayerStreamReceiver, diag::EventCode::TapGap,
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

    void onReplayUnavailable()
    {
        if (!m_replayerUnavailable)
        {
            m_replayerUnavailable = true;
            diag::Logger::fault(diag::Component::ReplayerStreamReceiver, diag::EventCode::ReplayUnavailable,
                                "this node's Replayer has no valid history to serve (its archive failed the "
                                "globalSeqNo-1 integrity check) — holding, not dispatching; repair the node's "
                                "archive and restart its Replayer");
        }
        m_lastRequestMs = m_actions.nowMs();
    }

    // An attached (or expected) replay stopped delivering — see the watchdog in doTimers.
    void onReplayStalled()
    {
        diag::Logger::warn(diag::Component::ReplayerStreamReceiver, diag::EventCode::TapGap,
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
        if (m_recoveryProgress.onProgress())
        {
            m_actions.recoveryStalled(false);
        }
        m_hdr.wrap(frame, 0U, 0U, length);
        const std::uint16_t templateId = m_hdr.templateId();
        m_header.wrap(frame, HdrSbe::encodedLength(), 0U, length);

        m_lastGlobalSeqNo = sequenceNumber;
        m_replayGapLogged = false;
        m_lastFramePosition = framePosition;
        if (!fromReplay && !m_caughtUp && !m_messagesOverflowed)
        {
            notifyCaughtUp();
        }

        const auto srcId = m_header.sourceId();
        const auto connId = m_header.connectionId();
        const auto sessId = m_header.sessionId();
        const auto ts = m_header.timestamp();
        const auto origin = m_header.origin();
        if (templateId == CLIENT_CONNECTED_TEMPLATE_ID || templateId == CLIENT_DISCONNECTED_TEMPLATE_ID)
        {
            // Both carry the same header-only LifecycleEvent; only the callback differs.
            const OnConnected& callback = templateId == CLIENT_CONNECTED_TEMPLATE_ID ? m_onConnected : m_onDisconnected;
            if (callback)
            {
                callback(LifecycleEvent{ .globalSeqNo = sequenceNumber,
                                         .sourceId = srcId,
                                         .connectionId = connId,
                                         .sourceSessionId = sessId,
                                         .clusterTimestamp = ts,
                                         .receiveTimeNs = receiveNs });
            }
            return;
        }
        if (templateId == LEADERSHIP_CHANGED_TEMPLATE_ID)
        {
            sbe::sequenced::LeadershipChanged leadershipChanged;
            leadershipChanged.wrapForDecode(frame, HdrSbe::encodedLength(), m_hdr.blockLength(), m_hdr.version(),
                                            length);
            m_currentLeaderMemberId = leadershipChanged.newLeaderMemberId();
            if (m_onLeadershipChanged)
            {
                m_onLeadershipChanged(m_currentLeaderMemberId, sequenceNumber);
            }
            return;
        }
        if (m_onSequenced)
        {
            m_onSequenced(SequencedEvent{ .globalSeqNo = sequenceNumber,
                                          .sourceId = srcId,
                                          .connectionId = connId,
                                          .sourceSessionId = sessId,
                                          .clusterTimestamp = ts,
                                          .receiveTimeNs = receiveNs,
                                          .origin = origin,
                                          .templateId = templateId,
                                          .blockLength = m_hdr.blockLength(),
                                          .version = m_hdr.version(),
                                          .payload = frame,
                                          .payloadLength = length,
                                          .position = framePosition });
        }
    }

    void retainMessages(const std::int64_t sequenceNumber, const char* const frame, const std::uint64_t len,
                        const std::int64_t framePosition, const std::int64_t receiveNs)
    {
        const std::size_t recordSize = sizeof(MessagesRecordHeader) + len;
        if (m_messagesFrameCount >= MAX_MESSAGES_FRAMES || (m_messagesBytes + len) > MAX_MESSAGES_BYTES ||
            recordSize > MessagesBlock::SIZE)
        {
            m_messagesOverflowed = true;
            if (!m_messagesOverflowLogged)
            {
                m_messagesOverflowLogged = true;
                diag::Logger::warn(diag::Component::ReplayerStreamReceiver, diag::EventCode::TapGap,
                                   "retained-frame buffer full at globalSeqNo=%lld (%zu frames, %zu bytes) — "
                                   "dropping ahead-of-hole frames; recovery falls back to re-walking",
                                   static_cast<long long>(sequenceNumber), m_messagesFrameCount, m_messagesBytes);
            }
        }
        else if (m_messagesFrameCount <= 0 || sequenceNumber > m_messagesTailSequenceNumber)
        {
            if (m_messagesBlocks.empty() || m_messagesBlocks.back()->used + recordSize > MessagesBlock::SIZE)
            {
                m_messagesBlocks.push_back(m_messagesBlockPool.acquire());
            }
            MessagesBlock* const tail = m_messagesBlocks.back();
            const MessagesRecordHeader header{ sequenceNumber, framePosition, receiveNs, static_cast<std::uint32_t>(len) };
            std::memcpy(tail->bytes.data() + tail->used, &header, sizeof(header));
            std::memcpy(tail->bytes.data() + tail->used + sizeof(header), frame, len);
            tail->used += recordSize;
            ++m_messagesFrameCount;
            m_messagesBytes += len;
            m_messagesTailSequenceNumber = sequenceNumber;
        }
    }

    // Reads the oldest retained record's header, recycling any fully-read blocks first. False when the FIFO is empty
    bool peekRetained(MessagesRecordHeader& header)
    {
        while (!m_messagesBlocks.empty() && m_messagesReadOffset >= m_messagesBlocks.front()->used)
        {
            m_messagesBlockPool.release(m_messagesBlocks.front());
            m_messagesBlocks.pop_front();
            m_messagesReadOffset = 0;
        }

        const bool empty = m_messagesBlocks.empty();
        if (!empty)
        {
            std::memcpy(&header, m_messagesBlocks.front()->bytes.data() + m_messagesReadOffset, sizeof(header));
        }
        return !empty;
    }

    // Exits when the FIFO runs dry, or on a hole below the oldest retained frame.
    void drainRetained()
    {
        MessagesRecordHeader header;
        while (peekRetained(header) && header.globalSeqNo <= m_lastGlobalSeqNo + 1)
        {
            std::uint8_t* const payload = m_messagesBlocks.front()->bytes.data() + m_messagesReadOffset + sizeof(header);
            m_messagesReadOffset += sizeof(header) + header.length;
            --m_messagesFrameCount;
            m_messagesBytes -= header.length;
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
        const bool success = m_messagesBlocks.empty() && !m_messagesOverflowed;
        if (!success)
        {
            endOverflowEpisode();
            requestReplay(0, 0);
        }
        return success;
    }

    void endOverflowEpisode()
    {
        m_messagesOverflowed = false;
        m_messagesOverflowLogged = false;
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

} // namespace org::limitless::phixeron::replayer::client
