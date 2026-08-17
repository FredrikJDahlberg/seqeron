#pragma once

// ReplayerRecovery — the walk/resume/gap decision state machine behind ReplayerStreamReceiver.
//
// Split out from the receiver so the decisions can be driven directly: cold-start walk, steady-state
// resume, contiguity/de-dupe, the retained-ahead FIFO, the Replayer control-stream transitions, and the
// two watchdogs. Everything needing a live Aeron publication/subscription/image sits behind
// ReplayerRecoveryActions and the clock is injected, so this class holds no Aeron runtime and no wall
// clock — the same decision/transport seam the project draws at Sequencer/SequencerService and
// Replayer/ReplayerService.
//
// Gap recovery is anchored on globalSeqNo and merely accelerated by position: a tap gap asks the
// Replayer to RESUME the active recording at the frame last dispatched, and any mismatch falls back to
// re-walking the chain from segment 0, which needs no position to be sound.

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

// LeadershipChanged (sbe-sequenced.xml template 5), synthesized onto the sequenced stream; like
// CLIENT_CONNECTED/DISCONNECTED it is non-FIX and outside the FIX-MsgType-derived template range.
inline constexpr std::uint16_t LEADERSHIP_CHANGED_TEMPLATE_ID = 5;

/**
 * Everything ReplayerRecovery cannot do itself: the sends, the replay subscription, and the gauge.
 * ReplayerStreamReceiver implements it against Aeron; the unit suite substitutes a recorder.
 */
class ReplayerRecoveryActions
{
  public:
    virtual ~ReplayerRecoveryActions() = default;

    // Offer a ReplayRequest. Best-effort by design — see requestReplay on why it must not be retried
    // any faster than the resend timer does.
    virtual void sendReplayRequest(std::int64_t requestId, std::int32_t segmentIndex, std::int64_t fromPosition) = 0;

    // Offer a ReplayComplete / ReplayHeartbeat; false if it did not reach the wire.
    virtual bool sendReplayComplete() = 0;
    virtual bool sendReplayHeartbeat() = 0;

    // Subscribe to exactly one replay — this one — or drop whatever subscription is open.
    virtual void openReplay(std::int64_t replaySessionId) = 0;
    virtual void closeReplay() = 0;

    // The phixeron.app.recoveryStalled gauge.
    virtual void recoveryStalled(bool stalled) = 0;
};

class ReplayerRecovery
{
  public:
    using OnSequenced = std::function<void(const SequencedEvent&)>;
    using OnConnected = std::function<void(const LifecycleEvent&)>;
    using OnDisconnected = std::function<void(const LifecycleEvent&)>;
    using OnLeadershipChanged = std::function<void(std::int32_t newLeaderMemberId, std::int64_t globalSeqNo)>;
    using OnCaughtUp = std::function<void()>;
    using Clock = std::function<std::int64_t()>; // epoch millis

    ReplayerRecovery(const std::int32_t clientId, ReplayerRecoveryActions& actions, Clock clock,
                     OnSequenced onSequenced, OnConnected onConnected = {}, OnDisconnected onDisconnected = {},
                     OnLeadershipChanged onLeadershipChanged = {}, OnCaughtUp onCaughtUp = {}) :
      m_clientId(clientId),
      m_actions(actions),
      m_clock(std::move(clock)),
      m_onSequenced(std::move(onSequenced)),
      m_onConnected(std::move(onConnected)),
      m_onDisconnected(std::move(onDisconnected)),
      m_onLeadershipChanged(std::move(onLeadershipChanged)),
      m_onCaughtUp(std::move(onCaughtUp))
    {}

    // Returns any blocks still held by the retained-ahead FIFO (see retainMessages/drainRetained) — the
    // pool's own destructor only frees its free list, not blocks still checked out to m_messagesBlocks.
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

    // One sequenced frame, off the live tap (fromReplay=false) or an attached replay image. framePosition
    // is where the frame starts in the recording — the tap, a replay image and the recording itself all
    // count positions in the same space, and requestResume anchors on it.
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
        const auto gseq = m_header.globalSeqNo();

        // First frame off a resume replay (see requestResume): it must be the frame whose position we
        // anchored the request on. Anything else means that position no longer denotes that frame — the
        // active recording rotated under us — so drop back to the walk rather than ride an arbitrary
        // mid-stream point. Checked ahead of the de-dupe below, which would otherwise swallow the
        // anchor frame itself and leave the mismatch invisible.
        if (fromReplay && m_resumeAnchorGseq != 0)
        {
            const std::int64_t anchor = m_resumeAnchorGseq;
            m_resumeAnchorGseq = 0;
            if (gseq != anchor)
            {
                diag::Logger::warn(diag::Component::ReplayerStreamReceiver, diag::EventCode::TapGap,
                                   "resume replay opened at globalSeqNo=%lld, expected %lld — the active "
                                   "recording rotated under us; re-walking the recording chain from segment 0",
                                   static_cast<long long>(gseq), static_cast<long long>(anchor));
                requestReplay(0, 0);
                return;
            }
        }

        // Contiguity / de-duplication — identical invariant to ClusterStreamClient: globalSeqNo
        // increments by exactly one per event, so any forward jump is a gap. Drop dups; a frame from
        // beyond the hole is RETAINED (see retainMessages) rather than dropped, so the replay closing the
        // hole hands straight over to it.
        if (m_lastGlobalSeqNo != 0)
        {
            if (gseq <= m_lastGlobalSeqNo)
            {
                return;
            }
            if (gseq > m_lastGlobalSeqNo + 1)
            {
                // Only a steady-state hole is a gap worth re-walking for. Mid-walk the tap legitimately
                // runs ahead of the replay, so isRecovering() suppresses the trigger and lets the
                // in-flight walk finish rather than superseding it with the frames it is racing.
                if (!fromReplay && !isRecovering())
                {
                    diag::Logger::warn(diag::Component::ReplayerStreamReceiver, diag::EventCode::TapGap,
                                       "tap gap: expected globalSeqNo=%lld got %lld — "
                                       "resuming the recording at globalSeqNo=%lld",
                                       static_cast<long long>(m_lastGlobalSeqNo + 1), static_cast<long long>(gseq),
                                       static_cast<long long>(m_lastGlobalSeqNo));
                    // No longer following live: consumers gate leader-only emission and FixGateway's
                    // tap-stall watchdog on isCaughtUp(), and neither holds again until the stream goes
                    // contiguous (notifyCaughtUp re-fires from the seam below).
                    m_caughtUp = false;
                    // Resume just below the hole rather than re-walking the whole chain; requestResume
                    // falls back to the walk if the recording rotated under the position it anchors on.
                    // Never resumes the stale walk index — that is a cold-start cursor, not a position.
                    requestResume();
                }
                if (!fromReplay)
                {
                    retainMessages(gseq, frame, length, framePosition, receiveNs);
                }
                else if (!m_replayGapLogged)
                {
                    // A hole in REPLAYED history: either the recording chain itself is discontinuous, or
                    // the tethered IPC replay lost a fragment, which it should not. Nothing unsafe
                    // follows — the frame is dropped and the contiguity invariant still holds — but the
                    // walk cannot converge past this, so it is worth saying once rather than retrying in
                    // silence. Cleared on the next in-order dispatch, so a later episode reports again.
                    m_replayGapLogged = true;
                    diag::Logger::warn(diag::Component::ReplayerStreamReceiver, diag::EventCode::TapGap,
                                       "gap in REPLAYED history: expected globalSeqNo=%lld got %lld — this "
                                       "node's recording chain does not cover the hole; recovery cannot "
                                       "converge until it does",
                                       static_cast<long long>(m_lastGlobalSeqNo + 1), static_cast<long long>(gseq));
                }
                return;
            }
        }
        else if (gseq != 1)
        {
            if (!fromReplay && isRecovering())
            {
                // No baseline yet and the cold-start walk is still in flight: this is just the live tap
                // running ahead of a replay that has not reached globalSeqNo 1 yet. Only the walk may
                // establish the baseline — adopting this frame's would BE the arbitrary mid-stream
                // baseline the abort below exists to prevent — but it is still real data, so retain it.
                retainMessages(gseq, frame, length, framePosition, receiveNs);
                return;
            }
            // The very first frame this client ever sees — replayed history, or the live tap right
            // after a cold-start NO_REPLAY_NEEDED, which the Replayer sends at segment 0 only when the
            // recording is empty — must be globalSeqNo 1.
            diag::Logger::fault(diag::Component::ReplayerStreamReceiver, diag::EventCode::FirstFrameNotOne,
                                "FATAL: first frame observed has globalSeqNo=%lld, expected 1 — "
                                "this node's recording does not reach the start of the log; aborting",
                                static_cast<long long>(gseq));
            std::abort();
        }
        dispatchFrame(frame, length, gseq, framePosition, receiveNs, fromReplay);
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
            usq::Replaying dec;
            dec.wrapForDecode(message, bodyOff, mh.blockLength(), mh.version(), length);
            if (dec.clientId() != m_clientId)
            {
                return; // another replica's reply on the shared control stream
            }
            if (dec.requestId() != m_requestId)
            {
                // Answer to a request we have already superseded by re-sending: its replay session was
                // stopped when the Replayer took the newer request. Attaching to it would ride an image
                // that closes short of its bound, and clearing m_awaitingReplay would stop the resend
                // timer while no live replay exists.
                return;
            }
            m_awaitingReplay = false;
            // The Replayer is serving again: whatever refusal episode was open has ended (an operator
            // repaired the archive and restarted it). Clearing here is what makes a second, distinct
            // outage report itself, and what keeps checkRecoveryProgress's replayerUnavailable field
            // reading current state rather than "was ever refused".
            m_replayerUnavailable = false;
            const std::int64_t session = dec.replaySessionId();
            if (session == REPLAYER_NO_REPLAY_NEEDED)
            {
                if (m_walkSegmentIndex < 0)
                {
                    // We asked to resume at a position we know sits below a hole, and the Replayer says
                    // that position is already at the recording's tip — so it is not our recording any
                    // more. Declaring ourselves caught up here would close the hole by fiat.
                    diag::Logger::warn(diag::Component::ReplayerStreamReceiver, diag::EventCode::TapGap,
                                       "resume at position %lld answered 'nothing to replay' while a hole is "
                                       "open above globalSeqNo=%lld — the active recording rotated under us; "
                                       "re-walking the recording chain from segment 0",
                                       static_cast<long long>(m_reqFromPosition),
                                       static_cast<long long>(m_lastGlobalSeqNo));
                    requestReplay(0, 0);
                    return;
                }
                if (dec.recordingId() >= 0)
                {
                    // Not the walk terminator. serveReplay answers NO_REPLAY_NEEDED for two different
                    // things and tells them apart by this field: it names the recording it found
                    // nothing in when a segment is merely EMPTY, and names none at all only once the
                    // request ran past the last recording in the chain. Ending the walk on the former
                    // drops every later segment — an empty leading recording (an unclean restart that
                    // created one before anything was published to it) would truncate the whole chain,
                    // and since the re-walk a later tap gap triggers lands on that same empty segment,
                    // it would never converge. Skip it and keep walking.
                    requestReplay(m_walkSegmentIndex + 1, 0);
                    return;
                }
                m_replaySessionId = -1;  // already at the tip — follow the live tap
                m_walkSegmentIndex = -1; // chain exhausted (or never a walk) → steady/resume mode
                // The chain is exhausted, but the frontier is what the retained-ahead FIFO knows, not
                // what the chain covered: a frame retained during the walk may still sit behind a hole
                // the replay never reached, and an overflow (retainMessages) dropped tap frames
                // outright. Same guard, same reason, as the resume path in onReplaySegmentComplete —
                // declaring caught up here is what opens FixGateway's accept gate.
                drainRetained();
                if (!m_messagesBlocks.empty() || m_messagesOverflowed)
                {
                    endOverflowEpisode();
                    requestReplay(0, 0);
                    return;
                }
                notifyCaughtUp();
            }
            else
            {
                if (m_walkSegmentIndex >= 0)
                {
                    // serveReplay re-resolves the recording chain on every request, and a stale
                    // still-recording span can be dropped from it once a newer one supersedes it
                    // (ReplayRecordings.stitch) — shifting which recording this segmentIndex denotes.
                    // A retried request for the SAME index must land on the SAME recording it did the
                    // first time; anything else means the chain moved under it, so abandon the walk and
                    // restart from segment 0 rather than risk replaying the wrong span.
                    const std::int64_t recordingId = dec.recordingId();
                    if (m_walkRecordingId >= 0 && recordingId != m_walkRecordingId)
                    {
                        diag::Logger::warn(diag::Component::ReplayerStreamReceiver, diag::EventCode::TapGap,
                                           "walk segment %d now resolves to recording %lld, previously %lld — "
                                           "the recording chain shifted under us; re-walking from segment 0",
                                           static_cast<int>(m_walkSegmentIndex), static_cast<long long>(recordingId),
                                           static_cast<long long>(m_walkRecordingId));
                        requestReplay(0, 0);
                        return;
                    }
                    m_walkRecordingId = recordingId;
                }
                m_replaySessionId = session;
                // Position the bounded replay ends at; the segment is done once the replay image reaches
                // it (a bounded replay of an active recording never closes its image at the bound).
                m_catchUpPosition = dec.catchUpPosition();
                m_actions.openReplay(session);
                // Arm the stall watchdog from here: this is the moment the replay starts existing.
                m_lastReplayPosition = -1;
                m_lastReplayProgressMs = m_clock();
            }
        }
        else if (mh.templateId() == usq::ReplayPending::sbeTemplateId())
        {
            usq::ReplayPending dec;
            dec.wrapForDecode(message, bodyOff, mh.blockLength(), mh.version(), length);
            if (dec.clientId() == m_clientId && dec.requestId() == m_requestId)
            {
                // Replayer has no free slot; keep holding. m_awaitingReplay stays true so the resend
                // timer keeps us alive if the eventual Replaying is ever lost, but ReplayPending itself is
                // just "wait" — reset the request clock so we don't spam while queued.
                m_lastRequestMs = m_clock();
                m_replayerUnavailable = false; // queued, not refused — the episode ended (see Replaying)
            }
        }
        else if (mh.templateId() == usq::ReplayUnavailable::sbeTemplateId())
        {
            usq::ReplayUnavailable dec;
            dec.wrapForDecode(message, bodyOff, mh.blockLength(), mh.version(), length);
            if (dec.clientId() == m_clientId && dec.requestId() == m_requestId)
            {
                onReplayUnavailable();
            }
        }
    }

    // Where the attached replay image has reached. Completion is by position, not by the image closing:
    // a bounded replay of an ACTIVE (still-recording) recording does NOT close its image at the bound
    // (verified: image position == tip, isClosed() stays false forever).
    void onReplayPosition(const std::int64_t position)
    {
        if (position >= m_catchUpPosition)
        {
            onReplaySegmentComplete();
            return;
        }
        if (position != m_lastReplayPosition)
        {
            m_lastReplayPosition = position;
            m_lastReplayProgressMs = m_clock();
        }
    }

    // Decides what a closed replay image means, from the position it closed at.
    //
    // A stopped historical segment's bounded replay closes on its own exactly at its stopPosition —
    // which is the bound we were handed, so that IS completion. Every other close is not: the Replayer
    // stopped this replay (superseded by a resend, or its slot reclaimed by the idle TTL), the archive
    // faulted, or the Replayer shut down — and the image then closes SHORT of the bound. Treating those
    // as completion advanced the walk over a segment that was never fully replayed, silently leaving a
    // hole in history that only the next tap gap (if any) would ever expose.
    void onReplayImageClosed(const std::int64_t finalPosition)
    {
        if (finalPosition >= m_catchUpPosition)
        {
            onReplaySegmentComplete();
            return;
        }
        diag::Logger::warn(diag::Component::ReplayerStreamReceiver, diag::EventCode::TapGap,
                           "replay image closed at position %lld, short of catchUpPosition %lld — the replay was "
                           "stopped under us; re-requesting the same segment (index %d)",
                           static_cast<long long>(finalPosition), static_cast<long long>(m_catchUpPosition),
                           static_cast<int>(m_walkSegmentIndex));
        reRequestCurrent();
    }

    // Timer-driven work, once per adapter duty cycle: the request resend, an unsent ReplayComplete, the
    // replay-slot heartbeat, and the replay stall watchdog. requestPublicationPending says the request
    // publication has not connected yet — retried every cycle rather than eating a full
    // RESEND_INTERVAL_MS of pure cold-start latency for a race that is much shorter than that.
    void doTimers(const bool requestPublicationPending)
    {
        const std::int64_t nowMs = m_clock();

        // Re-request if a prior request went unanswered (Replayer still starting, request lost, or
        // Replayer restarted). Covers both "no Replaying yet" and "Replaying seen but the replay image
        // never attached".
        if (m_awaitingReplay && (requestPublicationPending || (nowMs - m_lastRequestMs) > RESEND_INTERVAL_MS))
        {
            requestReplay(m_walkSegmentIndex, m_reqFromPosition); // re-send the same request verbatim
        }

        // A ReplayComplete that never made it out holds our slot until the 60s TTL, and nothing else
        // re-sends it — the walk supersedes its own slot, but a resume has no follow-up request. Outside
        // the m_replaySessionId block below on purpose: by the time this is pending we are caught up and
        // back on the live tap, so that block no longer runs.
        if (m_completePending)
        {
            sendReplayComplete();
        }

        if (m_replaySessionId < 0)
        {
            return;
        }

        // Hold our replay slot for as long as we are actually using it (see sendHeartbeat).
        if ((nowMs - m_lastHeartbeatMs) > RESEND_INTERVAL_MS)
        {
            sendHeartbeat();
        }

        // A replay that goes silent has no other way to surface. The resend timer above only covers
        // "no Replaying yet": once one arrives m_awaitingReplay is false, and a bounded replay of an
        // active recording never closes its image, so an image that simply stops advancing — the
        // archive faulted, the Replayer stopped the session without us seeing the close, the
        // publication is wedged — leaves this client waiting on it forever with nothing retrying.
        if ((nowMs - m_lastReplayProgressMs) > REPLAY_STALL_TIMEOUT_MS)
        {
            onReplayStalled();
        }
    }

    // Recovery has run without dispatching a frame for RECOVERY_PROGRESS_TIMEOUT_MS. Evaluated every
    // duty cycle; returns whether it reported.
    bool checkRecoveryProgress()
    {
        if (m_caughtUp || !m_recoveryProgress.onNoProgress(m_clock()))
        {
            return false;
        }
        // Reported, not acted on: holding IS the correct response to a baseline this node cannot
        // establish, so the only thing missing was someone saying so. The state printed is what tells
        // the causes apart — a chain that cannot cover the hole, a Replayer that never answers, a refusal.
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

    // Highest globalSeqNo dispatched in order, 0 before the first. The frontier a consumer measures its
    // own recovery progress by — recovery that never advances it is not converging.
    std::int64_t lastGlobalSeqNo() const
    {
        return m_lastGlobalSeqNo;
    }

    // memberId of the current leader per the last LeadershipChanged processed, or -1 until one is
    // seen. A replica emits iff its own node is this leader (design §3).
    std::int32_t currentLeaderMemberId() const
    {
        return m_currentLeaderMemberId;
    }

    // A request is out and the Replayer has not answered it yet.
    bool isAwaitingReplay() const
    {
        return m_awaitingReplay;
    }

    // Mid-walk (cold start or gap re-walk) or awaiting the Replayer's answer: a non-contiguous live-tap
    // frame is expected while this holds (the tap runs ahead of the replay), so onFrame drops it
    // without treating it as a new gap.
    bool isRecovering() const
    {
        return m_replaySessionId >= 0 || m_awaitingReplay;
    }

    // The replay currently being ridden, or -1. The adapter attaches its image by this id.
    std::int64_t replaySessionId() const
    {
        return m_replaySessionId;
    }

    // The position the current replay is bounded to; the segment is done once the image reaches it.
    std::int64_t catchUpPosition() const
    {
        return m_catchUpPosition;
    }

    // Cold-start walk cursor into the recording chain; -1 once caught up (steady/resume mode).
    std::int32_t walkSegmentIndex() const
    {
        return m_walkSegmentIndex;
    }

    // The recordingId last served for walkSegmentIndex(), or -1 — see the mismatch check in onControl.
    std::int64_t walkRecordingId() const
    {
        return m_walkRecordingId;
    }

    // The id the next reply must carry to be acted on — see onControl's correlation check.
    std::int64_t requestId() const
    {
        return m_requestId;
    }

    // The fromPosition of the current request: a gap asks to RESUME at the frame last dispatched
    // rather than re-walk from position 0 — see requestResume.
    std::int64_t requestFromPosition() const
    {
        return m_reqFromPosition;
    }

    // A ReplayComplete encoded but not yet out on the wire.
    bool completePending() const
    {
        return m_completePending;
    }

  private:
    static constexpr std::int64_t RESEND_INTERVAL_MS = 500;

    // How long an established replay may deliver nothing before it is re-requested (see doTimers).
    // Deliberately far above any legitimate pause: the archive reads local disk and measures ~19 MB/s
    // into the replay, so a replay with anything left to serve is never quiet for seconds. Kept well
    // clear of RESEND_INTERVAL_MS too, since a spurious fire costs a whole segment re-replayed.
    static constexpr std::int64_t REPLAY_STALL_TIMEOUT_MS = 5'000;

    // How long recovery may run without dispatching a single frame before it is reported as unconvergent
    // (see RecoveryProgressPolicy). A different question from REPLAY_STALL_TIMEOUT_MS above, which asks
    // whether one replay IMAGE is advancing: the re-walk loop this catches keeps starting and finishing
    // healthy replays and dispatches nothing out of any of them. Generous, because it alarms rather than
    // fences — a late report costs nothing, a false one costs an operator's attention.
    static constexpr std::int64_t RECOVERY_PROGRESS_TIMEOUT_MS = 30'000;

    // ReplayRequest.segmentIndex meaning "resume the active recording at fromPosition" rather than
    // "replay the segmentIndex-th recording of the chain" — see ReplayerService.serveReplay.
    static constexpr std::int32_t RESUME_SEGMENT_INDEX = -1;

    // Caps on frames retained ahead of a hole (see retainMessages) — enough to cover a walk over a normal
    // recording, not a whole trading day; past either bound recovery falls back to re-walking.
    static constexpr std::size_t MAX_MESSAGES_FRAMES = 65536;
    static constexpr std::size_t MAX_MESSAGES_BYTES = 16UL * 1024 * 1024;

    using HdrSbe = org::limitless::phixeron::sbe::sequenced::MessageHeader;
    using HeaderComposite = org::limitless::phixeron::sbe::sequenced::Header;

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

    // Pools MessagesBlocks instead of allocating (and copying, on growth) one buffer per retained frame.
    // Released blocks are kept for reuse rather than freed, so a later recovery episode reuses
    // already-resident pages instead of paying a fresh allocation/page-fault cost.
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
            if (m_freeList.empty())
            {
                return new MessagesBlock();
            }
            MessagesBlock* const block = m_freeList.back();
            m_freeList.pop_back();
            block->used = 0;
            return block;
        }

        void release(MessagesBlock* const block)
        {
            m_freeList.push_back(block);
        }

      private:
        std::vector<MessagesBlock*> m_freeList;
    };

    // Sends ReplayRequest(clientId, requestId, segmentIndex, fromPosition) and marks us awaiting the
    // reply. Idempotent on the Replayer side (it supersedes any in-flight replay for this clientId), so
    // the resend timer re-sending the same (segmentIndex, fromPosition) is safe.
    //
    // requestId advances on EVERY send, resends included — that is the point. A resend makes the
    // Replayer stop the in-flight session and start a new one, leaving the stale reply queued ahead of
    // the live one on the shared control stream; only a per-send id lets onControl tell them apart.
    void requestReplay(const std::int32_t segmentIndex, const std::int64_t fromPosition)
    {
        if (segmentIndex >= 0)
        {
            m_resumeAnchorGseq = 0; // a walk supersedes any resume in flight
            if (segmentIndex != m_walkSegmentIndex)
            {
                // A different segment than the one in flight: nothing to compare its recordingId
                // against yet (see the Replaying handler in onControl).
                m_walkRecordingId = -1;
            }
        }
        m_walkSegmentIndex = segmentIndex;
        m_reqFromPosition = fromPosition;
        m_awaitingReplay = true;
        m_replaySessionId = -1;
        // Drop any unsent release: ReplayComplete names only the clientId, so one landing late — after
        // this request took a fresh slot — would free the slot this replay is riding. A new request
        // supersedes the old slot on the Replayer side anyway, so there is nothing left to release.
        m_completePending = false;
        m_actions.closeReplay();
        m_lastRequestMs = m_clock();
        ++m_requestId;
        // Best-effort: a request that does not land is already covered — the resend timer re-sends it
        // verbatim after RESEND_INTERVAL_MS. Retrying it any sooner is actively harmful: every send does
        // ++m_requestId, and onControl only acts on a reply carrying the CURRENT id, so a per-poll retry
        // runs the counter away and every reply that arrives is discarded as stale — the client then
        // never attaches a replay and never catches up.
        m_actions.sendReplayRequest(m_requestId, segmentIndex, fromPosition);
    }

    // Steady-state gap recovery: ask for the active recording resumed at the frame we last dispatched,
    // rather than re-walking the whole chain from segment 0. Repairing a one-frame drop then costs a
    // one-frame replay instead of a replay of the entire trading day — during which nothing is
    // dispatched at all, a replay slot (of two) is held, and isCaughtUp() stays false.
    //
    // A bare position only means anything against the recording it was observed in, and the active
    // recording can rotate under us (this node's sequencer restarted and began a fresh one). Rather
    // than trying to prove that has not happened, the resumed replay is checked where it lands: its
    // first frame must be the very frame the position was anchored on (see onFrame), and if it is
    // not we fall back to the walk, which needs no position to be sound.
    void requestResume()
    {
        requestReplay(RESUME_SEGMENT_INDEX, m_lastFramePosition);
        m_resumeAnchorGseq = m_lastGlobalSeqNo;
    }

    // Re-ask for whatever is in flight. A resume must go back through requestResume() rather than
    // re-send the stale m_reqFromPosition verbatim, which would carry no anchor for onFrame to validate
    // against (the original anchor was already consumed by this episode's first replayed frame).
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

    // Releases our replay slot: we have reached the tip the Replayer bounded us to and are back on the
    // live tap. Only the resume path needs this — every step of a cold-start walk supersedes its own
    // slot with the next segment's request, and the walk's last request frees it via NO_REPLAY_NEEDED,
    // whereas a resume has no follow-up request at all. Without it the slot sits until the 60s idle TTL
    // reclaims it: one of MAX_CONCURRENT_REPLAYS slots, held by nobody. Unlike a request this has no
    // timer behind it and no follow-up that would supersede it, so a single dropped offer was the whole
    // release — hence m_completePending, retried from doTimers until it lands.
    void sendReplayComplete()
    {
        m_completePending = !m_actions.sendReplayComplete();
    }

    // Refreshes this client's replay slot while it rides an attached image, so the Replayer's idle TTL
    // measures "client stopped using the slot" rather than "the replay took a while" — a replay of a
    // full trading day legitimately outlives any fixed TTL. Best-effort: a lost heartbeat only risks the
    // slot being reclaimed, which the truncated-close path recovers from by re-requesting.
    void sendHeartbeat()
    {
        // Only a landed heartbeat refreshes the slot, so the clock measures when the Replayer last
        // actually heard from us. Advancing it on a dropped offer burnt a whole interval per loss and
        // walked a healthy client toward the TTL in silence.
        if (m_actions.sendReplayHeartbeat())
        {
            m_lastHeartbeatMs = m_clock();
        }
    }

    // The node's Replayer failed its startup integrity check: its archive does not reach globalSeqNo 1,
    // so it has no valid history for anyone and says so instead of serving a mid-stream replay we would
    // abort on. Deliberately NOT fatal here — that is the containment: an operator repairs the archive
    // and restarts the Replayer, and the resend timer picks up where it left off with no app restart. We
    // simply never go caught up, so every consumer gate stays shut and nothing is dispatched from a
    // baseline we cannot establish.
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
        // Hold exactly as for ReplayPending: still awaiting, request clock reset so the resend paces at
        // the normal interval rather than spinning on a permanent condition.
        m_lastRequestMs = m_clock();
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
            // A resume, not a walk step: there is no next segment. We hold every frame the recording
            // had when the request was served, so we are back at the tip — anything published since is
            // on the tap, retained ahead of the hole we just closed. Drain it before trusting that: a
            // retained-ahead frame the replay didn't cover may itself sit behind a hole (drainRetained
            // stops there), and an overflow (retainMessages) silently dropped tap frames outright, past
            // the bound this replay was even asked to cover — either leaves us short of the real
            // frontier. Only declare caught up once nothing is left waiting and no overflow is latched;
            // otherwise re-walk now rather than wait for some future tap frame to rediscover the hole,
            // which is unbounded under the same sustained load that caused the overflow.
            drainRetained();
            if (!m_messagesBlocks.empty() || m_messagesOverflowed)
            {
                endOverflowEpisode();
                requestReplay(0, 0);
                return;
            }
            sendReplayComplete();
            notifyCaughtUp();
            return;
        }
        requestReplay(m_walkSegmentIndex + 1, 0); // advance the walk to the next segment
    }

    // Everything past the contiguity check: advance the baseline, then decode and hand the frame to the
    // caller. Split out so a frame drained from the retained-messages buffer (which lives in its own
    // storage, not the subscription's) runs the identical path.
    void dispatchFrame(char* const frame, const std::uint64_t length, const std::int64_t gseq,
                       const std::int64_t framePosition, const std::int64_t receiveNs, const bool fromReplay)
    {
        // The one funnel every in-order frame passes through, replayed or live — so recovery advancing
        // its globalSeqNo is exactly this being reached (see RecoveryProgressPolicy). onProgress returns
        // the falling edge only, so the gauge is written once per episode rather than once per frame.
        if (m_recoveryProgress.onProgress())
        {
            m_actions.recoveryStalled(false);
        }
        m_hdr.wrap(frame, 0U, 0U, length);
        const std::uint16_t templateId = m_hdr.templateId();
        m_header.wrap(frame, HdrSbe::encodedLength(), 0U, length);

        m_lastGlobalSeqNo = gseq;
        m_replayGapLogged = false;
        // Where this frame starts in the recording — the tap, a replay image and the recording itself
        // all count positions in the same space. requestResume anchors on it.
        m_lastFramePosition = framePosition;
        if (!fromReplay && !m_caughtUp && !m_messagesOverflowed)
        {
            // First in-order frame straight off the live tap ⇒ we are following the live tip. Unless
            // retainMessages dropped frames: this one may well be a retained frame draining over a
            // closed hole with the dropped ones still missing above it, and contiguity here says
            // nothing about them. The re-walk requested at the end of this replay is what settles it.
            notifyCaughtUp();
        }

        const auto srcId = m_header.sourceId();
        const auto connId = m_header.connectionId();
        const auto sessId = m_header.sessionId();
        const auto ts = m_header.timestamp();
        const auto origin = m_header.origin();

        if (templateId == CLIENT_CONNECTED_TEMPLATE_ID)
        {
            if (m_onConnected)
            {
                m_onConnected(LifecycleEvent{ .globalSeqNo = gseq,
                                              .sourceId = srcId,
                                              .connectionId = connId,
                                              .sourceSessionId = sessId,
                                              .clusterTimestamp = ts,
                                              .receiveTimeNs = receiveNs });
            }
            return;
        }
        if (templateId == CLIENT_DISCONNECTED_TEMPLATE_ID)
        {
            if (m_onDisconnected)
            {
                m_onDisconnected(LifecycleEvent{ .globalSeqNo = gseq,
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
                m_onLeadershipChanged(m_currentLeaderMemberId, gseq);
            }
            return;
        }
        if (m_onSequenced)
        {
            m_onSequenced(SequencedEvent{ .globalSeqNo = gseq,
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

    // Keeps a live tap frame that sits beyond the current hole. This is what actually closes the
    // replay->live seam: the tap must be drained every duty cycle (it is untethered, so an unpolled
    // subscription falls behind), which means a frame not kept here is GONE — and every frame published
    // while a walk replays history is such a frame. Dropping them (as this did until 2026-08-05) left
    // every walk ending one guaranteed hole short of live, re-walking the whole chain, and re-opening the
    // same hole; convergence then depended on nothing being published during the final round trip.
    //
    // Bounded, and deliberately lossy past the bound: a re-walk over a full trading day cannot buffer a
    // day of traffic, so on overflow this falls back to the old drop-and-re-walk behaviour rather than
    // growing without limit. Retained as a FIFO of pooled blocks, not sorted by globalSeqNo: a single
    // Aeron image delivers strictly increasing globalSeqNo with no reordering, so arrival order already
    // is globalSeqNo order — only an exact-duplicate redelivery (a repeat of the most recently retained
    // globalSeqNo) needs an explicit check, not general sorting.
    void retainMessages(const std::int64_t gseq, const char* const frame, const std::uint64_t len,
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
                                   static_cast<long long>(gseq), m_messagesFrameCount, m_messagesBytes);
            }
            return;
        }
        if (m_messagesFrameCount > 0 && gseq <= m_messagesTailGseq)
        {
            return; // already retained (the tap redelivered it) — keep the first copy
        }
        if (m_messagesBlocks.empty() || m_messagesBlocks.back()->used + recordSize > MessagesBlock::SIZE)
        {
            m_messagesBlocks.push_back(m_messagesBlockPool.acquire());
        }
        MessagesBlock* const tail = m_messagesBlocks.back();
        const MessagesRecordHeader header{ gseq, framePosition, receiveNs, static_cast<std::uint32_t>(len) };
        std::memcpy(tail->bytes.data() + tail->used, &header, sizeof(header));
        std::memcpy(tail->bytes.data() + tail->used + sizeof(header), frame, len);
        tail->used += recordSize;
        ++m_messagesFrameCount;
        m_messagesBytes += len;
        m_messagesTailGseq = gseq;
    }

    // Hands over every retained frame that has become contiguous, discarding any the replay has since
    // covered. Called after each dispatch, so the seam closes the instant the replay reaches it. A block
    // is returned to the pool the moment it is fully consumed, so the next recovery episode reuses
    // already-resident memory instead of paying a fresh allocation.
    void drainRetained()
    {
        for (;;)
        {
            if (m_messagesBlocks.empty())
            {
                return;
            }
            MessagesBlock* const front = m_messagesBlocks.front();
            if (m_messagesReadOffset >= front->used)
            {
                m_messagesBlockPool.release(front);
                m_messagesBlocks.pop_front();
                m_messagesReadOffset = 0;
                continue;
            }
            MessagesRecordHeader header;
            std::memcpy(&header, front->bytes.data() + m_messagesReadOffset, sizeof(header));
            if (header.globalSeqNo > m_lastGlobalSeqNo + 1)
            {
                return; // still a hole below the oldest retained frame
            }
            std::uint8_t* const payload = front->bytes.data() + m_messagesReadOffset + sizeof(header);
            m_messagesReadOffset += sizeof(header) + header.length;
            --m_messagesFrameCount;
            m_messagesBytes -= header.length;
            if (header.globalSeqNo <= m_lastGlobalSeqNo)
            {
                continue; // the replay already covered it
            }
            dispatchFrame(reinterpret_cast<char*>(payload), header.length, header.globalSeqNo, header.position,
                          header.receiveNs, /*fromReplay=*/false);
        }
    }

    // The re-walk about to be requested is what covers the frames retainMessages dropped, so the
    // overflow ends HERE, where it is acted on — not in drainRetained, where clearing it (as this did
    // until 2026-08-14) forgot the drop at exactly the wrong moment: the frame whose dispatch cleared it
    // then declared us caught up at the seam, over a frontier the drops had already invalidated. That
    // opened FixGateway's accept gate over a hole, left for the next tap gap to rediscover — the outcome
    // the check above re-walks to avoid. A later overflow is a new episode and reports itself.
    void endOverflowEpisode()
    {
        m_messagesOverflowed = false;
        m_messagesOverflowLogged = false;
    }

    void notifyCaughtUp()
    {
        if (m_caughtUp)
        {
            return; // idempotent — reached from the replay-tip, no-replay, and first-live-frame paths
        }
        // Fires again after a gap cleared m_caughtUp, so consumers can re-arm on re-convergence
        // (FixGateway restarts its tap-stall watchdog here) rather than only on first catch-up.
        m_caughtUp = true;
        if (m_onCaughtUp)
        {
            m_onCaughtUp();
        }
    }

    const std::int32_t m_clientId;
    ReplayerRecoveryActions& m_actions;
    Clock m_clock;
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
    // A ReplayComplete that did not land. A healthy publication still returns BACK_PRESSURED/ADMIN_ACTION
    // transiently, and a discarded result made "attempted" indistinguishable from "sent". Only the
    // release needs this — see requestReplay for why the request must NOT be retried the same way.
    bool m_completePending = false;
    std::int64_t m_lastReplayPosition = -1;  // last replay-image position seen; -1 = not attached yet
    std::int64_t m_lastReplayProgressMs = 0; // when it last changed — the stall watchdog's clock
    // The Replayer is refusing to serve us (integrity check failed). State, not just a log latch: the
    // refusal is resent on every 500ms request, so report it once per episode, and checkRecoveryProgress
    // prints it as the fact that tells a refusal apart from a Replayer that never answered.
    bool m_replayerUnavailable = false;

    std::int64_t m_lastGlobalSeqNo = 0;   // highest globalSeqNo delivered; 0 = none yet
    std::int64_t m_lastFramePosition = 0; // where that frame starts in the recording; requestResume's anchor
    std::int64_t m_resumeAnchorGseq = 0;  // globalSeqNo a resume replay must open at, or 0 if not resuming
    bool m_replayGapLogged = false;       // report a hole in replayed history once per episode, not per frame
    bool m_caughtUp = false;              // following live; revoked on a tap gap, re-established at the seam

    // Recovery that runs without ever dispatching anything — contained, but otherwise silent.
    RecoveryProgressPolicy m_recoveryProgress{ RECOVERY_PROGRESS_TIMEOUT_MS };

    // Live tap frames from beyond the current hole, retained in arrival order (see retainMessages).
    MessagesBlockPool m_messagesBlockPool;
    std::deque<MessagesBlock*> m_messagesBlocks;
    std::size_t m_messagesReadOffset = 0; // offset of the next unconsumed record within m_messagesBlocks.front()
    std::int64_t m_messagesTailGseq = 0;  // globalSeqNo of the most recently retained frame; dedups redelivery
    std::size_t m_messagesFrameCount = 0;
    std::size_t m_messagesBytes = 0;
    // Frames were dropped ahead of the hole because the FIFO above was full: the frontier this client
    // holds is short of the real one, so it must re-walk rather than declare itself caught up. Cleared
    // only where that re-walk is requested (endOverflowEpisode).
    bool m_messagesOverflowed = false;
    bool m_messagesOverflowLogged = false; // that overflow reported once per episode, not per frame

    std::int32_t m_currentLeaderMemberId = -1;

    HdrSbe m_hdr;
    HeaderComposite m_header;
};

} // namespace org::limitless::phixeron::replayer::client
