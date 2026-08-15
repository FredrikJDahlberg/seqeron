#pragma once

// ReplayerStreamReceiver — app-replica side of the per-node Replayer (doc/router-design.md).
//
// Where ClusterStreamClient opens its own archive connection and replays the recorded sequenced stream
// directly, a ReplayerStreamReceiver reads the co-located SequencerService IPC tap LIVE and only touches the
// archive indirectly — by asking the node-local Replayer to replay when it detects a gap. That is the
// whole point of the Replayer — one process per node reads the archive; every replica reads the cheap
// local tap for live and asks the Replayer for history/gaps — so the sequencer keeps zero live network
// subscribers and audit.md S4 dissolves (design §0/§5).
//
// This deliberately mirrors ClusterStreamClient's proven "follow a replay image up to a catch-up
// position, then hand off to the live feed, de-duping the seam by globalSeqNo" handoff, with two
// substitutions:
//   • the "live feed" is the SequencerService IPC tap (FEEDER_STREAM_ID), read directly and
//     untethered — the same recorded aeron:ipc stream the Replayer replays from — not an archive
//     replay image;
//   • the replay is served by the Replayer (ReplayRequest -> Replaying) instead of the app calling
//     AeronArchive::startReplay itself.
// Catch-up is detected by position (Replaying.catchUpPosition), not by the replay image closing: the
// Replayer's replay is bounded to an ACTIVE recording, and a bounded replay of an active recording
// never closes its image at the bound (see poll()). ClusterStreamClient keeps its own role —
// fix_test_server still follows the archive with it, and FixConnection uses it for the resend scan —
// and this reuses its SequencedEvent/LifecycleEvent structs, its sequenced-schema decode, its
// CLIENT_*_TEMPLATE_ID constants and its frameStartPosition.
//
// Gap recovery is anchored on globalSeqNo (load-bearing) and merely accelerated by position. On a tap
// gap the client asks the Replayer to RESUME the active recording at the position of the frame it last
// dispatched (requestResume), so repairing a dropped frame costs a replay of the hole rather than of
// the whole trading day — the tap and every replay image count positions in the recording's own space,
// which is what makes that position meaningful at all.
//
// A position is not self-validating, though: it denotes a frame only within the recording it was
// observed in, and a member restart can leave the app holding a position from a recording that is no
// longer the active one. So the resumed replay is checked where it lands — its first frame must be the
// frame the position was anchored on — and on any mismatch (or a Replayer answering "nothing to replay"
// over a hole we know is open) the client falls back to re-walking the chain from segment 0
// (requestReplay(0, 0)), de-duping every already-seen frame by globalSeqNo (gseq <= last) until it
// re-reaches the tip. The walk needs no position to be sound, so it stays the backstop; the resume is
// only the fast path over it, and correctness never rests on it.
//
// The replay->live seam is closed by the tap itself, not by a round trip. Tap frames are drained AND
// dispatched while a walk is in flight, and — load-bearing — ones landing beyond the current hole are
// RETAINED in globalSeqNo order (retainMessages/drainRetained) rather than dropped, because the tap must be
// polled every duty cycle (it is untethered) and an unretained frame is therefore gone for good. When
// the replay reaches the hole, the retained frames hand straight over and the client is live with no
// residual. Dropping them (as this did until 2026-08-05) left every walk ending one guaranteed hole
// short of live and re-walking the whole chain, re-opening the same hole: convergence depended on
// nothing being published during the final round trip — on the publish rate, not on any progress
// invariant. Measured on gap-recovery-test.sh, a single injected frame drop under a 300-message flood
// cost 6 full re-walks; retention closes it in 1. The buffer is bounded (MAX_MESSAGES_*) and falls back to
// that drop-and-re-walk behaviour past the bound, since a re-walk over a full trading day cannot buffer
// a day of traffic.
//
// Two consequences of dispatching the tap mid-walk. A non-contiguous tap frame is then EXPECTED, not a
// new gap: re-walk is triggered only when !isRecovering(), so an in-flight walk runs to completion
// instead of being superseded by the very frames it is racing. And such a frame may not establish the
// globalSeqNo baseline before the walk has (see onFragment) — otherwise a cold start would adopt
// whatever the live tap happened to be carrying, which is exactly the mid-stream baseline the
// first-frame-must-be-1 abort exists to prevent.
//
// Two things the walk state machine must not conflate, both of which used to resolve the unsafe way:
//   • A stale reply vs. the current one. Replies are matched on requestId (which advances on every
//     send, resends included), not on clientId — a resend makes the Replayer supersede its in-flight
//     replay, so its reply is still queued ahead of the live one and would attach us to a stopped
//     session or stop the resend timer with no replay running.
//   • A closed replay image vs. a completed segment. Only a close AT the bound is completion; a close
//     short of it means the replay was stopped under us (supersede, idle-TTL reclaim, archive fault),
//     and advancing the walk there skips a segment that was never fully replayed. The same segment is
//     re-requested instead. Paired with the ReplayHeartbeat below, which keeps the Replayer's slot TTL
//     from being the thing that stops it: a full-log replay has no upper time bound.
//
// isCaughtUp() is a state, not a latch. It is cleared the moment a live-tap gap is detected and
// re-established (re-firing OnCaughtUp) when the stream goes contiguous again, because consumers gate
// real decisions on it — leader-only emission, and FixGateway's tap-stall watchdog, which measures
// silence in DISPATCHED frames. Leaving it latched meant a re-walk (during which nothing dispatches
// until the replay passes the hole) read as a stalled sequencer, so the gateway would close its cluster
// session and exit ~20s into its own recovery, forcing a standby promotion it did not need.

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "Aeron.h"
#include "FragmentAssembler.h"
#include "concurrent/AtomicBuffer.h"

// Reuses SequencedEvent / LifecycleEvent, the sequenced MessageHeader/Header codecs, the
// CLIENT_CONNECTED/DISCONNECTED template-id constants, and FEEDER_STREAM_ID (the recorded sequenced
// stream id, which the live tap and the Replayer's replays both address) — header-only; brings in
// archive headers it does not otherwise need, which is harmless — the un-rewired binaries include the
// same header.
#include "org/limitless/phixeron/replayer/RecoveryProgressPolicy.hpp"
#include "org/limitless/phixeron/sequencer/ClusterStreamReceiver.hpp"
#include "org/limitless/phixeron/util/Logger.hpp"
#include "org/limitless/phixeron/util/PhixeronCounters.hpp"

// Replay-protocol control codecs (sbe-unsequenced.xml) + LeadershipChanged (sbe-sequenced.xml)
#include "org_limitless_phixeron_sbe_sequenced/LeadershipChanged.h"
#include "org_limitless_phixeron_sbe_unsequenced/MessageHeader.h"
#include "org_limitless_phixeron_sbe_unsequenced/ReplayComplete.h"
#include "org_limitless_phixeron_sbe_unsequenced/ReplayHeartbeat.h"
#include "org_limitless_phixeron_sbe_unsequenced/ReplayPending.h"
#include "org_limitless_phixeron_sbe_unsequenced/ReplayRequest.h"
#include "org_limitless_phixeron_sbe_unsequenced/ReplayUnavailable.h"
#include "org_limitless_phixeron_sbe_unsequenced/Replaying.h"

namespace org::limitless::phixeron::sequencer {

namespace usq = org::limitless::phixeron::sbe::unsequenced;
namespace diag = org::limitless::phixeron::util;

// ── Node-local IPC channels/streams — MUST match org.limitless.phixeron.replayer.ReplayerService ─────────
inline constexpr const char* REPLAYER_IPC_CHANNEL = "aeron:ipc";
inline constexpr const char* FEEDER_CHANNEL = "aeron:ipc?tether=false";
// Untethered like the tap, and for the same reason: the Replayer answers every app from one duty-cycle
// thread, so an app that stops polling must not be able to back-pressure the stream the others are
// answered on. A dropped reply costs one RESEND_INTERVAL_MS, which the resend timer already covers.
// The replay stream stays tethered — a dropped replay fragment is a hole in history, not a lost reply.
// That is only safe because nothing subscribes to it except the one app riding a replay, and only while
// it is riding one: see openReplaySubscription for what a standing subscription here cost.
inline constexpr const char* REPLAYER_CONTROL_CHANNEL = "aeron:ipc?tether=false";
inline constexpr std::int32_t REPLAYER_REPLAY_STREAM_ID = 201;
inline constexpr std::int32_t REPLAYER_REQUEST_STREAM_ID = 202;
inline constexpr std::int32_t REPLAYER_CONTROL_STREAM_ID = 203;

// Replaying.replaySessionId sentinel: "nothing to replay, you are at the tip — follow the live tap".
inline constexpr std::int64_t REPLAYER_NO_REPLAY_NEEDED = -1;

// LeadershipChanged (sbe-sequenced.xml template 5), synthesized onto the sequenced stream; like
// CLIENT_CONNECTED/DISCONNECTED it is non-FIX and outside the FIX-MsgType-derived template range.
inline constexpr std::uint16_t LEADERSHIP_CHANGED_TEMPLATE_ID = 5;

/**
 * Follows the co-located SequencerService IPC tap directly, decoding and dispatching sbe-sequenced
 * messages to the caller exactly like ClusterStreamClient — same SequencedEvent/LifecycleEvent callbacks
 * — plus an OnLeadershipChanged callback and currentLeaderMemberId()/isCaughtUp() accessors that the
 * caller uses to gate leader-only emission (design §3).
 *
 * Startup: cold replicas walk the node's per-tenure recording chain by segment index (0,1,2,…) via the
 * Replayer, riding each segment's replay image and de-duping by globalSeqNo, until the Replayer answers
 * NO_REPLAY_NEEDED — so history spans every leader failover, not just the current recording. The tap is
 * dispatched throughout, so it closes the seam itself the moment the replay reaches it. A steady-state
 * tap gap clears isCaughtUp() and re-walks the same chain from segment 0, de-duping by globalSeqNo —
 * robust to a leader failover having rotated the active recording. No archive connection is opened here.
 */
class ReplayerStreamReceiver
{
  public:
    using OnSequenced = std::function<void(const SequencedEvent&)>;
    using OnConnected = std::function<void(const LifecycleEvent&)>;
    using OnDisconnected = std::function<void(const LifecycleEvent&)>;
    using OnLeadershipChanged = std::function<void(std::int32_t newLeaderMemberId, std::int64_t globalSeqNo)>;
    using OnCaughtUp = std::function<void()>;

    ReplayerStreamReceiver(std::int32_t clientId, OnSequenced onSequenced, OnConnected onConnected = {},
                           OnDisconnected onDisconnected = {}, OnLeadershipChanged onLeadershipChanged = {},
                           OnCaughtUp onCaughtUp = {}) :
      m_clientId(clientId),
      m_onSequenced(std::move(onSequenced)),
      m_onConnected(std::move(onConnected)),
      m_onDisconnected(std::move(onDisconnected)),
      m_onLeadershipChanged(std::move(onLeadershipChanged)),
      m_onCaughtUp(std::move(onCaughtUp)),
      m_tapHandler([this](auto& b, auto o, auto l, auto& h) { onFragment(b, o, l, h, /*fromReplay=*/false); }),
      m_replayHandler([this](auto& b, auto o, auto l, auto& h) { onFragment(b, o, l, h, /*fromReplay=*/true); }),
      m_controlHandler([this](auto& b, auto o, auto l, auto& h) { onControl(b, o, l, h); }),
      m_tapAssembler(std::make_unique<aeron::FragmentAssembler>(m_tapHandler)),
      m_replayAssembler(std::make_unique<aeron::FragmentAssembler>(m_replayHandler)),
      m_controlAssembler(std::make_unique<aeron::FragmentAssembler>(m_controlHandler)),
      m_tapPoll(m_tapAssembler->handler()),
      m_replayPoll(m_replayAssembler->handler()),
      m_controlPoll(m_controlAssembler->handler())
    {}

    // Returns any blocks still held by the retained-ahead FIFO (see retainMessages/drainRetained) — the
    // pool's own destructor only frees its free list, not blocks still checked out to m_messagesBlocks.
    ~ReplayerStreamReceiver()
    {
        for (auto* block : m_messagesBlocks)
        {
            delete block;
        }
    }

    // Subscribes the tap/replay/control streams, opens the request publication and the convergence
    // counter, and requests the cold-start replay from position 0. memberId is this app's node — needed
    // only to label that counter, since a node's metrics are merged with every other node's.
    void start(std::shared_ptr<aeron::Aeron> aeron, const std::int32_t memberId)
    {
        m_aeron = std::move(aeron);
        m_recoveryStalledCounterRegId = util::addAppCounter(
            m_aeron, util::APP_RECOVERY_STALLED_TYPE_ID,
            "phixeron.app.recoveryStalled member=" + std::to_string(memberId) + " client=" + std::to_string(m_clientId),
            memberId, m_clientId);
        m_tapSubRegId = m_aeron->addSubscription(FEEDER_CHANNEL, FEEDER_STREAM_ID);
        // No standing replay subscription — see openReplaySubscription: one is opened per replay
        // episode, filtered to that replay's own session id, and closed when the episode ends.
        m_controlSubRegId = m_aeron->addSubscription(REPLAYER_CONTROL_CHANNEL, REPLAYER_CONTROL_STREAM_ID);
        m_requestPubRegId = m_aeron->addPublication(REPLAYER_IPC_CHANNEL, REPLAYER_REQUEST_STREAM_ID);
        requestReplay(0, 0); // cold start: walk the recording chain from segment 0
    }

    // Test-only (see OrderExecClient's PHIXERON_FAULT_INJECTION hook): enable dropping live tap frames on
    // demand, to synthesize a consumer-side globalSeqNo gap so a test can drive the re-walk gap recovery
    // deterministically (src/test/scripts/gap-recovery-test.sh). A no-op in production (never enabled).
    void enableFaultInjection()
    {
        m_faultInjection = true;
    }

    // Arm a drop of the next n live tap frames. Called on the poll thread (deferred from a signal
    // handler); a no-op unless fault injection was enabled.
    void injectTapDrop(const int n)
    {
        if (m_faultInjection)
        {
            m_faultDropPending.fetch_add(n, std::memory_order_relaxed);
        }
    }

    // Test-only: feeds a fragment through the same decode/baseline/gap logic poll() drives off the
    // live tap subscription (fromReplay=false), without a real Aeron subscription or media driver —
    // see ReplayerStreamReceiverTest.cpp.
    void testDeliverTapFragment(const aeron::concurrent::AtomicBuffer& buffer, aeron::util::index_t offset,
                                aeron::util::index_t length, const aeron::Header& header)
    {
        onFragment(buffer, offset, length, header, /*fromReplay=*/false);
    }

    // Test-only: same as testDeliverTapFragment, but through the replay-image decode path
    // (fromReplay=true) poll() drives while riding an attached replay image — see ReplayerStreamReceiverTest.cpp.
    void testDeliverReplayFragment(const aeron::concurrent::AtomicBuffer& buffer, aeron::util::index_t offset,
                                   aeron::util::index_t length, const aeron::Header& header)
    {
        onFragment(buffer, offset, length, header, /*fromReplay=*/true);
    }

    // Test-only: feeds a fragment through the same control-stream decode path onControl() drives off
    // the Replayer's control subscription (Replaying / ReplayPending) — see ReplayerStreamReceiverTest.cpp.
    void testDeliverControl(const aeron::concurrent::AtomicBuffer& buffer, aeron::util::index_t offset,
                            aeron::util::index_t length, const aeron::Header& header)
    {
        onControl(buffer, offset, length, header);
    }

    // Test-only: simulates a replay image reaching its bounded catch-up position (or a stopped
    // segment's image closing) without a real Aeron replay image — see ReplayerStreamReceiverTest.cpp.
    void testCompleteReplaySegment()
    {
        onReplaySegmentComplete();
    }

    // Test-only: the decision poll() makes when it finds the replay image closed, given the position it
    // closed at — poll() reads that off a live Aeron image the unit suite has no way to fake.
    void testReplayImageClosed(const std::int64_t finalPosition)
    {
        onReplayImageClosed(finalPosition);
    }

    // Test-only: what poll()'s stall watchdog does once an established replay has gone
    // REPLAY_STALL_TIMEOUT_MS without advancing — the timer itself is wall-clock, which the unit suite
    // has no way to advance.
    void testReplayStalled()
    {
        onReplayStalled();
    }

    // Test-only: the convergence check poll() runs every cycle, with the clock supplied — poll() reads it
    // off the wall clock, which the unit suite has no way to advance. Returns whether it reported.
    bool testCheckRecoveryProgress(const std::int64_t nowMs)
    {
        return checkRecoveryProgress(nowMs);
    }

    // Test-only accessors into the walk/gap-recovery state machine — see ReplayerStreamReceiverTest.cpp.
    bool testIsAwaitingReplay() const
    {
        return m_awaitingReplay;
    }

    std::int64_t testReplaySessionId() const
    {
        return m_replaySessionId;
    }

    std::int32_t testWalkSegmentIndex() const
    {
        return m_walkSegmentIndex;
    }

    // Test-only: the recordingId last seen for the current walk segment, or -1 — see the recordingId
    // mismatch check in onControl's Replaying handler.
    std::int64_t testWalkRecordingId() const
    {
        return m_walkRecordingId;
    }

    // Test-only: when the current (or most recent) replay request was sent, so a test can observe the
    // resend timer (poll()'s RESEND_INTERVAL_MS check) actually re-sending rather than just re-checking
    // state that a resend wouldn't otherwise change.
    std::int64_t testLastRequestMs() const
    {
        return m_lastRequestMs;
    }

    // Test-only: a ReplayComplete encoded but not yet out on the wire. Distinguishes "slot released"
    // from "release attempted", which a discarded offer result used to conflate (review-3.md #9).
    bool testCompletePending() const
    {
        return m_completePending;
    }

    // Test-only: the id the next reply must carry to be acted on — lets a test build a stale reply
    // (see onControl's request-correlation check) without guessing the counter's internal start value.
    std::int64_t testRequestId() const
    {
        return m_requestId;
    }

    // Test-only: the fromPosition of the current request, so a test can see a gap ask to RESUME at the
    // frame it last dispatched rather than re-walk from position 0 — see requestResume.
    std::int64_t testReqFromPosition() const
    {
        return m_reqFromPosition;
    }

    // Test-only: the exact predicate onFragment uses to decide whether a non-contiguous live-tap frame
    // is a new gap (steady state) or merely the tap running ahead of an in-flight walk — see
    // ReplayerStreamReceiverTest.cpp's re-walk-trigger regression lock.
    bool testIsRecovering() const
    {
        return isRecovering();
    }

    // One duty-cycle iteration; returns fragments consumed. Poll ordering: always drain control (to
    // learn Replaying/ReplayPending), ride an attached replay image, and always drain AND dispatch the
    // tap — the contiguity check in onFragment, not the poll routing, decides what a tap frame is worth
    // mid-walk, which is what lets the tap itself close the replay->live seam (see file header).
    int poll()
    {
        resolveResources();

        int work = 0;
        if (m_controlSub)
        {
            work += m_controlSub->poll(m_controlPoll, FRAGMENT_LIMIT);
        }

        // Re-request if a prior request went unanswered (Replayer still starting, request lost, or
        // Replayer restarted — design §6). Covers both "no Replaying yet" and "Replaying seen but the
        // replay image never attached". The request publication connecting is a separate, much
        // shorter race (addPublication is async) — retry every poll while it is still pending rather
        // than eating a full RESEND_INTERVAL_MS of pure cold-start latency for it.
        const bool requestPubPending = m_requestPubRegId >= 0 && (!m_requestPub || !m_requestPub->isConnected());
        if (m_awaitingReplay && (requestPubPending || (nowMs() - m_lastRequestMs) > RESEND_INTERVAL_MS))
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

        if (m_replaySessionId >= 0)
        {
            // Hold our replay slot for as long as we are actually using it (see sendHeartbeat).
            if ((nowMs() - m_lastHeartbeatMs) > RESEND_INTERVAL_MS)
            {
                sendHeartbeat();
            }
            if (!m_replayImage && m_replaySub)
            {
                m_replayImage = m_replaySub->imageBySessionId(static_cast<std::int32_t>(m_replaySessionId));
            }
            if (m_replayImage)
            {
                if (!m_replayImage->isClosed())
                {
                    work += m_replayImage->poll(m_replayPoll, FRAGMENT_LIMIT);
                    const std::int64_t position = m_replayImage->position();
                    if (position >= m_catchUpPosition)
                    {
                        // Reached this segment's bounded tip. A bounded replay of an ACTIVE
                        // (still-recording) recording does NOT close its image at the bound (verified:
                        // image position == tip, isClosed() stays false forever), so completion is
                        // detected by position — exactly as ClusterStreamClient does for its live segment.
                        onReplaySegmentComplete();
                    }
                    else if (position != m_lastReplayPosition)
                    {
                        m_lastReplayPosition = position;
                        m_lastReplayProgressMs = nowMs();
                    }
                }
                else
                {
                    // A closed image still reports its final position, so this is exact (see
                    // onReplayImageClosed).
                    onReplayImageClosed(m_replayImage->position());
                }
            }
            // else: Replaying received, image not yet attached — nothing to poll this cycle, fall
            // through to the tap drain below rather than holding the whole duty cycle on it.

            // A replay that goes silent has no other way to surface. The resend timer above only covers
            // "no Replaying yet": once one arrives m_awaitingReplay is false, and a bounded replay of an
            // active recording never closes its image, so an image that simply stops advancing — the
            // archive faulted, the Replayer stopped the session without us seeing the close, the
            // publication is wedged — leaves this client waiting on it forever with nothing retrying.
            // Re-request the same segment verbatim, exactly as the truncated-close path does.
            if (m_replaySessionId >= 0 && (nowMs() - m_lastReplayProgressMs) > REPLAY_STALL_TIMEOUT_MS)
            {
                onReplayStalled();
            }
        }

        // Always drain the tap, even mid-walk (cold start or gap re-walk) or while merely awaiting the
        // Replayer's answer: it is untethered (FEEDER_CHANNEL's ?tether=false), so an Aeron subscription
        // that goes unpolled falls behind the publisher's log buffer. Always dispatch through the same
        // handler too — frames ahead of an in-flight replay drop on the contiguity check anyway, and the
        // one at the seam must not be thrown away (see file header).
        if (m_tapSub)
        {
            work += m_tapSub->poll(m_tapPoll, FRAGMENT_LIMIT);
        }

        checkRecoveryProgress(nowMs());
        return work;
    }

    bool isCaughtUp() const
    {
        return m_caughtUp;
    }

    // Highest globalSeqNo dispatched in order, 0 before the first. The frontier a consumer measures its
    // own recovery progress by — recovery that never advances it is not converging (see
    // RecoveryProgressPolicy, which applies the same predicate internally).
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

  private:
    static constexpr int FRAGMENT_LIMIT = 16;
    static constexpr std::int64_t RESEND_INTERVAL_MS = 500;

    // How long an established replay may deliver nothing before it is re-requested (see poll()'s stall
    // watchdog). Deliberately far above any legitimate pause: the archive reads local disk and measures
    // ~19 MB/s into the replay, so a replay with anything left to serve is never quiet for seconds. Kept
    // well clear of RESEND_INTERVAL_MS too, since a spurious fire costs a whole segment re-replayed.
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

    // Mid-walk (cold start or gap re-walk) or awaiting the Replayer's answer: a non-contiguous live-tap
    // frame is expected while this holds (the tap runs ahead of the replay), so onFragment drops it
    // without treating it as a new gap.
    bool isRecovering() const
    {
        return m_replaySessionId >= 0 || m_awaitingReplay;
    }

    void resolveResources()
    {
        if (!m_tapSub && m_tapSubRegId >= 0)
        {
            m_tapSub = m_aeron->findSubscription(m_tapSubRegId);
        }
        if (!m_replaySub && m_replaySubRegId >= 0)
        {
            m_replaySub = m_aeron->findSubscription(m_replaySubRegId);
        }
        if (!m_controlSub && m_controlSubRegId >= 0)
        {
            m_controlSub = m_aeron->findSubscription(m_controlSubRegId);
        }
        if (!m_requestPub && m_requestPubRegId >= 0)
        {
            m_requestPub = m_aeron->findPublication(m_requestPubRegId);
        }
        if (!m_recoveryStalledCounter && m_recoveryStalledCounterRegId >= 0)
        {
            m_recoveryStalledCounter = m_aeron->findCounter(m_recoveryStalledCounterRegId);
        }
    }

    // Sends ReplayRequest(clientId, requestId, segmentIndex, fromPosition) and marks us awaiting the
    // reply. Idempotent on the Replayer side (it supersedes any in-flight replay for this clientId), so
    // the resend timer re-sending the same (segmentIndex, fromPosition) is safe.
    //
    // requestId advances on EVERY send, resends included — that is the point. A resend makes the
    // Replayer stop the in-flight session and start a new one, leaving the stale reply queued ahead of
    // the live one on the shared control stream; only a per-send id lets onControl tell them apart.
    void requestReplay(std::int32_t segmentIndex, std::int64_t fromPosition)
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
        closeReplaySubscription();
        m_lastRequestMs = nowMs();
        ++m_requestId;
        if (!m_requestPub || !m_requestPub->isConnected())
        {
            return; // Replayer not up yet; the resend timer retries
        }

        alignas(16) std::array<std::uint8_t, 64> buf{};
        usq::ReplayRequest enc;
        enc.wrapAndApplyHeader(reinterpret_cast<char*>(buf.data()), 0, buf.size());
        enc.clientId(m_clientId).requestId(m_requestId).fromPosition(fromPosition).segmentIndex(segmentIndex);
        const auto len = static_cast<aeron::util::index_t>(usq::MessageHeader::encodedLength() + enc.encodedLength());
        aeron::concurrent::AtomicBuffer ab(buf.data(), buf.size());
        // Result deliberately discarded: unlike the two sends below, a request that does not land is
        // already covered — the resend timer re-sends it verbatim after RESEND_INTERVAL_MS. Retrying it
        // any sooner is actively harmful: every send does ++m_requestId, and onControl only acts on a
        // reply carrying the CURRENT id, so a per-poll retry runs the counter away and every reply that
        // arrives is discarded as stale — the client then never attaches a replay and never catches up.
        m_requestPub->offer(ab, 0, len);
    }

    // Steady-state gap recovery: ask for the active recording resumed at the frame we last dispatched,
    // rather than re-walking the whole chain from segment 0. Repairing a one-frame drop then costs a
    // one-frame replay instead of a replay of the entire trading day — during which nothing is
    // dispatched at all, a replay slot (of two) is held, and isCaughtUp() stays false.
    //
    // A bare position only means anything against the recording it was observed in, and the active
    // recording can rotate under us (this node's sequencer restarted and began a fresh one). Rather
    // than trying to prove that has not happened, the resumed replay is checked where it lands: its
    // first frame must be the very frame the position was anchored on (see onFragment), and if it is
    // not we fall back to the walk, which needs no position to be sound.
    void requestResume()
    {
        requestReplay(RESUME_SEGMENT_INDEX, m_lastFramePosition);
        m_resumeAnchorGseq = m_lastGlobalSeqNo;
    }

    // Subscribes to exactly one replay — this one — for as long as we ride it, and to nothing on the
    // replay stream the rest of the time.
    //
    // Load-bearing, not tidiness. The Replayer answers every app on one shared aeron:ipc stream, and an
    // Aeron publication is flow-controlled by its slowest TETHERED subscriber. A standing subscription
    // on that stream (which is what this was until 2026-08-07) made every idle app a subscriber of every
    // other app's replay — one that never polls, because poll() only ever reads the image of its OWN
    // session, so its position stays at 0 forever. The archive's replay then wedges one publication
    // window past the slowest of them — measured: pub-lmt pinned at exactly 33 554 432 (32 MiB, half a
    // 64 MB term) with two peer sub-pos at 0 — and never moves again. Any cold start with more than
    // ~32 MiB of history therefore hung permanently, which is what a restarted replica does after a
    // few hundred thousand messages. Filtered to the session id, a replay publication has exactly one
    // subscriber, and no app can hold back another's replay.
    void openReplaySubscription(const std::int64_t replaySessionId)
    {
        closeReplaySubscription();
        if (!m_aeron)
        {
            return; // unit suite drives onControl with no Aeron, exactly as requestReplay tolerates
        }
        // The archive's replaySessionId carries the Aeron image session id in its low 32 bits — the
        // same narrowing poll() used to hand imageBySessionId.
        const std::string channel = std::string(REPLAYER_IPC_CHANNEL) +
                                    "?session-id=" + std::to_string(static_cast<std::int32_t>(replaySessionId));
        m_replaySubRegId = m_aeron->addSubscription(channel, REPLAYER_REPLAY_STREAM_ID);
    }

    void closeReplaySubscription()
    {
        m_replayImage.reset();
        if (!m_replaySub && m_replaySubRegId >= 0 && m_aeron)
        {
            // Resolve before dropping: an add the driver has already answered but resolveResources has
            // not picked up would otherwise stay open with nothing holding it.
            m_replaySub = m_aeron->findSubscription(m_replaySubRegId);
        }
        m_replaySub.reset(); // last reference — Subscription's destructor closes it
        m_replaySubRegId = -1;
    }

    // Releases our replay slot: we have reached the tip the Replayer bounded us to and are back on the
    // live tap. Only the resume path needs this — every step of a cold-start walk supersedes its own
    // slot with the next segment's request, and the walk's last request frees it via NO_REPLAY_NEEDED,
    // whereas a resume has no follow-up request at all. Without it the slot sits until the 60s idle TTL
    // reclaims it: one of MAX_CONCURRENT_REPLAYS slots, held by nobody. Unlike a request this has no
    // timer behind it and no follow-up that would supersede it, so a single dropped offer was the whole
    // release — hence m_completePending, retried from poll() until it lands.
    void sendReplayComplete()
    {
        m_completePending = true;
        if (!m_requestPub || !m_requestPub->isConnected())
        {
            return;
        }
        alignas(16) std::array<std::uint8_t, 64> buf{};
        usq::ReplayComplete enc;
        enc.wrapAndApplyHeader(reinterpret_cast<char*>(buf.data()), 0, buf.size());
        enc.clientId(m_clientId);
        const auto len = static_cast<aeron::util::index_t>(usq::MessageHeader::encodedLength() + enc.encodedLength());
        aeron::concurrent::AtomicBuffer ab(buf.data(), buf.size());
        m_completePending = m_requestPub->offer(ab, 0, len) < 0;
    }

    // Refreshes this client's replay slot while it rides an attached image, so the Replayer's idle TTL
    // measures "client stopped using the slot" rather than "the replay took a while" — a replay of a
    // full trading day legitimately outlives any fixed TTL. Best-effort: a lost heartbeat only risks the
    // slot being reclaimed, which the truncated-close path in poll() recovers from by re-requesting.
    void sendHeartbeat()
    {
        if (!m_requestPub || !m_requestPub->isConnected())
        {
            return;
        }
        alignas(16) std::array<std::uint8_t, 64> buf{};
        usq::ReplayHeartbeat enc;
        enc.wrapAndApplyHeader(reinterpret_cast<char*>(buf.data()), 0, buf.size());
        enc.clientId(m_clientId);
        const auto len = static_cast<aeron::util::index_t>(usq::MessageHeader::encodedLength() + enc.encodedLength());
        aeron::concurrent::AtomicBuffer ab(buf.data(), buf.size());
        // Only a landed heartbeat refreshes the slot, so the clock measures when the Replayer last
        // actually heard from us. Advancing it on a dropped offer burnt a whole interval per loss and
        // walked a healthy client toward the TTL in silence.
        if (m_requestPub->offer(ab, 0, len) >= 0)
        {
            m_lastHeartbeatMs = nowMs();
        }
    }

    void onControl(const aeron::concurrent::AtomicBuffer& buffer, aeron::util::index_t offset,
                   aeron::util::index_t length, const aeron::Header&)
    {
        char* const raw = reinterpret_cast<char*>(buffer.buffer());
        const auto cap = static_cast<std::uint64_t>(buffer.capacity());
        const auto off = static_cast<std::uint64_t>(offset);
        if (static_cast<std::uint64_t>(length) < usq::MessageHeader::encodedLength())
        {
            return;
        }

        usq::MessageHeader mh;
        mh.wrap(raw, off, 0U, cap);
        const std::uint64_t bodyOff = off + usq::MessageHeader::encodedLength();

        if (mh.templateId() == usq::Replaying::sbeTemplateId())
        {
            usq::Replaying dec;
            dec.wrapForDecode(raw, bodyOff, mh.blockLength(), mh.version(), cap);
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
                // Position the bounded replay ends at; poll() declares caught up once the replay
                // image reaches it (a bounded replay of an active recording never closes its image
                // at the bound, so completion is by position, not image close — see file header).
                m_catchUpPosition = dec.catchUpPosition();
                openReplaySubscription(session);
                // Arm the stall watchdog from here: this is the moment the replay starts existing.
                m_lastReplayPosition = -1;
                m_lastReplayProgressMs = nowMs();
            }
        }
        else if (mh.templateId() == usq::ReplayPending::sbeTemplateId())
        {
            usq::ReplayPending dec;
            dec.wrapForDecode(raw, bodyOff, mh.blockLength(), mh.version(), cap);
            if (dec.clientId() == m_clientId && dec.requestId() == m_requestId)
            {
                // Replayer has no free slot; keep holding. m_awaitingReplay stays true so the resend
                // timer keeps us alive if the eventual Replaying is ever lost, but ReplayPending itself is
                // just "wait" — reset the request clock so we don't spam while queued.
                m_lastRequestMs = nowMs();
                m_replayerUnavailable = false; // queued, not refused — the episode ended (see Replaying)
            }
        }
        else if (mh.templateId() == usq::ReplayUnavailable::sbeTemplateId())
        {
            usq::ReplayUnavailable dec;
            dec.wrapForDecode(raw, bodyOff, mh.blockLength(), mh.version(), cap);
            if (dec.clientId() == m_clientId && dec.requestId() == m_requestId)
            {
                onReplayUnavailable();
            }
        }
    }

    // The node's Replayer failed its startup integrity check: its archive does not reach globalSeqNo 1,
    // so it has no valid history for anyone and says so instead of serving a mid-stream replay we would
    // abort on. Deliberately NOT fatal here — that is the containment: an operator repairs the archive
    // and restarts the Replayer, and the resend timer below picks up where it left off with no app
    // restart. We simply never go caught up, so every consumer gate stays shut and nothing is dispatched
    // from a baseline we cannot establish.
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
        m_lastRequestMs = nowMs();
    }

    using HdrSbe = org::limitless::phixeron::sbe::sequenced::MessageHeader;
    using HeaderComposite = org::limitless::phixeron::sbe::sequenced::Header;

    void onFragment(const aeron::concurrent::AtomicBuffer& buffer, aeron::util::index_t offset,
                    aeron::util::index_t length, const aeron::Header& header, bool fromReplay)
    {
        // Test-only fault injection (see enableFaultInjection): drop this live tap frame to synthesize a
        // consumer-side globalSeqNo gap, so the re-walk gap recovery below can be driven deterministically.
        // Dropped before the de-dupe/gap check updates m_lastGlobalSeqNo, so the NEXT frame reads as a gap.
        if (m_faultInjection && !fromReplay && m_faultDropPending.load(std::memory_order_relaxed) > 0)
        {
            m_faultDropPending.fetch_sub(1, std::memory_order_relaxed);
            return;
        }

        const std::int64_t receiveNs = nowNs();
        const std::int64_t framePosition = frameStartPosition(header);

        char* const raw = reinterpret_cast<char*>(buffer.buffer());
        const auto cap = static_cast<std::uint64_t>(buffer.capacity());
        const auto off = static_cast<std::uint64_t>(offset);
        const auto len = static_cast<std::uint64_t>(length);
        if (len < HdrSbe::encodedLength() + HeaderComposite::encodedLength())
        {
            return;
        }

        m_hdr.wrap(raw, off, 0U, cap);
        if (m_hdr.schemaId() != HdrSbe::sbeSchemaId())
        {
            return;
        }
        const std::uint64_t bodyOff = off + HdrSbe::encodedLength();

        m_header.wrap(raw, bodyOff, 0U, cap);
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
                    retainMessages(gseq, raw + off, len, framePosition, receiveNs);
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
                retainMessages(gseq, raw + off, len, framePosition, receiveNs);
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
        dispatchFrame(raw, off, len, cap, gseq, framePosition, receiveNs, fromReplay);
        drainRetained();
    }

    // Everything past the contiguity check: advance the baseline, then decode and hand the frame to the
    // caller. Split out so a frame drained from the retained-messages buffer (which lives in its own
    // storage, not the subscription's) runs the identical path.
    void dispatchFrame(char* const raw, const std::uint64_t off, const std::uint64_t len, const std::uint64_t cap,
                       const std::int64_t gseq, const std::int64_t framePosition, const std::int64_t receiveNs,
                       const bool fromReplay)
    {
        // The one funnel every in-order frame passes through, replayed or live — so recovery advancing
        // its globalSeqNo is exactly this being reached (see RecoveryProgressPolicy). onProgress returns
        // the falling edge only, so the gauge is written once per episode rather than once per frame.
        if (m_recoveryProgress.onProgress() && m_recoveryStalledCounter)
        {
            m_recoveryStalledCounter->set(0);
        }
        m_hdr.wrap(raw, off, 0U, cap);
        const std::uint16_t templateId = m_hdr.templateId();
        m_header.wrap(raw, off + HdrSbe::encodedLength(), 0U, cap);

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
            sbe::sequenced::LeadershipChanged leadershpChanged;
            leadershpChanged.wrapForDecode(raw, off + HdrSbe::encodedLength(), m_hdr.blockLength(), m_hdr.version(),
                                           cap);
            m_currentLeaderMemberId = leadershpChanged.newLeaderMemberId();
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
                                          .payload = raw + off,
                                          .payloadLength = len,
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
            dispatchFrame(reinterpret_cast<char*>(payload), 0, header.length, header.length, header.globalSeqNo,
                          header.position, header.receiveNs, /*fromReplay=*/false);
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
        if (m_walkSegmentIndex < 0)
        {
            // A resume, not a walk step: re-anchor via requestResume() rather than re-sending the stale
            // m_reqFromPosition verbatim, which would carry no anchor for onFragment to validate against
            // (the original anchor was already consumed by this episode's first replayed frame).
            requestResume();
        }
        else
        {
            requestReplay(m_walkSegmentIndex, m_reqFromPosition); // same request verbatim, new requestId
        }
    }

    // Recovery has run without dispatching a frame for RECOVERY_PROGRESS_TIMEOUT_MS. Evaluated every duty
    // cycle; nowMs is a parameter only so the unit suite can supply a clock poll() takes off the wall.
    bool checkRecoveryProgress(const std::int64_t nowMs)
    {
        if (m_caughtUp || !m_recoveryProgress.onNoProgress(nowMs))
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
        if (m_recoveryStalledCounter)
        {
            m_recoveryStalledCounter->set(1);
        }
        return true;
    }

    // An attached (or expected) replay stopped delivering — see the watchdog check in poll().
    void onReplayStalled()
    {
        diag::Logger::warn(diag::Component::ReplayerStreamReceiver, diag::EventCode::TapGap,
                           "replay session %lld made no progress for %lldms at position %lld of "
                           "catchUpPosition %lld (image %s) — re-requesting segment %d",
                           static_cast<long long>(m_replaySessionId), static_cast<long long>(REPLAY_STALL_TIMEOUT_MS),
                           static_cast<long long>(m_lastReplayPosition), static_cast<long long>(m_catchUpPosition),
                           m_replayImage ? "attached" : "never attached", static_cast<int>(m_walkSegmentIndex));
        if (m_walkSegmentIndex < 0)
        {
            // A resume, not a walk step: re-anchor via requestResume() — see onReplayImageClosed.
            requestResume();
        }
        else
        {
            requestReplay(m_walkSegmentIndex, m_reqFromPosition); // same request verbatim, new requestId
        }
    }

    // A replay segment finished (reached its bounded tip, or its image closed for a stopped segment).
    void onReplaySegmentComplete()
    {
        closeReplaySubscription();
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

    static std::int64_t nowMs()
    {
        using namespace std::chrono;
        return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
    }

    // Same wall-clock ns stamp ClusterStreamClient records (its own nowNs() is private).
    static std::int64_t nowNs()
    {
        using namespace std::chrono;
        return duration_cast<nanoseconds>(system_clock::now().time_since_epoch()).count();
    }

    const std::int32_t m_clientId;
    OnSequenced m_onSequenced;
    OnConnected m_onConnected;
    OnDisconnected m_onDisconnected;
    OnLeadershipChanged m_onLeadershipChanged;
    OnCaughtUp m_onCaughtUp;

    std::shared_ptr<aeron::Aeron> m_aeron;
    std::int64_t m_tapSubRegId = -1;
    std::int64_t m_replaySubRegId = -1;
    std::int64_t m_controlSubRegId = -1;
    std::int64_t m_requestPubRegId = -1;
    std::shared_ptr<aeron::Subscription> m_tapSub;
    std::shared_ptr<aeron::Subscription> m_replaySub;
    std::shared_ptr<aeron::Subscription> m_controlSub;
    std::shared_ptr<aeron::Publication> m_requestPub;
    std::shared_ptr<aeron::Image> m_replayImage;

    bool m_awaitingReplay = false;
    std::int64_t m_replaySessionId = -1;
    std::int64_t m_catchUpPosition = 0;  // bounded replay's end position; segment done once the image reaches it
    std::int32_t m_walkSegmentIndex = 0; // cold-start walk position; -1 once caught up (steady/resume mode)
    std::int64_t m_walkRecordingId = -1; // recordingId last served for m_walkSegmentIndex, or -1 if not yet known
    std::int64_t m_reqFromPosition = 0;  // fromPosition of the current request, for idempotent resend
    std::int64_t m_lastRequestMs = 0;
    std::int64_t m_requestId = 0; // advances per send; replies not carrying it are stale (see onControl)
    std::int64_t m_lastHeartbeatMs = 0;
    // A ReplayComplete that did not land (review-3.md #9). isConnected() only rules out NOT_CONNECTED; a
    // healthy publication still returns BACK_PRESSURED/ADMIN_ACTION transiently, and a discarded result
    // made "attempted" indistinguishable from "sent". Only the release needs this — see requestReplay
    // for why the request must NOT be retried the same way.
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

    // Recovery that runs without ever dispatching anything — contained, but silent until this (review-3.md #6).
    // The gauge is the same fact the fault line carries, in the form an alert can be written against;
    // null until the async add resolves (resolveResources), which no reporting path may depend on.
    RecoveryProgressPolicy m_recoveryProgress{ RECOVERY_PROGRESS_TIMEOUT_MS };
    std::int64_t m_recoveryStalledCounterRegId = -1;
    std::shared_ptr<aeron::Counter> m_recoveryStalledCounter;

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

    // Test-only fault injection (gated by enableFaultInjection): drop the next N live tap frames to
    // synthesize a consumer-side globalSeqNo gap (src/test/scripts/gap-recovery-test.sh). Armed on the
    // poll thread (deferred from OrderExecClient's SIGUSR1 handler) and consumed on the poll thread; the
    // atomic mirrors the Java side and stays safe if a caller ever arms it from another thread.
    bool m_faultInjection = false;
    std::atomic<int> m_faultDropPending{ 0 };

    aeron::fragment_handler_t m_tapHandler;
    aeron::fragment_handler_t m_replayHandler;
    aeron::fragment_handler_t m_controlHandler;

    std::unique_ptr<aeron::FragmentAssembler> m_tapAssembler;
    std::unique_ptr<aeron::FragmentAssembler> m_replayAssembler;
    std::unique_ptr<aeron::FragmentAssembler> m_controlAssembler;

    // Composed once: FragmentAssembler::handler() builds a fresh std::function per call, and these
    // are polled every duty-cycle iteration.
    aeron::fragment_handler_t m_tapPoll;
    aeron::fragment_handler_t m_replayPoll;
    aeron::fragment_handler_t m_controlPoll;

    HdrSbe m_hdr;
    HeaderComposite m_header;
};

} // namespace org::limitless::phixeron::sequencer
