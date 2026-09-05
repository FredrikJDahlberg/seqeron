#pragma once

// ReplayerStreamReceiver — app-replica side of the per-node Replayer.
//
// ReplayerStreamReceiver reads the co-located SequencerService IPC tap LIVE and asks the Replayer to replay
// when it detects a gap.
//
// This is the Aeron adapter only: subscriptions, the request publication, the replay image and the
// clocks. Every decision it makes about them lives in ReplayerRecovery, which holds none of them.
//
#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <string>

#include "Aeron.h"
#include "FragmentAssembler.h"
#include "concurrent/AtomicBuffer.h"

#include "org/limitless/phixeron/replayer/client/ReplayerRecovery.hpp"
#include "org/limitless/phixeron/util/PhixeronCounters.hpp"

// Request codecs (sbe-replay.xml); the replies are decoded in ReplayerRecovery.
#include "org_limitless_phixeron_sbe_replay/MessageHeader.h"
#include "org_limitless_phixeron_sbe_replay/ReplayComplete.h"
#include "org_limitless_phixeron_sbe_replay/ReplayHeartbeat.h"
#include "org_limitless_phixeron_sbe_replay/ReplayRequest.h"

namespace org::limitless::phixeron::replayer::client {

// FEEDER_STREAM_ID is the recorded sequenced stream id, which the live tap and the Replayer's replays
// both address; frameStartPosition derives a frame's recording position from its Aeron header.
using org::limitless::phixeron::sequencer::FEEDER_STREAM_ID;
using org::limitless::phixeron::sequencer::frameStartPosition;

// ── Node-local IPC channels/streams — MUST match org.limitless.phixeron.replayer.server.ReplayerService ─────────
inline constexpr const char* REPLAYER_IPC_CHANNEL = "aeron:ipc";
inline constexpr const char* FEEDER_CHANNEL = "aeron:ipc?tether=false";

// Untethered like the tap, because the Replayer answers every app from one duty-cycle thread.
inline constexpr const char* REPLAYER_CONTROL_CHANNEL = "aeron:ipc?tether=false";
inline constexpr std::int32_t REPLAYER_REPLAY_STREAM_ID = 201;
inline constexpr std::int32_t REPLAYER_REQUEST_STREAM_ID = 202;
inline constexpr std::int32_t REPLAYER_CONTROL_STREAM_ID = 203;

/**
 * Follows the co-located SequencerService IPC tap directly, decoding and dispatching sequenced
 * messages to the caller exactly like ClusterStreamClient — same SequencedEvent/LifecycleEvent callbacks
 * — plus an OnLeadershipChanged callback and currentLeaderMemberId()/isCaughtUp() accessors that the
 * caller uses to gate leader-only emission (design §3).
 *
 * Startup: cold replicas walk the node's per-tenure recording chain by segment index (0,1,2,…) via the
 * Replayer, riding each segment's replay image and de-duping by globalSeqNo, until the Replayer answers
 * NO_REPLAY_NEEDED — so history spans every leader failover, not just the current recording. The tap is
 * dispatched throughout, so it closes the seam itself the moment the replay reaches it. A steady-state
 * tap gap clears isCaughtUp() and resumes the active recording just below the hole, falling back to the
 * same chain walk if that resume does not land where it was anchored. No archive connection is opened
 * here.
 */
class ReplayerStreamReceiver final : private ReplayerRecoveryActions
{
  public:
    using OnSequenced = ReplayerRecovery::OnSequenced;
    using OnConnected = ReplayerRecovery::OnConnected;
    using OnDisconnected = ReplayerRecovery::OnDisconnected;
    using OnLeadershipChanged = ReplayerRecovery::OnLeadershipChanged;
    using OnCaughtUp = ReplayerRecovery::OnCaughtUp;

    ReplayerStreamReceiver(std::int32_t clientId, OnSequenced onSequenced, OnConnected onConnected = {},
                           OnDisconnected onDisconnected = {}, OnLeadershipChanged onLeadershipChanged = {},
                           OnCaughtUp onCaughtUp = {}) :
      m_clientId(clientId),
      m_recovery(clientId, *this, std::move(onSequenced), std::move(onConnected), std::move(onDisconnected),
                 std::move(onLeadershipChanged), std::move(onCaughtUp)),
      // Constructed from temporaries: FragmentAssembler copies the delegate into its own member, so
      // keeping our own copy alive would just be a second std::function per stream, never called again.
      m_tapAssembler(
          std::make_unique<aeron::FragmentAssembler>([this](auto& buffer, auto offset, auto length, auto& header) {
              onTapFragment(buffer, offset, length, header);
          })),
      m_replayAssembler(
          std::make_unique<aeron::FragmentAssembler>([this](auto& buffer, auto offset, auto length, auto& header) {
              onReplayFragment(buffer, offset, length, header);
          })),
      m_controlAssembler(std::make_unique<aeron::FragmentAssembler>(
          [this](auto& buffer, auto offset, auto length, auto&) { onControlFragment(buffer, offset, length); })),
      m_tapPoll(m_tapAssembler->handler()),
      m_replayPoll(m_replayAssembler->handler()),
      m_controlPoll(m_controlAssembler->handler())
    {}

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
        // No standing replay subscription — see openReplay: one is opened per replay episode, filtered to
        // that replay's own session id, and closed when the episode ends.
        m_controlSubRegId = m_aeron->addSubscription(REPLAYER_CONTROL_CHANNEL, REPLAYER_CONTROL_STREAM_ID);
        m_requestPubRegId = m_aeron->addPublication(REPLAYER_IPC_CHANNEL, REPLAYER_REQUEST_STREAM_ID);
        m_recovery.start();
    }

    // Test-only (see OrderExecServer's PHIXERON_FAULT_INJECTION hook): enable dropping live tap frames on
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

    // One duty-cycle iteration; returns fragments consumed. Poll ordering: always drain control (to
    // learn Replaying/ReplayPending), ride an attached replay image, and always drain AND dispatch the
    // tap — the contiguity check in ReplayerRecovery, not the poll routing, decides what a tap frame is
    // worth mid-walk, which is what lets the tap itself close the replay->live seam.
    int poll()
    {
        resolveResources();

        int work = 0;
        if (m_controlSub)
        {
            work += m_controlSub->poll(m_controlPoll, FRAGMENT_LIMIT);
        }

        // The request publication connecting is a short race (addPublication is async), so the resend
        // retries every poll while it is still pending rather than eating a full resend interval of pure
        // cold-start latency for it.
        const bool requestPubPending = m_requestPubRegId >= 0 && (!m_requestPub || !m_requestPub->isConnected());
        m_recovery.doTimers(requestPubPending);

        if (m_recovery.replaySessionId() >= 0)
        {
            if (!m_replayImage && m_replaySub)
            {
                m_replayImage = m_replaySub->imageBySessionId(static_cast<std::int32_t>(m_recovery.replaySessionId()));
            }
            if (m_replayImage)
            {
                // Held locally across the poll: a replayed frame can abandon this replay from inside the
                // handler (an anchor mismatch re-walks), which drops m_replayImage.
                const std::shared_ptr<aeron::Image> image = m_replayImage;
                if (!image->isClosed())
                {
                    work += image->poll(m_replayPoll, FRAGMENT_LIMIT);
                    if (m_replayImage == image)
                    {
                        m_recovery.onReplayPosition(image->position());
                    }
                }
                else
                {
                    // A closed image still reports its final position, so this is exact.
                    m_recovery.onReplayImageClosed(image->position());
                }
            }
            // else: Replaying received, image not yet attached — nothing to poll this cycle, fall
            // through to the tap drain below rather than holding the whole duty cycle on it.
        }

        // Always drain the tap, even mid-walk (cold start or gap re-walk) or while merely awaiting the
        // Replayer's answer: it is untethered (FEEDER_CHANNEL's ?tether=false), so an Aeron subscription
        // that goes unpolled falls behind the publisher's log buffer. Always dispatch through the same
        // handler too — frames ahead of an in-flight replay are retained or dropped on the contiguity
        // check anyway, and the one at the seam must not be thrown away.
        if (m_tapSub)
        {
            work += m_tapSub->poll(m_tapPoll, FRAGMENT_LIMIT);
        }

        m_recovery.checkRecoveryProgress();
        return work;
    }

    bool isCaughtUp() const
    {
        return m_recovery.isCaughtUp();
    }

    // Highest globalSeqNo dispatched in order, 0 before the first. The frontier a consumer measures its
    // own recovery progress by — recovery that never advances it is not converging (see
    // RecoveryProgressPolicy, which applies the same predicate internally).
    std::int64_t lastGlobalSeqNo() const
    {
        return m_recovery.lastGlobalSeqNo();
    }

    // memberId of the current leader per the last LeadershipChanged processed, or -1 until one is
    // seen. A replica emits iff its own node is this leader (design §3).
    std::int32_t currentLeaderMemberId() const
    {
        return m_recovery.currentLeaderMemberId();
    }

  private:
    static constexpr int FRAGMENT_LIMIT = 16;
    static constexpr std::size_t REQUEST_BUFFER_LENGTH = 64;

    void sendReplayRequest(const std::int64_t requestId, const std::int32_t segmentIndex,
                           const std::int64_t fromPosition) override
    {
        if (!m_requestPub || !m_requestPub->isConnected())
        {
            return; // Replayer not up yet; the resend timer retries
        }
        alignas(16) std::array<std::uint8_t, REQUEST_BUFFER_LENGTH> buf{};
        rpl::ReplayRequest enc;
        enc.wrapAndApplyHeader(reinterpret_cast<char*>(buf.data()), 0, buf.size());
        enc.clientId(m_clientId).requestId(requestId).fromPosition(fromPosition).segmentIndex(segmentIndex);
        const auto len = static_cast<aeron::util::index_t>(rpl::MessageHeader::encodedLength() + enc.encodedLength());
        aeron::concurrent::AtomicBuffer ab(buf.data(), buf.size());
        m_requestPub->offer(ab, 0, len); // result deliberately discarded — see ReplayerRecovery::requestReplay
    }

    bool sendReplayComplete() override
    {
        if (!m_requestPub || !m_requestPub->isConnected())
        {
            return false;
        }
        alignas(16) std::array<std::uint8_t, REQUEST_BUFFER_LENGTH> buf{};
        rpl::ReplayComplete enc;
        enc.wrapAndApplyHeader(reinterpret_cast<char*>(buf.data()), 0, buf.size());
        enc.clientId(m_clientId);
        const auto len = static_cast<aeron::util::index_t>(rpl::MessageHeader::encodedLength() + enc.encodedLength());
        aeron::concurrent::AtomicBuffer ab(buf.data(), buf.size());
        return m_requestPub->offer(ab, 0, len) >= 0;
    }

    bool sendReplayHeartbeat() override
    {
        if (!m_requestPub || !m_requestPub->isConnected())
        {
            return false;
        }
        alignas(16) std::array<std::uint8_t, REQUEST_BUFFER_LENGTH> buf{};
        rpl::ReplayHeartbeat enc;
        enc.wrapAndApplyHeader(reinterpret_cast<char*>(buf.data()), 0, buf.size());
        enc.clientId(m_clientId);
        const auto len = static_cast<aeron::util::index_t>(rpl::MessageHeader::encodedLength() + enc.encodedLength());
        aeron::concurrent::AtomicBuffer ab(buf.data(), buf.size());
        return m_requestPub->offer(ab, 0, len) >= 0;
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
    void openReplay(const std::int64_t replaySessionId) override
    {
        closeReplay();
        if (!m_aeron)
        {
            return;
        }
        // The archive's replaySessionId carries the Aeron image session id in its low 32 bits — the
        // same narrowing poll() uses to hand imageBySessionId.
        const std::string channel = std::string(REPLAYER_IPC_CHANNEL) +
                                    "?session-id=" + std::to_string(static_cast<std::int32_t>(replaySessionId));
        m_replaySubRegId = m_aeron->addSubscription(channel, REPLAYER_REPLAY_STREAM_ID);
    }

    void closeReplay() override
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

    // The gauge is the same fact ReplayerRecovery's fault line carries, in the form an alert can be
    // written against; null until the async add resolves (resolveResources), which no reporting path
    // may depend on.
    void recoveryStalled(const bool stalled) override
    {
        if (m_recoveryStalledCounter)
        {
            m_recoveryStalledCounter->set(stalled ? 1 : 0);
        }
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

    void onTapFragment(const aeron::concurrent::AtomicBuffer& buffer, const aeron::util::index_t offset,
                       const aeron::util::index_t length, const aeron::Header& header)
    {
        // Test-only fault injection (see enableFaultInjection): drop this live tap frame to synthesize a
        // consumer-side globalSeqNo gap, so the re-walk gap recovery can be driven deterministically.
        // Dropped before the receiver sees it, so the NEXT frame reads as a gap.
        if (m_faultInjection && m_faultDropPending.load(std::memory_order_relaxed) > 0)
        {
            m_faultDropPending.fetch_sub(1, std::memory_order_relaxed);
            return;
        }
        m_recovery.onFrame(frameAt(buffer, offset), static_cast<std::uint64_t>(length), frameStartPosition(header),
                           sequencer::nowNs(), /*fromReplay=*/false);
    }

    void onReplayFragment(const aeron::concurrent::AtomicBuffer& buffer, const aeron::util::index_t offset,
                          const aeron::util::index_t length, const aeron::Header& header)
    {
        m_recovery.onFrame(frameAt(buffer, offset), static_cast<std::uint64_t>(length), frameStartPosition(header),
                           sequencer::nowNs(), /*fromReplay=*/true);
    }

    void onControlFragment(const aeron::concurrent::AtomicBuffer& buffer, const aeron::util::index_t offset,
                           const aeron::util::index_t length)
    {
        m_recovery.onControl(frameAt(buffer, offset), static_cast<std::uint64_t>(length));
    }

    static char* frameAt(const aeron::concurrent::AtomicBuffer& buffer, const aeron::util::index_t offset)
    {
        return reinterpret_cast<char*>(buffer.buffer()) + offset;
    }

    std::int64_t nowMs() override
    {
        using namespace std::chrono;
        return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
    }

    const std::int32_t m_clientId;
    ReplayerRecovery m_recovery;

    std::shared_ptr<aeron::Aeron> m_aeron;
    std::int64_t m_tapSubRegId = -1;
    std::int64_t m_replaySubRegId = -1;
    std::int64_t m_controlSubRegId = -1;
    std::int64_t m_requestPubRegId = -1;
    std::int64_t m_recoveryStalledCounterRegId = -1;
    std::shared_ptr<aeron::Subscription> m_tapSub;
    std::shared_ptr<aeron::Subscription> m_replaySub;
    std::shared_ptr<aeron::Subscription> m_controlSub;
    std::shared_ptr<aeron::Publication> m_requestPub;
    std::shared_ptr<aeron::Image> m_replayImage;
    std::shared_ptr<aeron::Counter> m_recoveryStalledCounter;

    // Test-only fault injection (gated by enableFaultInjection): drop the next N live tap frames to
    // synthesize a consumer-side globalSeqNo gap (src/test/scripts/gap-recovery-test.sh). Armed on the
    // poll thread (deferred from OrderExecServer's SIGUSR1 handler) and consumed on the poll thread; the
    // atomic mirrors the Java side and stays safe if a caller ever arms it from another thread.
    bool m_faultInjection = false;
    std::atomic<int> m_faultDropPending{ 0 };

    std::unique_ptr<aeron::FragmentAssembler> m_tapAssembler;
    std::unique_ptr<aeron::FragmentAssembler> m_replayAssembler;
    std::unique_ptr<aeron::FragmentAssembler> m_controlAssembler;

    // Composed once: FragmentAssembler::handler() builds a fresh std::function per call, and these
    // are polled every duty-cycle iteration.
    aeron::fragment_handler_t m_tapPoll;
    aeron::fragment_handler_t m_replayPoll;
    aeron::fragment_handler_t m_controlPoll;
};

} // namespace org::limitless::phixeron::replayer::client
