#pragma once

// ReplayerStreamReceiver — app-replica side of the per-node Replayer. Reads the co-located tap live and
// asks the Replayer for history and gaps. The Aeron adapter only: every decision lives in ReplayerRecovery.
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

#include "org/limitless/seqeron/replayer/client/ReplayerRecovery.hpp"
#include "org/limitless/seqeron/util/SeqeronCounters.hpp"

// Request codecs (sbe-replay.xml); the replies are decoded in ReplayerRecovery.
#include "org_limitless_seqeron_sbe_replay/MessageHeader.h"
#include "org_limitless_seqeron_sbe_replay/ReplayComplete.h"
#include "org_limitless_seqeron_sbe_replay/ReplayHeartbeat.h"
#include "org_limitless_seqeron_sbe_replay/ReplayRequest.h"

namespace org::limitless::seqeron::replayer::client {

// FEEDER_STREAM_ID is the recorded sequenced stream id, which the live tap and the Replayer's replays
// both address; frameStartPosition derives a frame's recording position from its Aeron header.
using org::limitless::seqeron::sequencer::FEEDER_STREAM_ID;
using org::limitless::seqeron::sequencer::frameStartPosition;

// ── Node-local IPC channels/streams — MUST match org.limitless.seqeron.replayer.server.ReplayerService ─────────
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
      // Temporaries: FragmentAssembler copies the delegate into its own member.
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

    // Subscribes the tap and control streams, opens the request publication and the convergence counter,
    // and requests the cold-start replay. memberId is this app's node, to label the counter.
    void start(std::shared_ptr<aeron::Aeron> aeron, const std::int32_t memberId)
    {
        m_aeron = std::move(aeron);
        m_recoveryStalledCounterRegId = util::addAppCounter(
            m_aeron, util::APP_RECOVERY_STALLED_TYPE_ID,
            "seqeron.app.recoveryStalled member=" + std::to_string(memberId) + " client=" + std::to_string(m_clientId),
            memberId, m_clientId);
        m_tapSubRegId = m_aeron->addSubscription(FEEDER_CHANNEL, FEEDER_STREAM_ID);
        // No standing replay subscription: openReplay opens one per episode, filtered to its session id.
        m_controlSubRegId = m_aeron->addSubscription(REPLAYER_CONTROL_CHANNEL, REPLAYER_CONTROL_STREAM_ID);
        m_requestPubRegId = m_aeron->addPublication(REPLAYER_IPC_CHANNEL, REPLAYER_REQUEST_STREAM_ID);
        m_recovery.start();
    }

    // Test-only: lets injectTapDrop drop live tap frames, to drive gap recovery deterministically
    // (gap-recovery-test.sh). Never enabled in production.
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

    // One duty-cycle iteration; returns fragments consumed. Drains control, rides an attached replay image,
    // and always drains and dispatches the tap: ReplayerRecovery's contiguity check decides what a tap frame
    // is worth mid-walk.
    int poll()
    {
        resolveResources();

        int work = 0;
        if (m_controlSub)
        {
            work += m_controlSub->poll(m_controlPoll, FRAGMENT_LIMIT);
        }

        // The request publication's connect is a short race, so retry every poll while it is pending.
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

        // Always drain the tap: it is untethered, so an unpolled subscription falls behind. Frames ahead of
        // an in-flight replay are retained or dropped by the contiguity check; the one at the seam is needed.
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

    // Highest globalSeqNo dispatched in order, 0 before the first: the frontier recovery progress is
    // measured by.
    std::int64_t lastGlobalSeqNo() const
    {
        return m_recovery.lastGlobalSeqNo();
    }

    // memberId of the current leader per the last LeadershipChanged, or -1 until one is seen.
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

    // Subscribes to exactly one replay, this one, and to nothing on the replay stream otherwise. A standing
    // subscription would make every idle app a tethered, never-polled subscriber of every other app's
    // replay on the shared stream, and wedge the archive's replay one window in.
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

    // The gauge mirrors ReplayerRecovery's stall report; null until the async add resolves.
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
        // Test-only fault injection: dropped before the receiver sees it, so the next frame reads as a gap.
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

    // Test-only: live tap frames still to drop (see enableFaultInjection). Atomic, as on the Java side.
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

} // namespace org::limitless::seqeron::replayer::client
