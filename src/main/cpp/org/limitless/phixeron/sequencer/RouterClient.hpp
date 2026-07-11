#pragma once

// RouterClient — app-replica side of the per-node Router (doc/router-design.md).
//
// Where GlobalStreamClient opens its own archive connection and follows the leader's global-stream
// recording directly, a RouterClient never touches the archive: it consumes a node-local Router's
// aeron:ipc fan-out and asks that Router to replay when it detects a gap. That is the whole point of
// the Router — one process per node reads the archive; every replica reads cheap local IPC — so the
// leader keeps zero live network subscribers and audit.md S4 dissolves (design §0/§5).
//
// This deliberately mirrors GlobalStreamClient's proven "follow a replay image up to a catch-up
// position, fall back to a live sub, de-dupe the seam by globalSeqNo" handoff, with two substitutions:
//   • the "live sub" is the Router's IPC fan-out (ROUTER_FANOUT_STREAM_ID), not a UDP MDC sub;
//   • the replay is served by the Router (RouterReplayRequest -> RouterReplaying) instead of the app
//     calling AeronArchive::startReplay itself.
// Catch-up is detected by position (RouterReplaying.catchUpPosition), like GlobalStreamClient's live
// segment, not by the replay image closing: the Router's replay is bounded to an ACTIVE recording, and
// a bounded replay of an active recording never closes its image at the bound (see poll()).
// GlobalStreamClient itself is left untouched — FixSessionClient / fix_test_server /
// GlobalStreamLatencyProbe still use it — and this reuses its SequencedEvent/LifecycleEvent structs,
// its sequenced-schema decode, and its CLIENT_*_TEMPLATE_ID constants.
//
// Recording-position semantics (subtle, load-bearing): a frame's stream position on the fan-out is
// NOT the archive recording position, so RouterClient tracks m_lastGoodPosition only from
// REPLAY-sourced frames (whose image positions are recording positions). On a fan-out gap it
// re-requests from that last replay position — possibly older than the true hole — and the Router's
// bounded replay re-serves the overlap, which the app drops by globalSeqNo (gseq <= last). Correct,
// with a little over-replay; see design §4.

#include <array>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <functional>
#include <memory>
#include <string>

#include "Aeron.h"
#include "concurrent/AtomicBuffer.h"

// Reuses SequencedEvent / LifecycleEvent, the sequenced MessageHeader/Header codecs, and the
// CLIENT_CONNECTED/DISCONNECTED template-id constants (header-only; brings in archive headers it
// does not otherwise need, which is harmless — the un-rewired binaries include the same header).
#include "org/limitless/phixeron/sequencer/GlobalStreamClient.hpp"

// Router replay-protocol control codecs (sbe-unsequenced.xml) + LeadershipChanged (sbe-sequenced.xml)
#include "org_limitless_phixeron_sbe_sequenced/LeadershipChanged.h"
#include "org_limitless_phixeron_sbe_unsequenced/MessageHeader.h"
#include "org_limitless_phixeron_sbe_unsequenced/RouterPending.h"
#include "org_limitless_phixeron_sbe_unsequenced/RouterReplayComplete.h"
#include "org_limitless_phixeron_sbe_unsequenced/RouterReplayRequest.h"
#include "org_limitless_phixeron_sbe_unsequenced/RouterReplaying.h"

namespace org::limitless::phixeron::sequencer {

namespace usq = org::limitless::phixeron::sbe::unsequenced;

// ── Node-local IPC channels/streams — MUST match org.limitless.phixeron.router.Router ────────────
inline constexpr const char* ROUTER_IPC_CHANNEL = "aeron:ipc";
// Fan-out is subscribed untethered (design §5): a slow replica is dropped to a resting state rather
// than back-pressuring the Router, then re-detects its globalSeqNo gap and recovers via replay.
inline constexpr const char* ROUTER_FANOUT_CHANNEL = "aeron:ipc?tether=false";
inline constexpr std::int32_t ROUTER_FANOUT_STREAM_ID = 200;
inline constexpr std::int32_t ROUTER_REPLAY_STREAM_ID = 201;
inline constexpr std::int32_t ROUTER_REQUEST_STREAM_ID = 202;
inline constexpr std::int32_t ROUTER_CONTROL_STREAM_ID = 203;

// RouterReplaying.replaySessionId sentinel: "nothing to replay, you are at the tip — follow fan-out".
inline constexpr std::int64_t ROUTER_NO_REPLAY_NEEDED = -1;

// LeadershipChanged (sbe-sequenced.xml template 5), synthesized onto the global stream; like
// CLIENT_CONNECTED/DISCONNECTED it is non-FIX and outside the FIX-MsgType-derived template range.
inline constexpr std::uint16_t LEADERSHIP_CHANGED_TEMPLATE_ID = 5;

/**
 * Follows a co-located Router's IPC fan-out, decoding and dispatching sbe-sequenced messages to the
 * caller exactly like GlobalStreamClient — same SequencedEvent/LifecycleEvent callbacks — plus an
 * OnLeadershipChanged callback and currentLeaderMemberId()/isCaughtUp() accessors that the caller
 * uses to gate leader-only emission (design §3).
 *
 * Startup: cold replicas walk the Router's per-tenure recording chain by segment index (0,1,2,…),
 * riding each segment's replay image and de-duping by globalSeqNo, until the Router answers
 * NO_REPLAY_NEEDED — so history spans every leader failover, not just the current recording. Then they
 * fall back to the fan-out at the tip. Steady-state fan-out gaps resume the active recording by
 * position (segmentIndex < 0). No archive connection is opened here.
 */
class RouterClient {
   public:
    using OnSequenced = std::function<void(const SequencedEvent&)>;
    using OnConnected = std::function<void(const LifecycleEvent&)>;
    using OnDisconnected = std::function<void(const LifecycleEvent&)>;
    using OnLeadershipChanged = std::function<void(std::int32_t newLeaderMemberId, std::int64_t globalSeqNo)>;
    using OnCaughtUp = std::function<void()>;

    RouterClient(std::int32_t clientId, OnSequenced onSequenced, OnConnected onConnected = {},
                 OnDisconnected onDisconnected = {}, OnLeadershipChanged onLeadershipChanged = {},
                 OnCaughtUp onCaughtUp = {})
        : m_clientId(clientId),
          m_onSequenced(std::move(onSequenced)),
          m_onConnected(std::move(onConnected)),
          m_onDisconnected(std::move(onDisconnected)),
          m_onLeadershipChanged(std::move(onLeadershipChanged)),
          m_onCaughtUp(std::move(onCaughtUp)),
          m_fanoutHandler([this](auto& b, auto o, auto l, auto& h) { onFragment(b, o, l, h, /*fromReplay=*/false); }),
          m_replayHandler([this](auto& b, auto o, auto l, auto& h) { onFragment(b, o, l, h, /*fromReplay=*/true); }),
          m_controlHandler([this](auto& b, auto o, auto l, auto& h) { onControl(b, o, l, h); })
    {}

    // Subscribes the fan-out/replay/control streams, opens the request publication, and requests the
    // cold-start replay from position 0.
    void start(std::shared_ptr<aeron::Aeron> aeron)
    {
        m_aeron = std::move(aeron);
        m_fanoutSubRegId = m_aeron->addSubscription(ROUTER_FANOUT_CHANNEL, ROUTER_FANOUT_STREAM_ID);
        m_replaySubRegId = m_aeron->addSubscription(ROUTER_IPC_CHANNEL, ROUTER_REPLAY_STREAM_ID);
        m_controlSubRegId = m_aeron->addSubscription(ROUTER_IPC_CHANNEL, ROUTER_CONTROL_STREAM_ID);
        m_requestPubRegId = m_aeron->addPublication(ROUTER_IPC_CHANNEL, ROUTER_REQUEST_STREAM_ID);
        requestReplay(0, 0);  // cold start: walk the recording chain from segment 0
    }

    // One duty-cycle iteration; returns fragments consumed. Poll ordering: always drain control
    // (to learn Replaying/Pending); ride an attached replay image to exclusion of the fan-out (so
    // fan-out frames the app can't use yet aren't consumed and lost); otherwise, once caught up,
    // follow the fan-out.
    int poll()
    {
        resolveResources();

        int work = 0;
        if (m_controlSub)
        {
            work += m_controlSub->poll(m_controlHandler, FRAGMENT_LIMIT);
        }

        // Re-request if a prior request went unanswered (Router still starting, request lost, or
        // Router restarted — design §6). Covers both "no Replaying yet" and "Replaying seen but the
        // replay image never attached".
        if (m_awaitingReplay && (nowMs() - m_lastRequestMs) > RESEND_INTERVAL_MS)
        {
            requestReplay(m_walkSegmentIndex, m_reqFromPosition);  // re-send the same request verbatim
        }

        if (m_replaySessionId >= 0)
        {
            if (!m_replayImage && m_replaySub)
            {
                m_replayImage = m_replaySub->imageBySessionId(static_cast<std::int32_t>(m_replaySessionId));
            }
            if (m_replayImage)
            {
                if (!m_replayImage->isClosed())
                {
                    const int n = m_replayImage->poll(m_replayHandler, FRAGMENT_LIMIT);
                    if (m_replayImage->position() < m_catchUpPosition)
                    {
                        return work + n;  // still riding this segment up to its tip
                    }
                    // Reached this segment's bounded tip. A bounded replay of an ACTIVE (still-recording)
                    // recording does NOT close its image at the bound (verified: image position == tip,
                    // isClosed() stays false forever), so completion is detected by position — exactly as
                    // GlobalStreamClient does for its live segment (position() >= catchUpPosition).
                    onReplaySegmentComplete();
                    return work + n;
                }
                // Image closed on its own — a stopped historical segment's bounded replay does close at
                // its stopPosition. Same completion handling.
                onReplaySegmentComplete();
            }
            else
            {
                return work;  // Replaying received, image not yet attached — hold
            }
        }

        if (m_awaitingReplay)
        {
            return work;  // holding at the gap until the Router answers
        }

        if (m_fanoutSub)
        {
            work += m_fanoutSub->poll(m_fanoutHandler, FRAGMENT_LIMIT);
        }
        return work;
    }

    bool isCaughtUp() const
    {
        return m_caughtUp;
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

    void resolveResources()
    {
        if (!m_fanoutSub && m_fanoutSubRegId >= 0)
        {
            m_fanoutSub = m_aeron->findSubscription(m_fanoutSubRegId);
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
    }

    // Sends RouterReplayRequest(clientId, segmentIndex, fromPosition) and marks us awaiting the reply.
    // segmentIndex >= 0 asks for the segmentIndex-th recording of the cold-start chain (fromPosition
    // unused); segmentIndex < 0 resumes the active recording at fromPosition after a fan-out gap.
    // Idempotent on the Router side (it supersedes any in-flight replay for this clientId), so the
    // resend timer re-sending the same (segmentIndex, fromPosition) is safe.
    void requestReplay(std::int32_t segmentIndex, std::int64_t fromPosition)
    {
        m_walkSegmentIndex = segmentIndex;
        m_reqFromPosition = fromPosition;
        m_awaitingReplay = true;
        m_replaySessionId = -1;
        m_replayImage.reset();
        m_lastRequestMs = nowMs();
        if (!m_requestPub || !m_requestPub->isConnected())
        {
            return;  // Router not up yet; the resend timer retries
        }

        alignas(16) std::array<std::uint8_t, 64> buf{};
        usq::RouterReplayRequest enc;
        enc.wrapAndApplyHeader(reinterpret_cast<char*>(buf.data()), 0, buf.size());
        enc.clientId(m_clientId).fromPosition(fromPosition).segmentIndex(segmentIndex);
        const auto len = static_cast<aeron::util::index_t>(usq::MessageHeader::encodedLength() + enc.encodedLength());
        aeron::concurrent::AtomicBuffer ab(buf.data(), buf.size());
        m_requestPub->offer(ab, 0, len);
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

        if (mh.templateId() == usq::RouterReplaying::sbeTemplateId())
        {
            usq::RouterReplaying dec;
            dec.wrapForDecode(raw, bodyOff, mh.blockLength(), mh.version(), cap);
            if (dec.clientId() != m_clientId)
            {
                return;  // another replica's reply on the shared control stream
            }
            m_awaitingReplay = false;
            const std::int64_t session = dec.replaySessionId();
            if (session == ROUTER_NO_REPLAY_NEEDED)
            {
                m_replaySessionId = -1;   // already at the tip — follow the fan-out
                m_walkSegmentIndex = -1;  // chain exhausted (or never a walk) → steady/resume mode
                // Nothing to replay means we have walked the whole recording chain and are at the
                // Router's tip → caught up, following live (see the replay-tip case in poll() for why
                // consumers need this even with no live frame yet).
                notifyCaughtUp();
            }
            else
            {
                m_replaySessionId = session;
                // Position the bounded replay ends at; poll() declares caught up once the replay
                // image reaches it (a bounded replay of an active recording never closes its image
                // at the bound, so completion is by position, not image close — see file header).
                m_catchUpPosition = dec.catchUpPosition();
            }
        }
        else if (mh.templateId() == usq::RouterPending::sbeTemplateId())
        {
            usq::RouterPending dec;
            dec.wrapForDecode(raw, bodyOff, mh.blockLength(), mh.version(), cap);
            if (dec.clientId() == m_clientId)
            {
                // Router has no free slot; keep holding. m_awaitingReplay stays true so the resend
                // timer keeps us alive if the eventual Replaying is ever lost, but Pending itself is
                // just "wait" — reset the request clock so we don't spam while queued.
                m_lastRequestMs = nowMs();
            }
        }
    }

    using HdrSbe = org::limitless::phixeron::sbe::sequenced::MessageHeader;
    using HeaderComposite = org::limitless::phixeron::sbe::sequenced::Header;

    void onFragment(const aeron::concurrent::AtomicBuffer& buffer, aeron::util::index_t offset,
                    aeron::util::index_t length, const aeron::Header& header, bool fromReplay)
    {
        const std::int64_t receiveNs = nowNs();
        const std::int64_t framePosition = header.position() - header.frameLength();

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
        const std::uint16_t templateId = m_hdr.templateId();
        const std::uint64_t bodyOff = off + HdrSbe::encodedLength();

        m_header.wrap(raw, bodyOff, 0U, cap);
        const auto gseq = m_header.globalSeqNo();

        // Contiguity / de-duplication — identical invariant to GlobalStreamClient: globalSeqNo
        // increments by exactly one per event, so any forward jump is a gap. Drop dups; on a
        // fan-out gap, re-request a replay from the last recording position we actually know
        // (from a replay frame) and hold.
        if (m_lastGlobalSeqNo != 0)
        {
            if (gseq <= m_lastGlobalSeqNo)
            {
                return;
            }
            if (gseq > m_lastGlobalSeqNo + 1)
            {
                if (!fromReplay && !m_awaitingReplay)
                {
                    std::fprintf(stderr,
                                 "[RouterClient] fan-out gap: expected globalSeqNo=%lld got %lld — "
                                 "requesting replay from position %lld\n",
                                 static_cast<long long>(m_lastGlobalSeqNo + 1), static_cast<long long>(gseq),
                                 static_cast<long long>(m_lastGoodPosition));
                    requestReplay(-1, m_lastGoodPosition);  // resume the active recording at our last position
                }
                return;
            }
        }
        m_lastGlobalSeqNo = gseq;
        if (fromReplay)
        {
            // Only replay-image positions are true archive recording positions (see file header).
            m_lastGoodPosition = framePosition;
        }
        else if (!m_caughtUp)
        {
            // First in-order frame straight off the fan-out ⇒ we are following the live tip.
            notifyCaughtUp();
        }

        const auto srcId = m_header.sourceId();
        const auto connId = m_header.connectionId();
        const auto sessId = m_header.sessionId();
        const auto ts = m_header.timestamp();

        if (templateId == CLIENT_CONNECTED_TEMPLATE_ID)
        {
            if (m_onConnected)
            {
                m_onConnected(LifecycleEvent{gseq, sessId, ts, receiveNs});
            }
            return;
        }
        if (templateId == CLIENT_DISCONNECTED_TEMPLATE_ID)
        {
            if (m_onDisconnected)
            {
                m_onDisconnected(LifecycleEvent{gseq, sessId, ts, receiveNs});
            }
            return;
        }
        if (templateId == LEADERSHIP_CHANGED_TEMPLATE_ID)
        {
            org::limitless::phixeron::sbe::sequenced::LeadershipChanged lc;
            lc.wrapForDecode(raw, off + HdrSbe::encodedLength(), m_hdr.blockLength(), m_hdr.version(), cap);
            m_currentLeaderMemberId = lc.newLeaderMemberId();
            if (m_onLeadershipChanged)
            {
                m_onLeadershipChanged(m_currentLeaderMemberId, gseq);
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

    // A replay segment finished (reached its bounded tip, or its image closed for a stopped segment).
    // In a cold-start walk (m_walkSegmentIndex >= 0) ask the Router for the next segment and stay "not
    // caught up": later tenures may still need replaying, and delivering them with m_caughtUp set would
    // break the S2 "don't re-answer history" gate — the terminating NO_REPLAY_NEEDED (chain exhausted)
    // is what finally marks us caught up. Otherwise (steady-state gap resume, segmentIndex < 0) we are
    // back at the live tip: report caught up and follow the fan-out — the globalSeqNo de-dupe covers the
    // seam and any residual gap re-triggers a request. Consumers rely on the caught-up signal even on a
    // quiet cluster with no live frame yet (FixSessionClient gates TCP accept on isCaughtUp()).
    void onReplaySegmentComplete()
    {
        m_replayImage.reset();
        m_replaySessionId = -1;
        if (m_walkSegmentIndex >= 0)
        {
            requestReplay(m_walkSegmentIndex + 1, 0);  // advance the cold-start walk to the next segment
            return;
        }
        // Steady-state gap resume is done and we now follow the fan-out: release our Router replay slot
        // so it can serve another app immediately, rather than lingering until the idle-TTL (design §5).
        // (A cold-start walk needs no release — each of its slots is freed by the supersede on the next
        // segment request, and its final NO_REPLAY_NEEDED request frees the last one.)
        sendRelease();
        notifyCaughtUp();
    }

    // Fire-and-forget RouterReplayComplete: tells the Router we caught up and no longer need our replay
    // slot. A lost message just falls back to the Router's idle-TTL reclamation, and we hold no reply
    // state for it.
    void sendRelease()
    {
        if (!m_requestPub || !m_requestPub->isConnected())
        {
            return;
        }
        alignas(16) std::array<std::uint8_t, 64> buf{};
        usq::RouterReplayComplete enc;
        enc.wrapAndApplyHeader(reinterpret_cast<char*>(buf.data()), 0, buf.size());
        enc.clientId(m_clientId);
        const auto len = static_cast<aeron::util::index_t>(usq::MessageHeader::encodedLength() + enc.encodedLength());
        aeron::concurrent::AtomicBuffer ab(buf.data(), buf.size());
        m_requestPub->offer(ab, 0, len);
    }

    void notifyCaughtUp()
    {
        if (m_caughtUp)
        {
            return;  // idempotent — reached from the replay-tip, no-replay, and first-live-frame paths
        }
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

    // Same wall-clock ns stamp GlobalStreamClient records (its own nowNs() is private).
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
    std::int64_t m_fanoutSubRegId = -1;
    std::int64_t m_replaySubRegId = -1;
    std::int64_t m_controlSubRegId = -1;
    std::int64_t m_requestPubRegId = -1;
    std::shared_ptr<aeron::Subscription> m_fanoutSub;
    std::shared_ptr<aeron::Subscription> m_replaySub;
    std::shared_ptr<aeron::Subscription> m_controlSub;
    std::shared_ptr<aeron::Publication> m_requestPub;
    std::shared_ptr<aeron::Image> m_replayImage;

    bool m_awaitingReplay = false;
    std::int64_t m_replaySessionId = -1;
    std::int64_t m_catchUpPosition = 0;  // bounded replay's end position; segment done once the image reaches it
    std::int32_t m_walkSegmentIndex = 0;  // cold-start walk position; -1 once caught up (steady/resume mode)
    std::int64_t m_reqFromPosition = 0;   // fromPosition of the current request, for idempotent resend
    std::int64_t m_lastRequestMs = 0;

    std::int64_t m_lastGlobalSeqNo = 0;   // highest globalSeqNo delivered; 0 = none yet
    std::int64_t m_lastGoodPosition = 0;  // recording position of the last replay-sourced in-order frame
    bool m_caughtUp = false;
    std::int32_t m_currentLeaderMemberId = -1;

    aeron::fragment_handler_t m_fanoutHandler;
    aeron::fragment_handler_t m_replayHandler;
    aeron::fragment_handler_t m_controlHandler;

    HdrSbe m_hdr;
    HeaderComposite m_header;
};

}  // namespace org::limitless::phixeron::sequencer
