// ReplayerStreamReceiver's first-frame-must-be-globalSeqNo-1 baseline check (doc/todo.md "Replayer /
// ingress" — cold start answered NO_REPLAY_NEEDED adopting an arbitrary globalSeqNo baseline: nothing
// checked the first frame delivered was globalSeqNo 1, so a node whose own recording doesn't reach the
// start of the log would silently track position/state from mid-stream). globalSeqNo only increases,
// so once the first frame observed isn't 1, no later frame ever can be either — the condition is
// permanent, not transient, and retrying buys nothing. It fails fast: aborts the process rather than
// latching a flag a caller might not check, which would leave it running but silently doing nothing.
//
// Frames are fabricated by hand, the same way FrameStartPositionTest.cpp builds an aeron::Header,
// since the C++ suite runs no Aeron media driver (see CLAUDE.md) — testDeliverTapFragment feeds them
// through the exact decode path poll() drives off the live tap subscription (fromReplay=false).

#include <chrono>
#include <thread>

#include <gtest/gtest.h>

#include "aeron_image.h"  // aeron_header_t / aeron_data_header_t layout, to build a header by hand
#include "org/limitless/phixeron/replayer/ReplayerStreamReceiver.hpp"
#include "org/limitless/phixeron/util/Logger.hpp"
#include "org_limitless_phixeron_sbe_sequenced/Heartbeat.h"
#include "org_limitless_phixeron_sbe_unsequenced/ReplayPending.h"
#include "org_limitless_phixeron_sbe_unsequenced/ReplayUnavailable.h"
#include "org_limitless_phixeron_sbe_unsequenced/Replaying.h"

namespace org::limitless::phixeron::sequencer {
namespace {

namespace seq = org::limitless::phixeron::sbe::sequenced;
namespace diag = org::limitless::phixeron::util;

// Captures every LoggerEvent reported while in scope, in place of the installed default
// (StderrLoggerSink) — see Logger.hpp. RAII so a test that ASSERTs out early still restores
// the default sink rather than leaving a dangling pointer installed for later tests.
struct ScopedLoggerSink final : diag::LoggerSink
{
    ScopedLoggerSink()
    {
        diag::Logger::install(*this);
    }

    ~ScopedLoggerSink() override
    {
        diag::Logger::reset();
    }

    void record(const diag::LoggerEvent& event) override
    {
        events.push_back(event);
    }

    std::vector<diag::LoggerEvent> events;
};

constexpr std::int32_t DATA_HEADER_LENGTH = 32;  // AERON_DATA_HEADER_LENGTH
constexpr std::int32_t TERM_ID = 7;
constexpr std::int32_t INITIAL_TERM_ID = 7;         // termCount 0, so position == termOffset
constexpr std::size_t POSITION_BITS_TO_SHIFT = 16;  // 64 KiB terms

// A single unfragmented, 32-byte-aligned frame header, as a poll handler would see it — same
// fabrication FrameStartPositionTest.cpp uses, trimmed to the one shape this test needs.
struct Frame {
    aeron_data_header_t data{};
    aeron_header_t header{};

    Frame(const std::int32_t termOffset, const std::int32_t frameLength)
    {
        data.frame_header.frame_length = frameLength;
        data.term_offset = termOffset;
        data.term_id = TERM_ID;
        header.frame = &data;
        header.initial_term_id = INITIAL_TERM_ID;
        header.position_bits_to_shift = POSITION_BITS_TO_SHIFT;
        header.fragmented_frame_length = AERON_NULL_VALUE;
    }

    aeron::Header wrap()
    {
        return aeron::Header{&header};
    }
};

// One Heartbeat frame (template id 48) at the given globalSeqNo — clear of the
// ClientConnected/ClientDisconnected/LeadershipChanged special ids (1/2/5), so it always reaches
// onSequenced rather than being intercepted as a lifecycle/leadership event.
std::vector<std::uint8_t> encodeHeartbeat(const std::int64_t globalSeqNo)
{
    std::vector<std::uint8_t> buf(256, 0);
    seq::Heartbeat enc;
    enc.wrapAndApplyHeader(reinterpret_cast<char*>(buf.data()), 0, buf.size());
    enc.header().sourceId(1).connectionId(0).sessionId(1).globalSeqNo(globalSeqNo).timestamp(0);
    enc.seqNum(1).sendingTimeMs(0).origin(seq::Origin::Value::Client).possDupFlag(seq::PossDupFlag::Value::NULL_VALUE);
    enc.testReqID()[0] = '\0';
    buf.resize(enc.sbePosition());
    return buf;
}

// Feeds one Heartbeat frame straight into the client, exactly as poll() would off the live tap.
// termOffset places the frame within the term, which is what gives it a recording position — the
// default 0 suffices wherever a test does not care, but requestResume anchors on the last dispatched
// frame's position, so a test that checks the anchor must space its frames apart.
void deliverLive(ReplayerStreamReceiver& client, const std::int64_t globalSeqNo, const std::int32_t termOffset = 0)
{
    auto buf = encodeHeartbeat(globalSeqNo);
    const auto frameLength = DATA_HEADER_LENGTH + static_cast<std::int32_t>(buf.size());
    Frame frame{termOffset, frameLength};
    aeron::concurrent::AtomicBuffer ab(buf.data(), buf.size());
    client.testDeliverTapFragment(ab, 0, static_cast<aeron::util::index_t>(buf.size()), frame.wrap());
}

TEST(ReplayerStreamReceiverBaseline, FirstFrameAtGlobalSeqNoOneIsAccepted)
{
    int delivered = 0;
    ReplayerStreamReceiver client{1, [&](const SequencedEvent&) { ++delivered; }};

    deliverLive(client, 1);

    EXPECT_EQ(1, delivered);
    EXPECT_TRUE(client.isCaughtUp());
}

TEST(ReplayerStreamReceiverBaseline, FirstFrameNotAtGlobalSeqNoOneAbortsTheProcess)
{
    ReplayerStreamReceiver client{1, [](const SequencedEvent&) {}};

    EXPECT_DEATH(deliverLive(client, 57), "globalSeqNo=57, expected 1")
        << "must not silently adopt a mid-stream baseline";
}

// Coverage for the gap-recovery/re-walk state machine (doc/todo.md "gap re-walk starves the tap it is
// recovering", fixed 2026-07-31): mid-stream gap detection, de-dup of already-seen replayed frames, the
// Replayer control-stream transitions (Replaying / NO_REPLAY_NEEDED / ReplayPending), and the
// segment-chain walk advancing on completion. These drive the same private onFragment/onControl/
// onReplaySegmentComplete logic poll() calls, via test-only seams next to testDeliverTapFragment — the
// C++ suite runs no Aeron media driver (see file header) and poll() itself needs live Aeron
// subscriptions to resolve, so poll()'s own m_tapSub->poll(...) call and its "always drain AND dispatch
// the tap regardless of state" guarantee are exercised only by src/test/scripts/gap-recovery-test.sh,
// not here. What is locked below is the logic poll() feeds those fragments to: the contiguity/de-dupe
// decision, the isRecovering() predicate that separates a real hole from the tap running ahead of an
// in-flight walk, and the isCaughtUp() state transitions consumers gate on.

// One Heartbeat frame from a replay image (fromReplay=true), otherwise identical to deliverLive.
void deliverReplay(ReplayerStreamReceiver& client, const std::int64_t globalSeqNo)
{
    auto buf = encodeHeartbeat(globalSeqNo);
    const auto frameLength = DATA_HEADER_LENGTH + static_cast<std::int32_t>(buf.size());
    Frame frame{/*termOffset=*/0, frameLength};
    aeron::concurrent::AtomicBuffer ab(buf.data(), buf.size());
    client.testDeliverReplayFragment(ab, 0, static_cast<aeron::util::index_t>(buf.size()), frame.wrap());
}

// requestId must be the client's CURRENT one (client.testRequestId()) for a reply to be acted on — see
// onControl's correlation check; pass a different value to build the stale reply a resend leaves behind.
std::vector<std::uint8_t> encodeReplaying(const std::int32_t clientId, const std::int64_t requestId,
                                          const std::int64_t replaySessionId, const std::int64_t catchUpPosition)
{
    std::vector<std::uint8_t> buf(64, 0);
    usq::Replaying enc;
    enc.wrapAndApplyHeader(reinterpret_cast<char*>(buf.data()), 0, buf.size());
    enc.clientId(clientId).requestId(requestId).replaySessionId(replaySessionId).catchUpPosition(catchUpPosition);
    buf.resize(enc.sbePosition());
    return buf;
}

std::vector<std::uint8_t> encodeReplayPending(const std::int32_t clientId, const std::int64_t requestId)
{
    std::vector<std::uint8_t> buf(32, 0);
    usq::ReplayPending enc;
    enc.wrapAndApplyHeader(reinterpret_cast<char*>(buf.data()), 0, buf.size());
    enc.clientId(clientId).requestId(requestId);
    buf.resize(enc.sbePosition());
    return buf;
}

std::vector<std::uint8_t> encodeReplayUnavailable(const std::int32_t clientId, const std::int64_t requestId)
{
    std::vector<std::uint8_t> buf(32, 0);
    usq::ReplayUnavailable enc;
    enc.wrapAndApplyHeader(reinterpret_cast<char*>(buf.data()), 0, buf.size());
    enc.clientId(clientId).requestId(requestId);
    buf.resize(enc.sbePosition());
    return buf;
}

// Feeds one already-encoded control-stream message (Replaying/ReplayPending) straight into the client,
// exactly as onControl() would decode it off the Replayer's control subscription.
void deliverControl(ReplayerStreamReceiver& client, const std::vector<std::uint8_t>& body)
{
    Frame frame{/*termOffset=*/0, DATA_HEADER_LENGTH + static_cast<std::int32_t>(body.size())};
    aeron::concurrent::AtomicBuffer ab(const_cast<std::uint8_t*>(body.data()), body.size());
    client.testDeliverControl(ab, 0, static_cast<aeron::util::index_t>(body.size()), frame.wrap());
}

// Companion to ReplayerStreamReceiverBaseline.FirstFrameNotAtGlobalSeqNoOneAbortsTheProcess, and the
// regression the 2026-08-05 seam fix first introduced: once the tap is dispatched mid-walk, a cold start
// races a live tap already carrying mid-stream globalSeqNos against a replay that has not delivered 1
// yet. That tap frame is neither a valid baseline nor a fault — only the walk may establish the
// baseline. (Caught by gap-recovery-test.sh, where every consumer aborted at startup with "first frame
// observed has globalSeqNo=15"; this locks it where it costs a millisecond instead of a cluster.)
TEST(ReplayerStreamReceiverBaseline, LiveTapMayNotEstablishTheBaselineWhileTheColdStartWalkIsInFlight)
{
    int delivered = 0;
    ReplayerStreamReceiver client{1, [&](const SequencedEvent&) { ++delivered; }};

    // What start() does: request segment 0 before any frame has been seen.
    deliverControl(client, encodeReplaying(/*clientId=*/1, client.testRequestId(), /*replaySessionId=*/7,
                                           /*catchUpPosition=*/500));
    ASSERT_TRUE(client.testIsRecovering());

    deliverLive(client, 15);  // the live tip, far ahead of a replay that has not started delivering

    EXPECT_EQ(0, delivered) << "must not adopt the live tap's mid-stream baseline";
    EXPECT_FALSE(client.isCaughtUp());

    deliverReplay(client, 1);  // the walk supplies the real baseline

    EXPECT_EQ(1, delivered);
}

TEST(ReplayerStreamReceiverGapRecovery, MidStreamGapTriggersReplayRequestAndWithholdsTheOutOfOrderFrame)
{
    int delivered = 0;
    ReplayerStreamReceiver client{1, [&](const SequencedEvent&) { ++delivered; }};

    deliverLive(client, 1);
    deliverLive(client, 2);
    ASSERT_EQ(2, delivered);
    ASSERT_FALSE(client.testIsAwaitingReplay());

    deliverLive(client, 5);  // skips 3, 4 -> gap

    EXPECT_EQ(2, delivered) << "the out-of-order frame itself must not be dispatched";
    EXPECT_TRUE(client.testIsAwaitingReplay()) << "a gap must re-request a replay";
    EXPECT_EQ(-1, client.testReplaySessionId()) << "no session yet — only a request, until the Replayer answers";
}

TEST(ReplayerStreamReceiverGapRecovery, TapGapReportsAStructuredDiagnosticEvent)
{
    ScopedLoggerSink sink;
    ReplayerStreamReceiver client{1, [](const SequencedEvent&) {}};

    deliverLive(client, 1);
    deliverLive(client, 2);
    deliverLive(client, 5);  // skips 3, 4 -> gap

    ASSERT_EQ(1u, sink.events.size()) << "exactly the one gap detected above, nothing from setup";
    const auto& event = sink.events[0];
    EXPECT_EQ(diag::Component::ReplayerStreamReceiver, event.component);
    EXPECT_EQ(diag::Severity::Warn, event.severity);
    EXPECT_EQ(diag::EventCode::TapGap, event.code);
    const std::string text(event.text.data(), event.textLen);
    EXPECT_EQ("tap gap: expected globalSeqNo=3 got 5 — resuming the recording at globalSeqNo=2", text);
}

// A tap frame that is non-contiguous only because the tap runs ahead of an in-flight replay must not be
// mistaken for a new gap. Now that poll() dispatches the tap mid-walk instead of discarding it (the
// 2026-08-05 seam fix), this is the common case, not a rarity: every tap frame arriving while a walk
// replays history reads as "ahead". Superseding the walk on each one would restart it from segment 0
// forever under any sustained publish rate.
TEST(ReplayerStreamReceiverGapRecovery, TapFrameAheadOfAnInFlightWalkDoesNotSupersedeIt)
{
    ScopedLoggerSink sink;
    int delivered = 0;
    ReplayerStreamReceiver client{1, [&](const SequencedEvent&) { ++delivered; }};

    deliverLive(client, 1);
    deliverLive(client, 5);  // gap -> requestReplay(0, 0), awaiting
    ASSERT_TRUE(client.testIsAwaitingReplay());
    ASSERT_EQ(1u, sink.events.size()) << "the genuine steady-state gap";

    // The Replayer answers with an active session: now mid-walk (m_replaySessionId >= 0), not merely
    // awaiting an answer. (The e2e version of this scenario — re-arming a live-tap drop while a real walk
    // is in flight — was attempted and abandoned as impractical: local Aeron IPC replay of a small gap
    // completes too fast for a shell-level poll-then-signal loop to reliably land inside the window; see
    // doc/todo.md's 2026-08-02 note. This state transition is locked down here instead.)
    deliverControl(client, encodeReplaying(/*clientId=*/1, client.testRequestId(), /*replaySessionId=*/99,
                                           /*catchUpPosition=*/500));
    ASSERT_EQ(99, client.testReplaySessionId());
    ASSERT_FALSE(client.testIsAwaitingReplay());
    ASSERT_TRUE(client.testIsRecovering());

    // A later frame arrives on the live tap while that walk is still riding history. m_lastGlobalSeqNo is
    // still 1, so it is non-contiguous — but it is the tap running ahead, not a hole the walk won't cover.
    deliverLive(client, 12);

    EXPECT_EQ(99, client.testReplaySessionId()) << "the in-flight walk must run to completion, not be restarted";
    EXPECT_FALSE(client.testIsAwaitingReplay());
    EXPECT_EQ(-1, client.testWalkSegmentIndex()) << "still the resume the gap asked for, not restarted";
    EXPECT_EQ(1, delivered) << "the out-of-order frame itself is still withheld";
    EXPECT_EQ(1u, sink.events.size()) << "and it is not reported as a second gap";
}

// The replay->live seam (doc/review A2): the tap is dispatched throughout a walk, so the frame at
// gseq == last+1 lands the instant the replay reaches it. Before the fix poll() routed tap frames to a
// discard handler while recovering, consuming exactly these frames — so every walk ended one guaranteed
// gap short of live and re-walked the whole chain, converging only if nothing was published during the
// final round trip. NOTE: poll()'s handler *routing* needs a live tap subscription and is exercised only
// by src/test/scripts/gap-recovery-test.sh; what is locked here is the decision logic it feeds.
TEST(ReplayerStreamReceiverGapRecovery, LiveTapFrameAtTheSeamIsDispatchedWhileTheWalkIsStillInFlight)
{
    int delivered = 0;
    ReplayerStreamReceiver client{1, [&](const SequencedEvent&) { ++delivered; }};

    deliverControl(client, encodeReplaying(/*clientId=*/1, client.testRequestId(), /*replaySessionId=*/7,
                                           /*catchUpPosition=*/500));
    deliverReplay(client, 1);
    deliverReplay(client, 2);
    ASSERT_EQ(2, delivered);
    ASSERT_FALSE(client.isCaughtUp()) << "still riding replayed history";
    ASSERT_TRUE(client.testIsRecovering());

    deliverLive(client, 3);  // the seam: first tap frame contiguous with what the replay delivered

    EXPECT_EQ(3, delivered) << "the seam frame must be dispatched, not discarded as mid-walk noise";
    EXPECT_TRUE(client.isCaughtUp()) << "a contiguous frame off the live tap means we are following live";
}

// A steady-state gap resumes the recording at the frame last dispatched (doc/review A9), so repairing a
// dropped frame costs a replay of the hole rather than of the whole trading day. The stale walk index is
// the thing it must never carry into that request: that is a cold-start cursor into the recording chain,
// not a position, and resuming a walk from wherever the last one had got to is unsound.
TEST(ReplayerStreamReceiverGapRecovery, SteadyStateGapResumesAtTheLastDispatchedFrameNotTheStaleWalkIndex)
{
    ReplayerStreamReceiver client{1, [](const SequencedEvent&) {}};

    // Move the walk index off 0 first (simulating a cold-start walk already into segment 2).
    deliverControl(client, encodeReplaying(/*clientId=*/1, client.testRequestId(), /*replaySessionId=*/7,
                                           /*catchUpPosition=*/100));
    client.testCompleteReplaySegment();
    client.testCompleteReplaySegment();
    ASSERT_EQ(2, client.testWalkSegmentIndex());

    deliverControl(client, encodeReplaying(/*clientId=*/1, client.testRequestId(), REPLAYER_NO_REPLAY_NEEDED,
                                           /*catchUpPosition=*/0));
    ASSERT_TRUE(client.isCaughtUp());

    deliverLive(client, 1, /*termOffset=*/0);
    deliverLive(client, 2, /*termOffset=*/1024);  // the frame the resume must anchor on
    deliverLive(client, 5, /*termOffset=*/2048);  // steady-state gap

    EXPECT_EQ(-1, client.testWalkSegmentIndex()) << "a gap asks to resume, never to continue the old walk";
    EXPECT_EQ(1024, client.testReqFromPosition()) << "anchored at the last frame DISPATCHED, not at the "
                                                     "out-of-order one that exposed the hole";
}

TEST(ReplayerStreamReceiverGapRecovery, DuplicateAndStaleFramesFromReplayAreDropped)
{
    int delivered = 0;
    ReplayerStreamReceiver client{1, [&](const SequencedEvent&) { ++delivered; }};

    deliverLive(client, 1);
    deliverLive(client, 2);
    ASSERT_EQ(2, delivered);

    deliverReplay(client, 1);  // already seen live -> dup
    deliverReplay(client, 2);  // already seen live -> dup
    EXPECT_EQ(2, delivered) << "replayed frames already delivered off the live tap must be de-duped";

    deliverReplay(client, 3);  // new -> delivered
    EXPECT_EQ(3, delivered);
}

TEST(ReplayerStreamReceiverGapRecovery, NoReplayNeededMarksCaughtUpAndClearsWalkState)
{
    ReplayerStreamReceiver client{1, [](const SequencedEvent&) {}};

    ASSERT_FALSE(client.isCaughtUp());
    deliverControl(client, encodeReplaying(/*clientId=*/1, client.testRequestId(), REPLAYER_NO_REPLAY_NEEDED,
                                           /*catchUpPosition=*/0));

    EXPECT_TRUE(client.isCaughtUp()) << "NO_REPLAY_NEEDED means already at the tip of an empty/exhausted chain";
    EXPECT_FALSE(client.testIsAwaitingReplay());
    EXPECT_EQ(-1, client.testReplaySessionId());
    EXPECT_EQ(-1, client.testWalkSegmentIndex()) << "chain exhausted -> steady/resume mode, not mid-walk";
}

TEST(ReplayerStreamReceiverGapRecovery, ReplayingWithSessionArmsReplayWithoutMarkingCaughtUp)
{
    ReplayerStreamReceiver client{1, [](const SequencedEvent&) {}};

    deliverControl(client, encodeReplaying(/*clientId=*/1, client.testRequestId(), /*replaySessionId=*/42,
                                           /*catchUpPosition=*/1'000));

    EXPECT_FALSE(client.isCaughtUp()) << "a session id means there IS history to replay first";
    EXPECT_FALSE(client.testIsAwaitingReplay()) << "onControl always clears awaiting once answered";
    EXPECT_EQ(42, client.testReplaySessionId());
}

TEST(ReplayerStreamReceiverGapRecovery, ReplayingForAnotherClientIdIsIgnored)
{
    ReplayerStreamReceiver client{1, [](const SequencedEvent&) {}};

    deliverControl(client, encodeReplaying(/*clientId=*/2, client.testRequestId(), /*replaySessionId=*/42,
                                           /*catchUpPosition=*/1'000));

    EXPECT_EQ(-1, client.testReplaySessionId())
        << "a reply on the shared control stream addressed to a different replica must not be applied";
    EXPECT_FALSE(client.isCaughtUp());
}

TEST(ReplayerStreamReceiverGapRecovery, SegmentCompleteAdvancesTheWalkAndReRequestsTheNextSegment)
{
    ReplayerStreamReceiver client{1, [](const SequencedEvent&) {}};
    deliverControl(client, encodeReplaying(/*clientId=*/1, client.testRequestId(), /*replaySessionId=*/7,
                                           /*catchUpPosition=*/500));
    ASSERT_EQ(7, client.testReplaySessionId());

    client.testCompleteReplaySegment();

    EXPECT_EQ(1, client.testWalkSegmentIndex()) << "segment 0 done -> walk advances to segment 1";
    EXPECT_TRUE(client.testIsAwaitingReplay()) << "advancing re-requests the next segment";
    EXPECT_EQ(-1, client.testReplaySessionId()) << "no session until the Replayer answers the new request";
}

// ── Request/reply correlation (doc/review A3) ────────────────────────────────────────────────
// clientId alone cannot identify WHICH request a reply answers. A resend makes the Replayer stop the
// in-flight session and start a new one, so both replies sit in order on the one shared control
// publication and the stale one is always processed first. Acting on it attached the client to a
// session that no longer exists — and, because onControl cleared m_awaitingReplay unconditionally, also
// stopped the resend timer, leaving the client with no session, no image and no retry.
TEST(ReplayerStreamReceiverGapRecovery, ReplyForASupersededRequestIsIgnored)
{
    ReplayerStreamReceiver client{1, [](const SequencedEvent&) {}};
    const std::int64_t stale = client.testRequestId();

    client.testCompleteReplaySegment();  // advances the walk -> new request, new requestId
    ASSERT_NE(stale, client.testRequestId());
    ASSERT_TRUE(client.testIsAwaitingReplay());

    deliverControl(client, encodeReplaying(/*clientId=*/1, stale, /*replaySessionId=*/42, /*catchUpPosition=*/900));

    EXPECT_EQ(-1, client.testReplaySessionId()) << "must not attach to the superseded request's stopped session";
    EXPECT_TRUE(client.testIsAwaitingReplay()) << "and must keep awaiting, so the resend timer stays armed";
}

// The live reply that follows the stale one must still be taken — correlation must reject the stale
// reply without wedging the client against every later one.
TEST(ReplayerStreamReceiverGapRecovery, CurrentReplyIsStillAcceptedAfterAStaleOne)
{
    ReplayerStreamReceiver client{1, [](const SequencedEvent&) {}};
    const std::int64_t stale = client.testRequestId();
    client.testCompleteReplaySegment();

    deliverControl(client, encodeReplaying(/*clientId=*/1, stale, /*replaySessionId=*/42, /*catchUpPosition=*/900));
    deliverControl(client, encodeReplaying(/*clientId=*/1, client.testRequestId(), /*replaySessionId=*/43,
                                           /*catchUpPosition=*/1'000));

    EXPECT_EQ(43, client.testReplaySessionId()) << "the reply to the current request must be applied";
    EXPECT_FALSE(client.testIsAwaitingReplay());
}

// A stale ReplayPending must not reset the request clock either: that is what paces the resend, so
// honouring a superseded request's "wait" would defer the retry the client is relying on.
TEST(ReplayerStreamReceiverGapRecovery, ReplayPendingForASupersededRequestIsIgnored)
{
    ReplayerStreamReceiver client{1, [](const SequencedEvent&) {}};
    const std::int64_t stale = client.testRequestId();
    client.testCompleteReplaySegment();
    const std::int64_t requestedAtMs = client.testLastRequestMs();

    std::this_thread::sleep_for(std::chrono::milliseconds(2));
    deliverControl(client, encodeReplayPending(/*clientId=*/1, stale));

    EXPECT_EQ(requestedAtMs, client.testLastRequestMs()) << "a superseded request's ReplayPending must not "
                                                            "push out the current request's resend deadline";
}

// ── Closed image vs. completed segment (doc/review A4) ───────────────────────────────────────
// A bounded replay of a STOPPED recording closes its image on its own, exactly at the stopPosition it
// was bounded to — the one close that means "segment done".
TEST(ReplayerStreamReceiverGapRecovery, ReplayImageClosingAtTheBoundCompletesTheSegment)
{
    ReplayerStreamReceiver client{1, [](const SequencedEvent&) {}};
    deliverControl(client, encodeReplaying(/*clientId=*/1, client.testRequestId(), /*replaySessionId=*/7,
                                           /*catchUpPosition=*/500));
    ASSERT_EQ(0, client.testWalkSegmentIndex());

    client.testReplayImageClosed(/*finalPosition=*/500);

    EXPECT_EQ(1, client.testWalkSegmentIndex()) << "closing AT the bound is completion -> advance the walk";
    EXPECT_TRUE(client.testIsAwaitingReplay());
}

// Any other close (supersede, idle-TTL reclaim, archive fault, Replayer shutdown) leaves the image short
// of the bound. Advancing there skips history that was never replayed — a silent hole, since the walk
// rides on and only a later tap gap could ever expose it.
TEST(ReplayerStreamReceiverGapRecovery, ReplayImageClosingShortOfTheBoundReRequestsTheSameSegment)
{
    ScopedLoggerSink sink;
    ReplayerStreamReceiver client{1, [](const SequencedEvent&) {}};
    deliverControl(client, encodeReplaying(/*clientId=*/1, client.testRequestId(), /*replaySessionId=*/7,
                                           /*catchUpPosition=*/500));
    const std::int64_t requestId = client.testRequestId();

    client.testReplayImageClosed(/*finalPosition=*/312);  // stopped under us, mid-segment

    EXPECT_EQ(0, client.testWalkSegmentIndex()) << "the walk must NOT advance over a segment it only "
                                                   "partially replayed";
    EXPECT_TRUE(client.testIsAwaitingReplay()) << "the same segment is re-requested instead";
    EXPECT_EQ(-1, client.testReplaySessionId());
    EXPECT_NE(requestId, client.testRequestId()) << "re-requesting is a new request, so the reply to the "
                                                    "stopped one is recognisably stale";
    EXPECT_EQ(1u, sink.events.size()) << "a truncated replay is reported, not silently absorbed";
}

TEST(ReplayerStreamReceiverGapRecovery, ReplayPendingHoldsAtTheGapWithoutAssigningASession)
{
    int delivered = 0;
    ReplayerStreamReceiver client{1, [&](const SequencedEvent&) { ++delivered; }};
    deliverLive(client, 1);
    deliverLive(client, 5);  // gap -> awaiting a replay
    ASSERT_TRUE(client.testIsAwaitingReplay());

    deliverControl(client, encodeReplayPending(/*clientId=*/1, client.testRequestId()));

    EXPECT_TRUE(client.testIsAwaitingReplay()) << "no free Replayer slot -> keep holding at the gap";
    EXPECT_EQ(-1, client.testReplaySessionId());
    EXPECT_EQ(1, delivered) << "must not advance past the hole while pending";
}

// A Replayer whose archive failed the globalSeqNo-1 integrity check refuses instead of serving history
// its own check rejected. The refusal must be CONTAINED here: served anyway, the replay's first frame
// would not be 1 and this client would abort (see FirstFrameNotAtGlobalSeqNoOneAbortsTheProcess), taking
// down every co-located app over one node's bad archive. So this holds like ReplayPending — never caught
// up, nothing dispatched, and still resending, so repairing the archive heals the app without a restart.
TEST(ReplayerStreamReceiverGapRecovery, ReplayUnavailableHoldsWithoutAbortingOrAdvancing)
{
    int delivered = 0;
    ReplayerStreamReceiver client{1, [&](const SequencedEvent&) { ++delivered; }};
    deliverLive(client, 1);
    deliverLive(client, 5);  // gap -> awaiting a replay
    ASSERT_TRUE(client.testIsAwaitingReplay());
    ASSERT_FALSE(client.isCaughtUp());

    ScopedLoggerSink sink;  // installed after the setup gap, so it captures only the refusal below
    deliverControl(client, encodeReplayUnavailable(/*clientId=*/1, client.testRequestId()));

    EXPECT_TRUE(client.testIsAwaitingReplay()) << "a refusal must not stop the resend timer";
    EXPECT_EQ(-1, client.testReplaySessionId());
    EXPECT_EQ(1, delivered) << "must not advance past the hole";
    EXPECT_FALSE(client.isCaughtUp()) << "consumer gates must stay shut while history is unavailable";

    ASSERT_EQ(1u, sink.events.size());
    EXPECT_EQ(diag::Severity::Fault, sink.events[0].severity);
    EXPECT_EQ(diag::EventCode::ReplayUnavailable, sink.events[0].code);

    // The Replayer answers every resend the same way; the fault line must not repeat per reply.
    deliverControl(client, encodeReplayUnavailable(/*clientId=*/1, client.testRequestId()));
    EXPECT_EQ(1u, sink.events.size()) << "the refusal is permanent — report it once, not every 500ms";
}

TEST(ReplayerStreamReceiverGapRecovery, ReplayUnavailableForASupersededRequestIsIgnored)
{
    ReplayerStreamReceiver client{1, [](const SequencedEvent&) {}};
    deliverLive(client, 1);
    deliverLive(client, 5);
    const std::int64_t stale = client.testRequestId() - 1;
    const std::int64_t requestedAtMs = client.testLastRequestMs();

    ScopedLoggerSink sink;  // installed after the setup gap, so it captures only the refusal below
    deliverControl(client, encodeReplayUnavailable(/*clientId=*/1, stale));

    EXPECT_TRUE(sink.events.empty()) << "a stale refusal says nothing about the Replayer's current state";
    EXPECT_EQ(requestedAtMs, client.testLastRequestMs()) << "and must not reset the resend clock";
}

// poll() itself is safe to call directly here without ever calling start(): every subscription/
// publication registration id (m_tapSubRegId etc.) defaults to -1, so resolveResources() never
// dereferences the null m_aeron, and requestReplay()'s state updates happen before its
// !m_requestPub early return. That is enough to exercise the resend-timer branch (poll()'s
// `m_awaitingReplay && (nowMs() - m_lastRequestMs) > RESEND_INTERVAL_MS`), which touches only
// m_requestPub — never m_tapSub/m_replaySub/m_controlSub/m_replayImage — unlike the tap drain itself,
// which needs a live tap subscription poll() has no seam to fake.
TEST(ReplayerStreamReceiverGapRecovery, StuckAwaitingReplayResendsAfterTheIntervalElapses)
{
    ReplayerStreamReceiver client{1, [](const SequencedEvent&) {}};

    deliverLive(client, 1);
    deliverLive(client, 5);  // gap -> requestReplay(0, 0), arming the resend timer
    ASSERT_TRUE(client.testIsAwaitingReplay());
    const std::int64_t firstRequestMs = client.testLastRequestMs();

    client.poll();
    EXPECT_EQ(firstRequestMs, client.testLastRequestMs()) << "must not resend before RESEND_INTERVAL_MS elapses";
    EXPECT_TRUE(client.testIsAwaitingReplay());

    std::this_thread::sleep_for(std::chrono::milliseconds(600));  // > RESEND_INTERVAL_MS (500)

    client.poll();
    EXPECT_GT(client.testLastRequestMs(), firstRequestMs)
        << "the Replayer never answered -> poll() must re-send the same request rather than give up";
    EXPECT_TRUE(client.testIsAwaitingReplay()) << "still stuck at the gap — a resend, not a new state";
    EXPECT_EQ(-1, client.testReplaySessionId());
    EXPECT_EQ(-1, client.testWalkSegmentIndex()) << "resend repeats the same request verbatim, still the resume";
}

// isRecovering() is what separates "the tap is ahead of my replay" from "there is a hole" — narrowing it
// to miss a state means a walk supersedes itself on its own in-flight frames (see
// TapFrameAheadOfAnInFlightWalkDoesNotSupersedeIt), widening it means a real gap goes unreported. This
// walks every transition it must track. (It also guarded the discard routing behind doc/todo.md's
// 2026-07-31 "gap re-walk starves the tap it is recovering"; that discard is gone as of 2026-08-05 —
// poll() now always drains AND dispatches the tap — but the predicate itself carries more weight, not
// less.)
TEST(ReplayerStreamReceiverGapRecovery, RecoveringFlagTracksWalkAndAwaitingReplayState)
{
    ReplayerStreamReceiver client{1, [](const SequencedEvent&) {}};
    EXPECT_FALSE(client.testIsRecovering()) << "nothing in flight yet";

    deliverLive(client, 1);
    deliverLive(client, 5);  // skips 3, 4 -> gap, re-requests a replay
    EXPECT_TRUE(client.testIsRecovering()) << "awaiting the Replayer's answer is already mid-recovery";

    deliverControl(client, encodeReplaying(/*clientId=*/1, client.testRequestId(), /*replaySessionId=*/7,
                                           /*catchUpPosition=*/500));
    EXPECT_TRUE(client.testIsRecovering()) << "a session id means history remains to replay, not yet caught up";

    client.testCompleteReplaySegment();
    EXPECT_FALSE(client.testIsRecovering()) << "a resume ends at its bound — there is no next segment to ask for";

    // The walk shape of the same transitions, entered the way production enters it: a resume whose replay
    // opens on a frame other than the one it anchored on falls back to the chain walk.
    deliverLive(client, 9);  // another gap -> another resume
    deliverControl(client, encodeReplaying(/*clientId=*/1, client.testRequestId(), /*replaySessionId=*/8,
                                           /*catchUpPosition=*/500));
    deliverReplay(client, 42);
    ASSERT_EQ(0, client.testWalkSegmentIndex());
    EXPECT_TRUE(client.testIsRecovering()) << "awaiting the walk's first segment";

    deliverControl(client, encodeReplaying(/*clientId=*/1, client.testRequestId(), /*replaySessionId=*/9,
                                           /*catchUpPosition=*/500));
    client.testCompleteReplaySegment();
    EXPECT_TRUE(client.testIsRecovering()) << "advancing to the next segment stays mid-walk";

    deliverControl(client, encodeReplaying(/*clientId=*/1, client.testRequestId(), REPLAYER_NO_REPLAY_NEEDED,
                                           /*catchUpPosition=*/0));
    EXPECT_FALSE(client.testIsRecovering()) << "chain exhausted -> back to steady state";
}

// isCaughtUp() is a state, not a latch (doc/review A1). Consumers gate real decisions on it —
// OrderExecClient/BasicDataClient leader-only emission, and FixGateway's tap-stall watchdog, which
// measures silence in DISPATCHED frames and self-terminates the gateway after 20s. A re-walk dispatches
// nothing until the replay passes the hole, so leaving it latched made the gateway diagnose its own
// recovery as a stalled sequencer and force a standby promotion it did not need.
TEST(ReplayerStreamReceiverGapRecovery, TapGapRevokesCaughtUpUntilTheStreamGoesContiguousAgain)
{
    int caughtUpNotifications = 0;
    ReplayerStreamReceiver client{1, [](const SequencedEvent&) {}, {}, {}, {}, [&] { ++caughtUpNotifications; }};

    deliverLive(client, 1);
    deliverLive(client, 2);
    ASSERT_TRUE(client.isCaughtUp());
    ASSERT_EQ(1, caughtUpNotifications);

    deliverLive(client, 5);  // skips 3, 4 -> gap (and 5 is retained, not dropped)
    EXPECT_FALSE(client.isCaughtUp()) << "a hole means we are demonstrably not following live";

    deliverReplay(client, 2);  // a resume opens on the frame it anchored at — already delivered, deduped
    deliverReplay(client, 3);
    EXPECT_FALSE(client.isCaughtUp()) << "4 is still missing — the hole is not closed yet";
    EXPECT_EQ(1, caughtUpNotifications);

    deliverReplay(client, 4);  // closes the hole; the retained tap frame 5 then hands straight over

    EXPECT_TRUE(client.isCaughtUp()) << "the retained frame closes the seam with no further round trip";
    EXPECT_EQ(2, caughtUpNotifications) << "consumers must be able to re-arm on re-convergence, not just "
                                           "on first catch-up";
}

// The retention half of the seam fix (doc/review A2), stated on its own: a frame from beyond the hole is
// held and delivered exactly once when the hole closes — never dropped, never duplicated, always in
// globalSeqNo order. Dropping these is what made every walk end one hole short of live and re-walk.
TEST(ReplayerStreamReceiverGapRecovery, FramesBeyondTheHoleAreRetainedAndDeliveredOnceItCloses)
{
    std::vector<std::int64_t> delivered;
    ReplayerStreamReceiver client{1, [&](const SequencedEvent& event) { delivered.push_back(event.globalSeqNo); }};

    deliverLive(client, 1);
    deliverLive(client, 4);  // gap -> retained
    deliverLive(client, 5);  // still ahead of the hole -> retained
    deliverLive(client, 6);  // ditto
    ASSERT_EQ((std::vector<std::int64_t>{1}), delivered) << "nothing past the hole may be dispatched early";

    deliverReplay(client, 1);  // a resume opens on the frame it anchored at — already delivered, deduped
    deliverReplay(client, 2);
    EXPECT_EQ((std::vector<std::int64_t>{1, 2}), delivered) << "3 is still missing";

    deliverReplay(client, 3);  // closes the hole -> 4, 5, 6 drain behind it

    EXPECT_EQ((std::vector<std::int64_t>{1, 2, 3, 4, 5, 6}), delivered)
        << "retained frames must drain in order, exactly once, with no re-walk needed";
    EXPECT_TRUE(client.isCaughtUp());
}

// A hole in REPLAYED history was the one invariant violation that produced no log and no counter
// (doc/review A10): the frame is dropped, the walk rides on, and recovery quietly never converges.
// Nothing unsafe follows — contiguity still holds — so this reports rather than aborts, once per
// episode, since the walk retries against the same chain every 500ms.
TEST(ReplayerStreamReceiverGapRecovery, GapInReplayedHistoryIsReportedOncePerEpisode)
{
    int delivered = 0;
    ReplayerStreamReceiver client{1, [&](const SequencedEvent&) { ++delivered; }};
    deliverReplay(client, 1);
    ASSERT_EQ(1, delivered);

    ScopedLoggerSink sink;  // installed after the baseline, so it captures only the hole below
    deliverReplay(client, 7);  // the recording chain does not cover 2..6

    EXPECT_EQ(1, delivered) << "the frame past the hole must not be dispatched";
    ASSERT_EQ(1u, sink.events.size());
    EXPECT_EQ(diag::Severity::Warn, sink.events[0].severity);
    EXPECT_EQ(diag::EventCode::TapGap, sink.events[0].code);

    deliverReplay(client, 8);
    EXPECT_EQ(1u, sink.events.size()) << "the walk retries against the same chain — report the episode, not "
                                         "every frame";

    // A later, distinct episode must be reported again rather than swallowed by the latch.
    deliverReplay(client, 2);
    ASSERT_EQ(2, delivered);
    deliverReplay(client, 9);
    EXPECT_EQ(2u, sink.events.size()) << "the latch clears once history goes contiguous again";
}

// The resume's safety check (doc/review A9). A position denotes a frame only within the recording it was
// observed in, and a member restart can leave the app holding one from a recording that is no longer the
// active one. Rather than trying to prove that has not happened, the resumed replay is checked where it
// lands: its first frame must be the frame the position was anchored on. Riding it regardless would walk
// an arbitrary mid-stream point, and a replay-path gap is otherwise swallowed silently.
TEST(ReplayerStreamReceiverGapRecovery, ResumeOpeningOnTheWrongFrameFallsBackToTheChainWalk)
{
    ScopedLoggerSink sink;
    int delivered = 0;
    ReplayerStreamReceiver client{1, [&](const SequencedEvent&) { ++delivered; }};

    deliverLive(client, 1);
    deliverLive(client, 5);  // gap -> resume anchored on frame 1
    ASSERT_EQ(-1, client.testWalkSegmentIndex());
    deliverControl(client, encodeReplaying(/*clientId=*/1, client.testRequestId(), /*replaySessionId=*/7,
                                           /*catchUpPosition=*/500));
    ASSERT_EQ(7, client.testReplaySessionId());

    deliverReplay(client, 77);  // the recording rotated: that position is some other frame entirely

    EXPECT_EQ(0, client.testWalkSegmentIndex()) << "fall back to the walk, which needs no position to be sound";
    EXPECT_TRUE(client.testIsAwaitingReplay());
    EXPECT_EQ(-1, client.testReplaySessionId()) << "and let go of the replay it was riding";
    EXPECT_EQ(1, delivered) << "the frame it opened on must not be dispatched";
    EXPECT_FALSE(client.isCaughtUp());
    ASSERT_EQ(2u, sink.events.size()) << "the gap, then the mismatch";
    EXPECT_EQ(diag::EventCode::TapGap, sink.events[1].code);
}

// The same rotation seen one step earlier: the Replayer answers "nothing to replay" because the position
// is already at its recording's tip. For a walk that means caught up; for a resume it cannot, because we
// only resumed on account of a hole we know is open — taking it at face value would close that hole by
// fiat and leave the client silently short of history.
TEST(ReplayerStreamReceiverGapRecovery, NoReplayNeededOverAnOpenHoleFallsBackToTheChainWalk)
{
    ReplayerStreamReceiver client{1, [](const SequencedEvent&) {}};

    deliverLive(client, 1);
    deliverLive(client, 2);
    ASSERT_TRUE(client.isCaughtUp());
    deliverLive(client, 6);  // gap -> resume
    ASSERT_EQ(-1, client.testWalkSegmentIndex());

    deliverControl(client, encodeReplaying(/*clientId=*/1, client.testRequestId(), REPLAYER_NO_REPLAY_NEEDED,
                                           /*catchUpPosition=*/0));

    EXPECT_EQ(0, client.testWalkSegmentIndex()) << "re-walk instead of believing it";
    EXPECT_TRUE(client.testIsAwaitingReplay());
    EXPECT_FALSE(client.isCaughtUp()) << "the hole above globalSeqNo=2 is still open";
}

// A resume ends at the bound it was given and has no next segment to request — so unlike a walk step it
// must declare itself caught up and hand the slot back (ReplayComplete), or the slot sits until the 60s
// idle TTL reclaims it: half of MAX_CONCURRENT_REPLAYS, held by nobody.
TEST(ReplayerStreamReceiverGapRecovery, ResumeReachingItsBoundCatchesUpWithoutRequestingAnotherSegment)
{
    int caughtUpNotifications = 0;
    ReplayerStreamReceiver client{1, [](const SequencedEvent&) {}, {}, {}, {}, [&] { ++caughtUpNotifications; }};

    deliverLive(client, 1);
    ASSERT_EQ(1, caughtUpNotifications);
    deliverLive(client, 5);  // gap -> resume
    ASSERT_FALSE(client.isCaughtUp());
    deliverControl(client, encodeReplaying(/*clientId=*/1, client.testRequestId(), /*replaySessionId=*/7,
                                           /*catchUpPosition=*/500));

    client.testCompleteReplaySegment();

    EXPECT_FALSE(client.testIsAwaitingReplay()) << "no follow-up request — a resume has no next segment";
    EXPECT_EQ(-1, client.testReplaySessionId());
    EXPECT_TRUE(client.isCaughtUp()) << "we hold everything the recording had when the request was served";
    EXPECT_EQ(2, caughtUpNotifications) << "re-fired, so consumers re-arm on re-convergence";
}

// Retained frames the replay has meanwhile covered are discarded on drain rather than double-delivered —
// the ranges legitimately overlap, since a re-walk restarts from segment 0 while the tap keeps arriving.
TEST(ReplayerStreamReceiverGapRecovery, RetainedFramesAlreadyCoveredByTheReplayAreNotRedelivered)
{
    std::vector<std::int64_t> delivered;
    ReplayerStreamReceiver client{1, [&](const SequencedEvent& event) { delivered.push_back(event.globalSeqNo); }};

    deliverLive(client, 1);
    deliverLive(client, 3);  // gap -> retained
    deliverLive(client, 4);  // retained

    deliverReplay(client, 1);  // a resume opens on the frame it anchored at — already delivered, deduped
    deliverReplay(client, 2);
    deliverReplay(client, 3);  // the replay covers a frame the tap already retained
    deliverReplay(client, 4);  // and another

    EXPECT_EQ((std::vector<std::int64_t>{1, 2, 3, 4}), delivered) << "each globalSeqNo dispatched exactly once";
}

}  // namespace
}  // namespace org::limitless::phixeron::sequencer
