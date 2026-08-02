// ReplayerClient's first-frame-must-be-globalSeqNo-1 baseline check (doc/todo.md "Replayer /
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

#include <gtest/gtest.h>

#include "aeron_image.h"  // aeron_header_t / aeron_data_header_t layout, to build a header by hand
#include "org/limitless/phixeron/sequencer/ReplayerClient.hpp"
#include "org_limitless_phixeron_sbe_sequenced/Heartbeat.h"
#include "org_limitless_phixeron_sbe_unsequenced/ReplayPending.h"
#include "org_limitless_phixeron_sbe_unsequenced/Replaying.h"

namespace org::limitless::phixeron::sequencer {
namespace {

namespace seq = org::limitless::phixeron::sbe::sequenced;

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
void deliverLive(ReplayerClient& client, const std::int64_t globalSeqNo)
{
    auto buf = encodeHeartbeat(globalSeqNo);
    const auto frameLength = DATA_HEADER_LENGTH + static_cast<std::int32_t>(buf.size());
    Frame frame{/*termOffset=*/0, frameLength};
    aeron::concurrent::AtomicBuffer ab(buf.data(), buf.size());
    client.testDeliverTapFragment(ab, 0, static_cast<aeron::util::index_t>(buf.size()), frame.wrap());
}

TEST(ReplayerClientBaseline, FirstFrameAtGlobalSeqNoOneIsAccepted)
{
    int delivered = 0;
    ReplayerClient client{1, [&](const SequencedEvent&) { ++delivered; }};

    deliverLive(client, 1);

    EXPECT_EQ(1, delivered);
    EXPECT_TRUE(client.isCaughtUp());
}

TEST(ReplayerClientBaseline, FirstFrameNotAtGlobalSeqNoOneAbortsTheProcess)
{
    ReplayerClient client{1, [](const SequencedEvent&) {}};

    EXPECT_DEATH(deliverLive(client, 57), "globalSeqNo=57, expected 1")
        << "must not silently adopt a mid-stream baseline";
}

// Coverage for the gap-recovery/re-walk state machine (doc/todo.md "gap re-walk starves the tap it is
// recovering", fixed 2026-07-31): mid-stream gap detection, de-dup of already-seen replayed frames, the
// Replayer control-stream transitions (Replaying / NO_REPLAY_NEEDED / ReplayPending), and the
// segment-chain walk advancing on completion. These drive the same private onFragment/onControl/
// onReplaySegmentComplete logic poll() calls, via test-only seams next to testDeliverTapFragment — the
// C++ suite runs no Aeron media driver (see file header) and poll() itself needs live Aeron
// subscriptions to resolve, so the "recovering ? discard : dispatch" tap-routing inside poll() itself
// is exercised only by src/test/scripts/gap-recovery-test.sh, not here.

// One Heartbeat frame from a replay image (fromReplay=true), otherwise identical to deliverLive.
void deliverReplay(ReplayerClient& client, const std::int64_t globalSeqNo)
{
    auto buf = encodeHeartbeat(globalSeqNo);
    const auto frameLength = DATA_HEADER_LENGTH + static_cast<std::int32_t>(buf.size());
    Frame frame{/*termOffset=*/0, frameLength};
    aeron::concurrent::AtomicBuffer ab(buf.data(), buf.size());
    client.testDeliverReplayFragment(ab, 0, static_cast<aeron::util::index_t>(buf.size()), frame.wrap());
}

std::vector<std::uint8_t> encodeReplaying(const std::int32_t clientId, const std::int64_t replaySessionId,
                                           const std::int64_t catchUpPosition)
{
    std::vector<std::uint8_t> buf(64, 0);
    usq::Replaying enc;
    enc.wrapAndApplyHeader(reinterpret_cast<char*>(buf.data()), 0, buf.size());
    enc.clientId(clientId).replaySessionId(replaySessionId).catchUpPosition(catchUpPosition);
    buf.resize(enc.sbePosition());
    return buf;
}

std::vector<std::uint8_t> encodeReplayPending(const std::int32_t clientId)
{
    std::vector<std::uint8_t> buf(32, 0);
    usq::ReplayPending enc;
    enc.wrapAndApplyHeader(reinterpret_cast<char*>(buf.data()), 0, buf.size());
    enc.clientId(clientId);
    buf.resize(enc.sbePosition());
    return buf;
}

// Feeds one already-encoded control-stream message (Replaying/ReplayPending) straight into the client,
// exactly as onControl() would decode it off the Replayer's control subscription.
void deliverControl(ReplayerClient& client, const std::vector<std::uint8_t>& body)
{
    Frame frame{/*termOffset=*/0, DATA_HEADER_LENGTH + static_cast<std::int32_t>(body.size())};
    aeron::concurrent::AtomicBuffer ab(const_cast<std::uint8_t*>(body.data()), body.size());
    client.testDeliverControl(ab, 0, static_cast<aeron::util::index_t>(body.size()), frame.wrap());
}

TEST(ReplayerClientGapRecovery, MidStreamGapTriggersReplayRequestAndWithholdsTheOutOfOrderFrame)
{
    int delivered = 0;
    ReplayerClient client{1, [&](const SequencedEvent&) { ++delivered; }};

    deliverLive(client, 1);
    deliverLive(client, 2);
    ASSERT_EQ(2, delivered);
    ASSERT_FALSE(client.testIsAwaitingReplay());

    deliverLive(client, 5);  // skips 3, 4 -> gap

    EXPECT_EQ(2, delivered) << "the out-of-order frame itself must not be dispatched";
    EXPECT_TRUE(client.testIsAwaitingReplay()) << "a gap must re-request a replay";
    EXPECT_EQ(-1, client.testReplaySessionId()) << "no session yet — only a request, until the Replayer answers";
}

TEST(ReplayerClientGapRecovery, SteadyStateGapAlwaysRestartsTheWalkFromSegmentZero)
{
    ReplayerClient client{1, [](const SequencedEvent&) {}};

    // Move the walk index off 0 first (simulating a cold-start walk already into segment 2).
    deliverControl(client, encodeReplaying(/*clientId=*/1, /*replaySessionId=*/7, /*catchUpPosition=*/100));
    client.testCompleteReplaySegment();
    client.testCompleteReplaySegment();
    ASSERT_EQ(2, client.testWalkSegmentIndex());

    deliverControl(client, encodeReplaying(/*clientId=*/1, REPLAYER_NO_REPLAY_NEEDED, /*catchUpPosition=*/0));
    ASSERT_TRUE(client.isCaughtUp());

    deliverLive(client, 1);
    deliverLive(client, 5);  // steady-state gap

    EXPECT_EQ(0, client.testWalkSegmentIndex()) << "a steady-state gap re-walk must restart from segment 0, "
                                                    "not resume from wherever the last walk had gotten to — "
                                                    "robust to the recording having rotated under it";
}

TEST(ReplayerClientGapRecovery, DuplicateAndStaleFramesFromReplayAreDropped)
{
    int delivered = 0;
    ReplayerClient client{1, [&](const SequencedEvent&) { ++delivered; }};

    deliverLive(client, 1);
    deliverLive(client, 2);
    ASSERT_EQ(2, delivered);

    deliverReplay(client, 1);  // already seen live -> dup
    deliverReplay(client, 2);  // already seen live -> dup
    EXPECT_EQ(2, delivered) << "replayed frames already delivered off the live tap must be de-duped";

    deliverReplay(client, 3);  // new -> delivered
    EXPECT_EQ(3, delivered);
}

TEST(ReplayerClientGapRecovery, NoReplayNeededMarksCaughtUpAndClearsWalkState)
{
    ReplayerClient client{1, [](const SequencedEvent&) {}};

    ASSERT_FALSE(client.isCaughtUp());
    deliverControl(client, encodeReplaying(/*clientId=*/1, REPLAYER_NO_REPLAY_NEEDED, /*catchUpPosition=*/0));

    EXPECT_TRUE(client.isCaughtUp()) << "NO_REPLAY_NEEDED means already at the tip of an empty/exhausted chain";
    EXPECT_FALSE(client.testIsAwaitingReplay());
    EXPECT_EQ(-1, client.testReplaySessionId());
    EXPECT_EQ(-1, client.testWalkSegmentIndex()) << "chain exhausted -> steady/resume mode, not mid-walk";
}

TEST(ReplayerClientGapRecovery, ReplayingWithSessionArmsReplayWithoutMarkingCaughtUp)
{
    ReplayerClient client{1, [](const SequencedEvent&) {}};

    deliverControl(client, encodeReplaying(/*clientId=*/1, /*replaySessionId=*/42, /*catchUpPosition=*/1'000));

    EXPECT_FALSE(client.isCaughtUp()) << "a session id means there IS history to replay first";
    EXPECT_FALSE(client.testIsAwaitingReplay()) << "onControl always clears awaiting once answered";
    EXPECT_EQ(42, client.testReplaySessionId());
}

TEST(ReplayerClientGapRecovery, ReplayingForAnotherClientIdIsIgnored)
{
    ReplayerClient client{1, [](const SequencedEvent&) {}};

    deliverControl(client, encodeReplaying(/*clientId=*/2, /*replaySessionId=*/42, /*catchUpPosition=*/1'000));

    EXPECT_EQ(-1, client.testReplaySessionId())
        << "a reply on the shared control stream addressed to a different replica must not be applied";
    EXPECT_FALSE(client.isCaughtUp());
}

TEST(ReplayerClientGapRecovery, SegmentCompleteAdvancesTheWalkAndReRequestsTheNextSegment)
{
    ReplayerClient client{1, [](const SequencedEvent&) {}};
    deliverControl(client, encodeReplaying(/*clientId=*/1, /*replaySessionId=*/7, /*catchUpPosition=*/500));
    ASSERT_EQ(7, client.testReplaySessionId());

    client.testCompleteReplaySegment();

    EXPECT_EQ(1, client.testWalkSegmentIndex()) << "segment 0 done -> walk advances to segment 1";
    EXPECT_TRUE(client.testIsAwaitingReplay()) << "advancing re-requests the next segment";
    EXPECT_EQ(-1, client.testReplaySessionId()) << "no session until the Replayer answers the new request";
}

TEST(ReplayerClientGapRecovery, ReplayPendingHoldsAtTheGapWithoutAssigningASession)
{
    int delivered = 0;
    ReplayerClient client{1, [&](const SequencedEvent&) { ++delivered; }};
    deliverLive(client, 1);
    deliverLive(client, 5);  // gap -> awaiting a replay
    ASSERT_TRUE(client.testIsAwaitingReplay());

    deliverControl(client, encodeReplayPending(/*clientId=*/1));

    EXPECT_TRUE(client.testIsAwaitingReplay()) << "no free Replayer slot -> keep holding at the gap";
    EXPECT_EQ(-1, client.testReplaySessionId());
    EXPECT_EQ(1, delivered) << "must not advance past the hole while pending";
}

}  // namespace
}  // namespace org::limitless::phixeron::sequencer
