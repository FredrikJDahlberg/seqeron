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

}  // namespace
}  // namespace org::limitless::phixeron::sequencer
