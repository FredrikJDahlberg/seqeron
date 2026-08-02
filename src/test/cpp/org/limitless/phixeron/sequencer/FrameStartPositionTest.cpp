// frameStartPosition (ClusterStreamClient.hpp), pinned against Aeron's own Header semantics. Both
// stream clients — ClusterStreamClient and ReplayerClient — derive SequencedEvent::position with it.
//
// SequencedEvent::position must be the stream position of the frame's FIRST byte: FixConnection
// checkpoints it in m_seqOffsetIndex and later hands it to ReplayParams::position() to seek an
// archive-backed resend. A position past the target restarts the scan too late, finds nothing, and
// the resend GapFills messages that are sitting in the recording — the same silent-loss shape as the
// seek-index bug in doc/review-2026-07-25.md #6.
//
// The headers are fabricated rather than produced by a live subscription because the C++ suite runs
// no Aeron media driver (see CLAUDE.md). That is the point of the test: aeron_header_position's
// contract is what frameStartPosition is written against, so an Aeron bump that changes it should
// fail here rather than in a resend.

#include <gtest/gtest.h>

#include "aeron_image.h"  // aeron_header_t / aeron_data_header_t layout, to build a header by hand
#include "org/limitless/phixeron/sequencer/ClusterStreamClient.hpp"

namespace {

using org::limitless::phixeron::sequencer::frameStartPosition;

constexpr std::int32_t DATA_HEADER_LENGTH = 32;  // AERON_DATA_HEADER_LENGTH
constexpr std::int32_t TERM_ID = 7;
constexpr std::int32_t INITIAL_TERM_ID = 7;         // termCount 0, so position == termOffset
constexpr std::size_t POSITION_BITS_TO_SHIFT = 16;  // 64 KiB terms

// A frame header as a poll handler sees it. `fragmentedFrameLength` is AERON_NULL_VALUE on every
// ordinary frame and only set to the on-wire total by the FragmentAssembler's completed header.
struct Frame {
    aeron_data_header_t data{};
    aeron_header_t header{};

    Frame(const std::int32_t termOffset, const std::int32_t frameLength,
          const std::int32_t fragmentedFrameLength = AERON_NULL_VALUE)
    {
        data.frame_header.frame_length = frameLength;
        data.term_offset = termOffset;
        data.term_id = TERM_ID;
        header.frame = &data;
        header.initial_term_id = INITIAL_TERM_ID;
        header.position_bits_to_shift = POSITION_BITS_TO_SHIFT;
        header.fragmented_frame_length = fragmentedFrameLength;
    }

    aeron::Header wrap()
    {
        return aeron::Header{&header};
    }
};

// The formula frameStartPosition replaced, kept here so each test can show what it would have said.
std::int64_t legacyPosition(const aeron::Header& header)
{
    return header.position() - header.frameLength();
}

// A 32-byte-aligned payload is the one case the old subtraction got right, so it pins that
// frameStartPosition did not change the answer where the answer was already correct.
TEST(FrameStartPosition, AnAlignedFrameStartsWhereBothFormulasAgree)
{
    Frame frame{/*termOffset=*/1024, /*frameLength=*/DATA_HEADER_LENGTH + 96};  // 128, 32-aligned
    const aeron::Header header = frame.wrap();

    EXPECT_EQ(1024, frameStartPosition(header));
    EXPECT_EQ(1024, legacyPosition(header));
}

// SBE messages are arbitrary lengths, so this — not the aligned case — is what every real frame
// looks like. Header::position() reports the *next* frame's position (the end rounded up to the
// 32-byte alignment), so subtracting the unaligned frameLength() overshoots by the padding.
TEST(FrameStartPosition, AnUnalignedFrameOvershootsUnderTheOldSubtraction)
{
    Frame frame{/*termOffset=*/1024, /*frameLength=*/DATA_HEADER_LENGTH + 100};  // 132 → padded to 160
    const aeron::Header header = frame.wrap();

    EXPECT_EQ(1024, frameStartPosition(header));
    EXPECT_EQ(1184, header.position()) << "position() is the next frame's, not this one's end";
    EXPECT_EQ(1052, legacyPosition(header)) << "28 bytes past the frame, and not frame-aligned";
}

// A reassembled message: the header is the first fragment's, with frameLength() rewritten to the
// assembled length while position() has advanced past the last fragment. Two fragments of a ~8 KiB
// message over an 8 KiB IPC MTU is the case that motivated the FragmentAssembler.
TEST(FrameStartPosition, AReassembledMessageStartsAtItsFirstFragment)
{
    constexpr std::int32_t assembledLength = DATA_HEADER_LENGTH + 8320;  // 8352
    constexpr std::int32_t onWireLength = 8192 + 192;                    // two fragments, each header-prefixed
    Frame frame{/*termOffset=*/4096, assembledLength, /*fragmentedFrameLength=*/onWireLength};
    const aeron::Header header = frame.wrap();

    EXPECT_EQ(4096, frameStartPosition(header));
    EXPECT_EQ(4096 + onWireLength, header.position()) << "position() spans every fragment";
    EXPECT_GT(legacyPosition(header), 4096) << "the old subtraction seeks past the frame it names";
}

// The whole reason position matters: it is a replay start, and Aeron replays from frame boundaries.
TEST(FrameStartPosition, EveryDerivedPositionIsFrameAligned)
{
    for (std::int32_t payload = 1; payload <= 64; ++payload)
    {
        Frame frame{/*termOffset=*/2048, DATA_HEADER_LENGTH + payload};
        const aeron::Header header = frame.wrap();
        EXPECT_EQ(0, frameStartPosition(header) % 32) << "payload " << payload;
    }
}

// Term offset alone is not the position: a frame in a later term is that many term lengths along.
TEST(FrameStartPosition, ALaterTermAdvancesThePositionByWholeTerms)
{
    Frame frame{/*termOffset=*/512, DATA_HEADER_LENGTH + 64};
    frame.data.term_id = INITIAL_TERM_ID + 3;
    const aeron::Header header = frame.wrap();

    EXPECT_EQ((std::int64_t{3} << POSITION_BITS_TO_SHIFT) + 512, frameStartPosition(header));
}

}  // namespace
