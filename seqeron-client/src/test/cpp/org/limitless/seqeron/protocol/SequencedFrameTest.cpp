// What unwrapFrame does with a fragment that is not a whole frame. The sequencer validates every frame
// on ingress and the recording is what it wrote, so a rejection here means the recording itself is
// damaged — but the read must fail as an invalid FrameView, never as a throw or a read past the
// fragment: Image::poll hands whatever a fragment handler throws to the Aeron error handler and advances
// the subscriber position anyway, so a throw here loses the frame silently.
//
// The Java twin is SequencedFrameDecoderTest.java over SequencedFrameDecoder, case for case in the same
// order, so a divergence shows up as a missing case rather than as a decode failure on a live tap. Its
// one extra case is the offset one: wrap() takes an offset where unwrapFrame takes a pointer.

#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <string>
#include <utility>
#include <vector>

#include "org/limitless/seqeron/protocol/SequencedFrame.hpp"
#include "org_limitless_seqeron_sbe_frame/ClusterHeartbeat.h"
#include "org_limitless_seqeron_sbe_frame/MessageHeader.h"
#include "org_limitless_seqeron_sbe_frame/Sequenced.h"
#include "org_limitless_seqeron_sbe_frame/SequencedHeader.h"
#include "org_limitless_seqeron_sbe_frame/SequencedSystem.h"
#include "org_limitless_seqeron_sbe_frame/SequencedSystemHeader.h"

namespace frm = org::limitless::seqeron::sbe::frame;

namespace org::limitless::seqeron::protocol {
namespace {

constexpr std::int32_t SOURCE_ID = 7;
constexpr std::int32_t CONNECTION_ID = 42;
constexpr std::int64_t SESSION_ID = 0x5EE51000LL;
constexpr std::int64_t TIMESTAMP = 1700000000000LL;
constexpr std::uint16_t PAYLOAD_ID = 2;

// Offsets inside the outer MessageHeader.
constexpr std::uint64_t TEMPLATE_ID_OFFSET = 2;
constexpr std::uint64_t SCHEMA_ID_OFFSET = 4;

// Offset of the payload's length prefix: past the framing header and the 34-byte composite.
constexpr std::uint64_t PREFIX_OFFSET = frm::MessageHeader::encodedLength() + frm::SequencedHeader::encodedLength();

std::vector<char> filler(std::size_t length)
{
    std::vector<char> bytes(length);
    for (std::size_t i = 0; i < length; ++i)
    {
        bytes[i] = static_cast<char>(i * 31 + 7);
    }
    return bytes;
}

std::vector<char> payloadFrame(std::int64_t globalSeqNo, std::uint16_t payloadLength)
{
    const std::vector<char> payload = filler(payloadLength);
    std::vector<char> out(payloadLength + 64, 0);
    frm::Sequenced frame;
    frame.wrapAndApplyHeader(out.data(), 0, out.size());
    frame.header()
        .sourceId(SOURCE_ID)
        .connectionId(CONNECTION_ID)
        .sessionId(SESSION_ID)
        .payloadId(PAYLOAD_ID)
        .globalSeqNo(globalSeqNo)
        .timestamp(TIMESTAMP);
    frame.putPayload(payload.data(), payloadLength);
    out.resize(frm::MessageHeader::encodedLength() + frame.encodedLength());
    return out;
}

std::vector<char> systemFrame(std::int64_t globalSeqNo, std::uint16_t systemEventType, std::uint16_t bodyLength)
{
    const std::vector<char> body = filler(bodyLength);
    std::vector<char> out(bodyLength + 64, 0);
    frm::SequencedSystem frame;
    frame.wrapAndApplyHeader(out.data(), 0, out.size());
    frame.header()
        .sourceId(SOURCE_ID)
        .connectionId(CONNECTION_ID)
        .sessionId(SESSION_ID)
        .systemEventType(systemEventType)
        .globalSeqNo(globalSeqNo)
        .timestamp(TIMESTAMP);
    frame.putBody(body.data(), bodyLength);
    out.resize(frm::MessageHeader::encodedLength() + frame.encodedLength());
    return out;
}

std::vector<char> clusterHeartbeatFrame(std::int64_t globalSeqNo)
{
    std::vector<char> out(128, 0);
    frm::ClusterHeartbeat frame;
    frame.wrapAndApplyHeader(out.data(), 0, out.size());
    frame.header()
        .sourceId(-1)
        .connectionId(-1)
        .sessionId(-1)
        .systemEventType(CLUSTER_HEARTBEAT)
        .globalSeqNo(globalSeqNo)
        .timestamp(TIMESTAMP);
    out.resize(frm::MessageHeader::encodedLength() + frame.encodedLength());
    return out;
}

// A fragment of exactly `length` bytes, so ASan sees any read past its end for what it is.
std::vector<char> cut(const std::vector<char>& frame, std::size_t length)
{
    return std::vector<char>(frame.begin(), frame.begin() + static_cast<std::ptrdiff_t>(length));
}

// SBE writes its length prefixes little-endian, which is the only byte order these codecs are built for.
void putPrefix(std::vector<char>& frame, std::uint64_t offset, std::uint16_t value)
{
    std::memcpy(frame.data() + offset, &value, sizeof(value));
}

TEST(SequencedFrame, ShorterThanAMessageHeader)
{
    const std::vector<char> frame = payloadFrame(1, 8);
    for (std::size_t length = 0; length < frm::MessageHeader::encodedLength(); ++length)
    {
        const std::vector<char> fragment = cut(frame, length);
        EXPECT_FALSE(unwrapFrame(fragment.data(), length).valid) << length << " bytes cannot name a template";
    }
}

TEST(SequencedFrame, ForeignSchemaId)
{
    std::vector<char> frame = payloadFrame(1, 8);
    putPrefix(frame, SCHEMA_ID_OFFSET, static_cast<std::uint16_t>(frm::Sequenced::sbeSchemaId() + 1));
    EXPECT_FALSE(unwrapFrame(frame.data(), frame.size()).valid)
        << "template ids are unique per schema, so the schema has to match first";
}

TEST(SequencedFrame, UnknownTemplateId)
{
    std::vector<char> frame = payloadFrame(1, 8);
    putPrefix(frame, TEMPLATE_ID_OFFSET, 999);
    EXPECT_FALSE(unwrapFrame(frame.data(), frame.size()).valid) << "only the five sequenced messages decode";
}

TEST(SequencedFrame, ApplicationFrameCutBeforeItsPrefix)
{
    const std::vector<char> frame = payloadFrame(1, 8);
    const std::size_t prefixEnd = PREFIX_OFFSET + frm::Sequenced::payloadHeaderLength();
    for (std::size_t length = frm::MessageHeader::encodedLength(); length < prefixEnd; ++length)
    {
        const std::vector<char> fragment = cut(frame, length);
        EXPECT_FALSE(unwrapFrame(fragment.data(), length).valid)
            << "the payload's length is unreadable at " << length << " bytes";
    }
    EXPECT_TRUE(unwrapFrame(frame.data(), frame.size()).valid) << "the whole frame still decodes";
}

TEST(SequencedFrame, ApplicationPayloadRunsPastTheFragment)
{
    std::vector<char> frame = payloadFrame(1, 8);
    putPrefix(frame, PREFIX_OFFSET, 9);
    EXPECT_FALSE(unwrapFrame(frame.data(), frame.size()).valid)
        << "a payload prefix is a claim about the fragment, not a fact";
}

TEST(SequencedFrame, SystemFrameCutBeforeItsPrefix)
{
    const std::vector<char> frame = systemFrame(1, CONNECTION_CLOSED, 0);
    const std::size_t prefixEnd = PREFIX_OFFSET + frm::SequencedSystem::bodyHeaderLength();
    for (std::size_t length = frm::MessageHeader::encodedLength(); length < prefixEnd; ++length)
    {
        const std::vector<char> fragment = cut(frame, length);
        EXPECT_FALSE(unwrapFrame(fragment.data(), length).valid)
            << "the payload's length is unreadable at " << length << " bytes";
    }
    EXPECT_TRUE(unwrapFrame(frame.data(), frame.size()).valid) << "an empty payload is a whole frame (§5)";
}

TEST(SequencedFrame, SystemBodyRunsPastTheFragment)
{
    std::vector<char> frame = systemFrame(1, CLUSTER_STARTED, 8);
    putPrefix(frame, PREFIX_OFFSET, 9);
    EXPECT_FALSE(unwrapFrame(frame.data(), frame.size()).valid);
}

TEST(SequencedFrame, SynthesizedFrameCutInsideItsHeader)
{
    const std::vector<char> frame = clusterHeartbeatFrame(1);
    const std::size_t headerEnd = frm::MessageHeader::encodedLength() + frm::SequencedSystemHeader::encodedLength();
    for (std::size_t length = frm::MessageHeader::encodedLength(); length < headerEnd; ++length)
    {
        const std::vector<char> fragment = cut(frame, length);
        EXPECT_FALSE(unwrapFrame(fragment.data(), length).valid)
            << "globalSeqNo is not readable at " << length << " bytes, so P-3 cannot count";
    }
    EXPECT_TRUE(unwrapFrame(frame.data(), frame.size()).valid);
}

// The property behind the cases above, swept over every case and every cut.
TEST(SequencedFrame, EveryTruncationIsRejectedOrStaysInsideTheFragment)
{
    const std::vector<std::pair<std::string, std::vector<char>>> cases{
        { "an empty payload", payloadFrame(1, 0) },
        { "a payload shorter than a MessageHeader", payloadFrame(2, 3) },
        { "a payload naming its own message", payloadFrame(3, 24) },
        { "a system event with no payload", systemFrame(4, CONNECTION_CLOSED, 0) },
        { "a system event with a payload", systemFrame(5, CLUSTER_STARTED, 8) },
        { "a synthesized ClusterHeartbeat", clusterHeartbeatFrame(6) },
    };

    for (const auto& [name, whole] : cases)
    {
        ASSERT_TRUE(unwrapFrame(whole.data(), whole.size()).valid) << name << " does not decode whole";
        for (std::size_t length = 0; length < whole.size(); ++length)
        {
            const std::vector<char> fragment = cut(whole, length);
            const std::string where =
                name + " cut to " + std::to_string(length) + " of " + std::to_string(whole.size()) + " bytes";
            FrameView view{};
            ASSERT_NO_THROW(view = unwrapFrame(fragment.data(), length)) << where << " throws out of the poll thread";
            if (view.valid)
            {
                const std::uint64_t payloadOffset = static_cast<std::uint64_t>(view.payload - fragment.data());
                EXPECT_LE(payloadOffset + view.payloadLength, length)
                    << where << " decodes with a payload running past the fragment";
            }
        }
    }
}

} // namespace
} // namespace org::limitless::seqeron::protocol
