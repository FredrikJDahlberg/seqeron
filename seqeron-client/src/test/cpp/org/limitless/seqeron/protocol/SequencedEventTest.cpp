// sequencedEventOf and lifecycleEventOf (SequencedFrame.hpp): every field of a delivered event traces to
// the frame that produced it, plus the two stamps the delivery adds. What unwrapFrame reads out of the
// bytes is SequencedFrameTest's; what is pinned here is the mapping out of a FrameView, which both stream
// clients share and neither asserts — a sourceId and connectionId swapped in the factory would otherwise
// pass the whole suite, and surface as a consumer replying down the wrong connection.
//
// Every constant below is distinct for that reason: two fields holding the same value would hide a swap.
//
// C++ only, with no Java twin: Java's SequencedEvent is a view over SequencedFrameDecoder and copies no
// field at all, so it has no mapping to get wrong.

#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <vector>

#include "org/limitless/seqeron/protocol/SequencedFrame.hpp"
#include "org_limitless_seqeron_sbe_frame/MessageHeader.h"
#include "org_limitless_seqeron_sbe_frame/Sequenced.h"
#include "org_limitless_seqeron_sbe_frame/SequencedSystem.h"

namespace frm = org::limitless::seqeron::sbe::frame;

namespace org::limitless::seqeron::protocol {
namespace {

constexpr std::int32_t SOURCE_ID = 7;
constexpr std::int32_t CONNECTION_ID = 42;
constexpr std::int64_t SESSION_ID = 0x5EE51000LL;
constexpr std::int64_t TIMESTAMP = 1700000000000LL;
constexpr std::int64_t GLOBAL_SEQ_NO = 918;
constexpr std::uint16_t PAYLOAD_ID = 2;

// The payload's own declaration, which unwrapFrame reads off its 8-byte MessageHeader.
constexpr std::uint16_t PAYLOAD_BLOCK_LENGTH = 24;
constexpr std::uint16_t PAYLOAD_TEMPLATE_ID = 31;
constexpr std::uint16_t PAYLOAD_VERSION = 3;

// The two stamps the delivery adds rather than the frame carrying them.
constexpr std::int64_t RECEIVE_TIME_NS = 1234567890123LL;
constexpr std::int64_t POSITION = 98304;

// An application frame whose payload declares itself, so templateId/blockLength/version are readable
// rather than filler.
std::vector<char> payloadFrame()
{
    std::vector<char> payload(frm::MessageHeader::encodedLength() + PAYLOAD_BLOCK_LENGTH, 0);
    frm::MessageHeader declaration;
    declaration.wrap(payload.data(), 0, frm::MessageHeader::sbeSchemaVersion(), payload.size());
    declaration.blockLength(PAYLOAD_BLOCK_LENGTH)
        .templateId(PAYLOAD_TEMPLATE_ID)
        .schemaId(frm::Sequenced::sbeSchemaId())
        .version(PAYLOAD_VERSION);

    std::vector<char> out(payload.size() + 64, 0);
    frm::Sequenced frame;
    frame.wrapAndApplyHeader(out.data(), 0, out.size());
    frame.header()
        .sourceId(SOURCE_ID)
        .connectionId(CONNECTION_ID)
        .sessionId(SESSION_ID)
        .payloadId(PAYLOAD_ID)
        .globalSeqNo(GLOBAL_SEQ_NO)
        .timestamp(TIMESTAMP);
    frame.putPayload(payload.data(), static_cast<std::uint64_t>(payload.size()));
    out.resize(frm::MessageHeader::encodedLength() + frame.encodedLength());
    return out;
}

std::vector<char> systemFrame(std::uint16_t systemEventType, std::uint16_t bodyLength)
{
    const std::vector<char> body(bodyLength, 0x5A);
    std::vector<char> out(bodyLength + 64, 0);
    frm::SequencedSystem frame;
    frame.wrapAndApplyHeader(out.data(), 0, out.size());
    frame.header()
        .sourceId(SOURCE_ID)
        .connectionId(CONNECTION_ID)
        .sessionId(SESSION_ID)
        .systemEventType(systemEventType)
        .globalSeqNo(GLOBAL_SEQ_NO)
        .timestamp(TIMESTAMP);
    frame.putBody(body.data(), bodyLength);
    out.resize(frm::MessageHeader::encodedLength() + frame.encodedLength());
    return out;
}

TEST(SequencedEventFactory, ApplicationFrameFieldByField)
{
    std::vector<char> frame = payloadFrame();
    const FrameView view = unwrapFrame(frame.data(), frame.size());
    ASSERT_TRUE(view.valid);

    const SequencedEvent event = sequencedEventOf(view, RECEIVE_TIME_NS, POSITION);

    EXPECT_EQ(GLOBAL_SEQ_NO, event.globalSeqNo);
    EXPECT_EQ(SOURCE_ID, event.sourceId);
    EXPECT_EQ(CONNECTION_ID, event.connectionId);
    EXPECT_EQ(SESSION_ID, event.sourceSessionId);
    EXPECT_EQ(TIMESTAMP, event.clusterTimestampNs);
    EXPECT_EQ(RECEIVE_TIME_NS, event.receiveTimeNs);
    EXPECT_FALSE(event.system);
    EXPECT_EQ(PAYLOAD_ID, event.payloadId);
    EXPECT_EQ(0, event.systemEventType);
    EXPECT_EQ(PAYLOAD_TEMPLATE_ID, event.templateId);
    EXPECT_EQ(PAYLOAD_BLOCK_LENGTH, event.blockLength);
    EXPECT_EQ(PAYLOAD_VERSION, event.version);
    EXPECT_EQ(POSITION, event.position);

    // The payload is addressed in place, its own MessageHeader included — not copied.
    EXPECT_EQ(view.payload, event.payload);
    EXPECT_EQ(view.payloadLength, event.payloadLength);
    EXPECT_EQ(frm::MessageHeader::encodedLength() + PAYLOAD_BLOCK_LENGTH, event.payloadLength);
}

TEST(SequencedEventFactory, SystemFrameNamesItsEventAndNoPayloadId)
{
    std::vector<char> frame = systemFrame(GATEWAY_STARTED, 8);
    const FrameView view = unwrapFrame(frame.data(), frame.size());
    ASSERT_TRUE(view.valid);

    const SequencedEvent event = sequencedEventOf(view, RECEIVE_TIME_NS, POSITION);

    EXPECT_TRUE(event.system);
    EXPECT_EQ(GATEWAY_STARTED, event.systemEventType);
    EXPECT_EQ(0, event.payloadId);
    EXPECT_EQ(GLOBAL_SEQ_NO, event.globalSeqNo);
    EXPECT_EQ(SOURCE_ID, event.sourceId);
    EXPECT_EQ(CONNECTION_ID, event.connectionId);
    EXPECT_EQ(SESSION_ID, event.sourceSessionId);
    EXPECT_EQ(TIMESTAMP, event.clusterTimestampNs);
    EXPECT_EQ(8u, event.payloadLength);
}

TEST(SequencedEventFactory, LifecycleEventCarriesTheIdentityAndNoBody)
{
    std::vector<char> frame = systemFrame(CONNECTION_OPENED, 8);
    const FrameView view = unwrapFrame(frame.data(), frame.size());
    ASSERT_TRUE(view.valid);

    const LifecycleEvent event = lifecycleEventOf(view, RECEIVE_TIME_NS);

    EXPECT_EQ(GLOBAL_SEQ_NO, event.globalSeqNo);
    EXPECT_EQ(SOURCE_ID, event.sourceId);
    EXPECT_EQ(CONNECTION_ID, event.connectionId);
    EXPECT_EQ(SESSION_ID, event.sourceSessionId);
    EXPECT_EQ(TIMESTAMP, event.clusterTimestampNs);
    EXPECT_EQ(RECEIVE_TIME_NS, event.receiveTimeNs);
}

} // namespace
} // namespace org::limitless::seqeron::protocol
