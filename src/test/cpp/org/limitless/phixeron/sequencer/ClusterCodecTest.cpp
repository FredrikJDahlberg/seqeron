//
// Round-trip tests for the codec the cluster tier itself carries: the sbe-frame.xml envelopes, both ways
// round. The application families have schemas of their own — see src/test/cpp/.../order/OrderCodecTest.cpp
// and src/test/cpp/.../fix/SessionCodecTest.cpp.
//
#include <gtest/gtest.h>

#include <array>
#include <cstdint>

#include "org/limitless/phixeron/sequencer/SequencedFrame.hpp"
#include "org_limitless_phixeron_sbe_frame/ConnectionOpened.h"
#include "org_limitless_phixeron_sbe_frame/ConnectionClosed.h"
#include "org_limitless_phixeron_sbe_frame/ClusterHeartbeat.h"
#include "org_limitless_phixeron_sbe_frame/MessageHeader.h"
#include "org_limitless_phixeron_sbe_frame/Sequenced.h"
#include "org_limitless_phixeron_sbe_frame/SequencedSystem.h"
#include "org_limitless_phixeron_sbe_frame/Unsequenced.h"
#include "org_limitless_phixeron_sbe_frame/UnsequencedSystem.h"

namespace frm = org::limitless::phixeron::sbe::frame;
namespace sequencer = org::limitless::phixeron::sequencer;

// ── sbe-frame.xml ─────────────────────────────────────────────────────────
//
// The TCP lifecycle events are system messages, and a system message carries no framing of its own: the
// identity is the frame's and header.systemEventType is what names it. These check that both envelopes
// round-trip, which is what every consumer's dispatch rests on.

TEST(FrameCodec, ConnectionOpenedRoundTripsInsideAnUnsequencedSystemFrame)
{
    alignas(16) std::array<std::uint8_t, 64> body{};
    frm::ConnectionOpened enc;
    enc.wrapForEncode(reinterpret_cast<char*>(body.data()), 0, body.size());
    enc.putConnectionData(nullptr, 0);

    alignas(16) std::array<std::uint8_t, 128> buffer{};
    frm::UnsequencedSystem frame;
    frame.wrapAndApplyHeader(reinterpret_cast<char*>(buffer.data()), 0, buffer.size());
    frame.header().sourceId(77).connectionId(5).sessionId(88).systemEventType(sequencer::CONNECTION_OPENED);
    frame.putBody(reinterpret_cast<const char*>(body.data()), static_cast<std::uint16_t>(enc.encodedLength()));

    const auto view = sequencer::unwrapFrame(reinterpret_cast<const char*>(buffer.data()),
                                             frm::MessageHeader::encodedLength() + frame.encodedLength());
    ASSERT_FALSE(view.valid) << "an ingress frame is not a tap frame; only the sequenced shapes are";
}

TEST(FrameCodec, ConnectionClosedRoundTripsInsideASequencedSystemFrame)
{
    alignas(16) std::array<std::uint8_t, 64> body{};
    frm::ConnectionClosed enc;
    enc.wrapForEncode(reinterpret_cast<char*>(body.data()), 0, body.size());
    const auto bodyLength = static_cast<std::uint16_t>(enc.encodedLength());

    alignas(16) std::array<std::uint8_t, 128> buffer{};
    frm::SequencedSystem frame;
    frame.wrapAndApplyHeader(reinterpret_cast<char*>(buffer.data()), 0, buffer.size());
    frame.header()
        .sourceId(99)
        .connectionId(7)
        .sessionId(100)
        .systemEventType(sequencer::CONNECTION_CLOSED)
        .globalSeqNo(42)
        .timestamp(1700000000000LL);
    frame.putBody(reinterpret_cast<const char*>(body.data()), bodyLength);

    const auto view = sequencer::unwrapFrame(reinterpret_cast<const char*>(buffer.data()),
                                             frm::MessageHeader::encodedLength() + frame.encodedLength());
    ASSERT_TRUE(view.valid);
    EXPECT_TRUE(view.system);
    EXPECT_EQ(sequencer::CONNECTION_CLOSED, view.systemEventType);
    EXPECT_EQ(0, view.payloadId) << "payloadId means nothing on a system frame";
    EXPECT_EQ(99, view.sourceId);
    EXPECT_EQ(7, view.connectionId);
    EXPECT_EQ(100, view.sessionId);
    EXPECT_EQ(42, view.globalSeqNo);
    EXPECT_EQ(1700000000000LL, view.timestamp);
    EXPECT_EQ(bodyLength, view.payloadLength);
}

TEST(FrameCodec, ApplicationPayloadRoundTripsInsideASequencedFrame)
{
    // An application payload keeps its own MessageHeader: the frame layer neither knows nor decodes what
    // payloadId names (S-2), so the payload's framing is what a consumer verifies it against (P-4).
    alignas(16) std::array<std::uint8_t, 64> payload{ 0 };
    frm::MessageHeader payloadHdr;
    payloadHdr.wrap(reinterpret_cast<char*>(payload.data()), 0, 0, payload.size());
    payloadHdr.blockLength(12).templateId(34).schemaId(230).version(0);
    const std::uint16_t payloadLength = frm::MessageHeader::encodedLength() + 12;

    alignas(16) std::array<std::uint8_t, 128> buffer{};
    frm::Sequenced frame;
    frame.wrapAndApplyHeader(reinterpret_cast<char*>(buffer.data()), 0, buffer.size());
    frame.header().sourceId(3).connectionId(9).sessionId(11).payloadId(3).globalSeqNo(7).timestamp(1234);
    frame.putPayload(reinterpret_cast<const char*>(payload.data()), payloadLength);

    const auto view = sequencer::unwrapFrame(reinterpret_cast<const char*>(buffer.data()),
                                             frm::MessageHeader::encodedLength() + frame.encodedLength());
    ASSERT_TRUE(view.valid);
    EXPECT_FALSE(view.system);
    EXPECT_EQ(3, view.payloadId);
    EXPECT_EQ(0, view.systemEventType) << "systemEventType means nothing on an application frame";
    EXPECT_EQ(34, view.templateId);
    EXPECT_EQ(12, view.blockLength);
    EXPECT_EQ(payloadLength, view.payloadLength);
}

TEST(FrameCodec, ClusterHeartbeatCarriesItsFieldsInline)
{
    // One of the three the sequencer synthesizes: a template of its own, no body at all, and 42 bytes —
    // the cheapest frame in the system.
    alignas(16) std::array<std::uint8_t, 128> buffer{};
    frm::ClusterHeartbeat frame;
    frame.wrapAndApplyHeader(reinterpret_cast<char*>(buffer.data()), 0, buffer.size());
    frame.header()
        .sourceId(-1)
        .connectionId(-1)
        .sessionId(-1)
        .systemEventType(sequencer::CLUSTER_HEARTBEAT)
        .globalSeqNo(5)
        .timestamp(1700000000001LL);
    const auto length = frm::MessageHeader::encodedLength() + frame.encodedLength();
    EXPECT_EQ(42U, length);

    const auto view = sequencer::unwrapFrame(reinterpret_cast<const char*>(buffer.data()), length);
    ASSERT_TRUE(view.valid);
    EXPECT_TRUE(view.system);
    EXPECT_EQ(sequencer::CLUSTER_HEARTBEAT, view.systemEventType);
    EXPECT_EQ(-1, view.sourceId);
    EXPECT_EQ(5, view.globalSeqNo);
    EXPECT_EQ(1700000000001LL, view.timestamp);
}
