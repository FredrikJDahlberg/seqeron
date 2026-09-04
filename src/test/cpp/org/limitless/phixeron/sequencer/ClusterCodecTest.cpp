//
// Round-trip tests for the codec the cluster tier itself carries: the sbe-frame.xml envelope, both ways
// round. The application families have schemas of their own — see src/test/cpp/.../order/OrderCodecTest.cpp
// and src/test/cpp/.../fix/SessionCodecTest.cpp.
//
#include <gtest/gtest.h>

#include <array>
#include <cstdint>

#include "org/limitless/phixeron/sequencer/SequencedFrame.hpp"
#include "org_limitless_phixeron_sbe_frame/ClientConnected.h"
#include "org_limitless_phixeron_sbe_frame/ClientDisconnected.h"
#include "org_limitless_phixeron_sbe_frame/MessageHeader.h"
#include "org_limitless_phixeron_sbe_frame/Sequenced.h"
#include "org_limitless_phixeron_sbe_frame/Unsequenced.h"

namespace frm = org::limitless::phixeron::sbe::frame;
namespace sequencer = org::limitless::phixeron::sequencer;

// ── sbe-frame.xml ─────────────────────────────────────────────────────────
//
// The TCP lifecycle events are core payloads, and a core payload carries no header of its own: the
// identity is the frame's. These check that the envelope round-trips both ways round, which is what
// every consumer's dispatch rests on.

TEST(FrameCodec, ClientConnectedRoundTripsInsideAnUnsequencedFrame)
{
    alignas(16) std::array<std::uint8_t, 64> payload{};
    frm::ClientConnected enc;
    enc.wrapAndApplyHeader(reinterpret_cast<char*>(payload.data()), 0, payload.size());
    enc.putConnectionData(nullptr, 0);
    const auto payloadLength = static_cast<std::uint16_t>(frm::MessageHeader::encodedLength() + enc.encodedLength());

    alignas(16) std::array<std::uint8_t, 128> buffer{};
    frm::Unsequenced frame;
    frame.wrapAndApplyHeader(reinterpret_cast<char*>(buffer.data()), 0, buffer.size());
    frame.header().sourceId(77).connectionId(5).sessionId(88).payloadId(sequencer::CORE_PAYLOAD_ID);
    frame.putPayload(reinterpret_cast<const char*>(payload.data()), payloadLength);

    const auto view = sequencer::unwrapFrame(reinterpret_cast<const char*>(buffer.data()),
                                             frm::MessageHeader::encodedLength() + frame.encodedLength());
    ASSERT_FALSE(view.valid) << "an Unsequenced frame is not a tap frame; only Sequenced is";
}

TEST(FrameCodec, ClientDisconnectedRoundTripsInsideASequencedFrame)
{
    alignas(16) std::array<std::uint8_t, 64> payload{};
    frm::ClientDisconnected enc;
    enc.wrapAndApplyHeader(reinterpret_cast<char*>(payload.data()), 0, payload.size());
    const auto payloadLength = static_cast<std::uint16_t>(frm::MessageHeader::encodedLength() + enc.encodedLength());

    alignas(16) std::array<std::uint8_t, 128> buffer{};
    frm::Sequenced frame;
    frame.wrapAndApplyHeader(reinterpret_cast<char*>(buffer.data()), 0, buffer.size());
    frame.header()
        .sourceId(99)
        .connectionId(7)
        .sessionId(100)
        .payloadId(sequencer::CORE_PAYLOAD_ID)
        .globalSeqNo(42)
        .timestamp(1700000000000LL);
    frame.putPayload(reinterpret_cast<const char*>(payload.data()), payloadLength);

    const auto view = sequencer::unwrapFrame(reinterpret_cast<const char*>(buffer.data()),
                                             frm::MessageHeader::encodedLength() + frame.encodedLength());
    ASSERT_TRUE(view.valid);
    EXPECT_EQ(sequencer::CORE_PAYLOAD_ID, view.payloadId);
    EXPECT_EQ(frm::ClientDisconnected::sbeTemplateId(), view.templateId);
    EXPECT_EQ(99, view.sourceId);
    EXPECT_EQ(7, view.connectionId);
    EXPECT_EQ(100, view.sessionId);
    EXPECT_EQ(42, view.globalSeqNo);
    EXPECT_EQ(1700000000000LL, view.timestamp);
    EXPECT_EQ(payloadLength, view.payloadLength);
}
