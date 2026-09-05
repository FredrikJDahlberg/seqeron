//
// The C++ half of the protocol conformance suite — doc/seqeron-protocol-spec.md §14.
//
// §14 asks for the suite in both languages, but only the rows with a C++ implementation behind them can
// be mirrored: there is no C++ sequencer, so rows 1, 4, 4a, 5, 8 and 9 (which drive Sequencer's
// validation, synthesis and promotion) live in the Java suite alone. What is here is what C++ actually
// implements — the schema itself (row 2), the consumer's decode of every system shape (row 3), the
// producer's T-3 refusal (row 4b), the boundary payload sizes (row 6) and selective consumption (row 7).
//
// Every frame is built here, a few lines above the assertion that reads it, and every expectation is a
// rule §4 states: the offsets of §4.1, the sizes of §4.2, the bodies of §7. The wire format's assumptions
// live in the spec, so a test names the rule it is checking and fails saying which one broke.
//
#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <cstring>
#include <span>
#include <string>
#include <vector>

#include "org/limitless/phixeron/sequencer/ClusterStreamSender.hpp"
#include "org/limitless/phixeron/sequencer/IngressPublisher.hpp"
#include "org/limitless/phixeron/sequencer/SequencedFrame.hpp"
#include "org_limitless_phixeron_sbe_frame/ClientConnected.h"
#include "org_limitless_phixeron_sbe_frame/ClusterStarted.h"
#include "org_limitless_phixeron_sbe_frame/ClusterHeartbeat.h"
#include "org_limitless_phixeron_sbe_frame/GatewayActive.h"
#include "org_limitless_phixeron_sbe_frame/GatewayRegistered.h"
#include "org_limitless_phixeron_sbe_frame/GatewayStarted.h"
#include "org_limitless_phixeron_sbe_frame/LeadershipChanged.h"
#include "org_limitless_phixeron_sbe_frame/MessageHeader.h"
#include "org_limitless_phixeron_sbe_frame/PayloadIdRegistered.h"
#include "org_limitless_phixeron_sbe_frame/Sequenced.h"
#include "org_limitless_phixeron_sbe_frame/SequencedHeader.h"
#include "org_limitless_phixeron_sbe_frame/SequencedSystem.h"
#include "org_limitless_phixeron_sbe_frame/SequencedSystemHeader.h"
#include "org_limitless_phixeron_sbe_frame/UnsequencedHeader.h"
#include "org_limitless_phixeron_sbe_frame/UnsequencedSystemHeader.h"

namespace frm = org::limitless::phixeron::sbe::frame;

namespace org::limitless::phixeron::sequencer {
namespace {

constexpr std::int32_t SOURCE_ID = 7;
constexpr std::int32_t CONNECTION_ID = 42;
constexpr std::int64_t SESSION_ID = 0x5EE51000LL;
constexpr std::int64_t TIMESTAMP = 1700000000000LL;

// ── Fixture: the frame shapes, built here ─────────────────────────────────────
//
// Every frame these tests read is encoded a few lines above the assertion that reads it. The rules they
// check are §4.1's offsets, §4.2's sizes and §7's bodies — the spec states each one, so a test asserts
// it directly rather than against a stored copy of some earlier build's output.

// One SequencedSystem frame. `fill` writes the body and returns its length.
template<typename Fill>
std::vector<std::uint8_t> systemFrame(std::int64_t globalSeqNo, std::uint16_t systemEventType, Fill&& fill)
{
    std::array<char, 256> body{};
    const std::uint16_t bodyLength = fill(body.data(), body.size());

    std::vector<std::uint8_t> out(body.size() + 64, 0);
    frm::SequencedSystem frame;
    frame.wrapAndApplyHeader(reinterpret_cast<char*>(out.data()), 0, out.size());
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

// One of the three the sequencer synthesizes: its own template, fields inline, -1 identity (F-4).
template<typename Encoder, typename Fill>
std::vector<std::uint8_t> synthesizedFrame(std::int64_t globalSeqNo, std::uint16_t systemEventType, Fill&& fill)
{
    std::vector<std::uint8_t> out(128, 0);
    Encoder frame;
    frame.wrapAndApplyHeader(reinterpret_cast<char*>(out.data()), 0, out.size());
    frame.header()
        .sourceId(-1)
        .connectionId(-1)
        .sessionId(-1)
        .systemEventType(systemEventType)
        .globalSeqNo(globalSeqNo)
        .timestamp(TIMESTAMP);
    fill(frame);
    out.resize(frm::MessageHeader::encodedLength() + frame.encodedLength());
    return out;
}

// One application frame carrying `payloadLength` bytes the cluster tier never opens (S-2).
std::vector<std::uint8_t> payloadFrame(std::int64_t globalSeqNo, std::uint16_t payloadId, std::size_t payloadLength)
{
    std::vector<char> payload(payloadLength);
    for (std::size_t i = 0; i < payloadLength; ++i)
    {
        payload[i] = static_cast<char>(i * 31 + 7);
    }

    std::vector<std::uint8_t> out(payloadLength + 64, 0);
    frm::Sequenced frame;
    frame.wrapAndApplyHeader(reinterpret_cast<char*>(out.data()), 0, out.size());
    frame.header()
        .sourceId(SOURCE_ID)
        .connectionId(CONNECTION_ID)
        .sessionId(SESSION_ID)
        .payloadId(payloadId)
        .globalSeqNo(globalSeqNo)
        .timestamp(TIMESTAMP);
    frame.putPayload(payload.data(), static_cast<std::uint16_t>(payloadLength));
    out.resize(frm::MessageHeader::encodedLength() + frame.encodedLength());
    return out;
}

FrameView viewOf(const std::vector<std::uint8_t>& frame)
{
    return unwrapFrame(reinterpret_cast<const char*>(frame.data()), frame.size());
}

// ── Row 2. The prefix property (F-3) ──────────────────────────────────────────

TEST(Conformance, IngressCompositesAreThePrefixOfTheirSequencedCounterparts)
{
    EXPECT_EQ(18U, frm::UnsequencedHeader::encodedLength());
    EXPECT_EQ(18U, frm::UnsequencedSystemHeader::encodedLength());
    EXPECT_EQ(34U, frm::SequencedHeader::encodedLength());
    EXPECT_EQ(34U, frm::SequencedSystemHeader::encodedLength());

    alignas(16) std::array<std::uint8_t, 64> unsequenced{};
    frm::UnsequencedHeader a;
    a.wrap(reinterpret_cast<char*>(unsequenced.data()), 0, 0, unsequenced.size());
    a.sourceId(SOURCE_ID).connectionId(CONNECTION_ID).sessionId(SESSION_ID).payloadId(2);

    alignas(16) std::array<std::uint8_t, 64> unsequencedSystem{};
    frm::UnsequencedSystemHeader b;
    b.wrap(reinterpret_cast<char*>(unsequencedSystem.data()), 0, 0, unsequencedSystem.size());
    b.sourceId(SOURCE_ID).connectionId(CONNECTION_ID).sessionId(SESSION_ID).systemEventType(2);

    // The field at offset 16 is a payloadId on one and a systemEventType on the other; written to the
    // same bits, the two composites are the same 18 bytes.
    EXPECT_EQ(0, std::memcmp(unsequenced.data(), unsequencedSystem.data(), 18));

    alignas(16) std::array<std::uint8_t, 64> sequenced{};
    frm::SequencedHeader c;
    c.wrap(reinterpret_cast<char*>(sequenced.data()), 0, 0, sequenced.size());
    c.sourceId(SOURCE_ID)
        .connectionId(CONNECTION_ID)
        .sessionId(SESSION_ID)
        .payloadId(2)
        .globalSeqNo(9)
        .timestamp(TIMESTAMP);

    alignas(16) std::array<std::uint8_t, 64> sequencedSystem{};
    frm::SequencedSystemHeader d;
    d.wrap(reinterpret_cast<char*>(sequencedSystem.data()), 0, 0, sequencedSystem.size());
    d.sourceId(SOURCE_ID)
        .connectionId(CONNECTION_ID)
        .sessionId(SESSION_ID)
        .systemEventType(2)
        .globalSeqNo(9)
        .timestamp(TIMESTAMP);

    EXPECT_EQ(0, std::memcmp(sequenced.data(), sequencedSystem.data(), 34));
    EXPECT_EQ(0, std::memcmp(unsequenced.data(), sequenced.data(), 18)) << "the copy-18 of §9.5";
    EXPECT_EQ(0, std::memcmp(unsequencedSystem.data(), sequencedSystem.data(), 18));
}

TEST(Conformance, EveryFrameLayerFieldSitsAtTheOffsetSection41Gives)
{
    // §4.1's table, field by field. The prefix property above says the composites agree with each other;
    // this says they agree with the spec -- a permutation of two same-width fields passes the first and
    // fails here, which is the whole reason the offsets are written down rather than merely implied.
    EXPECT_EQ(0U, frm::UnsequencedHeader::sourceIdEncodingOffset());
    EXPECT_EQ(4U, frm::UnsequencedHeader::connectionIdEncodingOffset());
    EXPECT_EQ(8U, frm::UnsequencedHeader::sessionIdEncodingOffset());
    EXPECT_EQ(16U, frm::UnsequencedHeader::payloadIdEncodingOffset());

    EXPECT_EQ(0U, frm::UnsequencedSystemHeader::sourceIdEncodingOffset());
    EXPECT_EQ(4U, frm::UnsequencedSystemHeader::connectionIdEncodingOffset());
    EXPECT_EQ(8U, frm::UnsequencedSystemHeader::sessionIdEncodingOffset());
    EXPECT_EQ(16U, frm::UnsequencedSystemHeader::systemEventTypeEncodingOffset());

    EXPECT_EQ(0U, frm::SequencedHeader::sourceIdEncodingOffset());
    EXPECT_EQ(4U, frm::SequencedHeader::connectionIdEncodingOffset());
    EXPECT_EQ(8U, frm::SequencedHeader::sessionIdEncodingOffset());
    EXPECT_EQ(16U, frm::SequencedHeader::payloadIdEncodingOffset());
    EXPECT_EQ(18U, frm::SequencedHeader::globalSeqNoEncodingOffset());
    EXPECT_EQ(26U, frm::SequencedHeader::timestampEncodingOffset());

    EXPECT_EQ(0U, frm::SequencedSystemHeader::sourceIdEncodingOffset());
    EXPECT_EQ(4U, frm::SequencedSystemHeader::connectionIdEncodingOffset());
    EXPECT_EQ(8U, frm::SequencedSystemHeader::sessionIdEncodingOffset());
    EXPECT_EQ(16U, frm::SequencedSystemHeader::systemEventTypeEncodingOffset());
    EXPECT_EQ(18U, frm::SequencedSystemHeader::globalSeqNoEncodingOffset());
    EXPECT_EQ(26U, frm::SequencedSystemHeader::timestampEncodingOffset());
}

TEST(Conformance, FrameSizesAreSection42sTable)
{
    EXPECT_EQ(8U, frm::MessageHeader::encodedLength());
    EXPECT_EQ(2U, frm::Sequenced::payloadHeaderLength()) << "the var-data length prefix";
    EXPECT_EQ(2U, frm::SequencedSystem::bodyHeaderLength());

    // Fixed overhead: MessageHeader + the header composite + the length prefix.
    EXPECT_EQ(28U, MIN_INGRESS_LENGTH) << "ingress, both families";
    EXPECT_EQ(44U,
              frm::MessageHeader::encodedLength() + frm::SequencedHeader::encodedLength() +
                  frm::Sequenced::payloadHeaderLength())
        << "sequenced, both families";

    // A ClusterHeartbeat is a template of its own: no length prefix and no body at all.
    EXPECT_EQ(42U, synthesizedFrame<frm::ClusterHeartbeat>(1, CLUSTER_HEARTBEAT, [](auto&) {}).size())
        << "8 + 34, the cheapest frame in the system";

    // §12's ceiling, as a frame on the wire.
    EXPECT_EQ(44U + MAX_PAYLOAD_LENGTH, payloadFrame(1, 2, MAX_PAYLOAD_LENGTH).size());
}

// ── Row 3. Every system shape decodes to what it was built with (§7) ──────────

TEST(Conformance, EverySystemShapeNamesItsEventAndDecodesItsBody)
{
    const auto connected = systemFrame(1, CLIENT_CONNECTED, [](char* body, std::size_t cap) {
        frm::ClientConnected encoder;
        encoder.wrapForEncode(body, 0, cap);
        encoder.putConnectionData("GW-A/42", 7);
        return static_cast<std::uint16_t>(encoder.encodedLength());
    });
    auto view = viewOf(connected);
    ASSERT_TRUE(view.valid);
    EXPECT_TRUE(view.system);
    EXPECT_EQ(CLIENT_CONNECTED, view.systemEventType);
    EXPECT_EQ(0U, view.blockLength) << "a system body carries no declaration of its own (V-3)";

    const auto startedMarker = systemFrame(2, CLUSTER_STARTED, [](char* body, std::size_t cap) {
        frm::ClusterStarted encoder;
        encoder.wrapForEncode(body, 0, cap);
        encoder.correlationId(0x0102030405060708LL);
        return static_cast<std::uint16_t>(encoder.encodedLength());
    });
    view = viewOf(startedMarker);
    ASSERT_TRUE(view.valid);
    EXPECT_EQ(0x0102030405060708LL,
              decodeSystem<frm::ClusterStarted>(view.payload, view.payloadLength).correlationId());

    const auto registered = systemFrame(3, GATEWAY_REGISTERED, [](char* body, std::size_t cap) {
        frm::GatewayRegistered encoder;
        encoder.wrapForEncode(body, 0, cap);
        encoder.remaining(0).gatewayId(3).gatewaySourceId(5).preferenceRank(0);
        encoder.putGatewayName("EGW-A");
        return static_cast<std::uint16_t>(encoder.encodedLength());
    });
    view = viewOf(registered);
    ASSERT_TRUE(view.valid);
    EXPECT_EQ(GATEWAY_REGISTERED, view.systemEventType);
    auto row = decodeSystem<frm::GatewayRegistered>(view.payload, view.payloadLength);
    EXPECT_EQ(0, row.remaining());
    EXPECT_EQ(3, row.gatewayId());
    EXPECT_EQ(5, row.gatewaySourceId());

    const auto started = systemFrame(4, GATEWAY_STARTED, [](char* body, std::size_t cap) {
        frm::GatewayStarted encoder;
        encoder.wrapForEncode(body, 0, cap);
        encoder.gatewayId(3).firstConnectionId(1000);
        return static_cast<std::uint16_t>(encoder.encodedLength());
    });
    view = viewOf(started);
    ASSERT_TRUE(view.valid);
    auto start = decodeSystem<frm::GatewayStarted>(view.payload, view.payloadLength);
    EXPECT_EQ(3, start.gatewayId());
    EXPECT_EQ(1000, start.firstConnectionId());

    // The three synthesized ones: fields inline in the frame's own block, and -1 marking the class (F-4).
    view = viewOf(synthesizedFrame<frm::ClusterHeartbeat>(5, CLUSTER_HEARTBEAT, [](auto&) {}));
    ASSERT_TRUE(view.valid);
    EXPECT_EQ(CLUSTER_HEARTBEAT, view.systemEventType);
    EXPECT_EQ(-1, view.sourceId);
    EXPECT_EQ(-1, view.connectionId);
    EXPECT_EQ(-1, view.sessionId);

    const auto leadership = synthesizedFrame<frm::LeadershipChanged>(
        6, LEADERSHIP_CHANGED, [](frm::LeadershipChanged& f) { f.newLeaderMemberId(2); });
    view = viewOf(leadership);
    ASSERT_TRUE(view.valid);
    EXPECT_EQ(LEADERSHIP_CHANGED, view.systemEventType);
    EXPECT_EQ(2, decodeSystem<frm::LeadershipChanged>(view.payload, view.payloadLength).newLeaderMemberId());

    const auto active =
        synthesizedFrame<frm::GatewayActive>(7, GATEWAY_ACTIVE, [](frm::GatewayActive& f) { f.gatewayId(3); });
    view = viewOf(active);
    ASSERT_TRUE(view.valid);
    EXPECT_EQ(GATEWAY_ACTIVE, view.systemEventType);
    EXPECT_EQ(3, decodeSystem<frm::GatewayActive>(view.payload, view.payloadLength).gatewayId());
}

// ── Row 4b. The producer refuses before the wire (T-3, §12) ───────────────────

// Captures every frame offered, so the test can see that a refusal offered nothing at all.
class RecordingIngress : public IngressTransport
{
  public:
    std::vector<std::vector<std::uint8_t>> m_offered;

    bool offer(std::span<const std::uint8_t> bytes) override
    {
        m_offered.emplace_back(bytes.begin(), bytes.end());
        return true;
    }
};

class ConnectedSender : public ::testing::Test
{
  protected:
    void SetUp() override
    {
        auto egress = std::make_unique<FakeEgress>();
        auto ingress = std::make_unique<RecordingIngress>();
        m_ingress = ingress.get();
        m_sender.connect(std::move(ingress), std::move(egress));
        ASSERT_TRUE(m_sender.isConnected());
        m_ingress->m_offered.clear(); // drop the captured SessionConnectRequest
    }

    // Answers connect() with a single SessionEvent(OK), which is all this fixture needs.
    class FakeEgress : public EgressTransport
    {
      public:
        int poll(const FragmentHandler& handler) override
        {
            if (m_delivered)
            {
                return 0;
            }
            m_delivered = true;
            std::vector<std::uint8_t> buf(256, 0);
            cluster_sbe::SessionEvent enc;
            enc.wrapAndApplyHeader(reinterpret_cast<char*>(buf.data()), 0, buf.size());
            enc.clusterSessionId(SESSION_ID)
                .correlationId(1)
                .leadershipTermId(11)
                .leaderMemberId(0)
                .code(cluster_sbe::EventCode::Value::OK)
                .version(CLUSTER_PROTOCOL_VERSION)
                .leaderHeartbeatTimeoutNs(0);
            enc.putDetail(nullptr, 0);
            buf.resize(enc.sbePosition());
            handler(std::span<const std::uint8_t>(buf.data(), buf.size()));
            return 1;
        }

      private:
        bool m_delivered = false;
    };

    ClusterStreamSender m_sender;
    RecordingIngress* m_ingress{ nullptr };
};

TEST_F(ConnectedSender, PublishPayloadAdmitsTheCeilingAndRefusesOneMore)
{
    // ClientConnected is var-data only, so the payload is its 8-byte header plus the 2-byte prefix plus
    // the data: 1306 bytes of data is exactly MAX_PAYLOAD_LENGTH.
    const std::vector<char> atCeiling(MAX_PAYLOAD_LENGTH - frm::MessageHeader::encodedLength() -
                                          frm::ClientConnected::connectionDataHeaderLength(),
                                      'x');
    EXPECT_EQ(
        Publish::Published,
        publishPayload<frm::ClientConnected>(m_sender, SOURCE_ID, CONNECTION_ID, 2, [&](frm::ClientConnected& encoder) {
            encoder.putConnectionData(atCeiling.data(), static_cast<std::uint16_t>(atCeiling.size()));
        }));
    ASSERT_EQ(1U, m_ingress->m_offered.size());

    const std::vector<char> overCeiling(atCeiling.size() + 1, 'x');
    EXPECT_EQ(
        Publish::Refused,
        publishPayload<frm::ClientConnected>(m_sender, SOURCE_ID, CONNECTION_ID, 2, [&](frm::ClientConnected& encoder) {
            encoder.putConnectionData(overCeiling.data(), static_cast<std::uint16_t>(overCeiling.size()));
        }));
    EXPECT_EQ(1U, m_ingress->m_offered.size()) << "nothing was offered to any transport";

    // Local and permanent, and the producer survives it: the very next well-formed publish succeeds.
    EXPECT_EQ(Publish::Published,
              publishPayload<frm::ClusterStarted>(m_sender, SOURCE_ID, CONNECTION_ID, 2,
                                                  [](frm::ClusterStarted& encoder) { encoder.correlationId(1); }));
    EXPECT_EQ(2U, m_ingress->m_offered.size());
}

TEST_F(ConnectedSender, PublishSystemRefusesABodyOverTheCeiling)
{
    // A system body carries no header of its own, so the ceiling is the prefix plus the data.
    const std::vector<char> overCeiling(MAX_PAYLOAD_LENGTH - frm::ClientConnected::connectionDataHeaderLength() + 1,
                                        'x');
    EXPECT_EQ(Publish::Refused,
              publishSystem<frm::ClientConnected>(
                  m_sender, SOURCE_ID, CONNECTION_ID, CLIENT_CONNECTED, [&](frm::ClientConnected& encoder) {
                      encoder.putConnectionData(overCeiling.data(), static_cast<std::uint16_t>(overCeiling.size()));
                  }));
    EXPECT_TRUE(m_ingress->m_offered.empty());
}

// ── Row 6. The boundary payload sizes (§12) ───────────────────────────────────

TEST(Conformance, TheBoundaryPayloadSizesCrossIntact)
{
    // Empty, one byte, and the ceiling. The first is the case that found the short-payload defect: a
    // frame carrying no payload is legal (§5) and must still reach the consumer, or P-3's continuity
    // read sees a gap that is not there.
    for (const std::uint64_t payloadLength : { std::uint64_t{ 0 }, std::uint64_t{ 1 },
                                               std::uint64_t{ MAX_PAYLOAD_LENGTH } })
    {
        const auto frame = payloadFrame(11, 2, payloadLength);
        const auto view = viewOf(frame);
        ASSERT_TRUE(view.valid) << "payload of " << payloadLength << " bytes";
        EXPECT_FALSE(view.system);
        EXPECT_EQ(2, view.payloadId);
        EXPECT_EQ(payloadLength, view.payloadLength);
        EXPECT_EQ(44U + payloadLength, frame.size()) << "44 + the payload (§4.2)";
    }
}

// ── Row 7. Selective consumption (P-1 to P-3) ─────────────────────────────────

TEST(Conformance, UnknownPayloadsAndEventsAreReadableSoContinuityHolds)
{
    // globalSeqNo sits at 18 on all five sequenced shapes (F-3), so the continuity read is branch-free
    // over frames the consumer comprehends none of. Every one of these must come back valid: a frame
    // dropped here is a globalSeqNo missing from that read, which reads as a gap that is not there.
    std::vector<std::vector<std::uint8_t>> tap;
    tap.push_back(payloadFrame(1, 4095, 8));
    tap.push_back(systemFrame(2, PAYLOAD_ID_REGISTERED, [](char* body, std::size_t cap) {
        frm::PayloadIdRegistered encoder;
        encoder.wrapForEncode(body, 0, cap);
        encoder.payloadId(4).protocolVersion(1);
        encoder.putProtocolName("basicdata");
        return static_cast<std::uint16_t>(encoder.encodedLength());
    }));
    tap.push_back(synthesizedFrame<frm::ClusterHeartbeat>(3, CLUSTER_HEARTBEAT, [](auto&) {}));
    tap.push_back(synthesizedFrame<frm::LeadershipChanged>(
        4, LEADERSHIP_CHANGED, [](frm::LeadershipChanged& f) { f.newLeaderMemberId(2); }));
    tap.push_back(
        synthesizedFrame<frm::GatewayActive>(5, GATEWAY_ACTIVE, [](frm::GatewayActive& f) { f.gatewayId(3); }));

    std::int64_t expected = 1;
    for (const auto& frame : tap)
    {
        const auto view = viewOf(frame);
        EXPECT_TRUE(view.valid) << "frame " << expected << " is unreadable, so P-3 would lose its globalSeqNo";
        EXPECT_EQ(expected, view.globalSeqNo);
        ++expected;
    }

    // An unallocated payloadId is data like any other: the frame is read, and nothing dispatches on it.
    alignas(16) std::array<std::uint8_t, 128> buffer{};
    frm::Sequenced frame;
    frame.wrapAndApplyHeader(reinterpret_cast<char*>(buffer.data()), 0, buffer.size());
    frame.header()
        .sourceId(SOURCE_ID)
        .connectionId(CONNECTION_ID)
        .sessionId(SESSION_ID)
        .payloadId(4095)
        .globalSeqNo(77)
        .timestamp(TIMESTAMP);
    frame.putPayload(nullptr, 0);
    const auto length = frm::MessageHeader::encodedLength() + frame.encodedLength();

    const auto view = unwrapFrame(reinterpret_cast<const char*>(buffer.data()), length);
    ASSERT_TRUE(view.valid) << "an empty payload under an unallocated payloadId is still a frame";
    EXPECT_EQ(4095, view.payloadId);
    EXPECT_EQ(77, view.globalSeqNo);
    EXPECT_EQ(0U, view.templateId) << "no inner declaration, so nothing can dispatch on it (P-1)";
}

} // namespace
} // namespace org::limitless::phixeron::sequencer
