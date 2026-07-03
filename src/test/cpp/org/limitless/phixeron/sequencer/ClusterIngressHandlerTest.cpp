//
// Deterministic unit tests for ClusterIngressHandler — the FIX-session-facing
// bridge that turns TCP-received FIX messages into cluster ingress traffic
// (admin messages re-encoded as SBE, application messages forwarded raw).
//
// Same pattern as ClusterIngressSenderTest.cpp: ClusterIngressHandler talks to
// the cluster only through ClusterIngressSender, which in turn talks only
// through IngressTransport::offer/EgressTransport::poll, so these tests drive
// a real ClusterIngressSender connected to in-memory fakes — no threads, no
// polling loops, no sockets, no media driver. Raw FIX bytes are hand-built
// with correct BodyLength/CheckSum (mirroring patchResendFlags's arithmetic)
// and fed through the real org::limitless::fix::decoder::PayloadDecoder, so
// these tests exercise the exact tokenizer/dispatch path production traffic
// takes.

#include <gtest/gtest.h>

#include <cstdint>
#include <cstdio>
#include <deque>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "org/limitless/fix/decoder/PayloadDecoder.hpp"
#include "org/limitless/phixeron/sequencer/ClusterIngressHandler.hpp"

namespace org::limitless::phixeron::sequencer
{
namespace
{

// ── Fake transports (see ClusterIngressSenderTest.cpp for the same pair) ──────

class FakeIngressTransport : public IngressTransport
{
public:
    std::vector<std::vector<std::uint8_t>> offered;

    bool offer(std::span<const std::uint8_t> bytes) override
    {
        offered.emplace_back(bytes.begin(), bytes.end());
        return true;
    }
};

class FakeEgressTransport : public EgressTransport
{
public:
    std::deque<std::vector<std::uint8_t>> queued;

    int poll(const FragmentHandler& handler) override
    {
        if (queued.empty()) return 0;
        const std::vector<std::uint8_t> msg = std::move(queued.front());
        queued.pop_front();
        handler(std::span<const std::uint8_t>(msg.data(), msg.size()));
        return 1;
    }
};

std::vector<std::uint8_t> encodeSessionEvent(std::int64_t clusterSessionId,
                                              std::int64_t leadershipTermId,
                                              cluster_sbe::EventCode::Value code)
{
    std::vector<std::uint8_t> buf(256, 0);
    cluster_sbe::SessionEvent enc;
    enc.wrapAndApplyHeader(reinterpret_cast<char*>(buf.data()), 0, buf.size());
    enc.clusterSessionId(clusterSessionId)
       .correlationId(1)
       .leadershipTermId(leadershipTermId)
       .leaderMemberId(0)
       .code(code)
       .version(CLUSTER_PROTOCOL_VERSION)
       .leaderHeartbeatTimeoutNs(0);
    enc.putDetail(nullptr, 0);
    buf.resize(enc.sbePosition());
    return buf;
}

// ── Raw FIX message builder ───────────────────────────────────────────────────
//
// Builds a minimal, valid FIXT.1.1 message: standard header (BeginString,
// BodyLength, MsgType, SenderCompID, TargetCompID, MsgSeqNum, SendingTime)
// plus caller-supplied body fields, with BodyLength/CheckSum computed exactly
// the way PayloadDecoder validates them (see checkRequiredFields/
// processCheckSum in simdfix's PayloadDecoder.hpp): BodyLength is the byte
// count from the MsgType tag through the last SOH before "10="; CheckSum is
// the sum of every byte before "10=", mod 256.
std::vector<std::uint8_t> buildFix(char msgType, const std::vector<std::string>& bodyFields)
{
    std::string body;
    body += "35="; body += msgType; body += '\x01';
    body += "49=CLIENT\x01";
    body += "56=SEQNCR\x01";
    body += "34=1\x01";
    body += "52=20260703-12:00:00\x01";
    for (const auto& field : bodyFields) { body += field; body += '\x01'; }

    std::string msg = "8=FIXT.1.1\x01" "9=" + std::to_string(body.size()) + "\x01" + body;

    std::uint32_t sum = 0;
    for (const unsigned char c : msg) sum += c;
    sum %= 256;
    char checksum[4];
    std::snprintf(checksum, sizeof(checksum), "%03u", sum);
    msg += "10=";
    msg += checksum;
    msg += '\x01';

    return std::vector<std::uint8_t>(msg.begin(), msg.end());
}

// ── Decoding what ClusterIngressHandler offered to the cluster ingress ───────

// Every message ClusterIngressHandler sends is wrapped in a
// SessionMessageHeader (cluster_sbe) followed by an AppMessage SBE envelope
// (8-byte header + 2-byte varData length + 4-byte connId), matching
// ClusterIngressSender::send/encodeAppMessage.
constexpr std::size_t APP_MESSAGE_OFFSET = cluster_sbe::MessageHeader::encodedLength()
                                          + cluster_sbe::SessionMessageHeader::sbeBlockLength();
constexpr std::size_t APP_PAYLOAD_OFFSET = APP_MESSAGE_OFFSET + 10 + CONN_ID_PREFIX;

// Decodes the connId-prefixed payload as an admin SBE message
// (sbe-session.xml, schemaId=100).
template <typename AdminMsg>
AdminMsg decodeAdminFrame(std::vector<std::uint8_t>& frame, std::int32_t& connIdOut)
{
    std::memcpy(&connIdOut, frame.data() + APP_MESSAGE_OFFSET + 10, CONN_ID_PREFIX);

    char* body = reinterpret_cast<char*>(frame.data() + APP_PAYLOAD_OFFSET);
    const std::size_t bodyLen = frame.size() - APP_PAYLOAD_OFFSET;

    sbesess::MessageHeader hdr;
    hdr.wrap(body, 0, 0, bodyLen);
    EXPECT_EQ(AdminMsg::sbeTemplateId(), hdr.templateId());

    AdminMsg dec;
    dec.wrapForDecode(body, sbesess::MessageHeader::encodedLength(), hdr.blockLength(), hdr.version(), bodyLen);
    return dec;
}

// Decodes the connId-prefixed payload as raw FIX bytes (application
// messages: the client's own NewOrderSingle, or a generated ExecutionReport).
std::string_view decodeRawFixFrame(std::vector<std::uint8_t>& frame, std::int32_t& connIdOut)
{
    std::memcpy(&connIdOut, frame.data() + APP_MESSAGE_OFFSET + 10, CONN_ID_PREFIX);
    return std::string_view(reinterpret_cast<const char*>(frame.data() + APP_PAYLOAD_OFFSET),
                             frame.size() - APP_PAYLOAD_OFFSET);
}

std::string_view fixTagValue(std::string_view fix, std::string_view tag)
{
    const auto [vs, ve] = fixTagRange(reinterpret_cast<const std::uint8_t*>(fix.data()), fix.size(), tag);
    if (vs == std::string::npos) return {};
    return fix.substr(vs, ve - vs);
}

} // namespace

// ── Pure byte-level helpers ───────────────────────────────────────────────────

TEST(ClusterIngressHandlerWireHelpers, FindFixMessageEndLocatesTrailingChecksum)
{
    const auto msg = buildFix('0', {});
    EXPECT_EQ(msg.size(), findFixMessageEnd(msg));
}

TEST(ClusterIngressHandlerWireHelpers, FindFixMessageEndReturnsZeroWhenIncomplete)
{
    auto msg = buildFix('0', {});
    msg.resize(msg.size() - 1);   // drop the trailing SOH of the checksum field
    EXPECT_EQ(0u, findFixMessageEnd(msg));
}

TEST(ClusterIngressHandlerWireHelpers, IsAdminMsgTypeDistinguishesSessionFromApplicationMessages)
{
    const auto logon = buildFix('A', {"108=30"});
    const auto heartbeat = buildFix('0', {});
    const auto newOrder = buildFix('D', {"11=1", "21=1", "55=X", "54=1", "60=20260703-12:00:00", "38=1", "40=1"});
    EXPECT_TRUE(isAdminMsgType(logon.data(), logon.size()));
    EXPECT_TRUE(isAdminMsgType(heartbeat.data(), heartbeat.size()));
    EXPECT_FALSE(isAdminMsgType(newOrder.data(), newOrder.size()));
}

TEST(ClusterIngressHandlerWireHelpers, PatchResendFlagsInsertsPossDupAndFixesBodyLengthAndChecksum)
{
    auto msg = buildFix('0', {});
    patchResendFlags(msg);

    const auto [pdVs, pdVe] = fixTagRange(msg.data(), msg.size(), "43");
    ASSERT_NE(std::string::npos, pdVs);
    EXPECT_EQ("Y", std::string_view(reinterpret_cast<const char*>(msg.data() + pdVs), pdVe - pdVs));
    EXPECT_NE(std::string::npos, fixFindTag(msg.data(), msg.size(), "122"));

    // The patched message must still be self-consistent: PayloadDecoder
    // recomputes BodyLength/CheckSum from scratch and must accept it.
    EXPECT_EQ(msg.size(), findFixMessageEnd(msg));
}

// ── ClusterIngressHandler — admin messages (no session required) ─────────────

class ClusterIngressHandlerAdminOnly : public ::testing::Test
{
protected:
    static constexpr std::int32_t CONN_ID = 77;

    void SetUp() override
    {
        auto egress = std::make_unique<FakeEgressTransport>();
        egress->queued.push_back(encodeSessionEvent(55, 11, cluster_sbe::EventCode::Value::OK));
        auto ingress = std::make_unique<FakeIngressTransport>();
        ingress_ = ingress.get();

        sender_.connect(std::move(ingress), std::move(egress));
        ASSERT_TRUE(sender_.isConnected());
        ingress_->offered.clear(); // drop the captured SessionConnectRequest
    }

    ClusterIngressSender                        sender_;
    FakeIngressTransport*                        ingress_{nullptr};
    ClusterIngressHandler                        handler_{&sender_, CONN_ID};
    fix::decoder::PayloadDecoder<cfg::FIXT_1_1>  decoder_;
};

TEST_F(ClusterIngressHandlerAdminOnly, LogonIsReEncodedAsAdminSbeWithConnIdPrefix)
{
    const auto msg = buildFix('A', {"98=0", "108=45"});
    const auto result = decoder_.parse(std::span<const std::uint8_t>(msg.data(), msg.size()), handler_);
    ASSERT_EQ(fix::Result::Success, result.m_value);

    ASSERT_EQ(1u, ingress_->offered.size());
    std::int32_t connId = 0;
    auto logon = decodeAdminFrame<sbesess::Logon>(ingress_->offered[0], connId);
    EXPECT_EQ(CONN_ID, connId);
    EXPECT_EQ(45u, logon.heartbeatInterval());
}

TEST_F(ClusterIngressHandlerAdminOnly, LogoutIsReEncodedAsAdminSbe)
{
    const auto msg = buildFix('5', {});
    const auto result = decoder_.parse(std::span<const std::uint8_t>(msg.data(), msg.size()), handler_);
    ASSERT_EQ(fix::Result::Success, result.m_value);

    ASSERT_EQ(1u, ingress_->offered.size());
    std::int32_t connId = 0;
    decodeAdminFrame<sbesess::Logout>(ingress_->offered[0], connId);
    EXPECT_EQ(CONN_ID, connId);
}

TEST_F(ClusterIngressHandlerAdminOnly, HeartbeatCarriesTestReqIdWhenPresent)
{
    const auto msg = buildFix('0', {"112=PING1"});
    const auto result = decoder_.parse(std::span<const std::uint8_t>(msg.data(), msg.size()), handler_);
    ASSERT_EQ(fix::Result::Success, result.m_value);

    ASSERT_EQ(1u, ingress_->offered.size());
    std::int32_t connId = 0;
    auto hb = decodeAdminFrame<sbesess::Heartbeat>(ingress_->offered[0], connId);
    EXPECT_EQ(std::string("PING1"), std::string(hb.testReqID()));
}

TEST_F(ClusterIngressHandlerAdminOnly, HeartbeatHasEmptyTestReqIdWhenAbsent)
{
    const auto msg = buildFix('0', {});
    const auto result = decoder_.parse(std::span<const std::uint8_t>(msg.data(), msg.size()), handler_);
    ASSERT_EQ(fix::Result::Success, result.m_value);

    ASSERT_EQ(1u, ingress_->offered.size());
    std::int32_t connId = 0;
    auto hb = decodeAdminFrame<sbesess::Heartbeat>(ingress_->offered[0], connId);
    EXPECT_EQ(std::string(""), std::string(hb.testReqID()));
}

TEST_F(ClusterIngressHandlerAdminOnly, TestRequestCarriesTestReqId)
{
    const auto msg = buildFix('1', {"112=RUOK"});
    const auto result = decoder_.parse(std::span<const std::uint8_t>(msg.data(), msg.size()), handler_);
    ASSERT_EQ(fix::Result::Success, result.m_value);

    ASSERT_EQ(1u, ingress_->offered.size());
    std::int32_t connId = 0;
    auto tr = decodeAdminFrame<sbesess::TestRequest>(ingress_->offered[0], connId);
    EXPECT_EQ(std::string("RUOK"), std::string(tr.testReqID()));
}

TEST_F(ClusterIngressHandlerAdminOnly, ResendRequestCarriesBeginAndEndSeqNo)
{
    const auto msg = buildFix('2', {"7=5", "16=10"});
    const auto result = decoder_.parse(std::span<const std::uint8_t>(msg.data(), msg.size()), handler_);
    ASSERT_EQ(fix::Result::Success, result.m_value);

    ASSERT_EQ(1u, ingress_->offered.size());
    std::int32_t connId = 0;
    auto rr = decodeAdminFrame<sbesess::ResendRequest>(ingress_->offered[0], connId);
    EXPECT_EQ(5u, rr.beginSeqNo());
    EXPECT_EQ(10u, rr.endSeqNo());
}

TEST_F(ClusterIngressHandlerAdminOnly, SequenceResetCarriesNewSeqNo)
{
    const auto msg = buildFix('4', {"36=100"});
    const auto result = decoder_.parse(std::span<const std::uint8_t>(msg.data(), msg.size()), handler_);
    ASSERT_EQ(fix::Result::Success, result.m_value);

    ASSERT_EQ(1u, ingress_->offered.size());
    std::int32_t connId = 0;
    auto sr = decodeAdminFrame<sbesess::SequenceReset>(ingress_->offered[0], connId);
    EXPECT_EQ(100u, sr.newSeqNo());
}

TEST_F(ClusterIngressHandlerAdminOnly, ValidNewOrderSingleIsForwardedAsRawFixWithoutASession)
{
    const auto msg = buildFix('D', {"11=ORD-1", "21=1", "55=AAPL", "54=1",
                                     "60=20260703-12:00:00", "38=100", "40=1"});
    // Mirrors FixConnection::onRecv, which sets the raw bytes before parsing
    // so handle(NewOrderSingleDecoder&) can forward the untouched wire bytes.
    handler_.setRawBytes(std::span<const std::uint8_t>(msg.data(), msg.size()));
    const auto result = decoder_.parse(std::span<const std::uint8_t>(msg.data(), msg.size()), handler_);
    ASSERT_EQ(fix::Result::Success, result.m_value);

    // No session was supplied, so only the raw forward happens (no reject/
    // accept ExecutionReport is generated).
    ASSERT_EQ(1u, ingress_->offered.size());
    std::int32_t connId = 0;
    const auto rawFix = decodeRawFixFrame(ingress_->offered[0], connId);
    EXPECT_EQ(CONN_ID, connId);
    EXPECT_EQ("ORD-1", fixTagValue(rawFix, "11"));
    EXPECT_EQ("AAPL", fixTagValue(rawFix, "55"));
}

TEST_F(ClusterIngressHandlerAdminOnly, InvalidNewOrderSingleWithoutASessionSendsNothing)
{
    // ClOrdID present but empty triggers the business-rule rejection path,
    // which returns before the raw-forward — and without a session there is
    // nowhere to send the reject ExecutionReport either.
    const auto msg = buildFix('D', {"11=", "21=1", "55=AAPL", "54=1",
                                     "60=20260703-12:00:00", "38=100", "40=1"});
    handler_.setRawBytes(std::span<const std::uint8_t>(msg.data(), msg.size()));
    const auto result = decoder_.parse(std::span<const std::uint8_t>(msg.data(), msg.size()), handler_);
    ASSERT_EQ(fix::Result::Success, result.m_value);
    EXPECT_TRUE(ingress_->offered.empty());
}

// ── ClusterIngressHandler — NewOrderSingle with a live FixSession ─────────────
//
// Wires a real FixSession (CapturingTransport, fd=-1 so sendRaw is a no-op)
// so ExecutionReports generated for accepted/rejected orders are observable:
// CapturingTransport forwards them to the same cluster ingress (ExecutionReport
// is not an admin MsgType), per the file's deterministic design invariant.
class ClusterIngressHandlerWithSession : public ::testing::Test
{
protected:
    static constexpr std::int32_t CONN_ID = 88;

    void SetUp() override
    {
        auto egress = std::make_unique<FakeEgressTransport>();
        egress->queued.push_back(encodeSessionEvent(66, 22, cluster_sbe::EventCode::Value::OK));
        auto ingress = std::make_unique<FakeIngressTransport>();
        ingress_ = ingress.get();

        sender_.connect(std::move(ingress), std::move(egress));
        ASSERT_TRUE(sender_.isConnected());
        ingress_->offered.clear();

        session_.onTcpConnected();
    }

    ClusterIngressSender                        sender_;
    FakeIngressTransport*                        ingress_{nullptr};
    FixSession session_{FixSession::Builder{sess::NullStorage{}}
                             .transport(CapturingTransport{-1, &sender_, CONN_ID})
                             .build()};
    ClusterIngressHandler                        handler_{&sender_, CONN_ID, &session_};
    fix::decoder::PayloadDecoder<cfg::FIXT_1_1>  decoder_;
};

TEST_F(ClusterIngressHandlerWithSession, ValidNewOrderSingleForwardsRawBytesThenSendsExecutionReportNew)
{
    const auto msg = buildFix('D', {"11=ORD-2", "21=1", "55=MSFT", "54=1",
                                     "60=20260703-12:00:00", "38=50", "40=1"});
    handler_.setRawBytes(std::span<const std::uint8_t>(msg.data(), msg.size()));
    const auto result = decoder_.parse(std::span<const std::uint8_t>(msg.data(), msg.size()), handler_);
    ASSERT_EQ(fix::Result::Success, result.m_value);

    ASSERT_EQ(2u, ingress_->offered.size());

    std::int32_t connId = 0;
    const auto rawFix = decodeRawFixFrame(ingress_->offered[0], connId);
    EXPECT_EQ(CONN_ID, connId);
    EXPECT_EQ("ORD-2", fixTagValue(rawFix, "11"));

    const auto executionReport = decodeRawFixFrame(ingress_->offered[1], connId);
    EXPECT_EQ(CONN_ID, connId);
    EXPECT_EQ("8", fixTagValue(executionReport, "35")) << "ExecutionReport MsgType is not '8'";
    EXPECT_EQ(msg::code(msg::ExecType::New), fixTagValue(executionReport, "150"));
}

TEST_F(ClusterIngressHandlerWithSession, EmptyClOrdIdSendsRejectedExecutionReportOnly)
{
    const auto msg = buildFix('D', {"11=", "21=1", "55=MSFT", "54=1",
                                     "60=20260703-12:00:00", "38=50", "40=1"});
    const auto result = decoder_.parse(std::span<const std::uint8_t>(msg.data(), msg.size()), handler_);
    ASSERT_EQ(fix::Result::Success, result.m_value);

    // The reject path returns before the raw-forward: only the
    // ExecutionReport(Rejected) reaches the ingress.
    ASSERT_EQ(1u, ingress_->offered.size());
    std::int32_t connId = 0;
    const auto executionReport = decodeRawFixFrame(ingress_->offered[0], connId);
    EXPECT_EQ(msg::code(msg::ExecType::Rejected), fixTagValue(executionReport, "150"));
    EXPECT_EQ("ClOrdID is empty", fixTagValue(executionReport, "58"));
}

TEST_F(ClusterIngressHandlerWithSession, ZeroOrderQtyIsRejected)
{
    const auto msg = buildFix('D', {"11=ORD-3", "21=1", "55=MSFT", "54=1",
                                     "60=20260703-12:00:00", "38=0", "40=1"});
    const auto result = decoder_.parse(std::span<const std::uint8_t>(msg.data(), msg.size()), handler_);
    ASSERT_EQ(fix::Result::Success, result.m_value);

    ASSERT_EQ(1u, ingress_->offered.size());
    std::int32_t connId = 0;
    const auto executionReport = decodeRawFixFrame(ingress_->offered[0], connId);
    EXPECT_EQ("OrderQty must be > 0", fixTagValue(executionReport, "58"));
}

TEST_F(ClusterIngressHandlerWithSession, LimitOrderWithoutPriceIsRejected)
{
    const auto msg = buildFix('D', {"11=ORD-4", "21=1", "55=MSFT", "54=1",
                                     "60=20260703-12:00:00", "38=10", "40=2"});
    const auto result = decoder_.parse(std::span<const std::uint8_t>(msg.data(), msg.size()), handler_);
    ASSERT_EQ(fix::Result::Success, result.m_value);

    ASSERT_EQ(1u, ingress_->offered.size());
    std::int32_t connId = 0;
    const auto executionReport = decodeRawFixFrame(ingress_->offered[0], connId);
    EXPECT_EQ("Price required for Limit order", fixTagValue(executionReport, "58"));
}

} // namespace org::limitless::phixeron::sequencer
