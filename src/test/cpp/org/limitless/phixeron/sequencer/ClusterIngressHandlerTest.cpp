//
// Deterministic unit tests for ClusterIngressHandler — the FIX-session-facing
// bridge that turns TCP-received FIX messages into cluster ingress traffic,
// re-encoding every message (admin and application) as sbe-unsequenced.xml.
//
// Same pattern as ClusterIngressSenderTest.cpp: ClusterIngressHandler talks to
// the cluster only through ClusterIngressSender, which in turn talks only
// through IngressTransport::offer/EgressTransport::poll, so these tests drive
// a real ClusterIngressSender connected to in-memory fakes — no threads, no
// polling loops, no sockets, no media driver. Raw FIX bytes are hand-built
// with correct BodyLength/CheckSum (mirroring patchResendFlags's arithmetic)
// and fed through the real org::limitless::simdifx::decoder::PayloadDecoder, so
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

#include <sys/socket.h>
#include <unistd.h>

#include "org/limitless/simdifx/decoder/PayloadDecoder.hpp"
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
        if (queued.empty()) { return 0; }
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
    body += "56=SEQUENCER\x01";
    body += "34=1\x01";
    body += "52=20260703-12:00:00\x01";
    for (const auto& field : bodyFields) { body += field; body += '\x01'; }

    std::string msg = "8=FIXT.1.1\x01" "9=" + std::to_string(body.size()) + "\x01" + body;

    std::uint32_t sum = 0;
    for (const unsigned char c : msg) { sum += c; }
    sum %= 256;
    char checksum[4];
    std::snprintf(checksum, sizeof(checksum), "%03u", sum);
    msg += "10=";
    msg += checksum;
    msg += '\x01';

    return std::vector<std::uint8_t>(msg.begin(), msg.end());
}

// Same as buildFix, but with caller-supplied SenderCompID/TargetCompID/MsgSeqNum —
// used to reproduce the mid-session CompID-mismatch Reject bug (see
// ClusterIngressHandlerCompIdMismatch below), where buildFix's fixed
// "49=CLIENT"/"56=SEQUENCER"/"34=1" can't set up a mismatch or a specific seq.
std::vector<std::uint8_t> buildFixCustom(char msgType, std::string_view sender, std::string_view target,
                                         std::uint32_t seqNum, const std::vector<std::string>& bodyFields)
{
    std::string body;
    body += "35="; body += msgType; body += '\x01';
    body += "49="; body += sender; body += '\x01';
    body += "56="; body += target; body += '\x01';
    body += "34="; body += std::to_string(seqNum); body += '\x01';
    body += "52=20260703-12:00:00\x01";
    for (const auto& field : bodyFields) { body += field; body += '\x01'; }

    std::string msg = "8=FIXT.1.1\x01" "9=" + std::to_string(body.size()) + "\x01" + body;

    std::uint32_t sum = 0;
    for (const unsigned char c : msg) { sum += c; }
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
// SessionMessageHeader (cluster_sbe) followed directly by a sbe-unsequenced.xml
// message (schemaId=200), matching ClusterIngressSender::send.
constexpr std::size_t APP_MESSAGE_OFFSET = cluster_sbe::MessageHeader::encodedLength()
                                          + cluster_sbe::SessionMessageHeader::sbeBlockLength();

template <typename SbeMsg>
SbeMsg decodeUnsequenced(std::vector<std::uint8_t>& frame)
{
    char* body = reinterpret_cast<char*>(frame.data() + APP_MESSAGE_OFFSET);
    const std::size_t bodyLen = frame.size() - APP_MESSAGE_OFFSET;

    usq::MessageHeader hdr;
    hdr.wrap(body, 0, 0, bodyLen);
    EXPECT_EQ(SbeMsg::sbeTemplateId(), hdr.templateId());

    SbeMsg dec;
    dec.wrapForDecode(body, usq::MessageHeader::encodedLength(),
                      hdr.blockLength(), hdr.version(), bodyLen);
    return dec;
}

} // namespace

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
        m_ingress = ingress.get();

        m_sender.connect(std::move(ingress), std::move(egress));
        ASSERT_TRUE(m_sender.isConnected());
        m_ingress->offered.clear(); // drop the captured SessionConnectRequest
    }

    ClusterIngressSender m_sender;
    FakeIngressTransport* m_ingress{nullptr};
    ClusterIngressHandler m_handler{&m_sender, CONN_ID};
    fix::decoder::PayloadDecoder<cfg::FIXT_1_1>  m_decoder;
};

TEST_F(ClusterIngressHandlerAdminOnly, LogonIsReEncodedAsSbeUnsequencedWithHeader)
{
    const auto msg = buildFix('A', {"98=0", "108=45"});
    const auto result = m_decoder.parse(std::span<const std::uint8_t>(msg.data(), msg.size()), m_handler);
    ASSERT_EQ(fix::Result::Success, result.m_value);

    ASSERT_EQ(1u, m_ingress->offered.size());
    auto logon = decodeUnsequenced<usq::Logon>(m_ingress->offered[0]);
    EXPECT_EQ(CONN_ID, logon.header().sourceId());
    EXPECT_EQ(55, logon.header().sessionId());
    EXPECT_EQ(45u, logon.heartbeatInterval());
}

TEST_F(ClusterIngressHandlerAdminOnly, LogoutIsReEncodedAsSbeUnsequenced)
{
    const auto msg = buildFix('5', {});
    const auto result = m_decoder.parse(std::span<const std::uint8_t>(msg.data(), msg.size()), m_handler);
    ASSERT_EQ(fix::Result::Success, result.m_value);

    ASSERT_EQ(1u, m_ingress->offered.size());
    auto logout = decodeUnsequenced<usq::Logout>(m_ingress->offered[0]);
    EXPECT_EQ(CONN_ID, logout.header().sourceId());
}

TEST_F(ClusterIngressHandlerAdminOnly, HeartbeatCarriesTestReqIdWhenPresent)
{
    const auto msg = buildFix('0', {"112=PING1"});
    const auto result = m_decoder.parse(std::span<const std::uint8_t>(msg.data(), msg.size()), m_handler);
    ASSERT_EQ(fix::Result::Success, result.m_value);

    ASSERT_EQ(1u, m_ingress->offered.size());
    auto hb = decodeUnsequenced<usq::Heartbeat>(m_ingress->offered[0]);
    EXPECT_EQ(std::string("PING1"), std::string(hb.testReqID()));
}

TEST_F(ClusterIngressHandlerAdminOnly, HeartbeatHasEmptyTestReqIdWhenAbsent)
{
    const auto msg = buildFix('0', {});
    const auto result = m_decoder.parse(std::span<const std::uint8_t>(msg.data(), msg.size()), m_handler);
    ASSERT_EQ(fix::Result::Success, result.m_value);

    ASSERT_EQ(1u, m_ingress->offered.size());
    auto hb = decodeUnsequenced<usq::Heartbeat>(m_ingress->offered[0]);
    EXPECT_EQ(std::string(""), std::string(hb.testReqID()));
}

TEST_F(ClusterIngressHandlerAdminOnly, TestRequestCarriesTestReqId)
{
    const auto msg = buildFix('1', {"112=RUOK"});
    const auto result = m_decoder.parse(std::span<const std::uint8_t>(msg.data(), msg.size()), m_handler);
    ASSERT_EQ(fix::Result::Success, result.m_value);

    ASSERT_EQ(1u, m_ingress->offered.size());
    auto tr = decodeUnsequenced<usq::TestRequest>(m_ingress->offered[0]);
    EXPECT_EQ(std::string("RUOK"), std::string(tr.testReqID()));
}

TEST_F(ClusterIngressHandlerAdminOnly, ResendRequestCarriesBeginAndEndSeqNo)
{
    const auto msg = buildFix('2', {"7=5", "16=10"});
    const auto result = m_decoder.parse(std::span<const std::uint8_t>(msg.data(), msg.size()), m_handler);
    ASSERT_EQ(fix::Result::Success, result.m_value);

    ASSERT_EQ(1u, m_ingress->offered.size());
    auto rr = decodeUnsequenced<usq::ResendRequest>(m_ingress->offered[0]);
    EXPECT_EQ(5u, rr.beginSeqNo());
    EXPECT_EQ(10u, rr.endSeqNo());
}

TEST_F(ClusterIngressHandlerAdminOnly, SequenceResetCarriesNewSeqNo)
{
    const auto msg = buildFix('4', {"36=100"});
    const auto result = m_decoder.parse(std::span<const std::uint8_t>(msg.data(), msg.size()), m_handler);
    ASSERT_EQ(fix::Result::Success, result.m_value);

    ASSERT_EQ(1u, m_ingress->offered.size());
    auto sr = decodeUnsequenced<usq::SequenceReset>(m_ingress->offered[0]);
    EXPECT_EQ(100u, sr.newSeqNo());
}

TEST_F(ClusterIngressHandlerAdminOnly, ValidNewOrderSingleIsEncodedAsSbeUnsequencedWithoutASession)
{
    const auto msg = buildFix('D', {"11=ORD-1", "21=1", "55=AAPL", "54=1",
                                     "60=20260703-12:00:00", "38=100", "40=1"});
    // Mirrors FixConnection::onRecv, which sets the raw bytes before parsing.
    m_handler.setRawBytes(std::span<const std::uint8_t>(msg.data(), msg.size()));
    const auto result = m_decoder.parse(std::span<const std::uint8_t>(msg.data(), msg.size()), m_handler);
    ASSERT_EQ(fix::Result::Success, result.m_value);

    // No session was supplied, so only the NewOrderSingle submission happens
    // (no reject/accept ExecutionReport is generated).
    ASSERT_EQ(1u, m_ingress->offered.size());
    auto nos = decodeUnsequenced<usq::NewOrderSingle>(m_ingress->offered[0]);
    EXPECT_EQ(CONN_ID, nos.header().sourceId());
    EXPECT_EQ(std::string("ORD-1"), nos.getClOrdIDAsString());
    EXPECT_EQ(std::string("AAPL"), nos.getSymbolAsString());
}

TEST_F(ClusterIngressHandlerAdminOnly, InvalidNewOrderSingleWithoutASessionSendsNothing)
{
    // ClOrdID present but empty triggers the business-rule rejection path,
    // which returns before submitting the order — and without a session
    // there is nowhere to send the reject ExecutionReport either.
    const auto msg = buildFix('D', {"11=", "21=1", "55=AAPL", "54=1",
                                     "60=20260703-12:00:00", "38=100", "40=1"});
    m_handler.setRawBytes(std::span<const std::uint8_t>(msg.data(), msg.size()));
    const auto result = m_decoder.parse(std::span<const std::uint8_t>(msg.data(), msg.size()), m_handler);
    ASSERT_EQ(fix::Result::Success, result.m_value);
    EXPECT_TRUE(m_ingress->offered.empty());
}

// ── ClusterIngressHandler — NewOrderSingle with a live FixSession ─────────────
//
// Wires a real FixSession (CapturingTransport, fd=-1 so sendRaw is a no-op)
// so ExecutionReports generated for accepted/rejected orders are observable:
// they are submitted directly to the cluster ingress as sbe-unsequenced.xml
// ExecutionReport messages, with MsgSeqNum reserved from the session.
class ClusterIngressHandlerWithSession : public ::testing::Test
{
protected:
    static constexpr std::int32_t CONN_ID = 88;

    void SetUp() override
    {
        auto egress = std::make_unique<FakeEgressTransport>();
        egress->queued.push_back(encodeSessionEvent(66, 22, cluster_sbe::EventCode::Value::OK));
        auto ingress = std::make_unique<FakeIngressTransport>();
        m_ingress = ingress.get();

        m_sender.connect(std::move(ingress), std::move(egress));
        ASSERT_TRUE(m_sender.isConnected());
        m_ingress->offered.clear();

        m_session.onTcpConnected();
    }

    ClusterIngressSender m_sender;
    FakeIngressTransport* m_ingress{nullptr};
    FixSession m_session{FixSession::Builder{}
        .transport(CapturingTransport{-1}).build()};
    ClusterIngressHandler m_handler{&m_sender, CONN_ID, &m_session};
    fix::decoder::PayloadDecoder<cfg::FIXT_1_1>  m_decoder;
};

TEST_F(ClusterIngressHandlerWithSession, ValidNewOrderSingleSubmitsOrderThenSendsExecutionReportNew)
{
    const auto msg = buildFix('D', {"11=ORD-2", "21=1", "55=MSFT", "54=1",
                                     "60=20260703-12:00:00", "38=50", "40=1"});
    m_handler.setRawBytes(std::span<const std::uint8_t>(msg.data(), msg.size()));
    const auto result = m_decoder.parse(std::span<const std::uint8_t>(msg.data(), msg.size()), m_handler);
    ASSERT_EQ(fix::Result::Success, result.m_value);

    ASSERT_EQ(2u, m_ingress->offered.size());

    auto nos = decodeUnsequenced<usq::NewOrderSingle>(m_ingress->offered[0]);
    EXPECT_EQ(CONN_ID, nos.header().sourceId());
    EXPECT_EQ(std::string("ORD-2"), nos.getClOrdIDAsString());

    auto er = decodeUnsequenced<usq::ExecutionReport>(m_ingress->offered[1]);
    EXPECT_EQ(CONN_ID, er.header().sourceId());
    EXPECT_EQ(usq::ExecType::Value::New, er.execType());
    EXPECT_EQ(std::string("ORD-2"), er.getClOrdIDAsString());
}

TEST_F(ClusterIngressHandlerWithSession, EmptyClOrdIdSendsRejectedExecutionReportOnly)
{
    const auto msg = buildFix('D', {"11=", "21=1", "55=MSFT", "54=1",
                                     "60=20260703-12:00:00", "38=50", "40=1"});
    const auto result = m_decoder.parse(std::span<const std::uint8_t>(msg.data(), msg.size()), m_handler);
    ASSERT_EQ(fix::Result::Success, result.m_value);

    // The reject path returns before the order is submitted: only the
    // ExecutionReport(Rejected) reaches the ingress.
    ASSERT_EQ(1u, m_ingress->offered.size());
    auto er = decodeUnsequenced<usq::ExecutionReport>(m_ingress->offered[0]);
    EXPECT_EQ(usq::ExecType::Value::Rejected, er.execType());
    EXPECT_EQ(std::string("ClOrdID is empty"), er.getTextAsString());
}

TEST_F(ClusterIngressHandlerWithSession, ZeroOrderQtyIsRejected)
{
    const auto msg = buildFix('D', {"11=ORD-3", "21=1", "55=MSFT", "54=1",
                                     "60=20260703-12:00:00", "38=0", "40=1"});
    const auto result = m_decoder.parse(std::span<const std::uint8_t>(msg.data(), msg.size()), m_handler);
    ASSERT_EQ(fix::Result::Success, result.m_value);

    ASSERT_EQ(1u, m_ingress->offered.size());
    auto er = decodeUnsequenced<usq::ExecutionReport>(m_ingress->offered[0]);
    EXPECT_EQ(std::string("OrderQty must be > 0"), er.getTextAsString());
}

TEST_F(ClusterIngressHandlerWithSession, LimitOrderWithoutPriceIsRejected)
{
    const auto msg = buildFix('D', {"11=ORD-4", "21=1", "55=MSFT", "54=1",
                                     "60=20260703-12:00:00", "38=10", "40=2"});
    const auto result = m_decoder.parse(std::span<const std::uint8_t>(msg.data(), msg.size()), m_handler);
    ASSERT_EQ(fix::Result::Success, result.m_value);

    ASSERT_EQ(1u, m_ingress->offered.size());
    auto er = decodeUnsequenced<usq::ExecutionReport>(m_ingress->offered[0]);
    EXPECT_EQ(std::string("Price required for Limit order"), er.getTextAsString());
}

// ── ClusterIngressHandler — CompID mismatch mid-session (Reject RefSeqNum) ────
//
// Investigates a live-gateway finding: fix_test_server sent a Heartbeat with
// SenderCompID=WRONGSENDER, MsgSeqNum=2, expecting a session-level Reject with
// RefSeqNum=2, but got RefSeqNum=0 instead. simdfix's own MessageDecoderTest
// (MessageDecoder.HeartbeatWithLongCompIds) proves HeartbeatDecoder::
// sequenceNumber() decodes this exact byte layout correctly (returns 2), so
// the bug is not in the shared decoder — this test reproduces it against
// phixeron's real ClusterIngressHandler/Session, capturing the actual encoded
// Reject bytes over a real socketpair (CapturingTransport only knows how to
// write to a real fd) to find where the value gets lost.
class ClusterIngressHandlerCompIdMismatch : public ::testing::Test
{
protected:
    static constexpr std::int32_t CONN_ID = 99;

    void SetUp() override
    {
        ASSERT_EQ(0, ::socketpair(AF_UNIX, SOCK_STREAM, 0, m_fds));
        m_session = std::make_unique<FixSession>(FixSession::Builder{}
            .transport(CapturingTransport{m_fds[0]}).build());
        m_session->onTcpConnected();
        m_handler = std::make_unique<ClusterIngressHandler>(&m_sender, CONN_ID, m_session.get());
    }

    void TearDown() override
    {
        ::close(m_fds[0]);
        ::close(m_fds[1]);
    }

    // Reads whatever bytes are waiting on the test's end of the socketpair.
    std::vector<std::uint8_t> readReply()
    {
        std::uint8_t buf[512];
        const ssize_t n = ::recv(m_fds[1], buf, sizeof(buf), MSG_DONTWAIT);
        return n > 0 ? std::vector<std::uint8_t>(buf, buf + n) : std::vector<std::uint8_t>{};
    }

    int m_fds[2]{-1, -1};
    ClusterIngressSender m_sender;
    std::unique_ptr<FixSession> m_session;
    std::unique_ptr<ClusterIngressHandler> m_handler;
    fix::decoder::PayloadDecoder<cfg::FIXT_1_1> m_decoder;
};

// First, log on normally (SenderCompID=CLIENT, matching the session's
// hardcoded expected identity): ClusterIngressHandler::handle(LogonDecoder&)
// only forwards to the cluster (it never touches session state — see
// FixConnection.hpp's "Session state is updated ONLY by messages received
// from the cluster global stream" invariant), so activation is simulated
// directly here the same way FixConnection::onClusterAdmin does when the
// Logon echoes back confirmed. Without this, sendReject()'s
// `m_state != State::Active` guard would silently swallow the Reject below —
// a different failure than the one being investigated.
TEST_F(ClusterIngressHandlerCompIdMismatch, HeartbeatRejectCarriesRealRefSeqNum)
{
    const auto logon = buildFixCustom('A', "CLIENT", "SEQUENCER", 1, {"98=0", "108=30"});
    ASSERT_EQ(fix::Result::Success, m_decoder.parse(std::span<const std::uint8_t>(logon.data(), logon.size()), *m_handler).m_value);
    m_session->handleClusterLogon(30, 0);
    ASSERT_FALSE(readReply().empty());  // Logon's own reply, not under test

    const auto heartbeat = buildFixCustom('0', "WRONGSENDER", "SEQUENCER", 2, {});
    ASSERT_EQ(fix::Result::Success, m_decoder.parse(std::span<const std::uint8_t>(heartbeat.data(), heartbeat.size()), *m_handler).m_value);

    const auto reply = readReply();
    ASSERT_FALSE(reply.empty()) << "expected a Reject reply to the CompID-mismatched Heartbeat";
    const std::string replyStr(reply.begin(), reply.end());
    std::printf("[Test] Reject reply: %s\n", replyStr.c_str());

    // Find "45=" (RefSeqNum) and read its value directly out of the raw bytes.
    const auto pos = replyStr.find("45=");
    ASSERT_NE(std::string::npos, pos) << "RefSeqNum (45) missing from Reject";
    const auto end = replyStr.find('\x01', pos);
    const std::string refSeqNum = replyStr.substr(pos + 3, end - (pos + 3));
    EXPECT_EQ("2", refSeqNum) << "RefSeqNum should echo the mismatched Heartbeat's MsgSeqNum (2)";
}

} // namespace org::limitless::phixeron::sequencer
