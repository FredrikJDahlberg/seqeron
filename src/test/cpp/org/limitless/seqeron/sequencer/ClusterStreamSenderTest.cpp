//
// Deterministic unit tests for ClusterStreamSender's session lifecycle
// (connect → SessionEvent(OK) → send → keep-alive → close).
//
// ClusterStreamSender talks to the cluster purely through IngressTransport::offer
// and EgressTransport::poll (see ClusterStreamSender.hpp), so these tests drive
// it against small in-memory fakes instead of a real Aeron media driver — no
// threads, no polling loops, no sockets. Wire-level codec round-trips of the
// individual SBE messages are covered separately in ClusterCodecTest.cpp; these
// tests instead pin down the session state machine built on top of them.

#include <gtest/gtest.h>

#include <array>
#include <chrono>
#include <cstdint>
#include <deque>
#include <limits>
#include <string_view>
#include <vector>

#include "org/limitless/seqeron/sequencer/ClusterStreamSender.hpp"

namespace org::limitless::seqeron::sequencer {
namespace {

// ── Fake transports ───────────────────────────────────────────────────────────

// Captures every frame offered to the cluster ingress for inspection by tests.
class FakeIngressTransport : public IngressTransport
{
  public:
    std::vector<std::vector<std::uint8_t>> m_offered;

    bool offer(std::span<const std::uint8_t> bytes) override
    {
        m_offered.emplace_back(bytes.begin(), bytes.end());
        return true;
    }
};

// Rejects the first m_rejectCount offers (as a back-pressured or, during a leader
// failover, not-connected ingress publication would), then accepts — capturing the
// frame that finally lands. Drives ClusterStreamSender::send()'s reliable-offer spin.
class FlakyIngressTransport : public IngressTransport
{
  public:
    int m_rejectCount = 0; // reject this many offers before accepting the next
    int m_offerCalls = 0;
    std::vector<std::uint8_t> m_accepted; // the frame that finally landed

    bool offer(std::span<const std::uint8_t> bytes) override
    {
        ++m_offerCalls;
        if (m_rejectCount > 0)
        {
            --m_rejectCount;
            return false;
        }
        m_accepted.assign(bytes.begin(), bytes.end());
        return true;
    }
};

// Delivers pre-queued frames on poll(), one per call, mimicking a cluster
// egress subscription that already has messages buffered.
class FakeEgressTransport : public EgressTransport
{
  public:
    std::deque<std::vector<std::uint8_t>> m_queued;

    int poll(const FragmentHandler& handler) override
    {
        if (m_queued.empty())
        {
            return 0;
        }
        const std::vector<std::uint8_t> msg = std::move(m_queued.front());
        m_queued.pop_front();
        handler(std::span<const std::uint8_t>(msg.data(), msg.size()));
        return 1;
    }
};

// ── Fixture wire helpers ──────────────────────────────────────────────────────

std::vector<std::uint8_t> encodeSessionEvent(std::int64_t clusterSessionId, std::int64_t leadershipTermId,
                                             cluster_sbe::EventCode::Value code, std::int32_t leaderMemberId = 0,
                                             std::string_view detail = {})
{
    std::vector<std::uint8_t> buf(256 + detail.size(), 0);
    cluster_sbe::SessionEvent enc;
    enc.wrapAndApplyHeader(reinterpret_cast<char*>(buf.data()), 0, buf.size());
    enc.clusterSessionId(clusterSessionId)
        .correlationId(1)
        .leadershipTermId(leadershipTermId)
        .leaderMemberId(leaderMemberId)
        .code(code)
        .version(CLUSTER_PROTOCOL_VERSION)
        .leaderHeartbeatTimeoutNs(0);
    enc.putDetail(detail.data(), static_cast<int>(detail.size()));
    buf.resize(enc.sbePosition());
    return buf;
}

std::vector<std::uint8_t> encodeNewLeaderEvent(std::int64_t leadershipTermId, std::int32_t leaderMemberId = 0,
                                               std::string_view ingressEndpoints = {})
{
    std::vector<std::uint8_t> buf(128 + ingressEndpoints.size(), 0);
    cluster_sbe::NewLeaderEvent enc;
    enc.wrapAndApplyHeader(reinterpret_cast<char*>(buf.data()), 0, buf.size());
    enc.leadershipTermId(leadershipTermId).clusterSessionId(0).leaderMemberId(leaderMemberId);
    enc.putIngressEndpoints(ingressEndpoints.data(), static_cast<int>(ingressEndpoints.size()));
    buf.resize(enc.sbePosition());
    return buf;
}

// Builds an egress frame carrying an application-layer payload the way the
// real cluster echoes it back: SessionMessageHeader followed by arbitrary bytes.
std::vector<std::uint8_t> encodeSessionMessage(std::int64_t leadershipTermId, std::int64_t clusterSessionId,
                                               std::span<const std::uint8_t> appPayload)
{
    std::vector<std::uint8_t> buf(256 + appPayload.size(), 0);
    cluster_sbe::SessionMessageHeader enc;
    enc.wrapAndApplyHeader(reinterpret_cast<char*>(buf.data()), 0, buf.size())
        .leadershipTermId(leadershipTermId)
        .clusterSessionId(clusterSessionId)
        .timestamp(0);
    const auto pos = static_cast<std::size_t>(enc.sbePosition());
    std::memcpy(buf.data() + pos, appPayload.data(), appPayload.size());
    buf.resize(pos + appPayload.size());
    return buf;
}

// Decodes the MessageHeader + templateId-specific SBE message that
// ClusterStreamSender offered to the (fake) ingress transport.
template<typename SbeMsg>
SbeMsg decodeOffered(std::vector<std::uint8_t>& frame)
{
    cluster_sbe::MessageHeader hdr;
    hdr.wrap(reinterpret_cast<char*>(frame.data()), 0, 0, frame.size());
    EXPECT_EQ(SbeMsg::sbeTemplateId(), hdr.templateId());

    SbeMsg dec;
    dec.wrapForDecode(reinterpret_cast<char*>(frame.data()), cluster_sbe::MessageHeader::encodedLength(),
                      hdr.blockLength(), hdr.version(), frame.size());
    return dec;
}

// ── Tests ──────────────────────────────────────────────────────────────────────

TEST(ClusterStreamSender, ConnectSendsSessionConnectRequestAndAdoptsSessionOnOk)
{
    auto egress = std::make_unique<FakeEgressTransport>();
    egress->m_queued.push_back(encodeSessionEvent(42, 7, cluster_sbe::EventCode::Value::OK));
    auto* egressPtr = egress.get();

    auto ingress = std::make_unique<FakeIngressTransport>();
    auto* ingressPtr = ingress.get();

    // The responseChannel is the caller's own UDP endpoint — the cluster tier holds no default
    // for it, since the port belongs to whichever application is connecting (AppPorts.hpp).
    const std::string egressChannel = "aeron:udp?endpoint=localhost:9330";

    ClusterStreamSender sender;
    sender.connect(std::move(ingress), std::move(egress), egressChannel);

    EXPECT_TRUE(sender.isConnected());
    EXPECT_TRUE(egressPtr->m_queued.empty());

    ASSERT_EQ(1u, ingressPtr->m_offered.size());
    auto req = decodeOffered<cluster_sbe::SessionConnectRequest>(ingressPtr->m_offered[0]);
    EXPECT_EQ(1, req.correlationId());
    EXPECT_EQ(CLUSTER_EGRESS_STREAM_ID, req.responseStreamId());
    EXPECT_EQ(CLUSTER_PROTOCOL_VERSION, req.version());
    // SBE var-data fields must be read in schema order (responseChannel,
    // encodedCredentials, clientInfo) — each getter advances the decoder's
    // internal read position, so skipping a field would misread the next one.
    EXPECT_EQ(egressChannel, req.getResponseChannelAsString());
    EXPECT_EQ("", req.getEncodedCredentialsAsString());
    EXPECT_EQ(CLUSTER_CLIENT_INFO, req.getClientInfoAsString());
}

// A SessionConnectRequest that lands on a FOLLOWER still yields a session — the open is replicated
// and the leader answers OK — but a follower then silently drops every session message and keep-alive
// (ConsensusModuleAgent.onIngressMessage requires `Cluster.Role.LEADER == role` and falls through with
// no reply). The OK names the leader, and that is the only signal available: unlike REDIRECT and
// NewLeaderEvent it carries no endpoint CSV, so the endpoint is derived from the member id. Without
// this the client looked connected, had nothing it published sequenced, and died of a genuine session
// timeout ~10s later.
TEST(ClusterStreamSender, MemberIngressEndpointMatchesTheInitialEndpointFormula)
{
    // Member 0's derived endpoint must be exactly the constant connectColocated's UDP fallback aims
    // at, or "am I already on the leader?" would compare unequal strings for the same member.
    EXPECT_EQ(CLUSTER_INGRESS_ENDPOINT, memberIngressEndpoint(0));

    // …and every other member resolves to its own distinct endpoint.
    EXPECT_NE(memberIngressEndpoint(0), memberIngressEndpoint(1));
    EXPECT_NE(memberIngressEndpoint(1), memberIngressEndpoint(2));
    EXPECT_EQ("localhost:" + std::to_string(clusterIngressPort(2)), memberIngressEndpoint(2));
}

// The transport-agnostic connect() overload has no Aeron client to build a replacement publication
// with, so an OK naming a leader we are not publishing to must be a safe no-op rather than a crash —
// the same guard handleRedirect applies. The session is still adopted.
TEST(ClusterStreamSender, ConnectIgnoresNonLeaderIngressWithoutAeronClient)
{
    auto egress = std::make_unique<FakeEgressTransport>();
    egress->m_queued.push_back(encodeSessionEvent(42, 7, cluster_sbe::EventCode::Value::OK, /*leaderMemberId=*/2));

    ClusterStreamSender sender;
    sender.connect(std::make_unique<FakeIngressTransport>(), std::move(egress));

    EXPECT_TRUE(sender.isConnected());
    EXPECT_EQ(42, sender.clusterSessionId());
}

TEST(ClusterStreamSender, ConnectThrowsWhenClusterNeverAnswers)
{
    ClusterStreamSender sender;
    sender.setConnectTimeoutMs(20);

    EXPECT_THROW(sender.connect(std::make_unique<FakeIngressTransport>(), std::make_unique<FakeEgressTransport>()),
                 std::runtime_error);
    EXPECT_FALSE(sender.isConnected());
}

TEST(ClusterStreamSender, ConnectIgnoresErrorEventCodesUntilOkArrives)
{
    auto egress = std::make_unique<FakeEgressTransport>();
    egress->m_queued.push_back(encodeSessionEvent(-1, 0, cluster_sbe::EventCode::Value::ERROR));
    egress->m_queued.push_back(encodeSessionEvent(9, 3, cluster_sbe::EventCode::Value::OK));

    ClusterStreamSender sender;
    sender.connect(std::make_unique<FakeIngressTransport>(), std::move(egress));

    EXPECT_TRUE(sender.isConnected());
}

// The transport-agnostic connect() overload has no Aeron client to build a new ingress
// Publication with, so a REDIRECT must be a safe no-op (logged, not acted on) rather than a
// crash — the session still completes once the (already-queued) OK arrives.
TEST(ClusterStreamSender, ConnectIgnoresRedirectWithoutAeronClientThenConnectsOnOk)
{
    auto egress = std::make_unique<FakeEgressTransport>();
    egress->m_queued.push_back(
        encodeSessionEvent(-1, 0, cluster_sbe::EventCode::Value::REDIRECT, 1, "1=localhost:9312"));
    egress->m_queued.push_back(encodeSessionEvent(9, 3, cluster_sbe::EventCode::Value::OK));

    auto ingress = std::make_unique<FakeIngressTransport>();
    auto* ingressPtr = ingress.get();

    ClusterStreamSender sender;
    sender.connect(std::move(ingress), std::move(egress));

    EXPECT_TRUE(sender.isConnected());
    // No m_aeron to reconnect with, so handleRedirect must not have re-sent
    // SessionConnectRequest: only the original one was ever offered.
    EXPECT_EQ(1u, ingressPtr->m_offered.size());
}

class ConnectedClusterStreamSender : public ::testing::Test
{
  protected:
    void SetUp() override
    {
        auto egress = std::make_unique<FakeEgressTransport>();
        egress->m_queued.push_back(encodeSessionEvent(SESSION_ID, TERM_ID, cluster_sbe::EventCode::Value::OK));
        egress_ = egress.get();

        auto ingress = std::make_unique<FakeIngressTransport>();
        ingress_ = ingress.get();

        sender_.connect(std::move(ingress), std::move(egress));
        ASSERT_TRUE(sender_.isConnected());
        ingress_->m_offered.clear(); // drop the captured SessionConnectRequest
    }

    static constexpr std::int64_t SESSION_ID = 55;
    static constexpr std::int64_t TERM_ID = 11;

    ClusterStreamSender sender_;
    FakeIngressTransport* ingress_{ nullptr };
    FakeEgressTransport* egress_{ nullptr };
};

TEST_F(ConnectedClusterStreamSender, SendWrapsBytesWithSessionMessageHeader)
{
    const std::array<std::uint8_t, 5> body{ '8', '=', 'F', 'I', 'X' };
    EXPECT_TRUE(sender_.send(body.data(), static_cast<std::uint16_t>(body.size())));

    ASSERT_EQ(1u, ingress_->m_offered.size());
    auto hdr = decodeOffered<cluster_sbe::SessionMessageHeader>(ingress_->m_offered[0]);
    EXPECT_EQ(TERM_ID, hdr.leadershipTermId());
    EXPECT_EQ(SESSION_ID, hdr.clusterSessionId());

    // Bytes after the SessionMessageHeader are exactly the caller-supplied
    // message, with no separate envelope (unlike the old AppMessage scheme).
    const auto& frame = ingress_->m_offered[0];
    const std::size_t appOff =
        cluster_sbe::MessageHeader::encodedLength() + cluster_sbe::SessionMessageHeader::sbeBlockLength();
    ASSERT_GE(frame.size(), appOff + body.size());

    EXPECT_TRUE(std::equal(body.begin(), body.end(), frame.begin() + static_cast<std::ptrdiff_t>(appOff)));
}

TEST_F(ConnectedClusterStreamSender, SendFramesAPayloadOfTheLargestSupportedSize)
{
    // send()'s framing buffer is sized from MAX_PAYLOAD_LEN, which is also what every caller sizes its
    // encode buffer from. The two used to disagree — a 8192-byte encode buffer against a 4138-byte
    // framing array — so a full-size message memcpy'd past the end of it.
    // Under the Debug build's AddressSanitizer this fails on the write, not on the size assertion.
    const std::vector<std::uint8_t> body(ClusterStreamSender::MAX_PAYLOAD_LEN, 0xAB);
    EXPECT_TRUE(sender_.send(body.data(), static_cast<std::uint16_t>(body.size())));

    ASSERT_EQ(1u, ingress_->m_offered.size());
    const auto& frame = ingress_->m_offered[0];
    const std::size_t appOff = cluster_sbe::SessionMessageHeader::sbeBlockAndHeaderLength();
    ASSERT_EQ(appOff + body.size(), frame.size());
    EXPECT_TRUE(std::equal(body.begin(), body.end(), frame.begin() + static_cast<std::ptrdiff_t>(appOff)));
}

TEST_F(ConnectedClusterStreamSender, SendRefusesAPayloadLargerThanTheFramingBuffer)
{
    // Unreachable while every caller sizes its buffer from MAX_PAYLOAD_LEN; this guards the raw
    // (pointer, len) API against a future one that does not. Throwing rather than truncating or
    // dropping: a frame too large to place is a programming error no runtime handling can repair, and
    // dropping it would tear the outbound MsgSeqNum hole send() exists to prevent.
    const std::vector<std::uint8_t> body(ClusterStreamSender::MAX_PAYLOAD_LEN + 1, 0xCD);
    EXPECT_THROW((void)sender_.send(body.data(), static_cast<std::uint16_t>(body.size())), std::runtime_error);
    EXPECT_TRUE(ingress_->m_offered.empty());
}

TEST_F(ConnectedClusterStreamSender, SendReportsFailureOnceTheSessionIsClosed)
{
    // The one outcome the reliable-offer spin cannot fix: with no cluster session there is nothing to
    // offer to and no amount of waiting helps (a leader failover, which the spin does handle, keeps
    // the session id). Reporting it is what lets Session::publishOutbound leave the outbound MsgSeqNum
    // unspent instead of tearing a hole no resend can fill.
    sender_.close();
    ASSERT_FALSE(sender_.isConnected());
    ingress_->m_offered.clear();

    const std::array<std::uint8_t, 1> body{ '8' };
    EXPECT_FALSE(sender_.send(body.data(), 1));
    EXPECT_TRUE(ingress_->m_offered.empty());
}

TEST_F(ConnectedClusterStreamSender, KeepAliveSendsOnceThenThrottles)
{
    sender_.keepAlive();
    ASSERT_EQ(1u, ingress_->m_offered.size());
    auto ka = decodeOffered<cluster_sbe::SessionKeepAlive>(ingress_->m_offered[0]);
    EXPECT_EQ(TERM_ID, ka.leadershipTermId());
    EXPECT_EQ(SESSION_ID, ka.clusterSessionId());

    // Called again immediately: the keep-alive interval has not elapsed, so no
    // second frame should be offered.
    sender_.keepAlive();
    EXPECT_EQ(1u, ingress_->m_offered.size());
}

TEST_F(ConnectedClusterStreamSender, CloseSendsSessionCloseRequestAndForgetsSession)
{
    sender_.close();

    ASSERT_EQ(1u, ingress_->m_offered.size());
    auto req = decodeOffered<cluster_sbe::SessionCloseRequest>(ingress_->m_offered[0]);
    EXPECT_EQ(TERM_ID, req.leadershipTermId());
    EXPECT_EQ(SESSION_ID, req.clusterSessionId());

    EXPECT_FALSE(sender_.isConnected());

    // A second close() is a no-op: no session, nothing to send.
    sender_.close();
    EXPECT_EQ(1u, ingress_->m_offered.size());
}

TEST_F(ConnectedClusterStreamSender, PollEgressDeliversApplicationPayload)
{
    const std::array<std::uint8_t, 4> app{ '8', '=', 'x', 'x' };
    egress_->m_queued.push_back(encodeSessionMessage(TERM_ID, SESSION_ID, app));

    std::vector<std::uint8_t> received;
    sender_.pollEgress([&](const std::uint8_t* data, std::int32_t len) { received.assign(data, data + len); });

    ASSERT_EQ(app.size(), received.size());
    EXPECT_TRUE(std::equal(app.begin(), app.end(), received.begin()));
}

TEST_F(ConnectedClusterStreamSender, PollEgressUpdatesLeadershipTermOnNewLeaderEvent)
{
    egress_->m_queued.push_back(encodeNewLeaderEvent(999));

    sender_.pollEgress([](const std::uint8_t*, std::int32_t) {
        FAIL() << "NewLeaderEvent must not be forwarded as an application message";
    });

    // The new leadership term must now be used for subsequent sends.
    const std::array<std::uint8_t, 1> body{ '8' };
    EXPECT_TRUE(sender_.send(body.data(), 1));

    ASSERT_EQ(1u, ingress_->m_offered.size());
    auto hdr = decodeOffered<cluster_sbe::SessionMessageHeader>(ingress_->m_offered[0]);
    EXPECT_EQ(999, hdr.leadershipTermId());
}

// Same as above, but the event also carries a new ingress endpoint for the (fake-connected,
// m_aeron == nullptr) session. Without a real Aeron client there is nothing to reconnect with,
// so this must degrade to the same leadership-term-only update as the no-endpoints case above —
// not crash, and not touch the ingress transport.
TEST_F(ConnectedClusterStreamSender, PollEgressIgnoresNewLeaderEndpointWithoutAeronClient)
{
    egress_->m_queued.push_back(encodeNewLeaderEvent(999, 1, "0=localhost:9302,1=localhost:9312"));

    sender_.pollEgress([](const std::uint8_t*, std::int32_t) {
        FAIL() << "NewLeaderEvent must not be forwarded as an application message";
    });

    const std::array<std::uint8_t, 1> body{ '8' };
    EXPECT_TRUE(sender_.send(body.data(), 1));

    ASSERT_EQ(1u, ingress_->m_offered.size());
    auto hdr = decodeOffered<cluster_sbe::SessionMessageHeader>(ingress_->m_offered[0]);
    EXPECT_EQ(999, hdr.leadershipTermId());
}

// The cluster closes a session it has stopped hearing from (keep-alive timeout) exactly as it
// closes one on request: EventCode::CLOSED on egress, with the CloseReason as detail. A
// steady-state SessionEvent used to fall off the end of onFragment, so the client went on framing
// into a session id the cluster had already forgotten — and, being a FIX gateway, went on serving
// TCP clients whose traffic could no longer be sequenced.
TEST_F(ConnectedClusterStreamSender, PollEgressReportsTheClusterClosingThisSession)
{
    EXPECT_FALSE(sender_.isSessionLost());

    egress_->m_queued.push_back(
        encodeSessionEvent(SESSION_ID, TERM_ID, cluster_sbe::EventCode::Value::CLOSED, 0, "TIMEOUT"));
    sender_.pollEgress([](const std::uint8_t*, std::int32_t) {
        FAIL() << "a SessionEvent must not be forwarded as an application message";
    });

    EXPECT_TRUE(sender_.isSessionLost());
    EXPECT_FALSE(sender_.isConnected());
    // …and the session is genuinely gone, not merely flagged: nothing more may be framed onto it.
    const std::array<std::uint8_t, 1> body{ '8' };
    EXPECT_FALSE(sender_.send(body.data(), 1));
    EXPECT_TRUE(ingress_->m_offered.empty());
}

// A close for somebody else's session says nothing about ours.
TEST_F(ConnectedClusterStreamSender, PollEgressIgnoresACloseForAnotherSession)
{
    egress_->m_queued.push_back(
        encodeSessionEvent(SESSION_ID + 1, TERM_ID, cluster_sbe::EventCode::Value::CLOSED, 0, "CLIENT_ACTION"));
    sender_.pollEgress([](const std::uint8_t*, std::int32_t) {});

    EXPECT_FALSE(sender_.isSessionLost());
    EXPECT_TRUE(sender_.isConnected());
}

// ── Reliable send: spin until the offer lands (see ClusterStreamSender::send) ──────────────

// A back-pressured ingress publication rejects offers transiently; send() must keep
// re-offering the same frame until it lands rather than dropping it (a dropped ingress
// frame is an unrecoverable hole in the outbound MsgSeqNum stream).
TEST(ClusterStreamSenderReliableSend, SendSpinsUntilOfferAccepted)
{
    auto egress = std::make_unique<FakeEgressTransport>();
    egress->m_queued.push_back(encodeSessionEvent(55, 11, cluster_sbe::EventCode::Value::OK));

    auto ingress = std::make_unique<FlakyIngressTransport>();
    auto* ingressPtr = ingress.get();

    ClusterStreamSender sender;
    sender.connect(std::move(ingress), std::move(egress));
    ASSERT_TRUE(sender.isConnected());

    ingressPtr->m_offerCalls = 0;
    ingressPtr->m_rejectCount = 3; // reject three offers, accept the fourth

    const std::array<std::uint8_t, 5> body{ '8', '=', 'F', 'I', 'X' };
    EXPECT_TRUE(sender.send(body.data(), static_cast<std::uint16_t>(body.size())));

    EXPECT_EQ(4, ingressPtr->m_offerCalls); // spun until it landed
    ASSERT_FALSE(ingressPtr->m_accepted.empty());
    auto hdr = decodeOffered<cluster_sbe::SessionMessageHeader>(ingressPtr->m_accepted);
    EXPECT_EQ(11, hdr.leadershipTermId());
    EXPECT_EQ(55, hdr.clusterSessionId());
}

// The spin's other two exits both need a leader — one to accept the frame, the other to send the close —
// so when quorum is lost neither ever arrives. Left unbounded that stops the caller's whole duty cycle
// with it (no tap poll, no keep-alive, no tap-stall fence) silently, for as long as the outage lasts.
// Bounded, it becomes the session loss it already is, and the caller's existing isSessionLost() fence
// takes it from there.
TEST(ClusterStreamSender, SendGivesTheSessionUpWhenNoLeaderEverAcceptsIngress)
{
    auto egress = std::make_unique<FakeEgressTransport>();
    egress->m_queued.push_back(encodeSessionEvent(55, 11, cluster_sbe::EventCode::Value::OK));

    auto ingress = std::make_unique<FlakyIngressTransport>();
    auto* ingressPtr = ingress.get();

    ClusterStreamSender sender;
    sender.setIngressStallTimeoutMs(100);
    sender.connect(std::move(ingress), std::move(egress));
    ASSERT_TRUE(sender.isConnected());

    ingressPtr->m_rejectCount = std::numeric_limits<int>::max(); // no leader is ever coming back

    const std::array<std::uint8_t, 5> body{ '8', '=', 'F', 'I', 'X' };
    const auto start = std::chrono::steady_clock::now();
    EXPECT_FALSE(sender.send(body.data(), static_cast<std::uint16_t>(body.size())));
    const auto elapsed = std::chrono::steady_clock::now() - start;

    EXPECT_GE(elapsed, std::chrono::milliseconds(100)) << "the bound must be ridden out, not tripped on refusal one";
    EXPECT_TRUE(sender.isSessionLost()) << "what the caller fences on; nothing else here would have set it";
    EXPECT_FALSE(sender.isConnected());
}

// The bound is on elapsed time, not on attempts: a busy leader that refuses a great many offers in quick
// succession and then takes one is ordinary back-pressure, and must not be mistaken for an outage.
TEST(ClusterStreamSender, SendRidesOutManyFastRefusalsWithinTheStallBound)
{
    auto egress = std::make_unique<FakeEgressTransport>();
    egress->m_queued.push_back(encodeSessionEvent(55, 11, cluster_sbe::EventCode::Value::OK));

    auto ingress = std::make_unique<FlakyIngressTransport>();
    auto* ingressPtr = ingress.get();

    ClusterStreamSender sender;
    sender.setIngressStallTimeoutMs(60'000); // beyond anything this test can take
    sender.connect(std::move(ingress), std::move(egress));
    ASSERT_TRUE(sender.isConnected());

    ingressPtr->m_offerCalls = 0;
    ingressPtr->m_rejectCount = 5'000;

    const std::array<std::uint8_t, 5> body{ '8', '=', 'F', 'I', 'X' };
    EXPECT_TRUE(sender.send(body.data(), static_cast<std::uint16_t>(body.size())));

    EXPECT_EQ(5'001, ingressPtr->m_offerCalls);
    EXPECT_FALSE(sender.isSessionLost());
    EXPECT_TRUE(sender.isConnected());
}

// During a leader failover the offer fails while a NewLeaderEvent is waiting on egress.
// send()'s spin must pump egress itself — picking up the new leader (and, in production,
// swapping the ingress publication to it) — and re-stamp the frame's leadershipTermId to
// the new term before the retry lands, because the new leader rejects a frame carrying the
// old term. This also pins down the no-deadlock property: the swap that lets the offer
// succeed is driven from inside send(), on the same thread, not from the duty cycle.
TEST(ClusterStreamSenderReliableSend, SendReStampsLeadershipTermAfterMidSpinFailover)
{
    auto egress = std::make_unique<FakeEgressTransport>();
    egress->m_queued.push_back(encodeSessionEvent(55, 11, cluster_sbe::EventCode::Value::OK));
    auto* egressPtr = egress.get();

    auto ingress = std::make_unique<FlakyIngressTransport>();
    auto* ingressPtr = ingress.get();

    ClusterStreamSender sender;
    sender.connect(std::move(ingress), std::move(egress));
    ASSERT_TRUE(sender.isConnected());

    // A new leader (term 999) is waiting on egress; the current publication rejects the
    // first offer, as a not-connected one would mid-failover. The spin pumps egress
    // (consuming the NewLeaderEvent, updating the term) before the second offer lands.
    egressPtr->m_queued.push_back(encodeNewLeaderEvent(999));
    ingressPtr->m_offerCalls = 0;
    ingressPtr->m_rejectCount = 1;

    const std::array<std::uint8_t, 1> body{ '8' };
    EXPECT_TRUE(sender.send(body.data(), 1));

    EXPECT_EQ(2, ingressPtr->m_offerCalls);
    ASSERT_FALSE(ingressPtr->m_accepted.empty());
    auto hdr = decodeOffered<cluster_sbe::SessionMessageHeader>(ingressPtr->m_accepted);
    EXPECT_EQ(999, hdr.leadershipTermId()); // re-stamped to the new leader's term
}

// The cluster closes this session while send()'s spin is already running — the CLOSED event is
// waiting on egress and only the spin's own pump will see it. An offer on a closed session is
// rejected forever, so the spin MUST re-check the session it framed against and give up, not just
// check it once on entry: without that, send() never returns, the caller never sees the false or
// isSessionLost(), and its whole duty cycle (SIGTERM handling included) stops. The reject count is
// finite rather than permanent so a regression fails on the EXPECTs instead of hanging the suite.
TEST(ClusterStreamSenderReliableSend, SendGivesUpWhenTheSessionIsClosedMidSpin)
{
    auto egress = std::make_unique<FakeEgressTransport>();
    egress->m_queued.push_back(encodeSessionEvent(55, 11, cluster_sbe::EventCode::Value::OK));
    auto* egressPtr = egress.get();

    auto ingress = std::make_unique<FlakyIngressTransport>();
    auto* ingressPtr = ingress.get();

    ClusterStreamSender sender;
    sender.connect(std::move(ingress), std::move(egress));
    ASSERT_TRUE(sender.isConnected());

    egressPtr->m_queued.push_back(
        encodeSessionEvent(55, 11, cluster_sbe::EventCode::Value::CLOSED, 0, "SERVICE_ACTION"));
    ingressPtr->m_offerCalls = 0;
    ingressPtr->m_accepted.clear(); // drop the connect handshake frame, so this asserts on send() alone
    ingressPtr->m_rejectCount = 32; // far more than the one rejection the close needs to land

    const std::array<std::uint8_t, 1> body{ '8' };
    EXPECT_FALSE(sender.send(body.data(), 1));

    EXPECT_EQ(1, ingressPtr->m_offerCalls);      // gave up on the first rejection, not after 32
    EXPECT_TRUE(ingressPtr->m_accepted.empty()); // nothing was placed on the dead session
    EXPECT_TRUE(sender.isSessionLost());
    EXPECT_FALSE(sender.isConnected());
}

// Without a real Aeron client there is nothing to re-chase IPC with, so a NewLeaderEvent naming
// this client's own co-located member (set via the test-seam's memberId) must degrade to the
// same leadership-term-only update as PollEgressIgnoresNewLeaderEndpointWithoutAeronClient above
// — not attempt (and crash on) building an IPC publication with a null m_aeron.
TEST(ClusterStreamSenderColocated, PollEgressIgnoresIpcRechaseWithoutAeronClientEvenWhenLeaderIsCoLocatedMember)
{
    auto egress = std::make_unique<FakeEgressTransport>();
    egress->m_queued.push_back(encodeSessionEvent(9, 3, cluster_sbe::EventCode::Value::OK));
    auto* egressPtr = egress.get();

    auto primary = std::make_unique<FakeIngressTransport>();

    ClusterStreamSender sender;
    sender.connectColocated(
        std::move(primary), [&]() -> std::unique_ptr<IngressTransport> { return nullptr; }, std::move(egress),
        /*primaryConnectTimeoutMs=*/1500, /*primaryFailureReason=*/nullptr, /*memberId=*/3);
    ASSERT_TRUE(sender.isConnected());

    // New leader is member 3 — this client's own co-located member — but m_aeron is null.
    egressPtr->m_queued.push_back(encodeNewLeaderEvent(999, 3, "0=localhost:9302,3=localhost:9308"));
    sender.pollEgress([](const std::uint8_t*, std::int32_t) {
        FAIL() << "NewLeaderEvent must not be forwarded as an application message";
    });

    // The new leadership term must still be adopted for subsequent sends, exactly as the
    // no-co-located-member case does.
    const std::array<std::uint8_t, 1> body{ '8' };
    EXPECT_TRUE(sender.send(body.data(), 1));
}

// ── connectColocated's IPC-then-UDP fallback (see ClusterStreamSender.hpp) ────────────────

// Primary (IPC) attempt never answers within the short timeout, so the fallback ingress
// transport must be built and used instead, sharing the same egress transport throughout.
// The queued SessionEvent(OK) only appears once the fallback is built — mirroring how, in
// production, the cluster simply never answers an IPC SessionConnectRequest sent to a
// follower (no IPC ingress subscription exists there at all) until the client gives up and
// retries over UDP against a member that actually answers.
TEST(ClusterStreamSenderColocated, FallsBackToSecondaryWhenPrimaryNeverAnswers)
{
    auto egress = std::make_unique<FakeEgressTransport>();
    auto* egressPtr = egress.get();

    auto primary = std::make_unique<FakeIngressTransport>();
    auto* primaryPtr = primary.get();

    auto fallback = std::make_unique<FakeIngressTransport>();
    auto* fallbackPtr = fallback.get();
    bool fallbackBuilt = false;
    std::size_t primaryOfferedCountAtFallbackTime = 0;

    ClusterStreamSender sender;
    sender.connectColocated(
        std::move(primary),
        [&]() -> std::unique_ptr<IngressTransport> {
            fallbackBuilt = true;
            // primaryPtr is still valid here (the sender hasn't reassigned its ingress
            // transport to the fallback yet), but becomes dangling as soon as this lambda
            // returns and connect() replaces it — so snapshot what we need now.
            primaryOfferedCountAtFallbackTime = primaryPtr->m_offered.size();
            egressPtr->m_queued.push_back(encodeSessionEvent(9, 3, cluster_sbe::EventCode::Value::OK));
            return std::move(fallback);
        },
        std::move(egress),
        /*primaryConnectTimeoutMs=*/20,
        /*primaryFailureReason=*/nullptr);

    EXPECT_TRUE(sender.isConnected());
    EXPECT_TRUE(fallbackBuilt);
    // The primary transport never got a SessionEvent(OK), so its SessionConnectRequest was
    // offered but nothing else; the fallback transport is the one that actually completed
    // the handshake once the queued OK was polled.
    EXPECT_EQ(1u, primaryOfferedCountAtFallbackTime);
    ASSERT_EQ(1u, fallbackPtr->m_offered.size());
    decodeOffered<cluster_sbe::SessionConnectRequest>(fallbackPtr->m_offered[0]);
}

// A null primary (mirrors createIpcIngressPublication() itself throwing before any transport
// could be built) must skip straight to the fallback, still delivering the queued OK.
TEST(ClusterStreamSenderColocated, NullPrimarySkipsStraightToFallback)
{
    auto egress = std::make_unique<FakeEgressTransport>();
    egress->m_queued.push_back(encodeSessionEvent(9, 3, cluster_sbe::EventCode::Value::OK));

    auto fallback = std::make_unique<FakeIngressTransport>();
    auto* fallbackPtr = fallback.get();

    ClusterStreamSender sender;
    sender.connectColocated(
        nullptr, [&]() -> std::unique_ptr<IngressTransport> { return std::move(fallback); }, std::move(egress),
        /*primaryConnectTimeoutMs=*/20,
        /*primaryFailureReason=*/"IPC publication never connected");

    EXPECT_TRUE(sender.isConnected());
    ASSERT_EQ(1u, fallbackPtr->m_offered.size());
    decodeOffered<cluster_sbe::SessionConnectRequest>(fallbackPtr->m_offered[0]);
}

// When the primary attempt succeeds immediately, the fallback factory must never be invoked.
TEST(ClusterStreamSenderColocated, PrimarySuccessNeverBuildsFallback)
{
    auto egress = std::make_unique<FakeEgressTransport>();
    egress->m_queued.push_back(encodeSessionEvent(42, 7, cluster_sbe::EventCode::Value::OK));

    auto primary = std::make_unique<FakeIngressTransport>();
    auto* primaryPtr = primary.get();
    bool fallbackBuilt = false;

    ClusterStreamSender sender;
    sender.connectColocated(
        std::move(primary),
        [&]() -> std::unique_ptr<IngressTransport> {
            fallbackBuilt = true;
            return std::make_unique<FakeIngressTransport>();
        },
        std::move(egress),
        /*primaryConnectTimeoutMs=*/1500,
        /*primaryFailureReason=*/nullptr);

    EXPECT_TRUE(sender.isConnected());
    EXPECT_FALSE(fallbackBuilt);
    ASSERT_EQ(1u, primaryPtr->m_offered.size());
}

// ── findIngressEndpoint (the "memberId=host:port,..." CSV parser shared by
//    SessionEvent(REDIRECT).detail and NewLeaderEvent.ingressEndpoints) ──────────

TEST(FindIngressEndpoint, FindsMemberInMultiEntryCsv)
{
    std::string out;
    EXPECT_TRUE(findIngressEndpoint("0=localhost:9302,1=localhost:9312,2=localhost:9322", 1, out));
    EXPECT_EQ("localhost:9312", out);
}

TEST(FindIngressEndpoint, FindsLastEntryWithNoTrailingComma)
{
    std::string out;
    EXPECT_TRUE(findIngressEndpoint("0=localhost:9302,1=localhost:9312", 1, out));
    EXPECT_EQ("localhost:9312", out);
}

TEST(FindIngressEndpoint, FindsSoleEntry)
{
    std::string out;
    EXPECT_TRUE(findIngressEndpoint("0=localhost:9302", 0, out));
    EXPECT_EQ("localhost:9302", out);
}

TEST(FindIngressEndpoint, ReturnsFalseWhenMemberIdAbsent)
{
    std::string out;
    EXPECT_FALSE(findIngressEndpoint("0=localhost:9302,1=localhost:9312", 2, out));
}

TEST(FindIngressEndpoint, ReturnsFalseOnEmptyCsv)
{
    std::string out;
    EXPECT_FALSE(findIngressEndpoint("", 0, out));
}

} // namespace
} // namespace org::limitless::seqeron::sequencer
