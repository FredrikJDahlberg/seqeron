//
// Deterministic unit tests for ClusterIngressSender's session lifecycle
// (connect → SessionEvent(OK) → send → keep-alive → close).
//
// ClusterIngressSender talks to the cluster purely through IngressTransport::offer
// and EgressTransport::poll (see ClusterIngressSender.hpp), so these tests drive
// it against small in-memory fakes instead of a real Aeron media driver — no
// threads, no polling loops, no sockets. Wire-level codec round-trips of the
// individual SBE messages are covered separately in ClusterCodecTest.cpp; these
// tests instead pin down the session state machine built on top of them.

#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <deque>
#include <string_view>
#include <vector>

#include "org/limitless/phixeron/sequencer/ClusterIngressSender.hpp"

namespace org::limitless::phixeron::sequencer
{
namespace
{

// ── Fake transports ───────────────────────────────────────────────────────────

// Captures every frame offered to the cluster ingress for inspection by tests.
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

// Delivers pre-queued frames on poll(), one per call, mimicking a cluster
// egress subscription that already has messages buffered.
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

// ── Fixture wire helpers ──────────────────────────────────────────────────────

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

std::vector<std::uint8_t> encodeNewLeaderEvent(std::int64_t leadershipTermId)
{
    std::vector<std::uint8_t> buf(128, 0);
    cluster_sbe::NewLeaderEvent enc;
    enc.wrapAndApplyHeader(reinterpret_cast<char*>(buf.data()), 0, buf.size());
    enc.leadershipTermId(leadershipTermId).clusterSessionId(0).leaderMemberId(0);
    enc.putIngressEndpoints(nullptr, 0);
    buf.resize(enc.sbePosition());
    return buf;
}

// Builds an egress frame carrying an application-layer payload the way the
// real cluster echoes it back: SessionMessageHeader followed by arbitrary bytes.
std::vector<std::uint8_t> encodeSessionMessage(std::int64_t leadershipTermId,
                                                std::int64_t clusterSessionId,
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
// ClusterIngressSender offered to the (fake) ingress transport.
template <typename SbeMsg>
SbeMsg decodeOffered(std::vector<std::uint8_t>& frame)
{
    cluster_sbe::MessageHeader hdr;
    hdr.wrap(reinterpret_cast<char*>(frame.data()), 0, 0, frame.size());
    EXPECT_EQ(SbeMsg::sbeTemplateId(), hdr.templateId());

    SbeMsg dec;
    dec.wrapForDecode(reinterpret_cast<char*>(frame.data()),
                       cluster_sbe::MessageHeader::encodedLength(),
                       hdr.blockLength(), hdr.version(), frame.size());
    return dec;
}

// ── Tests ──────────────────────────────────────────────────────────────────────

TEST(ClusterIngressSender, ConnectSendsSessionConnectRequestAndAdoptsSessionOnOk)
{
    auto egress = std::make_unique<FakeEgressTransport>();
    egress->queued.push_back(encodeSessionEvent(42, 7, cluster_sbe::EventCode::Value::OK));
    auto* egressPtr = egress.get();

    auto ingress = std::make_unique<FakeIngressTransport>();
    auto* ingressPtr = ingress.get();

    ClusterIngressSender sender;
    sender.connect(std::move(ingress), std::move(egress));

    EXPECT_TRUE(sender.isConnected());
    EXPECT_TRUE(egressPtr->queued.empty());

    ASSERT_EQ(1u, ingressPtr->offered.size());
    auto req = decodeOffered<cluster_sbe::SessionConnectRequest>(ingressPtr->offered[0]);
    EXPECT_EQ(1, req.correlationId());
    EXPECT_EQ(CLUSTER_EGRESS_STREAM_ID, req.responseStreamId());
    EXPECT_EQ(CLUSTER_PROTOCOL_VERSION, req.version());
    // SBE var-data fields must be read in schema order (responseChannel,
    // encodedCredentials, clientInfo) — each getter advances the decoder's
    // internal read position, so skipping a field would misread the next one.
    EXPECT_EQ(CLUSTER_EGRESS_CHANNEL, req.getResponseChannelAsString());
    EXPECT_EQ("", req.getEncodedCredentialsAsString());
    EXPECT_EQ(CLUSTER_CLIENT_INFO, req.getClientInfoAsString());
}

TEST(ClusterIngressSender, ConnectThrowsWhenClusterNeverAnswers)
{
    ClusterIngressSender sender;
    sender.setConnectTimeoutMs(20);

    EXPECT_THROW(
        sender.connect(std::make_unique<FakeIngressTransport>(),
                       std::make_unique<FakeEgressTransport>()),
        std::runtime_error);
    EXPECT_FALSE(sender.isConnected());
}

TEST(ClusterIngressSender, ConnectIgnoresErrorEventCodesUntilOkArrives)
{
    auto egress = std::make_unique<FakeEgressTransport>();
    egress->queued.push_back(encodeSessionEvent(-1, 0, cluster_sbe::EventCode::Value::ERROR));
    egress->queued.push_back(encodeSessionEvent(9, 3, cluster_sbe::EventCode::Value::OK));

    ClusterIngressSender sender;
    sender.connect(std::make_unique<FakeIngressTransport>(), std::move(egress));

    EXPECT_TRUE(sender.isConnected());
}

class ConnectedClusterIngressSender : public ::testing::Test
{
protected:
    void SetUp() override
    {
        auto egress = std::make_unique<FakeEgressTransport>();
        egress->queued.push_back(encodeSessionEvent(SESSION_ID, TERM_ID, cluster_sbe::EventCode::Value::OK));
        egress_ = egress.get();

        auto ingress = std::make_unique<FakeIngressTransport>();
        ingress_ = ingress.get();

        sender_.connect(std::move(ingress), std::move(egress));
        ASSERT_TRUE(sender_.isConnected());
        ingress_->offered.clear(); // drop the captured SessionConnectRequest
    }

    static constexpr std::int64_t SESSION_ID = 55;
    static constexpr std::int64_t TERM_ID    = 11;

    ClusterIngressSender      sender_;
    FakeIngressTransport*     ingress_{nullptr};
    FakeEgressTransport*      egress_{nullptr};
};

TEST_F(ConnectedClusterIngressSender, SendWrapsBytesWithSessionMessageHeader)
{
    const std::array<std::uint8_t, 5> body{'8', '=', 'F', 'I', 'X'};
    sender_.send(body.data(), static_cast<std::uint16_t>(body.size()));

    ASSERT_EQ(1u, ingress_->offered.size());
    auto hdr = decodeOffered<cluster_sbe::SessionMessageHeader>(ingress_->offered[0]);
    EXPECT_EQ(TERM_ID, hdr.leadershipTermId());
    EXPECT_EQ(SESSION_ID, hdr.clusterSessionId());

    // Bytes after the SessionMessageHeader are exactly the caller-supplied
    // message, with no separate envelope (unlike the old AppMessage scheme).
    const auto& frame = ingress_->offered[0];
    const std::size_t appOff = cluster_sbe::MessageHeader::encodedLength()
                              + cluster_sbe::SessionMessageHeader::sbeBlockLength();
    ASSERT_GE(frame.size(), appOff + body.size());

    EXPECT_TRUE(std::equal(body.begin(), body.end(),
                            frame.begin() + static_cast<std::ptrdiff_t>(appOff)));
}

TEST_F(ConnectedClusterIngressSender, KeepAliveSendsOnceThenThrottles)
{
    sender_.keepAlive();
    ASSERT_EQ(1u, ingress_->offered.size());
    auto ka = decodeOffered<cluster_sbe::SessionKeepAlive>(ingress_->offered[0]);
    EXPECT_EQ(TERM_ID, ka.leadershipTermId());
    EXPECT_EQ(SESSION_ID, ka.clusterSessionId());

    // Called again immediately: the keep-alive interval has not elapsed, so no
    // second frame should be offered.
    sender_.keepAlive();
    EXPECT_EQ(1u, ingress_->offered.size());
}

TEST_F(ConnectedClusterIngressSender, CloseSendsSessionCloseRequestAndForgetsSession)
{
    sender_.close();

    ASSERT_EQ(1u, ingress_->offered.size());
    auto req = decodeOffered<cluster_sbe::SessionCloseRequest>(ingress_->offered[0]);
    EXPECT_EQ(TERM_ID, req.leadershipTermId());
    EXPECT_EQ(SESSION_ID, req.clusterSessionId());

    EXPECT_FALSE(sender_.isConnected());

    // A second close() is a no-op: no session, nothing to send.
    sender_.close();
    EXPECT_EQ(1u, ingress_->offered.size());
}

TEST_F(ConnectedClusterIngressSender, PollEgressDeliversApplicationPayload)
{
    const std::array<std::uint8_t, 4> app{'8', '=', 'x', 'x'};
    egress_->queued.push_back(encodeSessionMessage(TERM_ID, SESSION_ID, app));

    std::vector<std::uint8_t> received;
    sender_.pollEgress([&](const std::uint8_t* data, std::int32_t len)
    {
        received.assign(data, data + len);
    });

    ASSERT_EQ(app.size(), received.size());
    EXPECT_TRUE(std::equal(app.begin(), app.end(), received.begin()));
}

TEST_F(ConnectedClusterIngressSender, PollEgressUpdatesLeadershipTermOnNewLeaderEvent)
{
    egress_->queued.push_back(encodeNewLeaderEvent(999));

    sender_.pollEgress([](const std::uint8_t*, std::int32_t) {
        FAIL() << "NewLeaderEvent must not be forwarded as an application message";
    });

    // The new leadership term must now be used for subsequent sends.
    const std::array<std::uint8_t, 1> body{'8'};
    sender_.send(body.data(), 1);

    ASSERT_EQ(1u, ingress_->offered.size());
    auto hdr = decodeOffered<cluster_sbe::SessionMessageHeader>(ingress_->offered[0]);
    EXPECT_EQ(999, hdr.leadershipTermId());
}

} // namespace
} // namespace org::limitless::phixeron::sequencer
