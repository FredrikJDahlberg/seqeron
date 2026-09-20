// Unit tests for counting a producer's own frames back off its tap. Frames are real ingress frames; each body
// is one int, so a test names a frame by its number. Case for case with PendingSendsTest.java.

#include <array>
#include <cstdint>
#include <cstring>
#include <limits>
#include <vector>

#include <gtest/gtest.h>

#include "org/limitless/seqeron/app/PendingSends.hpp"

#include "org_limitless_seqeron_sbe_frame/Unsequenced.h"
#include "org_limitless_seqeron_sbe_frame/UnsequencedSystem.h"

namespace org::limitless::seqeron::app {
namespace {

namespace frm = sbe::frame;

constexpr std::size_t CAPACITY = 4;
constexpr std::int64_t OWN = 4242;
constexpr std::int64_t SIBLING = 4343;
constexpr std::uint16_t PAYLOAD_ID = 6;

// Takes resends as the cluster's new leader would, recording each frame's number, until told to refuse.
struct FakeSender
{
    bool send(const std::uint8_t* bytes, std::uint16_t)
    {
        if (acceptsLeft == 0)
        {
            return false;
        }
        --acceptsLeft;
        std::int32_t n = 0;
        std::memcpy(&n, bytes + protocol::MIN_INGRESS_LENGTH, sizeof(n));
        sent.push_back(n);
        return true;
    }

    [[nodiscard]] std::int64_t clusterSessionId() const
    {
        return session;
    }

    [[nodiscard]] std::int64_t leadershipTermId() const
    {
        return term;
    }

    std::int64_t term = 0;
    std::int64_t session = OWN;
    int acceptsLeft = std::numeric_limits<int>::max();
    std::vector<std::int32_t> sent;
};

class PendingSendsTest : public testing::Test
{
  protected:
    // Tracks application frame n as placed on session, stamped term.
    void send(const std::int64_t session, const std::int64_t term, const std::int32_t n)
    {
        frm::Unsequenced encoder;
        encoder.wrapAndApplyHeader(frameChars(), 0, m_frame.size());
        encoder.header().sourceId(1).connectionId(2).sessionId(session).payloadId(PAYLOAD_ID);
        encoder.putPayload(reinterpret_cast<const char*>(&n), sizeof(n));
        m_pending.track(m_frame.data(), length(encoder.encodedLength()), session, term);
    }

    void sendSystem(const std::int64_t session, const std::int64_t term, const std::uint16_t eventType,
                    const std::int32_t n)
    {
        frm::UnsequencedSystem encoder;
        encoder.wrapAndApplyHeader(frameChars(), 0, m_frame.size());
        encoder.header().sourceId(1).connectionId(2).sessionId(session).systemEventType(eventType);
        encoder.putBody(reinterpret_cast<const char*>(&n), sizeof(n));
        m_pending.track(m_frame.data(), length(encoder.encodedLength()), session, term);
    }

    // Application frame n arriving on the tap from session.
    void tap(const std::int64_t session, const std::int32_t n)
    {
        tap(session, false, PAYLOAD_ID, n);
    }

    void tap(const std::int64_t session, const bool system, const std::uint16_t id, const std::int32_t n)
    {
        protocol::SequencedEvent event{};
        event.sourceSessionId = session;
        event.system = system;
        event.payloadId = system ? 0 : id;
        event.systemEventType = system ? id : 0;
        event.payload = reinterpret_cast<const char*>(&n);
        event.payloadLength = sizeof(n);
        m_pending.onSequenced(event);
    }

    PendingSends m_pending{ CAPACITY };

  private:
    char* frameChars()
    {
        return reinterpret_cast<char*>(m_frame.data());
    }

    static std::uint16_t length(const std::uint64_t encodedLength)
    {
        return static_cast<std::uint16_t>(frm::MessageHeader::encodedLength() + encodedLength);
    }

    std::array<std::uint8_t, protocol::MAX_INGRESS_LENGTH> m_frame{};
};

TEST_F(PendingSendsTest, EveryFrameSeenLeavesNothingPending)
{
    send(OWN, 1, 1);
    send(OWN, 1, 2);
    EXPECT_EQ(2u, m_pending.size());
    tap(OWN, 1);
    tap(OWN, 2);
    EXPECT_EQ(0u, m_pending.size());
    EXPECT_EQ(0u, m_pending.missing());
    EXPECT_FALSE(m_pending.isFaulted());
}

TEST_F(PendingSendsTest, LeaderChangeAfterEveryFrameLosesNothing)
{
    send(OWN, 1, 1);
    tap(OWN, 1);
    m_pending.onLeadershipChanged(2);
    EXPECT_EQ(0u, m_pending.missing());
    EXPECT_EQ(0u, m_pending.size());
}

TEST_F(PendingSendsTest, FramesNotSeenBeforeALaterTermAreMissing)
{
    send(OWN, 1, 1);
    send(OWN, 1, 2);
    send(OWN, 1, 3);
    tap(OWN, 1);
    EXPECT_EQ(0u, m_pending.missing()) << "still pending until a later term closes the count";
    m_pending.onLeadershipChanged(2);
    EXPECT_EQ(2u, m_pending.missing());
    EXPECT_EQ(2u, m_pending.size());
}

TEST_F(PendingSendsTest, OwnTermsLeadershipChangedDoesNotCountIt)
{
    // A tap still replaying history reaches term 1's opening frame after frames stamped 1 went out.
    send(OWN, 1, 1);
    m_pending.onLeadershipChanged(1);
    EXPECT_EQ(0u, m_pending.missing());
    tap(OWN, 1);
    EXPECT_EQ(0u, m_pending.size());
}

TEST_F(PendingSendsTest, StaleTermFrameIsMissingAtOnce)
{
    // The tap showed the new term before egress brought the NewLeader, so the leader dropped this frame.
    m_pending.onLeadershipChanged(2);
    send(OWN, 1, 1);
    EXPECT_EQ(1u, m_pending.missing());
}

TEST_F(PendingSendsTest, ReplacedSessionsLostFramesStayMissing)
{
    send(OWN, 1, 1);
    send(OWN, 1, 2);
    m_pending.onLeadershipChanged(2);
    send(OWN + 1, 2, 3);
    send(OWN + 1, 2, 4);
    tap(OWN + 1, 3);
    tap(OWN + 1, 4);
    EXPECT_EQ(2u, m_pending.missing());
    EXPECT_EQ(2u, m_pending.size());
    EXPECT_FALSE(m_pending.isFaulted());
}

TEST_F(PendingSendsTest, SiblingFramesAreIgnored)
{
    send(OWN, 1, 1);
    tap(SIBLING, 1);
    tap(SIBLING, 7);
    EXPECT_EQ(1u, m_pending.size());
    EXPECT_FALSE(m_pending.isFaulted());
}

TEST_F(PendingSendsTest, OwnFrameDifferingFromOldestCopyLatchesFault)
{
    // The sequencer rejected frame 1 (S-7), so frame 2 is the next own frame on the tap.
    send(OWN, 1, 1);
    send(OWN, 1, 2);
    tap(OWN, 2);
    EXPECT_TRUE(m_pending.isFaulted());
}

TEST_F(PendingSendsTest, OwnSystemFrameComesBack)
{
    sendSystem(OWN, 1, protocol::CONNECTION_CLOSED, 1);
    tap(OWN, true, protocol::CONNECTION_CLOSED, 1);
    EXPECT_EQ(0u, m_pending.size());
    EXPECT_FALSE(m_pending.isFaulted());
}

TEST_F(PendingSendsTest, ApplicationFrameNeverMatchesSystemOne)
{
    sendSystem(OWN, 1, protocol::CONNECTION_CLOSED, 1);
    tap(OWN, false, protocol::CONNECTION_CLOSED, 1);
    EXPECT_TRUE(m_pending.isFaulted());
}

TEST_F(PendingSendsTest, TrackingIntoAFullRingLatchesFault)
{
    for (std::int32_t n = 0; n < static_cast<std::int32_t>(CAPACITY); ++n)
    {
        send(OWN, 1, n);
    }
    EXPECT_TRUE(m_pending.isFull());
    EXPECT_FALSE(m_pending.isFaulted());
    send(OWN, 1, static_cast<std::int32_t>(CAPACITY));
    EXPECT_TRUE(m_pending.isFaulted());
    EXPECT_EQ(CAPACITY, m_pending.size());
}

TEST_F(PendingSendsTest, NewLeaderHoldsUntilOlderFramesAreSeen)
{
    send(OWN, 1, 1);
    m_pending.onNewLeader(2);
    EXPECT_TRUE(m_pending.isHolding());
    tap(OWN, 1);
    EXPECT_FALSE(m_pending.isHolding());
}

TEST_F(PendingSendsTest, NewTermWithNothingPendingHoldsNothing)
{
    m_pending.onNewLeader(2);
    m_pending.onLeadershipChanged(2);
    EXPECT_FALSE(m_pending.isHolding());
}

TEST_F(PendingSendsTest, TapsLeadershipChangedHoldsBeforeNewLeader)
{
    FakeSender sender{ .term = 1 };
    send(OWN, 1, 1);
    m_pending.onLeadershipChanged(2);
    EXPECT_TRUE(m_pending.isHolding());
    EXPECT_EQ(0u, m_pending.resendMissing(sender));
    EXPECT_TRUE(sender.sent.empty());
}

TEST_F(PendingSendsTest, MissingFramesAreResentOldestFirst)
{
    FakeSender sender{ .term = 2 };
    send(OWN, 1, 1);
    send(OWN, 1, 2);
    send(OWN, 1, 3);
    tap(OWN, 1);
    m_pending.onNewLeader(2);
    m_pending.onLeadershipChanged(2);
    EXPECT_EQ(2u, m_pending.resendMissing(sender));
    EXPECT_EQ((std::vector<std::int32_t>{ 2, 3 }), sender.sent);
    EXPECT_EQ(0u, m_pending.missing());
    EXPECT_FALSE(m_pending.isHolding());
    tap(OWN, 2);
    tap(OWN, 3);
    EXPECT_EQ(0u, m_pending.size());
    EXPECT_FALSE(m_pending.isFaulted());
}

TEST_F(PendingSendsTest, ResendCutShortKeepsHolding)
{
    FakeSender sender{ .term = 2 };
    send(OWN, 1, 1);
    send(OWN, 1, 2);
    send(OWN, 1, 3);
    m_pending.onNewLeader(2);
    m_pending.onLeadershipChanged(2);
    sender.acceptsLeft = 1;
    EXPECT_EQ(1u, m_pending.resendMissing(sender));
    EXPECT_TRUE(m_pending.isHolding());
    EXPECT_EQ(2u, m_pending.missing());
    sender.acceptsLeft = std::numeric_limits<int>::max();
    EXPECT_EQ(2u, m_pending.resendMissing(sender));
    EXPECT_EQ((std::vector<std::int32_t>{ 1, 2, 3 }), sender.sent);
    EXPECT_FALSE(m_pending.isHolding());
    tap(OWN, 1);
    tap(OWN, 2);
    tap(OWN, 3);
    EXPECT_EQ(0u, m_pending.size());
    EXPECT_FALSE(m_pending.isFaulted());
}

TEST_F(PendingSendsTest, ResentFrameMatchesOnTheNewSession)
{
    FakeSender sender{ .term = 2, .session = OWN + 1 };
    send(OWN, 1, 1);
    m_pending.onNewLeader(2);
    m_pending.onLeadershipChanged(2);
    m_pending.resendMissing(sender);
    tap(OWN, 1);
    EXPECT_EQ(1u, m_pending.size());
    tap(OWN + 1, 1);
    EXPECT_EQ(0u, m_pending.size());
}

TEST_F(PendingSendsTest, ResentFrameLostAgainIsResentAgain)
{
    FakeSender sender{ .term = 2 };
    send(OWN, 1, 1);
    m_pending.onNewLeader(2);
    m_pending.onLeadershipChanged(2);
    m_pending.resendMissing(sender);
    m_pending.onNewLeader(3);
    m_pending.onLeadershipChanged(3);
    EXPECT_TRUE(m_pending.isHolding());
    EXPECT_EQ(1u, m_pending.missing());
    sender.term = 3;
    EXPECT_EQ(1u, m_pending.resendMissing(sender));
    EXPECT_EQ((std::vector<std::int32_t>{ 1, 1 }), sender.sent);
    tap(OWN, 1);
    EXPECT_EQ(0u, m_pending.size());
}

TEST_F(PendingSendsTest, FullRingResendsInPlace)
{
    FakeSender sender{ .term = 2 };
    for (std::int32_t n = 0; n < static_cast<std::int32_t>(CAPACITY); ++n)
    {
        send(OWN, 1, n);
    }
    m_pending.onNewLeader(2);
    m_pending.onLeadershipChanged(2);
    EXPECT_EQ(CAPACITY, m_pending.resendMissing(sender));
    EXPECT_EQ((std::vector<std::int32_t>{ 0, 1, 2, 3 }), sender.sent);
    for (std::int32_t n = 0; n < static_cast<std::int32_t>(CAPACITY); ++n)
    {
        tap(OWN, n);
    }
    EXPECT_EQ(0u, m_pending.size());
    EXPECT_FALSE(m_pending.isFaulted());
}

} // namespace
} // namespace org::limitless::seqeron::app
