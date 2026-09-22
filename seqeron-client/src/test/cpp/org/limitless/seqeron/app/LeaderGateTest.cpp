// Unit tests for the leader gate's edges. (isCaughtUp(), currentLeaderMemberId()) is exactly what a replica
// reads off its ReplayerStreamReceiver each duty cycle. Case for case with LeaderGateTest.java.

#include <gtest/gtest.h>

#include "org/limitless/seqeron/app/detail/LeaderGate.hpp"

namespace org::limitless::seqeron::app::detail {
namespace {

constexpr std::int32_t SELF = 1;
constexpr std::int32_t OTHER = 2;
constexpr std::int32_t NO_LEADER = -1;

using Transition = LeaderGate::Transition;

TEST(LeaderGate, StartsClosed)
{
    LeaderGate gate{ SELF };

    EXPECT_FALSE(gate.isOpen());
    EXPECT_EQ(Transition::None, gate.update(false, NO_LEADER));
}

TEST(LeaderGate, OpensOnce)
{
    LeaderGate gate{ SELF };

    EXPECT_EQ(Transition::Opened, gate.update(true, SELF));
    EXPECT_TRUE(gate.isOpen());
    EXPECT_EQ(Transition::None, gate.update(true, SELF)) << "an edge, not a level";
}

TEST(LeaderGate, StaysClosedUnlessBothHold)
{
    LeaderGate gate{ SELF };

    EXPECT_EQ(Transition::None, gate.update(false, SELF)) << "leader, but still recovering";
    EXPECT_EQ(Transition::None, gate.update(true, OTHER));
    EXPECT_EQ(Transition::None, gate.update(true, NO_LEADER));
    EXPECT_FALSE(gate.isOpen());
}

TEST(LeaderGate, ClosesWhenLeadershipMovesAway)
{
    LeaderGate gate{ SELF };
    gate.update(true, SELF);

    EXPECT_EQ(Transition::Closed, gate.update(true, OTHER));
    EXPECT_FALSE(gate.isOpen());
    EXPECT_EQ(Transition::None, gate.update(true, OTHER));
}

TEST(LeaderGate, ClosesWhenFallingBehind)
{
    LeaderGate gate{ SELF };
    gate.update(true, SELF);

    EXPECT_EQ(Transition::Closed, gate.update(false, SELF));
    EXPECT_EQ(Transition::Opened, gate.update(true, SELF));
}

TEST(LeaderGate, FlipBetweenUpdatesClosesForOneCycle)
{
    LeaderGate gate{ SELF };
    gate.update(true, SELF);

    gate.onLeadershipChanged(); // SELF -> OTHER
    gate.onLeadershipChanged(); // OTHER -> SELF, both applied within one duty cycle

    EXPECT_EQ(Transition::Closed, gate.update(true, SELF)) << "a reply sent during that election may be lost";
    EXPECT_EQ(Transition::Opened, gate.update(true, SELF));
    EXPECT_EQ(Transition::None, gate.update(true, SELF));
}

TEST(LeaderGate, LeadershipChangeWhileClosed)
{
    LeaderGate gate{ SELF };
    gate.onLeadershipChanged();

    EXPECT_EQ(Transition::Opened, gate.update(true, SELF)) << "nothing was dispatched, so no cycle is lost";
}

} // namespace
} // namespace org::limitless::seqeron::app::detail
