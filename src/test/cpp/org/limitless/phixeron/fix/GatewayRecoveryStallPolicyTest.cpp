// Unit tests for the pure recovery-deadline verdict behind FixGateway::checkTapStall's !isCaughtUp()
// branch (doc/review-2026-07-25.md #1). No Aeron runtime: isCaughtUp()/onCaughtUp() are exactly the two
// signals FixGateway feeds in off ReplayerStreamReceiver.

#include <gtest/gtest.h>

#include "org/limitless/phixeron/fix/GatewayRecoveryStallPolicy.hpp"

namespace org::limitless::phixeron::fix {
namespace {

constexpr std::int64_t DEADLINE_MS = 60'000;
// Arbitrary non-zero clock origin — 0 is the policy's "no recovery episode timed" sentinel.
constexpr std::int64_t T0 = 3 * 60 * 60 * 1000;

constexpr std::int64_t seconds(const std::int64_t s)
{
    return T0 + s * 1000;
}

TEST(GatewayRecoveryStallPolicy, ColdStartNeverTripsRegardlessOfHowLongItTakes)
{
    GatewayRecoveryStallPolicy policy{DEADLINE_MS};

    for (std::int64_t s = 0; s < 3600; s += 30)
    {
        EXPECT_FALSE(policy.onNotCaughtUp(seconds(s))) << "second " << s
            << ": a cold start that has never caught up must not be fenced, however long it takes";
    }
}

TEST(GatewayRecoveryStallPolicy, RecoveryThatConvergesWithinTheDeadlineNeverTrips)
{
    GatewayRecoveryStallPolicy policy{DEADLINE_MS};
    policy.onCaughtUp();

    EXPECT_FALSE(policy.onNotCaughtUp(seconds(0)));   // anchors the recovery clock
    EXPECT_FALSE(policy.onNotCaughtUp(seconds(10)));
    EXPECT_FALSE(policy.onNotCaughtUp(seconds(30)));
    policy.onCaughtUp();  // a normal re-walk converges well inside the deadline

    // Re-arms cleanly for the next episode rather than carrying the old clock forward.
    EXPECT_FALSE(policy.onNotCaughtUp(seconds(31)));
    EXPECT_FALSE(policy.onNotCaughtUp(seconds(50)));
}

TEST(GatewayRecoveryStallPolicy, UnconvergentRecoveryOnAnAlreadyActiveInstanceTripsAtTheDeadline)
{
    GatewayRecoveryStallPolicy policy{DEADLINE_MS};
    policy.onCaughtUp();  // this instance has been live before — the deadline is armed

    EXPECT_FALSE(policy.onNotCaughtUp(seconds(0)));   // anchors the recovery clock
    EXPECT_FALSE(policy.onNotCaughtUp(seconds(30)));
    EXPECT_FALSE(policy.onNotCaughtUp(seconds(59)));
    EXPECT_TRUE(policy.onNotCaughtUp(seconds(60))) << "continuous recovery reached the deadline";
    EXPECT_TRUE(policy.onNotCaughtUp(seconds(61))) << "stays tripped on every later observation";
}

TEST(GatewayRecoveryStallPolicy, ReconvergingAfterATripResetsTheClockForTheNextEpisode)
{
    GatewayRecoveryStallPolicy policy{DEADLINE_MS};
    policy.onCaughtUp();
    policy.onNotCaughtUp(seconds(0));
    ASSERT_TRUE(policy.onNotCaughtUp(seconds(60)));

    policy.onCaughtUp();  // recovers before the process is actually fenced (e.g. a slow but real re-walk)

    EXPECT_FALSE(policy.onNotCaughtUp(seconds(61)));  // anchors a fresh episode, does not inherit the old clock
    EXPECT_FALSE(policy.onNotCaughtUp(seconds(90)));
    EXPECT_TRUE(policy.onNotCaughtUp(seconds(121)));
}

}  // namespace
}  // namespace org::limitless::phixeron::fix
