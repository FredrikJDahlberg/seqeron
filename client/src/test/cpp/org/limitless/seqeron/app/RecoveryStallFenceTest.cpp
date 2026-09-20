// Unit tests for the recovery-deadline verdict a gateway applies while not caught up. No Aeron runtime:
// isCaughtUp()/onCaughtUp() and the dispatched globalSeqNo are exactly the three signals a gateway feeds in
// off ReplayerStreamReceiver. Case for case with RecoveryStallFenceTest.java.

#include <gtest/gtest.h>

#include "org/limitless/seqeron/app/RecoveryStallFence.hpp"

namespace org::limitless::seqeron::app {
namespace {

constexpr std::int64_t DEADLINE_MS = 60'000;
// Arbitrary non-zero clock origin — 0 is the policy's "no recovery episode timed" sentinel.
constexpr std::int64_t T0 = 3 * 60 * 60 * 1000;
// A frontier that never advances: the non-converging case every deadline test below drives.
constexpr std::int64_t STUCK = 4242;

constexpr std::int64_t seconds(const std::int64_t s)
{
    return T0 + s * 1000;
}

TEST(RecoveryStallFence, ColdStartNeverTrips)
{
    RecoveryStallFence policy{ DEADLINE_MS };

    for (std::int64_t s = 0; s < 3600; s += 30)
    {
        EXPECT_FALSE(policy.onNotCaughtUp(seconds(s), STUCK))
            << "second " << s << ": an instance that has never caught up must not be fenced";
    }
}

TEST(RecoveryStallFence, ConvergentRecoveryNeverTrips)
{
    RecoveryStallFence policy{ DEADLINE_MS };
    policy.onCaughtUp();

    EXPECT_FALSE(policy.onNotCaughtUp(seconds(0), STUCK)); // anchors the recovery clock
    EXPECT_FALSE(policy.onNotCaughtUp(seconds(10), STUCK));
    EXPECT_FALSE(policy.onNotCaughtUp(seconds(30), STUCK));
    policy.onCaughtUp(); // a normal re-walk converges well inside the deadline

    EXPECT_FALSE(policy.onNotCaughtUp(seconds(31), STUCK)) << "a fresh episode, not the old clock carried forward";
    EXPECT_FALSE(policy.onNotCaughtUp(seconds(50), STUCK));
}

TEST(RecoveryStallFence, UnconvergentRecoveryTrips)
{
    RecoveryStallFence policy{ DEADLINE_MS };
    policy.onCaughtUp(); // this instance has been live before — the deadline is armed

    EXPECT_FALSE(policy.onNotCaughtUp(seconds(0), STUCK)); // anchors the recovery clock
    EXPECT_FALSE(policy.onNotCaughtUp(seconds(30), STUCK));
    EXPECT_FALSE(policy.onNotCaughtUp(seconds(59), STUCK));
    EXPECT_TRUE(policy.onNotCaughtUp(seconds(60), STUCK)) << "recovery dispatched nothing for the whole deadline";
    EXPECT_TRUE(policy.onNotCaughtUp(seconds(61), STUCK)) << "stays tripped on every later observation";
}

TEST(RecoveryStallFence, ReconvergingAfterATripResetsTheClock)
{
    RecoveryStallFence policy{ DEADLINE_MS };
    policy.onCaughtUp();
    policy.onNotCaughtUp(seconds(0), STUCK);
    ASSERT_TRUE(policy.onNotCaughtUp(seconds(60), STUCK));

    policy.onCaughtUp(); // recovers before the process is actually fenced (a slow but real re-walk)

    EXPECT_FALSE(policy.onNotCaughtUp(seconds(61), STUCK)) << "anchors a fresh episode rather than inheriting";
    EXPECT_FALSE(policy.onNotCaughtUp(seconds(90), STUCK));
    EXPECT_TRUE(policy.onNotCaughtUp(seconds(121), STUCK));
}

TEST(RecoveryStallFence, DispatchingRecoveryIsNeverFenced)
{
    // The defect this measure exists for: a re-walk of the whole chain is recovery WORKING, and an
    // elapsed-time deadline fenced it regardless of the history it was delivering.
    RecoveryStallFence policy{ DEADLINE_MS };
    policy.onCaughtUp();

    for (std::int64_t s = 0; s < 3600; ++s)
    {
        EXPECT_FALSE(policy.onNotCaughtUp(seconds(s), 1000 + s))
            << "second " << s << ": a recovery advancing its globalSeqNo is converging";
    }
}

TEST(RecoveryStallFence, ProgressRestartsTheDeadline)
{
    RecoveryStallFence policy{ DEADLINE_MS };
    policy.onCaughtUp();

    EXPECT_FALSE(policy.onNotCaughtUp(seconds(0), 1000));
    EXPECT_FALSE(policy.onNotCaughtUp(seconds(59), 1000));
    EXPECT_FALSE(policy.onNotCaughtUp(seconds(59), 1001)) << "one frame dispatched — the deadline restarts here";
    EXPECT_FALSE(policy.onNotCaughtUp(seconds(118), 1001));
    EXPECT_TRUE(policy.onNotCaughtUp(seconds(119), 1001)) << "60s with the frontier frozen at the new value";
}

} // namespace
} // namespace org::limitless::seqeron::app
