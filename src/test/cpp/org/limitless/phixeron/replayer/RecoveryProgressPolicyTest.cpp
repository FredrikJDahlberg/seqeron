// Unit tests for the pure convergence verdict behind ReplayerStreamReceiver's recovery alarm
// (review-3.md #6). No Aeron runtime: onProgress()/onNoProgress() are exactly the two signals the
// receiver feeds in — an in-order dispatch, and one duty-cycle observation while !isCaughtUp().

#include <gtest/gtest.h>

#include "org/limitless/phixeron/replayer/RecoveryProgressPolicy.hpp"

namespace org::limitless::phixeron::sequencer {
namespace {

constexpr std::int64_t STALL_MS = 30'000;
// Arbitrary non-zero clock origin — 0 is the policy's "no episode timed" sentinel.
constexpr std::int64_t T0 = 3 * 60 * 60 * 1000;

constexpr std::int64_t
seconds(const std::int64_t s)
{
    return T0 + s * 1000;
}

TEST(RecoveryProgressPolicy, AColdStartThatKeepsDeliveringNeverTripsHoweverLongItRuns)
{
    RecoveryProgressPolicy policy{ STALL_MS };

    // The whole point of measuring progress rather than elapsed time: a full-log replay has no time
    // bound, and this one runs an hour without ever having been caught up.
    for (std::int64_t s = 0; s < 3600; s += 5)
    {
        EXPECT_FALSE(policy.onNoProgress(seconds(s))) << "second " << s << ": recovery is still delivering";
        policy.onProgress();
    }
}

TEST(RecoveryProgressPolicy, RecoveryThatDeliversNothingTripsOnceAtTheDeadline)
{
    RecoveryProgressPolicy policy{ STALL_MS };

    EXPECT_FALSE(policy.onNoProgress(seconds(0))); // anchors the clock; how long it had run is unknown
    EXPECT_FALSE(policy.onNoProgress(seconds(10)));
    EXPECT_FALSE(policy.onNoProgress(seconds(29)));
    EXPECT_TRUE(policy.onNoProgress(seconds(30))) << "nothing dispatched for the whole deadline";

    // Once per episode, not once per duty cycle: the caller logs, and the condition persists for as long
    // as the archive is broken.
    EXPECT_FALSE(policy.onNoProgress(seconds(31)));
    EXPECT_FALSE(policy.onNoProgress(seconds(300)));
}

TEST(RecoveryProgressPolicy, ASingleDispatchMidEpisodeRestartsTheDeadline)
{
    RecoveryProgressPolicy policy{ STALL_MS };
    policy.onNoProgress(seconds(0));
    ASSERT_FALSE(policy.onNoProgress(seconds(29)));

    policy.onProgress(); // one frame got through — this walk is slow, not stuck

    EXPECT_FALSE(policy.onNoProgress(seconds(30))); // anchors a fresh episode rather than inheriting the old clock
    EXPECT_FALSE(policy.onNoProgress(seconds(58)));
    EXPECT_TRUE(policy.onNoProgress(seconds(60)));
}

TEST(RecoveryProgressPolicy, OnlyProgressEndingAReportedEpisodeIsAFallingEdge)
{
    RecoveryProgressPolicy policy{ STALL_MS };

    // A healthy stream calls onProgress on every frame; a gauge driven off this must not be written on
    // every one of them, only on the edge where a reported episode actually ends.
    EXPECT_FALSE(policy.onProgress());
    policy.onNoProgress(seconds(0));
    EXPECT_FALSE(policy.onProgress()) << "an episode that was never reported has no edge to fall from";

    policy.onNoProgress(seconds(10));
    ASSERT_TRUE(policy.onNoProgress(seconds(40)));
    EXPECT_TRUE(policy.onProgress()) << "the reported episode ended here";
    EXPECT_FALSE(policy.onProgress()) << "and only here";
}

TEST(RecoveryProgressPolicy, ALaterEpisodeIsReportedAgainRatherThanSwallowedByTheFirst)
{
    RecoveryProgressPolicy policy{ STALL_MS };
    policy.onNoProgress(seconds(0));
    ASSERT_TRUE(policy.onNoProgress(seconds(30)));

    policy.onProgress(); // recovery converged after all (an operator restarted the node's Replayer)

    EXPECT_FALSE(policy.onNoProgress(seconds(100)));
    EXPECT_TRUE(policy.onNoProgress(seconds(130))) << "a second, distinct episode must report too";
}

} // namespace
} // namespace org::limitless::phixeron::sequencer
