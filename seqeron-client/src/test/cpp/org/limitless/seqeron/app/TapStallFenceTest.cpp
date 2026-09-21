// Unit tests for the tap-silence verdict a producer applies once caught up. No Aeron runtime: the catch-up
// transition and the ClusterHeartbeat arrivals are exactly the two signals app::Session feeds in off
// ReplayerStreamReceiver. Case for case with TapStallFenceTest.java.

#include <gtest/gtest.h>

#include "org/limitless/seqeron/app/TapStallFence.hpp"

namespace org::limitless::seqeron::app {
namespace {

constexpr std::int64_t DEADLINE_MS = 20 * 1000;
// Arbitrary clock origin: the fence is only ever read as a difference.
constexpr std::int64_t T0 = 7 * 60 * 60 * 1000;

constexpr std::int64_t seconds(const std::int64_t s)
{
    return T0 + s * 1000;
}

TEST(TapStallFence, ReplayNeverTrips)
{
    TapStallFence fence{ DEADLINE_MS };

    for (std::int64_t s = 0; s < 3600; s += 30)
    {
        EXPECT_FALSE(fence.isStalled(seconds(s))) << "second " << s << ": the fence arms on the first catch-up";
    }
}

TEST(TapStallFence, HeartbeatsHoldItOpen)
{
    TapStallFence fence{ DEADLINE_MS };
    fence.onCaughtUp(seconds(0));

    for (std::int64_t s = 1; s < 600; s++)
    {
        fence.onClusterHeartbeat(seconds(s));
        EXPECT_FALSE(fence.isStalled(seconds(s))) << "second " << s;
    }
}

TEST(TapStallFence, SilenceTrips)
{
    TapStallFence fence{ DEADLINE_MS };
    fence.onCaughtUp(seconds(0));
    fence.onClusterHeartbeat(seconds(5));

    EXPECT_FALSE(fence.isStalled(seconds(24))) << "one second short of the deadline";
    EXPECT_TRUE(fence.isStalled(seconds(25))) << "the deadline measured from the last heartbeat";
}

TEST(TapStallFence, CatchUpReAnchors)
{
    TapStallFence fence{ DEADLINE_MS };
    fence.onCaughtUp(seconds(0));

    EXPECT_TRUE(fence.isStalled(seconds(30))) << "silent through the deadline";
    fence.onCaughtUp(seconds(30));
    EXPECT_FALSE(fence.isStalled(seconds(30))) << "re-converged: the deadline starts again here";
    EXPECT_TRUE(fence.isStalled(seconds(50))) << "and runs from there";
}

} // namespace
} // namespace org::limitless::seqeron::app
