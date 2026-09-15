// Unit tests for the staleness verdict on live ClusterHeartbeats. No Aeron runtime: (clusterTimestampNs,
// receiveTimeNs) is exactly the pair a client reads off a heartbeat's SequencedEvent, and gating on
// isCaughtUp() is the caller's job. Case for case with TapLagMonitorTest.java.

#include <gtest/gtest.h>

#include "org/limitless/seqeron/app/TapLagMonitor.hpp"

namespace org::limitless::seqeron::app {
namespace {

constexpr std::int64_t THRESHOLD_NS = 20'000'000'000;
// Arbitrary consensus-clock origin; only differences matter.
constexpr std::int64_t T0 = 3LL * 60 * 60 * 1'000'000'000;

constexpr std::int64_t ms(const std::int64_t value)
{
    return value * 1'000'000;
}

// The heartbeat stamped s seconds after T0.
constexpr std::int64_t heartbeat(const std::int64_t s)
{
    return T0 + s * 1'000'000'000;
}

TEST(TapLagMonitor, HealthyHeartbeatsCrossNoEdge)
{
    TapLagMonitor monitor{ THRESHOLD_NS };

    for (std::int64_t s = 0; s < 300; ++s)
    {
        EXPECT_EQ(TapLag::None, monitor.onClusterHeartbeat(heartbeat(s), heartbeat(s) + ms(2)))
            << "second " << s << ": a couple of ms of delivery latency is not staleness";
    }
    EXPECT_FALSE(monitor.isStale());
    EXPECT_EQ(ms(2), monitor.peakLagNs());
    EXPECT_EQ(300, monitor.sampleCount());
}

TEST(TapLagMonitor, RetainedFramesAreNotStale)
{
    TapLagMonitor monitor{ THRESHOLD_NS };

    // 90 s of heartbeats retained during a 90 s walk, each captured 3 ms after it was stamped, all
    // dispatched in one burst at the seam.
    for (std::int64_t s = 0; s < 90; ++s)
    {
        EXPECT_EQ(TapLag::None, monitor.onClusterHeartbeat(heartbeat(s), heartbeat(s) + ms(3)))
            << "second " << s << ": a retained frame arrived on time and was merely held";
    }
    EXPECT_FALSE(monitor.isStale());
}

TEST(TapLagMonitor, LagReachingTheThresholdFiresOnce)
{
    TapLagMonitor monitor{ THRESHOLD_NS };

    EXPECT_EQ(TapLag::None, monitor.onClusterHeartbeat(heartbeat(0), heartbeat(0) + ms(5'000)));
    EXPECT_EQ(TapLag::None, monitor.onClusterHeartbeat(heartbeat(1), heartbeat(1) + THRESHOLD_NS - 1));
    EXPECT_EQ(TapLag::BecameStale, monitor.onClusterHeartbeat(heartbeat(2), heartbeat(2) + THRESHOLD_NS));
    EXPECT_TRUE(monitor.isStale());

    // Still stale, but the caller must not be made to log at the 1 Hz sample rate.
    EXPECT_EQ(TapLag::None, monitor.onClusterHeartbeat(heartbeat(3), heartbeat(3) + ms(45'000)));
    EXPECT_EQ(TapLag::None, monitor.onClusterHeartbeat(heartbeat(4), heartbeat(4) + ms(60'000)));
    EXPECT_EQ(ms(60'000), monitor.peakLagNs());
}

TEST(TapLagMonitor, RecoveringFiresTheFreshEdgeAndReArms)
{
    TapLagMonitor monitor{ THRESHOLD_NS };
    ASSERT_EQ(TapLag::BecameStale, monitor.onClusterHeartbeat(heartbeat(0), heartbeat(0) + ms(30'000)));

    EXPECT_EQ(TapLag::BecameFresh, monitor.onClusterHeartbeat(heartbeat(1), heartbeat(1) + ms(8)));
    EXPECT_FALSE(monitor.isStale());
    EXPECT_EQ(TapLag::None, monitor.onClusterHeartbeat(heartbeat(2), heartbeat(2) + ms(8)));

    // A second episode reports again rather than staying silent because the first one was already told.
    EXPECT_EQ(TapLag::BecameStale, monitor.onClusterHeartbeat(heartbeat(3), heartbeat(3) + ms(25'000)));
    EXPECT_EQ(ms(30'000), monitor.peakLagNs()) << "peak is over the whole run, not the current episode";
}

TEST(TapLagMonitor, ClockSkewIsReportedOnce)
{
    // A clock behind the leader's by more than the threshold makes the gauge fail open — a real 20 s lag
    // would read as fresh — so it is reported rather than quietly absorbed.
    TapLagMonitor monitor{ THRESHOLD_NS };

    EXPECT_EQ(TapLag::SkewSuspected, monitor.onClusterHeartbeat(heartbeat(0), heartbeat(0) - ms(35'000)));
    EXPECT_EQ(TapLag::None, monitor.onClusterHeartbeat(heartbeat(1), heartbeat(1) - ms(35'000)))
        << "a misconfigured clock is a standing fault, not a per-heartbeat event";
    EXPECT_FALSE(monitor.isStale());
    EXPECT_EQ(0, monitor.peakLagNs()) << "a negative sample must not become the peak";
}

TEST(TapLagMonitor, SmallNegativeLagIsQuiet)
{
    // An offset between two disciplined clocks reads as a small negative lag; it is not a fault.
    TapLagMonitor monitor{ THRESHOLD_NS };

    EXPECT_EQ(TapLag::None, monitor.onClusterHeartbeat(heartbeat(0), heartbeat(0) - 1));
    EXPECT_EQ(TapLag::None, monitor.onClusterHeartbeat(heartbeat(1), heartbeat(1) - ms(250)));
    EXPECT_FALSE(monitor.isStale());
}

} // namespace
} // namespace org::limitless::seqeron::app
