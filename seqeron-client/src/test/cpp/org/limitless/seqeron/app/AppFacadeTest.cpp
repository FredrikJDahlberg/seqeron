// The façades are class templates, so nothing checks them until a translation unit instantiates one. This
// is that unit: it builds both over stub listeners and asserts the pre-start surface — what each answers
// before any node has led or designated it. Everything past start() needs an Aeron runtime and belongs to
// the end-to-end scripts. The Java twins' reference consumer is tools/TestGateway; this side has none.

#include <type_traits>

#include <gtest/gtest.h>

#include "org/limitless/seqeron/app/ColocatedApplication.hpp"
#include "org/limitless/seqeron/app/Gateway.hpp"

namespace org::limitless::seqeron::app {
namespace {

// A Payload is handed out, never built: its constructor would put protocol::SequencedEvent in the surface.
static_assert(!std::is_constructible_v<Payload, const protocol::SequencedEvent&>);

// Records what the façade asked of its consumer; none of it fires before start().
struct StubListener
{
    bool onActivated(std::int32_t)
    {
        return true;
    }

    void onStandby()
    {}

    void onLeadershipChanged(bool leading)
    {
        leadingSeen = leading;
    }

    void onSequenced(const Payload&)
    {}

    void onConnectionOpened(std::int32_t, const char*, std::size_t)
    {}

    void onConnectionClosed(std::int32_t)
    {}

    void onCaughtUp(std::int64_t)
    {}

    void onClusterHeartbeat(std::int64_t, std::int64_t)
    {}

    void onFenced(ClusterError, const std::string&)
    {}

    bool leadingSeen = false;
};

TEST(ColocatedApplicationFacade, ShutGateDeclinesAndLeadsNothing)
{
    StubListener listener;
    ColocatedApplication<StubListener> app{ { .sourceId = 3, .clientId = 21, .memberId = 1 }, listener };

    EXPECT_FALSE(app.isLeading()) << "no LeadershipChanged has named this member yet";
    EXPECT_FALSE(app.canPublish()) << "a shut gate publishes nothing whatever ingress is doing";
    EXPECT_EQ(3, app.sourceId());
    EXPECT_EQ(0, app.lastGlobalSeqNo()) << "nothing has been dispatched";
}

TEST(GatewayFacade, UndesignatedInstanceServesNothing)
{
    StubListener listener;
    Gateway<StubListener> gateway{ { .gatewayName = "GW-T-A", .clientId = 11, .memberId = 0 }, listener };

    EXPECT_FALSE(gateway.isActivated()) << "no GatewayActive has named this instance";
    EXPECT_FALSE(gateway.isServing());
    EXPECT_FALSE(gateway.canAccept());
    EXPECT_EQ(Gateway<StubListener>::NO_CONNECTION, gateway.openConnection())
        << "a connection taken while standing down is refused rather than queued";
    EXPECT_EQ(Gateway<StubListener>::UNRESOLVED, gateway.sourceId()) << "no GatewayRegistered row names it yet";
}

} // namespace
} // namespace org::limitless::seqeron::app
