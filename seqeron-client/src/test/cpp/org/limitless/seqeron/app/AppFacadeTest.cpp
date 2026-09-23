// The façades are class templates, so nothing checks them until a translation unit instantiates one. This
// is that unit: it builds both over stub listeners and asserts the pre-start surface — what each answers
// before any node has led or designated it. Everything past start() needs an Aeron runtime and belongs to
// the end-to-end scripts. The Java twins' reference consumer is tools/TestGateway; this side's are
// seqeron-examples' ColocatedApp.cpp and GatewayApp.cpp.

#include <type_traits>

#include <gtest/gtest.h>

#include "org/limitless/seqeron/app/Application.hpp"
#include "org/limitless/seqeron/app/Gateway.hpp"

namespace org::limitless::seqeron::app {
namespace {

// A Payload is handed out, never built: its constructor would put protocol::SequencedEvent in the surface.
// A consumer testing its own handlers builds one through detail::makePayload.
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

static_assert(GatewayListener<StubListener> && ApplicationListener<StubListener>);

// Everything a co-located application needs, and none of a gateway's election or connection callbacks.
struct ColocatedOnlyListener
{
    void onLeadershipChanged(bool);
    void onSequenced(const Payload&);
    void onCaughtUp(std::int64_t);
    void onClusterHeartbeat(std::int64_t, std::int64_t);
    void onFenced(ClusterError, const std::string&);
};

static_assert(ApplicationListener<ColocatedOnlyListener>);
static_assert(!GatewayListener<ColocatedOnlyListener>);

// A listener may own its façade as a member, where the listener is still incomplete.
struct OwningListener : StubListener
{
    Gateway<OwningListener> gateway{ { .gatewayName = "GW-T-A", .clientId = 11, .memberId = 0 }, *this };
    Application<OwningListener> app{ { .sourceId = 3, .clientId = 21, .memberId = 1 }, *this };
};

TEST(ApplicationFacade, ShutGateDeclinesAndLeadsNothing)
{
    StubListener listener;
    Application<StubListener> app{ { .sourceId = 3, .clientId = 21, .memberId = 1 }, listener };

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

TEST(Facade, ListenerMayOwnItsFacade)
{
    OwningListener owner;

    EXPECT_FALSE(owner.gateway.isActivated());
    EXPECT_FALSE(owner.app.isLeading());
}

TEST(Facade, MakePayloadViewsTheEvent)
{
    const protocol::SequencedEvent event{
        .globalSeqNo = 7, .sourceId = 12, .connectionId = 4, .payloadId = 6, .templateId = 2
    };
    const Payload payload = detail::makePayload(event);

    EXPECT_EQ(7, payload.globalSeqNo());
    EXPECT_EQ(12, payload.sourceId());
    EXPECT_EQ(4, payload.connectionId());
    EXPECT_EQ(6, payload.payloadId());
    EXPECT_EQ(2, payload.templateId());
}

} // namespace
} // namespace org::limitless::seqeron::app
