// The façades are class templates, so nothing checks them until a translation unit instantiates one. This
// is that unit: it builds both over stub listeners and asserts the pre-start surface — what each answers
// before any node has led or designated it. Everything past start() needs an Aeron runtime and belongs to
// the end-to-end scripts. The Java twins' reference consumer is tools/TestGateway; this side's are
// seqeron-examples' ColocatedApp.cpp and GatewayApp.cpp.

#include <array>
#include <stdexcept>
#include <type_traits>

#include <gtest/gtest.h>

#include "org/limitless/seqeron/app/Application.hpp"
#include "org/limitless/seqeron/app/Consumer.hpp"
#include "org/limitless/seqeron/app/Gateway.hpp"

namespace org::limitless::seqeron::app {
namespace {

// A Payload is handed out, never built: its constructor would put protocol::SequencedEvent in the surface.
// A consumer testing its own handlers builds one through detail::makePayload.
static_assert(!std::is_constructible_v<Payload, const protocol::SequencedEvent&>);

// Records what the façade asked of its consumer; none of it fires before start().
struct StubListener
{
    void onGroupsResolved(const GroupDirectory&)
    {}

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
    void onGroupsResolved(const GroupDirectory&);
    void onLeadershipChanged(bool);
    void onSequenced(const Payload&);
    void onCaughtUp(std::int64_t);
    void onClusterHeartbeat(std::int64_t, std::int64_t);
    void onFenced(ClusterError, const std::string&);
};

static_assert(ApplicationListener<ColocatedOnlyListener>);
static_assert(!GatewayListener<ColocatedOnlyListener>);

// The least a listener provides: the optional callbacks are called only where they exist.
struct MinimalApplicationListener
{
    void onLeadershipChanged(bool)
    {}

    void onSequenced(const Payload&)
    {}

    void onFenced(ClusterError, const std::string&)
    {}
};

struct MinimalGatewayListener
{
    bool onActivated(std::int32_t)
    {
        return true;
    }

    void onStandby()
    {}

    void onSequenced(const Payload&)
    {}

    void onFenced(ClusterError, const std::string&)
    {}
};

static_assert(ApplicationListener<MinimalApplicationListener> && GatewayListener<MinimalGatewayListener>);

TEST(ApplicationFacade, ClosingTheGateRunsTheHookBeforeTheListenerHearsFalse)
{
    struct Recorder : MinimalApplicationListener
    {
        void onLeadershipChanged(bool leading)
        {
            seen.push_back(std::string(leading ? "true:" : "false:") + (*discharged ? "true" : "false"));
        }

        bool* discharged = nullptr;
        std::vector<std::string> seen;
    };

    Recorder listener;
    bool discharged = false;
    listener.discharged = &discharged;
    Application<Recorder> app{
        { .sourceId = 3, .clientId = 21, .memberId = 1, .onNotLeader = [&] { discharged = true; } }, listener
    };
    app.gateEdge(true);
    EXPECT_FALSE(discharged);
    app.gateEdge(false);
    EXPECT_EQ((std::vector<std::string>{ "true:false", "false:true" }), listener.seen);
}

TEST(ApplicationFacade, ClientIdIsRequiredAndEgressChannelIsNot)
{
    StubListener listener;
    EXPECT_THROW((Application<StubListener>{ { .sourceId = 3 }, listener }), std::invalid_argument);
    EXPECT_THROW((Gateway<StubListener>{ { .gatewayName = "GW-T-A" }, listener }), std::invalid_argument);

    Application<StubListener> offCluster{ { .sourceId = 3, .clientId = 21, .memberId = 9, .offCluster = true },
                                          listener };
    EXPECT_FALSE(offCluster.isLeading());
    EXPECT_EQ(3, offCluster.sourceId());
    EXPECT_THROW((Application<StubListener>{
                     { .sourceId = 3, .clientId = 21, .offCluster = true, .ipcConnectTimeoutMs = 4000 }, listener }),
                 std::invalid_argument);
}

TEST(ConsumerFacade, NeedsAClientIdAndAGroupAndIsIdleBeforeStart)
{
    MinimalApplicationListener listener;
    static_assert(ConsumerListener<MinimalApplicationListener>);
    EXPECT_THROW((Consumer<MinimalApplicationListener>{ { .groupId = 1 }, listener }), std::invalid_argument);
    EXPECT_THROW((Consumer<MinimalApplicationListener>{ { .clientId = 7 }, listener }), std::invalid_argument);

    Consumer<MinimalApplicationListener> consumer{ { .clientId = 7, .memberId = 2, .groupId = 1 }, listener };
    EXPECT_FALSE(consumer.isCaughtUp());
    EXPECT_EQ(0, consumer.lastClusterSeqNo());
}

TEST(ApplicationFacade, ConstructsWithOnlyTheRequiredCallbacksAndANotLeaderHook)
{
    MinimalApplicationListener listener;
    bool notLeader = false;
    Application<MinimalApplicationListener> app{
        { .sourceId = 3, .clientId = 21, .memberId = 1, .onNotLeader = [&] { notLeader = true; } }, listener
    };
    EXPECT_FALSE(notLeader);
    MinimalGatewayListener gatewayListener;
    Gateway<MinimalGatewayListener> gateway{ { .gatewayName = "GW-T-A", .clientId = 11, .memberId = 0 },
                                             gatewayListener };
}

// A listener may own its façade as a member, where the listener is still incomplete.
struct OwningListener : StubListener
{
    Gateway<OwningListener> gateway{ { .gatewayName = "GW-T-A", .clientId = 11, .memberId = 0 }, *this };
    Application<OwningListener> app{ { .sourceId = 3, .clientId = 21, .memberId = 1 }, *this };
};

// Takes part in no round; the façades only ask it for its format when a round is under way.
struct StubSnapshotListener : SnapshotListener
{
    std::uint32_t formatVersion() const override
    {
        return 1;
    }

    std::int32_t onSnapshot(std::span<std::uint8_t>, std::int32_t) override
    {
        return 0;
    }

    void onRestore(std::span<const std::uint8_t>, std::int32_t, std::uint32_t) override
    {}
};

TEST(ApplicationFacade, ShutGateDeclinesEveryPublish)
{
    StubListener listener;
    Application<StubListener> app{ { .sourceId = 3, .clientId = 21, .memberId = 1 }, listener };
    const std::array<std::uint8_t, 16> payload{};

    EXPECT_FALSE(app.isCaughtUp());
    EXPECT_EQ(protocol::Publish::Declined, app.publish(5, payload.data(), payload.size()));
    EXPECT_EQ(protocol::Publish::Declined, app.reply(RequestRef{ 2, 9, 1 }, 5, payload.data(), payload.size()));
}

TEST(ApplicationFacade, SnapshotsNeedADirectory)
{
    StubListener listener;
    StubSnapshotListener snapshots;

    EXPECT_THROW((Application<StubListener>{ { .sourceId = 3, .clientId = 21, .snapshots = Snapshots{ snapshots, {} } },
                                             listener }),
                 std::invalid_argument);
}

TEST(GatewayFacade, TakesNoConnectionUntilServing)
{
    StubListener listener;
    Gateway<StubListener> gateway{ { .gatewayName = "GW-T-A", .clientId = 11, .memberId = 0 }, listener };
    const char data[8] = {};

    EXPECT_FALSE(gateway.isCaughtUp());
    EXPECT_EQ(Gateway<StubListener>::UNRESOLVED, gateway.gatewayId());
    EXPECT_EQ(Gateway<StubListener>::NO_CONNECTION, gateway.openConnection(data, sizeof(data)));
    gateway.closeConnection(0);
    gateway.gateClosed();
    EXPECT_FALSE(gateway.isServing());
}

TEST(GatewayFacade, SnapshotsNeedASourceIdThenADirectory)
{
    StubListener listener;
    StubSnapshotListener snapshots;

    EXPECT_THROW(
        (Gateway<StubListener>{
            { .gatewayName = "GW-T-A", .clientId = 11, .snapshots = Snapshots{ snapshots, "unused" } }, listener }),
        std::invalid_argument);
    EXPECT_THROW(
        (Gateway<StubListener>{
            { .gatewayName = "GW-T-A", .clientId = 11, .sourceId = 4, .snapshots = Snapshots{ snapshots, {} } },
            listener }),
        std::invalid_argument);
}

TEST(ApplicationFacade, ShutGateDeclinesAndLeadsNothing)
{
    StubListener listener;
    Application<StubListener> app{ { .sourceId = 3, .clientId = 21, .memberId = 1 }, listener };

    EXPECT_FALSE(app.isLeading()) << "no LeadershipChanged has named this member yet";
    EXPECT_FALSE(app.canPublish()) << "a shut gate publishes nothing whatever ingress is doing";
    EXPECT_EQ(3, app.sourceId());
    EXPECT_EQ(0, app.lastClusterSeqNo()) << "nothing has been dispatched";
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
        .clusterSeqNo = 7, .sourceId = 12, .connectionId = 4, .payloadId = 6, .templateId = 2
    };
    const Payload payload = detail::makePayload(event);

    EXPECT_EQ(7, payload.clusterSeqNo());
    EXPECT_EQ(12, payload.sourceId());
    EXPECT_EQ(4, payload.connectionId());
    EXPECT_EQ(6, payload.payloadId());
    EXPECT_EQ(2, payload.templateId());
}

} // namespace

// Every member of both façades, the snapshot frames' encoders included, which no pre-start case reaches.
template class Application<StubListener>;
template class Gateway<StubListener>;

} // namespace org::limitless::seqeron::app
