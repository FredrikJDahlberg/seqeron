// Unit tests for the gateway election lifecycle, merged from phixeron's acceptor and initiator tests.
// Topology: GW-A (id 5) and GW-B (id 6) under gatewaySourceId 6, beside another pair (ids 1 and 2) under 0.
// Case for case with GatewayLifecycleTest.java.

#include <algorithm>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "org/limitless/seqeron/app/GatewayLifecycle.hpp"

namespace org::limitless::seqeron::app {
namespace {

constexpr const char* ME = "GW-A";
constexpr std::int32_t MY_ID = 5;
constexpr std::int32_t SIBLING_ID = 6;
constexpr std::int32_t MY_SOURCE_ID = 6;
constexpr std::int32_t OTHER_PAIR_ID = 1;
constexpr std::int32_t OTHER_PAIR_SOURCE_ID = 0;

struct RecordingActions
{
    void identityResolved(const std::int32_t gatewayId, const std::int32_t gatewaySourceId, std::int32_t)
    {
        calls.push_back("identity(" + std::to_string(gatewayId) + "," + std::to_string(gatewaySourceId) + ")");
    }

    bool publishGatewayStarted(const std::int32_t gatewayId)
    {
        calls.push_back("GatewayStarted(" + std::to_string(gatewayId) + ")");
        return gatewayStartedLands;
    }

    bool openGate()
    {
        calls.emplace_back("open");
        return gateOpens;
    }

    void closeGate()
    {
        calls.emplace_back("close");
    }

    [[nodiscard]] long count(const std::string& call) const
    {
        return std::ranges::count(calls, call);
    }

    std::vector<std::string> calls;
    bool gatewayStartedLands = true;
    bool gateOpens = true;
};

using Lifecycle = GatewayLifecycle<RecordingActions>;
using State = Lifecycle::State;

const std::string MY_STARTED = "GatewayStarted(" + std::to_string(MY_ID) + ")";

class GatewayLifecycleTest : public testing::Test
{
  protected:
    void loadTopology()
    {
        lifecycle.onGatewayRegistered(OTHER_PAIR_ID, OTHER_PAIR_SOURCE_ID, "OTHER-A", 0);
        lifecycle.onGatewayRegistered(2, OTHER_PAIR_SOURCE_ID, "OTHER-B", 1);
        lifecycle.onGatewayRegistered(MY_ID, MY_SOURCE_ID, ME, 0);
        lifecycle.onGatewayRegistered(SIBLING_ID, MY_SOURCE_ID, "GW-B", 1);
    }

    void becomeServing()
    {
        loadTopology();
        lifecycle.onGatewayActive(MY_ID);
        lifecycle.onCaughtUp();
        lifecycle.advance();
    }

    RecordingActions actions;
    Lifecycle lifecycle{ ME, actions };
};

TEST_F(GatewayLifecycleTest, ResolvesItsIdentityFromTheRowNamingIt)
{
    loadTopology();
    EXPECT_EQ(MY_ID, lifecycle.gatewayId());
    EXPECT_EQ(MY_SOURCE_ID, lifecycle.gatewaySourceId());
    EXPECT_EQ(std::vector<std::string>{ "identity(5,6)" }, actions.calls);
}

TEST_F(GatewayLifecycleTest, IgnoresAnActivationBeforeAnyRowNamesThisInstance)
{
    lifecycle.onGatewayActive(MY_ID);
    loadTopology();
    lifecycle.onCaughtUp();
    EXPECT_FALSE(lifecycle.isActivated());
    EXPECT_EQ(0, lifecycle.advance());
}

TEST_F(GatewayLifecycleTest, AnActivationSeenWhileReplayingOpensOnlyOnceCaughtUp)
{
    loadTopology();
    lifecycle.onGatewayActive(MY_ID);
    EXPECT_EQ(State::Replaying, lifecycle.state());
    EXPECT_EQ(0, lifecycle.advance());
    EXPECT_EQ(0, actions.count("open"));

    EXPECT_TRUE(lifecycle.onCaughtUp());
    EXPECT_FALSE(lifecycle.onCaughtUp());
    EXPECT_EQ(1, lifecycle.advance());
    EXPECT_EQ(State::Serving, lifecycle.state());
}

TEST_F(GatewayLifecycleTest, StaysShutWhenCaughtUpButNotActivated)
{
    loadTopology();
    lifecycle.onCaughtUp();
    EXPECT_EQ(State::Passive, lifecycle.state());
    EXPECT_EQ(0, lifecycle.advance());
    EXPECT_FALSE(lifecycle.isServing());
    EXPECT_EQ(0, actions.count("open"));
}

TEST_F(GatewayLifecycleTest, PublishesGatewayStartedBeforeOpeningTheGate)
{
    becomeServing();
    EXPECT_TRUE(lifecycle.isServing());
    EXPECT_EQ((std::vector<std::string>{ "identity(5,6)", MY_STARTED, "open" }), actions.calls);
}

TEST_F(GatewayLifecycleTest, DoesNotOpenTheGateWhileGatewayStartedIsBackPressured)
{
    actions.gatewayStartedLands = false;
    becomeServing();
    EXPECT_EQ(State::Passive, lifecycle.state());
    EXPECT_EQ(0, actions.count("open"));

    actions.gatewayStartedLands = true;
    EXPECT_EQ(1, lifecycle.advance());
    EXPECT_EQ(State::Serving, lifecycle.state());
}

TEST_F(GatewayLifecycleTest, RetriesAGateThatDidNotOpenWithoutRegisteringAgain)
{
    actions.gateOpens = false;
    becomeServing();
    EXPECT_EQ(State::Passive, lifecycle.state());

    actions.gateOpens = true;
    EXPECT_EQ(1, lifecycle.advance());
    EXPECT_EQ(State::Serving, lifecycle.state());
    EXPECT_EQ(1, actions.count(MY_STARTED)) << "once per activation, not per attempt";
    EXPECT_EQ(2, actions.count("open"));
}

TEST_F(GatewayLifecycleTest, AGateThatClosesByItselfReopensWithoutRegisteringAgain)
{
    becomeServing();
    lifecycle.onGateClosed();
    EXPECT_EQ(State::Passive, lifecycle.state());
    EXPECT_EQ(0, actions.count("close")) << "the gate is already closed";

    EXPECT_EQ(1, lifecycle.advance());
    EXPECT_EQ(1, actions.count(MY_STARTED));
    EXPECT_EQ(2, actions.count("open"));
}

TEST_F(GatewayLifecycleTest, StandsDownWhenAGatewayActiveNamesTheSibling)
{
    becomeServing();
    lifecycle.onGatewayActive(SIBLING_ID);
    EXPECT_EQ(State::Passive, lifecycle.state());
    EXPECT_FALSE(lifecycle.isActivated());
    EXPECT_EQ(1, actions.count("close"));
    EXPECT_EQ(0, lifecycle.advance());
}

TEST_F(GatewayLifecycleTest, ReopensOnASecondDesignationAsANewEpoch)
{
    becomeServing();
    lifecycle.onGatewayActive(SIBLING_ID);
    lifecycle.onGatewayActive(MY_ID);
    EXPECT_EQ(1, lifecycle.advance());
    EXPECT_EQ(State::Serving, lifecycle.state());
    EXPECT_EQ(2, actions.count(MY_STARTED));
}

TEST_F(GatewayLifecycleTest, IgnoresAnotherLogicalGatewaysElection)
{
    becomeServing();
    lifecycle.onGatewayActive(OTHER_PAIR_ID);
    EXPECT_EQ(State::Serving, lifecycle.state());
    EXPECT_TRUE(lifecycle.isActivated());
    EXPECT_EQ(0, actions.count("close"));
}

TEST_F(GatewayLifecycleTest, IgnoresAnActivationForAnUnknownGateway)
{
    becomeServing();
    lifecycle.onGatewayActive(99);
    EXPECT_EQ(State::Serving, lifecycle.state());
    EXPECT_TRUE(lifecycle.isActivated());
}

TEST_F(GatewayLifecycleTest, ASupersededActivationReplayedOnRestartLeavesItPassive)
{
    loadTopology();
    lifecycle.onGatewayActive(MY_ID);      // history: this instance once served
    lifecycle.onGatewayActive(SIBLING_ID); // and was replaced
    lifecycle.onCaughtUp();
    EXPECT_EQ(0, lifecycle.advance());
    EXPECT_EQ(State::Passive, lifecycle.state());
    EXPECT_EQ(0, actions.count("close")) << "never opened, so nothing to close";
    EXPECT_EQ(0, actions.count(MY_STARTED));
}

TEST_F(GatewayLifecycleTest, RefusesASessionAcquiredWhileTheGateIsShut)
{
    loadTopology();
    lifecycle.onCaughtUp();
    EXPECT_FALSE(lifecycle.onSessionAcquired());

    lifecycle.onGatewayActive(MY_ID);
    lifecycle.advance();
    EXPECT_TRUE(lifecycle.onSessionAcquired());

    lifecycle.onGatewayActive(SIBLING_ID); // the gate closed mid-logon
    EXPECT_FALSE(lifecycle.onSessionAcquired());
}

} // namespace
} // namespace org::limitless::seqeron::app
