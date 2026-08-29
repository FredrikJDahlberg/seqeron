// Unit tests for GatewayCatalogue — the gateway-identity derivation a FIX gateway does from its
// Gateway basic-data row (doc/todo.md item 8c), name-keyed. Pure logic, no Aeron.

#include "org/limitless/phixeron/basicdata/Gateways.hpp"
#include "gtest/gtest.h"

namespace org::limitless::phixeron::basicdata {
namespace {

TEST(GatewayCatalogue, ResolvesIdentityForAKnownName)
{
    Gateways cat;
    cat.add("GW-A", GatewayIdentity{ 1, 7, 0 });

    const auto id = cat.resolve("GW-A");
    ASSERT_TRUE(id.has_value());
    EXPECT_EQ(1, id->gatewayId);
    EXPECT_EQ(7, id->gatewaySourceId);
    EXPECT_EQ(0, id->preferenceRank);
}

// The fail-closed contract: an unknown name yields no identity, so the caller must not open its gate.
TEST(GatewayCatalogue, ResolvesNulloptForAnUnknownName)
{
    Gateways cat;
    cat.add("GW-A", GatewayIdentity{ 1, 7, 0 });

    EXPECT_FALSE(cat.resolve("GW-B").has_value());
    EXPECT_FALSE(cat.resolve("").has_value());
}

// The active and standby of one logical gateway share gatewaySourceId but differ in gatewayId/rank.
TEST(GatewayCatalogue, ActiveAndStandbyShareSourceIdButDifferInIdAndRank)
{
    Gateways cat;
    cat.add("GW-A", GatewayIdentity{ 1, 7, 0 }); // primary
    cat.add("GW-B", GatewayIdentity{ 2, 7, 1 }); // standby

    const auto a = cat.resolve("GW-A");
    const auto b = cat.resolve("GW-B");
    ASSERT_TRUE(a.has_value());
    ASSERT_TRUE(b.has_value());
    EXPECT_EQ(a->gatewaySourceId, b->gatewaySourceId); // one logical gateway
    EXPECT_NE(a->gatewayId, b->gatewayId);             // distinct instances
    EXPECT_EQ(0, a->preferenceRank);                   // A is the designated primary
    EXPECT_EQ(1, b->preferenceRank);
    EXPECT_EQ(2u, cat.size());
}

// A re-emitted load (leader change mid-load) re-asserts the same row — idempotent, not a second entry.
TEST(GatewayCatalogue, AddIsIdempotentOnName)
{
    Gateways cat;
    cat.add("GW-A", GatewayIdentity{ 1, 7, 0 });
    cat.add("GW-A", GatewayIdentity{ 1, 7, 0 });
    EXPECT_EQ(1u, cat.size());

    // A corrected value for the same name replaces the old one.
    cat.add("GW-A", GatewayIdentity{ 1, 9, 0 });
    EXPECT_EQ(1u, cat.size());
    EXPECT_EQ(9, cat.resolve("GW-A")->gatewaySourceId);
}

// Scopes a GatewayActive to one logical gateway: the frame carries a gatewayId, and a consumer applies
// it only when that id names an instance of its own gatewaySourceId.
TEST(GatewayCatalogue, IsInstanceOfScopesAGatewayIdToItsLogicalGateway)
{
    Gateways cat;
    cat.add("GW-A", GatewayIdentity{ 1, 7, 0 });
    cat.add("GW-B", GatewayIdentity{ 2, 7, 1 });
    cat.add("GW-C", GatewayIdentity{ 3, 8, 0 }); // a different logical gateway

    EXPECT_TRUE(cat.isInstanceOf(1, 7));
    EXPECT_TRUE(cat.isInstanceOf(2, 7));  // the standby is an instance of the same group
    EXPECT_FALSE(cat.isInstanceOf(3, 7)); // names another logical gateway — leaves this group alone
    EXPECT_TRUE(cat.isInstanceOf(3, 8));

    // gatewayId and gatewaySourceId are separate id spaces: a frame carrying the *sourceId* the pair
    // shares matches no instance, so it can no longer designate both of them at once.
    EXPECT_FALSE(cat.isInstanceOf(7, 7));

    // Fail closed before this gateway has resolved its own identity, and on an unknown instance.
    EXPECT_FALSE(cat.isInstanceOf(99, 7));
    EXPECT_FALSE(Gateways{}.isInstanceOf(1, 7));
}

} // namespace
} // namespace org::limitless::phixeron::basicdata
