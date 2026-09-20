// What makes "the same generator" a fact rather than a comment: the Java twin,
// helpers/SplitMix64Test.java, asserts these same numbers. A seed that produced a different stream on one
// side would leave the shared seed lists exploring different fault sequences — which is what
// std::mt19937_64 against java.util.Random did, silently, and what neither suite could have noticed on
// its own.

#include <gtest/gtest.h>

#include <array>
#include <cstdint>

#include "org/limitless/seqeron/helpers/SplitMix64.hpp"

namespace org::limitless::seqeron::helpers {
namespace {

// The first eight draws from seed 1, in both languages.
constexpr std::array<std::uint64_t, 8> FROM_ONE{ 0x910A2DEC89025CC1ULL, 0xBEEB8DA1658EEC67ULL, 0xF893A2EEFB32555EULL,
                                                 0x71C18690EE42C90BULL, 0x71BB54D8D101B5B9ULL, 0xC34D0BFF90150280ULL,
                                                 0xE099EC6CD7363CA5ULL, 0x85E7BB0F12278575ULL };

// And from 1597, the last seed on every property test's list.
constexpr std::array<std::uint64_t, 8> FROM_LAST_SEED{ 0x2E54B39256EAE37DULL, 0xCDA1DC480269FB77ULL,
                                                       0x1F8B427A562D0E6BULL, 0x43F4E3D7EB06AC9FULL,
                                                       0x55CDA1A87D634607ULL, 0xB77114355A6174A9ULL,
                                                       0x853CD981E2F645A4ULL, 0x7FB816A15035ADACULL };

constexpr std::array<int, 8> ROLL_100_FROM_ONE{ 65, 19, 90, 35, 61, 48, 45, 33 };
constexpr std::array<int, 8> ROLL_100_FROM_LAST_SEED{ 17, 15, 15, 35, 95, 21, 24, 52 };

void assertStream(const std::uint64_t seed, const std::array<std::uint64_t, 8>& expected)
{
    SplitMix64 rng{ seed };
    for (std::size_t i = 0; i < expected.size(); ++i)
    {
        EXPECT_EQ(expected[i], rng.next()) << "seed " << seed << ", draw " << i;
    }
}

void assertRolls(const std::uint64_t seed, const std::array<int, 8>& expected)
{
    SplitMix64 rng{ seed };
    for (std::size_t i = 0; i < expected.size(); ++i)
    {
        EXPECT_EQ(expected[i], rng.roll(100)) << "seed " << seed << ", roll " << i;
    }
}

TEST(SplitMix64, StreamMatchesTheJavaTwin)
{
    assertStream(1, FROM_ONE);
    assertStream(1597, FROM_LAST_SEED);
}

// The derivation matters as much as the stream: Java's signed % would go negative on half the draws and
// pick a different branch from this side on the same number.
TEST(SplitMix64, RollMatchesTheJavaTwin)
{
    assertRolls(1, ROLL_100_FROM_ONE);
    assertRolls(1597, ROLL_100_FROM_LAST_SEED);
}

TEST(SplitMix64, RollStaysInsideItsBound)
{
    SplitMix64 rng{ 1 };
    for (int i = 0; i < 10000; ++i)
    {
        const int roll = rng.roll(7);
        EXPECT_TRUE(roll >= 0 && roll < 7) << "roll " << i << " left its bound: " << roll;
    }
}

} // namespace
} // namespace org::limitless::seqeron::helpers
