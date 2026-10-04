using Xunit;

namespace Org.Limitless.Seqeron.Helpers;

/// <summary>
/// What makes "the same generator" a fact rather than a comment: <c>SplitMix64Test.java</c> and
/// <c>SplitMix64Test.cpp</c> assert these same numbers. A seed that produced a different stream on one side would
/// leave the shared seed lists exploring different fault sequences.
/// </summary>
public class SplitMix64Test
{
    // The first eight draws from seed 1, in every language.
    private static readonly ulong[] FromOne = {
        0x910A2DEC89025CC1UL, 0xBEEB8DA1658EEC67UL, 0xF893A2EEFB32555EUL, 0x71C18690EE42C90BUL,
        0x71BB54D8D101B5B9UL, 0xC34D0BFF90150280UL, 0xE099EC6CD7363CA5UL, 0x85E7BB0F12278575UL,
    };

    // And from 1597, the last seed on every property test's list.
    private static readonly ulong[] FromLastSeed = {
        0x2E54B39256EAE37DUL, 0xCDA1DC480269FB77UL, 0x1F8B427A562D0E6BUL, 0x43F4E3D7EB06AC9FUL,
        0x55CDA1A87D634607UL, 0xB77114355A6174A9UL, 0x853CD981E2F645A4UL, 0x7FB816A15035ADACUL,
    };

    private static readonly int[] Roll100FromOne = { 65, 19, 90, 35, 61, 48, 45, 33 };
    private static readonly int[] Roll100FromLastSeed = { 17, 15, 15, 35, 95, 21, 24, 52 };

    [Fact(DisplayName = "the stream is the same on every side of the port")]
    public void StreamMatchesTheTwins()
    {
        AssertStream(1, FromOne);
        AssertStream(1597, FromLastSeed);
    }

    [Fact(DisplayName = "Roll() derives the same bounded draws on every side")]
    public void RollMatchesTheTwins()
    {
        AssertRolls(1, Roll100FromOne);
        AssertRolls(1597, Roll100FromLastSeed);
    }

    [Fact(DisplayName = "every bounded draw is inside its bound, including where the raw draw is negative as a long")]
    public void RollStaysInsideItsBound()
    {
        var rng = new SplitMix64(1);
        for (int i = 0; i < 10_000; i++)
        {
            int roll = rng.Roll(7);
            Assert.True(roll >= 0 && roll < 7, $"roll {i} left its bound: {roll}");
        }
    }

    private static void AssertStream(ulong seed, ulong[] expected)
    {
        var rng = new SplitMix64(seed);
        for (int i = 0; i < expected.Length; i++)
        {
            Assert.Equal(expected[i], rng.Next());
        }
    }

    private static void AssertRolls(ulong seed, int[] expected)
    {
        var rng = new SplitMix64(seed);
        for (int i = 0; i < expected.Length; i++)
        {
            Assert.Equal(expected[i], rng.Roll(100));
        }
    }
}
