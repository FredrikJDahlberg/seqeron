package org.limitless.seqeron.helpers;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * What makes "the same generator" a fact rather than a comment: the C++ twin,
 * {@code helpers/SplitMix64Test.cpp}, asserts these same numbers. A seed that produced a different stream
 * on one side would leave the shared seed lists exploring different fault sequences — which is what
 * {@code java.util.Random} against {@code std::mt19937_64} did, silently, and what neither suite could
 * have noticed on its own.
 */
class SplitMix64Test {
    /** The first eight draws from seed 1, in both languages. */
    private static final long[] FROM_ONE = {
        0x910A2DEC89025CC1L, 0xBEEB8DA1658EEC67L, 0xF893A2EEFB32555EL, 0x71C18690EE42C90BL,
        0x71BB54D8D101B5B9L, 0xC34D0BFF90150280L, 0xE099EC6CD7363CA5L, 0x85E7BB0F12278575L,
    };

    /** And from 1597, the last seed on every property test's list. */
    private static final long[] FROM_LAST_SEED = {
        0x2E54B39256EAE37DL, 0xCDA1DC480269FB77L, 0x1F8B427A562D0E6BL, 0x43F4E3D7EB06AC9FL,
        0x55CDA1A87D634607L, 0xB77114355A6174A9L, 0x853CD981E2F645A4L, 0x7FB816A15035ADACL,
    };

    private static final int[] ROLL_100_FROM_ONE = { 65, 19, 90, 35, 61, 48, 45, 33 };
    private static final int[] ROLL_100_FROM_LAST_SEED = { 17, 15, 15, 35, 95, 21, 24, 52 };

    @Test
    @DisplayName("the stream is the same on both sides of the port")
    void streamMatchesTheCppTwin() {
        assertStream(1, FROM_ONE);
        assertStream(1597, FROM_LAST_SEED);
    }

    /**
     * The derivation matters as much as the stream: a signed {@code %} would go negative on half the draws
     * and pick a different branch from the C++ twin on the same number.
     */
    @Test
    @DisplayName("roll() derives the same bounded draws on both sides")
    void rollMatchesTheCppTwin() {
        assertRolls(1, ROLL_100_FROM_ONE);
        assertRolls(1597, ROLL_100_FROM_LAST_SEED);
    }

    @Test
    @DisplayName("every bounded draw is inside its bound, including where the raw draw is negative as a long")
    void rollStaysInsideItsBound() {
        final SplitMix64 rng = new SplitMix64(1);
        for (int i = 0; i < 10_000; i++) {
            final int roll = rng.roll(7);
            assertEquals(true, roll >= 0 && roll < 7, "roll " + i + " left its bound: " + roll);
        }
    }

    private static void assertStream(final long seed, final long[] expected) {
        final SplitMix64 rng = new SplitMix64(seed);
        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], rng.next(), "seed " + seed + ", draw " + i);
        }
    }

    private static void assertRolls(final long seed, final int[] expected) {
        final SplitMix64 rng = new SplitMix64(seed);
        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], rng.roll(100), "seed " + seed + ", roll " + i);
        }
    }
}
