package org.limitless.seqeron.helpers;

/**
 * The property suites' generator, and the same one in both languages.
 *
 * <p>{@code java.util.Random} and {@code std::mt19937_64} are each their own algorithm, so the shared seed
 * list every property test carries was exploring a different fault sequence per language — two independent
 * searches wearing a matched pair's clothes. splitmix64 is exact 64-bit integer arithmetic throughout, which
 * a two's-complement {@code long} reproduces bit for bit ({@code >>>} is C++'s shift on an unsigned), so one
 * seed is one stream on both sides. {@code SplitMix64Test} pins that against constants committed in both
 * suites.
 *
 * <p>The C++ twin is {@code helpers/SplitMix64.hpp}; keep the two in step.
 */
public final class SplitMix64 {
    private long state;

    public SplitMix64(final long seed) {
        this.state = seed;
    }

    public long next() {
        state += 0x9E3779B97F4A7C15L;
        long z = state;
        z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
        z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
        return z ^ (z >>> 31);
    }

    /**
     * Unsigned modulo: the low bias is the same shape on both sides, which matters here and the bias itself
     * does not. Signed {@code %} would go negative on half the draws.
     */
    public int roll(final int bound) {
        return (int)Long.remainderUnsigned(next(), bound);
    }

    public boolean chance(final int percent) {
        return roll(100) < percent;
    }
}
