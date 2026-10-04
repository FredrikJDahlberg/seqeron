namespace Org.Limitless.Seqeron.Helpers;

/// <summary>
/// The property suites' generator, and the same one in every language: splitmix64 is exact 64-bit integer
/// arithmetic, so one seed is one stream on every side. <see cref="SplitMix64Test"/> pins that against constants
/// committed in each suite. <c>SplitMix64.java</c> and <c>SplitMix64.hpp</c> are its twins; keep the three in step.
/// </summary>
internal sealed class SplitMix64
{
    private ulong _state;

    public SplitMix64(ulong seed)
    {
        _state = seed;
    }

    public ulong Next()
    {
        _state += 0x9E3779B97F4A7C15UL;
        ulong z = _state;
        z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9UL;
        z = (z ^ (z >> 27)) * 0x94D049BB133111EBUL;
        return z ^ (z >> 31);
    }

    /// <summary>Unsigned modulo, as the twins take it: the low bias is then identical on every side.</summary>
    public int Roll(int bound)
    {
        return (int)(Next() % (ulong)bound);
    }

    public bool Chance(int percent)
    {
        return Roll(100) < percent;
    }
}
