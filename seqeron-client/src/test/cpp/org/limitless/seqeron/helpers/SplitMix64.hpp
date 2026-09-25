#pragma once

// The property suites' generator, and the same one in both languages.
//
// std::mt19937_64 and java.util.Random are each their own algorithm, so the shared seed list every
// property test carries was exploring a different fault sequence per language — two independent
// searches wearing a matched pair's clothes. splitmix64 is exact 64-bit integer arithmetic throughout,
// which Java's two's-complement long reproduces bit for bit (>>> is the logical shift), so one seed is
// one stream on both sides. SplitMix64Test pins that against constants committed in both suites.
//
// The Java twin is helpers/SplitMix64.java; keep the two in step.

#include <cstdint>

namespace org::limitless::seqeron::helpers {

class SplitMix64
{
  public:
    explicit SplitMix64(const std::uint64_t seed) : m_state(seed)
    {}

    std::uint64_t next()
    {
        m_state += 0x9E3779B97F4A7C15ULL;
        std::uint64_t z = m_state;
        z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
        z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
        return z ^ (z >> 31);
    }

    // Unsigned modulo: the low bias is identical on both sides, which matters here and the bias
    // itself does not.
    int roll(const int bound)
    {
        return static_cast<int>(next() % static_cast<std::uint64_t>(bound));
    }

    bool chance(const int percent)
    {
        return roll(100) < percent;
    }

  private:
    std::uint64_t m_state;
};

} // namespace org::limitless::seqeron::helpers
