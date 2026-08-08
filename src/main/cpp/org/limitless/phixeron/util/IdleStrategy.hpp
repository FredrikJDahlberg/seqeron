#pragma once

// Aeron's C++ idle strategies (BusySpinIdleStrategy, YieldingIdleStrategy, BackoffIdleStrategy) are
// duck-typed with no common base — normally selected as a compile-time template parameter. That's
// wrong for this project: busy-spin/yielding only pay off when the calling thread owns an isolated
// core, but every phixeron C++ process (FixGateway, OrderExecClient, BasicDataClient) shares a
// handful of cores with the co-located SequencerNode/ReplayerNode and each other — on a dev box or
// the chaos/e2e harness that's a dozen-plus processes on a handful of cores. DynamicIdleStrategy
// wraps the choice in a std::variant so it becomes a PHIXERON_IDLE_STRATEGY env var instead, matching
// SequencerNode's sequencer.idleStrategy / ReplayerNode's replayer.idleStrategy on the Java side.

#include <cctype>
#include <cstdlib>
#include <stdexcept>
#include <string>
#include <variant>

#include "concurrent/BackOffIdleStrategy.h"
#include "concurrent/BusySpinIdleStrategy.h"
#include "concurrent/YieldingIdleStrategy.h"

namespace org::limitless::phixeron::util {

class DynamicIdleStrategy
{
public:
    using Variant = std::variant<aeron::concurrent::BusySpinIdleStrategy, aeron::concurrent::YieldingIdleStrategy,
        aeron::concurrent::BackoffIdleStrategy>;

    explicit DynamicIdleStrategy(Variant strategy) : m_strategy(std::move(strategy))
    {
    }

    inline void idle(int workCount)
    {
        std::visit([workCount](auto& s) { s.idle(workCount); }, m_strategy);
    }

    inline void idle()
    {
        std::visit([](auto& s) { s.idle(); }, m_strategy);
    }

    inline void reset()
    {
        std::visit([](auto& s) { s.reset(); }, m_strategy);
    }

private:
    Variant m_strategy;
};

// Resolves PHIXERON_IDLE_STRATEGY (case-insensitive): "backoff" (default), "yielding", or "busyspin".
inline DynamicIdleStrategy resolveIdleStrategy()
{
    const char* v = std::getenv("PHIXERON_IDLE_STRATEGY");
    std::string name = (v != nullptr && *v != '\0') ? v : "backoff";
    for (char& c : name)
    {
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
    if (name == "busyspin")
    {
        return DynamicIdleStrategy{aeron::concurrent::BusySpinIdleStrategy{}};
    }
    if (name == "yielding")
    {
        return DynamicIdleStrategy{aeron::concurrent::YieldingIdleStrategy{}};
    }
    if (name == "backoff")
    {
        return DynamicIdleStrategy{aeron::concurrent::BackoffIdleStrategy{}};
    }
    throw std::invalid_argument(
        "Unknown PHIXERON_IDLE_STRATEGY=" + name + " (expected 'backoff', 'yielding', or 'busyspin')");
}

}  // namespace org::limitless::phixeron::util
