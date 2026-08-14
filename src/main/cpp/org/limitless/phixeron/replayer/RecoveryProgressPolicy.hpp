#pragma once

#include <cstdint>

namespace org::limitless::phixeron::sequencer {

// Pure decision logic behind ReplayerStreamReceiver's convergence alarm (review-3.md #6). Recovery that
// never converges is correctly CONTAINED — nothing is dispatched from a baseline this node cannot
// establish, so every consumer gate stays shut — but it is also silent: a hole this node's recording
// chain cannot cover, a Replayer that never answers, and an onReplayUnavailable refusal all hold
// isCaughtUp() false indefinitely while the process looks busy.
//
// PROGRESS, not elapsed time. A legitimate cold start has no time bound — it replays the whole log, and
// the log grows through the trading day — so an elapsed deadline either fires on a healthy slow start or
// is too loose to catch anything. A converging recovery always advances globalSeqNo; a non-converging one
// never does. The re-walk loop is the clearest case: it re-delivers the history it already holds and
// de-dupes all of it, so replays keep starting and finishing with the last dispatched globalSeqNo frozen.
// "Nothing dispatched in order for stallMs" is therefore exact for the pathological case and unreachable
// for the healthy one, whatever the log's size.
//
// ALARM, not fence — it reports and returns. Holding is the right response to a baseline that cannot be
// established (review-2 finding 1); the only thing missing was someone saying so. Free of Aeron/logging
// types so it is unit-testable directly, mirroring GatewayRecoveryStallPolicy and GatewayTapLagPolicy.
class RecoveryProgressPolicy
{
  public:
    // stallMs: how long recovery may run without dispatching a single frame before it is called
    // unconvergent. It has to clear the longest legitimate pause with nothing dispatched — waiting on a
    // Replayer slot behind other co-located apps' walks, not any step of this client's own.
    explicit RecoveryProgressPolicy(std::int64_t stallMs)
      : m_stallMs{ stallMs }
    {}

    // Recovery is converging: a frame was dispatched in order. Clears the clock and re-arms the report,
    // so a later episode is reported again rather than swallowed. Returns true only when this ends an
    // episode that HAD been reported — the falling edge, so the caller's gauge clears exactly once
    // rather than on every frame of a healthy stream.
    bool onProgress()
    {
        const bool wasReported = m_reported;
        m_sinceMs = 0;
        m_reported = false;
        return wasReported;
    }

    // Evaluates one observation made while !isCaughtUp(). The first of an episode only anchors the clock
    // — how long recovery had already been running before this call is unknown — so the deadline is
    // measured from here. Returns true once per episode, on the observation that crosses stallMs.
    bool onNoProgress(const std::int64_t nowMs)
    {
        if (m_sinceMs == 0)
        {
            m_sinceMs = nowMs;
            return false;
        }
        if (m_reported || (nowMs - m_sinceMs) < m_stallMs)
        {
            return false;
        }
        m_reported = true;
        return true;
    }

  private:
    std::int64_t m_stallMs;
    std::int64_t m_sinceMs = 0; // 0 = no episode currently timed
    bool m_reported = false;
};

} // namespace org::limitless::phixeron::sequencer
