#pragma once

#include <atomic>

namespace org::limitless::seqeron::replayer::client {

// Test-only: drops live tap frames before a ReplayerStreamReceiver sees them, so the next frame reads as a
// gap (gap-recovery-test.sh). The caller owns it and hands it to the receiver's constructor.
class TapFaultInjector
{
  public:
    // Drop the next n live tap frames. Lock-free, so callable from a signal handler or any thread.
    void arm(const int n)
    {
        m_pending.fetch_add(n, std::memory_order_relaxed);
    }

  private:
    friend class ReplayerStreamReceiver;

    // Poll thread only, the one decrementer.
    bool dropNext()
    {
        if (m_pending.load(std::memory_order_relaxed) > 0)
        {
            m_pending.fetch_sub(1, std::memory_order_relaxed);
            return true;
        }
        return false;
    }

    static_assert(std::atomic<int>::is_always_lock_free);
    std::atomic<int> m_pending{ 0 };
};

} // namespace org::limitless::seqeron::replayer::client
