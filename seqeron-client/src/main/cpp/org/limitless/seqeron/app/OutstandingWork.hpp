#pragma once

#include <cstddef>
#include <cstdint>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace org::limitless::seqeron::app {

// Leader-only request/reply work tracked across a failover. Outstanding is replicated (sequenced request to
// sequenced reply); dispatched is local. Emission is at-least-once, so key on the request's globalSeqNo. The
// Java twin is app/OutstandingWork.java, which carries the rationale; keep the two in step.
template<typename Work, typename Key = std::int64_t>
class OutstandingWork
{
  public:
    /**
     * Records a request seen on the sequenced stream, on every replica. Idempotent on the key, keeping its
     * position.
     *
     * @param key  the request's key, normally its globalSeqNo
     * @param work what dispatching it takes
     */
    void onRequest(const Key& key, Work work)
    {
        if (m_outstanding.insert_or_assign(key, std::move(work)).second)
        {
            m_order.push_back(key);
        }
    }

    /**
     * Records a request's sequenced reply, on every replica: the request is answered.
     *
     * @param key the answered request's key
     */
    void onReply(const Key& key)
    {
        m_outstanding.erase(key);
        m_dispatched.erase(key);
    }

    // The leader gate closed: forget what this node dispatched, so the next opening re-dispatches it.
    void onNotLeader()
    {
        m_dispatched.clear();
    }

    /**
     * Records that a reply offer failed, so no sequenced reply will come. The request stays outstanding.
     *
     * @param key the request whose reply was not emitted
     */
    void onReplyNotEmitted(const Key& key)
    {
        m_dispatched.erase(key);
    }

    /**
     * Offers each undispatched outstanding request, in request order. Leader only, while the gate is open.
     *
     * @param dispatch called as dispatch(key, work); returns false to stop the sweep, and must not call back
     *                 into the tracker
     * @return how many were dispatched by this call
     */
    template<typename DispatchFn>
    int dispatchUndispatched(DispatchFn&& dispatch)
    {
        int dispatchedCount = 0;
        std::size_t live = 0;
        bool stopped = false;
        for (std::size_t i = 0; i < m_order.size(); ++i)
        {
            const Key& key = m_order[i];
            const auto entry = m_outstanding.find(key);
            if (entry != m_outstanding.end())
            {
                m_order[live++] = key;
                if (!stopped && !m_dispatched.contains(key))
                {
                    if (dispatch(key, entry->second))
                    {
                        m_dispatched.insert(key);
                        ++dispatchedCount;
                    }
                    else
                    {
                        stopped = true;
                    }
                }
            }
        }
        m_order.resize(live);
        return dispatchedCount;
    }

    [[nodiscard]] std::size_t size() const noexcept
    {
        return m_outstanding.size();
    }

    [[nodiscard]] bool isDispatched(const Key& key) const
    {
        return m_dispatched.contains(key);
    }

  private:
    std::unordered_map<Key, Work> m_outstanding;
    std::unordered_set<Key> m_dispatched;
    std::vector<Key> m_order; // request order; compacted by each sweep
};

} // namespace org::limitless::seqeron::app
