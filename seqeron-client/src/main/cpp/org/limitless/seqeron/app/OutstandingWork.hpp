#pragma once

#include <cstddef>
#include <cstdint>
#include <iterator>
#include <list>
#include <unordered_map>
#include <utility>

namespace org::limitless::seqeron::app {

// Leader-only request/reply work tracked across a failover. Outstanding is replicated (sequenced request to
// sequenced reply); dispatched is local. Emission is at-least-once, so key on the request's clusterSeqNo. The
// Java twin is app/OutstandingWork.java, which carries the rationale; keep the two in step.
template<typename Work, typename Key = std::int64_t>
class OutstandingWork
{
  public:
    /**
     * Records a request seen on the sequenced stream, on every replica. Idempotent on the key, keeping its
     * position.
     *
     * @param key  the request's key, normally its clusterSeqNo
     * @param work what dispatching it takes
     */
    void onRequest(const Key& key, Work work)
    {
        if (const auto found = m_outstanding.find(key); found != m_outstanding.end())
        {
            found->second->work = std::move(work);
            return;
        }
        m_order.push_back(Entry{ key, std::move(work), false });
        m_outstanding.emplace(key, std::prev(m_order.end()));
    }

    /**
     * Records a request's sequenced reply, on every replica: the request is answered.
     *
     * @param key the answered request's key
     */
    void onReply(const Key& key)
    {
        if (const auto found = m_outstanding.find(key); found != m_outstanding.end())
        {
            m_order.erase(found->second);
            m_outstanding.erase(found);
        }
    }

    // The leader gate closed: forget what this node dispatched, so the next opening re-dispatches it.
    void onNotLeader()
    {
        for (Entry& entry : m_order)
        {
            entry.dispatched = false;
        }
    }

    /**
     * Records that a reply offer failed, so no sequenced reply will come. The request stays outstanding.
     *
     * @param key the request whose reply was not emitted
     */
    void onReplyNotEmitted(const Key& key)
    {
        if (const auto found = m_outstanding.find(key); found != m_outstanding.end())
        {
            found->second->dispatched = false;
        }
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
        for (Entry& entry : m_order)
        {
            if (entry.dispatched)
            {
                continue;
            }
            if (!dispatch(entry.key, entry.work))
            {
                break;
            }
            entry.dispatched = true;
            ++dispatchedCount;
        }
        return dispatchedCount;
    }

    [[nodiscard]] std::size_t size() const noexcept
    {
        return m_outstanding.size();
    }

    [[nodiscard]] bool isDispatched(const Key& key) const
    {
        const auto found = m_outstanding.find(key);
        return found != m_outstanding.end() && found->second->dispatched;
    }

  private:
    struct Entry
    {
        const Key key;
        Work work;
        bool dispatched; // local to this node
    };

    std::list<Entry> m_order; // request order
    std::unordered_map<Key, typename std::list<Entry>::iterator> m_outstanding;
};

} // namespace org::limitless::seqeron::app
