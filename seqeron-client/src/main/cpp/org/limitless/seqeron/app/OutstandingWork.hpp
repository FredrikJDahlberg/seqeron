#pragma once

#include <cstddef>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace org::limitless::seqeron::app {

// Leader-only request/reply work tracked across a failover. Outstanding is replicated (sequenced request to
// sequenced reply); dispatched is local. Emission is at-least-once; the key is the request's clusterSeqNo. Each
// slot's Work is default-constructed once and reused, so a warm tracker allocates nothing per request. The Java twin
// is app/OutstandingWork.java, which carries the rationale; keep the two in step.
template<typename Work>
class OutstandingWork
{
  public:
    OutstandingWork() : m_keys(INITIAL_SLOTS), m_states(INITIAL_SLOTS, FREE), m_works(INITIAL_SLOTS)
    {}

    /**
     * Records a request seen on the sequenced stream, on every replica. Keys increase; one already outstanding is
     * the same request asserted again, and keeps its place.
     *
     * @param key the request's clusterSeqNo
     * @return the request's work, to fill with what dispatching it takes; reused once the request is answered, and
     *         the reference is valid until the next call on this tracker
     * @throws std::invalid_argument for a key no greater than an earlier one that is no longer outstanding
     */
    Work& onRequest(const std::int64_t key)
    {
        if (key <= m_newestKey)
        {
            const std::ptrdiff_t found = find(key);
            if (found < 0)
            {
                throw std::invalid_argument("request " + std::to_string(key) + " is not outstanding and not after " +
                                            std::to_string(m_newestKey));
            }
            return m_works[static_cast<std::size_t>(found)];
        }
        if (m_tail == m_keys.size())
        {
            compact();
        }
        m_newestKey = key;
        m_keys[m_tail] = key;
        m_states[m_tail] = OUTSTANDING;
        ++m_size;
        return m_works[m_tail++];
    }

    /**
     * Records a request's sequenced reply, on every replica: the request is answered.
     *
     * @param key the answered request's clusterSeqNo
     */
    void onReply(const std::int64_t key)
    {
        const std::ptrdiff_t found = find(key);
        if (found < 0)
        {
            return;
        }
        m_states[static_cast<std::size_t>(found)] = FREE;
        --m_size;
        while (m_head < m_tail && m_states[m_head] == FREE)
        {
            ++m_head;
        }
        if (m_head == m_tail)
        {
            m_head = 0;
            m_tail = 0;
        }
    }

    // The leader gate closed: forget what this node dispatched, so the next opening re-dispatches it.
    void onNotLeader()
    {
        for (std::size_t slot = m_head; slot < m_tail; ++slot)
        {
            if (m_states[slot] == DISPATCHED)
            {
                m_states[slot] = OUTSTANDING;
            }
        }
    }

    /**
     * Records that a reply offer failed, so no sequenced reply will come. The request stays outstanding.
     *
     * @param key the clusterSeqNo of the request whose reply was not emitted
     */
    void onReplyNotEmitted(const std::int64_t key)
    {
        if (const std::ptrdiff_t found = find(key); found >= 0)
        {
            m_states[static_cast<std::size_t>(found)] = OUTSTANDING;
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
        for (std::size_t slot = m_head; slot < m_tail; ++slot)
        {
            if (m_states[slot] != OUTSTANDING)
            {
                continue;
            }
            if (!dispatch(m_keys[slot], m_works[slot]))
            {
                break;
            }
            m_states[slot] = DISPATCHED;
            ++dispatchedCount;
        }
        return dispatchedCount;
    }

    [[nodiscard]] std::size_t size() const noexcept
    {
        return m_size;
    }

    [[nodiscard]] bool isDispatched(const std::int64_t key) const
    {
        const std::ptrdiff_t found = find(key);
        return found >= 0 && m_states[static_cast<std::size_t>(found)] == DISPATCHED;
    }

  private:
    static constexpr std::size_t INITIAL_SLOTS = 16;
    static constexpr std::uint8_t FREE = 0;
    static constexpr std::uint8_t OUTSTANDING = 1;
    static constexpr std::uint8_t DISPATCHED = 2; // local to this node

    // The slot holding this outstanding key, or -1.
    [[nodiscard]] std::ptrdiff_t find(const std::int64_t key) const noexcept
    {
        std::size_t low = m_head;
        std::size_t high = m_tail;
        while (low < high)
        {
            const std::size_t mid = low + (high - low) / 2;
            if (m_keys[mid] < key)
            {
                low = mid + 1;
            }
            else if (m_keys[mid] > key)
            {
                high = mid;
            }
            else
            {
                return m_states[mid] == FREE ? -1 : static_cast<std::ptrdiff_t>(mid);
            }
        }
        return -1;
    }

    // Moves the outstanding slots to the front, swapping their work, and doubles if over half are left.
    void compact()
    {
        std::size_t to = 0;
        for (std::size_t from = m_head; from < m_tail; ++from)
        {
            if (m_states[from] == FREE)
            {
                continue;
            }
            if (from != to)
            {
                m_keys[to] = m_keys[from];
                m_states[to] = m_states[from];
                m_states[from] = FREE;
                std::swap(m_works[to], m_works[from]);
            }
            ++to;
        }
        m_head = 0;
        m_tail = to;
        if (m_size > m_keys.size() / 2)
        {
            const std::size_t slots = m_keys.size() * 2;
            m_keys.resize(slots);
            m_states.resize(slots, FREE);
            m_works.resize(slots);
        }
    }

    // Slots [m_head, m_tail) in increasing key order; an answered one stays FREE until compacted away.
    std::vector<std::int64_t> m_keys;
    std::vector<std::uint8_t> m_states;
    std::vector<Work> m_works;
    std::size_t m_head = 0;
    std::size_t m_tail = 0;
    std::size_t m_size = 0;
    std::int64_t m_newestKey = std::numeric_limits<std::int64_t>::min();
};

} // namespace org::limitless::seqeron::app
