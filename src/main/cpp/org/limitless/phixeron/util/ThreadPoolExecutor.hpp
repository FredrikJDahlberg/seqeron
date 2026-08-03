#pragma once

#include <atomic>
#include <condition_variable>
#include <deque>
#include <functional>
#include <mutex>
#include <thread>
#include <vector>

namespace org::limitless::phixeron::util {

// Models "corePoolSize == maxPoolSize == poolSize with a zero-capacity queue" (the Java version's
// ThreadPoolExecutor + SynchronousQueue): a query only starts if a slot is immediately free.
// tryReserveSlot()/execute() are split so a caller can find out whether a slot was available
// before committing to (moving) the query it would run.
//
// A fixed pool of poolSize persistent workers (doc/audit.md C3) backs the queries admission
// already caps: since a query only ever starts once a slot is free, the pool never needs more
// workers than that to keep every admitted query running immediately, and no query pays a
// clone()/stack-alloc for a thread that's spun up and torn down just for it.
class ThreadPoolExecutor
{
public:
    explicit ThreadPoolExecutor(int poolSize) : m_poolSize{poolSize}
    {
        m_workers.reserve(poolSize);
        for (int i = 0; i < poolSize; ++i)
        {
            m_workers.emplace_back([this] { workerLoop(); });
        }
    }

    ~ThreadPoolExecutor()
    {
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_stopping = true;
        }
        m_cv.notify_all();
        for (auto& worker : m_workers)
        {
            worker.join();
        }
    }

    ThreadPoolExecutor(const ThreadPoolExecutor&) = delete;
    ThreadPoolExecutor& operator=(const ThreadPoolExecutor&) = delete;

    bool tryReserveSlot()
    {
        const int current = m_inFlight.fetch_add(1) + 1;
        if (current > m_poolSize)
        {
            m_inFlight.fetch_sub(1);
            return false;
        }
        return true;
    }

    // Queues fn on a worker thread; must only be called after a successful
    // tryReserveSlot(). Releases the slot when fn returns.
    void execute(std::function<void()> fn)
    {
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_tasks.push_back(std::move(fn));
        }
        m_cv.notify_one();
    }

private:
    void workerLoop()
    {
        while (true)
        {
            std::function<void()> task;
            {
                std::unique_lock<std::mutex> lock(m_mutex);
                m_cv.wait(lock, [this] { return m_stopping || !m_tasks.empty(); });
                if (m_tasks.empty())
                {
                    return;  // stopping, and every queued task has already drained
                }
                task = std::move(m_tasks.front());
                m_tasks.pop_front();
            }
            task();
            m_inFlight.fetch_sub(1);
        }
    }

    int m_poolSize;
    std::atomic<int> m_inFlight{0};
    std::mutex m_mutex;
    std::condition_variable m_cv;
    std::deque<std::function<void()>> m_tasks;
    bool m_stopping{false};
    std::vector<std::thread> m_workers;
};

}  // namespace org::limitless::phixeron::util
