// AsyncLoggerSink: the default sink's queueing, drop count, Fault wait and close, case for case with Java's.

#include <gtest/gtest.h>

#include <chrono>
#include <cstring>
#include <future>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "org/limitless/seqeron/util/Logger.hpp"

namespace {

using namespace org::limitless::seqeron::util;

LoggerEvent event(const Severity severity, const std::string& text)
{
    LoggerEvent result{ .component = component::Cluster, .severity = severity, .code = eventCode::Info };
    std::memcpy(result.text.data(), text.data(), text.size());
    result.textLen = static_cast<std::uint8_t>(text.size());
    return result;
}

std::string textOf(const LoggerEvent& event)
{
    return { event.text.data(), event.textLen };
}

// Records each event and the thread that wrote it; block() holds the writer inside the named event.
class RecordingSink final : public LoggerSink
{
  public:
    void record(const LoggerEvent& event) override
    {
        if (textOf(event) == m_blockOn)
        {
            m_blocked.set_value();
            m_release.wait();
        }
        const std::lock_guard lock(m_mutex);
        m_events.push_back(event);
        m_writers.push_back(std::this_thread::get_id());
    }

    std::shared_future<void> block(const std::string& text, std::shared_future<void> release)
    {
        m_blockOn = text;
        m_release = std::move(release);
        return m_blocked.get_future().share();
    }

    std::vector<LoggerEvent> events()
    {
        const std::lock_guard lock(m_mutex);
        return m_events;
    }

    std::vector<std::thread::id> writers()
    {
        const std::lock_guard lock(m_mutex);
        return m_writers;
    }

  private:
    std::mutex m_mutex;
    std::vector<LoggerEvent> m_events;
    std::vector<std::thread::id> m_writers;
    std::string m_blockOn = "\x01";
    std::promise<void> m_blocked;
    std::shared_future<void> m_release;
};

TEST(AsyncLoggerSinkTest, eventsAreWrittenInOrderOnTheLoggingThread)
{
    RecordingSink written;
    {
        AsyncLoggerSink sink(written, 16);
        for (int i = 0; i < 10; ++i)
        {
            sink.record(event(Severity::Info, "line " + std::to_string(i)));
        }
    }
    const auto events = written.events();
    const auto writers = written.writers();
    ASSERT_EQ(10U, events.size());
    for (int i = 0; i < 10; ++i)
    {
        EXPECT_EQ("line " + std::to_string(i), textOf(events[i]));
        EXPECT_NE(std::this_thread::get_id(), writers[i]);
    }
}

TEST(AsyncLoggerSinkTest, aFullQueueDropsAndCountsAndReportsTheDrops)
{
    RecordingSink written;
    std::promise<void> release;
    const auto blocked = written.block("block", release.get_future().share());
    {
        AsyncLoggerSink sink(written, 4);
        sink.record(event(Severity::Info, "block"));
        ASSERT_EQ(std::future_status::ready, blocked.wait_for(std::chrono::seconds(5)));
        for (int i = 0; i < 10; ++i)
        {
            sink.record(event(Severity::Error, "flood " + std::to_string(i))); // the reporting thread never waits
        }
        EXPECT_EQ(6U, sink.dropped());
        release.set_value();
    }
    const auto events = written.events();
    ASSERT_EQ(6U, events.size()); // the blocking line, the four queued and the drop line
    EXPECT_EQ(eventCode::LogRecordsDropped, events[5].code);
    EXPECT_EQ(component::Cluster, events[5].component);
    EXPECT_EQ(0U, textOf(events[5]).rfind("6 log records dropped", 0)) << textOf(events[5]);
}

TEST(AsyncLoggerSinkTest, aFaultEventReturnsOnceWritten)
{
    RecordingSink written;
    AsyncLoggerSink sink(written, 16);
    sink.record(event(Severity::Info, "before"));
    sink.record(event(Severity::Fault, "fatal"));
    EXPECT_EQ(2U, written.events().size()); // a halt may follow a fault, so it is on the sink before record returns
}

TEST(AsyncLoggerSinkTest, aFaultEventWaitsNoLongerThanTheBound)
{
    RecordingSink written;
    std::promise<void> release;
    written.block("fatal", release.get_future().share());
    AsyncLoggerSink sink(written, 16);
    const auto start = std::chrono::steady_clock::now();
    sink.record(event(Severity::Fault, "fatal"));
    const auto waited = std::chrono::steady_clock::now() - start;
    EXPECT_GE(waited, AsyncLoggerSink::FaultWait - std::chrono::milliseconds(1));
    EXPECT_LT(waited, 3 * AsyncLoggerSink::FaultWait);
    release.set_value();
}

TEST(AsyncLoggerSinkTest, afterCloseTheSinkIsCalledOnTheReportingThread)
{
    RecordingSink written;
    AsyncLoggerSink sink(written, 16);
    sink.close();
    sink.record(event(Severity::Info, "late"));
    ASSERT_EQ(1U, written.events().size());
    EXPECT_EQ(std::this_thread::get_id(), written.writers()[0]);
}

} // namespace
