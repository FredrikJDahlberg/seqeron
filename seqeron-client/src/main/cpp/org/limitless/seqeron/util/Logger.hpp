#pragma once

// Structured logging: call sites report a fixed-size, allocation-free LoggerEvent to the installed
// LoggerSink. The default sink writes it to stderr from a logging thread of its own (AsyncLoggerSink).

#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdarg>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <mutex>
#include <thread>
#include <vector>

namespace org::limitless::seqeron::util {

// Components and anomaly classes are open: core declares its own below, and a consumer declares its own
// alongside rather than growing core's. The Java twin is Logger.Component / Logger.EventCode.
struct Component
{
    const char* name;

    bool operator==(const Component&) const = default;
};

struct EventCode
{
    const char* name;

    bool operator==(const EventCode&) const = default;
};

// Core's own components; a consumer opens this namespace again in its own header to add its own.
namespace component {
inline constexpr Component Cluster{ "Cluster" };
inline constexpr Component ClusterStreamClient{ "ClusterStreamClient" };
inline constexpr Component ReplayerStreamReceiver{ "ReplayerStreamReceiver" };
} // namespace component

// Core's own anomaly classes, on the same terms. Info is the generic one: a routine status line with
// no anomaly of its own to identify.
namespace eventCode {
inline constexpr EventCode Info{ "Info" };
inline constexpr EventCode QueueGap{ "QueueGap" };
inline constexpr EventCode FirstFrameNotOne{ "FirstFrameNotOne" };
inline constexpr EventCode SnapshotRestoreFailed{ "SnapshotRestoreFailed" };
inline constexpr EventCode SnapshotStoreFailed{ "SnapshotStoreFailed" };
inline constexpr EventCode ClusterSessionError{ "ClusterSessionError" };
inline constexpr EventCode ClusterOfferFailed{ "ClusterOfferFailed" };
inline constexpr EventCode ClusterIpcFallback{ "ClusterIpcFallback" };
inline constexpr EventCode ClusterRedirectUnresolved{ "ClusterRedirectUnresolved" };
inline constexpr EventCode FragmentTooShort{ "FragmentTooShort" };
inline constexpr EventCode ArchiveConnectFailed{ "ArchiveConnectFailed" };
inline constexpr EventCode RecoveryStalled{ "RecoveryStalled" };
inline constexpr EventCode QueueStalled{ "QueueStalled" };
inline constexpr EventCode LogRecordsDropped{ "LogRecordsDropped" };
} // namespace eventCode

enum class Severity : std::uint8_t
{
    Info,
    Warn,
    Error,
    Fault
};

// Fixed-size, no heap allocation.
struct LoggerEvent
{
    Component component;
    Severity severity;
    EventCode code;
    std::array<char, 256> text{};
    std::uint8_t textLen = 0;
};

class LoggerSink
{
  public:
    virtual ~LoggerSink() = default;
    virtual void record(const LoggerEvent& event) = 0;
};

// Default sink: the "[Component] message" stderr line.
class StderrLoggerSink final : public LoggerSink
{
  public:
    void record(const LoggerEvent& event) override
    {
        std::fprintf(stderr, "[%s] %.*s\n", event.component.name, static_cast<int>(event.textLen), event.text.data());
    }
};

/**
 * A sink that writes on a thread of its own: record() copies the event into a bounded ring and returns, and the
 * logging thread hands it to the wrapped sink. A full ring drops the event and counts it. A Severity::Fault event
 * waits, up to FaultWait, until it is written, since the process stops after one. Destruction writes what is still
 * queued; after close() the wrapped sink is called on the reporting thread.
 */
class AsyncLoggerSink final : public LoggerSink
{
  public:
    static constexpr std::size_t DefaultCapacity = 4096;
    static constexpr std::chrono::milliseconds FaultWait{ 1000 };

    /**
     * Starts the logging thread.
     *
     * @param sink     where the logging thread writes each event; it must outlive this sink
     * @param capacity how many events the ring holds
     */
    AsyncLoggerSink(LoggerSink& sink, const std::size_t capacity) : m_sink(sink), m_ring(capacity)
    {
        m_thread = std::thread([this] { run(); });
    }

    AsyncLoggerSink(const AsyncLoggerSink&) = delete;
    AsyncLoggerSink& operator=(const AsyncLoggerSink&) = delete;

    ~AsyncLoggerSink() override
    {
        close();
    }

    void record(const LoggerEvent& event) override
    {
        std::unique_lock lock(m_mutex);
        if (!m_running)
        {
            lock.unlock();
            m_sink.record(event);
            return;
        }
        if (m_tail - m_head == m_ring.size())
        {
            m_lastDropped = event;
            m_dropped.fetch_add(1, std::memory_order_relaxed);
            return;
        }
        m_ring[m_tail++ % m_ring.size()] = event;
        if (event.severity == Severity::Fault)
        {
            const std::uint64_t count = m_tail;
            m_wake.notify_one();
            m_writtenCv.wait_for(lock, FaultWait, [&] { return m_written >= count || !m_running; });
        }
    }

    // Events dropped because the ring was full, since this sink was created.
    std::uint64_t dropped() const
    {
        return m_dropped.load(std::memory_order_relaxed);
    }

    // Writes what is queued and stops the logging thread.
    void close()
    {
        {
            const std::lock_guard lock(m_mutex);
            if (!m_running)
            {
                return;
            }
            m_running = false;
        }
        m_wake.notify_one();
        m_writtenCv.notify_all();
        m_thread.join();
    }

  private:
    void run()
    {
        std::uint64_t reported = 0;
        std::unique_lock lock(m_mutex);
        while (true)
        {
            if (m_head == m_tail)
            {
                reported = reportDrops(lock, reported);
                if (!m_running)
                {
                    return;
                }
                m_wake.wait_for(lock, std::chrono::milliseconds(1));
                continue;
            }
            const LoggerEvent event = m_ring[m_head++ % m_ring.size()];
            lock.unlock();
            m_sink.record(event);
            lock.lock();
            ++m_written;
            m_writtenCv.notify_all();
        }
    }

    // Writes one line for the events dropped since the last one; called and returns with the lock held.
    std::uint64_t reportDrops(std::unique_lock<std::mutex>& lock, const std::uint64_t reported)
    {
        const std::uint64_t count = dropped();
        if (count == reported)
        {
            return reported;
        }
        LoggerEvent report{ .component = m_lastDropped.component,
                            .severity = Severity::Warn,
                            .code = eventCode::LogRecordsDropped };
        const int len = std::snprintf(report.text.data(), report.text.size(),
                                      "%llu log records dropped, the logging queue being full",
                                      static_cast<unsigned long long>(count - reported));
        report.textLen = static_cast<std::uint8_t>(len > 0 ? len : 0);
        lock.unlock();
        m_sink.record(report);
        lock.lock();
        return count;
    }

    LoggerSink& m_sink;
    std::vector<LoggerEvent> m_ring;
    std::uint64_t m_head = 0;
    std::uint64_t m_tail = 0;
    std::uint64_t m_written = 0;
    bool m_running = true;
    LoggerEvent m_lastDropped{};
    std::atomic<std::uint64_t> m_dropped{ 0 };
    std::mutex m_mutex;
    std::condition_variable m_wake;
    std::condition_variable m_writtenCv;
    std::thread m_thread;
};

namespace Logger {

inline AsyncLoggerSink& defaultSink()
{
    static StderrLoggerSink stderrSink;
    static AsyncLoggerSink sink(stderrSink, AsyncLoggerSink::DefaultCapacity);
    return sink;
}

inline LoggerSink*& installedSink()
{
    static LoggerSink* sink = &defaultSink();
    return sink;
}

/**
 * Installs the sink every subsequent log() call forwards to.
 *
 * @param sink the sink; the caller owns its lifetime (e.g. a test's stack-local RecordingDiagnosticSink)
 *             and calls reset() before it goes out of scope
 */
inline void install(LoggerSink& sink)
{
    installedSink() = &sink;
}

inline void reset()
{
    installedSink() = &defaultSink();
}

// Records the default sink has dropped, its ring being full.
inline std::uint64_t dropped()
{
    return defaultSink().dropped();
}

inline void vlog(const Component component, const Severity severity, const EventCode code, const char* format,
                 va_list args)
{
    LoggerEvent event{ .component = component, .severity = severity, .code = code };
    const int written = std::vsnprintf(event.text.data(), event.text.size(), format, args);
    int len = written > 0 ? written : 0;
    if (len > static_cast<int>(event.text.size()) - 1)
    {
        len = static_cast<int>(event.text.size()) - 1;
    }
    event.textLen = static_cast<std::uint8_t>(len);
    installedSink()->record(event);
}

inline void log(const Component component, const Severity severity, const EventCode code, const char* format, ...)
{
    va_list args;
    va_start(args, format);
    vlog(component, severity, code, format, args);
    va_end(args);
}

inline void info(const Component component, const EventCode code, const char* format, ...)
{
    va_list args;
    va_start(args, format);
    vlog(component, Severity::Info, code, format, args);
    va_end(args);
}

inline void info(const Component component, const char* format, ...)
{
    va_list args;
    va_start(args, format);
    vlog(component, Severity::Info, eventCode::Info, format, args);
    va_end(args);
}

inline void warn(const Component component, const EventCode code, const char* format, ...)
{
    va_list args;
    va_start(args, format);
    vlog(component, Severity::Warn, code, format, args);
    va_end(args);
}

inline void error(const Component component, const EventCode code, const char* format, ...)
{
    va_list args;
    va_start(args, format);
    vlog(component, Severity::Error, code, format, args);
    va_end(args);
}

inline void fault(const Component component, const EventCode code, const char* format, ...)
{
    va_list args;
    va_start(args, format);
    vlog(component, Severity::Fault, code, format, args);
    va_end(args);
}

} // namespace Logger

} // namespace org::limitless::seqeron::util
