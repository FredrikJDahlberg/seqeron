#pragma once

// Call sites report a fixed-size, allocation-free LoggerEvent through the installed
// LoggerSink. The only sink today, StderrLoggerSink, formats and writes it to stderr —
// reproducing exactly what used to be a bare fprintf at each call site (routine stdout status
// lines are reproduced identically too, just tagged Severity::Info). The event shape already
// matches what a future SBE ErrorNotification encoder would need field-for-field, so swapping in a
// queued/sequenced sink later changes this file, not the call sites.

#include <array>
#include <cstdarg>
#include <cstdint>
#include <cstdio>

namespace org::limitless::seqeron::util {

// A component and an anomaly class are OPEN values: core declares its own below, and a consumer
// declares its own alongside them rather than growing core's set — the cluster tier names none of the
// processes that log through it. Each is a pointer to a string literal rather than an enumerator, so a
// future SBE ErrorNotification encoder writes the name; the rest of the event keeps its fixed width.
// The Java twin is Logger.Component / Logger.EventCode, open the same way.
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
inline constexpr EventCode TapGap{ "TapGap" };
inline constexpr EventCode FirstFrameNotOne{ "FirstFrameNotOne" };
inline constexpr EventCode ClusterSessionError{ "ClusterSessionError" };
inline constexpr EventCode ClusterOfferFailed{ "ClusterOfferFailed" };
inline constexpr EventCode ClusterIpcFallback{ "ClusterIpcFallback" };
inline constexpr EventCode ClusterRedirectUnresolved{ "ClusterRedirectUnresolved" };
inline constexpr EventCode FragmentTooShort{ "FragmentTooShort" };
inline constexpr EventCode ArchiveConnectFailed{ "ArchiveConnectFailed" };
inline constexpr EventCode ReplayUnavailable{ "ReplayUnavailable" };
inline constexpr EventCode RecoveryStalled{ "RecoveryStalled" };
} // namespace eventCode

enum class Severity : std::uint8_t
{
    Info,
    Warn,
    Error,
    Fault
};

// Fixed-size, no heap allocation — the shape a future SBE encoding would take.
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

// Default sink: reproduces today's "[Component] message" stderr line.
class StderrLoggerSink final : public LoggerSink
{
  public:
    void record(const LoggerEvent& event) override
    {
        std::fprintf(stderr, "[%s] %.*s\n", event.component.name, static_cast<int>(event.textLen), event.text.data());
    }
};

namespace Logger {

inline StderrLoggerSink& defaultSink()
{
    static StderrLoggerSink sink;
    return sink;
}

inline LoggerSink*& installedSink()
{
    static LoggerSink* sink = &defaultSink();
    return sink;
}

// Installs the sink every subsequent log() call forwards to. Caller owns the sink's lifetime (e.g.
// a test's stack-local RecordingDiagnosticSink) — reset() before it goes out of scope.
inline void install(LoggerSink& sink)
{
    installedSink() = &sink;
}

inline void reset()
{
    installedSink() = &defaultSink();
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
