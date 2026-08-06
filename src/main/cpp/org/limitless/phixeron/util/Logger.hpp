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

namespace org::limitless::phixeron::util {

enum class Component : std::uint8_t
{
    FixGateway,
    OrderExecClient,
    ReplayerStreamReceiver,
    BasicDataClient,
    Cluster,
    Tcp,
    FixSession,
    FixConnection,
    Resend,
    ClusterStreamClient,
    Ingress,
    App,
    TcpTransport
};

enum class Severity : std::uint8_t
{
    Info,
    Warn,
    Error,
    Fault
};

enum class EventCode : std::uint16_t
{
    TapGap,
    FirstFrameNotOne,
    Info,
    // Specific errors
    ClusterSessionError,
    ClusterOfferFailed,
    ClusterIpcFallback,
    ClusterRedirectUnresolved,
    FragmentTooShort,
    UnexpectedSchemaId,
    InboundSeqNumTooLow,
    InboundGap,
    InboundSeqNumDiscarded,
    ResendIgnored,
    ArchiveScanStalled,
    ArchiveRecoveryFailed,
    MessageRejected,
    StreamCorrupt,
    MessageTooLarge,
    UnknownTemplateId,
    RejectedByCluster,
    LogonQuickResync,
    LogonRejected,
    LogoutRejected,
    HeartbeatRejected,
    TestRequestRejected,
    ResendRequestRejected,
    SequenceResetRejected,
    NewOrderRejected,
    NewOrderAppRejected,
    NewOrderReceived,
    TcpSendFailed,
    GatewayNameUnresolved,
    GatewayNameMissing,
    TapStalled,
    ReplayUnavailable
};

// Fixed-size, no heap allocation — mirrors the field widths a future SBE encoding would use.
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

namespace diagnostic_detail {

inline const char* componentName(const Component component)
{
    switch (component)
    {
        case Component::FixGateway: return "FixGateway";
        case Component::OrderExecClient: return "OrderExecClient";
        case Component::ReplayerStreamReceiver: return "ReplayerStreamReceiver";
        case Component::BasicDataClient: return "BasicDataClient";
        case Component::Cluster: return "Cluster";
        case Component::Tcp: return "TCP";
        case Component::FixSession: return "FixSession";
        case Component::FixConnection: return "FixConnection";
        case Component::Resend: return "Resend";
        case Component::ClusterStreamClient: return "ClusterStreamClient";
        case Component::Ingress: return "Ingress";
        case Component::App: return "App";
        case Component::TcpTransport: return "TcpTransport";
    }
    return "Unknown";
}

}  // namespace diagnostic_detail

// Default sink: reproduces today's "[Component] message" stderr line.
class StderrLoggerSink final : public LoggerSink
{
   public:
    void record(const LoggerEvent& event) override
    {
        std::fprintf(stderr, "[%s] %.*s\n", diagnostic_detail::componentName(event.component),
                     static_cast<int>(event.textLen), event.text.data());
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

inline void vlog(const Component component, const Severity severity, const EventCode code,
                 const char* format, va_list args)
{
    LoggerEvent event{.component = component, .severity = severity, .code = code};
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
    vlog(component, Severity::Info, EventCode::Info, format, args);
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

}  // namespace diagnostic_detail

}  // namespace org::limitless::phixeron::util
