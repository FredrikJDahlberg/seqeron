package org.limitless.phixeron.util;

/**
 * <p>Call sites report a {@link LoggerEvent} through the installed {@link LoggerSink}. The
 * only sink today, the default one, reproduces exactly the {@code [Component/memberId] message} (or
 * {@code [Component] message} where there is no member context) line a bare {@code System.out}/
 * {@code System.err} print used to write. Component/severity/code are structured fields for a future
 * sink (e.g. one publishing a sequenced {@code ErrorNotification} instead of printing) to filter on;
 * swapping one in later changes this file, not the call sites.
 */
public final class Logger {

    private Logger() {
    }

    public enum Component {
        Sequencer,
        SequencerNode,
        SequencerService,
        ConsensusModule,
        ReplayerNode,
        ReplayerService
    }

    public enum Severity {
        Info,
        Warn,
        Error,
        Fault
    }

    public enum EventCode {
        // Generic: routine status lines with no anomaly of their own to identify.
        Info,
        // Specific: one per distinguishable anomaly class (mirrors the C++ side's convention).
        MalformedIngressMessage,
        ConsensusModuleError,
        ServiceError,
        ReplayerBackpressure,
        ArchiveIntegrityFailure,
        StaleActiveRecording,
        TapRecordingFailure,
        ControlReplyDropped,
        GatewayPromotionFailed,
        GatewayActivationTimeout,
        ReplayDutyCycleFailure,
        ReplayClientIdCollision,
        ShutdownTimeout
    }

    public record LoggerEvent(Component component, Severity severity, EventCode code, Integer memberId,
                              String message) {
    }

    public interface LoggerSink {
        void record(LoggerEvent event);
    }

    // Default sink: reproduces today's "[Component/memberId] message" (or "[Component] message") stderr line.
    private static final class StderrLoggerSink implements LoggerSink {
        @Override
        public void record(final LoggerEvent event) {
            final String tag = event.memberId() == null ? "[" + event.component() + "]"
                : "[" + event.component() + "/" + event.memberId() + "]";
            System.err.println(tag + " " + event.message());
        }
    }

    private static final LoggerSink DEFAULT_SINK = new StderrLoggerSink();
    private static volatile LoggerSink installedSink = DEFAULT_SINK;

    // Installs the sink every subsequent log() call forwards to. Caller owns the sink's lifetime (e.g.
    // a test's recording sink) — reset() before it goes out of scope.
    public static void install(final LoggerSink sink) {
        installedSink = sink;
    }

    public static void reset() {
        installedSink = DEFAULT_SINK;
    }

    public static void info(final Component component, final Integer memberId, final String format, final Object... args) {
        log(component, Severity.Info, EventCode.Info, memberId, format, args);
    }

    public static void error(final Component component, final EventCode code, final Integer memberId,
                             final String format, final Object... args) {
        log(component, Severity.Error, code, memberId, format, args);
    }

    public static void fault(final Component component, final EventCode code, final Integer memberId,
                             final String format, final Object... args) {
        log(component, Severity.Fault, code, memberId, format, args);
    }

    public static void log(final Component component, final Severity severity, final EventCode code,
                            final String format, final Object... args) {
        log(component, severity, code, null, format, args);
    }

    public static void log(final Component component, final Severity severity, final EventCode code,
                           final Integer memberId, final String format, final Object... args) {
        final String message = args.length == 0 ? format : String.format(format, args);
        installedSink.record(new LoggerEvent(component, severity, code, memberId, message));
    }
}
