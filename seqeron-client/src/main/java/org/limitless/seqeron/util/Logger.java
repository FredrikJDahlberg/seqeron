package org.limitless.seqeron.util;

/**
 * Structured logging: call sites report a {@link LoggerEvent} to the installed {@link LoggerSink}. The
 * default sink prints {@code [Component/memberId] message} to stderr.
 */
public final class Logger {
    private Logger() {
    }

    /** A component name. Core's own are {@link CoreComponent}; a consumer declares its own enum. */
    public interface Component {
        /** The tag this component prints under; an enum constant's own name. */
        String name();
    }

    /** The components core itself logs as, each named after the class or process that reports. */
    public enum CoreComponent implements Component {
        /** The replicated state machine. */
        Sequencer,
        /** The node process. */
        SequencerServer,
        /** The sequencer's Aeron Cluster adapter. */
        SequencerService,
        /** Aeron's own consensus module, whose errors reach this log through the service. */
        ConsensusModule,
        /** The co-located replay server process. */
        ReplayerServer,
        /** That server's duty cycle. */
        ReplayerService,
        /** The client-side receiver, reporting from inside a consumer's replica. */
        ReplayerStreamReceiver,
        /** The cluster session client, {@code ClusterStreamSender}. */
        Cluster,
        /** The end-to-end probe tool. */
        ClusterProbe
    }

    /** How bad it is. */
    public enum Severity {
        /** Routine status. */
        Info,
        /** An anomaly that resolved itself, or is expected to. */
        Warn,
        /** A fault the reporting process carries on past. */
        Error,
        /** One it does not: the reporter stops, or fences itself, rather than carry on. */
        Fault
    }

    /** An anomaly class, open the same way {@link Component} is. Core's own are {@link CoreEventCode}. */
    public interface EventCode {
        /** The code as it appears in a log line or a metric label. */
        String name();
    }

    /**
     * Core's anomaly classes: one per condition worth telling apart, plus {@link #Info} for the routine
     * lines that identify no anomaly at all. The last seven are the client tier's, and the C++ twin uses
     * the same names.
     */
    public enum CoreEventCode implements EventCode {
        /** Routine status, with no anomaly of its own to identify. */
        Info,
        /** An ingress message the sequencer would not sequence; {@code globalSeqNo} does not move. */
        MalformedIngressMessage,
        /** Aeron's consensus module raised an error. */
        ConsensusModuleError,
        /** The clustered service raised one. */
        ServiceError,
        /** A tap offer back-pressured; the sequencer spins rather than drop the frame. */
        TapBackpressure,
        /** A tap recording does not start where this node's chain says it must. */
        ArchiveIntegrityFailure,
        /** An older recording still reports as recording, which an unclean shutdown leaves behind. */
        StaleActiveRecording,
        /** This node can no longer record the history it is responsible for. */
        TapRecordingFailure,
        /** A replay control reply was dropped, its subscriber having gone away. */
        ControlReplyDropped,
        /** A gateway session closed with no standby to promote, leaving that gateway with none active. */
        GatewayPromotionFailed,
        /** A designated instance never declared itself started in time. */
        GatewayActivationTimeout,
        /** The Replayer's duty cycle failed — typically its media driver going away. */
        ReplayDutyCycleFailure,
        /** Two co-located replicas are using one client id, so neither converges. */
        ReplayClientIdCollision,
        /** A duty-cycle thread outlived the deadline for stopping, and is being left behind. */
        ShutdownTimeout,
        /** The live tap skipped a {@code globalSeqNo}; recovery replays the hole. */
        TapGap,
        /** This node's Replayer has no valid history to serve. */
        ReplayUnavailable,
        /** Recovery has dispatched nothing for longer than its deadline. */
        RecoveryStalled,
        /** The first frame observed was not {@code globalSeqNo} 1, so history is incomplete. */
        FirstFrameNotOne,
        /** The cluster session reported an error on submit. */
        ClusterSessionError,
        /** Cluster ingress took no frame for long enough to call the session lost. */
        ClusterOfferFailed,
        /** The co-located member did not answer IPC ingress, so the sender fell back to UDP. */
        ClusterIpcFallback
    }

    /**
     * One log record, as a sink receives it.
     *
     * @param component who is reporting
     * @param severity  how bad it is
     * @param code      which anomaly class, or {@link CoreEventCode#Info}
     * @param memberId  the node it happened on, or null where there is no member context
     * @param message   the formatted text
     */
    public record
        LoggerEvent(Component component, Severity severity, EventCode code, Integer memberId, String message) { }

    /** Where every log call lands. Install one with {@link #install}. */
    public interface LoggerSink {
        /** Called on the reporting thread, in call order; a sink that blocks stalls a duty cycle. */
        void record(LoggerEvent event);
    }

    // Default sink: "[Component/memberId] message", or "[Component] message" with no member context.
    private static final class StderrLoggerSink implements LoggerSink {
        @Override
        public void record(final LoggerEvent event) {
            final String name = event.component().name();
            final String tag = event.memberId() == null ? "[" + name + "]" : "[" + name + "/" + event.memberId() + "]";
            System.err.println(tag + " " + event.message());
        }
    }

    private static final LoggerSink DEFAULT_SINK = new StderrLoggerSink();
    private static volatile LoggerSink installedSink = DEFAULT_SINK;

    /**
     * Installs the sink every subsequent call forwards to. The caller owns the sink's lifetime — a test's
     * recording sink calls {@link #reset()} before it goes out of scope.
     */
    public static void install(final LoggerSink sink) {
        installedSink = sink;
    }

    /** Restores the default sink, which prints to stderr. */
    public static void reset() {
        installedSink = DEFAULT_SINK;
    }

    /** Logs routine status, at {@link Severity#Info} under {@link CoreEventCode#Info}. */
    public static void info(final Component component, final Integer memberId, final String format,
                            final Object... args) {
        log(component, Severity.Info, CoreEventCode.Info, memberId, format, args);
    }

    /** Logs a fault the process carries on past, at {@link Severity#Error}. */
    public static void error(final Component component, final EventCode code, final Integer memberId,
                             final String format, final Object... args) {
        log(component, Severity.Error, code, memberId, format, args);
    }

    /** Logs one it does not, at {@link Severity#Fault}: the caller stops or fences itself after this. */
    public static void fault(final Component component, final EventCode code, final Integer memberId,
                             final String format, final Object... args) {
        log(component, Severity.Fault, code, memberId, format, args);
    }

    /** Logs with no member context, for a process that is not a cluster node. */
    public static void log(final Component component, final Severity severity, final EventCode code,
                           final String format, final Object... args) {
        log(component, severity, code, null, format, args);
    }

    /**
     * The one call the others reach: formats {@code args} into {@code format} and hands the record to the
     * installed sink.
     */
    public static void log(final Component component, final Severity severity, final EventCode code,
                           final Integer memberId, final String format, final Object... args) {
        final String message = args.length == 0 ? format : String.format(format, args);
        installedSink.record(new LoggerEvent(component, severity, code, memberId, message));
    }
}
