package org.limitless.seqeron.util;

/**
 * Structured logging: call sites report a {@link LoggerEvent} to the installed {@link LoggerSink}. The
 * default sink formats and prints {@code [Component/memberId] message} to stderr from a logging thread of its own
 * ({@link AsyncLoggerSink}), so a call site formats nothing.
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
        /** The replay server: inside a member's SequencerServer, or a gateway host's own process. */
        ReplayerServer,
        /** That server's duty cycle. */
        ReplayerService,
        /** On a gateway host, the relay that copies a member's queue onto the local one. */
        QueueRelay,
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
     * lines that identify no anomaly at all. The last eight are the client tier's, and the C++ twin uses
     * the same names.
     */
    public enum CoreEventCode implements EventCode {
        /** Routine status, with no anomaly of its own to identify. */
        Info,
        /** An ingress message the sequencer would not sequence; {@code clusterSeqNo} does not move. */
        MalformedIngressMessage,
        /** Aeron's consensus module raised an error. */
        ConsensusModuleError,
        /** The clustered service raised one. */
        ServiceError,
        /** A queue offer back-pressured; the sequencer spins rather than drop the frame. */
        QueueBackpressure,
        /** An older recording still reports as recording, which an unclean shutdown leaves behind. */
        StaleActiveRecording,
        /** This node can no longer record the history it is responsible for. */
        QueueRecordingFailure,
        /** The consensus module truncated this node's log below what it applied; its state is not the log's. */
        AppliedLogTruncated,
        /** A replay control reply was dropped, its subscriber having gone away. */
        ControlReplyDropped,
        /** A gateway session closed with no standby to promote, leaving that gateway with none active. */
        GatewayPromotionFailed,
        /** A designated instance never declared itself started in time. */
        GatewayActivationTimeout,
        /** The Replayer's duty cycle failed — typically its media driver going away. */
        ReplayDutyCycleFailure,
        /** A duty-cycle thread outlived the deadline for stopping, and is being left behind. */
        ShutdownTimeout,
        /** A gateway host's relay stopped reading one member's archive and moved to the next. */
        RelaySourceLost,
        /** A replayer could not tell an instance which queue it reads; the instance waits until it can. */
        QueueUnresolved,
        /** The live queue skipped a {@code clusterSeqNo}; recovery replays the hole. */
        QueueGap,
        /** Recovery has dispatched nothing for longer than its deadline. */
        RecoveryStalled,
        /** The queue has carried no {@code ClusterHeartbeat} for longer than its deadline. */
        QueueStalled,
        /** The first frame observed was not {@code clusterSeqNo} 1, so history is incomplete. */
        FirstFrameNotOne,
        /** A source's latest snapshot cannot be restored, so its instance cannot recover. */
        SnapshotRestoreFailed,
        /** This instance could not write or read a snapshot file of its own; it has no copy of that round. */
        SnapshotStoreFailed,
        /** The cluster session reported an error on submit. */
        ClusterSessionError,
        /** Cluster ingress took no frame for long enough to call the session lost. */
        ClusterOfferFailed,
        /** The co-located member did not answer IPC ingress, so the sender fell back to UDP. */
        ClusterIpcFallback,
        /** The default sink's queue was full, and it dropped records. */
        LogRecordsDropped
    }

    /**
     * One log record, as a sink receives it: the format and its arguments, formatted by {@link #message} when the
     * sink writes it.
     *
     * @param component who is reporting
     * @param severity  how bad it is
     * @param code      which anomaly class, or {@link CoreEventCode#Info}
     * @param memberId  the node it happened on, or null where there is no member context
     * @param format    the {@link String#format} template, or the whole text where {@code args} is empty
     * @param args      the values it formats
     */
    public record LoggerEvent(Component component, Severity severity, EventCode code, Integer memberId, String format,
                              Object[] args) {
        /** A record of text already formatted. */
        public LoggerEvent(final Component component, final Severity severity, final EventCode code,
                           final Integer memberId, final String message) {
            this(component, severity, code, memberId, message, NO_ARGS);
        }

        /** The formatted text; formatted on each call. */
        public String message() {
            return args.length == 0 ? format : String.format(format, args);
        }
    }

    private static final Object[] NO_ARGS = { };

    /** Where every log call lands. Install one with {@link #install}. */
    public interface LoggerSink {
        /**
         * Called on the reporting thread, in call order; a sink that blocks stalls a duty cycle unless it is
         * wrapped in an {@link AsyncLoggerSink}, as the default is.
         */
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

    private static final AsyncLoggerSink DEFAULT_SINK =
        new AsyncLoggerSink(new StderrLoggerSink(), AsyncLoggerSink.DEFAULT_CAPACITY);
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

    /** Records the default sink has dropped, its queue being full. */
    public static long dropped() {
        return DEFAULT_SINK.dropped();
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
     * The one call the others reach: hands {@code format} and {@code args} to the installed sink unformatted. The
     * default sink formats them on its logging thread, so each argument must be a value that no longer changes.
     */
    public static void log(final Component component, final Severity severity, final EventCode code,
                           final Integer memberId, final String format, final Object... args) {
        installedSink.record(new LoggerEvent(component, severity, code, memberId, format, args));
    }
}
