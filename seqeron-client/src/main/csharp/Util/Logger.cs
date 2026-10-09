using System;
using System.Globalization;

namespace Org.Limitless.Seqeron.Util;

/// <summary>
/// Structured logging: call sites report a <see cref="LoggerEvent"/> to the installed <see cref="ILoggerSink"/>.
/// The default sink prints <c>[Component/memberId] message</c> to stderr.
/// <para>Components and event codes are any enum, so a consumer declares its own beside
/// <see cref="CoreComponent"/> and <see cref="CoreEventCode"/>; a value prints as its name.</para>
/// </summary>
public static class Logger
{
    /// <summary>The components core itself logs as, each named after the class or process that reports.</summary>
    public enum CoreComponent
    {
        /// <summary>The replicated state machine.</summary>
        Sequencer,

        /// <summary>The node process.</summary>
        SequencerServer,

        /// <summary>The sequencer's Aeron Cluster adapter.</summary>
        SequencerService,

        /// <summary>Aeron's own consensus module, whose errors reach this log through the service.</summary>
        ConsensusModule,

        /// <summary>The replay server.</summary>
        ReplayerServer,

        /// <summary>That server's duty cycle.</summary>
        ReplayerService,

        /// <summary>On a gateway host, the relay that copies a member's tap onto the local one.</summary>
        TapRelay,

        /// <summary>The client-side receiver, reporting from inside a consumer's replica.</summary>
        ReplayerStreamReceiver,

        /// <summary>The cluster session client, <c>ClusterStreamSender</c>.</summary>
        Cluster,

        /// <summary>The end-to-end probe tool.</summary>
        ClusterProbe
    }

    /// <summary>How bad it is.</summary>
    public enum Severity
    {
        /// <summary>Routine status.</summary>
        Info,

        /// <summary>An anomaly that resolved itself, or is expected to.</summary>
        Warn,

        /// <summary>A fault the reporting process carries on past.</summary>
        Error,

        /// <summary>One it does not: the reporter stops, or fences itself, rather than carry on.</summary>
        Fault
    }

    /// <summary>
    /// Core's anomaly classes: one per condition worth telling apart, plus <see cref="Info"/> for the routine lines
    /// that identify no anomaly at all. The same names as Java's and C++'s.
    /// </summary>
    public enum CoreEventCode
    {
        /// <summary>Routine status, with no anomaly of its own to identify.</summary>
        Info,

        /// <summary>An ingress message the sequencer would not sequence; <c>globalSeqNo</c> does not move.</summary>
        MalformedIngressMessage,

        /// <summary>Aeron's consensus module raised an error.</summary>
        ConsensusModuleError,

        /// <summary>The clustered service raised one.</summary>
        ServiceError,

        /// <summary>A tap offer back-pressured; the sequencer spins rather than drop the frame.</summary>
        TapBackpressure,

        /// <summary>A tap recording does not start where this node's chain says it must.</summary>
        ArchiveIntegrityFailure,

        /// <summary>An older recording still reports as recording, which an unclean shutdown leaves
        /// behind.</summary>
        StaleActiveRecording,

        /// <summary>This node can no longer record the history it is responsible for.</summary>
        TapRecordingFailure,

        /// <summary>A replay control reply was dropped, its subscriber having gone away.</summary>
        ControlReplyDropped,

        /// <summary>A gateway session closed with no standby to promote, leaving that gateway with none
        /// active.</summary>
        GatewayPromotionFailed,

        /// <summary>A designated instance never declared itself started in time.</summary>
        GatewayActivationTimeout,

        /// <summary>The Replayer's duty cycle failed, typically its media driver going away.</summary>
        ReplayDutyCycleFailure,

        /// <summary>Two co-located replicas are using one client id, so neither converges.</summary>
        ReplayClientIdCollision,

        /// <summary>A duty-cycle thread outlived the deadline for stopping, and is being left behind.</summary>
        ShutdownTimeout,

        /// <summary>A gateway host's relay stopped reading one member's archive and moved to the next.</summary>
        RelaySourceLost,

        /// <summary>The live tap skipped a <c>globalSeqNo</c>; recovery replays the hole.</summary>
        TapGap,

        /// <summary>This node's Replayer has no valid history to serve.</summary>
        ReplayUnavailable,

        /// <summary>Recovery has dispatched nothing for longer than its deadline.</summary>
        RecoveryStalled,

        /// <summary>The tap has carried no <c>ClusterHeartbeat</c> for longer than its deadline.</summary>
        TapStalled,

        /// <summary>The first frame observed was not <c>globalSeqNo</c> 1, so history is incomplete.</summary>
        FirstFrameNotOne,

        /// <summary>A source's latest snapshot cannot be restored, so its instance cannot recover.</summary>
        SnapshotRestoreFailed,

        /// <summary>This instance could not write or read a snapshot file of its own; it has no copy of that
        /// round.</summary>
        SnapshotStoreFailed,

        /// <summary>The cluster session reported an error on submit.</summary>
        ClusterSessionError,

        /// <summary>Cluster ingress took no frame for long enough to call the session lost.</summary>
        ClusterOfferFailed,

        /// <summary>The co-located member did not answer IPC ingress, so the sender fell back to UDP.</summary>
        ClusterIpcFallback
    }

    /// <summary>One log record, as a sink receives it.</summary>
    /// <param name="Component">who is reporting</param>
    /// <param name="Severity">how bad it is</param>
    /// <param name="Code">which anomaly class, or <see cref="CoreEventCode.Info"/></param>
    /// <param name="MemberId">the node it happened on, or null where there is no member context</param>
    /// <param name="Message">the formatted text</param>
    public sealed record LoggerEvent(Enum Component, Severity Severity, Enum Code, int? MemberId, string Message);

    /// <summary>Where every log call lands. Install one with <see cref="Install"/>.</summary>
    public interface ILoggerSink
    {
        /// <summary>Called on the reporting thread, in call order; a sink that blocks stalls a duty cycle.</summary>
        /// <param name="loggerEvent">the record</param>
        void Record(LoggerEvent loggerEvent);
    }

    // Default sink: "[Component/memberId] message", or "[Component] message" with no member context.
    private sealed class StderrLoggerSink : ILoggerSink
    {
        public void Record(LoggerEvent loggerEvent)
        {
            string tag = loggerEvent.MemberId == null ? $"[{loggerEvent.Component}]"
                                                      : $"[{loggerEvent.Component}/{loggerEvent.MemberId}]";
            Console.Error.WriteLine(tag + " " + loggerEvent.Message);
        }
    }

    private static readonly ILoggerSink DefaultSink = new StderrLoggerSink();
    private static volatile ILoggerSink _installedSink = DefaultSink;

    /// <summary>
    /// Installs the sink every subsequent call forwards to. The caller owns the sink's lifetime: a test's
    /// recording sink calls <see cref="Reset"/> before it goes out of scope.
    /// </summary>
    /// <param name="sink">the sink</param>
    public static void Install(ILoggerSink sink)
    {
        _installedSink = sink;
    }

    /// <summary>Restores the default sink, which prints to stderr.</summary>
    public static void Reset()
    {
        _installedSink = DefaultSink;
    }

    /// <summary>Logs routine status, at <see cref="Severity.Info"/> under <see cref="CoreEventCode.Info"/>.</summary>
    /// <param name="component">who is reporting</param>
    /// <param name="memberId">the node, or null</param>
    /// <param name="format">a composite format string</param>
    /// <param name="args">its arguments</param>
    public static void Info(Enum component, int? memberId, string format, params object[] args)
    {
        Log(component, Severity.Info, CoreEventCode.Info, memberId, format, args);
    }

    /// <summary>Logs a fault the process carries on past, at <see cref="Severity.Error"/>.</summary>
    /// <param name="component">who is reporting</param>
    /// <param name="code">which anomaly class</param>
    /// <param name="memberId">the node, or null</param>
    /// <param name="format">a composite format string</param>
    /// <param name="args">its arguments</param>
    public static void Error(Enum component, Enum code, int? memberId, string format, params object[] args)
    {
        Log(component, Severity.Error, code, memberId, format, args);
    }

    /// <summary>Logs one it does not, at <see cref="Severity.Fault"/>: the caller stops or fences itself after
    /// this.</summary>
    /// <param name="component">who is reporting</param>
    /// <param name="code">which anomaly class</param>
    /// <param name="memberId">the node, or null</param>
    /// <param name="format">a composite format string</param>
    /// <param name="args">its arguments</param>
    public static void Fault(Enum component, Enum code, int? memberId, string format, params object[] args)
    {
        Log(component, Severity.Fault, code, memberId, format, args);
    }

    /// <summary>
    /// The one call the others reach: formats <paramref name="args"/> into <paramref name="format"/> and hands the
    /// record to the installed sink.
    /// </summary>
    /// <param name="component">who is reporting</param>
    /// <param name="severity">how bad it is</param>
    /// <param name="code">which anomaly class</param>
    /// <param name="memberId">the node, or null for a process that is not a cluster node</param>
    /// <param name="format">a composite format string</param>
    /// <param name="args">its arguments</param>
    public static void Log(Enum component, Severity severity, Enum code, int? memberId, string format,
                           params object[] args)
    {
        string message = args.Length == 0 ? format : string.Format(CultureInfo.InvariantCulture, format, args);
        _installedSink.Record(new LoggerEvent(component, severity, code, memberId, message));
    }
}
