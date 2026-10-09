namespace Org.Limitless.Seqeron.App;

/// <summary>
/// Why a producer may no longer act. Each is terminal and latched: the process releases its cluster session so a
/// standby can take over, rather than carrying on behind a view of the log it cannot trust.
/// </summary>
public enum ClusterError
{
    /// <summary>The cluster closed this session, or a new leader never arrived, on a designated gateway instance; or
    /// no new session replaced it in time on any other. This instance can never be promoted again.</summary>
    ClusterSessionLost,

    /// <summary>An own frame came back differing from the oldest pending one, so what reached the log cannot be
    /// counted.</summary>
    IngressConfirmFaulted,

    /// <summary>Recovery has dispatched nothing for the deadline, on an instance that had caught up before; only a
    /// designated gateway instance or the leader's replica fences, any other logs it.</summary>
    RecoveryStalled,

    /// <summary>No <c>ClusterHeartbeat</c> for the deadline: this process has stopped seeing its node's tap. Fences
    /// as <see cref="RecoveryStalled"/> does.</summary>
    TapStalled,

    /// <summary>This instance's snapshot of a round differs from its source's sequenced one: its state is not the
    /// log's (A-7).</summary>
    SnapshotDiverged,

    /// <summary>This instance has a snapshot it cannot restore: a format or header version this build does not read,
    /// or a file whose records fail the <c>SnapshotEnd</c> the log holds for them.</summary>
    SnapshotUnrestorable
}
