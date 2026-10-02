#pragma once

#include <cstdint>

namespace org::limitless::seqeron::app {

// Why a producer may no longer act. Each is terminal and latched: the process releases its cluster session
// so a standby can take over, rather than carrying on behind a view of the log it cannot trust. The Java
// twin is app/ClusterError.java; keep the two in step.
enum class ClusterError : std::uint8_t
{
    // The cluster closed this session, or a new leader never arrived. This instance can never be promoted again.
    ClusterSessionLost,
    // An own frame came back differing from the oldest pending one, so what reached the log cannot be counted.
    IngressConfirmFaulted,
    // Recovery has dispatched nothing for the deadline, on an instance that had caught up before.
    RecoveryStalled,
    // No ClusterHeartbeat for the deadline: this process has stopped seeing its node's tap.
    TapStalled,
    // This instance's snapshot of a round differs from its source's sequenced one: its state is not the log's (A-7).
    SnapshotDiverged,
    // This instance's source has a snapshot it cannot restore: a format or header version this build does not
    // read, or records that fail the check the Replayer indexed them by.
    SnapshotUnrestorable,
};

/**
 * The error's name, for a log line.
 *
 * @param error the error to name
 * @return its name in upper snake case, e.g. "TAP_STALLED"
 */
inline const char* clusterErrorName(const ClusterError error)
{
    switch (error)
    {
        case ClusterError::ClusterSessionLost:
            return "CLUSTER_SESSION_LOST";
        case ClusterError::IngressConfirmFaulted:
            return "INGRESS_CONFIRM_FAULTED";
        case ClusterError::RecoveryStalled:
            return "RECOVERY_STALLED";
        case ClusterError::TapStalled:
            return "TAP_STALLED";
        case ClusterError::SnapshotDiverged:
            return "SNAPSHOT_DIVERGED";
        case ClusterError::SnapshotUnrestorable:
            return "SNAPSHOT_UNRESTORABLE";
    }
    return "UNKNOWN";
}

} // namespace org::limitless::seqeron::app
