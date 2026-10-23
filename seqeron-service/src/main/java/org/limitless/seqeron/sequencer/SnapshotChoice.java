package org.limitless.seqeron.sequencer;

import static io.aeron.cluster.ConsensusModule.Configuration.SERVICE_ID;

import io.aeron.cluster.RecordingLog;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.limitless.seqeron.replayer.server.RestartSafeLog;

/**
 * Which cluster snapshot a member starts from (doc/snapshot.md §10.8). A member restarts its queue at the snapshot's
 * cut, so one whose cut no co-located application is known to hold a file for would fence them: the launcher
 * invalidates, before the consensus module starts, every snapshot at the head of the recording log that the member's
 * {@code restart-safe.log} does not name, as {@code clusterctl invalidate-latest-snapshot} does, until the newest is
 * one it does. The member then restores it and replays the log from it, or with none replays the whole log, which it
 * may do only while the log still begins at its first term.
 */
final class SnapshotChoice {
    private SnapshotChoice() {
    }

    /**
     * Invalidates the snapshots the member may not start from.
     *
     * @param clusterDir the member's cluster directory, with its recording log if it has one
     * @param state      what the member's {@code restart-safe.log} holds
     * @return the log positions of the snapshots it invalidated, newest first
     * @throws IllegalStateException if the Raft log has been trimmed and no snapshot is left to start from
     */
    static List<Long> invalidateUnsafe(final File clusterDir, final RestartSafeLog.State state) {
        final List<Long> invalidated = new ArrayList<>();
        if (!new File(clusterDir, RecordingLog.RECORDING_LOG_FILE_NAME).exists()) {
            return invalidated;
        }
        try (RecordingLog recordingLog = new RecordingLog(clusterDir, false)) {
            for (RecordingLog.Entry latest = recordingLog.getLatestSnapshot(SERVICE_ID); latest != null;
                 latest = recordingLog.getLatestSnapshot(SERVICE_ID)) {
                if (state.safe().contains(latest.logPosition)) {
                    return invalidated;
                }
                recordingLog.invalidateLatestSnapshot();
                invalidated.add(latest.logPosition);
            }
        }
        if (state.trimmed()) {
            throw new IllegalStateException(
                "the Raft log has been trimmed and no snapshot of it may be restarted from (invalidated at log "
                + "positions " + invalidated + "): the log no longer begins at its first term, so it cannot be "
                + "replayed whole");
        }
        return invalidated;
    }
}
