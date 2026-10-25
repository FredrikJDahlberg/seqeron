package org.limitless.seqeron.sequencer;

import static io.aeron.cluster.ConsensusModule.Configuration.SERVICE_ID;

import io.aeron.archive.CatalogView;
import io.aeron.cluster.RecordingLog;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.function.LongUnaryOperator;
import org.limitless.seqeron.replayer.server.RestartSafeLog;

/**
 * Which cluster snapshot a member starts from (doc/snapshot.md §10.8). A member restarts its queue at the snapshot's
 * cut, so one whose cut no co-located application is known to hold a file for would fence them: the launcher
 * invalidates, before the consensus module starts, every snapshot at the head of the recording log that the member's
 * {@code restart-safe.log} does not name, as {@code clusterctl invalidate-latest-snapshot} does, until the newest is
 * one it does. The member then restores it and replays the log from it, or with none replays the whole log, which it
 * may do only while the log still begins at its first term, which the archive's catalog says: the first term's
 * recording starts at position 0 until {@code clusterctl trim-log} purges its head.
 */
final class SnapshotChoice {
    private SnapshotChoice() {
    }

    /**
     * Invalidates the snapshots the member may not start from.
     *
     * @param clusterDir     the member's cluster directory, with its recording log if it has one
     * @param state          what the member's {@code restart-safe.log} holds
     * @param recordingStart the start position of an archive recording, or -1 if the archive holds none by that id
     * @return the log positions of the snapshots it invalidated, newest first
     * @throws IllegalStateException if the Raft log has been trimmed and no snapshot is left to start from
     */
    static List<Long> invalidateUnsafe(final File clusterDir, final RestartSafeLog.State state,
                                      final LongUnaryOperator recordingStart) {
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
            if (logTrimmed(recordingLog.entries(), recordingStart)) {
                    throw new IllegalStateException(
                    "the Raft log has been trimmed and no snapshot of it may be restarted from (invalidated at log "
                    + "positions " + invalidated + "): the log no longer begins at its first term, so it cannot be "
                    + "replayed whole");
            }
        }
        return invalidated;
    }

    /**
     * @param entries        the member's recording log
     * @param recordingStart the start position of an archive recording, or -1 if the archive holds none by that id
     * @return whether the log no longer begins at its first term: the oldest term starts above log position 0, or its
     *         recording starts above 0 or is gone
     */
    static boolean logTrimmed(final List<RecordingLog.Entry> entries, final LongUnaryOperator recordingStart) {
        RecordingLog.Entry first = null;
        for (final RecordingLog.Entry entry : entries) {
            if (entry.type == RecordingLog.ENTRY_TYPE_TERM &&
                (first == null || entry.termBaseLogPosition < first.termBaseLogPosition)) {
                first = entry;
            }
        }
        return first != null &&
               (first.termBaseLogPosition > 0 || recordingStart.applyAsLong(first.recordingId) != 0);
    }

    /**
     * @param archiveDir the member's archive directory, which no archive need be running on
     * @return the lookup of a recording's start position in its catalog, -1 where there is none
     */
    static LongUnaryOperator catalogStart(final File archiveDir) {
        return recordingId -> {
            final long[] start = {-1};
            if (new File(archiveDir, "archive.catalog").exists()) {
                CatalogView.listRecording(archiveDir, recordingId,
                                          (controlSessionId, correlationId, id, startTimestamp, stopTimestamp,
                                           startPosition, stopPosition, initialTermId, segmentFileLength,
                                           termBufferLength, mtuLength, sessionId, streamId, strippedChannel,
                                           originalChannel, sourceIdentity) -> start[0] = startPosition);
            }
            return start[0];
        };
    }
}
