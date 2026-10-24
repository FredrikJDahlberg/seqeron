package org.limitless.seqeron.tools;

import io.aeron.cluster.RecordingLog;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.limitless.seqeron.replayer.server.RestartSafeLog;

/**
 * What {@code clusterctl trim-log} removes (doc/snapshot.md §10.7), free of Aeron's archive. The member keeps its two
 * newest restart-safe snapshots and the Raft log from the older one; everything below is the plan's.
 *
 * @param floor              the log position the log is kept from, the older of the two snapshots kept
 * @param snapshotRecordings the recordings of the snapshots older than the two, the consensus module's and the
 *                           service's
 * @param logRecordings      the recordings that hold the log, to be purged below {@code floor}
 */
record LogTrimPlan(long floor, Set<Long> snapshotRecordings, Set<Long> logRecordings) {
    /** The plan while the member has fewer than two restart-safe snapshots: nothing is removed. */
    static final LogTrimPlan NOTHING = new LogTrimPlan(-1, Set.of(), Set.of());

    /**
     * @param entries the member's recording log
     * @param state   what its {@code restart-safe.log} holds
     * @return the plan
     */
    static LogTrimPlan of(final List<RecordingLog.Entry> entries, final RestartSafeLog.State state) {
        final TreeSet<Long> safe = new TreeSet<>();
        for (final RecordingLog.Entry entry : entries) {
            if (entry.type == RecordingLog.ENTRY_TYPE_SNAPSHOT && entry.isValid &&
                state.safe().contains(entry.logPosition)) {
                safe.add(entry.logPosition);
            }
        }
        if (safe.size() < 2) {
            return NOTHING;
        }
        final long floor = safe.descendingSet().stream().skip(1).findFirst().orElseThrow();
        final Set<Long> snapshots = new TreeSet<>();
        final Set<Long> logs = new TreeSet<>();
        for (final RecordingLog.Entry entry : entries) {
            if (entry.type == RecordingLog.ENTRY_TYPE_SNAPSHOT && entry.logPosition < floor) {
                snapshots.add(entry.recordingId);
            } else if (entry.type == RecordingLog.ENTRY_TYPE_TERM) {
                logs.add(entry.recordingId);
            }
        }
        return new LogTrimPlan(floor, snapshots, logs);
    }
}
