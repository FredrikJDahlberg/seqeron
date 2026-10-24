package org.limitless.seqeron.tools;

import static io.aeron.cluster.ConsensusModule.Configuration.SERVICE_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.aeron.cluster.RecordingLog;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.limitless.seqeron.replayer.server.RestartSafeLog;

class LogTrimPlanTest {
    private final List<RecordingLog.Entry> entries = new ArrayList<>();

    private void term(final long recordingId, final long baseLogPosition) {
        entries.add(new RecordingLog.Entry(recordingId, 0, baseLogPosition, -1, 0, -1, RecordingLog.ENTRY_TYPE_TERM,
                                           null, true, 0, entries.size()));
    }

    /** A snapshot is two entries: the consensus module's and the service's. */
    private void snapshot(final long consensusRecordingId, final long serviceRecordingId, final long logPosition,
                          final boolean valid) {
        entries.add(new RecordingLog.Entry(consensusRecordingId, 0, 0, logPosition, 0, SERVICE_ID,
                                           RecordingLog.ENTRY_TYPE_SNAPSHOT, null, valid, 0, entries.size()));
        entries.add(new RecordingLog.Entry(serviceRecordingId, 0, 0, logPosition, 0, 0,
                                           RecordingLog.ENTRY_TYPE_SNAPSHOT, null, valid, 0, entries.size()));
    }

    private static RestartSafeLog.State safe(final Long... positions) {
        return new RestartSafeLog.State(Set.of(positions), Set.of(positions), false);
    }

    @Test
    void keepsTheTwoNewestSafeSnapshotsAndTheLogFromTheOlder() {
        term(0, 0);
        snapshot(10, 11, 1_000, true);
        snapshot(20, 21, 2_000, true);
        snapshot(30, 31, 3_000, true);

        final LogTrimPlan plan = LogTrimPlan.of(entries, safe(1_000L, 2_000L, 3_000L));

        assertEquals(2_000, plan.floor());
        assertEquals(Set.of(10L, 11L), plan.snapshotRecordings());
        assertEquals(Set.of(0L), plan.logRecordings());
    }

    @Test
    void aSnapshotThatIsNotRestartSafeIsNoneOfTheTwo() {
        term(0, 0);
        snapshot(10, 11, 1_000, true);
        snapshot(20, 21, 2_000, true);
        snapshot(30, 31, 3_000, true);

        final LogTrimPlan plan = LogTrimPlan.of(entries, safe(1_000L, 2_000L));

        assertEquals(1_000, plan.floor());
        assertEquals(Set.of(), plan.snapshotRecordings());
    }

    @Test
    void anInvalidatedSnapshotIsNoneOfTheTwoAndIsPurgedOnceBelowTheFloor() {
        term(0, 0);
        snapshot(10, 11, 1_000, false);
        snapshot(20, 21, 2_000, true);
        snapshot(30, 31, 3_000, true);
        snapshot(40, 41, 4_000, true);

        final LogTrimPlan plan = LogTrimPlan.of(entries, safe(1_000L, 2_000L, 3_000L, 4_000L));

        assertEquals(3_000, plan.floor());
        assertEquals(Set.of(10L, 11L, 20L, 21L), plan.snapshotRecordings());
    }

    @Test
    void withFewerThanTwoSafeSnapshotsNothingIsRemoved() {
        term(0, 0);
        snapshot(10, 11, 1_000, true);
        snapshot(20, 21, 2_000, true);

        assertEquals(LogTrimPlan.NOTHING, LogTrimPlan.of(entries, safe(2_000L)));
    }

    @Test
    void everyRecordingTheTermsNameHoldsLog() {
        term(0, 0);
        term(5, 4_096);
        snapshot(10, 11, 1_000, true);
        snapshot(20, 21, 2_000, true);

        assertEquals(Set.of(0L, 5L), LogTrimPlan.of(entries, safe(1_000L, 2_000L)).logRecordings());
    }
}
