package org.limitless.seqeron.sequencer;

import static io.aeron.cluster.ConsensusModule.Configuration.SERVICE_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.cluster.RecordingLog;
import java.io.File;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.function.LongUnaryOperator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.limitless.seqeron.replayer.server.RestartSafeLog;

/** Which cluster snapshot a member starts from (doc/snapshot.md §10.8). */
class SnapshotChoiceTest {
    private static final LongUnaryOperator UNTRIMMED = id -> 0;

    @TempDir
    Path directory;

    /** A log of one term holding a snapshot at each position: the service's entry, then the consensus module's. */
    private File logWithSnapshotsAt(final long... positions) {
        try (RecordingLog log = new RecordingLog(directory.toFile(), true)) {
            log.appendTerm(1, 0, 0, 1);
            long recordingId = 10;
            for (final long position : positions) {
                log.appendSnapshot(recordingId++, 0, 0, position, 2, 0);
                log.appendSnapshot(recordingId++, 0, 0, position, 2, SERVICE_ID);
            }
        }
        return directory.toFile();
    }

    private Long latestSnapshot() {
        try (RecordingLog log = new RecordingLog(directory.toFile(), false)) {
            final RecordingLog.Entry latest = log.getLatestSnapshot(SERVICE_ID);
            return latest == null ? null : latest.logPosition;
        }
    }

    private static RestartSafeLog.State safe(final long... positions) {
        final java.util.HashSet<Long> safe = new java.util.HashSet<>();
        for (final long position : positions) {
            safe.add(position);
        }
        return new RestartSafeLog.State(Set.of(), safe);
    }

    @Test
    @DisplayName("a member with no recording log starts from none")
    void noRecordingLogInvalidatesNothing() {
        assertEquals(List.of(), SnapshotChoice.invalidateUnsafe(directory.toFile(), safe(), UNTRIMMED));
    }

    @Test
    @DisplayName("a newest snapshot the member's log names is started from")
    void aSafeNewestSnapshotIsKept() {
        final File clusterDir = logWithSnapshotsAt(100, 200);

        assertEquals(List.of(), SnapshotChoice.invalidateUnsafe(clusterDir, safe(100, 200), UNTRIMMED));
        assertEquals(200L, latestSnapshot());
    }

    @Test
    @DisplayName("a newest snapshot it does not name is invalidated, down to the newest one it does")
    void anUnsafeSnapshotGivesWayToAnOlderSafeOne() {
        final File clusterDir = logWithSnapshotsAt(100, 200, 300);

        assertEquals(List.of(300L, 200L), SnapshotChoice.invalidateUnsafe(clusterDir, safe(100), UNTRIMMED));
        assertEquals(100L, latestSnapshot());
    }

    @Test
    @DisplayName("with none it names, every snapshot is invalidated and the log is replayed whole")
    void noSafeSnapshotFallsBackToTheWholeLog() {
        final File clusterDir = logWithSnapshotsAt(100, 200);

        assertEquals(List.of(200L, 100L), SnapshotChoice.invalidateUnsafe(clusterDir, safe(), UNTRIMMED));
        assertNull(latestSnapshot());
    }

    @Test
    @DisplayName("once the log is trimmed, no safe snapshot means the member does not start")
    void aTrimmedLogNeedsASafeSnapshot() {
        final File clusterDir = logWithSnapshotsAt(100);

        assertEquals(List.of(), SnapshotChoice.invalidateUnsafe(clusterDir, safe(100), id -> 4096),
                     "a trimmed log with a safe snapshot starts");
        assertThrows(IllegalStateException.class,
                     () -> SnapshotChoice.invalidateUnsafe(clusterDir, safe(), id -> 4096));
    }

    @Test
    @DisplayName("the log is trimmed when its first term's recording starts above 0, is gone, or the term starts later")
    void theCatalogSaysWhetherTheLogIsTrimmed() {
        final List<RecordingLog.Entry> entries;
        try (RecordingLog log = new RecordingLog(logWithSnapshotsAt(100), false)) {
            entries = log.entries();
        }

        assertFalse(SnapshotChoice.logTrimmed(entries, UNTRIMMED));
        assertTrue(SnapshotChoice.logTrimmed(entries, id -> 4096));
        assertTrue(SnapshotChoice.logTrimmed(entries, id -> -1));
        assertFalse(SnapshotChoice.logTrimmed(List.of(), id -> -1), "no log, nothing trimmed");
    }
}
