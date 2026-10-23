package org.limitless.seqeron.replayer.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** A member's record of its snapshots (doc/snapshot.md §10.8), which a restart reads. */
class RestartSafeLogTest {
    @TempDir
    Path directory;

    @Test
    @DisplayName("what was appended is read back by a later run")
    void aLogOutlivesTheRunThatWroteIt() {
        try (RestartSafeLog log = RestartSafeLog.open(directory)) {
            log.appendCut(100);
            log.appendCut(200);
            log.appendSafe(100);
            log.appendTrimmed();
            assertTrue(log.isCut(200));
            assertFalse(log.isSafe(200));
        }

        final RestartSafeLog.State state = RestartSafeLog.read(directory.resolve(RestartSafeLog.FILE_NAME));
        assertEquals(Set.of(100L, 200L), state.cuts());
        assertEquals(Set.of(100L), state.safe());
        assertTrue(state.trimmed());

        try (RestartSafeLog reopened = RestartSafeLog.open(directory)) {
            assertTrue(reopened.isCut(100));
            assertTrue(reopened.isSafe(100));
            reopened.appendSafe(200);
        }
        assertEquals(Set.of(100L, 200L), RestartSafeLog.read(directory.resolve(RestartSafeLog.FILE_NAME)).safe());
    }

    @Test
    @DisplayName("no file is an empty log, and a last line a crash cut short is ignored")
    void anAbsentOrTornLogReadsWhatItHolds() throws Exception {
        final Path file = directory.resolve(RestartSafeLog.FILE_NAME);
        assertEquals(new RestartSafeLog.State(Set.of(), Set.of(), false), RestartSafeLog.read(file));

        Files.writeString(file, "cut 100\nsafe 10", StandardCharsets.US_ASCII);
        final RestartSafeLog.State state = RestartSafeLog.read(file);
        assertEquals(Set.of(100L), state.cuts());
        assertEquals(Set.of(), state.safe(), "a line with no newline was never forced");
    }
}
