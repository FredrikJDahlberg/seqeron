package org.limitless.seqeron.sequencer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** A member's record of its snapshots, which a restart reads. */
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
            assertTrue(log.isCut(200));
            assertFalse(log.isSafe(200));
        }

        final RestartSafeLog.State state = RestartSafeLog.read(directory.resolve(RestartSafeLog.FILE_NAME));
        assertEquals(Set.of(100L, 200L), state.cuts());
        assertEquals(Set.of(100L), state.safe());

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
        assertEquals(new RestartSafeLog.State(Set.of(), Set.of()), RestartSafeLog.read(file));

        Files.writeString(file, "cut 100\nsafe 10", StandardCharsets.US_ASCII);
        final RestartSafeLog.State state = RestartSafeLog.read(file);
        assertEquals(Set.of(100L), state.cuts());
        assertEquals(Set.of(), state.safe(), "a line with no newline was never forced");
    }

    @Test
    @DisplayName("a line appended after one a crash cut short is read back")
    void aLineAfterATornOneIsKept() throws Exception {
        final Path file = directory.resolve(RestartSafeLog.FILE_NAME);
        Files.writeString(file, "cut 100\nsafe 10", StandardCharsets.US_ASCII);

        try (RestartSafeLog log = RestartSafeLog.open(directory)) {
            log.appendCut(200);
        }

        final RestartSafeLog.State state = RestartSafeLog.read(file);
        assertEquals(Set.of(100L, 200L), state.cuts());
        assertEquals(Set.of(), state.safe());
    }

    @Test
    @DisplayName("cut and safe lines written at once from two threads are each read back whole")
    void concurrentAppendsKeepEveryLine() throws Exception {
        final int count = 500;
        try (RestartSafeLog log = RestartSafeLog.open(directory)) {
            final Thread replayer = new Thread(() -> {
                for (int i = 0; i < count; i++) {
                    log.appendSafe(i);
                }
            });
            replayer.start();
            for (int i = 0; i < count; i++) {
                log.appendCut(i);
            }
            replayer.join();
            assertEquals(count, log.safeCount());
        }

        final Set<Long> expected = new HashSet<>();
        for (long i = 0; i < count; i++) {
            expected.add(i);
        }
        final RestartSafeLog.State state = RestartSafeLog.read(directory.resolve(RestartSafeLog.FILE_NAME));
        assertEquals(expected, state.cuts());
        assertEquals(expected, state.safe());
    }
}
