package org.limitless.seqeron.replayer.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.Aeron;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.limitless.seqeron.protocol.ReplayProtocol;

/** A cut search's replay that never connects: an archive's replay always does, so the scan is driven directly. */
class ScanDeadlineTest {
    private static final int SESSION_ID = 4711;

    @TempDir
    Path dir;

    @Test
    void aReplayThatNeverConnectsStallsAtItsDeadlineRatherThanBlocking() {
        final MediaDriver.Context context = new MediaDriver.Context()
            .aeronDirectoryName(dir.resolve("aeron").toString())
            .threadingMode(ThreadingMode.SHARED)
            .dirDeleteOnStart(true);
        final String channel = ReplayProtocol.IPC_CHANNEL + "|session-id=" + SESSION_ID;
        try (MediaDriver driver = MediaDriver.launch(context);
             Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(context.aeronDirectoryName()));
             Subscription replay = aeron.addSubscription(channel, ReplayProtocol.REPLAY_STREAM_ID)) {
            // Its own thread: a scan that never returns spins without checking for an interrupt.
            final long startMs = System.currentTimeMillis();
            final Replayer.ScanEnd end = assertTimeoutPreemptively(
                Duration.ofSeconds(10),
                () -> AeronReplayer.scan(replay, SESSION_ID, 1024, (position, clusterSeqNo) -> true),
                "the scan blocked: it would hold the Replayer's duty cycle");
            final long elapsedMs = System.currentTimeMillis() - startMs;

            assertEquals(Replayer.ScanEnd.STALLED, end);
            assertTrue(elapsedMs >= 2_000, "it stalled after " + elapsedMs + " ms, not 2 s");
        }
    }
}
