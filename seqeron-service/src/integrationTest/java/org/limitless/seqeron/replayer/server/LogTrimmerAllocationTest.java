package org.limitless.seqeron.replayer.server;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.management.ThreadMXBean;
import io.aeron.Aeron;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The Replayer calls {@code LogTrimmer.doWork} on every turn of its duty cycle, so an idle trimmer must not allocate.
 * Measured before C2 compiles the loop: its escape analysis removes here an allocation it keeps in the Replayer's.
 */
class LogTrimmerAllocationTest {
    /** Enough to load and initialise the classes on the path, short of C2's thresholds. */
    private static final int WARM_UP_CALLS = 100;
    private static final int MEASURED_CALLS = 5_000;
    /** Room for the JVM's own bookkeeping; a handler built per call is 80 KB over the measured calls. */
    private static final long ALLOWED_BYTES = 1024;

    @TempDir
    Path dir;

    @Test
    void anIdleDoWorkAllocatesNothing() {
        final MediaDriver.Context context = new MediaDriver.Context()
            .aeronDirectoryName(dir.resolve("aeron").toString())
            .threadingMode(ThreadingMode.SHARED)
            .dirDeleteOnStart(true);
        try (MediaDriver driver = MediaDriver.launch(context);
             Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(context.aeronDirectoryName()));
             RestartSafeLog safeLog = RestartSafeLog.open(dir.resolve("cluster"))) {
            // No request arrives and no snapshot becomes restart-safe, so the archive is never asked for anything.
            final LogTrimmer trimmer = new LogTrimmer(aeron, null, 0, safeLog, () -> Long.MAX_VALUE);
            for (int i = 0; i < WARM_UP_CALLS; i++) {
                trimmer.doWork();
            }
            final ThreadMXBean threads = (ThreadMXBean)ManagementFactory.getThreadMXBean();
            final long before = threads.getCurrentThreadAllocatedBytes();
            for (int i = 0; i < MEASURED_CALLS; i++) {
                trimmer.doWork();
            }
            final long allocated = threads.getCurrentThreadAllocatedBytes() - before;

            assertTrue(allocated <= ALLOWED_BYTES,
                       "doWork allocated " + allocated + " bytes over " + MEASURED_CALLS + " idle calls");
        }
    }
}
