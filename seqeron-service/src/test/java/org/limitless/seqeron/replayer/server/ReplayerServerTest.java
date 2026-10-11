package org.limitless.seqeron.replayer.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.driver.Configuration;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.limitless.seqeron.protocol.PortLayout;

class ReplayerServerTest {
    @Test
    void archiveEndpointsFollowTheHostList() {
        assertEquals("m0:" + PortLayout.archivePort(0) + ",m1:" + PortLayout.archivePort(1),
                     ReplayerServer.archiveEndpoints(List.of("m0", "m1")));
        assertNull(ReplayerServer.archiveEndpoints(List.of()));
    }

    @Test
    void untetheredTimeoutsEvictAStalledSubscriberBeforeTheRecordingStallIsFatal() {
        final MediaDriver.Context ctx = new MediaDriver.Context();
        NodeDriver.untetheredTimeouts(ctx);
        Configuration.validateUntetheredTimeouts(ctx.untetheredWindowLimitTimeoutNs(), ctx.untetheredLingerTimeoutNs(),
                                                 ctx.untetheredRestingTimeoutNs(), ctx.timerIntervalNs());
        // Each timeout is noticed up to one timer interval late.
        final long evictionNs = ctx.untetheredWindowLimitTimeoutNs() + ctx.untetheredLingerTimeoutNs() +
                                2 * ctx.timerIntervalNs();
        assertTrue(evictionNs < TimeUnit.MILLISECONDS.toNanos(TapRelay.RECORDING_STALL_FATAL_MS) / 2);
    }

    @Test
    void driverThreadsAreDedicatedUnlessAeronThreadingModeNamesOthers() {
        final String set = System.clearProperty(Configuration.THREADING_MODE_PROP_NAME);
        try {
            assertEquals(ThreadingMode.DEDICATED,
                         NodeDriver.context("aeron", BackoffIdleStrategy::new).threadingMode());
            System.setProperty(Configuration.THREADING_MODE_PROP_NAME, "SHARED");
            // Left unset, which the driver resolves from the property when it concludes the context.
            assertNull(NodeDriver.context("aeron", BackoffIdleStrategy::new).threadingMode());
        } finally {
            if (set == null) {
                System.clearProperty(Configuration.THREADING_MODE_PROP_NAME);
            } else {
                System.setProperty(Configuration.THREADING_MODE_PROP_NAME, set);
            }
        }
    }

    @Test
    void anAeronDirectoryTheDriverHasYetToCreateIsOnItsParentsFileSystem(@TempDir final Path parent)
        throws IOException {
        assertEquals(Files.getFileStore(parent).type(), NodeDriver.fileSystemType(parent.resolve("aeron/node-0")));
    }
}
