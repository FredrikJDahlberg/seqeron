package org.limitless.seqeron.replayer.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.driver.Configuration;
import io.aeron.driver.MediaDriver;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
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
    void anAeronDirectoryTheDriverHasYetToCreateIsOnItsParentsFileSystem(@TempDir final Path parent)
        throws IOException {
        assertEquals(Files.getFileStore(parent).type(), NodeDriver.fileSystemType(parent.resolve("aeron/node-0")));
    }
}
