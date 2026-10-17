package org.limitless.seqeron.replayer.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.archive.Archive;
import io.aeron.archive.checksum.Checksums;
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
import org.limitless.seqeron.util.Logger;

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
        assertTrue(evictionNs < TimeUnit.MILLISECONDS.toNanos(QueueRelay.RECORDING_STALL_FATAL_MS) / 2);
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
    void archivesWriteCrc32cChecksumsUnlessAeronRecordChecksumNamesOthers() {
        final String set = System.clearProperty(Archive.Configuration.RECORD_CHECKSUM_PROP_NAME);
        try {
            final Archive.Context unset = new Archive.Context();
            NodeDriver.recordChecksums(unset, Logger.CoreComponent.ReplayerServer, 0);
            assertSame(Checksums.crc32c(), unset.recordChecksum());

            System.setProperty(Archive.Configuration.RECORD_CHECKSUM_PROP_NAME, "");
            final Archive.Context off = new Archive.Context();
            NodeDriver.recordChecksums(off, Logger.CoreComponent.ReplayerServer, 0);
            // Left unset, which the archive resolves from the property when it concludes the context.
            assertNull(off.recordChecksum());
        } finally {
            if (set == null) {
                System.clearProperty(Archive.Configuration.RECORD_CHECKSUM_PROP_NAME);
            } else {
                System.setProperty(Archive.Configuration.RECORD_CHECKSUM_PROP_NAME, set);
            }
        }
    }

    @Test
    void anAeronDirectoryTheDriverHasYetToCreateIsOnItsParentsFileSystem(@TempDir final Path parent)
        throws IOException {
        assertEquals(Files.getFileStore(parent).type(), NodeDriver.fileSystemType(parent.resolve("aeron/node-0")));
    }
}
