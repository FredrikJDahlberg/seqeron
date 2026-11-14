package org.limitless.seqeron.tools;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.aeron.Aeron;
import io.aeron.archive.client.AeronArchive;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import org.agrona.CloseHelper;
import org.agrona.collections.LongArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.limitless.seqeron.protocol.PortLayout;
import org.limitless.seqeron.replayer.server.AeronReplayer;
import org.limitless.seqeron.replayer.server.DerivationAnnouncement;
import org.limitless.seqeron.replayer.server.DerivationStart;
import org.limitless.seqeron.replayer.server.DerivingReplayer;

/**
 * How a member's derivation of its queues starts. A member has no recorded path to
 * fall back on, so a start it cannot make ends its Replayer's duty cycle, which exits the node; and an announcement
 * its archive refuses to record is tried again rather than given up on, since a gateway host starts from it. A
 * {@link DerivingReplayer} of the test's own runs over a member's archive.
 */
@ExtendWith(InterruptingTestCallback.class)
class DerivationStartTest {
    private static final int RESPONSE_STREAM_ID = 126; // beside CheckpointedReplayTest's
    private static final int MEMBER = 0;
    private static final long TIMEOUT_NS = TimeUnit.SECONDS.toNanos(30);

    @RegisterExtension final SystemTestWatcher watcher = new SystemTestWatcher();

    @TempDir Path baseDir;

    private InProcessCluster cluster;
    private Aeron aeron;
    private AeronArchive archive;

    @BeforeEach
    void startCluster() {
        watcher.dataCollector().add(baseDir);
        cluster = new InProcessCluster(baseDir);
        cluster.startAll();
        cluster.awaitLeader();
        aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(cluster.aeronDir(MEMBER)));
        archive = AeronArchive.connect(new AeronArchive.Context()
                                           .aeron(aeron)
                                           .ownsAeronClient(false)
                                           .controlRequestChannel(PortLayout.ARCHIVE_CONTROL_CHANNEL)
                                           .controlRequestStreamId(PortLayout.ARCHIVE_CONTROL_STREAM_ID)
                                           .controlResponseChannel(PortLayout.ARCHIVE_CONTROL_CHANNEL)
                                           .controlResponseStreamId(RESPONSE_STREAM_ID));
    }

    @AfterEach
    void stopCluster() {
        CloseHelper.quietCloseAll(archive, aeron);
        if (cluster != null) {
            cluster.close();
        }
    }

    @Test
    @InterruptAfter(60)
    void aStartBelowTheLogsRecordingEndsTheDutyCycle() {
        final DerivationStart member = awaitStart();
        final DerivingReplayer deriving =
            deriving(()
                         -> new DerivationStart(-1, member.state(), false, member.queueStarts(), member.termLength(),
                                                member.mtu()));
        final long deadline = System.nanoTime() + TIMEOUT_NS;
        try {
            while (System.nanoTime() < deadline) {
                if (deriving.doWork() == 0) {
                    Tests.yield();
                }
            }
            fail("a derivation the log's recording cannot start was left to serve nothing");
        } catch (final IllegalStateException expected) {
            assertTrue(expected.getMessage().contains("starts at"), expected.getMessage());
        }
    }

    @Test
    @InterruptAfter(60)
    void anAnnouncementTheArchiveRefusesIsRecordedOnceItCan() {
        awaitStart();
        awaitRecordedAnnouncements();
        final DerivingReplayer deriving = deriving(() -> cluster.derivationStart(MEMBER));
        awaitWork(deriving, () -> deriving.checkpointCount() > 0, "the derivation to catch up");
        // The member's own Replayer records the announcement stream, so the archive refuses a second recording.
        assertFalse(deriving.announces(), "the archive recorded the announcement stream twice");
        final LongArrayList active = activeAnnouncementRecordings();
        assertFalse(active.isEmpty(), "the member records no announcements");
        // One recording subscription records every publication on the stream; stopping one recording closes it.
        assertTrue(archive.tryStopRecordingByIdentity(active.getLong(0)), "recording " + active.getLong(0));
        awaitWork(deriving, () -> activeAnnouncementRecordings().isEmpty(), "the announcements' recording to stop");
        awaitWork(deriving, deriving::announces, "the announcement to be recorded once the archive allows it");
    }

    private DerivationStart awaitStart() {
        final long deadline = System.nanoTime() + TIMEOUT_NS;
        DerivationStart start;
        while ((start = cluster.derivationStart(MEMBER)) == null) {
            assertTrue(System.nanoTime() < deadline, "member " + MEMBER + "'s service never started");
            Tests.yield();
        }
        assertNotNull(start);
        return start;
    }

    /** The member's own Replayer records the announcements once it starts; the test's must not get there first. */
    private void awaitRecordedAnnouncements() {
        final long deadline = System.nanoTime() + TIMEOUT_NS;
        while (activeAnnouncementRecordings().isEmpty()) {
            assertTrue(System.nanoTime() < deadline, "member " + MEMBER + " never recorded its announcements");
            Tests.yield();
        }
    }

    private DerivingReplayer deriving(final Supplier<DerivationStart> start) {
        return new DerivingReplayer(new AeronReplayer(aeron, archive, MEMBER), aeron, archive,
                                    cluster.archiveDir(MEMBER), MEMBER, start);
    }

    private static void awaitWork(final DerivingReplayer deriving, final BooleanSupplier done, final String what) {
        final long deadline = System.nanoTime() + TIMEOUT_NS;
        while (!done.getAsBoolean()) {
            assertTrue(System.nanoTime() < deadline, "timed out waiting for " + what);
            if (deriving.doWork() == 0) {
                Tests.yield();
            }
        }
    }

    private LongArrayList activeAnnouncementRecordings() {
        final LongArrayList ids = new LongArrayList();
        archive.listRecordingsForUri(0, Integer.MAX_VALUE, "", DerivationAnnouncement.STREAM_ID,
                                     (controlSessionId, correlationId, recordingId, startTimestamp, stopTimestamp,
                                      startPosition, stopPosition, initialTermId, segmentFileLength, termBufferLength,
                                      mtuLength, sessionId, streamId, strippedChannel, originalChannel,
                                      sourceIdentity) -> {
                                         if (stopTimestamp == AeronArchive.NULL_TIMESTAMP) {
                                             ids.addLong(recordingId);
                                         }
                                     });
        return ids;
    }
}
