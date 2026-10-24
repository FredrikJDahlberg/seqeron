package org.limitless.seqeron.tools;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.Aeron;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.ExclusivePublication;
import io.aeron.Subscription;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.test.Tests;
import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.agrona.ExpandableArrayBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.limitless.seqeron.protocol.FrameLayer;

/**
 * What a restored member relies on (doc/snapshot.md §10.3): a queue created at an initial position is recorded from
 * there, the archive's catalog says so, a consumer sees the same positions, and a replay of the recording works.
 */
class QueueAtInitialPositionTest {
    private static final int STREAM_ID = FrameLayer.queueStreamId(1);
    private static final int TERM_LENGTH = 64 * 1024;
    private static final int INITIAL_TERM_ID = 1000;

    @TempDir
    Path dir;

    @Test
    void aQueueCreatedAtAPositionIsRecordedAndReplayedFromThere() {
        final MediaDriver.Context driver = new MediaDriver.Context()
            .aeronDirectoryName(dir.resolve("aeron").toString())
            .threadingMode(ThreadingMode.SHARED)
            .dirDeleteOnStart(true);
        final Archive.Context archive = new Archive.Context()
            .aeronDirectoryName(driver.aeronDirectoryName())
            .archiveDir(new File(dir.toFile(), "archive"))
            .threadingMode(io.aeron.archive.ArchiveThreadingMode.SHARED)
            .controlChannel("aeron:udp?endpoint=localhost:0")
            .localControlChannel("aeron:ipc")
            .replicationChannel("aeron:udp?endpoint=localhost:0")
            .recordingEventsEnabled(false)
            .deleteArchiveOnStart(true);
        final long position = 5 * TERM_LENGTH + 4096; // five terms and a bit in
        try (ArchivingMediaDriver node = ArchivingMediaDriver.launch(driver, archive);
             Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
             AeronArchive client = AeronArchive.connect(new AeronArchive.Context().aeron(aeron).ownsAeronClient(false)
                                                  .controlRequestChannel("aeron:ipc").controlResponseChannel("aeron:ipc"))) {
            final String channel = new ChannelUriStringBuilder(FrameLayer.QUEUE_CHANNEL)
                                       .initialPosition(position, INITIAL_TERM_ID, TERM_LENGTH)
                                       .build();
            try (Subscription consumer = aeron.addSubscription(FrameLayer.QUEUE_CHANNEL + "|tether=false", STREAM_ID);
                 ExclusivePublication queue = aeron.addExclusivePublication(channel, STREAM_ID)) {
                assertEquals(position, queue.position(), "the publication starts at the position it was given");
                client.startRecording(FrameLayer.QUEUE_CHANNEL, STREAM_ID, SourceLocation.LOCAL);
                final ExpandableArrayBuffer frame = new ExpandableArrayBuffer(64);
                frame.putLong(0, 42);
                final List<Long> offered = new ArrayList<>();
                for (int i = 0; i < 5; i++) {
                    long result;
                    while ((result = queue.offer(frame, 0, 40)) < 0) {
                        Tests.yield();
                    }
                    offered.add(result);
                }
                final long tip = queue.position();

                final long[] catalog = new long[2];
                while (client.listRecordingsForUri(0, 10, "", STREAM_ID, (controlSessionId, correlationId, recordingId,
                    startTimestamp, stopTimestamp, startPosition, stopPosition, initialTermId, segmentFileLength,
                    termBufferLength, mtuLength, sessionId, streamId, strippedChannel, originalChannel, sourceIdentity)
                    -> { catalog[0] = startPosition; catalog[1] = recordingId; }) == 0) {
                    Tests.yield();
                }
                assertEquals(position, catalog[0], "the catalog's startPosition is the position it was created at");

                final long recordingId = catalog[1];
                while (client.getRecordingPosition(recordingId) < tip) {
                    Tests.yield();
                }
                final List<Long> replayed = new ArrayList<>();
                final long session = client.startReplay(recordingId, position, tip - position, "aeron:ipc", 201);
                try (Subscription replay = aeron.addSubscription("aeron:ipc?session-id=" + (int)session, 201)) {
                    while (replayed.size() < 5) {
                        if (replay.poll((buffer, offset, length, header) -> replayed.add(header.position()), 5) == 0) {
                            Tests.yield();
                        }
                    }
                }
                assertEquals(offered, replayed, "the replay carries the positions the queue had");
                assertTrue(consumer.imageCount() == 1, "a consumer joined the queue");
            }
        }
    }
}
