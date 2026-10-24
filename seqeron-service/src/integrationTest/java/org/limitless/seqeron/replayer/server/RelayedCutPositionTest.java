package org.limitless.seqeron.replayer.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.test.Tests;
import java.nio.file.Path;
import org.agrona.ExpandableArrayBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.limitless.seqeron.protocol.FrameLayer;

/** Where a frame starts, from the position its offer returned, on a publication that crosses term boundaries. */
class RelayedCutPositionTest {
    private static final int TERM_LENGTH = 64 * 1024;
    private static final int STREAM_ID = FrameLayer.queueStreamId(1);

    @TempDir
    Path dir;

    @Test
    void aFramesStartIsItsOfferResultLessItsAlignedLengthAcrossATermBoundary() {
        final MediaDriver.Context context = new MediaDriver.Context()
            .aeronDirectoryName(dir.resolve("aeron").toString())
            .threadingMode(ThreadingMode.SHARED)
            .dirDeleteOnStart(true);
        final String channel = FrameLayer.QUEUE_CHANNEL + "|term-length=" + TERM_LENGTH;
        final ExpandableArrayBuffer message = new ExpandableArrayBuffer(4096);
        int crossings = 0;
        try (MediaDriver driver = MediaDriver.launch(context);
             Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(context.aeronDirectoryName()));
             Subscription consumer = aeron.addSubscription(channel, STREAM_ID);
             ExclusivePublication queue = aeron.addExclusivePublication(channel, STREAM_ID)) {
            // Lengths that never divide a term, so some offer is the first that does not fit one.
            for (int i = 0; i < 400; i++) {
                final int length = 1000 + 37 * (i % 11);
                final long before = queue.position();
                long end;
                while ((end = queue.offer(message, 0, length)) < 0) {
                    consumer.poll((buffer, offset, len, header) -> { }, 64);
                    Tests.yield();
                }
                consumer.poll((buffer, offset, len, header) -> { }, 64);

                final long start = RelayedCut.startOf(end, length);
                if (start == before) {
                    continue;
                }
                crossings++;
                assertNotEquals(before, start);
                assertEquals(0, start % TERM_LENGTH, "padding fills the term, so the frame starts the next: " + start);
                assertEquals(before / TERM_LENGTH + 1, start / TERM_LENGTH, "and it is the next term");
            }
        }
        assertTrue(crossings >= 5, "the run crossed " + crossings + " term boundaries");
    }
}
