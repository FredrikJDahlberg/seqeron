package org.limitless.seqeron.tools;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.Aeron;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.limitless.seqeron.protocol.FrameLayer;

/**
 * A tethered subscriber that stops reading a member's queue, while its client stays alive, holds the queue a window
 * behind it until the member's queue agent skips it (doc/plans/queue-off-duty-cycle-plan.md, step 3): the queue is
 * revoked, which takes the subscriber's image down, and reopened at the commit position under a new session. The
 * member keeps running, and a client reading the queue throughout heals the skipped frames by replay.
 */
@ExtendWith(InterruptingTestCallback.class)
class QueueSkipTest {
    private static final String STALL_PROPERTY = "sequencer.queueStallTimeoutMs";
    /** The least, so the skip comes soon. */
    private static final String STALL_TIMEOUT_MS = "1000";
    /** At {@link #FILLER} bytes each, past the 8 MiB window of a 16 MiB IPC term, and under a second window. */
    private static final int FRAMES = 1_500;
    private static final int FILLER = 8_000;

    @RegisterExtension
    final SystemTestWatcher watcher = new SystemTestWatcher();

    @TempDir
    Path baseDir;

    private InProcessCluster cluster;
    private String stallTimeoutMs;

    @BeforeEach
    void startCluster() {
        watcher.dataCollector().add(baseDir);
        stallTimeoutMs = System.getProperty(STALL_PROPERTY);
        System.setProperty(STALL_PROPERTY, STALL_TIMEOUT_MS);
        cluster = new InProcessCluster(baseDir);
        cluster.startAll();
    }

    @AfterEach
    void stopCluster() {
        cluster.close();
        if (stallTimeoutMs == null) {
            System.clearProperty(STALL_PROPERTY);
        } else {
            System.setProperty(STALL_PROPERTY, stallTimeoutMs);
        }
    }

    @Test
    @InterruptAfter(120)
    void aQueueHeldByATetheredSubscriberIsSkippedAndItsConsumersHeal() {
        final int member = cluster.awaitLeader();
        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(cluster.aeronDir(member)));
             Subscription holder = aeron.addSubscription(FrameLayer.QUEUE_CHANNEL + "|tether=true",
                                                         FrameLayer.queueStreamId(InProcessCluster.GROUP_ID));
             ConfirmProducer producer = new ConfirmProducer(cluster.aeronDir(member), member, FRAMES, 0, FILLER)) {
            final Image held = awaitImage(holder);
            while (!producer.isDrained()) {
                assertFalse(producer.isStalled(), "the producer stalled with " + producer.held() + " held");
                producer.doWork();
            }
            producer.assertExactlyOnceInOrder();
            producer.assertOtherQueueShowsEveryEvenFrame();

            assertTrue(held.isClosed(), "the queue was not revoked from under the subscriber holding it");
            assertNotEquals(held.sessionId(), awaitImage(holder).sessionId(), "the queue did not reopen");
        }
        assertFalse(cluster.fatal(), "a member raised a fatal");
    }

    /** The subscription's open image, once the queue has one: its first frame creates it. */
    private static Image awaitImage(final Subscription subscription) {
        while (true) {
            for (int i = 0; i < subscription.imageCount(); i++) {
                final Image image = subscription.imageAtIndex(i);
                if (!image.isClosed()) {
                    return image;
                }
            }
            Tests.yield();
        }
    }
}
