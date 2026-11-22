package org.limitless.seqeron.tools;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.Aeron;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.limitless.seqeron.protocol.PortLayout;
import org.limitless.seqeron.protocol.SeqeronCounters;
import org.limitless.seqeron.sbe.frame.MessageHeaderEncoder;
import org.limitless.seqeron.sequencer.client.ClusterStreamSender;
import org.limitless.seqeron.util.Logger;

/**
 * A flood of rejected frames while the logging thread's writes to stderr block, as on a stalled pipe
 * (doc/records/plans/queue-off-duty-cycle-plan.md, step 4): every member's service applies the whole flood and keeps
 * sequencing, the log records it cannot write are dropped and counted, and none of it reaches a duty cycle.
 */
@ExtendWith(InterruptingTestCallback.class)
class LogFloodTest {
    /** Each one logged by every member's service, so several times the logging queue's capacity in all. */
    private static final int FLOOD = 20_000;
    private static final int CONFIRMED = 200;
    private static final int FOREIGN_SCHEMA_ID = 999;

    @RegisterExtension final SystemTestWatcher watcher = new SystemTestWatcher();

    @TempDir Path baseDir;

    private InProcessCluster cluster;
    private final CountDownLatch release = new CountDownLatch(1);
    private PrintStream stderr;

    @BeforeEach
    void startCluster() {
        watcher.dataCollector().add(baseDir);
        cluster = new InProcessCluster(baseDir);
        cluster.startAll();
        stderr = System.err;
        System.setErr(new PrintStream(stderr, true) {
            @Override
            public void println(final String line) {
                if (Thread.currentThread().getName().equals("seqeron-logger")) {
                    awaitRelease();
                }
                super.println(line);
            }
        });
    }

    @AfterEach
    void stopCluster() {
        release.countDown();
        System.setErr(stderr);
        cluster.close();
    }

    @Test
    @InterruptAfter(120)
    void aFloodOfRejectionsWithStderrBlockedNeitherStallsTheServiceNorReachesItsDutyCycle() {
        final int member = cluster.awaitLeader();
        final long droppedBefore = Logger.dropped();
        final long[] rejectedBefore = new long[InProcessCluster.MEMBERS];
        for (int m = 0; m < InProcessCluster.MEMBERS; m++) {
            rejectedBefore[m] = counter(m, SeqeronCounters.SEQUENCER_REJECTED_INGRESS_COUNT_TYPE_ID);
        }

        flood(member);
        for (int m = 0; m < InProcessCluster.MEMBERS; m++) {
            while (counter(m, SeqeronCounters.SEQUENCER_REJECTED_INGRESS_COUNT_TYPE_ID) < rejectedBefore[m] + FLOOD) {
                Tests.yield(); // a service blocked on the log line would never get here
            }
        }
        try (ConfirmProducer producer = new ConfirmProducer(cluster.aeronDir(member), member, CONFIRMED, 0)) {
            while (!producer.isDrained()) {
                assertFalse(producer.isStalled(), "the producer stalled with " + producer.held() + " held");
                producer.doWork();
            }
            producer.assertExactlyOnceInOrder();
        }

        assertTrue(Logger.dropped() > droppedBefore, "the logging thread is blocked, yet nothing was dropped");
        for (int m = 0; m < InProcessCluster.MEMBERS; m++) {
            while (counter(m, SeqeronCounters.REPLAYER_LOG_RECORDS_DROPPED_COUNT_TYPE_ID) <= droppedBefore) {
                Tests.yield();
            }
            assertEquals(rejectedBefore[m] + FLOOD,
                         counter(m, SeqeronCounters.SEQUENCER_REJECTED_INGRESS_COUNT_TYPE_ID));
        }
        assertFalse(cluster.fatal(), "a member raised a fatal");
    }

    /** Sends {@link #FLOOD} probe frames under a foreign schema id, which every member's sequencer refuses. */
    private void flood(final int member) {
        final ClusterProbe.MarkerEncoder marker = new ClusterProbe.MarkerEncoder();
        final int length =
            marker.encode(1, -1, ClusterProbe.PROBE_SOURCE_ID, 1 << (InProcessCluster.GROUP_ID - 1), new byte[0]);
        new MessageHeaderEncoder().wrap(marker.frame(), 0).schemaId(FOREIGN_SCHEMA_ID);
        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(cluster.aeronDir(member)));
             ClusterStreamSender sender = new ClusterStreamSender()) {
            sender.connect(aeron, "aeron:udp?endpoint=" + PortLayout.DEFAULT_HOST + ":0",
                           PortLayout.ingressEndpoints(InProcessCluster.MEMBERS));
            for (int i = 0; i < FLOOD; i++) {
                while (!sender.send(marker.frame(), length)) {
                    sender.pollEgress();
                    Tests.yield();
                }
            }
        }
    }

    private long counter(final int m, final int typeId) {
        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(cluster.aeronDir(m)))) {
            final long[] value = { -1 };
            aeron.countersReader().forEach((counterId, type, key, label) -> {
                if (type == typeId) {
                    value[0] = aeron.countersReader().getCounterValue(counterId);
                }
            });
            return value[0];
        }
    }

    private void awaitRelease() {
        try {
            release.await();
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }
}
