package org.limitless.seqeron.sequencer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** What {@link SequencerService} decides about a snapshot without an Aeron runtime. */
class SequencerServiceSnapshotTest {
    @TempDir
    Path clusterDir;

    @Test
    @DisplayName("a snapshot is refused while no <snapshots> policy is loaded, and nothing is written before the throw")
    void aSnapshotIsRefusedWithoutAPolicy() {
        // The throw precedes any use of the cluster or the publication, which is why null is safe to pass. A cluster
        // with no policy keeps its full-log replay.
        try (RestartSafeLog log = RestartSafeLog.open(clusterDir)) {
            final SequencerService service = new SequencerService(
                0, new ArrayDeque<>(), () -> { }, log, new ArrayDeque<>(), new AtomicReference<>(), 1 << 24, 4096,
                NodeDriver.DEFAULT_QUEUE_STALL_TIMEOUT_MS, SequencerServer.DEFAULT_SESSION_TIMEOUT_MS);

            assertThrows(IllegalStateException.class, () -> service.onTakeSnapshot(null));
            org.junit.jupiter.api.Assertions.assertFalse(log.isCut(0), "no cut was recorded");
        }
    }

    private static final String AGENT = "io.aeron.cluster.service.ClusteredServiceAgent";

    private static StackTraceElement frame(final String className, final String method) {
        return new StackTraceElement(className, method, null, -1);
    }

    /** Aeron 1.53.2's snapshot attempt, raising from {@code raisedIn} on its way to the service. */
    private static StackTraceElement[] attempt(final StackTraceElement raisedIn) {
        return new StackTraceElement[] { raisedIn,
                                         frame(AGENT, "onTakeSnapshot"),
                                         frame(AGENT, "executeAction"),
                                         frame(AGENT, "onServiceAction"),
                                         frame(AGENT, "doWork"),
                                         frame("org.agrona.concurrent.AgentRunner", "run") };
    }

    /** Where the container's handler runs once an error has escaped the agent's duty cycle. */
    private static final StackTraceElement[] RUNNER = { frame("org.agrona.concurrent.AgentRunner", "handleError"),
                                                        frame("org.agrona.concurrent.AgentRunner", "run") };

    @Test
    @DisplayName("an error that escaped Aeron's snapshot before it reached the service is a skipped round")
    void aSnapshotFailingBeforeTheServiceSkipsTheRound() {
        assertTrue(SequencerService.skippedSnapshot(attempt(frame("io.aeron.archive.client.AeronArchive", "connect")),
                                                    RUNNER));
    }

    @Test
    @DisplayName("the service's own refusal or failure, a report from inside a snapshot, or any other error is not")
    void otherErrorsSkipNoRound() {
        final StackTraceElement[] throughService = attempt(frame(SequencerService.class.getName(), "onTakeSnapshot"));
        assertFalse(SequencerService.skippedSnapshot(throughService, RUNNER), "the service was called");

        final StackTraceElement[] conductor = attempt(frame("io.aeron.ClientConductor", "onChannelEndpointError"));
        assertFalse(SequencerService.skippedSnapshot(conductor, conductor), "reported while the snapshot idles");

        assertFalse(SequencerService.skippedSnapshot(
            new StackTraceElement[] { frame("io.aeron.ClientConductor", "onError"), frame(AGENT, "doWork") }, RUNNER));
    }

    @Test
    @DisplayName("a skipped round raises one fatal, and no snapshot is taken after it")
    void aSkippedRoundIsFatalAndLatched() {
        final AtomicInteger fatals = new AtomicInteger();
        try (RestartSafeLog log = RestartSafeLog.open(clusterDir)) {
            final SequencerService service =
                new SequencerService(0, new ArrayDeque<>(), fatals::incrementAndGet, log, new ArrayDeque<>(),
                                     new AtomicReference<>(), 1 << 24, 4096, NodeDriver.DEFAULT_QUEUE_STALL_TIMEOUT_MS,
                                     SequencerServer.DEFAULT_SESSION_TIMEOUT_MS);
            final RuntimeException timeout = new RuntimeException("Archive connect timeout");
            timeout.setStackTrace(attempt(frame("io.aeron.archive.client.AeronArchive", "connect")));

            service.onServiceError(new IllegalStateException("unrelated"));
            assertEquals(0, fatals.get());
            service.onServiceError(timeout);
            service.onServiceError(timeout);
            assertEquals(1, fatals.get());
            assertThrows(IllegalStateException.class, () -> service.onTakeSnapshot(null));
            assertFalse(log.isCut(0), "no cut was recorded");
        }
    }
}
