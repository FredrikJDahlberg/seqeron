package org.limitless.seqeron.sequencer;

import static org.junit.jupiter.api.Assertions.assertThrows;

import io.aeron.archive.client.AeronArchive;
import java.nio.file.Path;
import java.util.ArrayDeque;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.limitless.seqeron.replayer.server.NodeDriver;
import org.limitless.seqeron.replayer.server.RestartSafeLog;

/** What {@link SequencerService} decides about a snapshot without an Aeron runtime. */
class SequencerServiceSnapshotTest {
    @TempDir
    Path clusterDir;

    @Test
    @DisplayName("a snapshot is refused while no <snapshots> policy is loaded, and nothing is written before the throw")
    void aSnapshotIsRefusedWithoutAPolicy() {
        // The throw precedes any use of the cluster or the publication, which is why null is safe to pass. A cluster
        // with no policy keeps its full-log replay (doc/snapshot.md §10.2).
        try (RestartSafeLog log = RestartSafeLog.open(clusterDir)) {
            final SequencerService service = new SequencerService(
                0, new AeronArchive.Context(), new ArrayDeque<>(),
                () -> { }, log, new ArrayDeque<>(), 1 << 24, 4096, NodeDriver.DEFAULT_QUEUE_STALL_TIMEOUT_MS);

            assertThrows(IllegalStateException.class, () -> service.onTakeSnapshot(null));
            org.junit.jupiter.api.Assertions.assertFalse(log.isCut(0), "no cut was recorded");
        }
    }
}
