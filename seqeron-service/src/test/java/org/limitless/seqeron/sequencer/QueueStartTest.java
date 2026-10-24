package org.limitless.seqeron.sequencer;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Where a queue's recording starts when a member starts (doc/snapshot.md §10.3). */
class QueueStartTest {
    private static final int TERM = 16 * 1024 * 1024;
    private static final int MTU = 4096;

    @Test
    @DisplayName("with no snapshot a queue is recorded from its first frame")
    void genesisStartsANewRecording() {
        assertEquals(QueueStart.Kind.GENESIS, QueueStart.decide(false, 0, 0, 0, TERM, MTU).kind());
    }

    @Test
    @DisplayName("with the snapshot's term length and MTU the queue starts at its marker's position")
    void equalSettingsKeepThePosition() {
        final QueueStart.Decision decision = QueueStart.decide(true, 5 * TERM + 4096, TERM, MTU, TERM, MTU);

        assertEquals(QueueStart.Kind.AT_POSITION, decision.kind());
        assertEquals(5 * TERM + 4096, decision.position());
    }

    @Test
    @DisplayName("with another term length or MTU the queue starts at 0 and the cut is found by its clusterSeqNo")
    void differingSettingsStartAtZero() {
        for (final int[] settings : new int[][] {{TERM / 2, MTU}, {TERM, MTU / 2}}) {
            final QueueStart.Decision decision =
                QueueStart.decide(true, 5 * TERM + 4096, TERM, MTU, settings[0], settings[1]);

            assertEquals(QueueStart.Kind.AT_ZERO, decision.kind());
            assertEquals(0, decision.position());
        }
    }

    @Test
    @DisplayName("a snapshot that names no marker position starts at 0")
    void noMarkerPositionStartsAtZero() {
        assertEquals(QueueStart.Kind.AT_ZERO, QueueStart.decide(true, -1, TERM, MTU, TERM, MTU).kind());
    }
}
