package org.limitless.seqeron.replayer.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The edges of {@link CutLookup} that {@link FakeReplayer} does not reach: it answers {@code UNREADABLE} where the
 * archive refuses a replay outright. {@code ReplayerServiceTest} covers the rest through the service.
 */
class CutLookupTest {
    private static final long RECORDING_ID = 6;
    private static final long START = 1_024;
    private static final long CUT = 50;

    private final CutLookup lookup = new CutLookup();

    @Test
    void aRecordingWithNothingInItYetHoldsTheLookupWithoutReadingIt() {
        final CutLookup.Answer answer = lookup.find(RECORDING_ID, START, START, START + 32, CUT,
                                                    (recordingId, from, tip, visitor) -> fail("read " + from));

        assertEquals(CutLookup.Verdict.PENDING, answer.verdict());
    }

    @Test
    void aHintTheArchiveRefusesToReplayFromIsPassedOverForTheRecordingsStart() {
        final long hint = 2_048 + 8; // no frame boundary
        final List<Long> scans = new ArrayList<>();
        final CutLookup.Scanner archive = (recordingId, from, tip, visitor) -> {
            scans.add(from);
            if (from == hint) {
                throw new IllegalArgumentException("requested replay start position does not point to a valid frame");
            }
            if (visitor.onFrame(START, CUT - 1)) {
                visitor.onFrame(4_096, CUT);
            }
            return Replayer.ScanEnd.STOPPED;
        };

        final CutLookup.Answer answer = lookup.find(RECORDING_ID, START, 8_192, hint, CUT, archive);

        assertEquals(new CutLookup.Answer(CutLookup.Verdict.FOUND, 4_096), answer);
        assertEquals(List.of(hint, START), scans);
    }
}
