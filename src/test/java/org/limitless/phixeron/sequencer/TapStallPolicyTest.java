package org.limitless.phixeron.sequencer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.limitless.phixeron.sequencer.TapStallPolicy.Action;

/**
 * Unit tests for the pure back-pressure/stall verdict behind {@code SequencerService.emit}. No Aeron
 * runtime: the archive's recording liveness and position are exactly the two values the service reads off
 * the {@code RecordingPos} counter and passes in here.
 */
class TapStallPolicyTest {
    private static final long STALL_NS = TimeUnit.SECONDS.toNanos(2);
    private static final long FATAL_NS = TimeUnit.SECONDS.toNanos(30);
    /** Arbitrary non-zero clock origin — 0 is the policy's "no stall in progress" sentinel. */
    private static final long T0 = TimeUnit.HOURS.toNanos(3);

    private static long seconds(final long s) {
        return T0 + TimeUnit.SECONDS.toNanos(s);
    }

    private final TapStallPolicy policy = new TapStallPolicy(STALL_NS, FATAL_NS);

    @Test
    @DisplayName("a busy archive that keeps draining is never killed, however long back-pressure lasts")
    void drainingArchiveIsNeverFatal() {
        long position = 4096;
        for (long s = 0; s < 300; s++) {
            assertEquals(Action.CONTINUE, policy.onBackPressure(seconds(s), true, position += 64),
                "second " + s);
        }
    }

    @Test
    @DisplayName("intermittent progress re-arms both clocks: the gauge can raise, the node still lives")
    void intermittentProgressRaisesTheGaugeButNeverTerminates() {
        // Progress every 5s, checked every second: each quiet stretch crosses the 2s gauge threshold but
        // the 30s fatal timeout is measured from the last advance and so is never reached.
        long position = 4096;
        boolean everStalled = false;
        for (long s = 0; s < 300; s++) {
            final Action action = policy.onBackPressure(seconds(s), true, s % 5 == 0 ? position += 64 : position);
            assertTrue(action == Action.CONTINUE || action == Action.STALLED, "second " + s + " gave " + action);
            everStalled |= action == Action.STALLED;
        }
        assertTrue(everStalled, "a 4s quiet stretch should have raised the stall gauge at least once");
    }

    @Test
    @DisplayName("a recording that stops advancing raises the gauge, then terminates the node")
    void frozenRecordingStallsThenTerminates() {
        assertEquals(Action.CONTINUE, policy.onBackPressure(seconds(0), true, 4096));   // anchors the clock
        assertEquals(Action.CONTINUE, policy.onBackPressure(seconds(1), true, 4096));
        assertEquals(Action.STALLED, policy.onBackPressure(seconds(2), true, 4096));
        assertEquals(Action.CONTINUE, policy.onBackPressure(seconds(3), true, 4096), "STALLED is edge-triggered");
        assertEquals(Action.CONTINUE, policy.onBackPressure(seconds(29), true, 4096));
        assertEquals(Action.FATAL_NO_PROGRESS, policy.onBackPressure(seconds(30), true, 4096));
    }

    @Test
    @DisplayName("a recording that has gone away is fatal on the first observation — waiting cannot help")
    void missingRecordingIsFatalImmediately() {
        assertEquals(Action.FATAL_RECORDING_GONE, policy.onBackPressure(seconds(0), false, 0));
    }

    @Test
    @DisplayName("the offer landing clears the gauge and starts the next stall on a fresh clock")
    void emittingClearsTheStallState() {
        policy.onBackPressure(seconds(0), true, 4096);
        assertEquals(Action.STALLED, policy.onBackPressure(seconds(2), true, 4096));
        assertTrue(policy.onEmitted(), "the gauge was raised, so it must now be cleared");
        assertFalse(policy.onEmitted(), "nothing to clear on an emit that never stalled");

        // Same recording position as before: it is the time since THIS stall began that counts, not the
        // position's age, so the fatal timeout must run again from here rather than fire straight away.
        assertEquals(Action.CONTINUE, policy.onBackPressure(seconds(3), true, 4096));
        assertEquals(Action.STALLED, policy.onBackPressure(seconds(5), true, 4096));
        assertEquals(Action.CONTINUE, policy.onBackPressure(seconds(32), true, 4096));
        assertEquals(Action.FATAL_NO_PROGRESS, policy.onBackPressure(seconds(33), true, 4096));
    }
}
