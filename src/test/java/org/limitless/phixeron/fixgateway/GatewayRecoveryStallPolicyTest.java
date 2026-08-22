package org.limitless.phixeron.fixgateway;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the pure recovery-deadline verdict behind {@code ExchangeGateway.checkTapStall}'s
 * not-caught-up branch. No Aeron runtime: {@code isCaughtUp()}/{@code onCaughtUp()} and the dispatched
 * globalSeqNo are exactly the three signals the gateway feeds in off {@code ReplayerStreamReceiver}.
 *
 * <p>The C++ twin is {@code GatewayRecoveryStallPolicyTest.cpp}; keep the two in step.
 */
class GatewayRecoveryStallPolicyTest {
    private static final long DEADLINE_MS = TimeUnit.SECONDS.toMillis(60);
    /** Arbitrary non-zero clock origin — 0 is the policy's "no recovery episode timed" sentinel. */
    private static final long T0 = TimeUnit.HOURS.toMillis(3);
    /** A frontier that never advances: the non-converging case every deadline test below drives. */
    private static final long STUCK = 4242;

    private static long seconds(final long s) {
        return T0 + TimeUnit.SECONDS.toMillis(s);
    }

    private final GatewayRecoveryStallPolicy policy = new GatewayRecoveryStallPolicy(DEADLINE_MS);

    @Test
    @DisplayName("a cold start is never fenced, however long it takes")
    void coldStartNeverTrips() {
        for (long s = 0; s < 3600; s += 30) {
            assertFalse(policy.onNotCaughtUp(seconds(s), STUCK),
                        "second " + s + ": an instance that has never caught up must not be fenced");
        }
    }

    @Test
    @DisplayName("recovery that converges within the deadline never trips, and re-arms cleanly")
    void convergentRecoveryNeverTrips() {
        policy.onCaughtUp();

        assertFalse(policy.onNotCaughtUp(seconds(0), STUCK)); // anchors the recovery clock
        assertFalse(policy.onNotCaughtUp(seconds(10), STUCK));
        assertFalse(policy.onNotCaughtUp(seconds(30), STUCK));
        policy.onCaughtUp(); // a normal re-walk converges well inside the deadline

        assertFalse(policy.onNotCaughtUp(seconds(31), STUCK), "a fresh episode, not the old clock carried forward");
        assertFalse(policy.onNotCaughtUp(seconds(50), STUCK));
    }

    @Test
    @DisplayName("unconvergent recovery on an instance that has served trips at the deadline")
    void unconvergentRecoveryTrips() {
        policy.onCaughtUp(); // this instance has been live before — the deadline is armed

        assertFalse(policy.onNotCaughtUp(seconds(0), STUCK)); // anchors the recovery clock
        assertFalse(policy.onNotCaughtUp(seconds(30), STUCK));
        assertFalse(policy.onNotCaughtUp(seconds(59), STUCK));
        assertTrue(policy.onNotCaughtUp(seconds(60), STUCK), "recovery dispatched nothing for the whole deadline");
        assertTrue(policy.onNotCaughtUp(seconds(61), STUCK), "stays tripped on every later observation");
    }

    @Test
    @DisplayName("re-converging after a trip resets the clock for the next episode")
    void reconvergingAfterATripResetsTheClock() {
        policy.onCaughtUp();
        policy.onNotCaughtUp(seconds(0), STUCK);
        assertTrue(policy.onNotCaughtUp(seconds(60), STUCK));

        policy.onCaughtUp(); // recovers before the process is actually fenced (a slow but real re-walk)

        assertFalse(policy.onNotCaughtUp(seconds(61), STUCK), "anchors a fresh episode rather than inheriting");
        assertFalse(policy.onNotCaughtUp(seconds(90), STUCK));
        assertTrue(policy.onNotCaughtUp(seconds(121), STUCK));
    }

    @Test
    @DisplayName("recovery that keeps dispatching is never fenced, however long it runs")
    void dispatchingRecoveryIsNeverFenced() {
        // The defect this measure exists for: a re-walk of the whole chain is recovery WORKING, and an
        // elapsed-time deadline fenced it regardless of the history it was delivering.
        policy.onCaughtUp();

        for (long s = 0; s < 3600; ++s) {
            assertFalse(policy.onNotCaughtUp(seconds(s), 1000 + s),
                        "second " + s + ": a recovery advancing its globalSeqNo is converging");
        }
    }

    @Test
    @DisplayName("progress part way through an episode restarts the deadline")
    void progressRestartsTheDeadline() {
        policy.onCaughtUp();

        assertFalse(policy.onNotCaughtUp(seconds(0), 1000));
        assertFalse(policy.onNotCaughtUp(seconds(59), 1000));
        assertFalse(policy.onNotCaughtUp(seconds(59), 1001), "one frame dispatched — the deadline restarts here");
        assertFalse(policy.onNotCaughtUp(seconds(118), 1001));
        assertTrue(policy.onNotCaughtUp(seconds(119), 1001), "60s with the frontier frozen at the new value");
    }
}
