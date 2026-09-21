package org.limitless.seqeron.app;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the tap-silence verdict a producer applies once caught up. No Aeron runtime: the catch-up
 * transition and the {@code ClusterHeartbeat} arrivals are exactly the two signals {@code app.Session} feeds
 * in off {@code ReplayerStreamReceiver}.
 */
class TapStallFenceTest {
    private static final long DEADLINE_MS = 20 * 1000;
    /** Arbitrary clock origin: the fence is only ever read as a difference. */
    private static final long T0 = TimeUnit.HOURS.toMillis(7);

    private static long seconds(final long s) {
        return T0 + TimeUnit.SECONDS.toMillis(s);
    }

    private final TapStallFence fence = new TapStallFence(DEADLINE_MS);

    @Test
    @DisplayName("an instance still replaying is never fenced, however long the replay takes")
    void replayNeverTrips() {
        for (long s = 0; s < 3600; s += 30) {
            assertFalse(fence.isStalled(seconds(s)), "second " + s + ": the fence arms on the first catch-up");
        }
    }

    @Test
    @DisplayName("a heartbeat each second holds the fence open indefinitely")
    void heartbeatsHoldItOpen() {
        fence.onCaughtUp(seconds(0));
        for (long s = 1; s < 600; s++) {
            fence.onClusterHeartbeat(seconds(s));
            assertFalse(fence.isStalled(seconds(s)), "second " + s);
        }
    }

    @Test
    @DisplayName("silence for the deadline after the last heartbeat trips it")
    void silenceTrips() {
        fence.onCaughtUp(seconds(0));
        fence.onClusterHeartbeat(seconds(5));
        assertFalse(fence.isStalled(seconds(24)), "one second short of the deadline");
        assertTrue(fence.isStalled(seconds(25)), "the deadline measured from the last heartbeat");
    }

    @Test
    @DisplayName("catching up re-anchors the deadline, so a healed gap is not fenced for the time it took")
    void catchUpReAnchors() {
        fence.onCaughtUp(seconds(0));
        assertTrue(fence.isStalled(seconds(30)), "silent through the deadline");
        fence.onCaughtUp(seconds(30));
        assertFalse(fence.isStalled(seconds(30)), "re-converged: the deadline starts again here");
        assertTrue(fence.isStalled(seconds(50)), "and runs from there");
    }
}
