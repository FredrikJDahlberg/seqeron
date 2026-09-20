package org.limitless.seqeron.app;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.limitless.seqeron.app.TapLagMonitor.TapLag;

/**
 * Unit tests for the staleness verdict on live {@code ClusterHeartbeat}s. No Aeron runtime:
 * {@code (clusterTimestampNs, receiveTimeNs)} is exactly the pair a client reads off a heartbeat's
 * {@code SequencedEvent}, and gating on {@code isCaughtUp()} is the caller's job. Case for case with
 * {@code TapLagMonitorTest.cpp}.
 */
class TapLagMonitorTest {
    private static final long THRESHOLD_NS = TimeUnit.SECONDS.toNanos(20);
    /** Arbitrary consensus-clock origin; only differences matter. */
    private static final long T0 = TimeUnit.HOURS.toNanos(3);

    private static long ms(final long ms) {
        return TimeUnit.MILLISECONDS.toNanos(ms);
    }

    /** The heartbeat stamped {@code s} seconds after T0. */
    private static long heartbeat(final long s) {
        return T0 + TimeUnit.SECONDS.toNanos(s);
    }

    private final TapLagMonitor monitor = new TapLagMonitor(THRESHOLD_NS);

    @Test
    @DisplayName("healthy heartbeats cross no edge")
    void healthyHeartbeatsCrossNoEdge() {
        for (long s = 0; s < 300; ++s) {
            assertEquals(TapLag.NONE, monitor.onClusterHeartbeat(heartbeat(s), heartbeat(s) + ms(2)),
                         "second " + s + ": a couple of ms of delivery latency is not staleness");
        }
        assertFalse(monitor.isStale());
        assertEquals(ms(2), monitor.peakLagNs());
        assertEquals(300, monitor.sampleCount());
    }

    @Test
    @DisplayName("retained frames drained after a long re-walk are not stale")
    void retainedFramesAreNotStale() {
        // 90 s of heartbeats retained during a 90 s walk, each captured 3 ms after it was stamped, all
        // dispatched in one burst at the seam.
        for (long s = 0; s < 90; ++s) {
            assertEquals(TapLag.NONE, monitor.onClusterHeartbeat(heartbeat(s), heartbeat(s) + ms(3)),
                         "second " + s + ": a retained frame arrived on time and was merely held");
        }
        assertFalse(monitor.isStale());
    }

    @Test
    @DisplayName("lag reaching the threshold fires once, on the edge")
    void lagReachingTheThresholdFiresOnce() {
        assertEquals(TapLag.NONE, monitor.onClusterHeartbeat(heartbeat(0), heartbeat(0) + ms(5_000)));
        assertEquals(TapLag.NONE, monitor.onClusterHeartbeat(heartbeat(1), heartbeat(1) + THRESHOLD_NS - 1));
        assertEquals(TapLag.BECAME_STALE, monitor.onClusterHeartbeat(heartbeat(2), heartbeat(2) + THRESHOLD_NS));
        assertTrue(monitor.isStale());

        // Still stale, but the caller must not be made to log at the 1 Hz sample rate.
        assertEquals(TapLag.NONE, monitor.onClusterHeartbeat(heartbeat(3), heartbeat(3) + ms(45_000)));
        assertEquals(TapLag.NONE, monitor.onClusterHeartbeat(heartbeat(4), heartbeat(4) + ms(60_000)));
        assertEquals(ms(60_000), monitor.peakLagNs());
    }

    @Test
    @DisplayName("recovering below the threshold fires the fresh edge and re-arms")
    void recoveringFiresTheFreshEdgeAndReArms() {
        assertEquals(TapLag.BECAME_STALE, monitor.onClusterHeartbeat(heartbeat(0), heartbeat(0) + ms(30_000)));

        assertEquals(TapLag.BECAME_FRESH, monitor.onClusterHeartbeat(heartbeat(1), heartbeat(1) + ms(8)));
        assertFalse(monitor.isStale());
        assertEquals(TapLag.NONE, monitor.onClusterHeartbeat(heartbeat(2), heartbeat(2) + ms(8)));

        // A second episode reports again rather than staying silent because the first one was already told.
        assertEquals(TapLag.BECAME_STALE, monitor.onClusterHeartbeat(heartbeat(3), heartbeat(3) + ms(25_000)));
        assertEquals(ms(30_000), monitor.peakLagNs(), "peak is over the whole run, not the current episode");
    }

    @Test
    @DisplayName("clock skew is reported once and never reads as stale")
    void clockSkewIsReportedOnce() {
        // A clock behind the leader's by more than the threshold makes the gauge fail open — a real 20 s lag
        // would read as fresh — so it is reported rather than quietly absorbed.
        assertEquals(TapLag.SKEW_SUSPECTED, monitor.onClusterHeartbeat(heartbeat(0), heartbeat(0) - ms(35_000)));
        assertEquals(TapLag.NONE, monitor.onClusterHeartbeat(heartbeat(1), heartbeat(1) - ms(35_000)),
                     "a misconfigured clock is a standing fault, not a per-heartbeat event");
        assertFalse(monitor.isStale());
        assertEquals(0, monitor.peakLagNs(), "a negative sample must not become the peak");
    }

    @Test
    @DisplayName("small negative lag is quiet noise")
    void smallNegativeLagIsQuiet() {
        // An offset between two disciplined clocks reads as a small negative lag; it is not a fault.
        assertEquals(TapLag.NONE, monitor.onClusterHeartbeat(heartbeat(0), heartbeat(0) - 1));
        assertEquals(TapLag.NONE, monitor.onClusterHeartbeat(heartbeat(1), heartbeat(1) - ms(250)));
        assertFalse(monitor.isStale());
    }
}
