package org.limitless.phixeron.replayer.client;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Unit test for the receive stamp the adapter puts on every frame. No Aeron runtime: the clock is a
 * static of its own, which is all this touches.
 */
class ReplayerStreamReceiverTest {
    @Test
    @DisplayName("the receive stamp has sub-millisecond resolution")
    void receiveStampIsNotMillisecondQuantised() {
        // A currentTimeMillis-derived stamp is always an exact multiple of 1_000_000ns, so it cannot
        // resolve the latency it is sampled for. One reading off a whole millisecond disproves that; 1000
        // of them make a real nanosecond clock landing on the boundary every time impossible in practice.
        boolean subMillisecond = false;
        for (int i = 0; i < 1000 && !subMillisecond; i++) {
            subMillisecond = (ReplayerStreamReceiver.nowNs() % 1_000_000L) != 0;
        }

        assertTrue(subMillisecond, "every stamp landed on a whole millisecond — the clock is currentTimeMillis");
    }

    @Test
    @DisplayName("the receive stamp is on the epoch the cluster timestamp is measured in")
    void receiveStampIsEpochBased() {
        // It is subtracted from clusterTimestamp * 1_000_000 (ClusterProbe's delivery-latency sample), so a
        // monotonic-since-boot clock would be meaningless here however fine its resolution.
        final long epochMs = ReplayerStreamReceiver.nowNs() / 1_000_000L;

        assertTrue(Math.abs(epochMs - System.currentTimeMillis()) < 1_000, "stamp is not wall-clock epoch ms");
    }
}
