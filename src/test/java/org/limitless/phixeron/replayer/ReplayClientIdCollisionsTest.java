package org.limitless.phixeron.replayer;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for the duplicate-{@code PHIXERON_REPLAYER_CLIENT_ID} detector (review-3 finding 4). The
 * whole point is the two cases it must not confuse: a client that <em>restarted</em> (one backwards
 * {@code requestId} step, then monotone) versus two live clients sharing an id (their two counters
 * interleave, so backwards steps keep coming). No Aeron runtime — time is a parameter.
 */
class ReplayClientIdCollisionsTest {
    private static final long WINDOW_MS = 10_000;
    private static final int CLIENT = 2;

    @Test
    @DisplayName("one live client's monotone requestIds never look like a collision")
    void monotoneRequestIdsAreNeverACollision() {
        final ReplayClientIdCollisions collisions = new ReplayClientIdCollisions(WINDOW_MS);

        for (long requestId = 1; requestId <= 100; requestId++) {
            assertFalse(collisions.onRequest(CLIENT, requestId, requestId * 500),
                    "a single client's requestIds only ever increase");
        }
    }

    @Test
    @DisplayName("a restart is one backwards step and must not be reported")
    void restartIsNotACollision() {
        final ReplayClientIdCollisions collisions = new ReplayClientIdCollisions(WINDOW_MS);
        for (long requestId = 1; requestId <= 20; requestId++) {
            collisions.onRequest(CLIENT, requestId, requestId * 500);
        }

        // The app restarted: same client id, counter back to 1, monotone from there.
        long nowMs = 20 * 500;
        assertFalse(collisions.onRequest(CLIENT, 1, nowMs += 500), "the restart itself is one step back");
        for (long requestId = 2; requestId <= 50; requestId++) {
            assertFalse(collisions.onRequest(CLIENT, requestId, nowMs += 500),
                    "a restarted client is monotone again — never a collision");
        }
    }

    @Test
    @DisplayName("repeated restarts spread beyond the window never accumulate into a collision")
    void restartsSpreadOverTimeNeverAccumulate() {
        final ReplayClientIdCollisions collisions = new ReplayClientIdCollisions(WINDOW_MS);
        long nowMs = 0;

        for (int restart = 0; restart < 10; restart++) {
            nowMs += WINDOW_MS * 6;  // hours apart, in window terms
            assertFalse(collisions.onRequest(CLIENT, 1, nowMs), "each restart is evidence of nothing on its own");
            for (long requestId = 2; requestId <= 30; requestId++) {
                assertFalse(collisions.onRequest(CLIENT, requestId, nowMs += 100));
            }
        }
    }

    @Test
    @DisplayName("two live clients interleaving their counters are reported, once")
    void interleavedCountersAreReportedOnce() {
        final ReplayClientIdCollisions collisions = new ReplayClientIdCollisions(WINDOW_MS);
        // Client A is well into its walk; client B, sharing the id, has just started. Both resend on
        // their own ~500ms timer, so the ids arrive interleaved.
        long nowMs = 0;
        int reports = 0;
        for (long step = 1; step <= 20; step++) {
            if (collisions.onRequest(CLIENT, 500 + step, nowMs += 250)) {
                reports++;
            }
            if (collisions.onRequest(CLIENT, step, nowMs += 250)) {
                reports++;
            }
        }

        assertEquals(1, reports, "reported exactly once, not at the pair's combined resend rate");
    }

    @Test
    @DisplayName("the report lands quickly, not after a long soak")
    void reportLandsWithinTheWindow() {
        final ReplayClientIdCollisions collisions = new ReplayClientIdCollisions(WINDOW_MS);
        long nowMs = 0;
        boolean reported = false;
        for (long step = 1; step <= 5 && !reported; step++) {
            collisions.onRequest(CLIENT, 500 + step, nowMs += 250);
            reported = collisions.onRequest(CLIENT, step, nowMs += 250);
        }

        assertTrue(reported, "three interleaved regressions is ~1.5s of two clients resending");
        assertTrue(nowMs < WINDOW_MS, "detected well inside the window, not at its expiry");
    }

    @Test
    @DisplayName("distinct client ids are tracked independently")
    void clientIdsAreIndependent() {
        final ReplayClientIdCollisions collisions = new ReplayClientIdCollisions(WINDOW_MS);
        long nowMs = 0;

        // Client 2 collides; client 1 and 3 are healthy throughout and must stay unreported.
        boolean otherReported = false;
        boolean collidingReported = false;
        for (long step = 1; step <= 10; step++) {
            otherReported |= collisions.onRequest(1, step, nowMs);
            otherReported |= collisions.onRequest(3, step, nowMs);
            collisions.onRequest(CLIENT, 500 + step, nowMs += 250);
            collidingReported |= collisions.onRequest(CLIENT, step, nowMs += 250);
        }

        assertTrue(collidingReported, "the colliding id is reported");
        assertFalse(otherReported, "a neighbour's collision must not implicate a healthy client");
    }
}
