package org.limitless.seqeron.replayer.server;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** When a cluster snapshot may be restarted from (doc/snapshot.md §10.8). */
class RestartSafetyTest {
    private static final long WINDOW = 1_000;
    private static final long NO_POSITION = RestartSafety.NO_POSITION;

    private final RestartSafety safety = new RestartSafety(WINDOW);

    @Test
    @DisplayName("with no application up, a cut is safe once the join window has passed")
    void aSourceThatNeverJoinsIsNotWaitedFor() {
        safety.onCut(1, 500, 0);
        assertEquals(NO_POSITION, safety.poll(WINDOW - 1), "an application may still join");
        assertEquals(500, safety.poll(WINDOW));
        assertEquals(NO_POSITION, safety.poll(WINDOW + 1), "and it is said once");
    }

    @Test
    @DisplayName("a source that joins is waited for, and the cut is safe when it confirms")
    void aSourceThatJoinsMustConfirm() {
        safety.onCut(1, 500, 0);
        safety.onJoined(1, 16);
        assertEquals(NO_POSITION, safety.poll(WINDOW + 5_000), "joined and not confirmed");
        safety.onConfirmed(1, 16);
        assertEquals(500, safety.poll(WINDOW + 5_001), "a replica that is slow makes it safe when it confirms");
    }

    @Test
    @DisplayName("every source that joined must confirm")
    void allMustConfirm() {
        safety.onCut(1, 500, 0);
        safety.onJoined(1, 16);
        safety.onJoined(1, 17);
        safety.onConfirmed(1, 16);
        assertEquals(NO_POSITION, safety.poll(WINDOW));
        safety.onConfirmed(1, 17);
        assertEquals(500, safety.poll(WINDOW));
    }

    @Test
    @DisplayName("a confirmation before the window ends also says the replica took part")
    void aConfirmationAloneCounts() {
        safety.onCut(1, 500, 0);
        safety.onConfirmed(1, 16);
        assertEquals(500, safety.poll(WINDOW));
    }

    @Test
    @DisplayName("a replica that joins after the window is not waited for")
    void aLateJoinerIsNotExpected() {
        safety.onCut(1, 500, 0);
        assertEquals(500, safety.poll(WINDOW));
        safety.onJoined(1, 16);
        assertEquals(NO_POSITION, safety.poll(WINDOW + 1));
    }

    @Test
    @DisplayName("a later round replaces one that did not finish, which stays unsafe")
    void aLaterRoundSupersedes() {
        safety.onCut(1, 500, 0);
        safety.onJoined(1, 16);
        safety.onCut(2, 900, 2_000);

        safety.onConfirmed(1, 16); // for the round before: nothing
        assertEquals(NO_POSITION, safety.poll(2_000 + WINDOW - 1));
        assertEquals(900, safety.poll(2_000 + WINDOW), "round 2 had no joiner of its own");
    }

    @Test
    @DisplayName("nothing is safe before a cut")
    void noCutNoSafety() {
        assertEquals(NO_POSITION, safety.poll(1_000_000));
        safety.onJoined(1, 16);
        safety.onConfirmed(1, 16);
        assertEquals(NO_POSITION, safety.poll(1_000_000));
    }
}
