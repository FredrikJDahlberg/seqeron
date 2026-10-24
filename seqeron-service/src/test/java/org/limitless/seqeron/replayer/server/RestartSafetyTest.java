package org.limitless.seqeron.replayer.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
        safety.onJoined(1, 16, 16);
        assertEquals(NO_POSITION, safety.poll(WINDOW + 5_000), "joined and not confirmed");
        safety.onConfirmed(1, 16, 16);
        assertEquals(500, safety.poll(WINDOW + 5_001), "a replica that is slow makes it safe when it confirms");
    }

    @Test
    @DisplayName("every source that joined must confirm")
    void allMustConfirm() {
        safety.onCut(1, 500, 0);
        safety.onJoined(1, 16, 16);
        safety.onJoined(1, 17, 17);
        safety.onConfirmed(1, 16, 16);
        assertEquals(NO_POSITION, safety.poll(WINDOW));
        safety.onConfirmed(1, 17, 17);
        assertEquals(500, safety.poll(WINDOW));
    }

    @Test
    @DisplayName("a confirmation before the window ends also says the replica took part")
    void aConfirmationAloneCounts() {
        safety.onCut(1, 500, 0);
        safety.onConfirmed(1, 16, 16);
        assertEquals(500, safety.poll(WINDOW));
    }

    @Test
    @DisplayName("a replica that joins after the window is not waited for")
    void aLateJoinerIsNotExpected() {
        safety.onCut(1, 500, 0);
        assertEquals(500, safety.poll(WINDOW));
        safety.onJoined(1, 16, 16);
        assertEquals(NO_POSITION, safety.poll(WINDOW + 1));
    }

    @Test
    @DisplayName("a later round replaces one that did not finish, which stays unsafe")
    void aLaterRoundSupersedes() {
        safety.onCut(1, 500, 0);
        safety.onJoined(1, 16, 16);
        safety.onCut(2, 900, 2_000);

        safety.onConfirmed(1, 16, 16); // for the round before: nothing
        assertEquals(NO_POSITION, safety.poll(2_000 + WINDOW - 1));
        assertEquals(900, safety.poll(2_000 + WINDOW), "round 2 had no joiner of its own");
    }

    @Test
    @DisplayName("nothing is safe before a cut")
    void noCutNoSafety() {
        assertEquals(NO_POSITION, safety.poll(1_000_000));
        safety.onJoined(1, 16, 16);
        safety.onConfirmed(1, 16, 16);
        assertEquals(NO_POSITION, safety.poll(1_000_000));
    }

    @Test
    @DisplayName("two clients joining for one source leave the round unsafe, whatever they confirm")
    void twoClientsForOneSourceAreUnsafe() {
        safety.onCut(1, 500, 0);
        assertFalse(safety.onJoined(1, 16, 100));
        assertTrue(safety.onJoined(1, 16, 101), "the first conflict is reported");
        assertFalse(safety.onConfirmed(1, 16, 100), "and only once");
        safety.onConfirmed(1, 16, 101);
        assertEquals(NO_POSITION, safety.poll(WINDOW + 5_000));
    }

    @Test
    @DisplayName("a client that confirms after joining is one client, and the next round is judged afresh")
    void theNextRoundIsJudgedAfresh() {
        safety.onCut(1, 500, 0);
        safety.onJoined(1, 16, 100);
        assertFalse(safety.onConfirmed(1, 16, 100));
        safety.onJoined(1, 16, 101);
        assertEquals(NO_POSITION, safety.poll(WINDOW));

        safety.onCut(2, 900, 2_000);
        safety.onJoined(2, 16, 101);
        safety.onConfirmed(2, 16, 101);
        assertEquals(900, safety.poll(2_000 + WINDOW));
    }

    @Test
    @DisplayName("a confirmation that precedes its round's cut is kept for it")
    void aConfirmationBeforeItsCutIsKept() {
        safety.onCut(1, 500, 0);
        safety.onConfirmed(2, 16, 100); // the round after the one tracked
        safety.onCut(2, 900, 2_000);

        assertEquals(NO_POSITION, safety.poll(2_000 + WINDOW - 1));
        safety.onJoined(2, 16, 100);
        assertEquals(900, safety.poll(2_000 + WINDOW), "the source joined and had already confirmed");
    }

    @Test
    @DisplayName("a kept confirmation holds a cut back until it is joined by an unconfirmed source too")
    void aKeptConfirmationDoesNotHideAnotherSource() {
        safety.onConfirmed(2, 16, 100);
        safety.onCut(2, 900, 2_000);
        safety.onJoined(2, 17, 101);

        assertEquals(NO_POSITION, safety.poll(2_000 + WINDOW));
        safety.onConfirmed(2, 17, 101);
        assertEquals(900, safety.poll(2_000 + WINDOW));
    }

    @Test
    @DisplayName("a confirmation for a round that is skipped is not carried to a later one")
    void aSkippedRoundsConfirmationIsDropped() {
        safety.onConfirmed(2, 16, 100);
        safety.onCut(3, 900, 2_000);
        safety.onJoined(3, 17, 101);
        safety.onConfirmed(3, 17, 101);

        assertEquals(900, safety.poll(2_000 + WINDOW));
        safety.onCut(2, 950, 9_000); // it is no longer expected, and nothing of it is left
        assertEquals(950, safety.poll(9_000 + WINDOW), "round 2 has no joiner of its own");
    }
}
