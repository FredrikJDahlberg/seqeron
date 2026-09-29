package org.limitless.seqeron.replayer.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The gateway-host relay's decisions: what it republishes, where a replay of a member's recording starts,
 * and when it leaves a member or stops. {@link TapRelay} holds no Aeron runtime and no clock, so each case
 * drives it with the frames and times a member's replay would produce.
 *
 * <p>The property every case protects is the local tap's: each {@code globalSeqNo} exactly once, in order,
 * across any number of member changes. A duplicate or a hole there is served to every client on the host.
 */
class TapRelayTest {
    private static final long START = 0;

    /** Where frame {@code n} ends in every member's recording, when the layouts agree. */
    private static long end(final long globalSeqNo) {
        return globalSeqNo * 64;
    }

    private final TapRelay relay = new TapRelay(3);

    /** Plays frames {@code from..to} through, publishing each one the relay asks for. */
    private void publish(final long from, final long to, final long nowMs) {
        for (long n = from; n <= to; n++) {
            assertEquals(TapRelay.Verdict.PUBLISH, relay.onFrame(n, nowMs), "frame " + n);
            relay.onPublished(n, end(n));
        }
    }

    @Test
    @DisplayName("a relay that has published nothing replays a member's recording from its start")
    void coldStartReplaysFromTheStart() {
        assertEquals(START, relay.replayFrom(START, end(100), 0));
        publish(1, 3, 0);
        assertEquals(3, relay.lastGlobalSeqNo());
    }

    @Test
    @DisplayName("a frame already published is skipped, and only the next one is published")
    void skipsWhatIsPublished() {
        relay.replayFrom(START, end(100), 0);
        publish(1, 5, 0);
        assertEquals(TapRelay.Verdict.SKIP, relay.onFrame(4, 0));
        assertEquals(TapRelay.Verdict.SKIP, relay.onFrame(5, 0));
        assertEquals(TapRelay.Verdict.PUBLISH, relay.onFrame(6, 0));
    }

    @Test
    @DisplayName("the next member's replay resumes at the position the last one reached")
    void resumesWhereTheLastMemberStopped() {
        relay.replayFrom(START, end(100), 0);
        publish(1, 10, 0);
        relay.onSourceLost(0);

        assertEquals(1, relay.sourceIndex());
        assertEquals(end(10), relay.replayFrom(START, end(200), 0));
        assertEquals(TapRelay.Verdict.PUBLISH, relay.onFrame(11, 0));
    }

    @Test
    @DisplayName("a member that has not recorded as far as the relay has published is not read")
    void aMemberBehindTheRelayIsNotSuitable() {
        relay.replayFrom(START, end(100), 0);
        publish(1, 10, 0);
        relay.onSourceLost(0);

        assertEquals(TapRelay.NOT_SUITABLE, relay.replayFrom(START, end(9), 0));
    }

    @Test
    @DisplayName("a member whose recording starts past the resume position is replayed from its own start")
    void aRecordingStartingLaterIsReplayedFromItsStart() {
        relay.replayFrom(START, end(100), 0);
        publish(1, 10, 0);
        relay.onSourceLost(0);

        assertEquals(end(20), relay.replayFrom(end(20), end(200), 0));
    }

    /**
     * Positions that do not line up on the next member: its frame at the resume position is not the next one.
     * The relay starts that recording again from its start and skips its way back to where it was.
     */
    @Test
    @DisplayName("a resumed replay that skips a frame is replayed again from the recording's start")
    void aHoleOnAResumedReplayRestartsFromTheStart() {
        relay.replayFrom(START, end(100), 0);
        publish(1, 10, 0);
        relay.onSourceLost(0);
        relay.replayFrom(START, end(200), 0);

        assertEquals(TapRelay.Verdict.REPLAY_FROM_START, relay.onFrame(12, 0));
        assertEquals(START, relay.replayFrom(START, end(200), 0));
        for (long n = 1; n <= 10; n++) {
            assertEquals(TapRelay.Verdict.SKIP, relay.onFrame(n, 0), "frame " + n);
        }
        assertEquals(TapRelay.Verdict.PUBLISH, relay.onFrame(11, 0));
    }

    @Test
    @DisplayName("a fragment that is not a frame, on a resumed replay, restarts it from the recording's start")
    void anUnreadableFragmentRestartsFromTheStart() {
        relay.replayFrom(START, end(100), 0);
        publish(1, 10, 0);
        relay.onSourceLost(0);
        relay.replayFrom(START, end(200), 0);

        assertEquals(TapRelay.Verdict.REPLAY_FROM_START, relay.onUnreadable());
        assertEquals(START, relay.replayFrom(START, end(200), 0));
    }

    @Test
    @DisplayName("a hole in a replay from the recording's start leaves that member")
    void aHoleFromTheStartDropsTheSource() {
        relay.replayFrom(START, end(100), 0);
        publish(1, 3, 0);

        assertEquals(TapRelay.Verdict.DROP_SOURCE, relay.onFrame(5, 0));
    }

    @Test
    @DisplayName("a member that sends nothing for the source timeout is stalled; any frame keeps it alive")
    void aSilentMemberStalls() {
        relay.replayFrom(START, end(100), 1_000);
        assertFalse(relay.isSourceStalled(1_000 + TapRelay.SOURCE_TIMEOUT_MS - 1));
        assertTrue(relay.isSourceStalled(1_000 + TapRelay.SOURCE_TIMEOUT_MS));

        publish(1, 1, 5_000);
        relay.onFrame(1, 7_000); // a skipped frame proves the member alive as well as a published one
        assertFalse(relay.isSourceStalled(7_000 + TapRelay.SOURCE_TIMEOUT_MS - 1));
    }

    @Test
    @DisplayName("a lost member moves the relay to the next, wrapping, and it pauses only once all have failed")
    void membersAreTriedInTurn() {
        relay.onSourceLost(0);
        assertEquals(1, relay.sourceIndex());
        assertTrue(relay.mayAttempt(0), "the next member is tried at once");
        relay.onSourceLost(0);
        assertEquals(2, relay.sourceIndex());
        assertTrue(relay.mayAttempt(0));

        relay.onSourceLost(0);
        assertEquals(0, relay.sourceIndex());
        assertFalse(relay.mayAttempt(TapRelay.RETRY_INTERVAL_MS - 1), "every member failed in a row");
        assertTrue(relay.mayAttempt(TapRelay.RETRY_INTERVAL_MS));
    }

    @Test
    @DisplayName("a member that delivered frames before it was lost does not count toward the pause")
    void aWorkingMemberResetsTheFailureCount() {
        relay.onSourceLost(0);
        relay.onSourceLost(0);
        relay.replayFrom(START, end(100), 0);
        publish(1, 1, 0);

        relay.onSourceLost(0);
        assertTrue(relay.mayAttempt(0));
    }

    @Test
    @DisplayName("back-pressure is fatal only once the local recording stops advancing for the stall limit")
    void backPressureIsFatalOnlyWithoutRecordingProgress() {
        relay.replayFrom(START, end(100), 0);
        assertEquals(TapRelay.Verdict.PUBLISH, relay.onFrame(1, 0));

        assertFalse(relay.onBackPressured(0, 100));
        assertFalse(relay.onBackPressured(900, 200), "the recording advanced, so the clock restarts");
        assertFalse(relay.onBackPressured(900 + TapRelay.RECORDING_STALL_FATAL_MS - 1, 200));
        assertTrue(relay.onBackPressured(900 + TapRelay.RECORDING_STALL_FATAL_MS, 200));
    }

    @Test
    @DisplayName("back-pressure does not stall the member, and a publish ends the episode")
    void backPressureKeepsTheMemberAndAPublishClearsIt() {
        relay.replayFrom(START, end(100), 0);
        relay.onFrame(1, 0);
        relay.onBackPressured(0, 100);
        relay.onBackPressured(TapRelay.SOURCE_TIMEOUT_MS, 100);
        assertFalse(relay.isSourceStalled(TapRelay.SOURCE_TIMEOUT_MS), "the local tap is what is slow");

        relay.onPublished(1, end(1));
        assertFalse(relay.onBackPressured(10 * TapRelay.RECORDING_STALL_FATAL_MS, 100), "a new episode");
    }

    @Test
    @DisplayName("a relay needs at least one member archive")
    void needsAMember() {
        assertThrows(IllegalArgumentException.class, () -> new TapRelay(0));
    }
}
