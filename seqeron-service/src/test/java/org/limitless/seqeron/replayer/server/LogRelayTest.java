package org.limitless.seqeron.replayer.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.limitless.seqeron.protocol.FrameLayer;
import org.limitless.seqeron.sequencer.LogDeriver;

/**
 * A gateway host's log relay's decisions: where a replay of a member's log starts, and when it leaves a member or
 * stops. {@link LogRelay} holds no Aeron runtime and no clock.
 */
class LogRelayTest {
    private static final long STALL_TIMEOUT_MS = NodeDriver.DEFAULT_QUEUE_STALL_TIMEOUT_MS;

    private final LogRelay relay = new LogRelay(3, STALL_TIMEOUT_MS);

    @Test
    @DisplayName("the first member read fixes the start, and the next resumes after the last entry applied")
    void resumesAfterTheLastEntryOnAnyMember() {
        assertEquals(4096, relay.replayFrom(4096, 0, 10_000, 0));
        relay.onEntry(4224, 0);
        relay.onEntry(4352, 0);
        relay.onSourceLost(0);
        assertEquals(1, relay.sourceIndex());
        // This member started from its log's first entry, which is not where the derivation continues.
        assertEquals(4352, relay.replayFrom(0, 0, 10_000, 0));
    }

    @Test
    @DisplayName("the start stays the first member's even if it is lost before an entry arrives")
    void theStartIsFixedOnceAMemberIsSuitable() {
        assertEquals(4096, relay.replayFrom(4096, 0, 10_000, 0));
        relay.onSourceLost(0);
        assertEquals(4096, relay.replayFrom(0, 0, 10_000, 0));
    }

    @Test
    @DisplayName("a member whose log starts past the position, or has not reached it, is not suitable")
    void aLogThatDoesNotHoldThePositionIsNotSuitable() {
        assertEquals(LogRelay.NOT_SUITABLE, relay.replayFrom(0, 8192, 10_000, 0));
        assertEquals(LogRelay.NOT_SUITABLE, relay.position(), "and fixes no start");
        assertEquals(0, relay.replayFrom(0, 0, 10_000, 0));
        relay.onEntry(6000, 0);
        relay.onSourceLost(0);
        assertEquals(LogRelay.NOT_SUITABLE, relay.replayFrom(0, 0, 5_000, 0));
        assertEquals(LogRelay.NOT_SUITABLE, relay.replayFrom(0, 8192, 10_000, 0));
    }

    /** The relay has applied up to 4352 on member 0 and lost it; member 1 is next. */
    private void appliedUpTo4352() {
        relay.replayFrom(4096, 0, 10_000, 0);
        relay.onEntry(4352, 0);
        relay.onSourceLost(0);
    }

    @Test
    @DisplayName("members whose logs start past the position are left in turn, and the last one ends the relay")
    void membersTrimmedPastTheRelayLeaveNothingToRelay() {
        appliedUpTo4352();
        for (int member = 0; member < 3; member++) {
            assertEquals(member < 2 ? LogRelay.NOT_SUITABLE : LogRelay.HISTORY_LOST,
                         relay.replayFrom(0, 8192, 20_000, 0), "member " + member);
            relay.onSourceLost(0);
        }
        assertEquals(4352, relay.position());
    }

    @Test
    @DisplayName("a member that cannot be reached, or has not reached the position, is still to be tried")
    void onlyAMemberTrimmedPastCounts() {
        appliedUpTo4352();
        assertEquals(LogRelay.NOT_SUITABLE, relay.replayFrom(0, 8192, 20_000, 0));
        relay.onSourceLost(0);
        relay.onSourceLost(0); // the next member did not connect
        assertEquals(LogRelay.NOT_SUITABLE, relay.replayFrom(0, 8192, 20_000, 0), "one member was never examined");
        relay.onSourceLost(0);
        assertEquals(LogRelay.NOT_SUITABLE, relay.replayFrom(0, 0, 4000, 0), "behind, not trimmed past");
        relay.onSourceLost(0);
        assertEquals(LogRelay.NOT_SUITABLE, relay.replayFrom(0, 8192, 20_000, 0));
    }

    @Test
    @DisplayName("an entry applied clears what was found of the members")
    void progressClearsTheMembersTrimmedPast() {
        appliedUpTo4352();
        relay.replayFrom(0, 8192, 20_000, 0);
        relay.onSourceLost(0);
        relay.replayFrom(0, 8192, 20_000, 0);
        relay.onSourceLost(0);
        assertEquals(4352, relay.replayFrom(0, 0, 20_000, 0));
        relay.onEntry(4480, 0);
        relay.onSourceLost(0);
        assertEquals(LogRelay.NOT_SUITABLE, relay.replayFrom(0, 8192, 20_000, 0));
        relay.onSourceLost(0);
        assertEquals(LogRelay.NOT_SUITABLE, relay.replayFrom(0, 8192, 20_000, 0));
    }

    @Test
    @DisplayName("before the start is fixed, a member whose log starts past its own announced start is only left")
    void theFirstReadNeverEndsTheRelay() {
        for (int member = 0; member < 6; member++) {
            assertEquals(LogRelay.NOT_SUITABLE, relay.replayFrom(4096, 8192, 20_000, 0), "member " + member % 3);
            relay.onSourceLost(0);
        }
    }

    @Test
    @DisplayName("it is live from the first entry recorded after its connect, and stays live across members")
    void liveOncePastWhatWasRecordedAtTheConnect() {
        relay.replayFrom(0, 0, 1_000, 0);
        relay.onEntry(1_000, 0);
        assertFalse(relay.isLive(), "the last entry recorded at the connect is history");
        relay.onEntry(1_128, 0);
        assertTrue(relay.isLive());
        relay.onSourceLost(0);
        relay.replayFrom(0, 0, 50_000, 0);
        assertTrue(relay.isLive());
    }

    @Test
    @DisplayName("a member that sends nothing for the source timeout is left, and all three failing pauses the relay")
    void aSilentMemberIsLeft() {
        relay.replayFrom(0, 0, 1_000, 0);
        assertFalse(relay.isSourceStalled(LogRelay.SOURCE_TIMEOUT_MS - 1));
        assertTrue(relay.isSourceStalled(LogRelay.SOURCE_TIMEOUT_MS));
        relay.onEntry(128, LogRelay.SOURCE_TIMEOUT_MS);
        assertFalse(relay.isSourceStalled(LogRelay.SOURCE_TIMEOUT_MS + 1));

        relay.onSourceLost(10_000);
        relay.onSourceLost(10_000);
        assertTrue(relay.mayAttempt(10_000));
        relay.onSourceLost(10_000);
        assertEquals(0, relay.sourceIndex());
        assertFalse(relay.mayAttempt(10_000 + LogRelay.RETRY_INTERVAL_MS - 1));
        assertTrue(relay.mayAttempt(10_000 + LogRelay.RETRY_INTERVAL_MS));
    }

    @Test
    @DisplayName("back-pressure stops the relay only once the local recording has made no progress for the timeout")
    void backPressureIsFatalOnlyWithoutProgress() {
        assertFalse(relay.onBackPressured(0, 100));
        assertFalse(relay.onBackPressured(STALL_TIMEOUT_MS - 1, 100));
        assertFalse(relay.onBackPressured(STALL_TIMEOUT_MS, 200), "the recording advanced");
        assertFalse(relay.onBackPressured(2 * STALL_TIMEOUT_MS - 1, 200));
        assertTrue(relay.onBackPressured(2 * STALL_TIMEOUT_MS, 200));
        relay.onOffered();
        assertFalse(relay.onBackPressured(3 * STALL_TIMEOUT_MS, 200));
    }

    @Test
    @DisplayName("an announcement decodes to what was encoded, and to nothing while a record is missing")
    void anAnnouncementRoundTrips() {
        final long[] queueStarts = new long[FrameLayer.MAX_GROUPS];
        queueStarts[0] = 1472;
        queueStarts[1] = 960;
        final DerivationStart start = new DerivationStart(
            1760, new LogDeriver.State(List.of(new byte[] { 1, 2, 3 }, new byte[] { 4 }), new long[] { 7, 9 }), true,
            queueStarts, 65536, 1408);
        final List<byte[]> messages = new DerivationAnnouncement(start, 42).encode();
        assertEquals(3, messages.size());
        assertNull(DerivationAnnouncement.decode(messages.subList(0, 2)));
        assertNull(DerivationAnnouncement.decode(List.of()));

        final DerivationAnnouncement decoded = DerivationAnnouncement.decode(messages);
        assertEquals(42, decoded.commitCounterId());
        assertEquals(1760, decoded.start().logPosition());
        assertTrue(decoded.start().restored());
        assertEquals(65536, decoded.start().termLength());
        assertEquals(1408, decoded.start().mtu());
        assertEquals(List.of(1472L, 960L), List.of(decoded.start().queueStarts()[0], decoded.start().queueStarts()[1]));
        assertEquals(List.of(7L, 9L),
                     List.of(decoded.start().state().openSessions()[0], decoded.start().state().openSessions()[1]));
        assertEquals(2, decoded.start().state().sequencer().size());
        assertEquals(4, decoded.start().state().sequencer().get(1)[0]);
    }

    @Test
    @DisplayName("of several announcements the last whole one is read")
    void theLastWholeAnnouncementIsRead() {
        final LogDeriver.State state = new LogDeriver.State(List.of(new byte[] { 1 }), new long[0]);
        final List<byte[]> messages = new ArrayList<>();
        for (final long logPosition : new long[] { 0, 4096, 8192 }) {
            messages.addAll(
                new DerivationAnnouncement(new DerivationStart(logPosition, state, logPosition > 0,
                                                               new long[FrameLayer.MAX_GROUPS], 65536, 1408),
                                           42)
                    .encode());
        }
        assertEquals(8192, DerivationAnnouncement.decode(messages).start().logPosition());
        assertEquals(4096, DerivationAnnouncement.decode(messages.subList(0, 5)).start().logPosition(),
                     "the third's record has not arrived");
    }
}
