package org.limitless.seqeron.sequencer.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.limitless.seqeron.protocol.FrameLayer;
import org.limitless.seqeron.protocol.SystemFrame;
import org.limitless.seqeron.replayer.client.SequencedEvents;
import org.limitless.seqeron.sequencer.client.IngressSender;

/**
 * Unit tests for counting a producer's own frames back off its tap. Frames are real ingress frames; each
 * body is one int, so a test names a frame by its number. Case for case with {@code PendingSendsTest.cpp}.
 */
class PendingSendsTest {
    private static final int CAPACITY = 4;
    private static final long OWN = 4242;
    private static final long SIBLING = 4343;
    private static final int PAYLOAD_ID = 6;

    /** Takes resends as the cluster's new leader would, recording each frame's number, until told to refuse. */
    private static final class FakeSender implements IngressSender {
        private final List<Integer> sent = new ArrayList<>();
        private long session = OWN;
        private long term;
        private int acceptsLeft = Integer.MAX_VALUE;

        FakeSender(final long term) {
            this.term = term;
        }

        @Override
        public boolean send(final DirectBuffer frame, final int length) {
            if (acceptsLeft == 0) {
                return false;
            }
            acceptsLeft--;
            sent.add(frame.getInt(FrameLayer.MIN_INGRESS_LENGTH));
            return true;
        }

        @Override
        public long clusterSessionId() {
            return session;
        }

        @Override
        public long leadershipTermId() {
            return term;
        }
    }

    private final PendingSends pending = new PendingSends(CAPACITY);
    private final SystemFrame envelope = new SystemFrame();
    private final ExpandableArrayBuffer frame = new ExpandableArrayBuffer(FrameLayer.MAX_INGRESS_LENGTH);
    private final UnsafeBuffer body = new UnsafeBuffer(new byte[Integer.BYTES]);

    /** Tracks application frame {@code n} as placed on {@code session}, stamped {@code term}. */
    private void send(final long session, final long term, final int n) {
        body.putInt(0, n);
        pending.track(frame, envelope.wrapPayload(frame, 1, 2, session, PAYLOAD_ID, body, Integer.BYTES), session,
                      term);
    }

    private void sendSystem(final long session, final long term, final int eventType, final int n) {
        body.putInt(0, n);
        pending.track(frame, envelope.wrap(frame, 1, 2, session, eventType, body, Integer.BYTES), session, term);
    }

    /** Application frame {@code n} arriving on the tap from {@code session}. */
    private void tap(final long session, final int n) {
        tap(session, false, PAYLOAD_ID, n);
    }

    private void tap(final long session, final boolean system, final int id, final int n) {
        body.putInt(0, n);
        pending.onSequenced(SequencedEvents.of(session, system, id, body, 0, Integer.BYTES));
    }

    @Test
    @DisplayName("every frame seen on its own tap leaves nothing pending")
    void everyFrameSeenLeavesNothingPending() {
        send(OWN, 1, 1);
        send(OWN, 1, 2);
        assertEquals(2, pending.size());
        tap(OWN, 1);
        tap(OWN, 2);
        assertEquals(0, pending.size());
        assertEquals(0, pending.missing());
        assertFalse(pending.isFaulted());
    }

    @Test
    @DisplayName("a leader change after every frame came back loses nothing")
    void leaderChangeAfterEveryFrameLosesNothing() {
        send(OWN, 1, 1);
        tap(OWN, 1);
        pending.onLeadershipChanged(2);
        assertEquals(0, pending.missing());
        assertEquals(0, pending.size());
    }

    @Test
    @DisplayName("frames not seen before a later term's LeadershipChanged are missing")
    void framesNotSeenBeforeALaterTermAreMissing() {
        send(OWN, 1, 1);
        send(OWN, 1, 2);
        send(OWN, 1, 3);
        tap(OWN, 1);
        assertEquals(0, pending.missing(), "still pending until a later term closes the count");
        pending.onLeadershipChanged(2);
        assertEquals(2, pending.missing());
        assertEquals(2, pending.size());
    }

    @Test
    @DisplayName("the LeadershipChanged that opens a frame's own term does not count it missing")
    void ownTermsLeadershipChangedDoesNotCountIt() {
        // A tap still replaying history reaches term 1's opening frame after frames stamped 1 went out.
        send(OWN, 1, 1);
        pending.onLeadershipChanged(1);
        assertEquals(0, pending.missing());
        tap(OWN, 1);
        assertEquals(0, pending.size());
    }

    @Test
    @DisplayName("a frame stamped with a term the tap has already closed is missing at once")
    void staleTermFrameIsMissingAtOnce() {
        // The tap showed the new term before egress brought the NewLeader, so the leader dropped this frame.
        pending.onLeadershipChanged(2);
        send(OWN, 1, 1);
        assertEquals(1, pending.missing());
    }

    @Test
    @DisplayName("a replaced session's lost frames stay missing while the new session's come back")
    void replacedSessionsLostFramesStayMissing() {
        send(OWN, 1, 1);
        send(OWN, 1, 2);
        pending.onLeadershipChanged(2);
        send(OWN + 1, 2, 3);
        send(OWN + 1, 2, 4);
        tap(OWN + 1, 3);
        tap(OWN + 1, 4);
        assertEquals(2, pending.missing());
        assertEquals(2, pending.size());
        assertFalse(pending.isFaulted());
    }

    @Test
    @DisplayName("the sibling's frames under the same sourceId are ignored")
    void siblingFramesAreIgnored() {
        send(OWN, 1, 1);
        tap(SIBLING, 1);
        tap(SIBLING, 7);
        assertEquals(1, pending.size());
        assertFalse(pending.isFaulted());
    }

    @Test
    @DisplayName("an own frame that differs from the oldest pending copy latches the fault")
    void ownFrameDifferingFromOldestCopyLatchesFault() {
        // The sequencer rejected frame 1 (S-7), so frame 2 is the next own frame on the tap.
        send(OWN, 1, 1);
        send(OWN, 1, 2);
        tap(OWN, 2);
        assertTrue(pending.isFaulted());
    }

    @Test
    @DisplayName("an own system frame comes back like an application one")
    void ownSystemFrameComesBack() {
        sendSystem(OWN, 1, SystemFrame.CONNECTION_CLOSED, 1);
        tap(OWN, true, SystemFrame.CONNECTION_CLOSED, 1);
        assertEquals(0, pending.size());
        assertFalse(pending.isFaulted());
    }

    @Test
    @DisplayName("an application frame never matches a system one with the same id")
    void applicationFrameNeverMatchesSystemOne() {
        sendSystem(OWN, 1, SystemFrame.CONNECTION_CLOSED, 1);
        tap(OWN, false, SystemFrame.CONNECTION_CLOSED, 1);
        assertTrue(pending.isFaulted());
    }

    @Test
    @DisplayName("tracking into a full ring latches the fault")
    void trackingIntoAFullRingLatchesFault() {
        for (int n = 0; n < CAPACITY; n++) {
            send(OWN, 1, n);
        }
        assertTrue(pending.isFull());
        assertFalse(pending.isFaulted());
        send(OWN, 1, CAPACITY);
        assertTrue(pending.isFaulted());
        assertEquals(CAPACITY, pending.size());
    }

    @Test
    @DisplayName("a new leader holds new sends until the older frames are seen")
    void newLeaderHoldsUntilOlderFramesAreSeen() {
        send(OWN, 1, 1);
        pending.onNewLeader(2);
        assertTrue(pending.isHolding());
        tap(OWN, 1);
        assertFalse(pending.isHolding());
    }

    @Test
    @DisplayName("a new term with nothing pending holds nothing")
    void newTermWithNothingPendingHoldsNothing() {
        pending.onNewLeader(2);
        pending.onLeadershipChanged(2);
        assertFalse(pending.isHolding());
    }

    @Test
    @DisplayName("the tap's LeadershipChanged holds before the NewLeader comes, and nothing is resent until it does")
    void tapsLeadershipChangedHoldsBeforeNewLeader() {
        final FakeSender sender = new FakeSender(1);
        send(OWN, 1, 1);
        pending.onLeadershipChanged(2);
        assertTrue(pending.isHolding());
        assertEquals(0, pending.resendMissing(sender));
        assertEquals(List.of(), sender.sent);
    }

    @Test
    @DisplayName("the missing frames are resent oldest first, and that releases the hold")
    void missingFramesAreResentOldestFirst() {
        final FakeSender sender = new FakeSender(2);
        send(OWN, 1, 1);
        send(OWN, 1, 2);
        send(OWN, 1, 3);
        tap(OWN, 1);
        pending.onNewLeader(2);
        pending.onLeadershipChanged(2);
        assertEquals(2, pending.resendMissing(sender));
        assertEquals(List.of(2, 3), sender.sent);
        assertEquals(0, pending.missing());
        assertFalse(pending.isHolding());
        tap(OWN, 2);
        tap(OWN, 3);
        assertEquals(0, pending.size());
        assertFalse(pending.isFaulted());
    }

    @Test
    @DisplayName("a resend cut short keeps holding, and the rest follow in order")
    void resendCutShortKeepsHolding() {
        final FakeSender sender = new FakeSender(2);
        send(OWN, 1, 1);
        send(OWN, 1, 2);
        send(OWN, 1, 3);
        pending.onNewLeader(2);
        pending.onLeadershipChanged(2);
        sender.acceptsLeft = 1;
        assertEquals(1, pending.resendMissing(sender));
        assertTrue(pending.isHolding());
        assertEquals(2, pending.missing());
        sender.acceptsLeft = Integer.MAX_VALUE;
        assertEquals(2, pending.resendMissing(sender));
        assertEquals(List.of(1, 2, 3), sender.sent);
        assertFalse(pending.isHolding());
        tap(OWN, 1);
        tap(OWN, 2);
        tap(OWN, 3);
        assertEquals(0, pending.size());
        assertFalse(pending.isFaulted());
    }

    @Test
    @DisplayName("a frame resent on a replaced session is matched on the new one")
    void resentFrameMatchesOnTheNewSession() {
        final FakeSender sender = new FakeSender(2);
        sender.session = OWN + 1;
        send(OWN, 1, 1);
        pending.onNewLeader(2);
        pending.onLeadershipChanged(2);
        pending.resendMissing(sender);
        tap(OWN, 1);
        assertEquals(1, pending.size());
        tap(OWN + 1, 1);
        assertEquals(0, pending.size());
    }

    @Test
    @DisplayName("a resent frame lost again is resent again")
    void resentFrameLostAgainIsResentAgain() {
        final FakeSender sender = new FakeSender(2);
        send(OWN, 1, 1);
        pending.onNewLeader(2);
        pending.onLeadershipChanged(2);
        pending.resendMissing(sender);
        pending.onNewLeader(3);
        pending.onLeadershipChanged(3);
        assertTrue(pending.isHolding());
        assertEquals(1, pending.missing());
        sender.term = 3;
        assertEquals(1, pending.resendMissing(sender));
        assertEquals(List.of(1, 1), sender.sent);
        tap(OWN, 1);
        assertEquals(0, pending.size());
    }

    @Test
    @DisplayName("a full ring resends in place, in order")
    void fullRingResendsInPlace() {
        final FakeSender sender = new FakeSender(2);
        for (int n = 0; n < CAPACITY; n++) {
            send(OWN, 1, n);
        }
        pending.onNewLeader(2);
        pending.onLeadershipChanged(2);
        assertEquals(CAPACITY, pending.resendMissing(sender));
        assertEquals(List.of(0, 1, 2, 3), sender.sent);
        for (int n = 0; n < CAPACITY; n++) {
            tap(OWN, n);
        }
        assertEquals(0, pending.size());
        assertFalse(pending.isFaulted());
    }
}
