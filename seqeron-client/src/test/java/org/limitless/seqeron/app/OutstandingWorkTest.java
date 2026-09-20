package org.limitless.seqeron.app;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the outstanding-work state machine, one situation per case. No Aeron runtime. Case for
 * case with {@code OutstandingWorkTest.cpp}; {@link OutstandingWorkPropertyTest} drives the interleavings.
 */
class OutstandingWorkTest {
    /** Records every request it takes, refusing once {@code slots} run out — a leader with no free worker. */
    private static final class Recorder implements OutstandingWork.Dispatcher<Long, Integer> {
        final List<Long> dispatched = new ArrayList<>();
        int slots = Integer.MAX_VALUE;

        @Override
        public boolean dispatch(final Long key, final Integer work) {
            if (slots <= 0) {
                return false;
            }
            --slots;
            dispatched.add(key);
            return true;
        }
    }

    private final OutstandingWork<Long, Integer> work = new OutstandingWork<>();

    @Test
    @DisplayName("a request is tracked, and re-asserting it adds nothing")
    void requestIsTrackedAndIdempotent() {
        work.onRequest(1L, 10);
        work.onRequest(1L, 11);

        assertEquals(1, work.size());
        assertFalse(work.isDispatched(1L));
    }

    @Test
    @DisplayName("the leader dispatches each request once")
    void leaderDispatchesEachRequestOnce() {
        work.onRequest(1L, 0);
        work.onRequest(2L, 0);

        final Recorder first = new Recorder();
        assertEquals(2, work.dispatchUndispatched(first));
        assertEquals(List.of(1L, 2L), first.dispatched);
        assertTrue(work.isDispatched(1L));
        assertTrue(work.isDispatched(2L));

        final Recorder again = new Recorder();
        assertEquals(0, work.dispatchUndispatched(again), "both are awaiting their replies");
        assertTrue(again.dispatched.isEmpty());
    }

    @Test
    @DisplayName("a promoted follower dispatches what it only tracked")
    void promotedFollowerDispatchesTrackedRequests() {
        work.onRequest(1L, 0);
        work.onRequest(2L, 0);
        work.onRequest(3L, 0);
        assertFalse(work.isDispatched(1L), "never dispatched while a follower");

        final Recorder promoted = new Recorder();
        assertEquals(3, work.dispatchUndispatched(promoted), "including requests sequenced before it led");
    }

    @Test
    @DisplayName("a sequenced reply removes the request, so no sweep dispatches it again")
    void replyRemovesRequest() {
        work.onRequest(1L, 0);
        work.dispatchUndispatched(new Recorder());

        work.onReply(1L);

        assertEquals(0, work.size());
        assertFalse(work.isDispatched(1L));
        assertEquals(0, work.dispatchUndispatched(new Recorder()));
    }

    @Test
    @DisplayName("losing leadership re-dispatches an unanswered request")
    void leadershipLossReDispatches() {
        work.onRequest(1L, 0);
        work.dispatchUndispatched(new Recorder()); // its reply is lost in flight

        work.onNotLeader();
        assertFalse(work.isDispatched(1L));

        final Recorder next = new Recorder();
        assertEquals(1, work.dispatchUndispatched(next));
        assertEquals(List.of(1L), next.dispatched);
    }

    @Test
    @DisplayName("a reply that never reached the cluster is re-dispatched by the same leader")
    void replyNotEmittedReDispatches() {
        work.onRequest(1L, 0);
        work.dispatchUndispatched(new Recorder());

        work.onReplyNotEmitted(1L);
        assertFalse(work.isDispatched(1L));
        assertEquals(1, work.size(), "still outstanding: only a sequenced reply discharges it");

        assertEquals(1, work.dispatchUndispatched(new Recorder()), "no leadership change needed");

        work.onReply(1L);
        assertEquals(0, work.size());
        assertEquals(0, work.dispatchUndispatched(new Recorder()));
    }

    @Test
    @DisplayName("running out of capacity stops the sweep, and the next call resumes it")
    void capacityStopsTheSweep() {
        work.onRequest(1L, 0);
        work.onRequest(2L, 0);
        work.onRequest(3L, 0);

        final Recorder first = new Recorder();
        first.slots = 2;
        assertEquals(2, work.dispatchUndispatched(first));

        final Recorder rest = new Recorder();
        assertEquals(1, work.dispatchUndispatched(rest));

        assertEquals(List.of(1L, 2L), first.dispatched);
        assertEquals(List.of(3L), rest.dispatched, "each dispatched exactly once");
    }

    @Test
    @DisplayName("a reply for an unknown key is harmless")
    void replyForUnknownKeyIsHarmless() {
        work.onReply(42L);

        assertEquals(0, work.size());
    }

    @Test
    @DisplayName("dispatch follows request order, and a re-asserted request keeps its place")
    void dispatchFollowsRequestOrder() {
        work.onRequest(5L, 0);
        work.onRequest(3L, 0);
        work.onRequest(9L, 0);
        work.onRequest(5L, 1);

        final Recorder recorder = new Recorder();
        work.dispatchUndispatched(recorder);

        assertEquals(List.of(5L, 3L, 9L), recorder.dispatched, "the order requests were sequenced, not key order");
    }
}
