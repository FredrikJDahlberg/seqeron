package org.limitless.seqeron.app;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.management.ThreadMXBean;
import java.lang.management.ManagementFactory;
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
    private static final class Recorder implements OutstandingWork.Dispatcher<int[]> {
        final List<Long> dispatched = new ArrayList<>();
        int slots = Integer.MAX_VALUE;

        @Override
        public boolean dispatch(final long key, final int[] work) {
            if (slots <= 0) {
                return false;
            }
            --slots;
            dispatched.add(key);
            return true;
        }
    }

    private final OutstandingWork<int[]> work = new OutstandingWork<>(() -> new int[1]);

    @Test
    @DisplayName("a request is tracked, and re-asserting it returns its work and adds nothing")
    void requestIsTrackedAndIdempotent() {
        final int[] request = work.onRequest(1L);
        request[0] = 10;

        assertSame(request, work.onRequest(1L));
        assertEquals(1, work.size());
        assertFalse(work.isDispatched(1L));
    }

    @Test
    @DisplayName("the leader dispatches each request once")
    void leaderDispatchesEachRequestOnce() {
        work.onRequest(1L);
        work.onRequest(2L);

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
        work.onRequest(1L);
        work.onRequest(2L);
        work.onRequest(3L);
        assertFalse(work.isDispatched(1L), "never dispatched while a follower");

        final Recorder promoted = new Recorder();
        assertEquals(3, work.dispatchUndispatched(promoted), "including requests sequenced before it led");
    }

    @Test
    @DisplayName("a sequenced reply removes the request, so no sweep dispatches it again")
    void replyRemovesRequest() {
        work.onRequest(1L);
        work.dispatchUndispatched(new Recorder());

        work.onReply(1L);

        assertEquals(0, work.size());
        assertFalse(work.isDispatched(1L));
        assertEquals(0, work.dispatchUndispatched(new Recorder()));
    }

    @Test
    @DisplayName("losing leadership re-dispatches an unanswered request")
    void leadershipLossReDispatches() {
        work.onRequest(1L);
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
        work.onRequest(1L);
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
        work.onRequest(1L);
        work.onRequest(2L);
        work.onRequest(3L);

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
    @DisplayName("a re-asserted request keeps its place")
    void reassertedRequestKeepsItsPlace() {
        work.onRequest(3L);
        work.onRequest(5L);
        work.onRequest(9L);
        work.onRequest(3L);

        final Recorder recorder = new Recorder();
        work.dispatchUndispatched(recorder);

        assertEquals(List.of(3L, 5L, 9L), recorder.dispatched);
    }

    @Test
    @DisplayName("a request answered and asked again is refused, as a clusterSeqNo never comes back")
    void answeredRequestAskedAgainIsRefused() {
        work.onRequest(1L);
        work.onRequest(2L);
        work.onReply(1L);

        assertThrows(IllegalArgumentException.class, () -> work.onRequest(1L));
        assertEquals(1, work.size());
    }

    @Test
    @DisplayName("an answered request's work is reused by a later one")
    void answeredWorkIsReused() {
        final int[] first = work.onRequest(1L);
        work.onReply(1L);

        assertSame(first, work.onRequest(2L));
    }

    @Test
    @DisplayName("request order and each request's work survive the slots growing and compacting")
    void orderSurvivesGrowthAndCompaction() {
        final List<Long> expected = new ArrayList<>();
        for (long key = 1; key <= 100; ++key) {
            work.onRequest(key)[0] = (int)key;
            if (key <= 40 && key % 2 == 1) {
                work.onReply(key);
            } else {
                expected.add(key);
            }
        }

        final List<Long> dispatched = new ArrayList<>();
        work.dispatchUndispatched((key, request) -> {
            assertEquals(key, request[0]);
            return dispatched.add(key);
        });

        assertEquals(expected, dispatched);
        assertEquals(expected.size(), work.size());
    }

    @Test
    @DisplayName("a warm tracker allocates nothing per request")
    void warmTrackerAllocatesNothing() {
        final OutstandingWork.Dispatcher<int[]> taken = (key, request) -> true;
        // As SequencerTest's: the first batch C2 has compiled whole is the one that counts.
        final ThreadMXBean threads = (ThreadMXBean)ManagementFactory.getThreadMXBean();
        final int requests = 100_000;
        long key = 0;
        long allocated = -1;
        for (int batch = 0; batch < 50 && allocated != 0; ++batch) {
            final long before = threads.getCurrentThreadAllocatedBytes();
            for (int i = 0; i < requests; ++i) {
                work.onRequest(++key)[0] = i;
                work.dispatchUndispatched(taken);
                work.onReply(key - 1);
            }
            allocated = threads.getCurrentThreadAllocatedBytes() - before;
        }

        assertEquals(1, work.size());
        assertEquals(0, allocated);
    }
}
