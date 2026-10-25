package org.limitless.seqeron.replayer.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.function.LongConsumer;
import java.util.function.LongUnaryOperator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.limitless.seqeron.protocol.ReplayProtocol;

/**
 * Unit tests for the pure admission-control/pending-request bookkeeping behind {@link
 * ReplayerService}'s {@code MAX_CONCURRENT_REPLAYS} cap. No Aeron/Archive runtime involved — {@link
 * ReplaySlotAllocator} never interprets the {@code token} it is handed.
 */
class ReplaySlotAllocatorTest {
    private static final long NO_CUT = ReplayProtocol.NO_CUT;
    private static final long STALL_MS = 10_000;
    private static final long REQUEST_ID = 1;
    private static final long CATCH_UP = 4096;
    private static final LongConsumer NO_STALL = token -> {
        throw new AssertionError("replay " + token + " was taken for stalled");
    };

    @Test
    @DisplayName("capacity holds until the max is reached, then refuses")
    void capacityHoldsUntilMaxThenRefuses() {
        final ReplaySlotAllocator allocator = new ReplaySlotAllocator(2);

        assertTrue(allocator.hasCapacity());
        allocator.activate(1, REQUEST_ID, 100, CATCH_UP);
        assertTrue(allocator.hasCapacity());
        allocator.activate(2, REQUEST_ID, 200, CATCH_UP);
        assertFalse(allocator.hasCapacity());
        assertEquals(2, allocator.activeCount());
    }

    @Test
    @DisplayName("a lowered limit admits nothing new until active slots end, and stops none of them")
    void aLoweredLimitAdmitsNothingNewAndStopsNone() {
        final ReplaySlotAllocator allocator = new ReplaySlotAllocator(4);
        allocator.activate(1, REQUEST_ID, 100, CATCH_UP);
        allocator.activate(2, REQUEST_ID, 200, CATCH_UP);

        allocator.limit(1);

        assertFalse(allocator.hasCapacity());
        assertEquals(2, allocator.activeCount());
        allocator.supersede(1);
        assertFalse(allocator.hasCapacity());
        allocator.supersede(2);
        assertTrue(allocator.hasCapacity());

        allocator.activate(3, REQUEST_ID, 300, CATCH_UP);
        allocator.limit(4);
        assertTrue(allocator.hasCapacity());
    }

    @Test
    @DisplayName("superseding a client's active slot frees capacity and returns its token")
    void supersedeFreesCapacityAndReturnsToken() {
        final ReplaySlotAllocator allocator = new ReplaySlotAllocator(1);
        allocator.activate(1, REQUEST_ID, 100, CATCH_UP);

        final long token = allocator.supersede(1);

        assertEquals(100, token);
        assertEquals(0, allocator.activeCount());
        assertTrue(allocator.hasCapacity());
    }

    @Test
    @DisplayName("superseding a client with no active slot is a no-op")
    void supersedingUnknownClientIsNoOp() {
        final ReplaySlotAllocator allocator = new ReplaySlotAllocator(1);
        allocator.activate(1, REQUEST_ID, 100, CATCH_UP);

        assertEquals(ReplaySlotAllocator.NO_SLOT, allocator.supersede(99));
        assertEquals(1, allocator.activeCount(), "the unrelated client's slot must survive");
    }

    @Test
    @DisplayName("pollPending refuses while every slot is taken, even with requests pending")
    void pollPendingRefusesAtCapacity() {
        final ReplaySlotAllocator allocator = new ReplaySlotAllocator(1);
        allocator.activate(1, REQUEST_ID, 100, CATCH_UP);
        allocator.addPending(2, /*requestId=*/1, 500, 1, NO_CUT);

        assertNull(allocator.pollPending());
        assertEquals(1, allocator.pendingCount(), "a refused poll must not consume the request");
    }

    @Test
    @DisplayName("pollPending returns pending requests in FIFO order once capacity frees")
    void pollPendingReturnsRequestsInFifoOrderOnceCapacityFrees() {
        final ReplaySlotAllocator allocator = new ReplaySlotAllocator(1);
        allocator.activate(1, REQUEST_ID, 100, CATCH_UP);
        allocator.addPending(2, /*requestId=*/77, 500, /*queueId=*/2, NO_CUT);
        allocator.addPending(3, /*requestId=*/78, 0, 1, NO_CUT);

        allocator.supersede(1); // frees the one slot

        final ReplaySlotAllocator.PendingRequest first = allocator.pollPending();
        assertEquals(2, first.clientId());
        assertEquals(500, first.fromPosition());
        assertEquals(2, first.queueId());
        // The reply sent when this is finally served must name the request that was held, not the
        // client's latest — the client discards a reply carrying any other requestId.
        assertEquals(77, first.requestId());
        assertEquals(1, allocator.pendingCount());

        allocator.activate(first.clientId(), REQUEST_ID, 999, CATCH_UP); // caller starts serving it, consuming the slot

        assertNull(allocator.pollPending(), "no capacity left for the next pending request");
        assertEquals(1, allocator.pendingCount());

        allocator.supersede(first.clientId()); // that replay finished/superseded, freeing the slot again
        final ReplaySlotAllocator.PendingRequest second = allocator.pollPending();
        assertEquals(3, second.clientId());
        assertEquals(0, allocator.pendingCount());
    }

    @Test
    @DisplayName("pollPending does not itself consume capacity — only activate() does")
    void pollPendingDoesNotConsumeCapacityByItself() {
        final ReplaySlotAllocator allocator = new ReplaySlotAllocator(1);
        allocator.addPending(1, /*requestId=*/1, 0, 1, NO_CUT);
        allocator.addPending(2, /*requestId=*/1, 0, 1, NO_CUT);

        final ReplaySlotAllocator.PendingRequest first = allocator.pollPending();
        assertEquals(1, first.clientId());

        // Nothing was activated for `first` yet, so capacity is still free — the next poll succeeds
        // too. This mirrors ReplayerService's drainPending loop, which rechecks capacity after every
        // startReplayForClient call rather than assuming the popped request consumed a slot.
        final ReplaySlotAllocator.PendingRequest second = allocator.pollPending();
        assertEquals(2, second.clientId());
    }

    @Test
    @DisplayName("a client's re-request replaces its pending entry rather than adding another")
    void aResendReplacesTheClientsPendingEntry() {
        final ReplaySlotAllocator allocator = new ReplaySlotAllocator(1);
        allocator.activate(1, REQUEST_ID, 100, CATCH_UP);

        // A pending client resends every ~500ms until it is served. Appending each one grew the list for
        // the whole of a node's cold start (three co-located apps against a cap of two, so one is always
        // pending) and left entries that start replays the client has long moved past.
        allocator.addPending(2, /*requestId=*/1, 0, 1, NO_CUT);
        allocator.addPending(2, /*requestId=*/2, 0, 1, NO_CUT);
        allocator.addPending(2, /*requestId=*/3, 0, 1, NO_CUT);

        assertEquals(1, allocator.pendingCount(), "one entry per client, however often it resends");

        allocator.supersede(1);
        assertEquals(3, allocator.pollPending().requestId(), "the client's current request, not its oldest");
    }

    @Test
    @DisplayName("a resent request keeps its place in line")
    void aResendKeepsTheClientsPlaceInLine() {
        final ReplaySlotAllocator allocator = new ReplaySlotAllocator(1);
        allocator.activate(1, REQUEST_ID, 100, CATCH_UP);
        allocator.addPending(2, /*requestId=*/1, 0, 1, NO_CUT);
        allocator.addPending(3, /*requestId=*/1, 0, 1, NO_CUT);

        allocator.addPending(2, /*requestId=*/2, 0, 1, NO_CUT); // client 2's resend while it waits

        // Sending it to the back on every resend would starve a client that resends on a timer.
        allocator.supersede(1);
        assertEquals(2, allocator.pollPending().clientId());
    }

    @Test
    @DisplayName("cancelPending drops only the named client's pending request")
    void cancelPendingDropsOnlyThatClientsPendingRequest() {
        final ReplaySlotAllocator allocator = new ReplaySlotAllocator(2);
        allocator.addPending(2, /*requestId=*/1, 0, 1, NO_CUT);
        allocator.addPending(3, /*requestId=*/1, 0, 1, NO_CUT);

        // Client 2 re-requested, found capacity, and is being served directly — its pending entry is now
        // stale. Left behind it would be drained into a second replay for a client that never asked.
        allocator.cancelPending(2);

        assertEquals(1, allocator.pendingCount());
        assertEquals(3, allocator.pollPending().clientId(), "an unrelated client's request must survive");
    }

    @Test
    @DisplayName("cancelPending for a client with nothing pending is a no-op")
    void cancelPendingForUnknownClientIsNoOp() {
        final ReplaySlotAllocator allocator = new ReplaySlotAllocator(2);
        allocator.addPending(2, /*requestId=*/1, 0, 1, NO_CUT);

        allocator.cancelPending(99);

        assertEquals(1, allocator.pendingCount());
    }

    @Test
    @DisplayName("pollPending with nothing pending returns null")
    void pollPendingWithNothingPendingReturnsNull() {
        final ReplaySlotAllocator allocator = new ReplaySlotAllocator(2);

        assertNull(allocator.pollPending());
    }

    @Test
    @DisplayName("reclaim removes only the slots whose replay has ended")
    void reclaimRemovesOnlyEndedReplays() {
        final ReplaySlotAllocator allocator = new ReplaySlotAllocator(3);
        allocator.activate(1, REQUEST_ID, 100, CATCH_UP);
        allocator.activate(2, REQUEST_ID, 200, CATCH_UP);
        allocator.activate(3, REQUEST_ID, 300, CATCH_UP);

        assertEquals(2, allocator.reclaim(token -> token == 200 ? 0 : Replayer.REPLAY_ENDED, 0, STALL_MS, NO_STALL));

        assertEquals(1, allocator.activeCount());
        assertEquals(ReplaySlotAllocator.NO_SLOT, allocator.supersede(1));
        assertEquals(200, allocator.supersede(2));
    }

    @Test
    @DisplayName("reclaim removes a replay that made no progress for the stall timeout, and names it")
    void reclaimRemovesAStalledReplay() {
        final ReplaySlotAllocator allocator = new ReplaySlotAllocator(2);
        allocator.activate(1, REQUEST_ID, 100, CATCH_UP);
        allocator.activate(2, REQUEST_ID, 200, CATCH_UP);
        final long[] progress = { 0, 0 };
        final LongUnaryOperator progressOf = token -> progress[token == 100 ? 0 : 1];
        final List<Long> stalled = new ArrayList<>();

        assertEquals(0, allocator.reclaim(progressOf, 0, STALL_MS, stalled::add));
        progress[1] = 4096; // 200's subscriber reads; 100's does not
        assertEquals(0, allocator.reclaim(progressOf, STALL_MS - 1, STALL_MS, stalled::add));
        assertEquals(1, allocator.reclaim(progressOf, STALL_MS, STALL_MS, stalled::add));

        assertEquals(List.of(100L), stalled);
        assertEquals(ReplaySlotAllocator.NO_SLOT, allocator.supersede(1));
        assertEquals(200, allocator.supersede(2));
    }

    @Test
    @DisplayName("progress restarts a replay's stall timeout")
    void progressRestartsTheStallTimeout() {
        final ReplaySlotAllocator allocator = new ReplaySlotAllocator(1);
        allocator.activate(1, REQUEST_ID, 100, CATCH_UP);
        final long[] progress = { 0 };

        allocator.reclaim(token -> progress[0], 0, STALL_MS, NO_STALL);
        progress[0] = 4096;
        allocator.reclaim(token -> progress[0], STALL_MS - 1, STALL_MS, NO_STALL);

        assertEquals(0, allocator.reclaim(token -> progress[0], 2 * STALL_MS - 2, STALL_MS, NO_STALL));
        assertEquals(1, allocator.activeCount());
    }

    @Test
    @DisplayName("clear removes and returns every active slot's token")
    void clearRemovesAndReturnsEveryToken() {
        final ReplaySlotAllocator allocator = new ReplaySlotAllocator(3);
        allocator.activate(1, REQUEST_ID, 100, CATCH_UP);
        allocator.activate(2, REQUEST_ID, 200, CATCH_UP);

        final List<Long> tokens = allocator.clear();

        assertEquals(List.of(100L, 200L), tokens);
        assertEquals(0, allocator.activeCount());
    }

    @Test
    @DisplayName("a resend of the request an active replay answers finds it, and where it ends")
    void aResendFindsTheReplayAnsweringIt() {
        final ReplaySlotAllocator allocator = new ReplaySlotAllocator(2);
        allocator.activate(1, 7, 100, 8192);
        allocator.activate(2, 7, 200, CATCH_UP);

        assertEquals(100, allocator.answering(1, 7));
        assertEquals(8192, allocator.catchUpPosition(100));
        assertEquals(ReplaySlotAllocator.NO_SLOT, allocator.answering(1, 8), "a new request is not a resend");
        assertEquals(ReplaySlotAllocator.NO_SLOT, allocator.answering(3, 7));
    }

    @Test
    @DisplayName("a replay superseded or reclaimed answers no resend")
    void aReplayGoneAnswersNoResend() {
        final ReplaySlotAllocator allocator = new ReplaySlotAllocator(2);
        allocator.activate(1, 7, 100, CATCH_UP);
        allocator.activate(2, 7, 200, CATCH_UP);

        allocator.supersede(1);
        allocator.reclaim(token -> Replayer.REPLAY_ENDED, 0, STALL_MS, NO_STALL);

        assertEquals(ReplaySlotAllocator.NO_SLOT, allocator.answering(1, 7));
        assertEquals(ReplaySlotAllocator.NO_SLOT, allocator.answering(2, 7));
    }
}
