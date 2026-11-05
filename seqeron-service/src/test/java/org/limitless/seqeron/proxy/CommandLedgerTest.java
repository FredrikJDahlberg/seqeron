package org.limitless.seqeron.proxy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.limitless.seqeron.protocol.Publish;

class CommandLedgerTest {
    private static final long WINDOW = 1_000;

    private static final long NO_SEQ = CommandLedger.NO_SEQ;

    private final List<String> reported = new ArrayList<>();
    private final CommandLedger ledger =
        new CommandLedger(WINDOW, (first, last, reason) -> reported.add(first + "-" + last + " " + reason));

    @BeforeEach
    void leading() {
        ledger.reset();
        ledger.edge(true);
    }

    @Test
    void aWindowedCommandThatFitsIsCountedUntilItLeaves() {
        assertFalse(ledger.exceedsWindow(600, true));
        ledger.enqueued(600, true);
        assertEquals(600, ledger.queuedBytes());
        assertTrue(ledger.settle(Publish.Published, 600, true, NO_SEQ));
        assertEquals(0, ledger.queuedBytes());
        assertEquals(600, ledger.retired());
        assertEquals(1, ledger.placed());
    }

    @Test
    void theWindowIsInclusiveAndCountsTheEncodedLength() {
        ledger.enqueued(600, true);
        assertFalse(ledger.exceedsWindow(400, true));
        assertTrue(ledger.exceedsWindow(401, true));
    }

    @Test
    void aCloseIsOutsideTheWindowAndTheRetiredCounter() {
        ledger.enqueued(900, true);
        assertFalse(ledger.exceedsWindow(500, false));
        ledger.enqueued(500, false);
        assertEquals(900, ledger.queuedBytes());
        assertTrue(ledger.settle(Publish.Published, 500, false, NO_SEQ));
        assertEquals(900, ledger.queuedBytes());
        assertEquals(0, ledger.retired());
    }

    @Test
    void thePeakIsTheHighestQueuedBytes() {
        ledger.enqueued(300, true);
        ledger.enqueued(500, true);
        ledger.settle(Publish.Published, 300, true, NO_SEQ);
        ledger.settle(Publish.Published, 500, true, NO_SEQ);
        assertEquals(800, ledger.peakQueuedBytes());
    }

    @Test
    void aCommandOfAnotherEpochIsDroppedAndRetired() {
        ledger.enqueued(100, true);
        assertTrue(ledger.stale(ledger.epoch() - 1));
        ledger.dropStale(100, true, NO_SEQ);
        assertEquals(1, ledger.epochDrops());
        assertEquals(100, ledger.retired());
        assertEquals(0, ledger.queuedBytes());
        assertEquals(0, ledger.placed());
    }

    @Test
    void aCommandOfTheCurrentEpochIsStaleOnceTheGateHasClosed() {
        final long decidedIn = ledger.epoch();
        assertFalse(ledger.stale(decidedIn));
        ledger.edge(false);
        assertTrue(ledger.stale(decidedIn));
        assertTrue(ledger.stale(ledger.epoch()));
        ledger.edge(true);
        assertTrue(ledger.stale(decidedIn));
        assertFalse(ledger.stale(ledger.epoch()));
    }

    @Test
    void everyEdgeRaisesTheEpoch() {
        final long first = ledger.epoch();
        ledger.edge(false);
        ledger.edge(true);
        ledger.edge(true);
        assertEquals(first + 3, ledger.epoch());
    }

    @Test
    void aDeclinedHeadStaysQueuedAndIsNotRetired() {
        ledger.enqueued(100, true);
        assertFalse(ledger.settle(Publish.Declined, 100, true, NO_SEQ));
        assertEquals(1, ledger.headDeclines());
        assertEquals(100, ledger.queuedBytes());
        assertEquals(0, ledger.retired());
        assertTrue(ledger.settle(Publish.Published, 100, true, NO_SEQ));
        assertEquals(0, ledger.queuedBytes());
        assertEquals(100, ledger.retired());
    }

    @Test
    void aRefusedHeadLeavesTheQueueAndIsCounted() {
        ledger.enqueued(100, true);
        assertTrue(ledger.settle(Publish.Refused, 100, true, NO_SEQ));
        assertEquals(1, ledger.refusedDrops());
        assertEquals(0, ledger.placed());
        assertEquals(100, ledger.retired());
    }

    @Test
    void discardingTheQueueClearsTheBytesButNotTheRetiredTotal() {
        ledger.enqueued(400, true);
        ledger.enqueued(100, true);
        ledger.settle(Publish.Published, 400, true, NO_SEQ);
        ledger.discarded();
        assertEquals(0, ledger.queuedBytes());
        assertEquals(400, ledger.retired());
    }

    @Test
    void aNewAttachmentStartsFromNothing() {
        ledger.enqueued(400, true);
        ledger.settle(Publish.Declined, 400, true, NO_SEQ);
        ledger.dropStale(0, true, NO_SEQ);
        ledger.reset();
        assertEquals(0, ledger.epoch());
        assertFalse(ledger.leading());
        assertEquals(0, ledger.queuedBytes());
        assertEquals(0, ledger.peakQueuedBytes());
        assertEquals(0, ledger.retired());
        assertEquals(0, ledger.headDeclines());
        assertEquals(0, ledger.epochDrops());
    }

    @Test
    void aRunOfStalePublishesIsReportedOnceWhenFlushed() {
        for (long seq = 1; seq <= 3; seq++) {
            ledger.enqueued(10, true);
            ledger.dropStale(10, true, seq);
        }
        assertEquals(List.of(), reported);
        assertEquals(0, ledger.resolved());
        ledger.flush();
        assertEquals(List.of("1-3 EPOCH"), reported);
        assertEquals(3, ledger.resolved());
    }

    @Test
    void aPlacedPublishClosesTheRunAndResolvesPastIt() {
        ledger.dropStale(10, true, 1);
        ledger.dropStale(10, true, 2);
        assertTrue(ledger.settle(Publish.Published, 10, true, 3));
        assertEquals(List.of("1-2 EPOCH"), reported);
        assertEquals(3, ledger.resolved());
    }

    @Test
    void aDifferentReasonStartsANewRun() {
        ledger.dropStale(10, true, 1);
        assertTrue(ledger.settle(Publish.Refused, 10, true, 2));
        ledger.dropStale(10, true, 3);
        ledger.flush();
        assertEquals(List.of("1-1 EPOCH", "2-2 REFUSED", "3-3 EPOCH"), reported);
    }

    @Test
    void resolvedNeverPassesAnUnreportedDrop() {
        assertTrue(ledger.settle(Publish.Published, 10, true, 1));
        ledger.dropStale(10, true, 2);
        assertEquals(1, ledger.resolved());
        ledger.flush();
        assertEquals(2, ledger.resolved());
    }

    @Test
    void aDeclinedPublishIsNotResolved() {
        assertFalse(ledger.settle(Publish.Declined, 10, true, 1));
        ledger.flush();
        assertEquals(0, ledger.resolved());
        assertEquals(List.of(), reported);
    }

    @Test
    void aCommandWithoutASequenceNumberLeavesTheRunAlone() {
        ledger.dropStale(10, true, 1);
        ledger.dropStale(10, true, NO_SEQ);
        assertTrue(ledger.settle(Publish.Published, 10, true, NO_SEQ));
        ledger.dropStale(10, true, 2);
        ledger.flush();
        assertEquals(List.of("1-2 EPOCH"), reported);
    }

    @Test
    void aNewAttachmentForgetsTheRunInProgress() {
        ledger.dropStale(10, true, 1);
        ledger.reset();
        ledger.flush();
        assertEquals(List.of(), reported);
        assertEquals(0, ledger.resolved());
    }
}
