package org.limitless.seqeron.proxy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.limitless.seqeron.protocol.Publish;

class CommandLedgerTest {
    private static final long WINDOW = 1_000;

    private final CommandLedger ledger = new CommandLedger(WINDOW);

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
        assertTrue(ledger.settle(Publish.Published, 600, true));
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
        assertTrue(ledger.settle(Publish.Published, 500, false));
        assertEquals(900, ledger.queuedBytes());
        assertEquals(0, ledger.retired());
    }

    @Test
    void thePeakIsTheHighestQueuedBytes() {
        ledger.enqueued(300, true);
        ledger.enqueued(500, true);
        ledger.settle(Publish.Published, 300, true);
        ledger.settle(Publish.Published, 500, true);
        assertEquals(800, ledger.peakQueuedBytes());
    }

    @Test
    void aCommandOfAnotherEpochIsDroppedAndRetired() {
        ledger.enqueued(100, true);
        assertTrue(ledger.stale(ledger.epoch() - 1));
        ledger.dropStale(100, true);
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
        assertFalse(ledger.settle(Publish.Declined, 100, true));
        assertEquals(1, ledger.headDeclines());
        assertEquals(100, ledger.queuedBytes());
        assertEquals(0, ledger.retired());
        assertTrue(ledger.settle(Publish.Published, 100, true));
        assertEquals(0, ledger.queuedBytes());
        assertEquals(100, ledger.retired());
    }

    @Test
    void aRefusedHeadLeavesTheQueueAndIsCounted() {
        ledger.enqueued(100, true);
        assertTrue(ledger.settle(Publish.Refused, 100, true));
        assertEquals(1, ledger.refusedDrops());
        assertEquals(0, ledger.placed());
        assertEquals(100, ledger.retired());
    }

    @Test
    void discardingTheQueueClearsTheBytesButNotTheRetiredTotal() {
        ledger.enqueued(400, true);
        ledger.enqueued(100, true);
        ledger.settle(Publish.Published, 400, true);
        ledger.discarded();
        assertEquals(0, ledger.queuedBytes());
        assertEquals(400, ledger.retired());
    }

    @Test
    void aNewAttachmentStartsFromNothing() {
        ledger.enqueued(400, true);
        ledger.settle(Publish.Declined, 400, true);
        ledger.dropStale(0, true);
        ledger.reset();
        assertEquals(0, ledger.epoch());
        assertFalse(ledger.leading());
        assertEquals(0, ledger.queuedBytes());
        assertEquals(0, ledger.peakQueuedBytes());
        assertEquals(0, ledger.retired());
        assertEquals(0, ledger.headDeclines());
        assertEquals(0, ledger.epochDrops());
    }
}
