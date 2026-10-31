package org.limitless.seqeron.proxy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.agrona.BufferUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LivenessTest {
    private static final long TIMEOUT_NS = 2_000_000_000L;
    private static final long MS = 1_000_000L;

    private CountersManager counters;
    private AtomicCounter counter;
    private Liveness liveness;

    @BeforeEach
    void watched() {
        counters = new CountersManager(new UnsafeBuffer(BufferUtil.allocateDirectAligned(16 * 1024, 64)),
                                       new UnsafeBuffer(BufferUtil.allocateDirectAligned(4096, 64)));
        counter = counters.newCounter("heartbeat", 5300);
        counters.setCounterRegistrationId(counter.id(), 42);
        liveness = new Liveness(counters, TIMEOUT_NS);
        liveness.watch(counter.id(), 42, 0);
    }

    @Test
    void aCounterThatChangesIsAlive() {
        counter.setOrdered(1);
        assertFalse(liveness.stale(1_900 * MS));
        counter.setOrdered(2);
        assertFalse(liveness.stale(3_800 * MS));
    }

    @Test
    void aCounterThatStaysIsStaleOnceTheTimeoutHasPassed() {
        assertFalse(liveness.stale(2_000 * MS));
        assertTrue(liveness.stale(2_001 * MS));
        assertEquals(2_001, liveness.silentMs(2_001 * MS));
    }

    @Test
    void aFreedCounterIsStaleAtOnce() {
        counters.free(counter.id());
        assertTrue(liveness.stale(1));
    }

    @Test
    void aCounterReallocatedUnderTheSameIdIsStale() {
        counters.setCounterRegistrationId(counter.id(), 43);
        assertTrue(liveness.stale(1));
    }
}
