package org.limitless.seqeron.app;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** A consumer's construction rules and its state before it starts. No Aeron is started. */
class ConsumerBuilderTest {
    private static final Consumer.Listener LISTENER = new Consumer.Listener() {
        @Override
        public void onSequenced(final Payload payload) {
        }

        @Override
        public void onFenced(final ClusterError fence, final String detail) {
        }
    };

    @Test
    @DisplayName("a client id, a group and a listener are required")
    void requiresClientIdGroupAndListener() {
        assertEquals("clientId", assertThrows(NullPointerException.class,
            () -> Consumer.builder().groupId(1).listener(LISTENER).build()).getMessage());
        assertEquals("groupId", assertThrows(NullPointerException.class,
            () -> Consumer.builder().clientId(1).listener(LISTENER).build()).getMessage());
        assertEquals("listener", assertThrows(NullPointerException.class,
            () -> Consumer.builder().clientId(1).groupId(1).build()).getMessage());
    }

    @Test
    @DisplayName("before it starts a consumer has seen nothing and does no work")
    void idleBeforeStart() {
        final Consumer consumer = Consumer.builder().clientId(1).memberId(2).groupId(1).listener(LISTENER)
            .tuning(Tuning.DEFAULT.withQueueStallTimeoutMs(2000)).build();
        assertFalse(consumer.isCaughtUp());
        assertEquals(0, consumer.lastClusterSeqNo());
    }
}
