package org.limitless.seqeron.app;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.limitless.seqeron.sbe.replay.MessageHeaderEncoder;
import org.limitless.seqeron.sbe.replay.QueueResolvedEncoder;
import org.limitless.seqeron.sbe.replay.ReplayPendingEncoder;

/**
 * Which queue an instance reads (spec R-6), resolved against a scripted Replayer. Case for case with
 * {@code QueueResolutionTest.cpp} and {@code QueueResolutionTest.cs}, in the same order.
 */
class QueueResolutionTest {
    private static final long CLIENT_ID = 4;
    private static final long CLOCK_MS = 3 * 60 * 60 * 1000L;

    private final List<Long> sent = new ArrayList<>();
    private long clockMs = CLOCK_MS;

    private final QueueResolution resolution = new QueueResolution(new QueueResolution.Actions() {
        @Override
        public void sendQueueRequest(final long requestId) {
            sent.add(requestId);
        }

        @Override
        public long clientId() {
            return CLIENT_ID;
        }

        @Override
        public long nowMs() {
            return clockMs;
        }
    });

    @Test
    void nothingIsSentBeforeStart() {
        clockMs += 10_000;
        resolution.doTimers(false);

        assertEquals(List.of(), sent);
        assertEquals(QueueResolution.UNRESOLVED, resolution.groupId());
    }

    @Test
    void startSendsTheFirstRequest() {
        resolution.start();

        assertEquals(List.of(1L), sent);
        assertEquals(QueueResolution.UNRESOLVED, resolution.groupId());
    }

    @Test
    void theAnswerToTheCurrentRequestResolvesTheQueue() {
        resolution.start();
        resolved(CLIENT_ID, 1, 3);

        assertEquals(3, resolution.groupId());
    }

    @Test
    void anAnswerForAnotherClientOrAnEarlierRequestIsIgnored() {
        resolution.start();
        clockMs += QueueResolution.RESEND_INTERVAL_MS + 1;
        resolution.doTimers(false);

        resolved(CLIENT_ID + 1, 2, 3);
        resolved(CLIENT_ID, 1, 3);

        assertEquals(QueueResolution.UNRESOLVED, resolution.groupId());
    }

    @Test
    void anUnansweredRequestIsResentWithANewRequestId() {
        resolution.start();
        clockMs += QueueResolution.RESEND_INTERVAL_MS;
        resolution.doTimers(false);
        assertEquals(List.of(1L), sent, "not before the interval has passed");

        clockMs += 1;
        resolution.doTimers(false);

        assertEquals(List.of(1L, 2L), sent);
        assertEquals(2, resolution.requestId());
    }

    @Test
    void aPendingAnswerHoldsTheResendOff() {
        resolution.start();
        clockMs += QueueResolution.RESEND_INTERVAL_MS;
        pending(1); // the row is not on the log yet

        clockMs += 1;
        resolution.doTimers(false);

        assertEquals(List.of(1L), sent);
    }

    @Test
    void anUnconnectedPublicationIsRetriedEveryCycle() {
        resolution.start();
        resolution.doTimers(true);

        assertEquals(List.of(1L, 2L), sent);
    }

    @Test
    void nothingIsSentOnceResolved() {
        resolution.start();
        resolved(CLIENT_ID, 1, 3);
        clockMs += 10_000;
        resolution.doTimers(true);
        resolved(CLIENT_ID, 1, 5);

        assertEquals(List.of(1L), sent);
        assertEquals(3, resolution.groupId(), "a source's group never changes");
    }

    private void resolved(final long clientId, final long requestId, final int queueId) {
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[64]);
        new QueueResolvedEncoder()
            .wrapAndApplyHeader(buffer, 0, new MessageHeaderEncoder())
            .clientId(clientId)
            .requestId(requestId)
            .queueId(queueId);
        resolution.onControl(buffer, 0, MessageHeaderEncoder.ENCODED_LENGTH + QueueResolvedEncoder.BLOCK_LENGTH);
    }

    private void pending(final long requestId) {
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[64]);
        new ReplayPendingEncoder()
            .wrapAndApplyHeader(buffer, 0, new MessageHeaderEncoder())
            .clientId(CLIENT_ID)
            .requestId(requestId);
        resolution.onControl(buffer, 0, MessageHeaderEncoder.ENCODED_LENGTH + ReplayPendingEncoder.BLOCK_LENGTH);
    }
}
