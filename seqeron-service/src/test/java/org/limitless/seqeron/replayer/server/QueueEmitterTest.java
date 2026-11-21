package org.limitless.seqeron.replayer.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.Publication;
import java.util.ArrayList;
import java.util.List;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.limitless.seqeron.protocol.FrameLayer;
import org.limitless.seqeron.sequencer.QueuePositions;

/**
 * The queue agent's decisions: a frame a queue does not take is kept with every frame after it, and a frame that lands
 * must end where the log puts it. {@link QueueEmitter} holds no Aeron runtime.
 */
class QueueEmitterTest {
    private static final int TERM_LENGTH = 64 * 1024;
    private static final int MTU = 1408;

    /** A queue per group that lands each frame where the log puts it, unless told otherwise. */
    private static final class FakeQueues implements QueueEmitter.Queues {
        final long[] positions = new long[FrameLayer.MAX_GROUPS];
        final List<String> landed = new ArrayList<>();
        long refuseWith;
        long skew;

        @Override
        public long offer(final int groupId, final DirectBuffer buffer, final int length) {
            if (refuseWith != 0) {
                return refuseWith;
            }
            final long start = QueuePositions.frameStart(positions[groupId - 1], length, TERM_LENGTH, MTU);
            positions[groupId - 1] = start + QueuePositions.framedLength(length, MTU) + skew;
            landed.add(groupId + ":" + buffer.getByte(0));
            return positions[groupId - 1];
        }

        @Override
        public boolean isUnrecoverable(final long offerResult) {
            return offerResult == Publication.CLOSED || offerResult == Publication.MAX_POSITION_EXCEEDED;
        }
    }

    private final FakeQueues queues = new FakeQueues();
    private final QueueEmitter emitter = new QueueEmitter(queues, new long[FrameLayer.MAX_GROUPS], TERM_LENGTH, MTU);

    private void publish(final int groupId, final int tag, final int length) {
        final UnsafeBuffer frame = new UnsafeBuffer(new byte[length]);
        frame.putByte(0, (byte)tag);
        emitter.publish(groupId, frame, length);
    }

    @Test
    @DisplayName("a frame a queue does not take is kept, and every frame after it, on any queue, waits behind it")
    void keepsTheRefusedFrameAndEveryFrameAfterIt() {
        publish(1, 1, 100);
        queues.refuseWith = Publication.BACK_PRESSURED;
        publish(1, 2, 100);
        assertTrue(emitter.isHolding());
        queues.refuseWith = 0;
        publish(2, 3, 100);
        publish(1, 4, 100);
        assertEquals(List.of("1:1"), queues.landed, "nothing is offered past a kept frame");

        assertEquals(3, emitter.flush());
        assertFalse(emitter.isHolding());
        assertEquals(List.of("1:1", "1:2", "2:3", "1:4"), queues.landed);
        assertNull(emitter.failure());
    }

    @Test
    @DisplayName("a flush stops at the first frame a queue still does not take")
    void aFlushStopsAtTheFirstRefusal() {
        queues.refuseWith = Publication.NOT_CONNECTED;
        publish(1, 1, 100);
        publish(1, 2, 100);
        assertEquals(0, emitter.flush());
        assertTrue(emitter.isHolding());
        queues.refuseWith = 0;
        assertEquals(2, emitter.flush());
        assertEquals(List.of("1:1", "1:2"), queues.landed);
    }

    @Test
    @DisplayName("positions follow the log's arithmetic across a fragmented frame and a term's end")
    void positionsFollowTheLog() {
        for (int i = 0; i < 60; i++) {
            publish(1, i, 3000); // three fragments each, and a term ends partway
        }
        assertNull(emitter.failure());
        assertEquals(queues.positions[0], emitter.position(1));
        assertTrue(emitter.position(1) > TERM_LENGTH);
    }

    @Test
    @DisplayName("a frame that lands anywhere but where the log puts it fails the emitter")
    void aMisplacedFrameFails() {
        publish(1, 1, 100);
        queues.skew = 32;
        publish(1, 2, 100);
        assertNotNull(emitter.failure());
        publish(1, 3, 100);
        assertEquals(List.of("1:1", "1:2"), queues.landed, "and it offers nothing more");
    }

    @Test
    @DisplayName("a queue that refuses a frame for good fails the emitter")
    void anUnrecoverableRefusalFails() {
        queues.refuseWith = Publication.CLOSED;
        publish(1, 1, 100);
        assertNotNull(emitter.failure());
    }

    @Test
    @DisplayName("each queue starts where it is told to")
    void startsWhereTold() {
        final long[] starts = new long[FrameLayer.MAX_GROUPS];
        starts[0] = 4096;
        queues.positions[0] = 4096;
        final QueueEmitter restored = new QueueEmitter(queues, starts, TERM_LENGTH, MTU);
        final UnsafeBuffer frame = new UnsafeBuffer(new byte[100]);
        restored.publish(1, frame, 100);
        assertNull(restored.failure());
        assertEquals(4096 + QueuePositions.framedLength(100, MTU), restored.position(1));
    }
}
