package org.limitless.seqeron.app;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.limitless.seqeron.protocol.SystemFrame;
import org.limitless.seqeron.replayer.client.SequencedEvent;
import org.limitless.seqeron.replayer.client.SequencedEvents;

/**
 * A consumer on a queue under a snapshot policy, which only an {@link Application} can follow. No Aeron runtime: the
 * events are encoded queue frames handed to the consumer's dispatch, and the queue that starts at a cut is
 * {@code ReplayerRecoveryTest}'s.
 */
class ConsumerTest {
    private static final int PAYLOAD_ID = 5;

    private final List<Integer> dispatched = new ArrayList<>();
    private final List<ClusterError> fences = new ArrayList<>();
    private final Consumer.Listener listener = new Consumer.Listener() {
        @Override
        public void onSequenced(final Payload payload) {
            dispatched.add(payload.payloadId());
        }

        @Override
        public void onFenced(final ClusterError fence, final String detail) {
            fences.add(fence);
        }
    };
    private final Consumer consumer = Consumer.builder().clientId(1).memberId(2).groupId(1).listener(listener).build();

    private static SequencedEvent payload() {
        return SequencedEvents.of(1, false, PAYLOAD_ID, new UnsafeBuffer(new byte[Long.BYTES]), 0, Long.BYTES);
    }

    private static SequencedEvent system(final int systemEventType) {
        return SequencedEvents.of(1, true, systemEventType, new UnsafeBuffer(new byte[0]), 0, 0);
    }

    @Test
    @DisplayName("a snapshot policy's row fences with SNAPSHOTTED_QUEUE, and nothing after it is dispatched")
    void aSnapshotPolicyFences() {
        consumer.onSequenced(payload());
        consumer.onSequenced(system(SystemFrame.SNAPSHOT_POLICY_REGISTERED));
        consumer.onSequenced(payload());
        consumer.checkSnapshotted();

        assertEquals(List.of(PAYLOAD_ID), dispatched);
        assertEquals(List.of(ClusterError.SNAPSHOTTED_QUEUE), fences);
    }

    @Test
    @DisplayName("a round's SnapshotStarted fences too")
    void aSnapshotRoundFences() {
        consumer.onSequenced(system(SystemFrame.SNAPSHOT_STARTED));
        consumer.checkSnapshotted();

        assertEquals(List.of(ClusterError.SNAPSHOTTED_QUEUE), fences);
    }

    @Test
    @DisplayName("a queue with no snapshot frame is followed")
    void aQueueWithoutSnapshotsIsFollowed() {
        consumer.onSequenced(system(SystemFrame.CLUSTER_HEARTBEAT));
        consumer.onSequenced(payload());
        consumer.checkSnapshotted();

        assertEquals(List.of(PAYLOAD_ID), dispatched);
        assertEquals(List.of(), fences);
    }
}
