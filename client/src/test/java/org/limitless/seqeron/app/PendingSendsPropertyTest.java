package org.limitless.seqeron.app;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.LongStream;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.limitless.seqeron.protocol.FrameLayer;
import org.limitless.seqeron.protocol.SystemFrame;
import org.limitless.seqeron.replayer.client.SequencedEvents;
import org.limitless.seqeron.sequencer.client.IngressSender;
import org.limitless.seqeron.helpers.SplitMix64;

/**
 * {@link PendingSends} driving a producer against a model cluster: a leader that appends ingress stamped with
 * its own term and drops the rest, commits a prefix, and loses everything uncommitted at an election. After
 * one, the old leader either still takes ingress and drops it, or is gone and a send spins until egress brings
 * the {@code NewLeader} — giving the send up if the hold is on, as {@code ClusterStreamSender} does. The
 * sender may reconnect on a new session then. The tap lags, a sibling shares the producer's {@code sourceId},
 * and term ids skip, as after a failed ballot.
 *
 * <p>Checked once the faults stop and everything has drained: the log holds every frame the producer sent
 * exactly once, in send order, and nothing faulted. Seeds are fixed; the failing one is in the test name.
 * Case for case with {@code PendingSendsPropertyTest.cpp}.
 */
class PendingSendsPropertyTest {
    private static final int CAPACITY = 64;
    private static final int STEPS = 5_000;
    private static final int QUIESCE_ROUNDS = 100;
    private static final long SIBLING = 1_000_000;
    private static final long LEADERSHIP_CHANGED = -1;
    private static final int PAYLOAD_ID = 6;

    /** One tap entry: frame {@code value} from {@code session}, or a term's LeadershipChanged. */
    private record Entry(long session, long value) { }

    private final PendingSends pending = new PendingSends(CAPACITY);
    private final SystemFrame envelope = new SystemFrame();
    private final ExpandableArrayBuffer frame = new ExpandableArrayBuffer(FrameLayer.MAX_INGRESS_LENGTH);
    private final UnsafeBuffer body = new UnsafeBuffer(new byte[Long.BYTES]);
    private final ModelSender sender = new ModelSender();
    /** Accepted by the current leader, not yet committed. */
    private final ArrayDeque<Entry> uncommitted = new ArrayDeque<>();
    /** Committed, not yet delivered to the producer's tap. */
    private final ArrayDeque<Entry> tap = new ArrayDeque<>();
    /** The producer's frames in log order. */
    private final List<Long> log = new ArrayList<>();
    private SplitMix64 rng;
    private long clusterTerm = 0;
    private long senderTerm = 0;
    private long session = 1;
    private boolean oldLeaderTakesIngress;
    private long next = 0;
    private int resent = 0;

    /** The cluster as a sender sees it. */
    private final class ModelSender implements IngressSender {
        @Override
        public boolean send(final DirectBuffer frame, final int length) {
            if (senderTerm != clusterTerm && !oldLeaderTakesIngress) {
                newLeader(); // from inside the spin, as pollEgress would
                if (pending.isHolding()) {
                    return false;
                }
            }
            if (senderTerm == clusterTerm) {
                uncommitted.add(new Entry(session, frame.getLong(FrameLayer.MIN_INGRESS_LENGTH)));
            }
            return true;
        }

        @Override
        public long clusterSessionId() {
            return session;
        }

        @Override
        public long leadershipTermId() {
            return senderTerm;
        }
    }

    @ParameterizedTest(name = "seed {0}")
    @ValueSource(longs = { 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610, 987, 1597 })
    void logsEveryFrameOnceInOrderAcrossFailovers(final long seed) {
        rng = new SplitMix64(seed);
        for (int step = 0; step < STEPS; step++) {
            final int action = rng.roll(100);
            if (action < 30) {
                send();
            } else if (action < 50) {
                commit(rng.roll(4));
            } else if (action < 55) {
                tap.add(new Entry(SIBLING, rng.roll(8)));
            } else if (action < 80) {
                deliver(rng.roll(4));
            } else if (action < 85) {
                resent += pending.resendMissing(sender);
            } else if (action < 87) {
                uncommitted.clear();
                clusterTerm += 1 + rng.roll(2);
                tap.add(new Entry(LEADERSHIP_CHANGED, clusterTerm));
                oldLeaderTakesIngress = rng.roll(2) == 0;
            } else if (senderTerm != clusterTerm) {
                newLeader();
            }
        }
        for (int round = 0; round < QUIESCE_ROUNDS && pending.size() > 0; round++) {
            if (senderTerm != clusterTerm) {
                newLeader();
            }
            resent += pending.resendMissing(sender);
            commit(Integer.MAX_VALUE);
            deliver(Integer.MAX_VALUE);
        }

        assertFalse(pending.isFaulted());
        assertEquals(0, pending.size(), "everything sent came back");
        assertTrue(resent > 0, "the run lost and resent something");
        assertEquals(LongStream.range(0, next).boxed().toList(), log);
    }

    private void send() {
        if (pending.isHolding() || pending.isFull()) {
            return;
        }
        body.putLong(0, next);
        final int length = envelope.wrapPayload(frame, 1, 2, session, PAYLOAD_ID, body, Long.BYTES);
        if (sender.send(frame, length)) {
            pending.track(frame, length, sender.clusterSessionId(), sender.leadershipTermId());
            next++;
        }
    }

    private void newLeader() {
        senderTerm = clusterTerm;
        if (rng.roll(2) == 0) {
            session++;
        }
        pending.onNewLeader(senderTerm);
    }

    private void commit(final int count) {
        for (int k = count; k > 0 && !uncommitted.isEmpty(); k--) {
            final Entry entry = uncommitted.poll();
            log.add(entry.value());
            tap.add(entry);
        }
    }

    private void deliver(final int count) {
        for (int k = count; k > 0 && !tap.isEmpty(); k--) {
            final Entry entry = tap.poll();
            if (entry.session() == LEADERSHIP_CHANGED) {
                pending.onLeadershipChanged(entry.value());
                continue;
            }
            body.putLong(0, entry.value());
            pending.onSequenced(SequencedEvents.of(entry.session(), false, PAYLOAD_ID, body, 0, Long.BYTES));
        }
    }
}
