package org.limitless.seqeron.app;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * {@link LeaderGate} and {@link OutstandingWork} on three simulated replicas sharing one log, with a seeded
 * generator interleaving requests, replica lag and batched polls, recovery, elections, failed reply offers
 * and replies lost in an election.
 *
 * <p><b>Safety</b>, checked on every step: a replica's outstanding set matches the log prefix it has
 * applied; only a caught-up replica that sees itself as leader dispatches; within one gate opening a
 * request is dispatched at most once unless its offer failed; a sweep dispatches in {@code globalSeqNo}
 * order; and a request this opening dispatched is never silently lost — its reply is pending, in flight or
 * in the log, or a leadership change this replica has yet to act on will close the gate.
 *
 * <p><b>Liveness</b>: once the faults stop, every request has a reply in the log and every replica's
 * outstanding set is empty. Replies may appear more than once; that is the at-least-once contract.
 *
 * <p>Seeds are fixed; the failing one is in the test name. Case for case with
 * {@code OutstandingWorkPropertyTest.cpp}.
 */
class OutstandingWorkPropertyTest {
    private static final int REPLICAS = 3;
    /** Long enough that a gate which ignores a flip away and back fails several of the seeds below. */
    private static final int CHAOS_STEPS = 10_000;
    private static final int QUIESCE_ROUNDS = 100;

    private enum Kind { REQUEST, REPLY, LEADER }

    private record Entry(Kind kind, long value) { }

    private final List<Entry> log = new ArrayList<>();
    /** The outstanding set after the whole log, and its size after each entry. */
    private final Set<Long> open = new HashSet<>();
    private final List<Integer> outstandingAfter = new ArrayList<>();
    /** Every key with a reply in the log, applied or not. */
    private final Set<Long> replied = new HashSet<>();
    /** Replies offered to the cluster and not yet in the log. */
    private final List<Long> inFlight = new ArrayList<>();
    private final List<Replica> replicas = new ArrayList<>();
    private Random rng;
    private boolean faults;
    private int leader;
    private int lastLeaderIndex;

    private final class Replica {
        final int memberId;
        final LeaderGate gate;
        final OutstandingWork<Long, Long> work = new OutstandingWork<>();
        /** Dispatched, reply not yet offered. */
        final ArrayDeque<Long> pendingReplies = new ArrayDeque<>();
        /** The model of what this gate opening has dispatched. */
        final Set<Long> tenure = new HashSet<>();
        int applied;
        int viewLeader = -1;
        boolean recovering;
        /** A LeadershipChanged applied since the last duty cycle. */
        boolean leadershipApplied;
        int slots;
        long lastInSweep;

        Replica(final int memberId) {
            this.memberId = memberId;
            this.gate = new LeaderGate(memberId);
        }

        void applyNext() {
            if (applied == log.size()) {
                return;
            }
            final Entry entry = log.get(applied++);
            switch (entry.kind()) {
            case REQUEST -> work.onRequest(entry.value(), entry.value());
            case REPLY -> {
                work.onReply(entry.value());
                tenure.remove(entry.value());
            }
            case LEADER -> {
                viewLeader = (int)entry.value();
                gate.onLeadershipChanged();
                leadershipApplied = true;
            }
            }
            assertEquals(outstandingAt(applied), work.size(), "member " + memberId + " at log index " + applied);
        }

        /** One poll that delivers everything behind this replica. */
        void applyAll() {
            while (applied < log.size()) {
                applyNext();
            }
        }

        void dutyCycle() {
            if (gate.update(!recovering, viewLeader) == LeaderGate.Transition.CLOSED) {
                work.onNotLeader();
                tenure.clear();
            }
            leadershipApplied = false;
            if (!gate.isOpen()) {
                return;
            }
            slots = faults ? rng.nextInt(3) : Integer.MAX_VALUE;
            lastInSweep = 0;
            work.dispatchUndispatched(this::dispatch);
        }

        boolean dispatch(final Long key, final Long request) {
            if (slots-- <= 0) {
                return false;
            }
            assertTrue(!recovering && viewLeader == memberId, "member " + memberId + " dispatched while not leader");
            assertTrue(key > lastInSweep, "member " + memberId + " dispatched " + key + " after " + lastInSweep);
            assertTrue(tenure.add(key), "member " + memberId + " dispatched " + key + " twice in one opening");
            assertEquals(key, request);
            lastInSweep = key;
            pendingReplies.add(key);
            return true;
        }

        void offer() {
            final Long key = pendingReplies.poll();
            if (key == null || !gate.isOpen()) {
                return; // a reply produced after the gate closed is dropped; the next leader re-dispatches
            }
            if (faults && rng.nextInt(4) == 0) {
                work.onReplyNotEmitted(key);
                tenure.remove(key);
            } else {
                inFlight.add(key);
            }
        }

        /** A request held as dispatched whose reply is gone is never dispatched again by this opening. */
        void checkNoLostDispatch() {
            if (!gate.isOpen() || leadershipApplied || lastLeaderIndex >= applied) {
                return; // a close is already due
            }
            for (final Long key : tenure) {
                assertTrue(pendingReplies.contains(key) || inFlight.contains(key) || replied.contains(key),
                           "member " + memberId + " holds " + key + " as dispatched, but its reply is gone");
            }
        }

        boolean isQuiet() {
            return applied == log.size() && pendingReplies.isEmpty() && work.size() == 0;
        }
    }

    private void append(final Entry entry) {
        log.add(entry);
        if (entry.kind() == Kind.REQUEST) {
            open.add(entry.value());
        } else if (entry.kind() == Kind.REPLY) {
            open.remove(entry.value());
            replied.add(entry.value());
        }
        outstandingAfter.add(open.size());
    }

    private int outstandingAt(final int length) {
        return length == 0 ? 0 : outstandingAfter.get(length - 1);
    }

    private void elect() {
        leader = (leader + 1 + rng.nextInt(REPLICAS - 1)) % REPLICAS;
        lastLeaderIndex = log.size();
        append(new Entry(Kind.LEADER, leader));
        if (faults) {
            inFlight.removeIf(key -> rng.nextBoolean()); // uncommitted on the old leader
        }
    }

    private void deliver() {
        append(new Entry(Kind.REPLY, inFlight.remove(0)));
    }

    private void chaosStep() {
        final Replica replica = replicas.get(rng.nextInt(REPLICAS));
        final int roll = rng.nextInt(100);
        if (roll < 20) {
            append(new Entry(Kind.REQUEST, log.size() + 1));
        } else if (roll < 45) {
            replica.applyNext();
        } else if (roll < 50) {
            replica.applyAll();
        } else if (roll < 70) {
            replica.dutyCycle();
        } else if (roll < 80) {
            replica.offer();
        } else if (roll < 90) {
            if (!inFlight.isEmpty()) {
                deliver();
            }
        } else if (roll < 93) {
            elect();
        } else {
            replica.recovering = !replica.recovering;
        }
    }

    private boolean isQuiet() {
        return inFlight.isEmpty() && replicas.stream().allMatch(Replica::isQuiet);
    }

    @ParameterizedTest(name = "seed {0}")
    @ValueSource(longs = { 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610, 987, 1597 })
    void answersEveryRequestUnderRandomFailovers(final long seed) {
        rng = new Random(seed);
        faults = true;
        for (int member = 0; member < REPLICAS; ++member) {
            replicas.add(new Replica(member));
        }
        append(new Entry(Kind.LEADER, leader));

        for (int step = 0; step < CHAOS_STEPS; ++step) {
            chaosStep();
            replicas.forEach(Replica::checkNoLostDispatch);
        }

        faults = false;
        replicas.forEach(replica -> replica.recovering = false);
        for (int round = 0; round < QUIESCE_ROUNDS && !isQuiet(); ++round) {
            while (!inFlight.isEmpty()) {
                deliver();
            }
            replicas.forEach(Replica::applyAll);
            replicas.forEach(Replica::dutyCycle);
            for (final Replica replica : replicas) {
                while (!replica.pendingReplies.isEmpty()) {
                    replica.offer();
                }
            }
        }

        assertTrue(isQuiet(), "did not converge once the faults stopped");
        assertEquals(0, outstandingAt(log.size()), "every request has a reply in the log");
    }
}
