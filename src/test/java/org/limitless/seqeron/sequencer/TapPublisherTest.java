package org.limitless.seqeron.sequencer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.limitless.seqeron.util.Logger;

/**
 * The reliable-offer discipline behind {@code SequencerService}: how long it waits on a back-pressured tap
 * or a consensus module that will not take a timer, and when it stops waiting and takes the node down
 * instead. {@link TapPublisher} holds no Aeron runtime and no clock of its own, so the suite drives it
 * directly — its transport is recorded through {@link TapPublisher.Actions} and its clock is owned by the
 * test, which advances it only where the real one would: on an idle between attempts.
 *
 * <p>This is the {@code EXIT_TAP_FATAL} contract, and the reason it is worth pinning here rather than only
 * in {@code chaos-runner.sh}: both halves fail silently. A node that terminates too eagerly costs the
 * cluster a member on transient back-pressure that would have cleared; one that never terminates keeps
 * sequencing history into an archive that is no longer recording it, and nothing says so until someone asks
 * that node for a replay.
 */
class TapPublisherTest {
    /** Aeron's {@code Publication.BACK_PRESSURED} — a failure that retrying clears. */
    private static final long BACK_PRESSURED = -2;

    /** Aeron's {@code Publication.CLOSED} — a failure no retry clears. */
    private static final long CLOSED = -4;

    private static final long CLUSTER_TIME_DEADLINE = 1_000;

    private Recorder actions;
    private TapPublisher publisher;

    /** Every alert logs, and the fatal tests raise a hundred — kept out of the build output. */
    @BeforeEach
    void setUp() {
        Logger.install(event -> { });
        actions = new Recorder();
        publisher = new TapPublisher(actions);
    }

    @AfterEach
    void restoreLogger() {
        Logger.reset();
    }

    /**
     * The transport recorded rather than performed. The clock advances only on {@link #idle}, so a test
     * says how long back-pressure lasts by saying how many offers fail — nothing here moves on its own.
     */
    private static final class Recorder implements TapPublisher.Actions {
        /**
         * How far the clock moves per idle, expressed as the span of one {@link
         * TapPublisher#SPINS_PER_CLOCK_CHECK} period. The alert interval by default, so one period is one
         * alert; a test needing a coarser unit sets it and keeps its loop counts small.
         */
        long nanosPerIdle = periodOf(TapPublisher.BACK_PRESSURE_ALERT_INTERVAL_NS);

        long nowNs;
        long failedOffers;   // offers to fail before one lands
        long failResult = BACK_PRESSURED;
        long failedTimers;   // scheduleTimer refusals before one is accepted
        boolean recordingActive = true;
        long recordedPosition;
        long recordedAdvancePerIdle; // an archive that is slow but still draining

        int offers;
        int alerts;
        int halts;
        int fatals;
        /** Every write to the stall gauge, in order — an episode's shape, not just its last value. */
        final List<Boolean> stalledGauge = new ArrayList<>();

        @Override
        public long offerFrame(final int length) {
            return ++offers <= failedOffers ? failResult : 1;
        }

        @Override
        public boolean isUnrecoverable(final long offerResult) {
            return offerResult == CLOSED;
        }

        @Override
        public boolean scheduleTimer(final long deadline) {
            return --failedTimers < 0;
        }

        @Override
        public void idle() {
            nowNs += nanosPerIdle;
            recordedPosition += recordedAdvancePerIdle;
        }

        @Override
        public long nanoTime() {
            return nowNs;
        }

        @Override
        public boolean recordingActive() {
            return recordingActive;
        }

        @Override
        public long recordedPosition() {
            return recordedPosition;
        }

        @Override
        public long recordingId() {
            return 7;
        }

        @Override
        public void tapBackPressureAlert() {
            ++alerts;
        }

        @Override
        public void tapStalled(final boolean stalled) {
            stalledGauge.add(stalled);
        }

        @Override
        public void signalFatal() {
            ++fatals;
        }

        @Override
        public void halt() {
            ++halts;
        }

        @Override
        public Integer memberId() {
            return 0;
        }

        @Override
        public long globalSeqNo() {
            return 42;
        }
    }

    // ── emit ──────────────────────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("an offer that lands reads no clock and raises nothing")
    void landedOfferCostsNothing() {
        publisher.emit(64);

        assertEquals(1, actions.offers);
        assertEquals(0, actions.nowNs, "the normal path must not idle, which is what reads the clock");
        assertEquals(0, actions.alerts);
        assertEquals(List.of(), actions.stalledGauge, "a gauge written per frame would be the cost this avoids");
    }

    @Test
    @DisplayName("back-pressure shorter than the alert interval is silent")
    void transientBackPressureIsSilent() {
        actions.failedOffers = clockReads(1); // the first read only anchors the period

        publisher.emit(64);

        assertEquals(0, actions.alerts);
        assertFalse(publisher.isFatalSignalled());
    }

    @Test
    @DisplayName("continuous back-pressure alerts once per interval")
    void sustainedBackPressureAlertsPerInterval() {
        actions.failedOffers = clockReads(4);

        publisher.emit(64);

        assertEquals(3, actions.alerts, "one per elapsed interval after the anchoring read");
        assertFalse(publisher.isFatalSignalled());
    }

    @Test
    @DisplayName("a recording that stops advancing raises the stall gauge, which the next landed offer clears")
    void noRecordingProgressRaisesAndClearsTheStallGauge() {
        actions.nanosPerIdle = periodOf(TapPublisher.SUSTAINED_BACKPRESSURE_THRESHOLD_NS);
        actions.failedOffers = clockReads(3); // anchor, first observation, then one threshold's worth

        publisher.emit(64);

        assertEquals(List.of(true, false), actions.stalledGauge,
                     "raised once — STALLED is edge-triggered — and cleared by the offer that landed");
        assertFalse(publisher.isFatalSignalled(), "a stall is not yet a reason to kill the node");
    }

    @Test
    @DisplayName("a recording still making no progress at the fatal timeout terminates the node")
    void noRecordingProgressPastTheFatalTimeoutTerminates() {
        actions.nanosPerIdle = periodOf(TapPublisher.SUSTAINED_BACKPRESSURE_THRESHOLD_NS);
        actions.failedOffers = clockReads(8); // five threshold periods past the first observation

        publisher.emit(64);

        assertTrue(publisher.isFatalSignalled());
        assertEquals(1, actions.fatals);
        assertEquals(Boolean.TRUE, actions.stalledGauge.get(actions.stalledGauge.size() - 1),
                     "the gauge must latch, not clear on the way out");
    }

    @Test
    @DisplayName("an archive that is slow but still draining is never fatal, however long it back-pressures")
    void slowButDrainingArchiveIsLeftAlone() {
        actions.nanosPerIdle = periodOf(TapPublisher.TAP_STALL_FATAL_TIMEOUT_NS);
        actions.recordedAdvancePerIdle = 1; // the recording keeps moving under the back-pressure
        actions.failedOffers = clockReads(20);

        publisher.emit(64);

        assertFalse(publisher.isFatalSignalled(), "this bounds an archive that has stopped, not a busy one");
        assertEquals(List.of(), actions.stalledGauge);
        assertTrue(actions.alerts > 0, "the operator is still told it is happening");
    }

    @Test
    @DisplayName("a recording that has gone away is fatal at the first evaluation")
    void recordingGoneIsFatal() {
        actions.recordingActive = false;
        actions.failedOffers = clockReads(2);

        publisher.emit(64);

        assertTrue(publisher.isFatalSignalled());
        assertEquals(1, actions.fatals);
        assertEquals(List.of(true), actions.stalledGauge, "the gauge must latch, not clear on the way out");
    }

    @Test
    @DisplayName("an offer result no retry can clear is fatal without waiting for an interval")
    void unrecoverableOfferResultIsFatalImmediately() {
        actions.failResult = CLOSED;
        actions.failedOffers = 1;

        publisher.emit(64);

        assertTrue(publisher.isFatalSignalled());
        assertEquals(1, actions.fatals);
        assertEquals(2, actions.offers, "the spin continues — the only exits are a landed offer and death");
    }

    @Test
    @DisplayName("the fatal decision is latched: later failures do not signal a second shutdown")
    void fatalIsLatched() {
        actions.recordingActive = false;
        actions.failedOffers = clockReads(6);

        publisher.emit(64);

        assertEquals(1, actions.fatals, "a second signalFatal would race the first shutdown");
        assertEquals(0, actions.halts, "and the backstop has not elapsed yet");
    }

    @Test
    @DisplayName("a shutdown that never completes is halted once the backstop elapses")
    void stalledShutdownIsHalted() {
        actions.nanosPerIdle = periodOf(TapPublisher.FATAL_SHUTDOWN_BACKSTOP_NS);
        actions.recordingActive = false;
        actions.failedOffers = clockReads(4);

        publisher.emit(64);

        assertEquals(1, actions.fatals);
        assertTrue(actions.halts > 0, "the graceful path can wedge on the very archive that failed");
    }

    // ── checkRecordingAlive ───────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("a live recording passes the 1 Hz check silently")
    void liveRecordingPassesTheCheck() {
        publisher.checkRecordingAlive();

        assertFalse(publisher.isFatalSignalled());
        assertEquals(0, actions.halts);
    }

    @Test
    @DisplayName("a recording that stopped is fatal even though it back-pressures nothing")
    void stoppedRecordingIsFatalOffTheHeartbeat() {
        actions.recordingActive = false;

        publisher.checkRecordingAlive();

        assertTrue(publisher.isFatalSignalled(), "a stopped recording is the case emit's bound never sees");
        assertEquals(1, actions.fatals);
    }

    @Test
    @DisplayName("the heartbeat check keeps the shutdown backstop alive after a fatal")
    void heartbeatCheckHaltsAStalledShutdown() {
        actions.recordingActive = false;
        publisher.checkRecordingAlive();
        assertEquals(1, actions.fatals);

        publisher.checkRecordingAlive();
        assertEquals(0, actions.halts, "still inside the backstop");

        actions.nowNs += TapPublisher.FATAL_SHUTDOWN_BACKSTOP_NS;
        publisher.checkRecordingAlive();

        assertEquals(1, actions.halts);
        assertEquals(1, actions.fatals, "and it re-reports nothing on the way out");
    }

    // ── scheduleHeartbeat ─────────────────────────────────────────────────────────────────────────────

    @Test
    @DisplayName("a timer the consensus module takes costs one call")
    void acceptedTimerDoesNotSpin() {
        publisher.scheduleHeartbeat(CLUSTER_TIME_DEADLINE);

        assertEquals(0, actions.nowNs);
        assertFalse(publisher.isFatalSignalled());
    }

    @Test
    @DisplayName("back-pressure on the timer clears on its own without going fatal")
    void transientTimerBackPressureIsNotFatal() {
        actions.nanosPerIdle = periodOf(TapPublisher.HEARTBEAT_SCHEDULE_FATAL_TIMEOUT_NS / 4);
        actions.failedTimers = clockReads(3);

        publisher.scheduleHeartbeat(CLUSTER_TIME_DEADLINE);

        assertFalse(publisher.isFatalSignalled());
    }

    @Test
    @DisplayName("a consensus module that never takes the cluster-clock timer is fatal")
    void wedgedConsensusModuleIsFatal() {
        actions.nanosPerIdle = periodOf(TapPublisher.HEARTBEAT_SCHEDULE_FATAL_TIMEOUT_NS);
        actions.failedTimers = clockReads(3);

        publisher.scheduleHeartbeat(CLUSTER_TIME_DEADLINE);

        assertTrue(publisher.isFatalSignalled(), "an unscheduled timer stops the cluster clock outright");
        assertEquals(1, actions.fatals);
    }

    @Test
    @DisplayName("a timer spin after a fatal runs the shutdown backstop rather than a second fatal")
    void timerSpinAfterAFatalHalts() {
        actions.recordingActive = false;
        publisher.checkRecordingAlive();
        assertEquals(1, actions.fatals);

        actions.nanosPerIdle = periodOf(TapPublisher.FATAL_SHUTDOWN_BACKSTOP_NS);
        actions.failedTimers = clockReads(3);
        publisher.scheduleHeartbeat(CLUSTER_TIME_DEADLINE);

        assertEquals(1, actions.fatals, "nothing reaches the fatal path once it is latched");
        assertTrue(actions.halts > 0);
    }

    // ── helpers ───────────────────────────────────────────────────────────────────────────────────────

    /**
     * How many failed attempts it takes to reach {@code reads} clock checks. The spin reads the clock every
     * {@link TapPublisher#SPINS_PER_CLOCK_CHECK} attempts, and the first read only anchors the period.
     */
    private static long clockReads(final int reads) {
        return (long)reads * TapPublisher.SPINS_PER_CLOCK_CHECK;
    }

    /**
     * The per-idle advance that makes one clock-check period span at least {@code spanNs} — rounded up, so
     * integer division cannot leave a period a few nanoseconds short of the deadline it is meant to cross.
     */
    private static long periodOf(final long spanNs) {
        return spanNs / TapPublisher.SPINS_PER_CLOCK_CHECK + 1;
    }
}
