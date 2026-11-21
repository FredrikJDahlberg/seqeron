package org.limitless.seqeron.sequencer;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.limitless.seqeron.util.Logger;

/**
 * How long {@code SequencerService} waits on a consensus module that will not take the cluster-clock timer, and when it
 * stops waiting and takes the node down instead. {@link HeartbeatTimer} holds no Aeron runtime and no clock of its own,
 * so the suite drives it directly: its clock is owned by the test, which advances it only on an idle between attempts.
 */
class HeartbeatTimerTest {
    private static final long CLUSTER_TIME_DEADLINE = 1_000;

    private Recorder actions;
    private HeartbeatTimer timer;

    @BeforeEach
    void setUp() {
        Logger.install(event -> { });
        actions = new Recorder();
        timer = new HeartbeatTimer(actions);
    }

    @AfterEach
    void restoreLogger() {
        Logger.reset();
    }

    /** The transport recorded rather than performed. The clock advances only on {@link #idle}. */
    private static final class Recorder implements HeartbeatTimer.Actions {
        long nanosPerIdle;
        long nowNs;
        long failedTimers; // scheduleTimer refusals before one is accepted
        int halts;

        @Override
        public boolean scheduleTimer(final long deadline) {
            return --failedTimers < 0;
        }

        @Override
        public void idle() {
            nowNs += nanosPerIdle;
        }

        @Override
        public long nanoTime() {
            return nowNs;
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
        public long clusterSeqNo() {
            return 42;
        }
    }

    @Test
    @DisplayName("a timer the consensus module takes costs one call")
    void acceptedTimerDoesNotSpin() {
        actions.nanosPerIdle = 1;
        timer.scheduleHeartbeat(CLUSTER_TIME_DEADLINE);

        assertEquals(0, actions.nowNs);
        assertEquals(0, actions.halts);
    }

    @Test
    @DisplayName("back-pressure on the timer clears on its own without going fatal")
    void transientTimerBackPressureIsNotFatal() {
        actions.nanosPerIdle = periodOf(HeartbeatTimer.HEARTBEAT_SCHEDULE_FATAL_TIMEOUT_NS / 4);
        actions.failedTimers = clockReads(3);

        timer.scheduleHeartbeat(CLUSTER_TIME_DEADLINE);

        assertEquals(0, actions.halts);
    }

    @Test
    @DisplayName("a consensus module that never takes the cluster-clock timer halts the node, once")
    void wedgedConsensusModuleIsFatal() {
        actions.nanosPerIdle = periodOf(HeartbeatTimer.HEARTBEAT_SCHEDULE_FATAL_TIMEOUT_NS);
        actions.failedTimers = clockReads(5);

        timer.scheduleHeartbeat(CLUSTER_TIME_DEADLINE);

        assertEquals(1, actions.halts, "a graceful shutdown would wait on this thread, which is still spinning");
    }

    /**
     * How many failed attempts it takes to reach {@code reads} clock checks. The spin reads the clock every
     * {@link HeartbeatTimer#SPINS_PER_CLOCK_CHECK} attempts, and the first read only anchors the period.
     */
    private static long clockReads(final int reads) {
        return (long)reads * HeartbeatTimer.SPINS_PER_CLOCK_CHECK;
    }

    /**
     * The per-idle advance that makes one clock-check period span at least {@code spanNs} — rounded up, so
     * integer division cannot leave a period a few nanoseconds short of the deadline it is meant to cross.
     */
    private static long periodOf(final long spanNs) {
        return spanNs / HeartbeatTimer.SPINS_PER_CLOCK_CHECK + 1;
    }
}
