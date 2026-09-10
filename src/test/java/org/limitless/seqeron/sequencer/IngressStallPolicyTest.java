package org.limitless.seqeron.sequencer;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.aeron.Publication;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.limitless.seqeron.sequencer.IngressStallPolicy.Action;

/**
 * Unit tests for the verdict inside {@code ClusterStreamSender.send}'s spin. No Aeron runtime: the offer
 * result and the two session flags are exactly what the sender reads, and the clock is the caller's.
 */
class IngressStallPolicyTest {
    private static final long STALL_NS = TimeUnit.SECONDS.toNanos(10);
    private static final long ALERT_NS = TimeUnit.SECONDS.toNanos(1);
    /** Arbitrary non-zero clock origin — 0 is the policy's "no block in progress" sentinel. */
    private static final long T0 = TimeUnit.HOURS.toNanos(5);

    private static long seconds(final long s) {
        return T0 + TimeUnit.SECONDS.toNanos(s);
    }

    private final IngressStallPolicy policy = new IngressStallPolicy(STALL_NS, ALERT_NS);

    @Test
    @DisplayName("CLOSED is an election in progress, not a dead session: the spin keeps going")
    void closedIsRetriedForTheLengthOfAnElection() {
        // The regression this class exists for. A leader that dies has AeronCluster close the ingress
        // publication while it awaits a NewLeader event, so every offer returns CLOSED meanwhile — on a
        // session the cluster still holds. Reading it as terminal fails every submit made during one.
        assertEquals(Action.RETRY, policy.onOfferFailed(seconds(0), Publication.CLOSED, false, false));
        for (long s = 1; s < 5; s++) {
            final Action action = policy.onOfferFailed(seconds(s), Publication.CLOSED, false, false);
            assertEquals(Action.ALERT, action, "second " + s + " should alert and keep spinning");
        }
        // …and the frame lands once the new leader's publication is installed.
        policy.onOffered();
        assertEquals(0, policy.blockedNs(seconds(5)));
    }

    @Test
    @DisplayName("ordinary back-pressure retries silently until the alert interval elapses")
    void backPressureRetriesQuietlyThenAlerts() {
        assertEquals(Action.RETRY, policy.onOfferFailed(T0, Publication.BACK_PRESSURED, false, false));
        assertEquals(Action.RETRY, policy.onOfferFailed(T0 + ALERT_NS / 2, Publication.BACK_PRESSURED, false, false));
        assertEquals(Action.ALERT, policy.onOfferFailed(T0 + ALERT_NS, Publication.BACK_PRESSURED, false, false));
        assertEquals(Action.RETRY,
                     policy.onOfferFailed(T0 + ALERT_NS + 1, Publication.BACK_PRESSURED, false, false));
    }

    @Test
    @DisplayName("NOT_CONNECTED is retried: the publication is being (re)established")
    void notConnectedIsRetried() {
        assertEquals(Action.RETRY, policy.onOfferFailed(T0, Publication.NOT_CONNECTED, false, false));
    }

    @Test
    @DisplayName("MAX_POSITION_EXCEEDED is unrecoverable, and is so from the first observation")
    void maxPositionExceededIsFatalImmediately() {
        assertEquals(Action.FATAL, policy.onOfferFailed(T0, Publication.MAX_POSITION_EXCEEDED, false, false));
    }

    @Test
    @DisplayName("a session the cluster closed ends the spin — there is nothing left to retry on")
    void sessionLostEndsTheSpin() {
        assertEquals(Action.RETRY, policy.onOfferFailed(T0, Publication.CLOSED, false, false));
        assertEquals(Action.SESSION_GONE, policy.onOfferFailed(seconds(1), Publication.CLOSED, true, false));
    }

    @Test
    @DisplayName("a client that closed itself ends it too")
    void closedClientEndsTheSpin() {
        assertEquals(Action.SESSION_GONE, policy.onOfferFailed(T0, Publication.CLOSED, false, true));
    }

    @Test
    @DisplayName("nothing accepted for the fatal timeout: the session is called what it has become")
    void sustainedBlockStalls() {
        assertEquals(Action.RETRY, policy.onOfferFailed(T0, Publication.BACK_PRESSURED, false, false));
        assertEquals(Action.STALLED,
                     policy.onOfferFailed(T0 + STALL_NS, Publication.BACK_PRESSURED, false, false));
    }

    @Test
    @DisplayName("the fatal clock is per block: a landed offer resets it, so two short blocks are not one long one")
    void aLandedOfferResetsTheClock() {
        assertEquals(Action.RETRY, policy.onOfferFailed(T0, Publication.BACK_PRESSURED, false, false));
        assertEquals(Action.ALERT,
                     policy.onOfferFailed(T0 + STALL_NS - 1, Publication.BACK_PRESSURED, false, false));
        policy.onOffered();
        // A new block starting just before the old clock would have fired must anchor its own.
        assertEquals(Action.RETRY,
                     policy.onOfferFailed(T0 + STALL_NS, Publication.BACK_PRESSURED, false, false));
        assertEquals(Action.RETRY,
                     policy.onOfferFailed(T0 + STALL_NS + 1, Publication.BACK_PRESSURED, false, false));
    }

    @Test
    @DisplayName("the first failure only anchors the clock: how long it had been failing is unknown")
    void firstFailureAnchorsRatherThanJudges() {
        assertEquals(Action.RETRY, policy.onOfferFailed(seconds(100), Publication.BACK_PRESSURED, false, false));
        assertEquals(0, policy.blockedNs(seconds(100)));
        assertEquals(TimeUnit.SECONDS.toNanos(1), policy.blockedNs(seconds(101)));
    }
}
