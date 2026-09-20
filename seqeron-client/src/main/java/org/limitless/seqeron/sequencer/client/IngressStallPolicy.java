package org.limitless.seqeron.sequencer.client;

import io.aeron.Publication;

/**
 * Decides what {@link ClusterStreamSender#send}'s spin does about a failed offer: keep spinning, alert, give
 * the session up, or fail. Split off so it is unit-testable; the sender cannot run without Aeron.
 *
 * <p><b>{@code CLOSED} is a retry.</b> During an election {@code AeronCluster} closes the ingress publication
 * and waits for a {@code NewLeader}, on a session the cluster still holds; reading it as terminal fails
 * every submit made during a leadership change. The Aeron import is for the result constants only.
 */
final class IngressStallPolicy {
    /** What the sender should do about the offer that just failed. */
    public enum Action {
        /** Keep spinning: transient back-pressure, a publication not yet connected, or an election. */
        RETRY,
        /** Still blocked, and the alert interval has elapsed: say so, then keep spinning. */
        ALERT,
        /** The cluster closed this session, or the client closed itself. Nothing left to retry on. */
        SESSION_GONE,
        /** Nothing has been accepted for the fatal timeout: the session is what it has become. */
        STALLED,
        /** The publication cannot advance. Unrecoverable, and no caller's retry can repair it. */
        FATAL
    }

    private final long stallTimeoutMs;
    private final long alertIntervalMs;

    /** Monotonic ms reading when the current block started; 0 when no offer is failing. */
    private long blockedSinceMs;
    private long nextAlertMs;

    /**
     * @param stallTimeoutMs  how long offers may keep failing before the session is called lost
     * @param alertIntervalMs how often a still-blocked spin says so
     */
    public IngressStallPolicy(final long stallTimeoutMs, final long alertIntervalMs) {
        this.stallTimeoutMs = stallTimeoutMs;
        this.alertIntervalMs = alertIntervalMs;
    }

    /**
     * Evaluates one failed offer. The first failure of a block only anchors the clock: how long the
     * publication had been unable to take a frame before it is unknown, so the timers start here.
     *
     * @param nowMs        monotonic clock reading, in milliseconds
     * @param offerResult  what {@code offer} returned — a negative Aeron result code
     * @param sessionLost  whether egress has already reported this session closed or errored
     * @param clientClosed whether the cluster client has closed itself
     * @return what the sender should do
     */
    public Action onOfferFailed(final long nowMs, final long offerResult, final boolean sessionLost,
                                final boolean clientClosed) {
        if (offerResult == Publication.MAX_POSITION_EXCEEDED) {
            return Action.FATAL;
        }
        if (sessionLost || clientClosed) {
            return Action.SESSION_GONE;
        }
        if (blockedSinceMs == 0) {
            blockedSinceMs = nowMs;
            nextAlertMs = nowMs + alertIntervalMs;
            return Action.RETRY;
        }
        if (nowMs - blockedSinceMs >= stallTimeoutMs) {
            return Action.STALLED;
        }
        if (nowMs >= nextAlertMs) {
            nextAlertMs = nowMs + alertIntervalMs;
            return Action.ALERT;
        }
        return Action.RETRY;
    }

    /** How long the current block has lasted, for the line an {@link Action#ALERT} prints. */
    public long blockedMs(final long nowMs) {
        return blockedSinceMs == 0 ? 0 : nowMs - blockedSinceMs;
    }

    /** Records that an offer landed, clearing the clocks so the next block starts its own. */
    public void onOffered() {
        blockedSinceMs = 0;
        nextAlertMs = 0;
    }
}
