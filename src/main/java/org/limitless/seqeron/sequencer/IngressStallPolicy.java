package org.limitless.seqeron.sequencer;

import io.aeron.Publication;

/**
 * Pure decision logic behind {@link ClusterStreamSender#send}'s spin: given what the offer returned and
 * how long it has been failing, it decides whether to keep spinning, alert, give the session up, or fail
 * outright. Split off and unit-tested the way {@link TapStallPolicy} is, and for the same reason — the
 * classification below is the part that is easy to get wrong and impossible to observe once it is inside
 * a loop.
 *
 * <p><b>{@code CLOSED} is a retry, and that is the whole point of testing this.</b> A leader that dies
 * closes the client's egress image, and {@code AeronCluster} responds by closing the ingress publication
 * and awaiting a {@code NewLeader} event — so every offer returns {@code CLOSED} for the length of an
 * election, on a session the cluster still holds. Reading that as terminal fails every submit made during
 * a leadership change, which is what {@code ClusterCtl} did before it came through here.
 *
 * <p>The Aeron import is for {@code Publication}'s result constants alone: naming which values are
 * terminal is this class's subject, so it must not be the caller restating them. Nothing here starts,
 * touches or needs an Aeron runtime.
 */
public final class IngressStallPolicy {
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

    private final long stallTimeoutNs;
    private final long alertIntervalNs;

    /** Monotonic reading when the current block started; 0 when no offer is failing. */
    private long blockedSinceNs;
    private long nextAlertNs;

    /**
     * @param stallTimeoutNs  how long offers may keep failing before the session is called lost
     * @param alertIntervalNs how often a still-blocked spin says so
     */
    public IngressStallPolicy(final long stallTimeoutNs, final long alertIntervalNs) {
        this.stallTimeoutNs = stallTimeoutNs;
        this.alertIntervalNs = alertIntervalNs;
    }

    /**
     * Evaluates one failed offer. The first failure of a block only anchors the clock: how long the
     * publication had been unable to take a frame before it is unknown, so the timers start here.
     *
     * @param nowNs        monotonic clock reading
     * @param offerResult  what {@code offer} returned — a negative Aeron result code
     * @param sessionLost  whether egress has already reported this session closed or errored
     * @param clientClosed whether the cluster client has closed itself
     * @return what the sender should do
     */
    public Action onOfferFailed(final long nowNs, final long offerResult, final boolean sessionLost,
                                final boolean clientClosed) {
        if (offerResult == Publication.MAX_POSITION_EXCEEDED) {
            return Action.FATAL;
        }
        if (sessionLost || clientClosed) {
            return Action.SESSION_GONE;
        }
        if (blockedSinceNs == 0) {
            blockedSinceNs = nowNs;
            nextAlertNs = nowNs + alertIntervalNs;
            return Action.RETRY;
        }
        if (nowNs - blockedSinceNs >= stallTimeoutNs) {
            return Action.STALLED;
        }
        if (nowNs >= nextAlertNs) {
            nextAlertNs = nowNs + alertIntervalNs;
            return Action.ALERT;
        }
        return Action.RETRY;
    }

    /** How long the current block has lasted, for the line an {@link Action#ALERT} prints. */
    public long blockedNs(final long nowNs) {
        return blockedSinceNs == 0 ? 0 : nowNs - blockedSinceNs;
    }

    /** Records that an offer landed, clearing the clocks so the next block starts its own. */
    public void onOffered() {
        blockedSinceNs = 0;
        nextAlertNs = 0;
    }
}
