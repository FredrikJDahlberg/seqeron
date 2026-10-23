package org.limitless.seqeron.replayer.client;

/**
 * Everything {@link ReplayerRecovery} cannot do itself: the sends, the replay subscription, and the gauge.
 * {@link ReplayerStreamReceiver} implements it against Aeron; the unit suite substitutes a recorder.
 */
interface ReplayerRecoveryActions {
    /**
     * Offers a {@code ReplayRequest}. Best-effort by design — see {@code ReplayerRecovery.requestReplay} on
     * why it must not be retried any faster than the resend timer does.
     * @param fromPosition where to resume the active recording, or {@code ReplayProtocol.FROM_START}
     * @param cutClusterSeqNo the cut of the snapshot a restore checks, else {@code ReplayProtocol.NO_CUT}
     */
    void sendReplayRequest(long requestId, long fromPosition, long cutClusterSeqNo);

    /** Subscribes to exactly one replay — this one — for as long as the client rides it. */
    void openReplay(long replaySessionId);

    /** Drops whatever replay subscription is open. */
    void closeReplay();

    /** The {@code seqeron.app.recoveryStalled} gauge. */
    void recoveryStalled(boolean stalled);

    /** The node this app runs on, or null before start — log attribution only. */
    Integer memberId();

    /** The id the Replayer addresses this client's replies to, unique on its media driver; -1 before start. */
    long clientId();

    /** Monotonic millis; the unit suite advances it. */
    long nowMs();
}
