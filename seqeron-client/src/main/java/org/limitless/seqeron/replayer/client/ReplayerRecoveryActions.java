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
     */
    void sendReplayRequest(long requestId, long fromPosition);

    /** Offers a {@code SnapshotQuery}; best-effort, like the request. */
    void sendSnapshotQuery(long requestId, int sourceId, long round);

    /** @return whether the {@code ReplayComplete} reached the wire */
    boolean sendReplayComplete();

    /** @return whether the {@code ReplayHeartbeat} reached the wire */
    boolean sendReplayHeartbeat();

    /** Subscribes to exactly one replay — this one — for as long as the client rides it. */
    void openReplay(long replaySessionId);

    /** Drops whatever replay subscription is open. */
    void closeReplay();

    /** The {@code seqeron.app.recoveryStalled} gauge. */
    void recoveryStalled(boolean stalled);

    /** The node this app runs on, or null before start — log attribution only. */
    Integer memberId();

    /** Epoch millis; the unit suite advances it. */
    long nowMs();
}
