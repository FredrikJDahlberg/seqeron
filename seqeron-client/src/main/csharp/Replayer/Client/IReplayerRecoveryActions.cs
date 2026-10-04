namespace Org.Limitless.Seqeron.Replayer.Client;

/// <summary>
/// Everything <see cref="ReplayerRecovery"/> cannot do itself: the sends, the replay subscription, and the gauge.
/// <c>ReplayerStreamReceiver</c> implements it against Aeron; the unit suite substitutes a recorder.
/// </summary>
internal interface IReplayerRecoveryActions
{
    /// <summary>Offers a <c>ReplayRequest</c>. Best-effort: it must not be retried any faster than the resend timer
    /// does.</summary>
    /// <param name="requestId">the id the reply must carry</param>
    /// <param name="fromPosition">where to resume the active recording, or <c>ReplayProtocol.FromStart</c></param>
    void SendReplayRequest(long requestId, long fromPosition);

    /// <summary>Offers a <c>SnapshotQuery</c>; best-effort, like the request.</summary>
    /// <param name="requestId">the id the reply must carry</param>
    /// <param name="sourceId">the source restored</param>
    /// <param name="round">the round of the local file asked about</param>
    void SendSnapshotQuery(long requestId, int sourceId, long round);

    /// <summary>Whether the <c>ReplayComplete</c> reached the wire.</summary>
    bool SendReplayComplete();

    /// <summary>Whether the <c>ReplayHeartbeat</c> reached the wire.</summary>
    bool SendReplayHeartbeat();

    /// <summary>Subscribes to exactly one replay, this one, for as long as the client rides it.</summary>
    /// <param name="replaySessionId">the replay's session</param>
    void OpenReplay(long replaySessionId);

    /// <summary>Drops whatever replay subscription is open.</summary>
    void CloseReplay();

    /// <summary>The <c>seqeron.app.recoveryStalled</c> gauge.</summary>
    /// <param name="stalled">its new value</param>
    void RecoveryStalled(bool stalled);

    /// <summary>The node this app runs on, or null before start; log attribution only.</summary>
    int? MemberId { get; }

    /// <summary>Epoch millis; the unit suite advances it.</summary>
    long NowMs();
}
