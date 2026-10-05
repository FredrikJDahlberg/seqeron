using Adaptive.Aeron;

namespace Org.Limitless.Seqeron.Sequencer.Client;

/// <summary>
/// Decides what <c>ClusterStreamSender.Send</c>'s spin does about a failed offer: keep spinning, alert, give the
/// session up, or fail. Split off so it is unit-testable; the sender cannot run without Aeron.
/// <para><b><c>CLOSED</c> is a retry.</b> During an election <c>AeronCluster</c> closes the ingress publication
/// and waits for a <c>NewLeader</c>, on a session the cluster still holds; reading it as terminal fails every
/// submit made during a leadership change. The Aeron import is for the result constants only.</para>
/// <para><c>IngressStallPolicy.java</c> is its twin; keep the two in step.</para>
/// </summary>
internal sealed class IngressStallPolicy
{
    /// <summary>What the sender should do about the offer that just failed.</summary>
    public enum Action
    {
        /// <summary>Keep spinning: transient back-pressure, a publication not yet connected, or an
        /// election.</summary>
        Retry,

        /// <summary>Still blocked, and the alert interval has elapsed: say so, then keep spinning.</summary>
        Alert,

        /// <summary>The cluster closed this session, or the client closed itself. Nothing left to retry
        /// on.</summary>
        SessionGone,

        /// <summary>Nothing has been accepted for the fatal timeout: the session is what it has become.</summary>
        Stalled,

        /// <summary>The publication cannot advance. Unrecoverable, and no caller's retry can repair it.</summary>
        Fatal
    }

    private readonly long _stallTimeoutMs;
    private readonly long _alertIntervalMs;

    // Monotonic ms reading when the current block started; 0 when no offer is failing.
    private long _blockedSinceMs;
    private long _nextAlertMs;

    /// <param name="stallTimeoutMs">how long offers may keep failing before the session is called lost</param>
    /// <param name="alertIntervalMs">how often a still-blocked spin says so</param>
    public IngressStallPolicy(long stallTimeoutMs, long alertIntervalMs)
    {
        _stallTimeoutMs = stallTimeoutMs;
        _alertIntervalMs = alertIntervalMs;
    }

    /// <summary>Evaluates one failed offer. The first failure of a block only anchors the clock: how long the
    /// publication had been unable to take a frame before it is unknown, so the timers start here.</summary>
    /// <param name="nowMs">monotonic clock reading, in milliseconds</param>
    /// <param name="offerResult">what <c>Offer</c> returned — a negative Aeron result code</param>
    /// <param name="sessionLost">whether egress has already reported this session closed or errored</param>
    /// <param name="clientClosed">whether the cluster client has closed itself</param>
    /// <returns>what the sender should do</returns>
    public Action OnOfferFailed(long nowMs, long offerResult, bool sessionLost, bool clientClosed)
    {
        if (offerResult == Publication.MAX_POSITION_EXCEEDED)
        {
            return Action.Fatal;
        }
        if (sessionLost || clientClosed)
        {
            return Action.SessionGone;
        }
        if (_blockedSinceMs == 0)
        {
            _blockedSinceMs = nowMs;
            _nextAlertMs = nowMs + _alertIntervalMs;
            return Action.Retry;
        }
        if (nowMs - _blockedSinceMs >= _stallTimeoutMs)
        {
            return Action.Stalled;
        }
        if (nowMs >= _nextAlertMs)
        {
            _nextAlertMs = nowMs + _alertIntervalMs;
            return Action.Alert;
        }
        return Action.Retry;
    }

    /// <summary>How long the current block has lasted, for the line an <see cref="Action.Alert"/> prints.</summary>
    /// <param name="nowMs">monotonic clock reading, in milliseconds</param>
    public long BlockedMs(long nowMs)
    {
        return _blockedSinceMs == 0 ? 0 : nowMs - _blockedSinceMs;
    }

    /// <summary>Records that an offer landed, clearing the clocks so the next block starts its own.</summary>
    public void OnOffered()
    {
        _blockedSinceMs = 0;
        _nextAlertMs = 0;
    }
}
