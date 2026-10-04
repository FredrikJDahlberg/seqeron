using Adaptive.Agrona;

namespace Org.Limitless.Seqeron.Sequencer.Client;

/// <summary>
/// What <see cref="IngressPublisher"/> tells, and asks, about each frame it places, so a producer confirms its
/// ingress on the tap without handling a frame itself; and what the sender tells, and asks, about a leader change.
/// A send spinning through an election would land in the new term ahead of older frames that election lost, so the
/// sender gives it up while the hold is on. <see cref="PendingSends"/> is the implementation;
/// <c>IngressTracker.java</c> and <c>sequencer/client/IngressTracker.hpp</c> are its twins.
/// </summary>
public interface IIngressTracker
{
    /// <summary>Whether another frame can be tracked; one that cannot must not be sent.</summary>
    bool IsFull { get; }

    /// <summary>A frame just placed, with the sender's <c>ClusterSessionId</c> and <c>LeadershipTermId</c>.</summary>
    /// <param name="frame">the frame, from offset 0</param>
    /// <param name="length">its length in bytes</param>
    /// <param name="clusterSessionId">the session it went out on</param>
    /// <param name="leadershipTermId">the term it was stamped with</param>
    void Track(IDirectBuffer frame, int length, long clusterSessionId, long leadershipTermId);

    /// <summary>Egress named a new leader, for this term.</summary>
    /// <param name="leadershipTermId">the new leader's term</param>
    void OnNewLeader(long leadershipTermId);

    /// <summary>Whether frames from an earlier term may still need resending ahead of any new one.</summary>
    bool IsHolding { get; }
}
