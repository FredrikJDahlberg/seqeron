namespace Org.Limitless.Seqeron.Replayer.Client;

/// <summary>Receives every in-order frame that is not intercepted as a leadership change.</summary>
/// <param name="sequencedEvent">a flyweight, valid only for this call; copy anything that must outlive it</param>
public delegate void SequencedHandler(SequencedEvent sequencedEvent);

/// <summary>Receives each <c>LeadershipChanged</c> as it is dispatched, in log order: once per term, whether or not
/// the leader changed with it.</summary>
/// <param name="newLeaderMemberId">the member leading from this frame onward</param>
/// <param name="leadershipTermId">the term that begins here</param>
/// <param name="globalSeqNo">this frame's own sequence number, which counts toward continuity</param>
public delegate void LeadershipHandler(int newLeaderMemberId, long leadershipTermId, long globalSeqNo);

/// <summary>Fires on every transition to caught-up: when the receiver reaches the live tap, and again after each gap
/// it heals.</summary>
public delegate void CaughtUpHandler();
