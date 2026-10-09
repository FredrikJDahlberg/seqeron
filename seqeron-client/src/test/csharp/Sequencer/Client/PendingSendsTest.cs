using System.Collections.Generic;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Org.Limitless.Seqeron.Protocol;
using Org.Limitless.Seqeron.Replayer.Client;
using Xunit;

namespace Org.Limitless.Seqeron.Sequencer.Client;

/// <summary>
/// Unit tests for counting a producer's own frames back off its tap. Frames are real ingress frames; each payload is
/// one int, so a test names a frame by its number. Case for case with <c>PendingSendsTest.java</c> and
/// <c>PendingSendsTest.cpp</c>.
/// </summary>
public class PendingSendsTest
{
    private const int Capacity = 4;
    private const long Own = 4242;
    private const long Sibling = 4343;
    private const int PayloadId = 6;

    // Takes resends as the cluster's new leader would, recording each frame's number, until told to refuse.
    private sealed class FakeSender : IIngressSender
    {
        public readonly List<int> Sent = new List<int>();
        public long Session = Own;
        public long Term;
        public int AcceptsLeft = int.MaxValue;

        public FakeSender(long term)
        {
            Term = term;
        }

        public bool Send(IDirectBuffer frame, int length)
        {
            if (AcceptsLeft == 0)
            {
                return false;
            }
            AcceptsLeft--;
            Sent.Add(frame.GetInt(FrameLayer.MinIngressLength));
            return true;
        }

        public long ClusterSessionId => Session;

        public long LeadershipTermId => Term;
    }

    private readonly PendingSends _pending = new PendingSends(Capacity);
    private readonly SystemFrame _envelope = new SystemFrame();
    private readonly UnsafeBuffer _frame = new UnsafeBuffer(new byte[FrameLayer.MaxIngressLength]);
    private readonly byte[] _bodyBytes = new byte[sizeof(int)];
    private readonly UnsafeBuffer _body;

    public PendingSendsTest()
    {
        _body = new UnsafeBuffer(_bodyBytes);
    }

    [Fact(DisplayName = "every frame seen on its own tap leaves nothing pending")]
    public void EveryFrameSeenLeavesNothingPending()
    {
        Send(Own, 1, 1);
        Send(Own, 1, 2);
        Assert.Equal(2, _pending.Count);
        Tap(Own, 1);
        Tap(Own, 2);
        Assert.Equal(0, _pending.Count);
        Assert.Equal(0, _pending.Missing());
        Assert.False(_pending.IsFaulted);
    }

    [Fact(DisplayName = "a leader change after every frame came back loses nothing")]
    public void LeaderChangeAfterEveryFrameLosesNothing()
    {
        Send(Own, 1, 1);
        Tap(Own, 1);
        _pending.OnLeadershipChanged(2);
        Assert.Equal(0, _pending.Missing());
        Assert.Equal(0, _pending.Count);
    }

    [Fact(DisplayName = "frames not seen before a later term's LeadershipChanged are missing")]
    public void FramesNotSeenBeforeALaterTermAreMissing()
    {
        Send(Own, 1, 1);
        Send(Own, 1, 2);
        Send(Own, 1, 3);
        Tap(Own, 1);
        Assert.Equal(0, _pending.Missing()); // still pending until a later term closes the count
        _pending.OnLeadershipChanged(2);
        Assert.Equal(2, _pending.Missing());
        Assert.Equal(2, _pending.Count);
    }

    [Fact(DisplayName = "the LeadershipChanged that opens a frame's own term does not count it missing")]
    public void OwnTermsLeadershipChangedDoesNotCountIt()
    {
        // A tap still replaying history reaches term 1's opening frame after frames stamped 1 went out.
        Send(Own, 1, 1);
        _pending.OnLeadershipChanged(1);
        Assert.Equal(0, _pending.Missing());
        Tap(Own, 1);
        Assert.Equal(0, _pending.Count);
    }

    [Fact(DisplayName = "a frame stamped with a term the tap has already closed is missing at once")]
    public void StaleTermFrameIsMissingAtOnce()
    {
        // The tap showed the new term before egress brought the NewLeader, so the leader dropped this frame.
        _pending.OnLeadershipChanged(2);
        Send(Own, 1, 1);
        Assert.Equal(1, _pending.Missing());
    }

    [Fact(DisplayName = "a replaced session's lost frames stay missing while the new session's come back")]
    public void ReplacedSessionsLostFramesStayMissing()
    {
        Send(Own, 1, 1);
        Send(Own, 1, 2);
        _pending.OnLeadershipChanged(2);
        Send(Own + 1, 2, 3);
        Send(Own + 1, 2, 4);
        Tap(Own + 1, 3);
        Tap(Own + 1, 4);
        Assert.Equal(2, _pending.Missing());
        Assert.Equal(2, _pending.Count);
        Assert.False(_pending.IsFaulted);
    }

    [Fact(DisplayName = "the sibling's frames under the same sourceId are ignored")]
    public void SiblingFramesAreIgnored()
    {
        Send(Own, 1, 1);
        Tap(Sibling, 1);
        Tap(Sibling, 7);
        Assert.Equal(1, _pending.Count);
        Assert.False(_pending.IsFaulted);
    }

    [Fact(DisplayName = "an own frame that differs from the oldest pending copy latches the fault")]
    public void OwnFrameDifferingFromOldestCopyLatchesFault()
    {
        // The sequencer rejected frame 1 (S-7), so frame 2 is the next own frame on the tap.
        Send(Own, 1, 1);
        Send(Own, 1, 2);
        Tap(Own, 2);
        Assert.True(_pending.IsFaulted);
    }

    [Fact(DisplayName = "an own system frame comes back like an application one")]
    public void OwnSystemFrameComesBack()
    {
        SendSystem(Own, 1, SystemFrame.ConnectionClosed, 1);
        Tap(Own, true, SystemFrame.ConnectionClosed, 1);
        Assert.Equal(0, _pending.Count);
        Assert.False(_pending.IsFaulted);
    }

    [Fact(DisplayName = "an application frame never matches a system one with the same id")]
    public void ApplicationFrameNeverMatchesSystemOne()
    {
        SendSystem(Own, 1, SystemFrame.ConnectionClosed, 1);
        Tap(Own, false, SystemFrame.ConnectionClosed, 1);
        Assert.True(_pending.IsFaulted);
    }

    [Fact(DisplayName = "tracking into a full ring latches the fault")]
    public void TrackingIntoAFullRingLatchesFault()
    {
        for (int n = 0; n < Capacity; n++)
        {
            Send(Own, 1, n);
        }
        Assert.True(_pending.IsFull);
        Assert.False(_pending.IsFaulted);
        Send(Own, 1, Capacity);
        Assert.True(_pending.IsFaulted);
        Assert.Equal(Capacity, _pending.Count);
    }

    [Fact(DisplayName = "a new leader holds new sends until the older frames are seen")]
    public void NewLeaderHoldsUntilOlderFramesAreSeen()
    {
        Send(Own, 1, 1);
        _pending.OnNewLeader(2);
        Assert.True(_pending.IsHolding);
        Tap(Own, 1);
        Assert.False(_pending.IsHolding);
    }

    [Fact(DisplayName = "a new term with nothing pending holds nothing")]
    public void NewTermWithNothingPendingHoldsNothing()
    {
        _pending.OnNewLeader(2);
        _pending.OnLeadershipChanged(2);
        Assert.False(_pending.IsHolding);
    }

    [Fact(DisplayName =
              "the tap's LeadershipChanged holds before the NewLeader comes, and nothing is resent until it does")]
    public void TapsLeadershipChangedHoldsBeforeNewLeader()
    {
        var sender = new FakeSender(1);
        Send(Own, 1, 1);
        _pending.OnLeadershipChanged(2);
        Assert.True(_pending.IsHolding);
        Assert.Equal(0, _pending.ResendMissing(sender));
        Assert.Empty(sender.Sent);
    }

    [Fact(DisplayName = "the missing frames are resent oldest first, and that releases the hold")]
    public void MissingFramesAreResentOldestFirst()
    {
        var sender = new FakeSender(2);
        Send(Own, 1, 1);
        Send(Own, 1, 2);
        Send(Own, 1, 3);
        Tap(Own, 1);
        _pending.OnNewLeader(2);
        _pending.OnLeadershipChanged(2);
        Assert.Equal(2, _pending.ResendMissing(sender));
        Assert.Equal(new List<int> { 2, 3 }, sender.Sent);
        Assert.Equal(0, _pending.Missing());
        Assert.False(_pending.IsHolding);
        Tap(Own, 2);
        Tap(Own, 3);
        Assert.Equal(0, _pending.Count);
        Assert.False(_pending.IsFaulted);
    }

    [Fact(DisplayName = "a resend cut short keeps holding, and the rest follow in order")]
    public void ResendCutShortKeepsHolding()
    {
        var sender = new FakeSender(2);
        Send(Own, 1, 1);
        Send(Own, 1, 2);
        Send(Own, 1, 3);
        _pending.OnNewLeader(2);
        _pending.OnLeadershipChanged(2);
        sender.AcceptsLeft = 1;
        Assert.Equal(1, _pending.ResendMissing(sender));
        Assert.True(_pending.IsHolding);
        Assert.Equal(2, _pending.Missing());
        sender.AcceptsLeft = int.MaxValue;
        Assert.Equal(2, _pending.ResendMissing(sender));
        Assert.Equal(new List<int> { 1, 2, 3 }, sender.Sent);
        Assert.False(_pending.IsHolding);
        Tap(Own, 1);
        Tap(Own, 2);
        Tap(Own, 3);
        Assert.Equal(0, _pending.Count);
        Assert.False(_pending.IsFaulted);
    }

    [Fact(DisplayName = "a frame resent on a replaced session is matched on the new one")]
    public void ResentFrameMatchesOnTheNewSession()
    {
        var sender = new FakeSender(2) { Session = Own + 1 };
        Send(Own, 1, 1);
        _pending.OnNewLeader(2);
        _pending.OnLeadershipChanged(2);
        _pending.ResendMissing(sender);
        Tap(Own, 1);
        Assert.Equal(1, _pending.Count);
        Tap(Own + 1, 1);
        Assert.Equal(0, _pending.Count);
    }

    [Fact(DisplayName = "a resent frame lost again is resent again")]
    public void ResentFrameLostAgainIsResentAgain()
    {
        var sender = new FakeSender(2);
        Send(Own, 1, 1);
        _pending.OnNewLeader(2);
        _pending.OnLeadershipChanged(2);
        _pending.ResendMissing(sender);
        _pending.OnNewLeader(3);
        _pending.OnLeadershipChanged(3);
        Assert.True(_pending.IsHolding);
        Assert.Equal(1, _pending.Missing());
        sender.Term = 3;
        Assert.Equal(1, _pending.ResendMissing(sender));
        Assert.Equal(new List<int> { 1, 1 }, sender.Sent);
        Tap(Own, 1);
        Assert.Equal(0, _pending.Count);
    }

    [Fact(DisplayName = "a full ring resends in place, in order")]
    public void FullRingResendsInPlace()
    {
        var sender = new FakeSender(2);
        for (int n = 0; n < Capacity; n++)
        {
            Send(Own, 1, n);
        }
        _pending.OnNewLeader(2);
        _pending.OnLeadershipChanged(2);
        Assert.Equal(Capacity, _pending.ResendMissing(sender));
        Assert.Equal(new List<int> { 0, 1, 2, 3 }, sender.Sent);
        for (int n = 0; n < Capacity; n++)
        {
            Tap(Own, n);
        }
        Assert.Equal(0, _pending.Count);
        Assert.False(_pending.IsFaulted);
    }

    [Fact(DisplayName = "a lost session's frames are discarded, and one that committed comes back matching nothing")]
    public void DiscardedSessionsFramesMatchNothing()
    {
        Send(Own, 1, 1);
        Send(Own, 1, 2);
        _pending.OnNewLeader(2);
        Assert.True(_pending.IsHolding);
        Assert.Equal(2, _pending.DiscardUnconfirmed());
        Assert.Equal(0, _pending.Count);
        Assert.False(_pending.IsHolding);
        Tap(Own, 1);
        Send(Own + 1, 2, 3);
        Tap(Own + 1, 3);
        Assert.Equal(0, _pending.Count);
        Assert.False(_pending.IsFaulted);
    }

    // Tracks application frame n as placed on session, stamped term.
    private void Send(long session, long term, int n)
    {
        _body.PutInt(0, n);
        _pending.Track(_frame, _envelope.WrapPayload(_frame, 1, 2, session, PayloadId, _body, sizeof(int)), session,
                       term);
    }

    private void SendSystem(long session, long term, int eventType, int n)
    {
        _body.PutInt(0, n);
        _pending.Track(_frame, _envelope.Wrap(_frame, 1, 2, session, eventType, _body, sizeof(int)), session, term);
    }

    // Application frame n arriving on the tap from session.
    private void Tap(long session, int n)
    {
        Tap(session, false, PayloadId, n);
    }

    private void Tap(long session, bool system, int id, int n)
    {
        _body.PutInt(0, n);
        _pending.OnSequenced(SequencedEvents.Of(session, system, id, _bodyBytes));
    }
}
