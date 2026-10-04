using System;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Org.Limitless.Seqeron.Protocol;
using Org.Limitless.Seqeron.Replayer.Client;
using Org.Limitless.Seqeron.Util;
using Frame = Org.Limitless.Seqeron.Sbe.Frame;
using SbeBuffer = Org.SbeTool.Sbe.Dll.DirectBuffer;

namespace Org.Limitless.Seqeron.Sequencer.Client;

/// <summary>
/// A producer's ingress frames that its own tap has not yet shown, which of them a leader change lost, and their
/// resend ahead of anything new.
/// <para><b>Why a count is enough.</b> A leader drops ingress stamped with any term but its own, and every election
/// discards the ingress the old leader had not read. So a frame stamped term T is either on the tap before the first
/// <c>LeadershipChanged</c> with a term above T, or lost, and the lost ones are the newest frames stamped T. Once
/// that boundary has passed, every frame still pending with a term below it is <see cref="Missing"/>. The same holds
/// when the same member wins the new term.</para>
/// <para><b>Resent in order, never twice.</b> A committed frame is never missing, so nothing is duplicated. From a
/// newer term's first sign — the sender's <c>NewLeader</c> or the tap's <c>LeadershipChanged</c>, whichever comes
/// first — until every older frame is seen or resent, <see cref="IsHolding"/>: send nothing new, and give this to
/// the sender with <c>SetIngressHold</c>, so a send already spinning through the election is given up rather than
/// landing ahead of the resend. <see cref="ResendMissing"/> does the resend. Given to an
/// <see cref="IngressPublisher"/>, this is tracked and gated on without the producer handling a frame.</para>
/// <para><b>Own frames are matched by session, not <c>sourceId</c></b>: a gateway pair shares its <c>sourceId</c>,
/// and cluster session ids are never reused. Each frame records the session it went out on, so a session replaced
/// mid-term (the sender's reconnect) still matches its earlier frames. An own frame must equal the oldest pending
/// copy; anything else latches <see cref="IsFaulted"/>, because the order the count relies on no longer holds — most
/// often the sequencer rejected a frame (S-7), so check §9.2 before sending. Tracking into a full ring latches it
/// too: an untracked send breaks the count.</para>
/// <para>Limits: it lasts only as long as the process, so a promoted standby starts with nothing pending; it needs
/// the producer to follow its own tap; a lost session stays terminal; loss with no leader change has no boundary and
/// is not detected. <c>PendingSends.java</c> and <c>sequencer/client/PendingSends.hpp</c> are its twins; keep the
/// three in step.</para>
/// </summary>
public sealed class PendingSends : IIngressTracker
{
    private const int SlotLength = FrameLayer.MaxIngressLength;
    private const int NoTerm = -1;

    private readonly int _capacity;
    private readonly UnsafeBuffer _frames;
    private readonly int[] _lengths;
    private readonly long[] _sessionIds;
    private readonly long[] _termIds;
    private readonly SbeBuffer _view = new SbeBuffer();
    private readonly Frame.MessageHeader _messageHeader = new Frame.MessageHeader();
    private readonly Frame.UnsequencedHeader _frameHeader = new Frame.UnsequencedHeader();
    private readonly UnsafeBuffer _resendView = new UnsafeBuffer();
    private int _head;
    private int _size;
    private long _closedTermId = NoTerm;
    private long _newLeaderTermId = NoTerm;
    private bool _faulted;

    /// <summary>A tracker holding nothing, for one producer's own sends.</summary>
    /// <param name="capacity">frames that may be pending at once; each holds one
    /// <see cref="FrameLayer.MaxIngressLength"/> copy</param>
    public PendingSends(int capacity)
    {
        _capacity = capacity;
        _frames = new UnsafeBuffer(GC.AllocateArray<byte>(capacity * SlotLength, true));
        _lengths = new int[capacity];
        _sessionIds = new long[capacity];
        _termIds = new long[capacity];
    }

    /// <summary>Check before sending: a frame that cannot be tracked must not be sent.</summary>
    public bool IsFull => _size == _capacity;

    /// <summary>A frame the sender placed, read straight after its <c>Send</c> returned true.</summary>
    /// <param name="frame">the frame, from offset 0</param>
    /// <param name="length">its length in bytes</param>
    /// <param name="clusterSessionId">the sender's <c>ClusterSessionId</c></param>
    /// <param name="leadershipTermId">the sender's <c>LeadershipTermId</c></param>
    public void Track(IDirectBuffer frame, int length, long clusterSessionId, long leadershipTermId)
    {
        if (IsFull)
        {
            _faulted = true;
            return;
        }
        int slot = Slot(_size);
        _frames.PutBytes(slot * SlotLength, frame, 0, length);
        _lengths[slot] = length;
        _sessionIds[slot] = clusterSessionId;
        _termIds[slot] = leadershipTermId;
        _size++;
    }

    /// <summary>From the stream client's leadership callback: closes the count on every earlier term.</summary>
    /// <param name="leadershipTermId">the <c>LeadershipChanged</c>'s term</param>
    public void OnLeadershipChanged(long leadershipTermId)
    {
        _closedTermId = Math.Max(_closedTermId, leadershipTermId);
    }

    /// <summary>From the sender, which calls it on every <c>NewLeader</c>.</summary>
    /// <param name="leadershipTermId">the new leader's term</param>
    public void OnNewLeader(long leadershipTermId)
    {
        _newLeaderTermId = Math.Max(_newLeaderTermId, leadershipTermId);
    }

    /// <summary>Send nothing new while this holds: an older term's frames are still unseen or unresent.</summary>
    public bool IsHolding => _size > 0 && _termIds[_head] < Math.Max(_newLeaderTermId, _closedTermId);

    /// <summary>Resends the missing frames oldest first, each going back to the end of the ring as pending under the
    /// term it now carries. Stops at the first send that fails, so a later call picks up where this left
    /// off.</summary>
    /// <param name="sender">the session to resend on</param>
    /// <returns>how many were resent</returns>
    public int ResendMissing(IIngressSender sender)
    {
        if (sender.LeadershipTermId < _closedTermId)
        {
            return 0; // the sender has no NewLeader yet, and the leader would drop them again
        }
        int resent = 0;
        while (_size > 0 && _termIds[_head] < _closedTermId)
        {
            _resendView.Wrap(_frames, _head * SlotLength, _lengths[_head]);
            if (!sender.Send(_resendView, _lengths[_head]))
            {
                break;
            }
            MoveHeadToTail(sender.ClusterSessionId, sender.LeadershipTermId);
            resent++;
        }
        return resent;
    }

    /// <summary>Every frame off the tap, in order. Only this producer's own frames change anything.</summary>
    /// <param name="sequencedEvent">the frame</param>
    public void OnSequenced(SequencedEvent sequencedEvent)
    {
        int index = Missing();
        if (index == _size)
        {
            return;
        }
        int slot = Slot(index);
        if (sequencedEvent.SourceSessionId != _sessionIds[slot])
        {
            return;
        }
        if (!Matches(slot, sequencedEvent))
        {
            _faulted = true;
            return;
        }
        Remove(index);
    }

    /// <summary>Pending frames a leader change has lost, oldest first at the front.</summary>
    public int Missing()
    {
        int count = 0;
        while (count < _size && _termIds[Slot(count)] < _closedTermId)
        {
            count++;
        }
        return count;
    }

    /// <summary>Frames tracked and not yet seen on the tap, missing ones included.</summary>
    public int Count => _size;

    /// <summary>Latched: the count can no longer be trusted, so fence the producer.</summary>
    public bool IsFaulted => _faulted;

    private int Slot(int index)
    {
        return (_head + index) % _capacity;
    }

    // The two families' headers share their layout (F-3), so one decoder reads either one's id at offset 16.
    private bool Matches(int slot, SequencedEvent sequencedEvent)
    {
        int frameBase = slot * SlotLength;
        SbeBuffers.Wrap(_view, _frames, frameBase, _lengths[slot]);
        _messageHeader.Wrap(_view, 0, Frame.MessageHeader.SbeSchemaVersion);
        bool system = _messageHeader.TemplateId == Frame.UnsequencedSystem.TemplateId;
        _frameHeader.Wrap(_view, Frame.MessageHeader.Size, Frame.MessageHeader.SbeSchemaVersion);
        int id = _frameHeader.PayloadId;
        int bodyLength = _lengths[slot] - FrameLayer.MinIngressLength;
        if (sequencedEvent.IsSystem != system ||
            (system ? sequencedEvent.SystemEventType : sequencedEvent.PayloadId) != id ||
            sequencedEvent.PayloadLength != bodyLength)
        {
            return false;
        }
        return SbeBuffers.Bytes(_frames, frameBase + FrameLayer.MinIngressLength, bodyLength)
            .SequenceEqual(SbeBuffers.Bytes(sequencedEvent.Buffer, sequencedEvent.PayloadOffset, bodyLength));
    }

    private void MoveHeadToTail(long clusterSessionId, long leadershipTermId)
    {
        int from = _head;
        int to = Slot(_size);
        if (to != from)
        {
            _frames.PutBytes(to * SlotLength, _frames, from * SlotLength, _lengths[from]);
            _lengths[to] = _lengths[from];
        }
        _sessionIds[to] = clusterSessionId;
        _termIds[to] = leadershipTermId;
        _head = Slot(1);
    }

    // Drops the entry at index, moving the missing ones ahead of it up one slot to keep them in order.
    private void Remove(int index)
    {
        for (int i = index; i > 0; i--)
        {
            int to = Slot(i);
            int from = Slot(i - 1);
            _frames.PutBytes(to * SlotLength, _frames, from * SlotLength, _lengths[from]);
            _lengths[to] = _lengths[from];
            _sessionIds[to] = _sessionIds[from];
            _termIds[to] = _termIds[from];
        }
        _head = Slot(1);
        _size--;
    }
}
