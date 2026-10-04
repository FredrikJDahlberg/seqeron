using Adaptive.Agrona;
using Org.Limitless.Seqeron.Replayer.Client;
using Frame = Org.Limitless.Seqeron.Sbe.Frame;

namespace Org.Limitless.Seqeron.App;

/// <summary>
/// One application payload delivered in order, with the frame layer off it: the envelope is stripped, and
/// <see cref="BodyOffset"/> strips the payload's own <c>MessageHeader</c> too, which is where an SBE decoder wraps.
/// A payload that carries no header (spec §13.2) is addressed by <see cref="PayloadOffset"/> instead.
/// <para>Nothing of seqeron's own vocabulary reaches here — the system family is the façade's business, and a frame
/// of it never arrives as a payload. What is left is the payload's identity in the total order
/// (<see cref="GlobalSeqNo"/>), who sent it and on whose behalf (<see cref="SourceId"/>,
/// <see cref="ConnectionId"/>), the consensus clock it was stamped with, and the bytes.</para>
/// <para>Dispatch on <c>(PayloadId, TemplateId)</c>, never <c>TemplateId</c> alone: template ids are unique per
/// schema, so two applications' templates can collide.</para>
/// <para>A flyweight: valid only during the handler call; copy anything that must outlive it.</para>
/// </summary>
public sealed class Payload
{
    private SequencedEvent _event;

    internal Payload()
    {
    }

    internal void Wrap(SequencedEvent sequencedEvent)
    {
        _event = sequencedEvent;
    }

    /// <summary>Cluster-wide monotone sequence number; increments by exactly one per frame.</summary>
    public long GlobalSeqNo => _event.GlobalSeqNo;

    /// <summary>The producer that submitted this frame (spec §5).</summary>
    public int SourceId => _event.SourceId;

    /// <summary>The producer's connection this frame belongs to, or -1 for a producer-scoped one.</summary>
    public int ConnectionId => _event.ConnectionId;

    /// <summary>The cluster session it was submitted on; a gateway pair shares its <c>SourceId</c> but not
    /// this.</summary>
    public long SourceSessionId => _event.SourceSessionId;

    /// <summary>The Raft consensus timestamp, identical on every node.</summary>
    public long ClusterTimestampNs => _event.ClusterTimestampNs;

    /// <summary>When this process read it — a delivery stamp, not the frame's.</summary>
    public long ReceiveTimeNs => _event.ReceiveTimeNs;

    /// <summary>Where this frame's first byte sits in the node's recording — a delivery stamp too, and what a
    /// consumer that replays that recording itself (a FIX gateway serving its own resend) anchors on.</summary>
    public long Position => _event.Position;

    /// <summary>Which protocol the body speaks (spec §13).</summary>
    public int PayloadId => _event.PayloadId;

    /// <summary>The body's own template within that protocol.</summary>
    public int TemplateId => _event.TemplateId;

    /// <summary>For the decoder's wrap, from the payload's own <c>MessageHeader</c>.</summary>
    public int BlockLength => _event.BlockLength;

    /// <summary>For the decoder's wrap, from the payload's own <c>MessageHeader</c>.</summary>
    public int Version => _event.Version;

    /// <summary>The buffer the body sits in.</summary>
    public IDirectBuffer Buffer => _event.Buffer;

    /// <summary>Where the payload starts: past the envelope, its own <c>MessageHeader</c> included if it has
    /// one.</summary>
    public int PayloadOffset => _event.PayloadOffset;

    /// <summary>The payload's length from <see cref="PayloadOffset"/>.</summary>
    public int PayloadLength => _event.PayloadLength;

    /// <summary>Where the body starts: past the envelope and past the payload's own <c>MessageHeader</c>.</summary>
    public int BodyOffset => _event.PayloadOffset + Frame.MessageHeader.Size;

    /// <summary>The body's length from <see cref="BodyOffset"/>.</summary>
    public int BodyLength => _event.PayloadLength - Frame.MessageHeader.Size;
}
