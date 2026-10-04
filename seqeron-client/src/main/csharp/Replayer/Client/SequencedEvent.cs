using Org.Limitless.Seqeron.Protocol;

namespace Org.Limitless.Seqeron.Replayer.Client;

/// <summary>
/// One frame delivered in order off the sequenced stream, live or replayed; <c>SequencedEvent.java</c> and the C++
/// <c>SequencedEvent</c> are its twins. The envelope is stripped: split on
/// <see cref="SequencedFrameDecoder.IsSystem"/>, then dispatch on <c>(PayloadId, TemplateId)</c> or
/// <see cref="SequencedFrameDecoder.SystemEventType"/>. Every system frame but <c>LeadershipChanged</c>, which has
/// its own callback, arrives here.
/// <para>A flyweight: the buffer and every field are valid only during the handler call; copy to keep.</para>
/// </summary>
public sealed class SequencedEvent : SequencedFrameDecoder
{
    internal SequencedEvent()
    {
    }

    /// <summary>Wall-clock ns at receipt by this client.</summary>
    public long ReceiveTimeNs { get; private set; }

    /// <summary>Recording/stream position of this frame's first byte: what a resumed replay is anchored on.</summary>
    public long Position { get; private set; }

    internal void Set(long receiveTimeNs, long position)
    {
        ReceiveTimeNs = receiveTimeNs;
        Position = position;
    }
}
