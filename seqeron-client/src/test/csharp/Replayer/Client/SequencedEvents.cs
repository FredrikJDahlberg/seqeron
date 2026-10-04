using System;
using Adaptive.Agrona.Concurrent;
using Org.SbeTool.Sbe.Dll;
using Frame = Org.Limitless.Seqeron.Sbe.Frame;

namespace Org.Limitless.Seqeron.Replayer.Client;

/// <summary>
/// Builds a <see cref="SequencedEvent"/> for the tests of what consumes one. An event is a view over a real tap
/// frame, so this encodes one rather than setting fields: the same bytes the sequencer would emit, read back
/// through the same decoder.
/// </summary>
internal static class SequencedEvents
{
    /// <param name="sourceSessionId">the frame's session</param>
    /// <param name="system">whether it is of the system family</param>
    /// <param name="id">a <c>payloadId</c>, or a <c>systemEventType</c> when <paramref name="system"/></param>
    /// <param name="body">the payload</param>
    public static SequencedEvent Of(long sourceSessionId, bool system, int id, ReadOnlySpan<byte> body)
    {
        var frame = new byte[body.Length + 64];
        int frameLength =
            system ? EncodeSystem(frame, sourceSessionId, id, body) : EncodePayload(frame, sourceSessionId, id, body);

        var sequencedEvent = new SequencedEvent();
        if (!sequencedEvent.Wrap(new UnsafeBuffer(frame), 0, frameLength))
        {
            throw new InvalidOperationException("encoded frame did not read back as one");
        }
        sequencedEvent.Set(0, 0);
        return sequencedEvent;
    }

    private static int EncodePayload(byte[] frame, long sourceSessionId, int payloadId, ReadOnlySpan<byte> body)
    {
        var sequenced = new Frame.Sequenced();
        sequenced.WrapForEncodeAndApplyHeader(new DirectBuffer(frame), 0, new Frame.MessageHeader());
        Frame.SequencedHeader header = sequenced.Header;
        header.SourceId = 0;
        header.ConnectionId = 0;
        header.SessionId = sourceSessionId;
        header.PayloadId = (ushort)payloadId;
        header.GlobalSeqNo = 0;
        header.Timestamp = 0;
        sequenced.SetPayload(body);
        return Frame.MessageHeader.Size + sequenced.Size;
    }

    private static int EncodeSystem(byte[] frame, long sourceSessionId, int systemEventType, ReadOnlySpan<byte> body)
    {
        var sequenced = new Frame.SequencedSystem();
        sequenced.WrapForEncodeAndApplyHeader(new DirectBuffer(frame), 0, new Frame.MessageHeader());
        Frame.SequencedSystemHeader header = sequenced.Header;
        header.SourceId = 0;
        header.ConnectionId = 0;
        header.SessionId = sourceSessionId;
        header.SystemEventType = (ushort)systemEventType;
        header.GlobalSeqNo = 0;
        header.Timestamp = 0;
        sequenced.SetBody(body);
        return Frame.MessageHeader.Size + sequenced.Size;
    }
}
