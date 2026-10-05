using System;
using Org.SbeTool.Sbe.Dll;
using Frame = Org.Limitless.Seqeron.Sbe.Frame;

namespace Org.Limitless.Seqeron.Protocol;

// Sequenced frames as the sequencer writes them onto the tap, built with the generated encoders.
internal static class Frames
{
    public const int SourceId = 7;
    public const int ConnectionId = 42;
    public const long SessionId = 0x5EE5_1000L;
    public const long Timestamp = 1_700_000_000_000_000_000L;

    // One application frame carrying `payloadLength` bytes the cluster tier never opens (S-2).
    public static byte[] Payload(long globalSeqNo, int payloadLength, int payloadId = 2)
    {
        var bytes = new byte[payloadLength + 64];
        var encoder = new Frame.Sequenced();
        encoder.WrapForEncodeAndApplyHeader(new DirectBuffer(bytes), 0, new Frame.MessageHeader());
        Frame.SequencedHeader header = encoder.Header;
        header.SourceId = SourceId;
        header.ConnectionId = ConnectionId;
        header.SessionId = SessionId;
        header.PayloadId = (ushort)payloadId;
        header.GlobalSeqNo = globalSeqNo;
        header.Timestamp = Timestamp;
        encoder.SetPayload(Filler(payloadLength));
        return bytes[..(Frame.MessageHeader.Size + encoder.Size)];
    }

    // One SequencedSystem frame whose payload is `bodyLength` filler bytes.
    public static byte[] SystemEvent(long globalSeqNo, int systemEventType, int bodyLength)
    {
        return SystemEvent(globalSeqNo, systemEventType, Filler(bodyLength));
    }

    // One SequencedSystem frame around an encoded body.
    public static byte[] SystemEvent(long globalSeqNo, int systemEventType, ReadOnlySpan<byte> body)
    {
        var bytes = new byte[body.Length + 64];
        var encoder = new Frame.SequencedSystem();
        encoder.WrapForEncodeAndApplyHeader(new DirectBuffer(bytes), 0, new Frame.MessageHeader());
        Frame.SequencedSystemHeader header = encoder.Header;
        header.SourceId = SourceId;
        header.ConnectionId = ConnectionId;
        header.SessionId = SessionId;
        header.SystemEventType = (ushort)systemEventType;
        header.GlobalSeqNo = globalSeqNo;
        header.Timestamp = Timestamp;
        encoder.SetBody(body);
        return bytes[..(Frame.MessageHeader.Size + encoder.Size)];
    }

    // The four the sequencer synthesizes: their own templates, fields inline, -1 identity (F-4).
    public static byte[] ClusterHeartbeat(long globalSeqNo)
    {
        var bytes = new byte[128];
        var encoder = new Frame.ClusterHeartbeat();
        encoder.WrapForEncodeAndApplyHeader(new DirectBuffer(bytes), 0, new Frame.MessageHeader());
        Synthesized(encoder.Header, globalSeqNo, SystemFrame.ClusterHeartbeat);
        return bytes[..(Frame.MessageHeader.Size + encoder.Size)];
    }

    public static byte[] LeadershipChanged(long globalSeqNo, int newLeaderMemberId, long leadershipTermId)
    {
        var bytes = new byte[128];
        var encoder = new Frame.LeadershipChanged();
        encoder.WrapForEncodeAndApplyHeader(new DirectBuffer(bytes), 0, new Frame.MessageHeader());
        Synthesized(encoder.Header, globalSeqNo, SystemFrame.LeadershipChanged);
        encoder.NewLeaderMemberId = newLeaderMemberId;
        encoder.LeadershipTermId = leadershipTermId;
        return bytes[..(Frame.MessageHeader.Size + encoder.Size)];
    }

    public static byte[] GatewayActive(long globalSeqNo, int gatewayId)
    {
        var bytes = new byte[128];
        var encoder = new Frame.GatewayActive();
        encoder.WrapForEncodeAndApplyHeader(new DirectBuffer(bytes), 0, new Frame.MessageHeader());
        Synthesized(encoder.Header, globalSeqNo, SystemFrame.GatewayActive);
        encoder.GatewayId = gatewayId;
        return bytes[..(Frame.MessageHeader.Size + encoder.Size)];
    }

    public static byte[] SnapshotStarted(long globalSeqNo, long round)
    {
        var bytes = new byte[128];
        var encoder = new Frame.SnapshotStarted();
        encoder.WrapForEncodeAndApplyHeader(new DirectBuffer(bytes), 0, new Frame.MessageHeader());
        Synthesized(encoder.Header, globalSeqNo, SystemFrame.SnapshotStarted);
        encoder.Round = round;
        return bytes[..(Frame.MessageHeader.Size + encoder.Size)];
    }

    public static byte[] Filler(int length)
    {
        var bytes = new byte[length];
        for (int i = 0; i < length; i++)
        {
            bytes[i] = (byte)(i * 31 + 7);
        }
        return bytes;
    }

    private static void Synthesized(Frame.SequencedSystemHeader header, long globalSeqNo, int systemEventType)
    {
        header.SourceId = -1;
        header.ConnectionId = -1;
        header.SessionId = -1;
        header.SystemEventType = (ushort)systemEventType;
        header.GlobalSeqNo = globalSeqNo;
        header.Timestamp = Timestamp;
    }
}
