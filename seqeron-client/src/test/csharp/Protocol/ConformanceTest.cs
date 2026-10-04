using System;
using System.Buffers.Binary;
using System.Text;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Org.SbeTool.Sbe.Dll;
using Xunit;
using Frame = Org.Limitless.Seqeron.Sbe.Frame;

namespace Org.Limitless.Seqeron.Protocol;

/// <summary>
/// The C# half of the protocol conformance suite, spec §14: the rows with a client implementation behind them,
/// as <c>ConformanceTest.cpp</c> has them. Rows 1, 4, 4a, 5, 8 and 9 drive the sequencer and live in the Java
/// suite alone. Row 4b, the producer's refusal, is tested here at <see cref="SystemFrame"/>, where it is made.
/// <para>Every frame is built a few lines above the assertion that reads it, and every expectation is a rule §4
/// states: the offsets of §4.1, the sizes of §4.2, the payloads of §7.</para>
/// </summary>
public class ConformanceTest
{
    private const int SourceId = Frames.SourceId;
    private const int ConnectionId = Frames.ConnectionId;
    private const long SessionId = Frames.SessionId;

    private readonly SequencedFrameDecoder _view = new SequencedFrameDecoder();

    // ── Row 2. The prefix property (F-3) ──────────────────────────────────────────

    [Fact]
    public void IngressCompositesAreThePrefixOfTheirSequencedCounterparts()
    {
        Assert.Equal(18, Frame.UnsequencedHeader.Size);
        Assert.Equal(18, Frame.UnsequencedSystemHeader.Size);
        Assert.Equal(34, Frame.SequencedHeader.Size);
        Assert.Equal(34, Frame.SequencedSystemHeader.Size);

        byte[] unsequenced = new byte[64], unsequencedSystem = new byte[64];
        byte[] sequenced = new byte[64], sequencedSystem = new byte[64];

        var a = new Frame.UnsequencedHeader();
        a.Wrap(new DirectBuffer(unsequenced), 0, 0);
        a.SourceId = SourceId;
        a.ConnectionId = ConnectionId;
        a.SessionId = SessionId;
        a.PayloadId = 2;

        var b = new Frame.UnsequencedSystemHeader();
        b.Wrap(new DirectBuffer(unsequencedSystem), 0, 0);
        b.SourceId = SourceId;
        b.ConnectionId = ConnectionId;
        b.SessionId = SessionId;
        b.SystemEventType = 2;

        // The field at offset 16 is a payloadId on one and a systemEventType on the other; written to the same
        // bits, the two composites are the same 18 bytes.
        Assert.Equal(unsequenced[..18], unsequencedSystem[..18]);

        var c = new Frame.SequencedHeader();
        c.Wrap(new DirectBuffer(sequenced), 0, 0);
        c.SourceId = SourceId;
        c.ConnectionId = ConnectionId;
        c.SessionId = SessionId;
        c.PayloadId = 2;
        c.GlobalSeqNo = 9;
        c.Timestamp = Frames.Timestamp;

        var d = new Frame.SequencedSystemHeader();
        d.Wrap(new DirectBuffer(sequencedSystem), 0, 0);
        d.SourceId = SourceId;
        d.ConnectionId = ConnectionId;
        d.SessionId = SessionId;
        d.SystemEventType = 2;
        d.GlobalSeqNo = 9;
        d.Timestamp = Frames.Timestamp;

        Assert.Equal(sequenced[..34], sequencedSystem[..34]);
        Assert.Equal(unsequenced[..18], sequenced[..18]); // the copy-18 of §9.5
        Assert.Equal(unsequencedSystem[..18], sequencedSystem[..18]);
    }

    [Fact]
    public void EveryFrameLayerFieldSitsAtTheOffsetSection41Gives()
    {
        // §4.1's table, field by field: a distinct value per field, read back at the offset the spec gives. The
        // prefix property says the composites agree with each other; this says they agree with the spec, which a
        // permutation of two same-width fields would pass above and fail here.
        byte[] bytes = new byte[64];
        var header = new Frame.SequencedHeader();
        header.Wrap(new DirectBuffer(bytes), 0, 0);
        header.SourceId = 0x01020304;
        header.ConnectionId = 0x05060708;
        header.SessionId = 0x1112131415161718;
        header.PayloadId = 0x2122;
        header.GlobalSeqNo = 0x3132333435363738;
        header.Timestamp = 0x4142434445464748;
        AssertSection41(bytes, 0x2122, sequenced: true);

        bytes = new byte[64];
        var system = new Frame.SequencedSystemHeader();
        system.Wrap(new DirectBuffer(bytes), 0, 0);
        system.SourceId = 0x01020304;
        system.ConnectionId = 0x05060708;
        system.SessionId = 0x1112131415161718;
        system.SystemEventType = 0x2122;
        system.GlobalSeqNo = 0x3132333435363738;
        system.Timestamp = 0x4142434445464748;
        AssertSection41(bytes, 0x2122, sequenced: true);

        bytes = new byte[64];
        var ingress = new Frame.UnsequencedHeader();
        ingress.Wrap(new DirectBuffer(bytes), 0, 0);
        ingress.SourceId = 0x01020304;
        ingress.ConnectionId = 0x05060708;
        ingress.SessionId = 0x1112131415161718;
        ingress.PayloadId = 0x2122;
        AssertSection41(bytes, 0x2122, sequenced: false);

        bytes = new byte[64];
        var ingressSystem = new Frame.UnsequencedSystemHeader();
        ingressSystem.Wrap(new DirectBuffer(bytes), 0, 0);
        ingressSystem.SourceId = 0x01020304;
        ingressSystem.ConnectionId = 0x05060708;
        ingressSystem.SessionId = 0x1112131415161718;
        ingressSystem.SystemEventType = 0x2122;
        AssertSection41(bytes, 0x2122, sequenced: false);
    }

    [Fact]
    public void FrameSizesAreSection42sTable()
    {
        Assert.Equal(8, Frame.MessageHeader.Size);
        Assert.Equal(2, Frame.Sequenced.PayloadHeaderSize); // the var-data length prefix
        Assert.Equal(2, Frame.SequencedSystem.BodyHeaderSize);

        // Fixed overhead: MessageHeader + the header composite + the length prefix.
        Assert.Equal(28, FrameLayer.MinIngressLength); // ingress, both families
        Assert.Equal(44, Frame.MessageHeader.Size + Frame.SequencedHeader.Size + Frame.Sequenced.PayloadHeaderSize);

        // A ClusterHeartbeat is a template of its own: no length prefix and no payload at all.
        Assert.Equal(42, Frames.ClusterHeartbeat(1).Length);   // 8 + 34, the cheapest frame in the system
        Assert.Equal(50, Frames.SnapshotStarted(1, 1).Length); // 8 + 34 + the round

        // §12's ceiling, as a frame on the wire.
        Assert.Equal(44 + FrameLayer.MaxPayloadLength, Frames.Payload(1, FrameLayer.MaxPayloadLength).Length);
        Assert.Equal(8912, FrameLayer.MaxIngressLength);
    }

    // ── Row 3. Every system message decodes to what it was built with (§7) ────────

    [Fact]
    public void EverySystemMessageNamesItsEventAndDecodesItsBody()
    {
        var body = new byte[256];

        var connectionOpened = new Frame.ConnectionOpened();
        connectionOpened.WrapForEncode(new DirectBuffer(body), 0);
        connectionOpened.SetConnectionData(Encoding.ASCII.GetBytes("GW-A/42"));
        byte[] frame = Frames.SystemEvent(1, SystemFrame.ConnectionOpened, body.AsSpan(0, connectionOpened.Size));
        Assert.True(Wrap(frame));
        Assert.True(_view.IsSystem);
        Assert.Equal(SystemFrame.ConnectionOpened, _view.SystemEventType);
        Assert.Equal(0, _view.BlockLength); // a system payload carries no declaration of its own (V-3)
        var connectionOpenedIn = Decode(frame, new Frame.ConnectionOpened(),
                                        (c, v, o) => c.WrapForDecode(v, o, Frame.ConnectionOpened.BlockLength,
                                                                     Frame.ConnectionOpened.SchemaVersion));
        var data = new byte[16];
        Assert.Equal(Encoding.ASCII.GetBytes("GW-A/42"),
                     data[..connectionOpenedIn.GetConnectionData(data, 0, data.Length)]);

        var clusterStarted = new Frame.ClusterStarted();
        clusterStarted.WrapForEncode(new DirectBuffer(body), 0);
        clusterStarted.CorrelationId = 0x0102030405060708;
        frame = Frames.SystemEvent(2, SystemFrame.ClusterStarted, body.AsSpan(0, clusterStarted.Size));
        Assert.True(Wrap(frame));
        Assert.Equal(0x0102030405060708, Decode(frame, new Frame.ClusterStarted(),
                                                (c, v, o) => c.WrapForDecode(v, o, Frame.ClusterStarted.BlockLength,
                                                                             Frame.ClusterStarted.SchemaVersion))
                                             .CorrelationId);

        var registered = new Frame.GatewayRegistered();
        registered.WrapForEncode(new DirectBuffer(body), 0);
        registered.Remaining = 0;
        registered.GatewayId = 3;
        registered.GatewaySourceId = 5;
        registered.PreferenceRank = 0;
        registered.SetGatewayName("EGW-A");
        frame = Frames.SystemEvent(3, SystemFrame.GatewayRegistered, body.AsSpan(0, registered.Size));
        Assert.True(Wrap(frame));
        Assert.Equal(SystemFrame.GatewayRegistered, _view.SystemEventType);
        var row = Decode(frame, new Frame.GatewayRegistered(),
                         (c, v, o) => c.WrapForDecode(v, o, Frame.GatewayRegistered.BlockLength,
                                                      Frame.GatewayRegistered.SchemaVersion));
        Assert.Equal(0, row.Remaining);
        Assert.Equal(3, row.GatewayId);
        Assert.Equal(5, row.GatewaySourceId);

        var started = new Frame.GatewayStarted();
        started.WrapForEncode(new DirectBuffer(body), 0);
        started.GatewayId = 3;
        started.FirstConnectionId = 1000;
        frame = Frames.SystemEvent(4, SystemFrame.GatewayStarted, body.AsSpan(0, started.Size));
        Assert.True(Wrap(frame));
        var start = Decode(
            frame, new Frame.GatewayStarted(),
            (c, v, o) => c.WrapForDecode(v, o, Frame.GatewayStarted.BlockLength, Frame.GatewayStarted.SchemaVersion));
        Assert.Equal(3, start.GatewayId);
        Assert.Equal(1000, start.FirstConnectionId);

        var end = new Frame.SnapshotEnd();
        end.WrapForEncode(new DirectBuffer(body), 0);
        end.Round = 3;
        end.RecordCount = 5;
        end.Length = 6000;
        end.Crc32c = 0xDEADBEEF;
        end.FormatVersion = 2;
        frame = Frames.SystemEvent(9, SystemFrame.SnapshotEnd, body.AsSpan(0, end.Size));
        Assert.True(Wrap(frame));
        var endIn =
            Decode(frame, new Frame.SnapshotEnd(),
                   (c, v, o) => c.WrapForDecode(v, o, Frame.SnapshotEnd.BlockLength, Frame.SnapshotEnd.SchemaVersion));
        Assert.Equal(5, endIn.RecordCount);
        Assert.Equal(6000, endIn.Length);
        Assert.Equal(0xDEADBEEF, endIn.Crc32c);
        Assert.Equal(2u, endIn.FormatVersion);

        // The four synthesized ones: fields inline in the frame's own block, and -1 marking the class (F-4). The
        // payload offset is the block's, so a decoder wraps there with its own compiled constants.
        Assert.True(Wrap(Frames.ClusterHeartbeat(5)));
        Assert.Equal(SystemFrame.ClusterHeartbeat, _view.SystemEventType);
        Assert.Equal(-1, _view.SourceId);
        Assert.Equal(-1, _view.ConnectionId);
        Assert.Equal(-1, _view.SourceSessionId);

        frame = Frames.LeadershipChanged(6, 2, 7);
        Assert.True(Wrap(frame));
        Assert.Equal(SystemFrame.LeadershipChanged, _view.SystemEventType);
        var leadership = Decode(frame, new Frame.LeadershipChanged(),
                                (c, v, o) => c.WrapForDecode(v, o, Frame.LeadershipChanged.BlockLength,
                                                             Frame.LeadershipChanged.SchemaVersion));
        Assert.Equal(2, leadership.NewLeaderMemberId);
        Assert.Equal(7, leadership.LeadershipTermId);

        frame = Frames.GatewayActive(7, 3);
        Assert.True(Wrap(frame));
        Assert.Equal(SystemFrame.GatewayActive, _view.SystemEventType);
        Assert.Equal(3, Decode(frame, new Frame.GatewayActive(),
                               (c, v, o) => c.WrapForDecode(v, o, Frame.GatewayActive.BlockLength,
                                                            Frame.GatewayActive.SchemaVersion))
                            .GatewayId);

        frame = Frames.SnapshotStarted(10, 4);
        Assert.True(Wrap(frame));
        Assert.Equal(SystemFrame.SnapshotStarted, _view.SystemEventType);
        Assert.Equal(4, Decode(frame, new Frame.SnapshotStarted(),
                               (c, v, o) => c.WrapForDecode(v, o, Frame.SnapshotStarted.BlockLength,
                                                            Frame.SnapshotStarted.SchemaVersion))
                            .Round);
    }

    // ── Row 4b. The producer refuses before the wire (T-3, §12) ───────────────────

    [Fact]
    public void WrapPayloadAdmitsTheCeilingAndRefusesOneMore()
    {
        var encoder = new SystemFrame();
        var frame = new UnsafeBuffer(new byte[FrameLayer.MaxIngressLength + 16]);
        var payload = new UnsafeBuffer(new byte[FrameLayer.MaxPayloadLength + 1]);

        Assert.Equal(FrameLayer.MaxIngressLength, encoder.WrapPayload(frame, SourceId, ConnectionId, SessionId, 2,
                                                                      payload, FrameLayer.MaxPayloadLength));

        var untouched = new UnsafeBuffer(new byte[FrameLayer.MaxIngressLength + 16]);
        Assert.Equal(SystemFrame.Refused, encoder.WrapPayload(untouched, SourceId, ConnectionId, SessionId, 2, payload,
                                                              FrameLayer.MaxPayloadLength + 1));
        Assert.All(untouched.ByteArray, b => Assert.Equal(0, b)); // nothing was encoded

        // Local and permanent, and the producer survives it: the very next well-formed frame encodes.
        Assert.Equal(FrameLayer.MinIngressLength + 8,
                     encoder.WrapPayload(frame, SourceId, ConnectionId, SessionId, 2, payload, 8));
    }

    [Fact]
    public void WrapRefusesABodyOverTheCeiling()
    {
        var encoder = new SystemFrame();
        var frame = new UnsafeBuffer(new byte[FrameLayer.MaxIngressLength + 16]);
        var body = new UnsafeBuffer(new byte[FrameLayer.MaxPayloadLength + 1]);

        Assert.Equal(FrameLayer.MaxIngressLength,
                     encoder.Wrap(frame, SourceId, ConnectionId, SessionId, SystemFrame.ConnectionOpened, body,
                                  FrameLayer.MaxPayloadLength));
        Assert.Equal(SystemFrame.Refused,
                     encoder.Wrap(frame, SourceId, ConnectionId, SessionId, SystemFrame.ConnectionOpened, body,
                                  FrameLayer.MaxPayloadLength + 1));
    }

    [Fact]
    public void FramesTheSequencerWouldRejectAreRefused()
    {
        // §9.2 conditions 6 to 9: what a producer can check without the sequencer's state.
        var encoder = new SystemFrame();
        var frame = new UnsafeBuffer(new byte[256]);
        var body = new UnsafeBuffer(new byte[64]);

        Assert.Equal(SystemFrame.Refused, encoder.WrapPayload(frame, -1, ConnectionId, SessionId, 2, body, 8));
        Assert.Equal(SystemFrame.Refused, encoder.WrapPayload(frame, SourceId, ConnectionId, SessionId, 0, body, 8));
        Assert.Equal(SystemFrame.Refused, encoder.WrapPayload(frame, SourceId, ConnectionId, SessionId, 1, body, 8));

        Assert.Equal(SystemFrame.Refused, encoder.Wrap(frame, -1, ConnectionId, SessionId, SystemFrame.ClusterStarted,
                                                       body, Frame.ClusterStarted.BlockLength));
        foreach (int synthesized in new[] { SystemFrame.LeadershipChanged, SystemFrame.ClusterHeartbeat,
                                            SystemFrame.GatewayActive, SystemFrame.SnapshotStarted, 999 })
        {
            Assert.Equal(SystemFrame.Refused, encoder.Wrap(frame, SourceId, ConnectionId, SessionId, synthesized, body,
                                                           Frame.ClusterStarted.BlockLength));
        }
        Assert.Equal(SystemFrame.Refused,
                     encoder.Wrap(frame, SourceId, ConnectionId, SessionId, SystemFrame.GatewayStarted, body,
                                  Frame.GatewayStarted.BlockLength - 1));
        Assert.All(frame.ByteArray, b => Assert.Equal(0, b));
    }

    [Fact]
    public void AnIngressFrameIsTheSpecsLayout()
    {
        // What WrapPayload and Wrap put on the wire, byte by byte against §4: the frame the sequencer validates.
        var encoder = new SystemFrame();
        var buffer = new UnsafeBuffer(new byte[64]);
        int length =
            encoder.WrapPayload(buffer, SourceId, ConnectionId, SessionId, 2, new UnsafeBuffer(Frames.Filler(5)), 5);
        byte[] frame = buffer.ByteArray[..length];
        Assert.Equal(FrameLayer.MinIngressLength + 5, length);
        AssertIngressFrame(frame, Frame.Unsequenced.TemplateId, 2, Frames.Filler(5));

        var clusterStarted = new byte[Frame.ClusterStarted.BlockLength];
        BinaryPrimitives.WriteInt64LittleEndian(clusterStarted, 0x0102030405060708);
        length = encoder.Wrap(buffer, SourceId, ConnectionId, SessionId, SystemFrame.ClusterStarted,
                              new UnsafeBuffer(clusterStarted), clusterStarted.Length);
        AssertIngressFrame(buffer.ByteArray[..length], Frame.UnsequencedSystem.TemplateId, SystemFrame.ClusterStarted,
                           clusterStarted);
    }

    [Fact]
    public void AnExpandableFrameGrowsToTheFrame()
    {
        IMutableDirectBuffer frame = new ExpandableArrayBuffer(16);
        int length = new SystemFrame().WrapPayload(frame, SourceId, ConnectionId, SessionId, 2,
                                                   new UnsafeBuffer(new byte[1000]), 1000);
        Assert.Equal(FrameLayer.MinIngressLength + 1000, length);
        Assert.True(frame.Capacity >= length);
    }

    // ── Row 6. The boundary payload sizes (§12) ───────────────────────────────────

    [Fact]
    public void TheBoundaryPayloadSizesCrossIntact()
    {
        // Empty, one byte, and the ceiling. A frame carrying no payload is legal (§5) and must still reach the
        // consumer, or P-3's continuity read sees a gap that is not there.
        foreach (int payloadLength in new[] { 0, 1, FrameLayer.MaxPayloadLength })
        {
            byte[] frame = Frames.Payload(11, payloadLength);
            Assert.True(Wrap(frame), $"payload of {payloadLength} bytes");
            Assert.False(_view.IsSystem);
            Assert.Equal(2, _view.PayloadId);
            Assert.Equal(payloadLength, _view.PayloadLength);
            Assert.Equal(44 + payloadLength, frame.Length); // 44 + the payload (§4.2)
        }
    }

    // ── Row 7. Selective consumption (P-1 to P-3) ─────────────────────────────────

    [Fact]
    public void UnknownPayloadsAndEventsAreReadableSoContinuityHolds()
    {
        // globalSeqNo sits at 18 on all six sequenced messages (F-3), so the continuity read is branch-free over
        // frames the consumer comprehends none of. A frame dropped here is a globalSeqNo missing from that read,
        // which reads as a gap that is not there.
        var body = new byte[64];
        var registered = new Frame.PayloadIdRegistered();
        registered.WrapForEncode(new DirectBuffer(body), 0);
        registered.PayloadId = 4;
        registered.ProtocolVersion = 1;
        registered.SetProtocolName("basicdata");

        byte[][] tap = {
            Frames.Payload(1, 8, payloadId: 4095),
            Frames.SystemEvent(2, SystemFrame.PayloadIdRegistered, body.AsSpan(0, registered.Size)),
            Frames.ClusterHeartbeat(3),
            Frames.LeadershipChanged(4, 2, 1),
            Frames.GatewayActive(5, 3),
            Frames.SnapshotStarted(6, 1),
        };
        long expected = 1;
        foreach (byte[] frame in tap)
        {
            Assert.True(Wrap(frame), $"frame {expected} is unreadable, so P-3 would lose its globalSeqNo");
            Assert.Equal(expected, _view.GlobalSeqNo);
            expected++;
        }

        // An unallocated payloadId is data like any other: the frame is read, and nothing dispatches on it.
        byte[] unallocated = Frames.Payload(77, 0, payloadId: 4095);
        Assert.True(Wrap(unallocated), "an empty payload under an unallocated payloadId is still a frame");
        Assert.Equal(4095, _view.PayloadId);
        Assert.Equal(77, _view.GlobalSeqNo);
        Assert.Equal(0, _view.TemplateId); // no inner declaration, so nothing can dispatch on it (P-1)
    }

    private bool Wrap(byte[] frame)
    {
        return _view.Wrap(new UnsafeBuffer(frame), 0, frame.Length);
    }

    // A system payload is decoded with the codec's own compiled constants, wrapped where the view says it is.
    private T Decode<T>(byte[] frame, T codec, Action<T, DirectBuffer, int> wrapForDecode)
    {
        Assert.True(Wrap(frame));
        wrapForDecode(codec, new DirectBuffer(frame), _view.PayloadOffset);
        return codec;
    }

    private static void AssertSection41(byte[] bytes, ushort atSixteen, bool sequenced)
    {
        Assert.Equal(0x01020304, BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(0)));
        Assert.Equal(0x05060708, BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(4)));
        Assert.Equal(0x1112131415161718, BinaryPrimitives.ReadInt64LittleEndian(bytes.AsSpan(8)));
        Assert.Equal(atSixteen, BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(16)));
        if (sequenced)
        {
            Assert.Equal(0x3132333435363738, BinaryPrimitives.ReadInt64LittleEndian(bytes.AsSpan(18)));
            Assert.Equal(0x4142434445464748, BinaryPrimitives.ReadInt64LittleEndian(bytes.AsSpan(26)));
        }
    }

    private static void AssertIngressFrame(byte[] frame, int templateId, int atSixteen, byte[] payload)
    {
        Assert.Equal(18, BinaryPrimitives.ReadUInt16LittleEndian(frame.AsSpan(0))); // blockLength, §9.2 condition 4
        Assert.Equal(templateId, BinaryPrimitives.ReadUInt16LittleEndian(frame.AsSpan(2)));
        Assert.Equal(210, BinaryPrimitives.ReadUInt16LittleEndian(frame.AsSpan(4)));
        Assert.Equal(0, BinaryPrimitives.ReadUInt16LittleEndian(frame.AsSpan(6)));
        Assert.Equal(SourceId, BinaryPrimitives.ReadInt32LittleEndian(frame.AsSpan(8)));
        Assert.Equal(ConnectionId, BinaryPrimitives.ReadInt32LittleEndian(frame.AsSpan(12)));
        Assert.Equal(SessionId, BinaryPrimitives.ReadInt64LittleEndian(frame.AsSpan(16)));
        Assert.Equal(atSixteen, BinaryPrimitives.ReadUInt16LittleEndian(frame.AsSpan(24)));
        Assert.Equal(payload.Length, BinaryPrimitives.ReadUInt16LittleEndian(frame.AsSpan(26)));
        Assert.Equal(payload, frame[28..]);
        Assert.Equal(FrameLayer.MinIngressLength + payload.Length, frame.Length); // §9.2 condition 5
    }
}
