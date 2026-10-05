using System;
using System.Collections.Generic;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Org.Limitless.Seqeron.Protocol;
using Xunit;
using Frame = Org.Limitless.Seqeron.Sbe.Frame;
using SbeBuffer = Org.SbeTool.Sbe.Dll.DirectBuffer;

namespace Org.Limitless.Seqeron.Sequencer.Client;

/// <summary>
/// Unit tests for the encode-and-offer preamble every producer writes. No Aeron runtime: what the sender does with a
/// frame is a boolean, and the frames themselves are read back with the same decoder a consumer uses — which is what
/// makes the round trip here worth asserting at all.
/// <para>The C++ twin's coverage of this sits in <c>ClusterStreamSenderTest</c>, which drives the session handshake
/// through fake transports. That machine is <c>AeronCluster</c>'s on this side, so what is left to test is the part
/// seqeron still owns: the frame, and the three-valued outcome. Case for case with
/// <c>IngressPublisherTest.java</c>.</para>
/// </summary>
public class IngressPublisherTest
{
    private const int SourceId = 10;
    private const int ConnectionId = 7;
    private const long SessionId = 4242;
    private const int PayloadId = 6;

    // Records what it was handed, and answers whatever the test told it to.
    private sealed class FakeSender : IIngressSender
    {
        public readonly byte[] Sent = new byte[FrameLayer.MaxIngressLength];
        public bool Accept = true;
        public int Length;
        public int Calls;

        public bool Send(IDirectBuffer frame, int length)
        {
            Calls++;
            Length = length;
            frame.GetBytes(0, Sent, 0, length);
            return Accept;
        }

        public long ClusterSessionId => SessionId;

        public long LeadershipTermId => 1;
    }

    private sealed record TrackedFrame(int Length, long ClusterSessionId, long LeadershipTermId);

    // Answers whatever the test told it to, and records each frame it is asked to track.
    private sealed class FakeTracker : IIngressTracker
    {
        public readonly List<TrackedFrame> Tracked = new List<TrackedFrame>();
        public bool Holding;
        public bool Full;

        public void OnNewLeader(long leadershipTermId)
        {
        }

        public bool IsHolding => Holding;

        public bool IsFull => Full;

        public void Track(IDirectBuffer frame, int length, long clusterSessionId, long leadershipTermId)
        {
            Tracked.Add(new TrackedFrame(length, clusterSessionId, leadershipTermId));
        }
    }

    private readonly FakeSender _sender = new FakeSender();
    private readonly FakeTracker _tracker = new FakeTracker();
    private readonly IngressPublisher _tracked;
    private readonly IngressPublisher _publisher = new IngressPublisher();
    private readonly Frame.MessageHeader _messageHeader = new Frame.MessageHeader();
    private readonly Frame.Unsequenced _unsequenced = new Frame.Unsequenced();
    private readonly Frame.UnsequencedSystem _unsequencedSystem = new Frame.UnsequencedSystem();

    public IngressPublisherTest()
    {
        _tracked = new IngressPublisher(_tracker);
    }

    private static UnsafeBuffer Payload(int length)
    {
        var buffer = new UnsafeBuffer(new byte[Math.Max(length, 1)]);
        for (int i = 0; i < length; i++)
        {
            buffer.PutByte(i, (byte)('a' + i % 26));
        }
        return buffer;
    }

    [Fact(DisplayName = "an application payload is wrapped with the caller's identity and the sender's session")]
    public void PayloadCarriesTheHeaderTheCallerAsked()
    {
        UnsafeBuffer body = Payload(16);
        Assert.Equal(Publish.Published,
                     _publisher.PublishPayload(_sender, SourceId, ConnectionId, PayloadId, body, 16));
        Assert.Equal(1, _sender.Calls);

        var sent = new SbeBuffer(_sender.Sent);
        _messageHeader.Wrap(sent, 0, Frame.MessageHeader.SbeSchemaVersion);
        Assert.Equal(Frame.Unsequenced.TemplateId, _messageHeader.TemplateId);
        _unsequenced.WrapForDecode(sent, Frame.MessageHeader.Size, _messageHeader.BlockLength, _messageHeader.Version);
        Assert.Equal(SourceId, _unsequenced.Header.SourceId);
        Assert.Equal(ConnectionId, _unsequenced.Header.ConnectionId);
        Assert.Equal(SessionId, _unsequenced.Header.SessionId); // the sender's session, not the caller's
        Assert.Equal(PayloadId, _unsequenced.Header.PayloadId);

        var carried = new byte[16];
        Assert.Equal(16, _unsequenced.PayloadLength());
        _unsequenced.GetPayload(carried, 0, carried.Length);
        for (int i = 0; i < carried.Length; i++)
        {
            Assert.True(body.GetByte(i) == carried[i], $"payload byte {i}");
        }
    }

    [Fact(DisplayName = "a payload above MAX_PAYLOAD_LENGTH is refused locally — nothing is encoded, nothing offered")]
    public void OversizeBodyIsRefusedWithoutTouchingTheSender()
    {
        int tooLong = FrameLayer.MaxPayloadLength + 1;
        Assert.Equal(Publish.Refused,
                     _publisher.PublishPayload(_sender, SourceId, ConnectionId, PayloadId, Payload(tooLong), tooLong));
        Assert.True(_sender.Calls == 0, "a Refused publish must not reach the transport");
    }

    [Fact(DisplayName = "a payload at exactly MAX_PAYLOAD_LENGTH is admitted")]
    public void MaximumBodyIsAdmitted()
    {
        int atLimit = FrameLayer.MaxPayloadLength;
        Assert.Equal(Publish.Published,
                     _publisher.PublishPayload(_sender, SourceId, ConnectionId, PayloadId, Payload(atLimit), atLimit));
    }

    [Fact(DisplayName = "a transport that will not take the frame is Declined, which a caller may retry")]
    public void DeclinedIsTheTransportsAnswer()
    {
        _sender.Accept = false;
        Assert.Equal(Publish.Declined,
                     _publisher.PublishPayload(_sender, SourceId, ConnectionId, PayloadId, Payload(8), 8));
        Assert.Equal(1, _sender.Calls);
    }

    [Fact(DisplayName = "a system event is wrapped in the system family, named by systemEventType")]
    public void SystemEventUsesTheSystemFamily()
    {
        UnsafeBuffer body = Payload(12);
        Assert.Equal(Publish.Published,
                     _publisher.PublishSystem(_sender, SourceId, ConnectionId, SystemFrame.GatewayStarted, body, 12));

        var sent = new SbeBuffer(_sender.Sent);
        _messageHeader.Wrap(sent, 0, Frame.MessageHeader.SbeSchemaVersion);
        Assert.Equal(Frame.UnsequencedSystem.TemplateId, _messageHeader.TemplateId);
        _unsequencedSystem.WrapForDecode(sent, Frame.MessageHeader.Size, _messageHeader.BlockLength,
                                         _messageHeader.Version);
        // The uint16 at offset 16 is a systemEventType on this family and a payloadId on the other, which is why a
        // consumer splits by family before it reads it.
        Assert.Equal(SystemFrame.GatewayStarted, _unsequencedSystem.Header.SystemEventType);
        Assert.Equal(12, _unsequencedSystem.BodyLength());
    }

    [Fact(DisplayName = "one publisher, many frames: the buffer it owns is reused and never grows a frame's tail")]
    public void BufferIsReusedAcrossPublishes()
    {
        Assert.Equal(Publish.Published,
                     _publisher.PublishPayload(_sender, SourceId, ConnectionId, PayloadId, Payload(64), 64));
        int longFrame = _sender.Length;
        Assert.Equal(Publish.Published,
                     _publisher.PublishPayload(_sender, SourceId, ConnectionId, PayloadId, Payload(8), 8));
        Assert.True(_sender.Length < longFrame, "the second frame is shorter than the first");
    }

    [Fact(DisplayName = "a tracked publish records the frame it placed under the sender's session and term")]
    public void TrackedPublishRecordsThePlacedFrame()
    {
        Assert.Equal(Publish.Published,
                     _tracked.PublishPayload(_sender, SourceId, ConnectionId, PayloadId, Payload(8), 8));
        Assert.Equal(new List<TrackedFrame> { new TrackedFrame(_sender.Length, SessionId, 1) }, _tracker.Tracked);
    }

    [Fact(DisplayName = "while the tracker holds or is full, nothing is sent and the publish is Declined")]
    public void HoldingOrFullTrackerDeclinesWithoutSending()
    {
        _tracker.Holding = true;
        Assert.Equal(Publish.Declined,
                     _tracked.PublishPayload(_sender, SourceId, ConnectionId, PayloadId, Payload(8), 8));
        _tracker.Holding = false;
        _tracker.Full = true;
        Assert.Equal(Publish.Declined, _tracked.PublishSystem(_sender, SourceId, ConnectionId,
                                                              SystemFrame.GatewayStarted, Payload(8), 8));
        Assert.Equal(0, _sender.Calls);
        Assert.Empty(_tracker.Tracked);
    }

    [Fact(DisplayName = "a frame the transport declines is not tracked")]
    public void TransportDeclineIsNotTracked()
    {
        _sender.Accept = false;
        Assert.Equal(Publish.Declined,
                     _tracked.PublishPayload(_sender, SourceId, ConnectionId, PayloadId, Payload(8), 8));
        Assert.Empty(_tracker.Tracked);
    }

    [Fact(DisplayName = "an oversize payload is Refused even while the tracker holds")]
    public void RefusalComesBeforeTheHold()
    {
        _tracker.Holding = true;
        int tooLong = FrameLayer.MaxPayloadLength + 1;
        Assert.Equal(Publish.Refused,
                     _tracked.PublishPayload(_sender, SourceId, ConnectionId, PayloadId, Payload(tooLong), tooLong));
    }

    [Fact(DisplayName = "a frame the sequencer would reject (§9.2 conditions 6-9) is Refused locally, nothing offered")]
    public void FramesTheSequencerWouldRejectAreRefused()
    {
        Assert.Equal(Publish.Refused, _publisher.PublishPayload(_sender, -1, ConnectionId, PayloadId, Payload(8), 8));
        Assert.Equal(Publish.Refused, _publisher.PublishPayload(_sender, SourceId, ConnectionId, 0, Payload(8), 8));
        Assert.Equal(Publish.Refused, _publisher.PublishPayload(_sender, SourceId, ConnectionId, 1, Payload(8), 8));
        Assert.Equal(Publish.Refused, _publisher.PublishSystem(_sender, SourceId, ConnectionId,
                                                               SystemFrame.LeadershipChanged, Payload(8), 8));
        Assert.Equal(Publish.Refused, _publisher.PublishSystem(_sender, SourceId, ConnectionId,
                                                               SystemFrame.GatewayStarted, Payload(4), 4));
        Assert.Equal(0, _sender.Calls);
    }
}
