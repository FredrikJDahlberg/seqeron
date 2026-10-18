using System.Collections.Generic;
using Adaptive.Agrona.Concurrent;
using Org.SbeTool.Sbe.Dll;
using Xunit;
using Replay = Org.Limitless.Seqeron.Sbe.Replay;

namespace Org.Limitless.Seqeron.App;

/// <summary>
/// Which queue an instance reads (spec R-6), resolved against a scripted Replayer. Case for case with
/// <c>QueueResolutionTest.java</c> and <c>QueueResolutionTest.cpp</c>, in the same order.
/// </summary>
public class QueueResolutionTest
{
    private const long ClientId = 4;
    private const long ClockMs = 3 * 60 * 60 * 1000L;

    private readonly Replayer _replayer = new Replayer();
    private readonly QueueResolution _resolution;

    public QueueResolutionTest()
    {
        _resolution = new QueueResolution(_replayer);
    }

    private sealed class Replayer : QueueResolution.IActions
    {
        public readonly List<long> Sent = new List<long>();
        public long Clock = ClockMs;

        public void SendQueueRequest(long requestId)
        {
            Sent.Add(requestId);
        }

        public long ClientId => QueueResolutionTest.ClientId;

        public long NowMs()
        {
            return Clock;
        }
    }

    [Fact]
    public void NothingIsSentBeforeStart()
    {
        _replayer.Clock += 10_000;
        _resolution.DoTimers(false);

        Assert.Empty(_replayer.Sent);
        Assert.Equal(QueueResolution.Unresolved, _resolution.GroupId);
    }

    [Fact]
    public void StartSendsTheFirstRequest()
    {
        _resolution.Start();

        Assert.Equal(new List<long> { 1 }, _replayer.Sent);
        Assert.Equal(QueueResolution.Unresolved, _resolution.GroupId);
    }

    [Fact]
    public void TheAnswerToTheCurrentRequestResolvesTheQueue()
    {
        _resolution.Start();
        Resolved(ClientId, 1, 3);

        Assert.Equal(3, _resolution.GroupId);
    }

    [Fact]
    public void AnAnswerForAnotherClientOrAnEarlierRequestIsIgnored()
    {
        _resolution.Start();
        _replayer.Clock += QueueResolution.ResendIntervalMs + 1;
        _resolution.DoTimers(false);

        Resolved(ClientId + 1, 2, 3);
        Resolved(ClientId, 1, 3);

        Assert.Equal(QueueResolution.Unresolved, _resolution.GroupId);
    }

    [Fact]
    public void AnUnansweredRequestIsResentWithANewRequestId()
    {
        _resolution.Start();
        _replayer.Clock += QueueResolution.ResendIntervalMs;
        _resolution.DoTimers(false);
        Assert.Equal(new List<long> { 1 }, _replayer.Sent); // not before the interval has passed

        _replayer.Clock += 1;
        _resolution.DoTimers(false);

        Assert.Equal(new List<long> { 1, 2 }, _replayer.Sent);
        Assert.Equal(2, _resolution.RequestId);
    }

    [Fact]
    public void APendingAnswerHoldsTheResendOff()
    {
        _resolution.Start();
        _replayer.Clock += QueueResolution.ResendIntervalMs;
        Pending(1); // the row is not on the log yet

        _replayer.Clock += 1;
        _resolution.DoTimers(false);

        Assert.Equal(new List<long> { 1 }, _replayer.Sent);
    }

    [Fact]
    public void AnUnconnectedPublicationIsRetriedEveryCycle()
    {
        _resolution.Start();
        _resolution.DoTimers(true);

        Assert.Equal(new List<long> { 1, 2 }, _replayer.Sent);
    }

    [Fact]
    public void NothingIsSentOnceResolved()
    {
        _resolution.Start();
        Resolved(ClientId, 1, 3);
        _replayer.Clock += 10_000;
        _resolution.DoTimers(true);
        Resolved(ClientId, 1, 5);

        Assert.Equal(new List<long> { 1 }, _replayer.Sent);
        Assert.Equal(3, _resolution.GroupId); // a source's group never changes
    }

    private void Resolved(long clientId, long requestId, int queueId)
    {
        var bytes = new byte[Replay.MessageHeader.Size + Replay.QueueResolved.BlockLength];
        var encoder = new Replay.QueueResolved();
        encoder.WrapForEncodeAndApplyHeader(new DirectBuffer(bytes), 0, new Replay.MessageHeader());
        encoder.ClientId = clientId;
        encoder.RequestId = requestId;
        encoder.QueueId = queueId;
        using var wire = new UnsafeBuffer(bytes);
        _resolution.OnControl(wire, 0, bytes.Length);
    }

    private void Pending(long requestId)
    {
        var bytes = new byte[Replay.MessageHeader.Size + Replay.ReplayPending.BlockLength];
        var encoder = new Replay.ReplayPending();
        encoder.WrapForEncodeAndApplyHeader(new DirectBuffer(bytes), 0, new Replay.MessageHeader());
        encoder.ClientId = ClientId;
        encoder.RequestId = requestId;
        using var wire = new UnsafeBuffer(bytes);
        _resolution.OnControl(wire, 0, bytes.Length);
    }
}
