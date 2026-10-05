using System;
using System.Collections.Generic;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Org.Limitless.Seqeron.Protocol;
using Org.Limitless.Seqeron.Replayer.Client;
using Org.Limitless.Seqeron.Sequencer.Client;
using Xunit;

namespace Org.Limitless.Seqeron;

/// <summary>
/// The per-frame paths allocate nothing once warm, as the Java duty cycle does not: a delegate, closure or boxed
/// value on them would be garbage per frame. C# only — Java and C++ have no such hazard to guard. Measured with
/// <see cref="GC.GetAllocatedBytesForCurrentThread"/> over a run after a warm-up of the same length; every frame
/// and event is built before the clock starts.
/// </summary>
public class HotPathAllocationTest
{
    private const int Frames = 1000;
    private const int ClientId = 3;
    private const long SessionId = 4242;
    private const int PayloadId = 6;

    [Fact(DisplayName = "a live tap frame is dispatched without allocating")]
    public void LiveTapDispatchAllocatesNothing()
    {
        var recovery = new ReplayerRecovery(ClientId, new QuietActions(), Ignore, null, null);
        recovery.Start();
        byte[] replaying = ControlMessages.Replaying(ClientId, recovery.RequestId, ReplayProtocol.NoReplayNeeded, 0);
        using (var control = new UnsafeBuffer(replaying))
        {
            recovery.OnControl(control, 0, replaying.Length);
        }

        // Every frame back to back in one buffer, so the measured loop wraps nothing.
        var offsets = new List<int>();
        var lengths = new List<int>();
        var tap = new List<byte>();
        for (long globalSeqNo = 1; globalSeqNo <= 2 * Frames; globalSeqNo++)
        {
            byte[] frame = globalSeqNo % 2 == 0 ? Protocol.Frames.ClusterHeartbeat(globalSeqNo)
                                                : Protocol.Frames.Payload(globalSeqNo, 32);
            offsets.Add(tap.Count);
            lengths.Add(frame.Length);
            tap.AddRange(frame);
        }
        using var buffer = new UnsafeBuffer(tap.ToArray());

        Deliver(recovery, buffer, offsets, lengths, 0);
        Assert.True(recovery.IsCaughtUp);
        long before = GC.GetAllocatedBytesForCurrentThread();
        Deliver(recovery, buffer, offsets, lengths, Frames);
        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.Equal(2 * Frames, recovery.LastGlobalSeqNo);
        Assert.Equal(0, allocated);
    }

    [Fact(DisplayName = "a tracked publish, and its confirmation off the tap, allocate nothing")]
    public void TrackedPublishAllocatesNothing()
    {
        var pending = new PendingSends(4);
        var publisher = new IngressPublisher(pending);
        var sender = new AcceptingSender();
        var bodyBytes = new byte[16];
        using var body = new UnsafeBuffer(bodyBytes);
        var events = new SequencedEvent[2 * Frames];
        for (int i = 0; i < events.Length; i++)
        {
            BitConverter.TryWriteBytes(bodyBytes, (long)i);
            events[i] = SequencedEvents.Of(SessionId, false, PayloadId, bodyBytes);
        }

        PublishAndConfirm(publisher, pending, sender, body, events, 0);
        long before = GC.GetAllocatedBytesForCurrentThread();
        PublishAndConfirm(publisher, pending, sender, body, events, Frames);
        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.Equal(0, pending.Count);
        Assert.False(pending.IsFaulted);
        Assert.Equal(0, allocated);
    }

    private static void Deliver(ReplayerRecovery recovery, UnsafeBuffer buffer, List<int> offsets, List<int> lengths,
                                int from)
    {
        for (int i = from; i < from + Frames; i++)
        {
            recovery.OnFrame(buffer, offsets[i], lengths[i], (i + 1) * 1024L, 0, false);
        }
    }

    private static void PublishAndConfirm(IngressPublisher publisher, PendingSends pending, AcceptingSender sender,
                                          UnsafeBuffer body, SequencedEvent[] events, int from)
    {
        for (int i = from; i < from + Frames; i++)
        {
            body.PutLong(0, i);
            Assert.True(publisher.PublishPayload(sender, 1, 2, PayloadId, body, 16) == Publish.Published);
            pending.OnSequenced(events[i]);
        }
    }

    private static void Ignore(SequencedEvent sequencedEvent)
    {
    }

    private sealed class AcceptingSender : IIngressSender
    {
        public bool Send(IDirectBuffer frame, int length)
        {
            return true;
        }

        public long ClusterSessionId => SessionId;

        public long LeadershipTermId => 1;
    }

    private sealed class QuietActions : IReplayerRecoveryActions
    {
        public void SendReplayRequest(long requestId, long fromPosition)
        {
        }

        public void SendSnapshotQuery(long requestId, int sourceId, long round)
        {
        }

        public bool SendReplayComplete()
        {
            return true;
        }

        public bool SendReplayHeartbeat()
        {
            return true;
        }

        public void OpenReplay(long replaySessionId)
        {
        }

        public void CloseReplay()
        {
        }

        public void RecoveryStalled(bool stalled)
        {
        }

        public int? MemberId => null;

        public long NowMs()
        {
            return 0;
        }
    }
}
