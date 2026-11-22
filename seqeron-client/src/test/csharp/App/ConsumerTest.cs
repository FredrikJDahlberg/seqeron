using System.Collections.Generic;
using Org.Limitless.Seqeron.Protocol;
using Org.Limitless.Seqeron.Replayer.Client;
using Xunit;

namespace Org.Limitless.Seqeron.App;

/// <summary>
/// A consumer on a queue under a snapshot policy, which only an <see cref="Application"/> can follow. No Aeron runtime:
/// the events are encoded queue frames handed to the consumer's dispatch, and the queue that starts at a cut is
/// <c>ReplayerRecoveryTest</c>'s. Case for case with <c>ConsumerTest.java</c>.
/// </summary>
public class ConsumerTest
{
    private const int PayloadId = 5;

    private readonly RecordingListener _listener = new RecordingListener();
    private readonly Consumer _consumer;

    public ConsumerTest()
    {
        _consumer = new Consumer(new ConsumerOptions { ClientId = 1, MemberId = 2, GroupId = 1, Listener = _listener });
    }

    private static SequencedEvent PayloadEvent()
    {
        return SequencedEvents.Of(1, false, PayloadId, new byte[sizeof(long)]);
    }

    private static SequencedEvent SystemEvent(int systemEventType)
    {
        return SequencedEvents.Of(1, true, systemEventType, new byte[0]);
    }

    [Fact(DisplayName = "a snapshot policy's row fences with SNAPSHOTTED_QUEUE, and nothing after it is dispatched")]
    public void ASnapshotPolicyFences()
    {
        _consumer.OnSequenced(PayloadEvent());
        _consumer.OnSequenced(SystemEvent(SystemFrame.SnapshotPolicyRegistered));
        _consumer.OnSequenced(PayloadEvent());
        _consumer.CheckSnapshotted();

        Assert.Equal(new List<int> { PayloadId }, _listener.Dispatched);
        Assert.Equal(new List<ClusterError> { ClusterError.SnapshottedQueue }, _listener.Fences);
    }

    [Fact(DisplayName = "a round's SnapshotStarted fences too")]
    public void ASnapshotRoundFences()
    {
        _consumer.OnSequenced(SystemEvent(SystemFrame.SnapshotStarted));
        _consumer.CheckSnapshotted();

        Assert.Equal(new List<ClusterError> { ClusterError.SnapshottedQueue }, _listener.Fences);
    }

    [Fact(DisplayName = "a queue with no snapshot frame is followed")]
    public void AQueueWithoutSnapshotsIsFollowed()
    {
        _consumer.OnSequenced(SystemEvent(SystemFrame.ClusterHeartbeat));
        _consumer.OnSequenced(PayloadEvent());
        _consumer.CheckSnapshotted();

        Assert.Equal(new List<int> { PayloadId }, _listener.Dispatched);
        Assert.Empty(_listener.Fences);
    }

    private sealed class RecordingListener : IConsumerListener
    {
        public List<int> Dispatched { get; } = new List<int>();

        public List<ClusterError> Fences { get; } = new List<ClusterError>();

        public void OnSequenced(Payload payload)
        {
            Dispatched.Add(payload.PayloadId);
        }

        public void OnFenced(ClusterError fence, string detail)
        {
            Fences.Add(fence);
        }
    }
}
