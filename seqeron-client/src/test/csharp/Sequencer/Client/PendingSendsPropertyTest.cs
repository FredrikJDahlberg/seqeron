using System.Collections.Generic;
using System.Linq;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Org.Limitless.Seqeron.Helpers;
using Org.Limitless.Seqeron.Protocol;
using Org.Limitless.Seqeron.Replayer.Client;
using Xunit;

namespace Org.Limitless.Seqeron.Sequencer.Client;

/// <summary>
/// <see cref="PendingSends"/> driving a producer against a model cluster: a leader that appends ingress stamped with
/// its own term and drops the rest, commits a prefix, and loses everything uncommitted at an election. After one,
/// the old leader either still takes ingress and drops it, or is gone and a send spins until egress brings the
/// <c>NewLeader</c> — giving the send up if the hold is on, as <c>ClusterStreamSender</c> does. The sender may
/// reconnect on a new session then. The tap lags, a sibling shares the producer's <c>sourceId</c>, and term ids
/// skip, as after a failed ballot.
/// <para>Checked once the faults stop and everything has drained: the log holds every frame the producer sent
/// exactly once, in send order, and nothing faulted. Seeds are fixed; the failing one is in the test name. Draws are
/// made in the same order as <c>PendingSendsPropertyTest.java</c> and <c>PendingSendsPropertyTest.cpp</c>, so a
/// seed is the same run in all three.</para>
/// </summary>
public class PendingSendsPropertyTest
{
    private const int Capacity = 64;
    private const int Steps = 5_000;
    private const int QuiesceRounds = 100;
    private const long Sibling = 1_000_000;
    private const long LeadershipChanged = -1;
    private const int PayloadId = 6;

    // One tap entry: frame Value from Session, or a term's LeadershipChanged.
    private readonly record struct Entry(long Session, long Value);

    private readonly PendingSends _pending = new PendingSends(Capacity);
    private readonly SystemFrame _envelope = new SystemFrame();
    private readonly UnsafeBuffer _frame = new UnsafeBuffer(new byte[FrameLayer.MaxIngressLength]);
    private readonly byte[] _bodyBytes = new byte[sizeof(long)];
    private readonly UnsafeBuffer _body;
    private readonly ModelSender _sender;

    // Accepted by the current leader, not yet committed.
    private readonly Queue<Entry> _uncommitted = new Queue<Entry>();

    // Committed, not yet delivered to the producer's tap.
    private readonly Queue<Entry> _tap = new Queue<Entry>();

    // The producer's frames in log order.
    private readonly List<long> _log = new List<long>();
    private SplitMix64 _rng;
    private long _clusterTerm;
    private long _senderTerm;
    private long _session = 1;
    private bool _oldLeaderTakesIngress;
    private long _next;
    private int _resent;

    public PendingSendsPropertyTest()
    {
        _body = new UnsafeBuffer(_bodyBytes);
        _sender = new ModelSender(this);
    }

    // The cluster as a sender sees it.
    private sealed class ModelSender : IIngressSender
    {
        private readonly PendingSendsPropertyTest _test;

        public ModelSender(PendingSendsPropertyTest test)
        {
            _test = test;
        }

        public bool Send(IDirectBuffer frame, int length)
        {
            if (_test._senderTerm != _test._clusterTerm && !_test._oldLeaderTakesIngress)
            {
                _test.NewLeader(); // from inside the spin, as PollEgress would
                if (_test._pending.IsHolding)
                {
                    return false;
                }
            }
            if (_test._senderTerm == _test._clusterTerm)
            {
                _test._uncommitted.Enqueue(new Entry(_test._session, frame.GetLong(FrameLayer.MinIngressLength)));
            }
            return true;
        }

        public long ClusterSessionId => _test._session;

        public long LeadershipTermId => _test._senderTerm;
    }

    [Theory]
    [InlineData(1UL), InlineData(2UL), InlineData(3UL), InlineData(5UL), InlineData(8UL), InlineData(13UL)]
    [InlineData(21UL), InlineData(34UL), InlineData(55UL), InlineData(89UL), InlineData(144UL), InlineData(233UL)]
    [InlineData(377UL), InlineData(610UL), InlineData(987UL), InlineData(1597UL)]
    public void LogsEveryFrameOnceInOrderAcrossFailovers(ulong seed)
    {
        _rng = new SplitMix64(seed);
        for (int step = 0; step < Steps; step++)
        {
            int action = _rng.Roll(100);
            if (action < 30)
            {
                Send();
            }
            else if (action < 50)
            {
                Commit(_rng.Roll(4));
            }
            else if (action < 55)
            {
                _tap.Enqueue(new Entry(Sibling, _rng.Roll(8)));
            }
            else if (action < 80)
            {
                Deliver(_rng.Roll(4));
            }
            else if (action < 85)
            {
                _resent += _pending.ResendMissing(_sender);
            }
            else if (action < 87)
            {
                _uncommitted.Clear();
                _clusterTerm += 1 + _rng.Roll(2);
                _tap.Enqueue(new Entry(LeadershipChanged, _clusterTerm));
                _oldLeaderTakesIngress = _rng.Roll(2) == 0;
            }
            else if (_senderTerm != _clusterTerm)
            {
                NewLeader();
            }
        }
        for (int round = 0; round < QuiesceRounds && _pending.Count > 0; round++)
        {
            if (_senderTerm != _clusterTerm)
            {
                NewLeader();
            }
            _resent += _pending.ResendMissing(_sender);
            Commit(int.MaxValue);
            Deliver(int.MaxValue);
        }

        Assert.False(_pending.IsFaulted);
        Assert.True(_pending.Count == 0, "everything sent came back");
        Assert.True(_resent > 0, "the run lost and resent something");
        Assert.Equal(Enumerable.Range(0, (int)_next).Select(n => (long)n), _log);
    }

    private void Send()
    {
        if (_pending.IsHolding || _pending.IsFull)
        {
            return;
        }
        _body.PutLong(0, _next);
        int length = _envelope.WrapPayload(_frame, 1, 2, _session, PayloadId, _body, sizeof(long));
        if (_sender.Send(_frame, length))
        {
            _pending.Track(_frame, length, _sender.ClusterSessionId, _sender.LeadershipTermId);
            _next++;
        }
    }

    private void NewLeader()
    {
        _senderTerm = _clusterTerm;
        if (_rng.Roll(2) == 0)
        {
            _session++;
        }
        _pending.OnNewLeader(_senderTerm);
    }

    private void Commit(int count)
    {
        for (int k = count; k > 0 && _uncommitted.Count > 0; k--)
        {
            Entry entry = _uncommitted.Dequeue();
            _log.Add(entry.Value);
            _tap.Enqueue(entry);
        }
    }

    private void Deliver(int count)
    {
        for (int k = count; k > 0 && _tap.Count > 0; k--)
        {
            Entry entry = _tap.Dequeue();
            if (entry.Session == LeadershipChanged)
            {
                _pending.OnLeadershipChanged(entry.Value);
                continue;
            }
            _body.PutLong(0, entry.Value);
            _pending.OnSequenced(SequencedEvents.Of(entry.Session, false, PayloadId, _bodyBytes));
        }
    }
}
