using System.Collections.Generic;
using System.Linq;
using Org.Limitless.Seqeron.Helpers;
using Xunit;

namespace Org.Limitless.Seqeron.App;

/// <summary>
/// <see cref="LeaderGate"/> and <see cref="OutstandingWork{TKey,TWork}"/> on three simulated replicas sharing one
/// log, with a seeded generator interleaving requests, replica lag and batched polls, recovery, elections, failed
/// reply offers and replies lost in an election.
/// <para><b>Safety</b>, checked on every step: a replica's outstanding set matches the log prefix it has applied;
/// only a caught-up replica that sees itself as leader dispatches; within one gate opening a request is dispatched
/// at most once unless its offer failed; a sweep dispatches in <c>globalSeqNo</c> order; and a request this opening
/// dispatched is never silently lost — its reply is pending, in flight or in the log, or a leadership change this
/// replica has yet to act on will close the gate.</para>
/// <para><b>Liveness</b>: once the faults stop, every request has a reply in the log and every replica's
/// outstanding set is empty. Replies may appear more than once; that is the at-least-once contract.</para>
/// <para>Seeds are fixed; the failing one is in the test name. Draws are made in the same order as
/// <c>OutstandingWorkPropertyTest.java</c> and <c>OutstandingWorkPropertyTest.cpp</c>, so a seed is the same
/// interleaving in all three.</para>
/// </summary>
public class OutstandingWorkPropertyTest
{
    private const int Replicas = 3;

    // Long enough that a gate which ignores a flip away and back fails several of the seeds below.
    private const int ChaosSteps = 10_000;
    private const int QuiesceRounds = 100;

    private enum Kind
    {
        Request,
        Reply,
        Leader
    }

    private readonly record struct Entry(Kind Kind, long Value);

    private readonly List<Entry> _log = new List<Entry>();

    // The outstanding set after the whole log, and its size after each entry.
    private readonly HashSet<long> _open = new HashSet<long>();
    private readonly List<int> _outstandingAfter = new List<int>();

    // Every key with a reply in the log, applied or not.
    private readonly HashSet<long> _replied = new HashSet<long>();

    // Replies offered to the cluster and not yet in the log.
    private readonly List<long> _inFlight = new List<long>();
    private readonly List<Replica> _replicas = new List<Replica>();
    private SplitMix64 _rng;
    private bool _faults;
    private int _leader;
    private int _lastLeaderIndex;

    private sealed class Replica
    {
        private readonly OutstandingWorkPropertyTest _test;
        private readonly int _memberId;
        private readonly LeaderGate _gate;
        private readonly OutstandingWork<long, long> _work = new OutstandingWork<long, long>();

        // Dispatched, reply not yet offered.
        public readonly Queue<long> PendingReplies = new Queue<long>();

        // The model of what this gate opening has dispatched.
        private readonly HashSet<long> _tenure = new HashSet<long>();
        private int _applied;
        private int _viewLeader = -1;
        public bool Recovering;

        // A LeadershipChanged applied since the last duty cycle.
        private bool _leadershipApplied;
        private int _slots;
        private long _lastInSweep;

        public Replica(OutstandingWorkPropertyTest test, int memberId)
        {
            _test = test;
            _memberId = memberId;
            _gate = new LeaderGate(memberId);
        }

        public void ApplyNext()
        {
            if (_applied == _test._log.Count)
            {
                return;
            }
            Entry entry = _test._log[_applied++];
            switch (entry.Kind)
            {
                case Kind.Request:
                    _work.OnRequest(entry.Value, entry.Value);
                    break;
                case Kind.Reply:
                    _work.OnReply(entry.Value);
                    _tenure.Remove(entry.Value);
                    break;
                case Kind.Leader:
                    _viewLeader = (int)entry.Value;
                    _gate.OnLeadershipChanged();
                    _leadershipApplied = true;
                    break;
            }
            Assert.True(_test.OutstandingAt(_applied) == _work.Count, $"member {_memberId} at log index {_applied}");
        }

        // One poll that delivers everything behind this replica.
        public void ApplyAll()
        {
            while (_applied < _test._log.Count)
            {
                ApplyNext();
            }
        }

        public void DutyCycle()
        {
            if (_gate.Update(!Recovering, _viewLeader) == LeaderGate.Transition.Closed)
            {
                _work.OnNotLeader();
                _tenure.Clear();
            }
            _leadershipApplied = false;
            if (!_gate.IsOpen)
            {
                return;
            }
            _slots = _test._faults ? _test._rng.Roll(3) : int.MaxValue;
            _lastInSweep = 0;
            _work.DispatchUndispatched(Dispatch);
        }

        private bool Dispatch(long key, long request)
        {
            if (_slots-- <= 0)
            {
                return false;
            }
            Assert.True(!Recovering && _viewLeader == _memberId, $"member {_memberId} dispatched while not leader");
            Assert.True(key > _lastInSweep, $"member {_memberId} dispatched {key} after {_lastInSweep}");
            Assert.True(_tenure.Add(key), $"member {_memberId} dispatched {key} twice in one opening");
            Assert.Equal(key, request);
            _lastInSweep = key;
            PendingReplies.Enqueue(key);
            return true;
        }

        public void Offer()
        {
            if (!PendingReplies.TryDequeue(out long key) || !_gate.IsOpen)
            {
                return; // a reply produced after the gate closed is dropped; the next leader re-dispatches
            }
            if (_test._faults && _test._rng.Roll(4) == 0)
            {
                _work.OnReplyNotEmitted(key);
                _tenure.Remove(key);
            }
            else
            {
                _test._inFlight.Add(key);
            }
        }

        // A request held as dispatched whose reply is gone is never dispatched again by this opening.
        public void CheckNoLostDispatch()
        {
            if (!_gate.IsOpen || _leadershipApplied || _test._lastLeaderIndex >= _applied)
            {
                return; // a close is already due
            }
            foreach (long key in _tenure)
            {
                Assert.True(PendingReplies.Contains(key) || _test._inFlight.Contains(key) ||
                                _test._replied.Contains(key),
                            $"member {_memberId} holds {key} as dispatched, but its reply is gone");
            }
        }

        public bool IsQuiet => _applied == _test._log.Count && PendingReplies.Count == 0 && _work.Count == 0;
    }

    [Theory]
    [InlineData(1UL), InlineData(2UL), InlineData(3UL), InlineData(5UL), InlineData(8UL), InlineData(13UL)]
    [InlineData(21UL), InlineData(34UL), InlineData(55UL), InlineData(89UL), InlineData(144UL), InlineData(233UL)]
    [InlineData(377UL), InlineData(610UL), InlineData(987UL), InlineData(1597UL)]
    public void AnswersEveryRequestUnderRandomFailovers(ulong seed)
    {
        _rng = new SplitMix64(seed);
        _faults = true;
        for (int member = 0; member < Replicas; ++member)
        {
            _replicas.Add(new Replica(this, member));
        }
        Append(new Entry(Kind.Leader, _leader));

        for (int step = 0; step < ChaosSteps; ++step)
        {
            ChaosStep();
            _replicas.ForEach(replica => replica.CheckNoLostDispatch());
        }

        _faults = false;
        _replicas.ForEach(replica => replica.Recovering = false);
        for (int round = 0; round < QuiesceRounds && !IsQuiet(); ++round)
        {
            while (_inFlight.Count > 0)
            {
                Deliver();
            }
            _replicas.ForEach(replica => replica.ApplyAll());
            _replicas.ForEach(replica => replica.DutyCycle());
            foreach (Replica replica in _replicas)
            {
                while (replica.PendingReplies.Count > 0)
                {
                    replica.Offer();
                }
            }
        }

        Assert.True(IsQuiet(), "did not converge once the faults stopped");
        Assert.True(OutstandingAt(_log.Count) == 0, "every request has a reply in the log");
    }

    private void Append(Entry entry)
    {
        _log.Add(entry);
        if (entry.Kind == Kind.Request)
        {
            _open.Add(entry.Value);
        }
        else if (entry.Kind == Kind.Reply)
        {
            _open.Remove(entry.Value);
            _replied.Add(entry.Value);
        }
        _outstandingAfter.Add(_open.Count);
    }

    private int OutstandingAt(int length)
    {
        return length == 0 ? 0 : _outstandingAfter[length - 1];
    }

    private void Elect()
    {
        _leader = (_leader + 1 + _rng.Roll(Replicas - 1)) % Replicas;
        _lastLeaderIndex = _log.Count;
        Append(new Entry(Kind.Leader, _leader));
        if (_faults)
        {
            _inFlight.RemoveAll(IsLostInElection); // uncommitted on the old leader
        }
    }

    private bool IsLostInElection(long key)
    {
        return _rng.Roll(2) == 0;
    }

    private void Deliver()
    {
        Append(new Entry(Kind.Reply, _inFlight[0]));
        _inFlight.RemoveAt(0);
    }

    private void ChaosStep()
    {
        Replica replica = _replicas[_rng.Roll(Replicas)];
        int roll = _rng.Roll(100);
        if (roll < 20)
        {
            Append(new Entry(Kind.Request, _log.Count + 1));
        }
        else if (roll < 45)
        {
            replica.ApplyNext();
        }
        else if (roll < 50)
        {
            replica.ApplyAll();
        }
        else if (roll < 70)
        {
            replica.DutyCycle();
        }
        else if (roll < 80)
        {
            replica.Offer();
        }
        else if (roll < 90)
        {
            if (_inFlight.Count > 0)
            {
                Deliver();
            }
        }
        else if (roll < 93)
        {
            Elect();
        }
        else
        {
            replica.Recovering = !replica.Recovering;
        }
    }

    private bool IsQuiet()
    {
        return _inFlight.Count == 0 && _replicas.All(replica => replica.IsQuiet);
    }
}
