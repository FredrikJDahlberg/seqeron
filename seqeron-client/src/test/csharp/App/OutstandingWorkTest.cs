using System;
using System.Collections.Generic;
using Xunit;

namespace Org.Limitless.Seqeron.App;

/// <summary>
/// Unit tests for the outstanding-work state machine, one situation per case. No Aeron runtime. Case for case with
/// <c>OutstandingWorkTest.java</c> and <c>OutstandingWorkTest.cpp</c>; <see cref="OutstandingWorkPropertyTest"/>
/// drives the interleavings.
/// </summary>
public class OutstandingWorkTest
{
    // Records every request it takes, refusing once Slots run out — a leader with no free worker.
    private sealed class Recorder
    {
        public readonly List<long> Dispatched = new List<long>();
        public int Slots = int.MaxValue;

        public bool Dispatch(long key, int work)
        {
            if (Slots <= 0)
            {
                return false;
            }
            --Slots;
            Dispatched.Add(key);
            return true;
        }
    }

    private readonly OutstandingWork<int> _work = new OutstandingWork<int>(() => 0);

    [Fact(DisplayName = "a request is tracked, and re-asserting it returns its work and adds nothing")]
    public void RequestIsTrackedAndIdempotent()
    {
        _work.OnRequest(1) = 10;

        Assert.Equal(10, _work.OnRequest(1));
        Assert.Equal(1, _work.Count);
        Assert.False(_work.IsDispatched(1));
    }

    [Fact(DisplayName = "the leader dispatches each request once")]
    public void LeaderDispatchesEachRequestOnce()
    {
        _work.OnRequest(1);
        _work.OnRequest(2);

        var first = new Recorder();
        Assert.Equal(2, _work.DispatchUndispatched(first.Dispatch));
        Assert.Equal(new List<long> { 1, 2 }, first.Dispatched);
        Assert.True(_work.IsDispatched(1));
        Assert.True(_work.IsDispatched(2));

        var again = new Recorder();
        Assert.Equal(0, _work.DispatchUndispatched(again.Dispatch)); // both are awaiting their replies
        Assert.Empty(again.Dispatched);
    }

    [Fact(DisplayName = "a promoted follower dispatches what it only tracked")]
    public void PromotedFollowerDispatchesTrackedRequests()
    {
        _work.OnRequest(1);
        _work.OnRequest(2);
        _work.OnRequest(3);
        Assert.False(_work.IsDispatched(1), "never dispatched while a follower");

        var promoted = new Recorder();
        Assert.Equal(3, _work.DispatchUndispatched(promoted.Dispatch)); // including requests sequenced before it led
    }

    [Fact(DisplayName = "a sequenced reply removes the request, so no sweep dispatches it again")]
    public void ReplyRemovesRequest()
    {
        _work.OnRequest(1);
        _work.DispatchUndispatched(new Recorder().Dispatch);

        _work.OnReply(1);

        Assert.Equal(0, _work.Count);
        Assert.False(_work.IsDispatched(1));
        Assert.Equal(0, _work.DispatchUndispatched(new Recorder().Dispatch));
    }

    [Fact(DisplayName = "losing leadership re-dispatches an unanswered request")]
    public void LeadershipLossReDispatches()
    {
        _work.OnRequest(1);
        _work.DispatchUndispatched(new Recorder().Dispatch); // its reply is lost in flight

        _work.OnNotLeader();
        Assert.False(_work.IsDispatched(1));

        var next = new Recorder();
        Assert.Equal(1, _work.DispatchUndispatched(next.Dispatch));
        Assert.Equal(new List<long> { 1 }, next.Dispatched);
    }

    [Fact(DisplayName = "a reply that never reached the cluster is re-dispatched by the same leader")]
    public void ReplyNotEmittedReDispatches()
    {
        _work.OnRequest(1);
        _work.DispatchUndispatched(new Recorder().Dispatch);

        _work.OnReplyNotEmitted(1);
        Assert.False(_work.IsDispatched(1));
        Assert.Equal(1, _work.Count); // still outstanding: only a sequenced reply discharges it

        Assert.Equal(1, _work.DispatchUndispatched(new Recorder().Dispatch)); // no leadership change needed

        _work.OnReply(1);
        Assert.Equal(0, _work.Count);
        Assert.Equal(0, _work.DispatchUndispatched(new Recorder().Dispatch));
    }

    [Fact(DisplayName = "running out of capacity stops the sweep, and the next call resumes it")]
    public void CapacityStopsTheSweep()
    {
        _work.OnRequest(1);
        _work.OnRequest(2);
        _work.OnRequest(3);

        var first = new Recorder { Slots = 2 };
        Assert.Equal(2, _work.DispatchUndispatched(first.Dispatch));

        var rest = new Recorder();
        Assert.Equal(1, _work.DispatchUndispatched(rest.Dispatch));

        Assert.Equal(new List<long> { 1, 2 }, first.Dispatched);
        Assert.Equal(new List<long> { 3 }, rest.Dispatched); // each dispatched exactly once
    }

    [Fact(DisplayName = "a reply for an unknown key is harmless")]
    public void ReplyForUnknownKeyIsHarmless()
    {
        _work.OnReply(42);

        Assert.Equal(0, _work.Count);
    }

    [Fact(DisplayName = "a re-asserted request keeps its place")]
    public void ReassertedRequestKeepsItsPlace()
    {
        _work.OnRequest(3);
        _work.OnRequest(5);
        _work.OnRequest(9);
        _work.OnRequest(3);

        var recorder = new Recorder();
        _work.DispatchUndispatched(recorder.Dispatch);

        Assert.Equal(new List<long> { 3, 5, 9 }, recorder.Dispatched);
    }

    [Fact(DisplayName = "a request answered and asked again is refused, as a clusterSeqNo never comes back")]
    public void AnsweredRequestAskedAgainIsRefused()
    {
        _work.OnRequest(1);
        _work.OnRequest(2);
        _work.OnReply(1);

        Assert.Throws<ArgumentException>(() => _work.OnRequest(1));
        Assert.Equal(1, _work.Count);
    }

    [Fact(DisplayName = "an answered request's work is reused by a later one")]
    public void AnsweredWorkIsReused()
    {
        _work.OnRequest(1) = 7;
        _work.OnReply(1);

        Assert.Equal(7, _work.OnRequest(2));
    }

    [Fact(DisplayName = "request order and each request's work survive the slots growing and compacting")]
    public void OrderSurvivesGrowthAndCompaction()
    {
        var expected = new List<long>();
        for (long key = 1; key <= 100; ++key)
        {
            _work.OnRequest(key) = (int)key;
            if (key <= 40 && key % 2 == 1)
            {
                _work.OnReply(key);
            }
            else
            {
                expected.Add(key);
            }
        }

        var dispatched = new List<long>();
        _work.DispatchUndispatched(Take);

        Assert.Equal(expected, dispatched);
        Assert.Equal(expected.Count, _work.Count);

        bool Take(long key, int request)
        {
            Assert.Equal(key, (long)request);
            dispatched.Add(key);
            return true;
        }
    }

    [Fact(DisplayName = "a warm tracker allocates nothing per request")]
    public void WarmTrackerAllocatesNothing()
    {
        OutstandingWork<int>.Dispatcher taken = (key, request) => true;
        long key = 0;
        Requests(taken, ref key);
        long before = GC.GetAllocatedBytesForCurrentThread();
        Requests(taken, ref key);
        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.Equal(1, _work.Count);
        Assert.Equal(0, allocated);
    }

    private void Requests(OutstandingWork<int>.Dispatcher taken, ref long key)
    {
        for (int i = 0; i < 100_000; ++i)
        {
            _work.OnRequest(++key) = i;
            _work.DispatchUndispatched(taken);
            _work.OnReply(key - 1);
        }
    }
}
