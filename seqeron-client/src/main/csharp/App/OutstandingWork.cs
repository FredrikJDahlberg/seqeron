using System;

namespace Org.Limitless.Seqeron.App;

/// <summary>
/// Leader-only request/reply work tracked across a failover. Every replica feeds it the same
/// <c>clusterSeqNo</c>-ordered stream, so all replicas hold the same outstanding set.
/// <list type="bullet">
/// <item><b>Outstanding is replicated:</b> a request is outstanding from its sequenced request
/// (<see cref="OnRequest"/>) until its sequenced reply (<see cref="OnReply"/>).</item>
/// <item><b>Dispatched is local:</b> "this node is handling it", cleared by <see cref="OnNotLeader"/> and
/// <see cref="OnReplyNotEmitted"/>. Only the leader dispatches.</item>
/// </list>
/// <para>Emission is at-least-once: a reply lost in an election is dispatched again by the next leader. The key is the
/// request's <c>clusterSeqNo</c>; make the reply a pure function of the request, so a re-emission is identical to the
/// one it may duplicate and consumers can drop it.</para>
/// <para>Dispatch runs in request order, which is <c>clusterSeqNo</c> order, so emission order is the same on every
/// replica. Each slot's work is made once by the factory and reused, so a warm tracker allocates nothing per request;
/// the slots double when more requests are outstanding than they hold. <c>OutstandingWork.java</c> and
/// <c>app/OutstandingWork.hpp</c> are its twins; keep the three in step.</para>
/// </summary>
/// <typeparam name="TWork">the work a dispatch needs, filled in place</typeparam>
public sealed class OutstandingWork<TWork>
{
    /// <summary>Takes one request; must not call back into the tracker.</summary>
    /// <param name="key">the request's <c>clusterSeqNo</c></param>
    /// <param name="work">what dispatching it needs</param>
    /// <returns>true if taken, false to stop the sweep (no capacity); the rest wait for the next call</returns>
    public delegate bool Dispatcher(long key, TWork work);

    private const int InitialSlots = 16;
    private const byte Free = 0;
    private const byte Outstanding = 1;
    private const byte Dispatched = 2; // local to this node

    private readonly Func<TWork> _factory;

    // Slots [_head, _tail) in increasing key order; an answered one stays Free until compacted away.
    private long[] _keys = new long[InitialSlots];
    private byte[] _states = new byte[InitialSlots];
    private TWork[] _works = new TWork[InitialSlots];
    private int _head;
    private int _tail;
    private long _newestKey = long.MinValue;

    /// <summary>A tracker holding no work.</summary>
    /// <param name="factory">makes the work of each slot, once</param>
    public OutstandingWork(Func<TWork> factory)
    {
        _factory = factory;
        for (int slot = 0; slot < InitialSlots; ++slot)
        {
            _works[slot] = factory();
        }
    }

    /// <summary>A request seen on the sequenced stream, on every replica. Keys increase; one already outstanding is
    /// the same request asserted again, and keeps its place.</summary>
    /// <param name="key">the request's <c>clusterSeqNo</c></param>
    /// <returns>the request's work, to fill with what dispatching it needs; reused once the request is answered, and
    /// the reference is valid until the next call on this tracker</returns>
    /// <exception cref="ArgumentException">for a key no greater than an earlier one that is no longer
    /// outstanding</exception>
    public ref TWork OnRequest(long key)
    {
        if (key <= _newestKey)
        {
            int found = Find(key);
            if (found < 0)
            {
                throw new ArgumentException($"request {key} is not outstanding and not after {_newestKey}");
            }
            return ref _works[found];
        }
        if (_tail == _keys.Length)
        {
            Compact();
        }
        _newestKey = key;
        _keys[_tail] = key;
        _states[_tail] = Outstanding;
        ++Count;
        return ref _works[_tail++];
    }

    /// <summary>Its sequenced reply, on every replica: the request is answered.</summary>
    /// <param name="key">the request's <c>clusterSeqNo</c></param>
    public void OnReply(long key)
    {
        int slot = Find(key);
        if (slot < 0)
        {
            return;
        }
        _states[slot] = Free;
        --Count;
        while (_head < _tail && _states[_head] == Free)
        {
            ++_head;
        }
        if (_head == _tail)
        {
            _head = 0;
            _tail = 0;
        }
    }

    /// <summary>The leader gate closed: forget what this node dispatched, so the next opening re-dispatches
    /// it.</summary>
    public void OnNotLeader()
    {
        for (int slot = _head; slot < _tail; ++slot)
        {
            if (_states[slot] == Dispatched)
            {
                _states[slot] = Outstanding;
            }
        }
    }

    /// <summary>The reply offer failed, so no sequenced reply will come. The request stays outstanding.</summary>
    /// <param name="key">the request's <c>clusterSeqNo</c></param>
    public void OnReplyNotEmitted(long key)
    {
        int slot = Find(key);
        if (slot >= 0)
        {
            _states[slot] = Outstanding;
        }
    }

    /// <summary>Leader only, while the gate is open. Offers each undispatched outstanding request in request
    /// order.</summary>
    /// <param name="dispatcher">takes each request</param>
    /// <returns>how many were dispatched by this call</returns>
    public int DispatchUndispatched(Dispatcher dispatcher)
    {
        int count = 0;
        for (int slot = _head; slot < _tail; ++slot)
        {
            if (_states[slot] != Outstanding)
            {
                continue;
            }
            if (!dispatcher(_keys[slot], _works[slot]))
            {
                break;
            }
            _states[slot] = Dispatched;
            ++count;
        }
        return count;
    }

    /// <summary>Requests held, dispatched or not.</summary>
    public int Count { get; private set; }

    /// <summary>Whether this request has been dispatched and not yet completed.</summary>
    /// <param name="key">the request's <c>clusterSeqNo</c></param>
    public bool IsDispatched(long key)
    {
        int slot = Find(key);
        return slot >= 0 && _states[slot] == Dispatched;
    }

    // The slot holding this outstanding key, or -1.
    private int Find(long key)
    {
        int low = _head;
        int high = _tail - 1;
        while (low <= high)
        {
            int mid = (low + high) / 2;
            if (_keys[mid] < key)
            {
                low = mid + 1;
            }
            else if (_keys[mid] > key)
            {
                high = mid - 1;
            }
            else
            {
                return _states[mid] == Free ? -1 : mid;
            }
        }
        return -1;
    }

    // Moves the outstanding slots to the front, swapping their work, and doubles if over half are left.
    private void Compact()
    {
        int to = 0;
        for (int from = _head; from < _tail; ++from)
        {
            if (_states[from] == Free)
            {
                continue;
            }
            if (from != to)
            {
                _keys[to] = _keys[from];
                _states[to] = _states[from];
                _states[from] = Free;
                TWork free = _works[to];
                _works[to] = _works[from];
                _works[from] = free;
            }
            ++to;
        }
        _head = 0;
        _tail = to;
        if (Count > _keys.Length / 2)
        {
            int slots = _keys.Length * 2;
            Array.Resize(ref _keys, slots);
            Array.Resize(ref _states, slots);
            Array.Resize(ref _works, slots);
            for (int slot = slots / 2; slot < slots; ++slot)
            {
                _works[slot] = _factory();
            }
        }
    }
}
