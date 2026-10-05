using System.Collections.Generic;

namespace Org.Limitless.Seqeron.App;

/// <summary>
/// Leader-only request/reply work tracked across a failover. Every replica feeds it the same
/// <c>globalSeqNo</c>-ordered stream, so all replicas hold the same outstanding set.
/// <list type="bullet">
/// <item><b>Outstanding is replicated:</b> a request is outstanding from its sequenced request
/// (<see cref="OnRequest"/>) until its sequenced reply (<see cref="OnReply"/>).</item>
/// <item><b>Dispatched is local:</b> "this node is handling it", cleared by <see cref="OnNotLeader"/> and
/// <see cref="OnReplyNotEmitted"/>. Only the leader dispatches.</item>
/// </list>
/// <para>Emission is at-least-once: a reply lost in an election is dispatched again by the next leader. Key on the
/// request's <c>globalSeqNo</c> and make the reply a pure function of the request, so a re-emission is identical to
/// the one it may duplicate and consumers can drop it.</para>
/// <para>Dispatch runs in request order, which is <c>globalSeqNo</c> order, so emission order is the same on every
/// replica. <c>OutstandingWork.java</c> and <c>app/OutstandingWork.hpp</c> are its twins; keep the three in
/// step.</para>
/// </summary>
/// <typeparam name="TKey">the request key</typeparam>
/// <typeparam name="TWork">the work a dispatch needs</typeparam>
public sealed class OutstandingWork<TKey, TWork>
{
    /// <summary>Takes one request; must not call back into the tracker.</summary>
    /// <param name="key">the request's key</param>
    /// <param name="work">what dispatching it needs</param>
    /// <returns>true if taken, false to stop the sweep (no capacity); the rest wait for the next call</returns>
    public delegate bool Dispatcher(TKey key, TWork work);

    // Request order, with each key's node, so a re-asserted request keeps its place and a reply removes in O(1).
    private readonly LinkedList<KeyValuePair<TKey, TWork>> _order = new LinkedList<KeyValuePair<TKey, TWork>>();
    private readonly Dictionary<TKey, LinkedListNode<KeyValuePair<TKey, TWork>>> _outstanding =
        new Dictionary<TKey, LinkedListNode<KeyValuePair<TKey, TWork>>>();
    private readonly HashSet<TKey> _dispatched = new HashSet<TKey>();

    /// <summary>A request seen on the sequenced stream, on every replica. Idempotent on the key, keeping its
    /// position.</summary>
    /// <param name="key">the request's key</param>
    /// <param name="work">what dispatching it needs</param>
    public void OnRequest(TKey key, TWork work)
    {
        var entry = new KeyValuePair<TKey, TWork>(key, work);
        if (_outstanding.TryGetValue(key, out LinkedListNode<KeyValuePair<TKey, TWork>> node))
        {
            node.Value = entry;
            return;
        }
        _outstanding.Add(key, _order.AddLast(entry));
    }

    /// <summary>Its sequenced reply, on every replica: the request is answered.</summary>
    /// <param name="key">the request's key</param>
    public void OnReply(TKey key)
    {
        if (_outstanding.Remove(key, out LinkedListNode<KeyValuePair<TKey, TWork>> node))
        {
            _order.Remove(node);
        }
        _dispatched.Remove(key);
    }

    /// <summary>The leader gate closed: forget what this node dispatched, so the next opening re-dispatches
    /// it.</summary>
    public void OnNotLeader()
    {
        _dispatched.Clear();
    }

    /// <summary>The reply offer failed, so no sequenced reply will come. The request stays outstanding.</summary>
    /// <param name="key">the request's key</param>
    public void OnReplyNotEmitted(TKey key)
    {
        _dispatched.Remove(key);
    }

    /// <summary>Leader only, while the gate is open. Offers each undispatched outstanding request in request
    /// order.</summary>
    /// <param name="dispatcher">takes each request</param>
    /// <returns>how many were dispatched by this call</returns>
    public int DispatchUndispatched(Dispatcher dispatcher)
    {
        int count = 0;
        foreach (KeyValuePair<TKey, TWork> entry in _order)
        {
            if (_dispatched.Contains(entry.Key))
            {
                continue;
            }
            if (!dispatcher(entry.Key, entry.Value))
            {
                break;
            }
            _dispatched.Add(entry.Key);
            ++count;
        }
        return count;
    }

    /// <summary>Requests held, dispatched or not.</summary>
    public int Count => _outstanding.Count;

    /// <summary>Whether this request has been dispatched and not yet completed.</summary>
    /// <param name="key">the request's key</param>
    public bool IsDispatched(TKey key)
    {
        return _dispatched.Contains(key);
    }
}
