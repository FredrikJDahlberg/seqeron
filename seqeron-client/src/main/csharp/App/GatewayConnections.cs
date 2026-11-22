using System.Collections.Generic;
using Org.Limitless.Seqeron.Protocol;

namespace Org.Limitless.Seqeron.App;

/// <summary>
/// The connections one gateway instance holds, the lifecycle frames it still owes the log for them, and what a payload
/// may be published on. I/O-free: <see cref="Gateway"/> places the frames and asks <see cref="GatewayLifecycle"/>
/// nothing more.
/// <para>Single-threaded. The Java twin is <c>app/GatewayConnections.java</c>; keep the two in step.</para>
/// </summary>
internal sealed class GatewayConnections
{
    /// <summary>Places one connection lifecycle frame.</summary>
    internal interface IPlacer
    {
        /// <returns>whether it landed; false stops the drain, to be retried in order</returns>
        bool Place(int connectionId, int systemEventType, byte[] connectionData);
    }

    private static readonly byte[] NoData = new byte[0];

    private readonly GatewayLifecycle _lifecycle;

    // Lifecycle frames still to be placed, in the order they were asked for.
    private readonly List<Entry> _backlog = new List<Entry>();

    // Connections whose ConnectionOpened has not landed: nothing may be published on one yet.
    private readonly HashSet<int> _unopened = new HashSet<int>();

    // Connections taken and not yet closed while serving, in the order taken; the connections gauge.
    private readonly List<int> _held = new List<int>();

    private int _nextConnectionId;

    public GatewayConnections(GatewayLifecycle lifecycle)
    {
        _lifecycle = lifecycle;
    }

    /// <summary>Where allocation resumes for an activation: past every id this logical gateway's history
    /// holds.</summary>
    public void ResumeAt(int firstConnectionId)
    {
        _nextConnectionId = firstConnectionId;
    }

    /// <summary>The id <see cref="Open"/> allocates next.</summary>
    public int NextConnectionId => _nextConnectionId;

    /// <summary>Allocates a connection id and queues its <c>ConnectionOpened</c>.</summary>
    /// <returns>the id, or <see cref="Gateway.NoConnection"/> if this instance is not serving</returns>
    public int Open(byte[] connectionData)
    {
        if (!_lifecycle.OnSessionAcquired())
        {
            return Gateway.NoConnection;
        }
        int connectionId = _nextConnectionId++;
        _backlog.Add(new Entry(connectionId, SystemFrame.ConnectionOpened, connectionData));
        _unopened.Add(connectionId);
        _held.Add(connectionId);
        return connectionId;
    }

    /// <summary>Queues a connection's <c>ConnectionClosed</c>; one the log has not seen opened is dropped
    /// instead.</summary>
    public void Close(int connectionId)
    {
        if (!_lifecycle.IsServing)
        {
            // Not serving: the connections already went with the edge.
            return;
        }
        _held.Remove(connectionId);
        if (_unopened.Remove(connectionId))
        {
            _backlog.RemoveAll(entry => entry.ConnectionId == connectionId);
            return;
        }
        _backlog.Add(new Entry(connectionId, SystemFrame.ConnectionClosed, NoData));
    }

    /// <summary>
    /// The edge closed under a designation that stands. No <c>GatewayStarted</c> follows to release the connections,
    /// so each one the log has seen opened is closed on it, and one it has not is never announced.
    /// </summary>
    public void OnEdgeClosed()
    {
        _backlog.RemoveAll(entry => _unopened.Contains(entry.ConnectionId));
        foreach (int connectionId in _held)
        {
            if (!_unopened.Contains(connectionId))
            {
                _backlog.Add(new Entry(connectionId, SystemFrame.ConnectionClosed, NoData));
            }
        }
        _unopened.Clear();
        _held.Clear();
    }

    /// <summary>Stood down: the connections go with the edge, and a successor's <c>GatewayStarted</c> releases
    /// them.</summary>
    public void OnStandby()
    {
        _backlog.Clear();
        _unopened.Clear();
        _held.Clear();
    }

    /// <summary>Whether a payload may be published on <paramref name="connectionId"/> now: serving, and its
    /// <c>ConnectionOpened</c> landed.</summary>
    public bool MayPublish(int connectionId)
    {
        return _lifecycle.IsServing && !_unopened.Contains(connectionId);
    }

    /// <summary>Connections held open.</summary>
    public int Count => _held.Count;

    /// <summary>Places what is owed, oldest first, stopping at the first frame that does not land.</summary>
    /// <returns>frames placed</returns>
    public int Drain(IPlacer placer)
    {
        int work = 0;
        while (_backlog.Count > 0)
        {
            Entry next = _backlog[0];
            if (!placer.Place(next.ConnectionId, next.SystemEventType, next.ConnectionData))
            {
                break;
            }
            _backlog.RemoveAt(0);
            _unopened.Remove(next.ConnectionId);
            work++;
        }
        return work;
    }

    // One connection lifecycle frame waiting to be placed.
    private sealed record Entry(int ConnectionId, int SystemEventType, byte[] ConnectionData);
}
