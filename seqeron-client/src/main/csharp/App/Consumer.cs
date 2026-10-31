using System;
using Adaptive.Aeron;
using Org.Limitless.Seqeron.Protocol;
using Org.Limitless.Seqeron.Replayer.Client;
using Org.Limitless.Seqeron.Util;

namespace Org.Limitless.Seqeron.App;

/// <summary>
/// A read-only client: it reads one group's queue and publishes nothing, so it opens no cluster session, takes no
/// part in an election and needs no topology row. Audit, risk or a downstream feed. It replays local history through
/// the co-located Replayer, switches to the live queue once caught up, and delivers every application message once,
/// in <c>clusterSeqNo</c> order, as a <see cref="Payload"/> — the same view the producers' façades give.
/// <para>Fenced, like them, when it can no longer be trusted to see the queue: <see cref="ClusterError.RecoveryStalled"/>
/// and <see cref="ClusterError.QueueStalled"/>. It takes no snapshots and so always restarts from the start of its
/// queue, which a cluster with a <c>&lt;snapshots&gt;</c> policy no longer holds; a consumer there is an
/// <see cref="Application"/> or <see cref="Gateway"/> with a snapshot listener that writes no records.</para>
/// <para>Single-threaded: every member belongs to the caller's one duty-cycle thread, which calls
/// <see cref="DoWork"/> each iteration. <c>Consumer.java</c> and <c>app/Consumer.hpp</c> are its twins; keep the
/// three in step. <see cref="ReplayerStreamReceiver"/> is the lower-level path this sits on.</para>
/// </summary>
public sealed class Consumer : IDisposable
{
    private readonly IConsumerListener _listener;
    private readonly int _memberId;
    private readonly int _groupId;
    private readonly long _recoveryStallTimeoutMs;
    private readonly long _queueStallTimeoutMs;
    private readonly ReplayerStreamReceiver _receiver;
    private readonly RecoveryStallFence _recoveryStall;
    private readonly QueueStallFence _queueStall;
    private readonly Payload _payload = new Payload();
    private bool _caughtUp;
    private bool _fenced;

    /// <summary>A consumer configured by <paramref name="options"/>; nothing is connected until
    /// <see cref="Start"/>.</summary>
    /// <exception cref="ArgumentException">if a required option is missing</exception>
    public Consumer(ConsumerOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        Application.Require(options.Listener, nameof(options.Listener));
        _listener = options.Listener;
        _memberId = options.MemberId;
        _groupId = options.GroupId;
        _queueStallTimeoutMs = options.Tuning.QueueStallTimeoutMs;
        _recoveryStallTimeoutMs = options.Tuning.RecoveryStallTimeoutMs;
        _recoveryStall = new RecoveryStallFence(_recoveryStallTimeoutMs);
        _queueStall = new QueueStallFence(_queueStallTimeoutMs);
        _receiver = new ReplayerStreamReceiver(options.ClientId, OnSequenced, null, null);
    }

    /// <summary>Starts following the group's queue on this node; the Aeron client is the caller's.</summary>
    public void Start(Aeron aeron)
    {
        _receiver.Start(aeron, _memberId, _groupId);
    }

    /// <summary>One duty-cycle iteration. Nothing once fenced.</summary>
    /// <returns>units of work done, for the caller's idle strategy</returns>
    public int DoWork()
    {
        if (_fenced)
        {
            return 0;
        }
        int work = _receiver.Poll();
        CheckCaughtUp();
        if (!_fenced)
        {
            CheckFences();
        }
        return work;
    }

    public bool IsCaughtUp => _receiver.IsCaughtUp;

    public long LastClusterSeqNo => _receiver.LastClusterSeqNo;

    /// <summary>Closes the queue and replay subscriptions. The Aeron client is the caller's and is left
    /// open.</summary>
    public void Dispose()
    {
        _receiver.Dispose();
    }

    private void OnSequenced(SequencedEvent sequencedEvent)
    {
        if (sequencedEvent.IsSystem)
        {
            if (sequencedEvent.SystemEventType == SystemFrame.ClusterHeartbeat)
            {
                _queueStall.OnClusterHeartbeat(Clocks.MonotonicMs());
                _listener.OnClusterHeartbeat(sequencedEvent.ClusterTimestampNs, sequencedEvent.ReceiveTimeNs);
            }
            return;
        }
        _payload.Wrap(sequencedEvent);
        _listener.OnSequenced(_payload);
    }

    // Polled rather than taken off the receiver's callback: neither fence may wait on the next frame to land.
    private void CheckCaughtUp()
    {
        bool now = _receiver.IsCaughtUp;
        if (now == _caughtUp)
        {
            return;
        }
        _caughtUp = now;
        if (now)
        {
            _recoveryStall.OnCaughtUp();
            _queueStall.OnCaughtUp(Clocks.MonotonicMs());
            _listener.OnCaughtUp(_receiver.LastClusterSeqNo);
        }
    }

    private void CheckFences()
    {
        long nowMs = Clocks.MonotonicMs();
        if (!_receiver.IsCaughtUp)
        {
            if (_recoveryStall.OnNotCaughtUp(nowMs, _receiver.LastClusterSeqNo))
            {
                Fence(ClusterError.RecoveryStalled, "recovery has dispatched nothing for >" + _recoveryStallTimeoutMs +
                      "ms (clusterSeqNo stuck at " + _receiver.LastClusterSeqNo + ")");
            }
        }
        else if (_queueStall.IsStalled(nowMs))
        {
            Fence(ClusterError.QueueStalled, "no ClusterHeartbeat for >" + _queueStallTimeoutMs + "ms");
        }
    }

    // Releases the subscriptions first, so a fenced consumer holds no Replayer slot while its listener hears why.
    private void Fence(ClusterError reason, string detail)
    {
        _fenced = true;
        _receiver.Dispose();
        _listener.OnFenced(reason, detail);
    }
}
