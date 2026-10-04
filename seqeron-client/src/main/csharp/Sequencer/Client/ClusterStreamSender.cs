using System;
using Adaptive.Aeron;
using Adaptive.Aeron.Exceptions;
using Adaptive.Aeron.LogBuffer;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Adaptive.Cluster.Client;
using Adaptive.Cluster.Codecs;
using Org.Limitless.Seqeron.Protocol;
using Org.Limitless.Seqeron.Util;

namespace Org.Limitless.Seqeron.Sequencer.Client;

/// <summary>
/// The cluster session a producer submits on, with the method names and semantics of <c>ClusterStreamSender.java</c>
/// and <c>sequencer/client/ClusterStreamSender.hpp</c>. Small, because Aeron.NET's <c>AeronCluster</c> already is the
/// cluster protocol the C++ class implements by hand; this adds only <see cref="ConnectColocated"/>, a
/// <see cref="Send"/> that spins through back-pressure and elections, and a self-throttling
/// <see cref="KeepAlive"/>.
/// <para>As in Java, when leadership moves off the co-located member this reconnects (a new cluster session id)
/// where C++ swaps the publication, because <c>AeronCluster</c> owns its publication.</para>
/// <para>Every timeout Java sets is set here explicitly: Aeron.NET's defaults are Aeron's, and its new-leader
/// timeout is shorter than an election.</para>
/// <para>Not thread-safe: every method belongs to the caller's one duty-cycle thread.</para>
/// </summary>
public sealed class ClusterStreamSender : IIngressSender, IDisposable
{
    // Ingress channel of a client sharing its member's media driver. Only the leader subscribes to it.
    private const string IngressChannelIpc = "aeron:ipc";

    // Ingress channel of a client reaching members over the network; endpoints name the members.
    private const string IngressChannelUdp = "aeron:udp";

    // No co-located member: this sender reaches the cluster over the network.
    private const int NoMember = -1;

    internal const long ConnectTimeoutNs = 10_000_000_000L;

    // How long the client waits for a NewLeader before closing itself. The default, 2x the cluster's 200ms
    // leaderHeartbeatTimeoutNs, is shorter than an election.
    internal const long NewLeaderTimeoutNs = 5_000_000_000L;

    // Well inside the cluster's sequencer.sessionTimeoutMs (1s), as the twins' is.
    private const long KeepAliveIntervalMs = 200;

    // How long Send spins before calling the session what it has become.
    private const long IngressStallFatalTimeoutMs = 10_000;

    private const long BackpressureAlertIntervalMs = 1_000;

    private readonly YieldingIdleStrategy _idle = new YieldingIdleStrategy();
    private readonly SessionListener _listener;

    // Which offer results are terminal, held where a test can drive it.
    private readonly IngressStallPolicy _stallPolicy =
        new IngressStallPolicy(IngressStallFatalTimeoutMs, BackpressureAlertIntervalMs);

    private Aeron _aeron;
    private AeronCluster _cluster;
    private IEgressListener _appListener;
    private string _egressChannel;
    private string _ingressEndpoints;
    private int _colocatedMemberId = NoMember;
    private bool _sessionLost;
    private int _newLeaderMemberId = NoMember;

    // Whether the open session's ingress is the co-located member's aeron:ipc.
    private bool _overIpc;

    // IPC ingress lost its leader and reaches nobody else, so the session must be replaced.
    private bool _reconnectDue;
    private IIngressTracker _hold;
    private bool _newLeaderDuringSend;
    private long _lastKeepAliveMs;

    /// <summary>A sender with no session; <see cref="ConnectColocated"/> or <see cref="Connect"/> opens
    /// one.</summary>
    public ClusterStreamSender()
    {
        _listener = new SessionListener(this);
    }

    /// <summary>Connects a client co-located with no member: UDP ingress against the whole endpoint set.</summary>
    /// <param name="aeron">the caller's client, left open by <see cref="Dispose"/></param>
    /// <param name="egressChannel">this client's own egress endpoint; two media drivers on one host cannot both bind
    /// a port (doc/ops.md, "Ports")</param>
    /// <param name="ingressEndpoints">the members to reach, <see cref="PortLayout.IngressEndpoints()"/> for the
    /// default set</param>
    /// <param name="appListener">sees every egress event, or null; a gateway's cluster-session fence</param>
    public void Connect(Aeron aeron, string egressChannel, string ingressEndpoints, IEgressListener appListener = null)
    {
        _aeron = aeron;
        _egressChannel = egressChannel;
        _ingressEndpoints = RequireEndpoints(ingressEndpoints);
        _appListener = appListener;
        _cluster = OpenSession(IngressChannelUdp, ingressEndpoints, ConnectTimeoutNs);
    }

    /// <summary>Connects a client sharing a cluster member's Aeron directory: that member's <c>aeron:ipc</c> first,
    /// within the short <paramref name="ipcConnectTimeoutMs"/> since a follower never answers there, then UDP. If
    /// leadership later moves away, <see cref="PollEgress"/> reconnects over UDP; moving back is not
    /// chased.</summary>
    /// <param name="aeron">the caller's client, on the member's media driver</param>
    /// <param name="memberId">the co-located member</param>
    /// <param name="ipcConnectTimeoutMs">how long the member's IPC ingress may take to answer</param>
    /// <param name="egressChannel">this client's own egress endpoint</param>
    /// <param name="ingressEndpoints">the members to reach over UDP</param>
    /// <param name="appListener">sees every egress event, or null</param>
    public void ConnectColocated(Aeron aeron, int memberId, long ipcConnectTimeoutMs, string egressChannel,
                                 string ingressEndpoints, IEgressListener appListener = null)
    {
        _aeron = aeron;
        _colocatedMemberId = memberId;
        _egressChannel = egressChannel;
        _ingressEndpoints = RequireEndpoints(ingressEndpoints);
        _appListener = appListener;
        try
        {
            // No endpoints with IPC ingress: AeronCluster refuses the pair, and there is nothing to name.
            _cluster = OpenSession(IngressChannelIpc, null, ipcConnectTimeoutMs * 1_000_000);
        }
        catch (AeronException ex)
        {
            Logger.Error(Logger.CoreComponent.Cluster, Logger.CoreEventCode.ClusterIpcFallback, memberId,
                         "member {0} did not answer ingress on {1} ({2}) — falling back to UDP", memberId,
                         IngressChannelIpc, ex.Message);
            _cluster = OpenSession(IngressChannelUdp, ingressEndpoints, ConnectTimeoutNs);
        }
    }

    /// <summary>Hears every <c>NewLeader</c>, and gives up a send that met one while it holds.</summary>
    /// <param name="hold">the tracker, normally the producer's <see cref="PendingSends"/></param>
    public void SetIngressHold(IIngressTracker hold)
    {
        _hold = hold;
    }

    /// <summary>
    /// Offers one pre-encoded frame to cluster ingress, spinning until it lands. <c>CLOSED</c> is not terminal:
    /// during an election <c>AeronCluster</c> closes the ingress publication and waits for a <c>NewLeader</c>, which
    /// <see cref="PollEgress"/> installs — hence the spin polls. Bounded, since losing quorum would otherwise stop the
    /// caller's duty cycle. No keep-alive from in here: it would offer on the same stuck publication.
    /// </summary>
    /// <param name="frame">the frame, from offset 0</param>
    /// <param name="length">its length in bytes</param>
    /// <returns>false when there is no session left to take it, or when a <c>NewLeader</c> arrived mid-spin while
    /// the <see cref="IIngressTracker"/> holds: nothing was placed, and the frame goes again once it
    /// releases</returns>
    public bool Send(IDirectBuffer frame, int length)
    {
        if (_cluster == null || _cluster.Closed || length == 0)
        {
            return false;
        }
        if (length > FrameLayer.MaxIngressLength)
        {
            throw new ArgumentException(
                $"ingress frame {length} exceeds MaxIngressLength {FrameLayer.MaxIngressLength}");
        }
        long result;
        _newLeaderDuringSend = false;
        while ((result = _cluster.Offer(frame, 0, length)) < 0)
        {
            long now = Clocks.MonotonicMs();
            switch (_stallPolicy.OnOfferFailed(now, result, _sessionLost, _cluster.Closed))
            {
                case IngressStallPolicy.Action.Fatal:
                    throw new InvalidOperationException("cluster ingress offer failed: " + result);
                case IngressStallPolicy.Action.SessionGone:
                    return false;
                case IngressStallPolicy.Action.Stalled:
                    Logger.Error(Logger.CoreComponent.Cluster, Logger.CoreEventCode.ClusterOfferFailed, Member,
                                 "cluster ingress took no frame for {0}s — calling the session lost",
                                 IngressStallFatalTimeoutMs / 1000);
                    _sessionLost = true;
                    return false;
                case IngressStallPolicy.Action.Alert:
                    Logger.Error(Logger.CoreComponent.Cluster, Logger.CoreEventCode.ClusterOfferFailed, Member,
                                 "cluster ingress back-pressured (offer={0}) for {1}ms", result,
                                 _stallPolicy.BlockedMs(now));
                    break;
            }
            PollEgress();
            if (_newLeaderDuringSend && _hold != null && _hold.IsHolding)
            {
                return false;
            }
            _idle.Idle();
        }
        _stallPolicy.OnOffered();
        return true;
    }

    /// <summary>Sends a keep-alive if the interval has elapsed; call it every duty-cycle iteration.</summary>
    public void KeepAlive()
    {
        if (_cluster == null || _cluster.Closed)
        {
            return;
        }
        long now = Clocks.MonotonicMs();
        if (now - _lastKeepAliveMs < KeepAliveIntervalMs)
        {
            return;
        }
        _lastKeepAliveMs = now;
        if (!_cluster.SendKeepAlive())
        {
            Logger.Error(Logger.CoreComponent.Cluster, Logger.CoreEventCode.ClusterOfferFailed, Member,
                         "keep-alive offer failed");
        }
    }

    /// <summary>Drains cluster egress, then applies any reconnect it called for, outside the poll.</summary>
    /// <returns>fragments read</returns>
    public int PollEgress()
    {
        if (_cluster == null)
        {
            return 0;
        }
        int fragments = _cluster.PollEgress();
        if (_reconnectDue)
        {
            ReconnectOverUdp();
        }
        return fragments;
    }

    /// <summary>Whether a session is open.</summary>
    public bool IsConnected => _cluster != null && !_cluster.Closed;

    /// <summary>True once the cluster has closed this session, as opposed to never having opened one.
    /// Latched.</summary>
    public bool IsSessionLost => _sessionLost;

    /// <inheritdoc/>
    public long ClusterSessionId => _cluster == null ? Aeron.NULL_VALUE : _cluster.ClusterSessionId;

    /// <summary><c>AeronCluster</c> moves its stamp only inside <see cref="PollEgress"/>, which <see cref="Send"/>
    /// polls between failed offers.</summary>
    public long LeadershipTermId => _cluster == null ? Aeron.NULL_VALUE : _cluster.LeadershipTermId;

    /// <summary>Closes the session. The Aeron client is the caller's and is left open.</summary>
    public void Dispose()
    {
        CloseSession();
    }

    private static string RequireEndpoints(string ingressEndpoints)
    {
        // The UDP fallback and the reconnect both need them, so a connect without them could never recover.
        return ingressEndpoints ?? throw new ArgumentNullException(nameof(ingressEndpoints));
    }

    // The context every session opens with, apart from where it connects.
    internal static AeronCluster.Context NewContext(Aeron aeron, string ingressChannel, string ingressEndpoints,
                                                    string egressChannel, IEgressListener listener, long timeoutNs)
    {
        return new AeronCluster.Context()
            .AeronClient(aeron)
            .IngressChannel(ingressChannel)
            .IngressEndpoints(ingressEndpoints)
            .EgressChannel(egressChannel)
            .EgressListener(listener)
            .MessageTimeoutNs(timeoutNs)
            .NewLeaderTimeoutNs(NewLeaderTimeoutNs);
    }

    private AeronCluster OpenSession(string ingressChannel, string ingressEndpoints, long timeoutNs)
    {
        AeronCluster session = AeronCluster.Connect(
            NewContext(_aeron, ingressChannel, ingressEndpoints, _egressChannel, _listener, timeoutNs));
        _overIpc = ingressChannel == IngressChannelIpc;
        _reconnectDue = false;
        return session;
    }

    // Replaces an IPC session whose leader moved away: with no endpoints, AeronCluster would wait on the same
    // aeron:ipc forever. UDP lets the cluster client chase the leader itself.
    private void ReconnectOverUdp()
    {
        Logger.Info(Logger.CoreComponent.Cluster, Member,
                    "leadership moved to member {0} — reconnecting ingress over UDP", _newLeaderMemberId);
        CloseSession();
        _cluster = OpenSession(IngressChannelUdp, _ingressEndpoints, ConnectTimeoutNs);
    }

    private void CloseSession()
    {
        try
        {
            _cluster?.Dispose();
        }
        catch (Exception)
        {
            // As Java's CloseHelper.quietClose: a session that fails to close is gone either way.
        }
        _cluster = null;
    }

    private int? Member => _colocatedMemberId == NoMember ? null : _colocatedMemberId;

    // Watches egress for what this class acts on, and passes every event to the caller's listener.
    private sealed class SessionListener : IEgressListener
    {
        private readonly ClusterStreamSender _sender;

        public SessionListener(ClusterStreamSender sender)
        {
            _sender = sender;
        }

        public void OnMessage(long clusterSessionId, long timestamp, IDirectBuffer buffer, int offset, int length,
                              Header header)
        {
            _sender._appListener?.OnMessage(clusterSessionId, timestamp, buffer, offset, length, header);
        }

        public void OnSessionEvent(long correlationId, long clusterSessionId, long leadershipTermId, int leaderMemberId,
                                   EventCode code, string detail)
        {
            if (code == EventCode.CLOSED || code == EventCode.ERROR)
            {
                _sender._sessionLost = true;
            }
            _sender._appListener?.OnSessionEvent(correlationId, clusterSessionId, leadershipTermId, leaderMemberId,
                                                 code, detail);
        }

        public void OnNewLeader(long clusterSessionId, long leadershipTermId, int leaderMemberId,
                                string ingressEndpoints)
        {
            _sender._newLeaderMemberId = leaderMemberId;
            _sender._newLeaderDuringSend = true;
            _sender._hold?.OnNewLeader(leadershipTermId);
            // Leadership coming back is deliberately not chased, unlike the C++ twin: it would cost a session.
            _sender._reconnectDue = _sender._overIpc && leaderMemberId != _sender._colocatedMemberId;
            _sender._appListener?.OnNewLeader(clusterSessionId, leadershipTermId, leaderMemberId, ingressEndpoints);
        }

        public void OnAdminResponse(long clusterSessionId, long correlationId, AdminRequestType requestType,
                                    AdminResponseCode responseCode, string message, IDirectBuffer payload,
                                    int payloadOffset, int payloadLength)
        {
            _sender._appListener?.OnAdminResponse(clusterSessionId, correlationId, requestType, responseCode, message,
                                                  payload, payloadOffset, payloadLength);
        }
    }
}
