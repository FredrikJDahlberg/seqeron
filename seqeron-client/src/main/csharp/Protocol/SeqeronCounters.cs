using System.Text;
using Adaptive.Aeron;
using Adaptive.Agrona.Concurrent;

namespace Org.Limitless.Seqeron.Protocol;

/// <summary>
/// Registry of seqeron's custom Aeron counter type ids (Aeron reserves 0-999). One block per process family, so
/// a range scan (<c>clusterctl counters</c>) finds them all. The ids and key layout match
/// <c>SeqeronCounters.java</c> and <c>SeqeronCounters.hpp</c>.
/// </summary>
public static class SeqeronCounters
{
    // SequencerService (the Aeron Cluster service)

    /// <summary>First type id of the sequencer's block.</summary>
    public const int SequencerTypeIdMin = 5000;

    /// <summary>Last type id of the sequencer's block.</summary>
    public const int SequencerTypeIdMax = 5099;

    /// <summary>The last globalSeqNo emitted on this node's tap.</summary>
    public const int SequencerGlobalSeqNoTypeId = 5000;

    /// <summary>Count of times the tap-emit back-pressure alert threshold has fired.</summary>
    public const int SequencerTapBackpressureAlertsTypeId = 5001;

    /// <summary>Count of malformed ingress messages the sequencer skipped.</summary>
    public const int SequencerRejectedIngressCountTypeId = 5002;

    /// <summary>Count of leadership changes this node has observed and sequenced.</summary>
    public const int SequencerLeadershipChangeCountTypeId = 5003;

    /// <summary>memberId of the leader last recorded by this node's sequencer.</summary>
    public const int SequencerCurrentLeaderMemberIdTypeId = 5004;

    /// <summary>Consensus timestamp of the last 1 Hz <c>ClusterHeartbeat</c> emitted.</summary>
    public const int SequencerLastClusterHeartbeatTimestampTypeId = 5005;

    /// <summary>Count of standby-promotion <c>GatewayActive</c> frames emitted on a gateway session close.</summary>
    public const int SequencerGatewayPromotionCountTypeId = 5006;

    /// <summary>1 once the bootstrap <c>GatewayActive</c> has been emitted in this cluster's log, else 0.</summary>
    public const int SequencerBootstrapActivatedTypeId = 5007;

    /// <summary>1 while tap back-pressure has lasted past the stall threshold with no recording progress, else
    /// 0.</summary>
    public const int SequencerTapStalledTypeId = 5008;

    /// <summary>Current count of TCP clients connected across every gateway, derived from the sequenced
    /// log.</summary>
    public const int SequencerConnectedClientsTypeId = 5009;

    /// <summary>Count of ingress messages successfully sequenced; <c>rate()</c> over this is ingress
    /// throughput.</summary>
    public const int SequencerIngressMessagesTypeId = 5010;

    /// <summary>Count of gateway session closes that found no standby to promote.</summary>
    public const int SequencerGatewayPromotionFailedCountTypeId = 5011;

    // ReplayerService (per-node replay server)

    /// <summary>First type id of the Replayer's block.</summary>
    public const int ReplayerTypeIdMin = 5100;

    /// <summary>Last type id of the Replayer's block.</summary>
    public const int ReplayerTypeIdMax = 5199;

    /// <summary>1 while the local archive is unreachable for replay (live delivery is unaffected), else 0.</summary>
    public const int ReplayerStalledTypeId = 5100;

    /// <summary>1 once the co-located tap recording is visible and replay requests are being served, else
    /// 0.</summary>
    public const int ReplayerReadyTypeId = 5101;

    /// <summary>Current count of in-flight replays.</summary>
    public const int ReplayerActiveSlotsTypeId = 5102;

    /// <summary>Current count of replay requests waiting for a free slot.</summary>
    public const int ReplayerPendingRequestsTypeId = 5103;

    /// <summary>Count of replays started since this node came up.</summary>
    public const int ReplayerReplaysServedCountTypeId = 5104;

    /// <summary>Count of replay slots reclaimed by the idle-TTL backstop rather than a normal release.</summary>
    public const int ReplayerIdleTtlReclaimedCountTypeId = 5105;

    /// <summary>1 once a tap recording has failed the startup integrity check, else 0. Latched.</summary>
    public const int ReplayerIntegrityFailureTypeId = 5106;

    /// <summary>Count of control replies dropped because an app stopped draining the control stream.</summary>
    public const int ReplayerControlRepliesDroppedCountTypeId = 5107;

    /// <summary>1 once two co-located replicas were seen sharing one client id, else 0.</summary>
    public const int ReplayerClientIdCollisionTypeId = 5108;

    /// <summary>The round of one source's latest snapshot this node's Replayer has indexed, keyed
    /// <c>{memberId, sourceId}</c>.</summary>
    public const int ReplayerSnapshotRoundTypeId = 5109;

    // Co-located application replicas (5200-5299). Core reserves 5200; a consumer allocates its own in the range
    // (doc/ops.md, "Counter type ids").

    /// <summary>First type id of the app range: core reserves <see cref="AppRecoveryStalledTypeId"/> and no
    /// more.</summary>
    public const int AppTypeIdMin = 5200;

    /// <summary>Last type id of the app range.</summary>
    public const int AppTypeIdMax = 5299;

    /// <summary>1 while a co-located replica's recovery has dispatched nothing while not caught up, else
    /// 0.</summary>
    public const int AppRecoveryStalledTypeId = 5200;

    /// <summary>First type id seqeron owns, for a typeId-range scan.</summary>
    public const int MinTypeId = SequencerTypeIdMin;

    /// <summary>Last type id seqeron owns.</summary>
    public const int MaxTypeId = AppTypeIdMax;

    /// <summary>Offset of the memberId int within a counter's key.</summary>
    public const int KeyMemberIdOffset = 0;

    /// <summary>
    /// Offset of the replayer clientId in an <b>app</b> counter's key, so a node's several replicas render as
    /// separate series. Only the app range carries it.
    /// </summary>
    public const int KeyClientIdOffset = sizeof(int);

    /// <summary>Offset of the <c>sourceId</c> in a per-source counter's key.</summary>
    public const int KeySourceIdOffset = sizeof(int);

    /// <summary>Length of an operator counter's key: one int, the memberId.</summary>
    public const int KeyLength = sizeof(int);

    /// <summary>Length of an app or per-source counter's key: memberId, then clientId or sourceId.</summary>
    public const int AppKeyLength = 2 * sizeof(int);

    /// <summary>Allocates an operator counter keyed on <paramref name="memberId"/>, so a reader recovers the member
    /// without parsing the label.</summary>
    /// <param name="aeron">the client to allocate it through</param>
    /// <param name="typeId">its type id</param>
    /// <param name="label">its label, US-ASCII</param>
    /// <param name="memberId">the member it describes</param>
    public static Counter AddCounter(Aeron aeron, int typeId, string label, int memberId)
    {
        using var key = new UnsafeBuffer(new byte[KeyLength]);
        key.PutInt(KeyMemberIdOffset, memberId);
        return Add(aeron, typeId, key, label);
    }

    /// <summary>Allocates an <b>app</b> counter keyed on <c>{memberId, clientId}</c>, the layout the Java and C++
    /// twins write.</summary>
    /// <param name="aeron">the client to allocate it through</param>
    /// <param name="typeId">its type id, in the app range</param>
    /// <param name="label">its label, US-ASCII</param>
    /// <param name="memberId">the replica's node</param>
    /// <param name="clientId">the replica's Replayer client id</param>
    public static Counter AddAppCounter(Aeron aeron, int typeId, string label, int memberId, int clientId)
    {
        using var key = new UnsafeBuffer(new byte[AppKeyLength]);
        key.PutInt(KeyMemberIdOffset, memberId);
        key.PutInt(KeyClientIdOffset, clientId);
        return Add(aeron, typeId, key, label);
    }

    /// <summary>Allocates a per-source counter keyed on <c>{memberId, sourceId}</c>, the app counters'
    /// layout.</summary>
    /// <param name="aeron">the client to allocate it through</param>
    /// <param name="typeId">its type id</param>
    /// <param name="label">its label, US-ASCII</param>
    /// <param name="memberId">the member it describes</param>
    /// <param name="sourceId">the source it describes</param>
    public static Counter AddSourceCounter(Aeron aeron, int typeId, string label, int memberId, int sourceId)
    {
        using var key = new UnsafeBuffer(new byte[AppKeyLength]);
        key.PutInt(KeyMemberIdOffset, memberId);
        key.PutInt(KeySourceIdOffset, sourceId);
        return Add(aeron, typeId, key, label);
    }

    private static Counter Add(Aeron aeron, int typeId, UnsafeBuffer key, string label)
    {
        byte[] labelBytes = Encoding.ASCII.GetBytes(label);
        using var labelBuffer = new UnsafeBuffer(labelBytes);
        return aeron.AddCounter(typeId, key, 0, key.Capacity, labelBuffer, 0, labelBytes.Length);
    }
}
