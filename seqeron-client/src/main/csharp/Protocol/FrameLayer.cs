using Org.Limitless.Seqeron.Sbe.Frame;

namespace Org.Limitless.Seqeron.Protocol;

/// <summary>
/// The frame layer's constants: the tap's identity, the cluster clock, and the size limits of spec §12. The C#
/// mirror of the spec, as <c>FrameLayer.java</c> and <c>SequencedFrame.hpp</c> are; a change is a wire change
/// (<b>V-3</b>) and lands in all three. Never read from a transport: <b>S-3</b> forbids checking a node's own
/// MTU, which would fork <c>globalSeqNo</c>.
/// </summary>
public static class FrameLayer
{
    /// <summary>
    /// Largest payload any frame may carry (<b>T-2</b>): an 8960-byte IPC MTU, one jumbo frame, less the 76 bytes
    /// of Aeron data header and sequenced envelope, so a tap whose MTU is that large never fragments.
    /// </summary>
    public const int MaxPayloadLength = 8884;

    /// <summary>
    /// Smallest ingress frame of either family, 28 bytes: framing header, 18-byte header composite and the
    /// payload's 2-byte length prefix. An empty payload is legal.
    /// </summary>
    public const int MinIngressLength = MessageHeader.Size + UnsequencedHeader.Size + Unsequenced.PayloadHeaderSize;

    /// <summary>Largest ingress frame: a full payload behind that framing, 8912 bytes. §9.2 condition 1's
    /// ceiling.</summary>
    public const int MaxIngressLength = MinIngressLength + MaxPayloadLength;

    /// <summary>
    /// The tap: the node-local IPC stream every node republishes each sequenced frame on, in
    /// <c>globalSeqNo</c> order, and records into its own archive. Co-located apps follow it live.
    /// </summary>
    public const string FeederChannel = "aeron:ipc";

    /// <summary>Stream id of the tap.</summary>
    public const int FeederStreamId = 205;

    /// <summary>
    /// Period of the cluster clock: every node emits a <c>ClusterHeartbeat</c> carrying the consensus timestamp,
    /// so consumers have a clock that advances while producers are silent. A consumer's tap watchdog must span
    /// whole periods.
    /// </summary>
    public const long ClusterHeartbeatIntervalMs = 1000;

    /// <summary><see cref="ClusterHeartbeatIntervalMs"/> in consensus time, which is epoch nanoseconds.</summary>
    public const long ClusterHeartbeatIntervalNs = ClusterHeartbeatIntervalMs * 1_000_000;
}
