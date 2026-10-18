using Org.Limitless.Seqeron.Sbe.Frame;

namespace Org.Limitless.Seqeron.Protocol;

/// <summary>
/// The frame layer's constants: the queue's identity, the cluster clock, and the size limits of spec §12. The C#
/// mirror of the spec, as <c>FrameLayer.java</c> and <c>SequencedFrame.hpp</c> are; a change is a wire change
/// (<b>V-3</b>) and lands in all three. Never read from a transport: <b>S-3</b> forbids checking a node's own
/// MTU, which would fork <c>clusterSeqNo</c>.
/// </summary>
public static class FrameLayer
{
    /// <summary>
    /// Largest payload any frame may carry (<b>T-2</b>): an 8960-byte IPC MTU, one jumbo frame, less the 86 bytes
    /// of Aeron data header and sequenced envelope, so a queue whose MTU is that large never fragments.
    /// </summary>
    public const int MaxPayloadLength = 8874;

    /// <summary>
    /// Smallest ingress frame of either family, 30 bytes: framing header, 20-byte header composite and the
    /// payload's 2-byte length prefix. An empty payload is legal.
    /// </summary>
    public const int MinIngressLength = MessageHeader.Size + UnsequencedHeader.Size + Unsequenced.PayloadHeaderSize;

    /// <summary>Largest ingress frame: a full payload behind that framing, 8904 bytes. §9.2 condition 1's
    /// ceiling.</summary>
    public const int MaxIngressLength = MinIngressLength + MaxPayloadLength;

    /// <summary>
    /// The queue: the node-local IPC stream every node republishes each sequenced frame on, in
    /// <c>clusterSeqNo</c> order, and records into its own archive. Co-located apps follow it live. Each group's
    /// queue is this channel on its own stream, <see cref="QueueStreamIdOf"/>.
    /// </summary>
    public const string QueueChannel = "aeron:ipc?alias=seqeron-queue";

    /// <summary>The width of <c>groupMask</c>: <c>groupId</c>s run 1 to 16.</summary>
    public const int MaxGroups = 16;

    /// <summary>Group <c>groupId</c>'s queue is stream <c>QueueStreamIdBase + groupId</c>.</summary>
    public const int QueueStreamIdBase = 2000;

    /// <summary>The stream id of a group's queue.</summary>
    /// <param name="groupId">1 to <see cref="MaxGroups"/></param>
    public static int QueueStreamIdOf(int groupId)
    {
        return QueueStreamIdBase + groupId;
    }

    /// <summary>
    /// Period of the cluster clock: every node emits a <c>ClusterHeartbeat</c> carrying the consensus timestamp,
    /// so consumers have a clock that advances while producers are silent. A consumer's queue watchdog must span
    /// whole periods.
    /// </summary>
    public const long ClusterHeartbeatIntervalMs = 1000;

    /// <summary><see cref="ClusterHeartbeatIntervalMs"/> in consensus time, which is epoch nanoseconds.</summary>
    public const long ClusterHeartbeatIntervalNs = ClusterHeartbeatIntervalMs * 1_000_000;
}
