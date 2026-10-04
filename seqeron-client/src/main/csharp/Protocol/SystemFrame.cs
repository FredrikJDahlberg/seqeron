using Adaptive.Agrona;
using Org.Limitless.Seqeron.Util;
using Frame = Org.Limitless.Seqeron.Sbe.Frame;
using SbeBuffer = Org.SbeTool.Sbe.Dll.DirectBuffer;

namespace Org.Limitless.Seqeron.Protocol;

/// <summary>
/// The ingress side of the frame layer: wraps an already-encoded payload in its family's envelope and returns the
/// length; the offer is the caller's. A system payload carries no <c>MessageHeader</c>: it is encoded bare, and
/// decoded with its codec's compiled constants (§7, <b>V-3</b>).
/// <para>Not thread-safe: one instance per producing thread, reusing its encoders.</para>
/// </summary>
public sealed class SystemFrame
{
    /// <summary>
    /// Returned instead of a length when the payload is above <see cref="FrameLayer.MaxPayloadLength"/>, or the
    /// frame breaks §9.2 conditions 6 to 9: the checks a producer can make without the sequencer's state, so a
    /// frame the sequencer would drop is never sent (<b>T-3</b>). Local and permanent: nothing was encoded, and
    /// retrying cannot succeed.
    /// </summary>
    public const int Refused = -1;

    /// <summary><see cref="IngressBlockLength"/>'s answer for a <c>systemEventType</c> that may not be
    /// submitted.</summary>
    public const int NotIngressLegal = -1;

    /// <summary><c>sourceId</c> -1 is the cluster's own (<b>F-4</b>); §9.2 condition 6 refuses it on
    /// ingress.</summary>
    private const int ClusterSourceId = -1;

    /// <summary>§9.2 condition 7: 0 names no protocol, and 1 is core's retired id.</summary>
    private const int RetiredCorePayloadId = 1;

    // The systemEventType table (§7). A submitted event's value is its payload codec's template id; the four
    // synthesized events have templates of their own and stamp these at offset 16, so that field discriminates
    // every frame on the tap. SystemFrame.java and SequencedFrame.hpp hold the same table.

    /// <summary>A connection opened at a producer; <c>header.connectionId</c> names it.</summary>
    public const int ConnectionOpened = Frame.ConnectionOpened.TemplateId;

    /// <summary>A connection closed at a producer; <c>header.connectionId</c> names it.</summary>
    public const int ConnectionClosed = Frame.ConnectionClosed.TemplateId;

    /// <summary>Synthesis-only; <c>LeadershipChanged.TemplateId</c> is the frame's, not this.</summary>
    public const int LeadershipChanged = 5;

    /// <summary>Operator marker: the cluster is up and open for the day.</summary>
    public const int ClusterStarted = Frame.ClusterStarted.TemplateId;

    /// <summary>Operator marker: an orderly shutdown, the last business event in the log.</summary>
    public const int ClusterStopped = Frame.ClusterStopped.TemplateId;

    /// <summary>Synthesis-only.</summary>
    public const int ClusterHeartbeat = 16;

    /// <summary>One row of the topology list, and its <c>remaining</c> is the completeness edge.</summary>
    public const int GatewayRegistered = Frame.GatewayRegistered.TemplateId;

    /// <summary>Synthesis-only. An operator asks for one with <see cref="GatewayActivationRequested"/>.</summary>
    public const int GatewayActive = 18;

    /// <summary>A gateway instance announcing itself, which binds its session to its <c>gatewayId</c>.</summary>
    public const int GatewayStarted = Frame.GatewayStarted.TemplateId;

    /// <summary>One <c>payloadId</c>'s name in this deployment; labelling only.</summary>
    public const int PayloadIdRegistered = Frame.PayloadIdRegistered.TemplateId;

    /// <summary>An operator asking for one instance to be made active.</summary>
    public const int GatewayActivationRequested = Frame.GatewayActivationRequested.TemplateId;

    /// <summary>One co-located application's row; labelling only, and no election behind it.</summary>
    public const int ApplicationRegistered = Frame.ApplicationRegistered.TemplateId;

    /// <summary>An operator asking for a snapshot round.</summary>
    public const int SnapshotRequested = Frame.SnapshotRequested.TemplateId;

    /// <summary>A source's snapshot of one round: its digest, the bytes staying with each instance.</summary>
    public const int SnapshotEnd = Frame.SnapshotEnd.TemplateId;

    /// <summary>The topology file's snapshot policy.</summary>
    public const int SnapshotPolicyRegistered = Frame.SnapshotPolicyRegistered.TemplateId;

    /// <summary>Synthesis-only; <c>SnapshotStarted.TemplateId</c> is the frame's, not this.</summary>
    public const int SnapshotStarted = 30;

    /// <summary>
    /// The compiled block length of the event <paramref name="systemEventType"/> names, or
    /// <see cref="NotIngressLegal"/> if it is unallocated or synthesis-only: §9.2 conditions 8 and 9 in one lookup.
    /// </summary>
    /// <param name="systemEventType">the event a frame names</param>
    public static int IngressBlockLength(int systemEventType)
    {
        switch (systemEventType)
        {
            case ConnectionOpened:
                return Frame.ConnectionOpened.BlockLength;
            case ConnectionClosed:
                return Frame.ConnectionClosed.BlockLength;
            case ClusterStarted:
                return Frame.ClusterStarted.BlockLength;
            case ClusterStopped:
                return Frame.ClusterStopped.BlockLength;
            case GatewayRegistered:
                return Frame.GatewayRegistered.BlockLength;
            case GatewayStarted:
                return Frame.GatewayStarted.BlockLength;
            case PayloadIdRegistered:
                return Frame.PayloadIdRegistered.BlockLength;
            case GatewayActivationRequested:
                return Frame.GatewayActivationRequested.BlockLength;
            case ApplicationRegistered:
                return Frame.ApplicationRegistered.BlockLength;
            case SnapshotRequested:
                return Frame.SnapshotRequested.BlockLength;
            case SnapshotEnd:
                return Frame.SnapshotEnd.BlockLength;
            case SnapshotPolicyRegistered:
                return Frame.SnapshotPolicyRegistered.BlockLength;
            default:
                return NotIngressLegal;
        }
    }

    private readonly Frame.MessageHeader _headerEncoder = new Frame.MessageHeader();
    private readonly Frame.UnsequencedSystem _systemEncoder = new Frame.UnsequencedSystem();
    private readonly Frame.Unsequenced _payloadEncoder = new Frame.Unsequenced();
    private readonly SbeBuffer _view = new SbeBuffer();

    /// <summary>Encodes one <c>UnsequencedSystem</c> frame around <paramref name="body"/>.</summary>
    /// <param name="frame">where the frame is written, from offset 0</param>
    /// <param name="sourceId">the producing process's <c>gatewaySourceId</c>; -1 is reserved for the cluster</param>
    /// <param name="connectionId">the connection this frame belongs to, or -1 for a producer-scoped one</param>
    /// <param name="sessionId">this process's cluster session; advisory, the sequencer overwrites it</param>
    /// <param name="systemEventType">which of §7's submitted events <paramref name="body"/> holds</param>
    /// <param name="body">the event's SBE block, with no <c>MessageHeader</c> of its own</param>
    /// <param name="bodyLength">bytes of <paramref name="body"/> to carry</param>
    /// <returns>the frame's length in bytes, or <see cref="Refused"/> if <paramref name="bodyLength"/> is above
    /// <see cref="FrameLayer.MaxPayloadLength"/> or the frame breaks §9.2 condition 6, 8 or 9</returns>
    public int Wrap(IMutableDirectBuffer frame, int sourceId, int connectionId, long sessionId, int systemEventType,
                    IDirectBuffer body, int bodyLength)
    {
        // Ahead of every write, so a refusal leaves the frame exactly as it found it (T-3).
        int blockLength = IngressBlockLength(systemEventType);
        if (bodyLength > FrameLayer.MaxPayloadLength || sourceId == ClusterSourceId || blockLength == NotIngressLegal ||
            bodyLength < blockLength)
        {
            return Refused;
        }
        WrapView(frame, FrameLayer.MinIngressLength + bodyLength);
        _systemEncoder.WrapForEncodeAndApplyHeader(_view, 0, _headerEncoder);
        Frame.UnsequencedSystemHeader header = _systemEncoder.Header;
        header.SourceId = sourceId;
        header.ConnectionId = connectionId;
        header.SessionId = sessionId;
        header.SystemEventType = (ushort)systemEventType;
        _systemEncoder.SetBody(SbeBuffers.Bytes(body, 0, bodyLength));
        return Frame.MessageHeader.Size + _systemEncoder.Size;
    }

    /// <summary>The same, for an application's own payload rather than a system event's.</summary>
    /// <param name="frame">where the frame is written, from offset 0</param>
    /// <param name="sourceId">the producing process's <c>gatewaySourceId</c>; -1 is reserved for the cluster</param>
    /// <param name="connectionId">the connection this frame belongs to, or -1 for a producer-scoped one</param>
    /// <param name="sessionId">this process's cluster session; advisory, the sequencer overwrites it</param>
    /// <param name="payloadId">names the payload's decoder namespace and encoding; 0 and 1 are invalid on the
    /// wire</param>
    /// <param name="payload">the payload, its own 8-byte <c>MessageHeader</c> included</param>
    /// <param name="payloadLength">bytes of <paramref name="payload"/> to carry</param>
    /// <returns>the frame's length in bytes, or <see cref="Refused"/> if <paramref name="payloadLength"/> is
    /// above <see cref="FrameLayer.MaxPayloadLength"/> or the frame breaks §9.2 condition 6 or 7</returns>
    public int WrapPayload(IMutableDirectBuffer frame, int sourceId, int connectionId, long sessionId, int payloadId,
                           IDirectBuffer payload, int payloadLength)
    {
        if (payloadLength > FrameLayer.MaxPayloadLength || sourceId == ClusterSourceId || payloadId == 0 ||
            payloadId == RetiredCorePayloadId)
        {
            return Refused;
        }
        WrapView(frame, FrameLayer.MinIngressLength + payloadLength);
        _payloadEncoder.WrapForEncodeAndApplyHeader(_view, 0, _headerEncoder);
        Frame.UnsequencedHeader header = _payloadEncoder.Header;
        header.SourceId = sourceId;
        header.ConnectionId = connectionId;
        header.SessionId = sessionId;
        header.PayloadId = (ushort)payloadId;
        _payloadEncoder.SetPayload(SbeBuffers.Bytes(payload, 0, payloadLength));
        return Frame.MessageHeader.Size + _payloadEncoder.Size;
    }

    // An expandable frame grows to the length first, as Agrona's does under Java's encoders.
    private void WrapView(IMutableDirectBuffer frame, int length)
    {
        frame.CheckLimit(length);
        SbeBuffers.Wrap(_view, frame, 0, length);
    }
}
