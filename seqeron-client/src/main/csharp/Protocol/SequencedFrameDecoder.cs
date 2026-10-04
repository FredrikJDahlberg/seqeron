using Adaptive.Agrona;
using Org.Limitless.Seqeron.Util;
using Frame = Org.Limitless.Seqeron.Sbe.Frame;
using SbeBuffer = Org.SbeTool.Sbe.Dll.DirectBuffer;

namespace Org.Limitless.Seqeron.Protocol;

/// <summary>
/// One frame off the tap with the envelope stripped; <c>SequencedFrameDecoder.java</c> and <c>FrameView</c> in
/// <c>SequencedFrame.hpp</c> are its twins, and the three stay in step. <see cref="IsSystem"/> says which family:
/// an application frame dispatches on <c>(PayloadId, TemplateId)</c>, never <c>TemplateId</c> alone, and a system
/// frame on <see cref="SystemEventType"/>.
/// <para>A flyweight: <see cref="Buffer"/> points into the caller's storage and every field is valid only until
/// the next <see cref="Wrap"/>.</para>
/// </summary>
public class SequencedFrameDecoder
{
    private readonly SbeBuffer _view = new SbeBuffer();
    private readonly Frame.MessageHeader _messageHeader = new Frame.MessageHeader();
    private readonly Frame.MessageHeader _payloadHeader = new Frame.MessageHeader();
    private readonly Frame.SequencedHeader _frameHeader = new Frame.SequencedHeader();
    private readonly Frame.SequencedSystemHeader _systemHeader = new Frame.SequencedSystemHeader();

    /// <summary>
    /// Reads one fragment, stripping the envelope. <c>false</c> means the fragment is not a whole frame (a damaged
    /// recording) and the caller drops it. It never reads past the fragment and never throws on its contents:
    /// <c>Image.Poll</c> advances past a fragment whose handler throws, so a throw would lose the frame silently.
    /// </summary>
    /// <param name="source">the buffer the fragment is in</param>
    /// <param name="offset">of the fragment's first byte</param>
    /// <param name="length">of the fragment</param>
    /// <returns>whether the fragment was readable as a frame</returns>
    public bool Wrap(IDirectBuffer source, int offset, int length)
    {
        Buffer = source;
        if (length < Frame.MessageHeader.Size)
        {
            return false;
        }
        SbeBuffers.Wrap(_view, source, offset, length);
        _messageHeader.Wrap(_view, 0, Frame.MessageHeader.SbeSchemaVersion);
        if (_messageHeader.SchemaId != Frame.MessageHeader.SbeSchemaId)
        {
            return false;
        }
        int frameTemplateId = _messageHeader.TemplateId;
        if (frameTemplateId == Frame.Sequenced.TemplateId)
        {
            return WrapPayload(source, offset, length);
        }
        if (frameTemplateId == Frame.SequencedSystem.TemplateId)
        {
            return WrapSystemBody(source, offset, length);
        }
        if (frameTemplateId == Frame.ClusterHeartbeat.TemplateId ||
            frameTemplateId == Frame.LeadershipChanged.TemplateId ||
            frameTemplateId == Frame.GatewayActive.TemplateId || frameTemplateId == Frame.SnapshotStarted.TemplateId)
        {
            return WrapSynthesized(offset, length);
        }
        return false;
    }

    // The block of every sequenced template starts right after the framing header; positions below are relative
    // to the fragment, as _view is.
    private const int BlockOffset = Frame.MessageHeader.Size;

    /// <summary>The application family: one opaque payload carrying its own <c>MessageHeader</c>.</summary>
    private bool WrapPayload(IDirectBuffer source, int offset, int length)
    {
        int prefixOffset = BlockOffset + Frame.SequencedHeader.Size;
        if (prefixOffset + Frame.Sequenced.PayloadHeaderSize > length)
        {
            return false;
        }
        IsSystem = false;
        _frameHeader.Wrap(_view, BlockOffset, Frame.Sequenced.SchemaVersion);
        PayloadId = _frameHeader.PayloadId;
        SystemEventType = 0;
        ReadIdentity(_frameHeader.SourceId, _frameHeader.ConnectionId, _frameHeader.SessionId, _frameHeader.GlobalSeqNo,
                     _frameHeader.Timestamp);

        PayloadLength = source.GetShort(offset + prefixOffset, ByteOrder.LittleEndian) & 0xFFFF;
        int payloadStart = prefixOffset + Frame.Sequenced.PayloadHeaderSize;
        PayloadOffset = offset + payloadStart;
        if (payloadStart + PayloadLength > length)
        {
            return false; // a truncated payload: the recording itself is damaged
        }
        // A payload too short for a MessageHeader is still a frame (§5, §13.2) and must reach the consumer (P-3);
        // the three stay 0, which no (PayloadId, TemplateId) dispatch matches.
        if (PayloadLength < Frame.MessageHeader.Size)
        {
            TemplateId = 0;
            BlockLength = 0;
            Version = 0;
            return true;
        }
        _payloadHeader.Wrap(_view, payloadStart, Frame.MessageHeader.SbeSchemaVersion);
        TemplateId = _payloadHeader.TemplateId;
        BlockLength = _payloadHeader.BlockLength;
        Version = _payloadHeader.Version;
        return true;
    }

    /// <summary>A submitted system event: its payload has no header, so a consumer decodes with compiled constants
    /// (<b>V-3</b>).</summary>
    private bool WrapSystemBody(IDirectBuffer source, int offset, int length)
    {
        int prefixOffset = BlockOffset + Frame.SequencedSystemHeader.Size;
        if (prefixOffset + Frame.SequencedSystem.BodyHeaderSize > length)
        {
            return false;
        }
        ReadSystemHeader();
        PayloadLength = source.GetShort(offset + prefixOffset, ByteOrder.LittleEndian) & 0xFFFF;
        int payloadStart = prefixOffset + Frame.SequencedSystem.BodyHeaderSize;
        PayloadOffset = offset + payloadStart;
        return payloadStart + PayloadLength <= length;
    }

    /// <summary>One of the four synthesized events: no payload, its fields inline in the frame's own
    /// block.</summary>
    private bool WrapSynthesized(int offset, int length)
    {
        if (BlockOffset + Frame.SequencedSystemHeader.Size > length)
        {
            return false;
        }
        ReadSystemHeader();
        PayloadOffset = offset + BlockOffset;
        PayloadLength = length - Frame.MessageHeader.Size;
        return true;
    }

    private void ReadSystemHeader()
    {
        IsSystem = true;
        _systemHeader.Wrap(_view, BlockOffset, Frame.SequencedSystem.SchemaVersion);
        SystemEventType = _systemHeader.SystemEventType;
        PayloadId = 0;
        TemplateId = 0;
        BlockLength = 0;
        Version = 0;
        ReadIdentity(_systemHeader.SourceId, _systemHeader.ConnectionId, _systemHeader.SessionId,
                     _systemHeader.GlobalSeqNo, _systemHeader.Timestamp);
    }

    private void ReadIdentity(int sourceId, int connectionId, long sessionId, long globalSeqNo, long timestamp)
    {
        SourceId = sourceId;
        ConnectionId = connectionId;
        SourceSessionId = sessionId;
        GlobalSeqNo = globalSeqNo;
        ClusterTimestampNs = timestamp;
    }

    /// <summary>Whether this frame is one of the system messages; if so <see cref="PayloadId"/> means
    /// nothing.</summary>
    public bool IsSystem { get; private set; }

    /// <summary>Which protocol <see cref="TemplateId"/> belongs to; 0 on a system frame.</summary>
    public int PayloadId { get; private set; }

    /// <summary>Which of §7's events this frame carries; 0 on an application frame.</summary>
    public int SystemEventType { get; private set; }

    /// <summary>Publishing producer process (<c>header.sourceId</c>); -1 on a frame the cluster
    /// synthesized.</summary>
    public int SourceId { get; private set; }

    /// <summary>Connection at that producer (<c>header.connectionId</c>); routes the reply.</summary>
    public int ConnectionId { get; private set; }

    /// <summary>Cluster session the frame was submitted on (<c>header.sessionId</c>), as the sequencer stamped
    /// it.</summary>
    public long SourceSessionId { get; private set; }

    /// <summary>Cluster-wide monotone sequence number; increments by exactly one per frame.</summary>
    public long GlobalSeqNo { get; private set; }

    /// <summary>Cluster consensus time (epoch ns) at which the frame was committed
    /// (<c>header.timestamp</c>).</summary>
    public long ClusterTimestampNs { get; private set; }

    /// <summary>The payload's own templateId, never the envelope's; 0 on a system frame.</summary>
    public int TemplateId { get; private set; }

    /// <summary>
    /// What to wrap a decoder over <see cref="PayloadOffset"/> with on an application frame: the payload's own.
    /// 0 on every system frame, whose decoder's compiled constants are the only ones there are (<b>V-3</b>).
    /// </summary>
    public int BlockLength { get; private set; }

    /// <summary>See <see cref="BlockLength"/>.</summary>
    public int Version { get; private set; }

    /// <summary>Buffer the frame sits in; valid only while the frame it was wrapped over is.</summary>
    public IDirectBuffer Buffer { get; private set; }

    /// <summary>
    /// Offset within <see cref="Buffer"/> of what a consumer decodes: the payload, its own 8-byte
    /// <c>MessageHeader</c> included, on an application frame; the payload on a submitted system frame; the
    /// frame's own block on one of the synthesized four.
    /// </summary>
    public int PayloadOffset { get; private set; }

    /// <summary>Length in bytes of what <see cref="PayloadOffset"/> addresses; the envelope is not in it.</summary>
    public int PayloadLength { get; private set; }
}
