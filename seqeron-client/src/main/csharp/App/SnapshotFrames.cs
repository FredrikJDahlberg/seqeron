using System;
using Adaptive.Agrona.Concurrent;
using Org.Limitless.Seqeron.Protocol;
using Org.Limitless.Seqeron.Util;
using Frame = Org.Limitless.Seqeron.Sbe.Frame;
using SbeBuffer = Org.SbeTool.Sbe.Dll.DirectBuffer;

namespace Org.Limitless.Seqeron.App;

/// <summary>
/// The frame of a round a façade publishes, its <c>SnapshotEnd</c> (doc/snapshot.md §4), under its <c>sourceId</c>
/// and belonging to no connection. <c>SnapshotFrames.java</c> and <c>app/detail/SnapshotFrames.hpp</c> are its
/// twins; keep the three in step.
/// </summary>
internal sealed class SnapshotFrames : SnapshotTaker.IActions
{
    private const int NoConnection = -1;

    private readonly UnsafeBuffer _body = new UnsafeBuffer(GC.AllocateArray<byte>(Frame.SnapshotEnd.BlockLength, true));
    private readonly SbeBuffer _view = new SbeBuffer();
    private readonly Frame.SnapshotEnd _end = new Frame.SnapshotEnd();
    private readonly Session _session;
    private readonly Func<int> _sourceId;

    /// <param name="session">what places the frames</param>
    /// <param name="sourceId">the façade's <c>sourceId</c>, read at each frame: a gateway's resolves from its
    /// row</param>
    internal SnapshotFrames(Session session, Func<int> sourceId)
    {
        _session = session;
        _sourceId = sourceId;
        SbeBuffers.Wrap(_view, _body, 0, Frame.SnapshotEnd.BlockLength);
    }

    public Publish PublishEnd(long round, int recordCount, long length, uint crc32c, int formatVersion)
    {
        _end.WrapForEncode(_view, 0);
        _end.Round = round;
        _end.RecordCount = recordCount;
        _end.Length = length;
        _end.Crc32c = crc32c;
        _end.FormatVersion = (uint)formatVersion;
        return Placed(_session.PublishSystem(_sourceId(), NoConnection, SystemFrame.SnapshotEnd, _body, _end.Size));
    }

    // A refused end is this class's own bug, never a condition to wait out.
    private static Publish Placed(Publish outcome)
    {
        if (outcome == Publish.Refused)
        {
            throw new InvalidOperationException("a SnapshotEnd the sequencer would reject " +
                                                "(doc/seqeron-protocol-spec.md §9.2)");
        }
        return outcome;
    }
}
