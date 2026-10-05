using System;
using Adaptive.Agrona.Concurrent;
using Org.Limitless.Seqeron.Protocol;
using Org.SbeTool.Sbe.Dll;
using Frame = Org.Limitless.Seqeron.Sbe.Frame;
using Replay = Org.Limitless.Seqeron.Sbe.Replay;

namespace Org.Limitless.Seqeron.Replayer.Client;

/// <summary>
/// A snapshot as a restore meets it (doc/snapshot.md §5, §7): the instance's own file, the round's frames, and the
/// Replayer's answer confirming it.
/// </summary>
internal static class RestoreFrames
{
    /// <summary>What a <c>SnapshotEnd</c> says of a snapshot's records.</summary>
    public sealed record Digest(int RecordCount, long Length, uint Crc32C)
    {
        public static Digest Of(params byte[][] records)
        {
            long length = 0;
            uint crc = 0;
            foreach (byte[] record in records)
            {
                length += record.Length;
                crc = SnapshotFormat.Update(crc, record);
            }
            return new Digest(records.Length, length, crc);
        }
    }

    public static byte[] Started(long globalSeqNo, long round)
    {
        return Frames.SnapshotStarted(globalSeqNo, round);
    }

    /// <summary>The end the records <paramref name="records"/> match.</summary>
    public static byte[] End(long globalSeqNo, int sourceId, long round, int formatVersion, params byte[][] records)
    {
        Digest digest = Digest.Of(records);
        var body = new byte[Frame.SnapshotEnd.BlockLength];
        var encoder = new Frame.SnapshotEnd();
        encoder.WrapForEncode(new DirectBuffer(body), 0);
        encoder.Round = round;
        encoder.RecordCount = digest.RecordCount;
        encoder.Length = digest.Length;
        encoder.Crc32c = digest.Crc32C;
        encoder.FormatVersion = (uint)formatVersion;

        var bytes = new byte[256];
        var frame = new Frame.SequencedSystem();
        frame.WrapForEncodeAndApplyHeader(new DirectBuffer(bytes), 0, new Frame.MessageHeader());
        Frame.SequencedSystemHeader header = frame.Header;
        header.SourceId = sourceId;
        header.ConnectionId = -1;
        header.SessionId = 5;
        header.SystemEventType = SystemFrame.SnapshotEnd;
        header.GlobalSeqNo = globalSeqNo;
        header.Timestamp = globalSeqNo * 1000;
        frame.SetBody(body);
        return bytes[..(Frame.MessageHeader.Size + frame.Size)];
    }

    /// <summary>Writes <paramref name="records"/> as round <paramref name="round"/>'s file, as the instance did when
    /// it serialized them.</summary>
    public static void Write(SnapshotStore store, long round, int formatVersion, params byte[][] records)
    {
        Digest digest = Digest.Of(records);
        store.Begin(round);
        foreach (byte[] record in records)
        {
            using var buffer = new UnsafeBuffer(record);
            store.Append(buffer, 0, record.Length);
        }
        store.Commit(digest.RecordCount, digest.Length, digest.Crc32C, formatVersion);
    }

    /// <summary>An application's header record.</summary>
    public static byte[] Header(long leadershipTermId, int leaderMemberId)
    {
        var header = new SnapshotHeader(leadershipTermId, leaderMemberId, null);
        var bytes = new byte[header.EncodedLength];
        using var buffer = new UnsafeBuffer(bytes);
        header.Encode(buffer, 0);
        return bytes;
    }

    /// <summary>A record holding one long.</summary>
    public static byte[] Record(long value)
    {
        return BitConverter.GetBytes(value);
    }

    /// <summary>The Replayer's answer: round <paramref name="round"/>'s cut and sequenced end, or none for round
    /// -1.</summary>
    public static byte[] Location(int clientId, long requestId, long round, long asOfGlobalSeqNo, long asOfPosition,
                                  long formatVersion, Digest end)
    {
        var bytes = new byte[Replay.MessageHeader.Size + Replay.SnapshotLocation.BlockLength];
        var encoder = new Replay.SnapshotLocation();
        encoder.WrapForEncodeAndApplyHeader(new DirectBuffer(bytes), 0, new Replay.MessageHeader());
        encoder.ClientId = clientId;
        encoder.RequestId = requestId;
        encoder.Round = round;
        encoder.AsOfGlobalSeqNo = asOfGlobalSeqNo;
        encoder.AsOfPosition = asOfPosition;
        encoder.FormatVersion = (uint)formatVersion;
        encoder.RecordCount = end.RecordCount;
        encoder.Length = end.Length;
        encoder.Crc32c = end.Crc32C;
        return bytes;
    }
}

/// <summary>The Replayer's other replies on the control stream, as <see cref="ReplayerRecovery.OnControl"/> reads
/// them.</summary>
internal static class ControlMessages
{
    public static byte[] Replaying(int clientId, long requestId, long replaySessionId, long catchUpPosition)
    {
        var bytes = new byte[Replay.MessageHeader.Size + Replay.Replaying.BlockLength];
        var encoder = new Replay.Replaying();
        encoder.WrapForEncodeAndApplyHeader(new DirectBuffer(bytes), 0, new Replay.MessageHeader());
        encoder.ClientId = clientId;
        encoder.RequestId = requestId;
        encoder.ReplaySessionId = replaySessionId;
        encoder.CatchUpPosition = catchUpPosition;
        return bytes;
    }

    public static byte[] ReplayPending(int clientId, long requestId)
    {
        var bytes = new byte[Replay.MessageHeader.Size + Replay.ReplayPending.BlockLength];
        var encoder = new Replay.ReplayPending();
        encoder.WrapForEncodeAndApplyHeader(new DirectBuffer(bytes), 0, new Replay.MessageHeader());
        encoder.ClientId = clientId;
        encoder.RequestId = requestId;
        return bytes;
    }

    public static byte[] ReplayUnavailable(int clientId, long requestId)
    {
        var bytes = new byte[Replay.MessageHeader.Size + Replay.ReplayUnavailable.BlockLength];
        var encoder = new Replay.ReplayUnavailable();
        encoder.WrapForEncodeAndApplyHeader(new DirectBuffer(bytes), 0, new Replay.MessageHeader());
        encoder.ClientId = clientId;
        encoder.RequestId = requestId;
        return bytes;
    }

    public static byte[] ReplayClientIdInUse(int clientId)
    {
        var bytes = new byte[Replay.MessageHeader.Size + Replay.ReplayClientIdInUse.BlockLength];
        var encoder = new Replay.ReplayClientIdInUse();
        encoder.WrapForEncodeAndApplyHeader(new DirectBuffer(bytes), 0, new Replay.MessageHeader());
        encoder.ClientId = clientId;
        return bytes;
    }
}
