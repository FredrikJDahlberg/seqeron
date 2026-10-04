using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Adaptive.Agrona;

namespace Org.Limitless.Seqeron.Protocol;

/// <summary>
/// The header a façade writes ahead of the application's body: what it derives from frames before the round's
/// cut, which a restore never replays (doc/snapshot.md §6). Little-endian, packed:
/// <code>
///  0  headerVersion       uint16   HeaderVersion
///  2  headerLength        uint32   bytes up to the body
///  6  leadershipTermId    int64    from the last LeadershipChanged before the cut
/// 14  leaderMemberId      int32    that frame's newLeaderMemberId
///     — a gateway's header continues —
/// 18  gatewaySourceId     int32
/// 22  activeGatewayId     int32    the current GatewayActive's, or NoGateway
/// 26  highestConnectionId int32    the highest in the source's history
/// 30  rowCount            uint16
/// 32  rows, in list order: gatewayId int32, preferenceRank uint8, gatewayName char[32]
/// </code>
/// Every instance of a source writes the same header (A-6), so it carries the rows of every instance of a gateway
/// and an instance finds its own by name, as it does in the list. <c>SnapshotHeader.java</c> and
/// <c>Snapshot.hpp</c> are its twins; keep the three in step.
/// </summary>
/// <param name="LeadershipTermId">the term of the last <c>LeadershipChanged</c> before the cut</param>
/// <param name="LeaderMemberId">that term's leader</param>
/// <param name="Gateway">a gateway's continuation, or null for an application's header</param>
public sealed record SnapshotHeader(long LeadershipTermId, int LeaderMemberId, SnapshotHeader.GatewayState Gateway)
{
    /// <summary>The one <c>headerVersion</c> this build writes and reads.</summary>
    public const int HeaderVersion = 1;

    /// <summary>An application's header, and the start of a gateway's continuation.</summary>
    public const int ApplicationLength = 18;

    /// <summary>No <c>GatewayActive</c> yet.</summary>
    public const int NoGateway = -1;

    private const int GatewayFixedLength = 14;
    private const int GatewayNameLength = 32;
    private const int GatewayRowLength = 5 + GatewayNameLength;
    private const ByteOrder LE = ByteOrder.LittleEndian;

    /// <summary>A gateway's continuation.</summary>
    /// <param name="GatewaySourceId">the logical gateway</param>
    /// <param name="ActiveGatewayId">the instance the current <c>GatewayActive</c> names, or
    /// <see cref="NoGateway"/></param>
    /// <param name="HighestConnectionId">the highest <c>connectionId</c> in the source's history</param>
    /// <param name="Rows">the list rows of every instance of the gateway, in list order</param>
    public sealed record GatewayState(int GatewaySourceId, int ActiveGatewayId, int HighestConnectionId,
                                      IReadOnlyList<GatewayRow> Rows)
    {
        /// <summary>Equal when every field is, the rows compared in order.</summary>
        /// <param name="other">the state to compare with</param>
        public bool Equals(GatewayState other)
        {
            return other is not null && GatewaySourceId == other.GatewaySourceId &&
                   ActiveGatewayId == other.ActiveGatewayId && HighestConnectionId == other.HighestConnectionId &&
                   Rows.SequenceEqual(other.Rows);
        }

        /// <inheritdoc/>
        public override int GetHashCode()
        {
            return HashCode.Combine(GatewaySourceId, ActiveGatewayId, HighestConnectionId, Rows.Count);
        }
    }

    /// <summary>One instance's list row.</summary>
    /// <param name="GatewayId">the instance</param>
    /// <param name="PreferenceRank">its rank</param>
    /// <param name="GatewayName">its name, at most 32 US-ASCII characters</param>
    public sealed record GatewayRow(int GatewayId, int PreferenceRank, string GatewayName);

    /// <summary>Bytes <see cref="Encode"/> writes; the value of <c>headerLength</c>.</summary>
    public int EncodedLength => Gateway == null
                                    ? ApplicationLength
                                    : ApplicationLength + GatewayFixedLength + GatewayRowLength * Gateway.Rows.Count;

    /// <summary>Writes the header.</summary>
    /// <param name="buffer">where to write</param>
    /// <param name="offset">of the header's first byte</param>
    /// <returns>the bytes written, <see cref="EncodedLength"/></returns>
    public int Encode(IMutableDirectBuffer buffer, int offset)
    {
        int length = EncodedLength;
        buffer.CheckLimit(offset + length);
        buffer.PutShort(offset, HeaderVersion, LE);
        buffer.PutInt(offset + 2, length, LE);
        buffer.PutLong(offset + 6, LeadershipTermId, LE);
        buffer.PutInt(offset + 14, LeaderMemberId, LE);
        if (Gateway != null)
        {
            buffer.PutInt(offset + 18, Gateway.GatewaySourceId, LE);
            buffer.PutInt(offset + 22, Gateway.ActiveGatewayId, LE);
            buffer.PutInt(offset + 26, Gateway.HighestConnectionId, LE);
            buffer.PutShort(offset + 30, (short)Gateway.Rows.Count, LE);
            int row = offset + ApplicationLength + GatewayFixedLength;
            foreach (GatewayRow each in Gateway.Rows)
            {
                buffer.PutInt(row, each.GatewayId, LE);
                buffer.PutByte(row + 4, (byte)each.PreferenceRank);
                byte[] name = Encoding.ASCII.GetBytes(each.GatewayName);
                int nameLength = Math.Min(name.Length, GatewayNameLength);
                buffer.PutBytes(row + 5, name, 0, nameLength);
                buffer.SetMemory(row + 5 + nameLength, GatewayNameLength - nameLength, 0);
                row += GatewayRowLength;
            }
        }
        return length;
    }

    /// <summary>The <c>headerVersion</c> at <paramref name="offset"/>, or -1 if <paramref name="length"/> cannot
    /// hold one.</summary>
    /// <param name="buffer">holding the snapshot</param>
    /// <param name="offset">of its first byte</param>
    /// <param name="length">the snapshot's bytes from <paramref name="offset"/></param>
    public static int Version(IDirectBuffer buffer, int offset, int length)
    {
        return length < 2 ? -1 : buffer.GetShort(offset, LE) & 0xFFFF;
    }

    /// <summary>Reads a header written by <see cref="Encode"/>.</summary>
    /// <param name="buffer">holding the snapshot</param>
    /// <param name="offset">of its first byte</param>
    /// <param name="length">the snapshot's bytes from <paramref name="offset"/></param>
    /// <returns>the header, or null if it is not a <see cref="HeaderVersion"/> header or does not fit
    /// <paramref name="length"/></returns>
    public static SnapshotHeader Decode(IDirectBuffer buffer, int offset, int length)
    {
        if (length < ApplicationLength || Version(buffer, offset, length) != HeaderVersion)
        {
            return null;
        }
        long headerLength = buffer.GetInt(offset + 2, LE) & 0xFFFF_FFFFL;
        long leadershipTermId = buffer.GetLong(offset + 6, LE);
        int leaderMemberId = buffer.GetInt(offset + 14, LE);
        if (headerLength == ApplicationLength)
        {
            return new SnapshotHeader(leadershipTermId, leaderMemberId, null);
        }
        if (headerLength > length || headerLength < ApplicationLength + GatewayFixedLength)
        {
            return null;
        }
        int rowCount = buffer.GetShort(offset + 30, LE) & 0xFFFF;
        if (headerLength != ApplicationLength + GatewayFixedLength + (long)GatewayRowLength * rowCount)
        {
            return null;
        }
        var rows = new GatewayRow[rowCount];
        int row = offset + ApplicationLength + GatewayFixedLength;
        for (int i = 0; i < rowCount; i++)
        {
            int nameLength = 0;
            while (nameLength < GatewayNameLength && buffer.GetByte(row + 5 + nameLength) != 0)
            {
                nameLength++;
            }
            rows[i] = new GatewayRow(buffer.GetInt(row, LE), buffer.GetByte(row + 4),
                                     buffer.GetStringWithoutLengthAscii(row + 5, nameLength));
            row += GatewayRowLength;
        }
        return new SnapshotHeader(leadershipTermId, leaderMemberId,
                                  new GatewayState(buffer.GetInt(offset + 18, LE), buffer.GetInt(offset + 22, LE),
                                                   buffer.GetInt(offset + 26, LE), Array.AsReadOnly(rows)));
    }
}
