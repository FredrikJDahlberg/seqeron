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
/// 18  groupCount          uint8
/// 19  groups, in ascending groupId: groupId uint8, groupName char[32]
///     — a gateway's header continues at G = 19 + 33 × groupCount —
/// G       gatewaySourceId     int32
/// G + 4   activeGatewayId     int32    the current GatewayActive's, or NoGateway
/// G + 8   highestConnectionId int32    the highest in the source's history
/// G + 12  rowCount            uint16
/// G + 14  rows, in list order: gatewayId int32, preferenceRank uint8, gatewayName char[32]
/// </code>
/// Every instance of a source writes the same header (A-6), so it carries the rows of every instance of a gateway
/// and an instance finds its own by name, as it does in the list. <c>SnapshotHeader.java</c> and
/// <c>Snapshot.hpp</c> are its twins; keep the three in step.
/// </summary>
/// <param name="LeadershipTermId">the term of the last <c>LeadershipChanged</c> before the cut</param>
/// <param name="LeaderMemberId">that term's leader</param>
/// <param name="Groups">the active groups before the cut, in ascending <c>groupId</c></param>
/// <param name="Gateway">a gateway's continuation, or null for an application's header</param>
public sealed record SnapshotHeader(long LeadershipTermId, int LeaderMemberId,
                                    IReadOnlyList<SnapshotHeader.Group> Groups, SnapshotHeader.GatewayState Gateway)
{
    /// <summary>The one <c>headerVersion</c> this build writes and reads.</summary>
    public const int HeaderVersion = 2;

    /// <summary>No <c>GatewayActive</c> yet.</summary>
    public const int NoGateway = -1;

    private const int FixedLength = 19;
    private const int NameLength = 32;
    private const int GroupLength = 1 + NameLength;
    private const int GatewayFixedLength = 14;
    private const int GatewayRowLength = 5 + NameLength;
    private const ByteOrder LE = ByteOrder.LittleEndian;

    /// <summary>One active group.</summary>
    /// <param name="GroupId">1 to <see cref="FrameLayer.MaxGroups"/></param>
    /// <param name="GroupName">its name, at most 32 US-ASCII characters</param>
    public sealed record Group(int GroupId, string GroupName);

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

    /// <summary>Equal when every field is, the groups compared in order.</summary>
    /// <param name="other">the header to compare with</param>
    public bool Equals(SnapshotHeader other)
    {
        return other is not null && LeadershipTermId == other.LeadershipTermId &&
               LeaderMemberId == other.LeaderMemberId && Groups.SequenceEqual(other.Groups) &&
               Equals(Gateway, other.Gateway);
    }

    /// <inheritdoc/>
    public override int GetHashCode()
    {
        return HashCode.Combine(LeadershipTermId, LeaderMemberId, Groups.Count, Gateway);
    }

    /// <summary>Bytes <see cref="Encode"/> writes; the value of <c>headerLength</c>.</summary>
    public int EncodedLength => Gateway == null
                                    ? ApplicationLength
                                    : ApplicationLength + GatewayFixedLength + GatewayRowLength * Gateway.Rows.Count;

    private int ApplicationLength => FixedLength + GroupLength * Groups.Count;

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
        buffer.PutByte(offset + 18, (byte)Groups.Count);
        int group = offset + FixedLength;
        foreach (Group each in Groups)
        {
            buffer.PutByte(group, (byte)each.GroupId);
            PutName(buffer, group + 1, each.GroupName);
            group += GroupLength;
        }
        if (Gateway != null)
        {
            buffer.PutInt(group, Gateway.GatewaySourceId, LE);
            buffer.PutInt(group + 4, Gateway.ActiveGatewayId, LE);
            buffer.PutInt(group + 8, Gateway.HighestConnectionId, LE);
            buffer.PutShort(group + 12, (short)Gateway.Rows.Count, LE);
            int row = group + GatewayFixedLength;
            foreach (GatewayRow each in Gateway.Rows)
            {
                buffer.PutInt(row, each.GatewayId, LE);
                buffer.PutByte(row + 4, (byte)each.PreferenceRank);
                PutName(buffer, row + 5, each.GatewayName);
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
        if (length < FixedLength || Version(buffer, offset, length) != HeaderVersion)
        {
            return null;
        }
        long headerLength = buffer.GetInt(offset + 2, LE) & 0xFFFF_FFFFL;
        long leadershipTermId = buffer.GetLong(offset + 6, LE);
        int leaderMemberId = buffer.GetInt(offset + 14, LE);
        int groupCount = buffer.GetByte(offset + 18);
        int applicationLength = FixedLength + GroupLength * groupCount;
        if (headerLength > length || headerLength < applicationLength)
        {
            return null;
        }
        var groups = new Group[groupCount];
        int group = offset + FixedLength;
        for (int i = 0; i < groupCount; i++)
        {
            groups[i] = new Group(buffer.GetByte(group), GetName(buffer, group + 1));
            group += GroupLength;
        }
        if (headerLength == applicationLength)
        {
            return new SnapshotHeader(leadershipTermId, leaderMemberId, Array.AsReadOnly(groups), null);
        }
        if (headerLength < applicationLength + GatewayFixedLength)
        {
            return null;
        }
        int rowCount = buffer.GetShort(group + 12, LE) & 0xFFFF;
        if (headerLength != applicationLength + GatewayFixedLength + (long)GatewayRowLength * rowCount)
        {
            return null;
        }
        var rows = new GatewayRow[rowCount];
        int row = group + GatewayFixedLength;
        for (int i = 0; i < rowCount; i++)
        {
            rows[i] = new GatewayRow(buffer.GetInt(row, LE), buffer.GetByte(row + 4), GetName(buffer, row + 5));
            row += GatewayRowLength;
        }
        return new SnapshotHeader(leadershipTermId, leaderMemberId, Array.AsReadOnly(groups),
                                  new GatewayState(buffer.GetInt(group, LE), buffer.GetInt(group + 4, LE),
                                                   buffer.GetInt(group + 8, LE), Array.AsReadOnly(rows)));
    }

    private static void PutName(IMutableDirectBuffer buffer, int offset, string name)
    {
        byte[] bytes = Encoding.ASCII.GetBytes(name);
        int nameLength = Math.Min(bytes.Length, NameLength);
        buffer.PutBytes(offset, bytes, 0, nameLength);
        buffer.SetMemory(offset + nameLength, NameLength - nameLength, 0);
    }

    private static string GetName(IDirectBuffer buffer, int offset)
    {
        int nameLength = 0;
        while (nameLength < NameLength && buffer.GetByte(offset + nameLength) != 0)
        {
            nameLength++;
        }
        return buffer.GetStringWithoutLengthAscii(offset, nameLength);
    }
}
