using System;
using System.Linq;
using System.Text;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Xunit;

namespace Org.Limitless.Seqeron.Protocol;

/// <summary>
/// The snapshot byte format (doc/snapshot.md §2, §6). The golden vectors are the ones
/// <c>SnapshotFormatTest.java</c> and the C++ <c>SnapshotFormatTest</c> share, computed by an independent
/// implementation, so the three languages agree with each other and not merely with themselves.
/// </summary>
public class SnapshotFormatTest
{
    // Groups 1 "orders" and 3 "risk".
    private static readonly SnapshotHeader.Group[] Groups = { new SnapshotHeader.Group(1, "orders"),
                                                              new SnapshotHeader.Group(3, "risk") };

    // Groups as a header carries them.
    private static readonly string GroupsBytes = "02" + "016f7264657273" + Zeros(26) + "037269736b" + Zeros(28);

    // An application's header: term 7, leader 2, Groups.
    private static readonly string ApplicationHeader = "020055000000070000000000000002000000" + GroupsBytes;

    // A gateway's: the same, then source 9, active 10, highest connection 42, rows GW-A and GW-B.
    private static readonly string GatewayHeader = "0200ad000000070000000000000002000000" + GroupsBytes +
                                                   "090000000a0000002a0000000200" + "0a0000000047572d41" + Zeros(28) +
                                                   "0b0000000147572d42" + Zeros(28);

    // A version 1 application header, cut from a log before groups.
    private const string Version1Header = "010012000000070000000000000002000000";

    // CRC-32C of ApplicationHeader followed by Body().
    private const uint ApplicationSnapshotCrc = 0x8a4aa58f;

    private static string Zeros(int count)
    {
        return string.Concat(Enumerable.Repeat("00", count));
    }

    // A 3000-byte body; any split of it into records gives the same CRC.
    private static byte[] Body()
    {
        var body = new byte[3000];
        for (int i = 0; i < body.Length; i++)
        {
            body[i] = (byte)(i * 31 + 7);
        }
        return body;
    }

    private static SnapshotHeader GatewayHeaderValue()
    {
        return new SnapshotHeader(
            7, 2, Groups,
            new SnapshotHeader.GatewayState(
                9, 10, 42,
                new[] { new SnapshotHeader.GatewayRow(10, 0, "GW-A"), new SnapshotHeader.GatewayRow(11, 1, "GW-B") }));
    }

    [Fact(DisplayName =
              "CRC-32C is the Castagnoli check value and the shared golden one, and a record is at most 65535 bytes")]
    public void CrcAndRecordSize()
    {
        byte[] check = Encoding.ASCII.GetBytes("123456789");
        Assert.Equal(0xE3069283, SnapshotFormat.Crc32C(new UnsafeBuffer(check), 0, check.Length));
        byte[] snapshot = Snapshot();
        Assert.Equal(ApplicationSnapshotCrc, SnapshotFormat.Crc32C(new UnsafeBuffer(snapshot), 0, snapshot.Length));
        Assert.Equal(65535, SnapshotFormat.MaxRecordLength);
    }

    [Fact(DisplayName = "a running CRC over any split of the bytes is the CRC of the whole")]
    public void RunningCrcOverAnySplit()
    {
        byte[] snapshot = Snapshot();
        foreach (int split in new[] { 0, 1, 7, 8, 9, 1000, snapshot.Length })
        {
            uint crc = SnapshotFormat.Update(0, snapshot.AsSpan(0, split));
            Assert.Equal(ApplicationSnapshotCrc, SnapshotFormat.Update(crc, snapshot.AsSpan(split)));
        }
    }

    [Fact(DisplayName = "both headers encode to the shared golden bytes and decode back")]
    public void HeadersEncodeToTheGoldenBytes()
    {
        var application = new SnapshotHeader(7, 2, Groups, null);
        Assert.Equal(Convert.FromHexString(ApplicationHeader), Encode(application));
        Assert.Equal(Convert.FromHexString(GatewayHeader), Encode(GatewayHeaderValue()));

        foreach (SnapshotHeader header in new[] { application, GatewayHeaderValue() })
        {
            byte[] bytes = Encode(header);
            Assert.Equal(header, SnapshotHeader.Decode(new UnsafeBuffer(bytes), 0, bytes.Length));
        }
    }

    [Fact(DisplayName = "a header of another version, or one its bytes cannot hold, does not decode")]
    public void ForeignOrShortHeadersDoNotDecode()
    {
        byte[] gateway = Encode(GatewayHeaderValue());
        Assert.Null(SnapshotHeader.Decode(new UnsafeBuffer(gateway), 0, gateway.Length - 1));
        Assert.Null(SnapshotHeader.Decode(new UnsafeBuffer(gateway), 0, 18));

        byte[] version1 = Convert.FromHexString(Version1Header);
        Assert.Equal(1, SnapshotHeader.Version(new UnsafeBuffer(version1), 0, version1.Length));
        Assert.Null(SnapshotHeader.Decode(new UnsafeBuffer(version1), 0, version1.Length)); // cut before groups

        gateway[0] = 3;
        Assert.Equal(3, SnapshotHeader.Version(new UnsafeBuffer(gateway), 0, gateway.Length));
        Assert.Null(SnapshotHeader.Decode(new UnsafeBuffer(gateway), 0, gateway.Length));

        byte[] groups = Encode(new SnapshotHeader(7, 2, Groups, null));
        groups[18] = 3;
        Assert.Null(SnapshotHeader.Decode(new UnsafeBuffer(groups), 0, groups.Length)); // groupCount disagrees

        byte[] rows = Encode(GatewayHeaderValue());
        rows[85 + 12] = 3;
        Assert.Null(SnapshotHeader.Decode(new UnsafeBuffer(rows), 0, rows.Length));
    }

    private static byte[] Snapshot()
    {
        IMutableDirectBuffer buffer = new ExpandableArrayBuffer();
        int headerLength = new SnapshotHeader(7, 2, Groups, null).Encode(buffer, 0);
        buffer.PutBytes(headerLength, Body());
        var snapshot = new byte[headerLength + 3000];
        buffer.GetBytes(0, snapshot);
        return snapshot;
    }

    private static byte[] Encode(SnapshotHeader header)
    {
        IMutableDirectBuffer buffer = new ExpandableArrayBuffer();
        var bytes = new byte[header.Encode(buffer, 0)];
        buffer.GetBytes(0, bytes);
        return bytes;
    }
}
