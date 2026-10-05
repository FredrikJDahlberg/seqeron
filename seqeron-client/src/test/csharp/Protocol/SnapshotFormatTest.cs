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
    // An application's header: term 7, leader 2.
    private const string ApplicationHeader = "010012000000070000000000000002000000";

    // A gateway's header: term 7, leader 2, source 9, active 10, highest connection 42, rows GW-A and GW-B.
    private static readonly string GatewayHeader = "01006a000000070000000000000002000000090000000a0000002a0000000200" +
                                                   "0a0000000047572d41" + string.Concat(Enumerable.Repeat("00", 28)) +
                                                   "0b0000000147572d42" + string.Concat(Enumerable.Repeat("00", 28));

    // CRC-32C of ApplicationHeader followed by Body().
    private const uint ApplicationSnapshotCrc = 0x6c24f56b;

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
            7, 2,
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
        var application = new SnapshotHeader(7, 2, null);
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
        Assert.Null(SnapshotHeader.Decode(new UnsafeBuffer(gateway), 0, 17));

        gateway[0] = 2;
        Assert.Equal(2, SnapshotHeader.Version(new UnsafeBuffer(gateway), 0, gateway.Length));
        Assert.Null(SnapshotHeader.Decode(new UnsafeBuffer(gateway), 0, gateway.Length));

        byte[] rows = Encode(GatewayHeaderValue());
        rows[30] = 3;
        Assert.Null(SnapshotHeader.Decode(new UnsafeBuffer(rows), 0, rows.Length));
    }

    private static byte[] Snapshot()
    {
        IMutableDirectBuffer buffer = new ExpandableArrayBuffer();
        int headerLength = new SnapshotHeader(7, 2, null).Encode(buffer, 0);
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
