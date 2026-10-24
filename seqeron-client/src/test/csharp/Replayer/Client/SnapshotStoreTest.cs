using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Adaptive.Agrona.Concurrent;
using Org.Limitless.Seqeron.Helpers;
using Org.Limitless.Seqeron.Protocol;
using Xunit;

namespace Org.Limitless.Seqeron.Replayer.Client;

/// <summary>
/// An instance's own snapshot files (doc/snapshot.md §4.1). The golden trailer is shared with
/// <c>SnapshotStoreTest.java</c> and <c>SnapshotStoreTest.cpp</c>, so a file one language writes is one the others
/// read.
/// </summary>
public class SnapshotStoreTest : IDisposable
{
    // Round 3's trailer over the application header and the 3000-byte body as three records, formatVersion 1, cut at
    // clusterSeqNo 42, position 43008, timestamp 42000.
    private const string GoldenTrailer = "0300000000000000" + "0d0c000000000000" + "04000000" + "8fa54a8a" +
                                         "01000000" + "2a00000000000000" + "00a8000000000000" + "10a4000000000000" +
                                         "534e5032";

    // The shared application header, groups 1 "orders" and 3 "risk".
    private static readonly SnapshotHeader Header = new SnapshotHeader(
        7, 2, new[] { new SnapshotHeader.Group(1, "orders"), new SnapshotHeader.Group(3, "risk") }, null);
    private const int HeaderLength = 85;

    private const long CutClusterSeqNo = 42;
    private const long CutPosition = 43008;
    private const long CutTimestampNs = 42000;

    private readonly TempDirectory _directory = new TempDirectory();

    public void Dispose()
    {
        _directory.Dispose();
    }

    [Fact(DisplayName = "a confirmed round reads back record by record, its trailer the shared golden bytes")]
    public void ConfirmedRoundReadsBack()
    {
        var store = new SnapshotStore(_directory.Path);
        List<byte[]> records = Records();
        Write(store, 3, records);

        byte[] file = File.ReadAllBytes(FileOf(3));
        Assert.Equal(3085 + 2 * 4 + SnapshotStore.TrailerLength, file.Length);
        Assert.Equal(GoldenTrailer, Convert.ToHexStringLower(file[^SnapshotStore.TrailerLength..]));

        using SnapshotStore.Reader reader = store.Open(3);
        Assert.NotNull(reader);
        Assert.Equal(4, reader.RecordCount);
        Assert.Equal(3085, reader.Length);
        Assert.Equal(0x8a4aa58fU, reader.Crc32C);
        Assert.Equal(1, reader.FormatVersion);
        Assert.Equal(CutClusterSeqNo, reader.CutClusterSeqNo);
        Assert.Equal(CutPosition, reader.CutPosition);
        Assert.Equal(CutTimestampNs, reader.CutTimestampNs);
        using var view = new UnsafeBuffer();
        foreach (byte[] record in records)
        {
            Assert.Equal(record.Length, reader.Next(view));
            var read = new byte[record.Length];
            view.GetBytes(0, read);
            Assert.Equal(record, read);
        }
        Assert.Equal(SnapshotStore.Reader.End, reader.Next(view));
    }

    [Fact(DisplayName = "only a confirmed round counts, and the newest round below a bound is found")]
    public void OnlyConfirmedRoundsCount()
    {
        var store = new SnapshotStore(_directory.Path);
        using var eight = new UnsafeBuffer(new byte[8]);
        Begin(store, 5);
        store.Append(eight, 0, 8);
        store.Abandon();
        store.Confirm(5);
        store.AwaitWrites();
        Assert.True(Names().Count == 0, "an abandoned write leaves no file, and confirms nothing");

        Commit(store, 2, Records());
        Assert.True(store.LatestRound(long.MaxValue) == -1, "a round the log has not confirmed");
        Assert.Null(store.Open(2));
        store.Confirm(1);
        store.AwaitWrites();
        Assert.Equal(new List<string> { "2.tmp" }, Names()); // a confirmation of another round confirms nothing

        Write(store, 4, Records());
        File.Copy(FileOf(4), FileOf(3)); // as a crash may leave one
        Assert.Equal(4, store.LatestRound(long.MaxValue));
        Assert.Equal(3, store.LatestRound(4));
        Assert.Equal(-1, store.LatestRound(3));
        Assert.Null(store.Open(7)); // no such round
    }

    [Fact(DisplayName =
              "a file cut short or of another round does not open; records that fail its trailer are damaged")]
    public void DamagedFiles()
    {
        var store = new SnapshotStore(_directory.Path);
        Write(store, 2, Records());
        string file = FileOf(2);
        byte[] intact = File.ReadAllBytes(file);

        File.Copy(file, FileOf(3));
        Assert.Null(store.Open(3)); // its trailer names round 2

        File.WriteAllBytes(file, intact[..(intact.Length - 1)]);
        Assert.Null(store.Open(2)); // cut short: no trailer

        byte[] flipped = (byte[])intact.Clone();
        flipped[2 + HeaderLength + 2 + 5] ^= 1; // a body byte of record 1
        File.WriteAllBytes(file, flipped);
        Assert.Equal(new List<int> { HeaderLength, 1000, 1302, 698, SnapshotStore.Reader.Damaged },
                     ReadAll(store.Open(2)));

        byte[] oversized = (byte[])intact.Clone();
        oversized[2 + HeaderLength + 1] = 0xFF; // record 1's length, past the file's records
        File.WriteAllBytes(file, oversized);
        Assert.Equal(new List<int> { HeaderLength, SnapshotStore.Reader.Damaged }, ReadAll(store.Open(2)));

        byte[] miscounted = (byte[])intact.Clone();
        miscounted[intact.Length - SnapshotStore.TrailerLength + 16] = 5; // a recordCount the records do not reach
        File.WriteAllBytes(file, miscounted);
        Assert.Equal(new List<int> { HeaderLength, 1000, 1302, 698, SnapshotStore.Reader.Damaged },
                     ReadAll(store.Open(2)));
    }

    [Fact(
        DisplayName =
            "a confirmation replaces every older file but the newest confirmed one, and any write a crash left behind")]
    public void ConfirmationReplacesOlderFiles()
    {
        var crashed = new SnapshotStore(_directory.Path);
        Write(crashed, 1, Records());
        Commit(crashed, 2, Records());
        // What a crash mid-write leaves: a temporary file no process holds open. Windows refuses to delete one a
        // live writer still holds, as Begin(crashed, 3) would.
        File.WriteAllBytes(Path.Combine(_directory.Path, "3.tmp"), new byte[] { 1, 2, 3 });

        var store = new SnapshotStore(_directory.Path);
        Write(store, 4, Records());
        Assert.Equal(new List<string> { "1.snapshot", "4.snapshot" }, Names());

        Commit(store, 5, Records());
        Begin(store, 6); // round 5's end never came
        store.Abandon();
        Assert.Equal(new List<string> { "1.snapshot", "4.snapshot", "5.tmp" }, Names());
        Write(store, 7, Records());
        Assert.Equal(new List<string> { "4.snapshot", "7.snapshot" }, Names());
    }

    [Fact(DisplayName = "a confirmation of a round already on disk replaces its file, and keeps the one before it")]
    public void ConfirmationReplacesItsOwnRound()
    {
        var store = new SnapshotStore(_directory.Path);
        Write(store, 4, Records());
        Write(store, 5, Records());
        string file = FileOf(5);
        byte[] damaged = File.ReadAllBytes(file);
        damaged[2 + HeaderLength + 2 + 5] ^= 1; // a body byte of record 1
        File.WriteAllBytes(file, damaged);

        Write(store, 5, Records()); // the round again, on the way back to the tail
        Assert.Equal(new List<string> { "4.snapshot", "5.snapshot" }, Names());
        Assert.Equal(new List<int> { HeaderLength, 1000, 1302, 698, SnapshotStore.Reader.End }, ReadAll(store.Open(5)));
    }

    [Fact(DisplayName = "a round whose file never became durable deletes nothing older")]
    public void UndurableRoundDeletesNothing()
    {
        var store = new SnapshotStore(_directory.Path);
        Write(store, 1, Records());
        Directory.CreateDirectory(Path.Combine(FileOf(2), "occupied")); // the name cannot be taken
        Write(store, 2, Records());

        Assert.True(File.Exists(FileOf(1)));
        Assert.True(File.Exists(Path.Combine(_directory.Path, "2.tmp")));
    }

    [Fact(DisplayName =
              "a round whose file cannot be created confirms nothing, and the next round is written as usual")]
    public void UncreatableRoundConfirmsNothing()
    {
        var store = new SnapshotStore(_directory.Path);
        Write(store, 1, Records());
        Directory.CreateDirectory(Path.Combine(_directory.Path, "2.tmp", "occupied")); // the file cannot be created

        Write(store, 2, Records());
        Assert.Equal(1, store.DurableRound);
        Assert.Equal(1, store.LatestRound(long.MaxValue));

        Write(store, 3, Records());
        Assert.Equal(3, store.DurableRound);
        Assert.Equal(new List<int> { HeaderLength, 1000, 1302, 698, SnapshotStore.Reader.End }, ReadAll(store.Open(3)));
    }

    [Fact(DisplayName = "a record of the most bytes its length prefix can say reads back whole")]
    public void LargestRecordReadsBack()
    {
        var store = new SnapshotStore(_directory.Path);
        var largest = new byte[SnapshotFormat.MaxRecordLength];
        Array.Fill(largest, (byte)7);
        Write(store, 1, new List<byte[]> { largest });

        using SnapshotStore.Reader reader = store.Open(1);
        using var view = new UnsafeBuffer();
        Assert.Equal(SnapshotFormat.MaxRecordLength, reader.Next(view));
        Assert.Equal(7, view.GetByte(SnapshotFormat.MaxRecordLength - 1));
        Assert.Equal(SnapshotStore.Reader.End, reader.Next(view));
    }

    private string FileOf(long round)
    {
        return Path.Combine(_directory.Path, round + ".snapshot");
    }

    private List<string> Names()
    {
        return Directory.EnumerateFiles(_directory.Path).Select(Path.GetFileName).Order().ToList();
    }

    // The application header, then the 3000-byte body as records of 1000, 1302 and 698 bytes.
    private static List<byte[]> Records()
    {
        var header = new byte[HeaderLength];
        using (var buffer = new UnsafeBuffer(header))
        {
            Header.Encode(buffer, 0);
        }
        byte[] body = Frames.Filler(3000);
        return new List<byte[]> { header, body[..1000], body[1000..2302], body[2302..] };
    }

    // Writes and confirms a round's file.
    private static void Write(SnapshotStore store, long round, List<byte[]> records)
    {
        Commit(store, round, records);
        store.Confirm(round);
        store.AwaitWrites();
    }

    // Writes a round's file, as the instance does before the log confirms it.
    private static void Commit(SnapshotStore store, long round, List<byte[]> records)
    {
        RestoreFrames.Digest digest = RestoreFrames.Digest.Of(records.ToArray());
        Begin(store, round);
        foreach (byte[] record in records)
        {
            using var buffer = new UnsafeBuffer(record);
            store.Append(buffer, 0, record.Length);
        }
        store.Commit(digest.RecordCount, digest.Length, digest.Crc32C, 1);
    }

    private static void Begin(SnapshotStore store, long round)
    {
        store.Begin(round, CutClusterSeqNo, CutPosition, CutTimestampNs);
    }

    // Each record's length in order, then what ends the read.
    private static List<int> ReadAll(SnapshotStore.Reader reader)
    {
        var lengths = new List<int>();
        using var view = new UnsafeBuffer();
        int length;
        do
        {
            length = reader.Next(view);
            lengths.Add(length);
        } while (length >= 0);
        reader.Dispose();
        return lengths;
    }
}
