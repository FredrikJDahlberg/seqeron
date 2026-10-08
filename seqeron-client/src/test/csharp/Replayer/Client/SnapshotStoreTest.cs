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
/// An instance's own snapshot files (doc/snapshot.md §4). The golden trailer is shared with
/// <c>SnapshotStoreTest.java</c> and <c>SnapshotStoreTest.cpp</c>, so a file one language writes is one the others
/// read.
/// </summary>
public class SnapshotStoreTest : IDisposable
{
    // Round 3's trailer over the application header and the 3000-byte body as three records, formatVersion 1.
    private const string GoldenTrailer =
        "0300000000000000" + "ca0b000000000000" + "04000000" + "6bf5246c" + "01000000" + "534e4150";

    private readonly TempDirectory _directory = new TempDirectory();

    public void Dispose()
    {
        _directory.Dispose();
    }

    [Fact(DisplayName = "a committed round reads back record by record, its trailer the shared golden bytes")]
    public void CommittedRoundReadsBack()
    {
        var store = new SnapshotStore(_directory.Path);
        List<byte[]> records = Records();
        Write(store, 3, records);

        byte[] file = File.ReadAllBytes(FileOf(3));
        Assert.Equal(3018 + 2 * 4 + SnapshotStore.TrailerLength, file.Length);
        Assert.Equal(GoldenTrailer, Convert.ToHexStringLower(file[^SnapshotStore.TrailerLength..]));

        using SnapshotStore.Reader reader = store.Open(3);
        Assert.NotNull(reader);
        Assert.Equal(4, reader.RecordCount);
        Assert.Equal(3018, reader.Length);
        Assert.Equal(0x6c24f56bU, reader.Crc32C);
        Assert.Equal(1, reader.FormatVersion);
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

    [Fact(DisplayName = "only a committed write counts, and the newest round below a bound is found")]
    public void OnlyCommittedWritesCount()
    {
        var store = new SnapshotStore(_directory.Path);
        using var eight = new UnsafeBuffer(new byte[8]);
        store.Begin(5);
        store.Append(eight, 0, 8);
        store.Abandon();
        Assert.True(store.LatestRound(long.MaxValue) == -1, "an abandoned write leaves no file");

        Write(store, 2, Records());
        Write(store, 4, Records());
        store.Begin(6); // never committed, as a crash leaves it
        store.Append(eight, 0, 8);

        Assert.Equal(4, store.LatestRound(long.MaxValue));
        Assert.Equal(2, store.LatestRound(4));
        Assert.Equal(-1, store.LatestRound(2));
        Assert.Null(store.Open(6));
        Assert.Null(store.Open(7)); // no such round
        store.Abandon();
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
        flipped[2 + SnapshotHeader.ApplicationLength + 2 + 5] ^= 1; // a body byte of record 1
        File.WriteAllBytes(file, flipped);
        Assert.Equal(new List<int> { 18, 1000, 1302, 698, SnapshotStore.Reader.Damaged }, ReadAll(store.Open(2)));

        byte[] oversized = (byte[])intact.Clone();
        oversized[2 + SnapshotHeader.ApplicationLength + 1] = 0xFF; // record 1's length, past the file's records
        File.WriteAllBytes(file, oversized);
        Assert.Equal(new List<int> { 18, SnapshotStore.Reader.Damaged }, ReadAll(store.Open(2)));

        byte[] miscounted = (byte[])intact.Clone();
        miscounted[intact.Length - SnapshotStore.TrailerLength + 16] = 5; // a recordCount the records do not reach
        File.WriteAllBytes(file, miscounted);
        Assert.Equal(new List<int> { 18, 1000, 1302, 698, SnapshotStore.Reader.Damaged }, ReadAll(store.Open(2)));
    }

    [Fact(DisplayName =
              "deleting before a durable round keeps it and every newer one, and clears a write a crash left behind")]
    public void DeleteBeforeKeepsTheRoundAndNewer()
    {
        var crashed = new SnapshotStore(_directory.Path);
        Write(crashed, 1, Records());
        Write(crashed, 2, Records());
        // What a crash mid-write leaves: a temporary file no process holds open. Windows refuses to delete one a
        // live writer still holds, as crashed.Begin(3) would.
        File.WriteAllBytes(Path.Combine(_directory.Path, "3.tmp"), new byte[] { 1, 2, 3 });

        var store = new SnapshotStore(_directory.Path);
        store.DeleteBefore(4);
        store.AwaitWrites();
        Assert.Equal(new List<string> { "1.snapshot", "2.snapshot", "3.tmp" }, Names()); // round 4 is not durable here

        Write(store, 4, Records());
        Write(store, 5, Records());
        store.DeleteBefore(4);
        store.AwaitWrites();
        Assert.Equal(new List<string> { "4.snapshot", "5.snapshot" }, Names());
    }

    [Fact(DisplayName = "a round whose file never became durable deletes nothing older")]
    public void UndurableRoundDeletesNothing()
    {
        var store = new SnapshotStore(_directory.Path);
        Write(store, 1, Records());
        Directory.CreateDirectory(Path.Combine(FileOf(2), "occupied")); // the name cannot be taken
        Write(store, 2, Records());

        store.DeleteBefore(2);
        store.AwaitWrites();
        Assert.True(File.Exists(FileOf(1)));
        Assert.True(File.Exists(Path.Combine(_directory.Path, "2.tmp")));
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
        byte[] header = RestoreFrames.Header(7, 2);
        byte[] body = Frames.Filler(3000);
        return new List<byte[]> { header, body[..1000], body[1000..2302], body[2302..] };
    }

    private static void Write(SnapshotStore store, long round, List<byte[]> records)
    {
        RestoreFrames.Write(store, round, 1, records.ToArray());
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
