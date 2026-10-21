using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Org.Limitless.Seqeron.Helpers;
using Org.Limitless.Seqeron.Protocol;
using Org.Limitless.Seqeron.Replayer.Client;
using Xunit;

namespace Org.Limitless.Seqeron.App;

/// <summary>
/// A façade's side of snapshot rounds (doc/snapshot.md §4), driven through its <c>IActions</c> seam. Case for case
/// with <c>SnapshotTakerTest.java</c>.
/// </summary>
public class SnapshotTakerTest : IDisposable
{
    private static readonly SnapshotHeader Header =
        new SnapshotHeader(3, 1, new[] { new SnapshotHeader.Group(1, "orders") }, null);

    // Encodes Records records of 1000 bytes, each filled with its index, then answers 0.
    private class State : ISnapshotListener
    {
        public int Records = 2;
        public int Serialized;
        public int BadLength;
        public readonly List<string> Restored = new List<string>();

        public int FormatVersion => 5;

        public int OnSnapshot(IMutableDirectBuffer buffer, int recordIndex)
        {
            if (recordIndex == 0)
            {
                Serialized++;
            }
            if (recordIndex == Records)
            {
                return 0;
            }
            if (BadLength != 0)
            {
                return BadLength;
            }
            buffer.SetMemory(0, 1000, (byte)recordIndex);
            return 1000;
        }

        public void OnRestore(IDirectBuffer buffer, int length, int recordIndex, int formatVersion)
        {
            Restored.Add($"{recordIndex}:{length}:{buffer.GetByte(0)}:v{formatVersion}");
        }
    }

    // Reads format 4 beside its own 5, as a build that changed its format does.
    private sealed class UpgradedState : State, ISnapshotListener
    {
        public bool SupportsFormatVersion(int formatVersion)
        {
            return formatVersion == 4 || formatVersion == 5;
        }
    }

    private sealed record End(long Round, int RecordCount, long Length, uint Crc32C, int FormatVersion);

    // Records the ends placed; declines once Budget have gone.
    private sealed class Frames : SnapshotTaker.IActions
    {
        public readonly List<End> Ends = new List<End>();
        public int Budget = int.MaxValue;

        public Publish PublishEnd(long round, int recordCount, long length, uint crc32c, int formatVersion)
        {
            if (Budget == 0)
            {
                return Publish.Declined;
            }
            Budget--;
            Ends.Add(new End(round, recordCount, length, crc32c, formatVersion));
            return Publish.Published;
        }
    }

    private readonly TempDirectory _directory = new TempDirectory();
    private readonly State _state = new State();
    private readonly Frames _frames = new Frames();
    private readonly List<SnapshotStore> _stores = new List<SnapshotStore>();
    private int _keepAlives;

    public void Dispose()
    {
        _stores.ForEach(store => store.AwaitWrites());
        _directory.Dispose();
    }

    [Fact(DisplayName = "without a listener, or with the row off, a replica serializes, writes and submits nothing")]
    public void NotParticipating()
    {
        var noListener = new SnapshotTaker(null, null, KeepAlive);
        noListener.SetParticipating(true);
        Assert.False(noListener.IsParticipating);

        var rowOff = new SnapshotTaker(_state, Store(), KeepAlive);
        rowOff.SetParticipating(false);
        Started(rowOff, 1, true);
        Assert.Equal(0, _state.Serialized);
        Assert.Equal(-1, StoreOf(1).LatestRound(long.MaxValue));
        Assert.Equal(0, rowOff.Submit(_frames));
        Assert.True(rowOff.OnSnapshotEnd(1, 3, 2018, 0), "nothing serialized, nothing to disagree with");
    }

    [Fact(DisplayName =
              "the round's file holds the header, the records and the cut; the publisher submits the end it matches")]
    public void PublisherWritesThenSubmitsTheEnd()
    {
        SnapshotTaker taker = Participating();
        Started(taker, 7, true);
        Assert.Equal(1, _state.Serialized);
        Assert.True(taker.IsPublishing);
        Assert.Equal(1, taker.Submit(_frames)); // the end, once
        Assert.False(taker.IsPublishing);
        Assert.Equal(0, taker.Submit(_frames));

        End end = _frames.Ends[0];
        Assert.Equal(7, end.Round);
        Assert.Equal(3, end.RecordCount);
        Assert.Equal(2052, end.Length);
        Assert.Equal(5, end.FormatVersion); // the listener's formatVersion
        Assert.Null(StoreOf(1).Open(7));    // not confirmed before its end is sequenced
        Assert.True(taker.OnSnapshotEnd(7, end.RecordCount, end.Length, end.Crc32C), "its own end agrees with it");

        SnapshotStore.Reader reader = StoreOf(1).Open(7);
        Assert.NotNull(reader);
        Assert.Equal(3, reader.RecordCount);
        Assert.Equal(end.Length, reader.Length);
        Assert.Equal(end.Crc32C, reader.Crc32C);
        Assert.Equal(70, reader.CutClusterSeqNo);
        Assert.Equal(70 * 1024, reader.CutPosition);
        Assert.Equal(70_000, reader.CutTimestampNs);
        List<byte[]> records = ReadAll(reader);
        using (var first = new UnsafeBuffer(records[0]))
        {
            Assert.Equal(Header, SnapshotHeader.Decode(first, 0, records[0].Length));
        }
        Assert.Equal(Filled(0), records[1]);
        Assert.Equal(Filled(1), records[2]);
    }

    [Fact(DisplayName = "a declined end is placed again on the next cycle")]
    public void DeclinedEndWaitsForTheNextCycle()
    {
        SnapshotTaker taker = Participating();
        Started(taker, 2, true);
        _frames.Budget = 0;
        Assert.Equal(0, taker.Submit(_frames));
        Assert.True(taker.IsPublishing);
        _frames.Budget = int.MaxValue;
        Assert.Equal(1, taker.Submit(_frames));
        Assert.Single(_frames.Ends);
    }

    [Fact(DisplayName =
              "a replica that does not publish writes its file, submits nothing, and compares the source's end")]
    public void OtherReplicasCompare()
    {
        SnapshotTaker publisher = Participating();
        Started(publisher, 4, true);
        publisher.Submit(_frames);
        End end = _frames.Ends[0];

        SnapshotTaker follower = Participating();
        Started(follower, 4, false);
        Assert.Equal(0, follower.Submit(new Frames()));
        Assert.True(follower.OnSnapshotEnd(4, end.RecordCount, end.Length, end.Crc32C));
        Assert.Equal(4, StoreOf(1).LatestRound(long.MaxValue));

        SnapshotTaker diverged = Participating();
        _state.Records = 3;
        Started(diverged, 4, false);
        Assert.False(diverged.OnSnapshotEnd(4, end.RecordCount, end.Length, end.Crc32C),
                     "different state, different records");
        Assert.Equal(-1, StoreOf(1).LatestRound(long.MaxValue)); // its file is never confirmed

        SnapshotTaker otherRound = Participating();
        Started(otherRound, 5, false);
        Assert.True(otherRound.OnSnapshotEnd(4, 99, 0, 0), "a late end of an earlier round is no evidence");
    }

    [Fact(DisplayName =
              "a matching end confirms its round's file, which replaces every older one but the newest; a mismatch " +
              "confirms nothing")]
    public void MatchingEndConfirmsTheRound()
    {
        SnapshotTaker taker = Participating();
        Started(taker, 1, true);
        taker.Submit(_frames);
        End end = _frames.Ends[0];
        Assert.Equal(0, taker.ConfirmedRound);
        Assert.True(taker.OnSnapshotEnd(1, end.RecordCount, end.Length, end.Crc32C));
        Assert.Equal(1, taker.ConfirmedRound);
        Assert.Equal(1, StoreOf(1).LatestRound(long.MaxValue));

        Started(taker, 2, false);
        Assert.False(taker.OnSnapshotEnd(2, 99, 0, 0));
        Assert.Equal(1, taker.ConfirmedRound);
        Assert.Equal(1, StoreOf(1).LatestRound(long.MaxValue)); // a diverged instance keeps what it had

        Started(taker, 3, false);
        Assert.True(taker.OnSnapshotEnd(3, end.RecordCount, end.Length, end.Crc32C));
        Assert.Equal(3, taker.ConfirmedRound);
        Assert.Equal(3, StoreOf(1).LatestRound(long.MaxValue));
        Assert.Equal(1, StoreOf(1).LatestRound(3)); // the round before it stays
        Assert.Equal(-1, StoreOf(1).LatestRound(1));
    }

    [Fact(DisplayName = "a publisher that loses the role stops for good, with no end")]
    public void LostRoleStops()
    {
        SnapshotTaker taker = Participating();
        Started(taker, 6, true);
        taker.StopPublishing();
        Assert.Equal(0, taker.Submit(_frames));
        Assert.Empty(_frames.Ends);
        Assert.Equal(-1, StoreOf(1).LatestRound(long.MaxValue)); // no end, so nothing is confirmed
    }

    [Fact(DisplayName = "a listener answering a length no record can have drops the round, with no file and no end")]
    public void BadLengthDropsTheRound()
    {
        foreach (int length in new[] { -1, 65536 })
        {
            _state.BadLength = length;
            SnapshotTaker taker = Participating();
            Started(taker, 3, true);
            Assert.False(taker.IsPublishing);
            Assert.Equal(0, taker.Submit(_frames));
            Assert.Equal(-1, StoreOf(1).LatestRound(long.MaxValue));
            Assert.True(taker.OnSnapshotEnd(3, 3, 2018, 0), "nothing serialized, nothing to disagree with");
        }
    }

    [Fact(DisplayName = "a new round supersedes one whose end is not yet placed: only the new one ends")]
    public void NewRoundSupersedes()
    {
        SnapshotTaker taker = Participating();
        Started(taker, 8, true);
        Started(taker, 9, true);
        Assert.Equal(1, taker.Submit(_frames));
        Assert.Single(_frames.Ends);
        Assert.Equal(9, _frames.Ends[0].Round);
        Assert.True(taker.OnSnapshotEnd(8, 99, 0, 0), "nothing held for round 8 any more");
    }

    [Fact(DisplayName = "a round started after the row turned off still ends the one being submitted")]
    public void RoundAfterTheRowTurnedOffAbandonsTheOpenOne()
    {
        SnapshotTaker taker = Participating();
        Started(taker, 8, true);
        taker.SetParticipating(false);
        Started(taker, 9, true);
        Assert.False(taker.IsPublishing);
        Assert.Equal(0, taker.Submit(_frames));
        Assert.Empty(_frames.Ends); // round 8 never ended
        Assert.True(taker.OnSnapshotEnd(8, 99, 0, 0), "nothing held for round 8 any more");
    }

    [Fact(DisplayName = "a header longer than a record drops the round")]
    public void OversizedHeaderDropsTheRound()
    {
        var state35 = new SnapshotHeader.GatewayState(
            9, 10, 42, Enumerable.Repeat(new SnapshotHeader.GatewayRow(10, 0, "GW"), 1771).ToList());
        SnapshotTaker taker = Participating();
        Started(taker, 3, new SnapshotHeader(7, 2, Array.Empty<SnapshotHeader.Group>(), state35), true);
        Assert.Equal(0, _state.Serialized); // 1771 rows outgrow a record
        Assert.False(taker.IsPublishing);
        Assert.Null(StoreOf(1).Open(3));
    }

    [Fact(DisplayName =
              "a restore reads only this build's format, makes the source take part, and hands its records on")]
    public void RestoreHandsRecordsToTheListener()
    {
        var taker = new SnapshotTaker(_state, Store(), KeepAlive);
        Assert.True(taker.SupportsFormatVersion(5));
        Assert.False(taker.SupportsFormatVersion(6));

        taker.OnSnapshotHeader(Header);
        Assert.True(taker.IsParticipating, "its topology row lies before the cut");
        using var record = new UnsafeBuffer(Filled(1));
        taker.OnSnapshotRecord(record, 1000, 0, 5);
        Assert.Equal(new List<string> { "0:1000:1:v5" }, _state.Restored);
    }

    [Fact(DisplayName = "a restore reads an older format its listener still reads, and tells the listener which")]
    public void RestoreReadsAnOlderFormatTheListenerReads()
    {
        var upgraded = new UpgradedState();
        var taker = new SnapshotTaker(upgraded, Store(), KeepAlive);
        Assert.True(taker.SupportsFormatVersion(4));
        Assert.False(taker.SupportsFormatVersion(3));

        taker.OnSnapshotHeader(Header);
        using var record = new UnsafeBuffer(Filled(1));
        taker.OnSnapshotRecord(record, 1000, 0, 4);
        Assert.Equal(new List<string> { "0:1000:1:v4" }, upgraded.Restored);
    }

    [Fact(DisplayName = "serializing a round keeps the cluster session alive after every record")]
    public void SerializationKeepsTheSessionAlive()
    {
        _state.Records = 3;
        SnapshotTaker taker = Participating();
        Started(taker, 1, true);
        Assert.Equal(4, _keepAlives); // the header and three records
    }

    private void KeepAlive()
    {
        _keepAlives++;
    }

    // A participating instance with a directory of its own.
    private SnapshotTaker Participating()
    {
        var taker = new SnapshotTaker(_state, Store(), KeepAlive);
        taker.SetParticipating(true);
        return taker;
    }

    // A round's SnapshotStarted: cut at clusterSeqNo round * 10.
    private static void Started(SnapshotTaker taker, long round, bool mayPublish)
    {
        Started(taker, round, Header, mayPublish);
    }

    private static void Started(SnapshotTaker taker, long round, SnapshotHeader header, bool mayPublish)
    {
        taker.OnSnapshotStarted(round, round * 10, round * 10 * 1024, round * 10_000, header, mayPublish);
    }

    private SnapshotStore Store()
    {
        var store = new SnapshotStore(Path.Combine(_directory.Path, "instance-" + _stores.Count));
        _stores.Add(store);
        return store;
    }

    // The store of the instance created `back` calls ago, 1 for the last, once its writes are done.
    private SnapshotStore StoreOf(int back)
    {
        SnapshotStore store = _stores[_stores.Count - back];
        store.AwaitWrites();
        return store;
    }

    private static byte[] Filled(int value)
    {
        var record = new byte[1000];
        Array.Fill(record, (byte)value);
        return record;
    }

    private static List<byte[]> ReadAll(SnapshotStore.Reader reader)
    {
        var records = new List<byte[]>();
        using var view = new UnsafeBuffer();
        int length;
        while ((length = reader.Next(view)) >= 0)
        {
            var record = new byte[length];
            view.GetBytes(0, record);
            records.Add(record);
        }
        Assert.Equal(SnapshotStore.Reader.End, length);
        reader.Dispose();
        return records;
    }
}
