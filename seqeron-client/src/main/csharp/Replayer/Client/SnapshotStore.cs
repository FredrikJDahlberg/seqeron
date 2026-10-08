using System;
using System.Buffers.Binary;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Org.Limitless.Seqeron.Protocol;
using Org.Limitless.Seqeron.Util;

namespace Org.Limitless.Seqeron.Replayer.Client;

/// <summary>
/// This instance's own snapshots, one file per round in a directory no other instance writes (doc/snapshot.md
/// §4). <c>&lt;round&gt;.snapshot</c> holds the records, each a little-endian uint16 length and its bytes, record 0
/// the façade's header, then a trailer:
/// <code>
///  0  round          int64
///  8  length         int64    the records' bytes, as SnapshotEnd.length
/// 16  recordCount    int32
/// 20  crc32c         uint32
/// 24  formatVersion  uint32
/// 28  magic          uint32   Magic
/// </code>
/// A file is written as <c>&lt;round&gt;.tmp</c>, then forced to disk and renamed on a thread of its own, so the
/// caller does not wait for the disk. .NET forces no directory; a journaling file system commits the rename before
/// the deletions that follow it. A write that fails leaves no file and is logged. Not thread-safe: one thread calls
/// it, and only its file work runs on the other. <c>SnapshotStore.java</c> and <c>SnapshotStore.hpp</c> are its
/// twins; keep the three in step.
/// </summary>
public sealed class SnapshotStore
{
    /// <summary>Bytes after the records.</summary>
    internal const int TrailerLength = 32;

    /// <summary>"SNAP", last in the file, so a file cut short has none.</summary>
    internal const int Magic = 0x50414E53;

    private const string Suffix = ".snapshot";
    private const string TemporarySuffix = ".tmp";
    private const int RecordPrefixLength = sizeof(ushort);

    // Each file stream's buffer: a multiple of every SSD page size, and few system calls per file.
    private const int IoBufferLength = 64 * 1024;

    private readonly string _directory;
    private FileStream _out;
    private long _writingRound;

    // What runs off the caller's thread, one step after another: making files durable and deleting them.
    private Task _writes = Task.CompletedTask;

    // The newest round whose file is durable under its name. Touched by _writes alone.
    private long _durableRound = -1;

    /// <summary>Opens this instance's snapshot directory.</summary>
    /// <param name="directory">this instance's own; created if missing</param>
    /// <exception cref="IOException">if it cannot be</exception>
    public SnapshotStore(string directory)
    {
        _directory = directory;
        try
        {
            Directory.CreateDirectory(directory);
        }
        catch (Exception ex) when (ex is IOException || ex is UnauthorizedAccessException)
        {
            throw new IOException("cannot create snapshot directory " + directory, ex);
        }
    }

    /// <summary>Starts writing a round's file, dropping any write still open, once the rounds before it are
    /// durable.</summary>
    /// <param name="round">its round</param>
    public void Begin(long round)
    {
        AwaitWrites();
        Abandon();
        _writingRound = round;
        try
        {
            _out =
                new FileStream(TemporaryFile(round), FileMode.Create, FileAccess.Write, FileShare.None, IoBufferLength);
        }
        catch (Exception ex) when (IsIoFailure(ex))
        {
            FailWrite(ex);
        }
    }

    /// <summary>Appends one record to the file being written.</summary>
    /// <param name="record">holding it</param>
    /// <param name="offset">of its first byte</param>
    /// <param name="length">its length, at most <see cref="SnapshotFormat.MaxRecordLength"/></param>
    public void Append(IDirectBuffer record, int offset, int length)
    {
        if (_out == null)
        {
            return;
        }
        Span<byte> prefix = stackalloc byte[RecordPrefixLength];
        BinaryPrimitives.WriteUInt16LittleEndian(prefix, (ushort)length);
        try
        {
            _out.Write(prefix);
            _out.Write(SbeBuffers.Bytes(record, offset, length));
        }
        catch (Exception ex) when (IsIoFailure(ex))
        {
            FailWrite(ex);
        }
    }

    /// <summary>Completes the file being written: its trailer, then, off this thread, its name once it is
    /// durable.</summary>
    /// <param name="recordCount">records appended</param>
    /// <param name="length">their bytes</param>
    /// <param name="crc32c">their CRC-32C</param>
    /// <param name="formatVersion">the listener's</param>
    public void Commit(int recordCount, long length, uint crc32c, int formatVersion)
    {
        if (_out == null)
        {
            return;
        }
        Span<byte> trailer = stackalloc byte[TrailerLength];
        BinaryPrimitives.WriteInt64LittleEndian(trailer, _writingRound);
        BinaryPrimitives.WriteInt64LittleEndian(trailer.Slice(8), length);
        BinaryPrimitives.WriteInt32LittleEndian(trailer.Slice(16), recordCount);
        BinaryPrimitives.WriteUInt32LittleEndian(trailer.Slice(20), crc32c);
        BinaryPrimitives.WriteInt32LittleEndian(trailer.Slice(24), formatVersion);
        BinaryPrimitives.WriteInt32LittleEndian(trailer.Slice(28), Magic);
        try
        {
            _out.Write(trailer);
            _out.Dispose();
            _out = null;
        }
        catch (Exception ex) when (IsIoFailure(ex))
        {
            FailWrite(ex);
            return;
        }
        long round = _writingRound;
        AfterWrites(() => MakeDurable(round));
    }

    /// <summary>Drops the write in progress, if any.</summary>
    public void Abandon()
    {
        if (_out == null)
        {
            return;
        }
        try
        {
            _out.Dispose();
        }
        catch (Exception ex) when (IsIoFailure(ex))
        {
            // The file goes regardless.
        }
        _out = null;
        try
        {
            File.Delete(TemporaryFile(_writingRound));
        }
        catch (Exception ex) when (IsIoFailure(ex))
        {
            // A leftover goes with the next DeleteBefore.
        }
    }

    /// <summary>Deletes the file of every round before <paramref name="round"/>, and any write of one a crash left
    /// behind, off this thread once <paramref name="round"/>'s file is durable; if it never is, nothing.</summary>
    /// <param name="round">the oldest round to keep, one this store committed</param>
    public void DeleteBefore(long round)
    {
        AfterWrites(() => DeleteFiles(round));
    }

    /// <summary>Waits until the rounds committed so far are durable, or failed, and the deletions asked for are
    /// done.</summary>
    public void AwaitWrites()
    {
        _writes.Wait();
    }

    /// <summary>The newest round below <paramref name="belowRound"/> with a complete file, or -1.</summary>
    /// <param name="belowRound"><see cref="long.MaxValue"/> for the newest of all</param>
    public long LatestRound(long belowRound)
    {
        long latest = -1;
        try
        {
            foreach (string path in Directory.EnumerateFiles(_directory, "*" + Suffix))
            {
                long round = RoundOf(Path.GetFileName(path), Suffix);
                if (round < belowRound && round > latest)
                {
                    latest = round;
                }
            }
        }
        catch (Exception ex) when (IsIoFailure(ex))
        {
            Logger.Log(Logger.CoreComponent.ReplayerStreamReceiver, Logger.Severity.Warn,
                       Logger.CoreEventCode.SnapshotStoreFailed, null, "cannot list snapshots in {0}: {1}", _directory,
                       ex.Message);
        }
        return latest;
    }

    /// <summary>Opens a round's file to read its records.</summary>
    /// <param name="round">its round</param>
    /// <returns>null if it has none, or its trailer is cut short or names another round</returns>
    public Reader Open(long round)
    {
        string path = FileOf(round);
        FileStream input = null;
        try
        {
            input = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, IoBufferLength);
            long size = input.Length;
            if (size < TrailerLength)
            {
                input.Dispose();
                return null;
            }
            Span<byte> trailer = stackalloc byte[TrailerLength];
            input.Seek(size - TrailerLength, SeekOrigin.Begin);
            // A short read is a file cut short underneath.
            if (input.ReadAtLeast(trailer, TrailerLength, false) != TrailerLength ||
                BinaryPrimitives.ReadInt32LittleEndian(trailer.Slice(28)) != Magic ||
                BinaryPrimitives.ReadInt64LittleEndian(trailer) != round)
            {
                input.Dispose();
                return null;
            }
            input.Seek(0, SeekOrigin.Begin);
            return new Reader(input, size - TrailerLength, BinaryPrimitives.ReadInt32LittleEndian(trailer.Slice(16)),
                              BinaryPrimitives.ReadInt64LittleEndian(trailer.Slice(8)),
                              BinaryPrimitives.ReadUInt32LittleEndian(trailer.Slice(20)),
                              BinaryPrimitives.ReadInt32LittleEndian(trailer.Slice(24)));
        }
        catch (Exception ex) when (ex is FileNotFoundException || ex is DirectoryNotFoundException)
        {
            return null;
        }
        catch (Exception ex) when (IsIoFailure(ex))
        {
            input?.Dispose();
            Logger.Log(Logger.CoreComponent.ReplayerStreamReceiver, Logger.Severity.Warn,
                       Logger.CoreEventCode.SnapshotStoreFailed, null, "cannot open {0}: {1}", path, ex.Message);
            return null;
        }
    }

    private void FailWrite(Exception ex)
    {
        Logger.Log(Logger.CoreComponent.ReplayerStreamReceiver, Logger.Severity.Warn,
                   Logger.CoreEventCode.SnapshotStoreFailed, null, "cannot write round {0}'s snapshot in {1}: {2}",
                   _writingRound, _directory, ex.Message);
        Abandon();
    }

    // Runs a step on a thread of its own once the step before it is done.
    private void AfterWrites(Action step)
    {
        _writes = _writes.ContinueWith(
            _ => step(), CancellationToken.None, TaskContinuationOptions.LongRunning, TaskScheduler.Default);
    }

    // Forces a committed round's file to disk, then names it: only then does the round count as durable.
    private void MakeDurable(long round)
    {
        try
        {
            using (var file = new FileStream(TemporaryFile(round), FileMode.Open, FileAccess.Write))
            {
                file.Flush(true);
            }
            File.Move(TemporaryFile(round), FileOf(round), true);
            _durableRound = round;
        }
        catch (Exception ex) when (IsIoFailure(ex))
        {
            Logger.Log(Logger.CoreComponent.ReplayerStreamReceiver, Logger.Severity.Warn,
                       Logger.CoreEventCode.SnapshotStoreFailed, null,
                       "cannot make round {0}'s snapshot durable in {1}: {2}", round, _directory, ex.Message);
        }
    }

    private void DeleteFiles(long round)
    {
        if (_durableRound < round)
        {
            return;
        }
        try
        {
            foreach (string path in Directory.EnumerateFiles(_directory))
            {
                string name = Path.GetFileName(path);
                long fileRound = Math.Max(RoundOf(name, Suffix), RoundOf(name, TemporarySuffix));
                if (fileRound >= 0 && fileRound < round)
                {
                    File.Delete(path);
                }
            }
        }
        catch (Exception ex) when (IsIoFailure(ex))
        {
            Logger.Log(Logger.CoreComponent.ReplayerStreamReceiver, Logger.Severity.Warn,
                       Logger.CoreEventCode.SnapshotStoreFailed, null,
                       "cannot delete snapshots before round {0} in {1}: {2}", round, _directory, ex.Message);
        }
    }

    private string FileOf(long round)
    {
        return Path.Combine(_directory, round.ToString(CultureInfo.InvariantCulture) + Suffix);
    }

    private string TemporaryFile(long round)
    {
        return Path.Combine(_directory, round.ToString(CultureInfo.InvariantCulture) + TemporarySuffix);
    }

    // What .NET's file APIs throw for a failed read, write, rename or listing.
    private static bool IsIoFailure(Exception ex)
    {
        return ex is IOException || ex is UnauthorizedAccessException;
    }

    /// <summary>The round a name ending in <paramref name="suffix"/> holds, or -1 for any other name.</summary>
    private static long RoundOf(string name, string suffix)
    {
        if (!name.EndsWith(suffix, StringComparison.Ordinal))
        {
            return -1;
        }
        return long.TryParse(name.AsSpan(0, name.Length - suffix.Length), NumberStyles.AllowLeadingSign,
                             CultureInfo.InvariantCulture, out long round)
                   ? round
                   : -1;
    }

    /// <summary>One round's file, read record by record, its trailer known before the first.</summary>
    public sealed class Reader : IDisposable
    {
        /// <summary>After the last record, which every record before it matched the trailer to.</summary>
        public const int End = -1;

        /// <summary>The records do not match the trailer, or could not be read. Latched.</summary>
        public const int Damaged = -2;

        private readonly FileStream _in;
        private readonly long _recordsLength;
        private readonly byte[] _record = new byte[SnapshotFormat.MaxRecordLength];
        private readonly UnsafeBuffer _recordBuffer;
        private long _consumed;
        private long _bytes;
        private int _count;
        private uint _crc;
        private bool _damaged;

        internal Reader(FileStream input, long recordsLength, int recordCount, long length, uint crc32c,
                        int formatVersion)
        {
            _in = input;
            _recordsLength = recordsLength;
            RecordCount = recordCount;
            Length = length;
            Crc32C = crc32c;
            FormatVersion = formatVersion;
            _recordBuffer = new UnsafeBuffer(_record);
        }

        /// <summary>The trailer's <c>recordCount</c>.</summary>
        public int RecordCount { get; }

        /// <summary>The trailer's <c>length</c>.</summary>
        public long Length { get; }

        /// <summary>The trailer's <c>crc32c</c>.</summary>
        public uint Crc32C { get; }

        /// <summary>The trailer's <c>formatVersion</c>.</summary>
        public int FormatVersion { get; }

        /// <summary>Points <paramref name="view"/> at the next record, valid until the next call.</summary>
        /// <param name="view">wrapped over the record</param>
        /// <returns>its length, <see cref="End"/> or <see cref="Damaged"/></returns>
        public int Next(UnsafeBuffer view)
        {
            if (_damaged)
            {
                return Damaged;
            }
            if (_consumed == _recordsLength)
            {
                _damaged = _count != RecordCount || _bytes != Length || _crc != Crc32C;
                return _damaged ? Damaged : End;
            }
            try
            {
                Span<byte> prefix = stackalloc byte[RecordPrefixLength];
                if (_consumed + RecordPrefixLength > _recordsLength ||
                    _in.ReadAtLeast(prefix, RecordPrefixLength, false) != RecordPrefixLength)
                {
                    return Damage();
                }
                int recordLength = BinaryPrimitives.ReadUInt16LittleEndian(prefix);
                if (_consumed + RecordPrefixLength + recordLength > _recordsLength ||
                    _in.ReadAtLeast(_record.AsSpan(0, recordLength), recordLength, false) != recordLength)
                {
                    return Damage();
                }
                _crc = SnapshotFormat.Update(_crc, _record.AsSpan(0, recordLength));
                _consumed += RecordPrefixLength + recordLength;
                _bytes += recordLength;
                _count++;
                view.Wrap(_recordBuffer, 0, recordLength);
                return recordLength;
            }
            catch (Exception ex) when (IsIoFailure(ex))
            {
                return Damage();
            }
        }

        /// <summary>Closes the file; read-only, so nothing is lost.</summary>
        public void Dispose()
        {
            _in.Dispose();
            _recordBuffer.Dispose();
        }

        private int Damage()
        {
            _damaged = true;
            return Damaged;
        }
    }
}
