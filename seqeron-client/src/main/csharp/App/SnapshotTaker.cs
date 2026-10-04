using System;
using Adaptive.Agrona;
using Adaptive.Agrona.Concurrent;
using Org.Limitless.Seqeron.Protocol;
using Org.Limitless.Seqeron.Replayer.Client;

namespace Org.Limitless.Seqeron.App;

/// <summary>
/// A façade's side of snapshot rounds, with no Aeron in it (doc/snapshot.md §4): serialize at the cut into this
/// instance's own file, submit the round's <c>SnapshotEnd</c> if this instance may publish, and compare the
/// source's sequenced <c>SnapshotEnd</c> with what this instance serialized (A-7). The façade feeds it the frames
/// and drives <see cref="Submit"/> from its duty cycle; every outcome is the façade's to act on.
/// <para>No takeover: an instance that is not the publisher at the cut submits nothing for that round, and a
/// publisher that loses the role stops for good.</para>
/// <para>It is also what a restore hands the snapshot's records to (§7), passing the source's own to the
/// listener.</para>
/// <para>A passive instance holds no state until it is activated (§4): it takes part in no round, a restore hands
/// its listener nothing, and the façade dispatches it no payload while <see cref="HoldsState"/> is false.
/// <c>SnapshotTaker.java</c> and <c>app/detail/SnapshotTaker.hpp</c> are its twins; keep the three in step.</para>
/// </summary>
internal sealed class SnapshotTaker : ISnapshotRestoreHandler
{
    /// <summary>How an instance places the end of a round it publishes.</summary>
    internal interface IActions
    {
        Publish PublishEnd(long round, int recordCount, long length, uint crc32c, int formatVersion);
    }

    private readonly ISnapshotListener _listener;
    private readonly SnapshotStore _store;
    private readonly Action _keepAlive;
    private readonly UnsafeBuffer _encoding =
        new UnsafeBuffer(GC.AllocateArray<byte>(SnapshotFormat.MaxRecordLength, true));
    private uint _crc;
    private bool _participating;
    private bool _passive;
    private long _round = -1;
    private bool _serialized;
    private bool _publishing;
    private int _recordCount;
    private long _length;

    /// <param name="listener">what serializes the state, or null if this build takes part in no round</param>
    /// <param name="store">where this instance keeps its snapshots; null exactly when <paramref name="listener"/>
    /// is</param>
    /// <param name="passive">whether this instance holds no state until it is activated</param>
    /// <param name="keepAlive">keeps the cluster session alive while a round is serialized; self-throttling</param>
    internal SnapshotTaker(ISnapshotListener listener, SnapshotStore store, bool passive, Action keepAlive)
    {
        _listener = listener;
        _store = store;
        _passive = passive;
        _keepAlive = keepAlive;
    }

    /// <summary>Whether this instance holds the source's state: false while it is passive.</summary>
    internal bool HoldsState => !_passive;

    /// <summary>Ends passivity once this instance is activated and caught up on the election.</summary>
    /// <returns>true exactly then: the façade restarts its recovery, which restores the state this instance now
    /// holds</returns>
    internal bool Activate(bool activated, bool caughtUp)
    {
        if (!_passive || !activated || !caughtUp)
        {
            return false;
        }
        _passive = false;
        return true;
    }

    /// <summary>Whether the source's topology row takes part. Without a listener it cannot, whatever the row
    /// says.</summary>
    /// <param name="snapshot">the row's <c>snapshot</c></param>
    internal void SetParticipating(bool snapshot)
    {
        _participating = snapshot && _listener != null;
    }

    internal bool IsParticipating => _participating;

    /// <summary>
    /// A <c>SnapshotStarted</c> was dispatched: supersede any round still open and serialize this one into its
    /// file, pulling records from the listener until it returns 0. A length outside <c>0 … 65535</c> is the
    /// listener's bug; the round is dropped, as it is on every instance of the same build, and so is one whose
    /// header outgrows a record. One that no longer takes part still abandons the round it held. The cluster
    /// session is kept alive after each record, so the round may take as long as its state needs.
    /// </summary>
    /// <param name="startedRound">its <c>round</c></param>
    /// <param name="header">the façade's header as of the cut</param>
    /// <param name="mayPublish">whether this instance is the one that publishes at the cut</param>
    internal void OnSnapshotStarted(long startedRound, SnapshotHeader header, bool mayPublish)
    {
        _publishing = false;
        _serialized = false;
        if (_passive || !_participating || header.EncodedLength > SnapshotFormat.MaxRecordLength)
        {
            return;
        }
        _round = startedRound;
        _crc = 0;
        _recordCount = 0;
        _length = 0;
        _store.Begin(_round);
        Append(header.Encode(_encoding, 0));
        for (int recordIndex = 0;; recordIndex++)
        {
            int recordLength = _listener.OnSnapshot(_encoding, recordIndex);
            if (recordLength == 0)
            {
                break;
            }
            if (recordLength < 0 || recordLength > SnapshotFormat.MaxRecordLength)
            {
                _store.Abandon();
                return;
            }
            Append(recordLength);
        }
        _store.Commit(_recordCount, _length, _crc, _listener.FormatVersion);
        _serialized = true;
        _publishing = mayPublish;
    }

    /// <summary>The source's own <c>SnapshotEnd</c> was dispatched. One that matches makes this round's file the
    /// oldest this instance keeps.</summary>
    /// <returns>false if it is for the round this instance serialized and disagrees with it: this instance has
    /// diverged from the log (A-7)</returns>
    internal bool OnSnapshotEnd(long endRound, int endRecordCount, long endLength, uint crc32c)
    {
        if (!_serialized || endRound != _round)
        {
            return true;
        }
        _serialized = false;
        _publishing = false;
        if (endRecordCount != _recordCount || endLength != _length || crc32c != _crc)
        {
            return false;
        }
        _store.DeleteBefore(_round);
        return true;
    }

    /// <summary>This instance may no longer publish this round: another took the role. It does not resume.</summary>
    internal void StopPublishing()
    {
        _publishing = false;
    }

    internal bool IsPublishing => _publishing;

    /// <summary>The build reads only the format it writes (§9).</summary>
    public bool SupportsFormatVersion(long formatVersion)
    {
        return formatVersion == (uint)_listener.FormatVersion;
    }

    /// <summary>The source took part at the cut, and its topology row lies before it, never to be dispatched
    /// here.</summary>
    public void OnSnapshotHeader(SnapshotHeader header)
    {
        if (!_passive)
        {
            SetParticipating(true);
        }
    }

    /// <inheritdoc/>
    public void OnSnapshotRecord(IDirectBuffer record, int length, int recordIndex)
    {
        if (!_passive)
        {
            _listener.OnRestore(record, length, recordIndex);
        }
    }

    /// <summary>Places the end of the round being published. A declined end is placed again on the next
    /// call.</summary>
    /// <returns>frames placed</returns>
    internal int Submit(IActions actions)
    {
        if (!_publishing ||
            actions.PublishEnd(_round, _recordCount, _length, _crc, _listener.FormatVersion) != Publish.Published)
        {
            return 0;
        }
        _publishing = false;
        return 1;
    }

    // Takes the record _encoding holds.
    private void Append(int recordLength)
    {
        _crc = SnapshotFormat.Update(_crc, _encoding, 0, recordLength);
        _recordCount++;
        _length += recordLength;
        _store.Append(_encoding, 0, recordLength);
        _keepAlive();
    }
}
