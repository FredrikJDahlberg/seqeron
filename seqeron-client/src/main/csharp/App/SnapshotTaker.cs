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
/// <para>It is also what a restore hands the snapshot's records to (§7), passing the source's own to the listener.
/// <c>SnapshotTaker.java</c> and <c>app/detail/SnapshotTaker.hpp</c> are its twins; keep the three in step.</para>
/// </summary>
internal sealed class SnapshotTaker : ISnapshotRestoreHandler
{
    /// <summary>How an instance places the end of a round it publishes.</summary>
    internal interface IActions
    {
        Publish PublishEnd(long round, int recordCount, long length, uint crc32c, int formatVersion);
    }

    /// <summary>How an application tells its node's Replayer where a round stands; each answers whether the offer
    /// landed.</summary>
    internal interface ISignals
    {
        bool Joined(long round);

        bool Confirmed(long round);
    }

    private readonly ISnapshotListener _listener;
    private readonly SnapshotStore _store;
    private readonly Action _keepAlive;
    private readonly UnsafeBuffer _encoding =
        new UnsafeBuffer(GC.AllocateArray<byte>(SnapshotFormat.MaxRecordLength, true));
    private uint _crc;
    private bool _participating;
    private long _round = -1;
    // The last round this instance confirmed whose file is durable; 0 for none yet.
    private long _confirmedRound;
    // The round to answer a SnapshotRound for, or 0; only the latest is kept.
    private long _pendingJoined;
    // The round confirmed and not yet told to the Replayer, or 0; told once its file is durable.
    private long _pendingConfirmed;
    private bool _serialized;
    private bool _publishing;
    private int _recordCount;
    private long _length;

    /// <param name="listener">what serializes the state, or null if this build takes part in no round</param>
    /// <param name="store">where this instance keeps its snapshots; null exactly when <paramref name="listener"/>
    /// is</param>
    /// <param name="keepAlive">keeps the cluster session alive while a round is serialized; self-throttling</param>
    internal SnapshotTaker(ISnapshotListener listener, SnapshotStore store, Action keepAlive)
    {
        _listener = listener;
        _store = store;
        _keepAlive = keepAlive;
    }

    /// <summary>Whether this build takes part in rounds at all: it was given a listener.</summary>
    internal bool IsListening => _listener != null;

    /// <summary>The last round this instance confirmed whose file is durable, its snapshot counter's value; 0 for none
    /// yet.</summary>
    internal long ConfirmedRound => _confirmedRound;

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
    /// <param name="cutClusterSeqNo">its <c>clusterSeqNo</c>, the cut</param>
    /// <param name="cutPosition">where it starts in the recording</param>
    /// <param name="cutTimestampNs">its consensus timestamp</param>
    /// <param name="header">the façade's header as of the cut</param>
    /// <param name="mayPublish">whether this instance is the one that publishes at the cut</param>
    internal void OnSnapshotStarted(long startedRound, long cutClusterSeqNo, long cutPosition, long cutTimestampNs,
                                    SnapshotHeader header, bool mayPublish)
    {
        _publishing = false;
        _serialized = false;
        if (!_participating || header.EncodedLength > SnapshotFormat.MaxRecordLength)
        {
            return;
        }
        _round = startedRound;
        _crc = 0;
        _recordCount = 0;
        _length = 0;
        _store.Begin(_round, cutClusterSeqNo, cutPosition, cutTimestampNs);
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

    /// <summary>The source's own <c>SnapshotEnd</c> was dispatched. One that matches confirms this instance's file
    /// of the round, which then replaces every older one but the newest of them.</summary>
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
        _store.Confirm(_round);
        _pendingConfirmed = _round;
        return true;
    }

    /// <summary>This instance may no longer publish this round: another took the role. It does not resume.</summary>
    internal void StopPublishing()
    {
        _publishing = false;
    }

    internal bool IsPublishing => _publishing;

    /// <summary>The listener's to decide (§9).</summary>
    public bool SupportsFormatVersion(long formatVersion)
    {
        return _listener.SupportsFormatVersion((int)formatVersion);
    }

    /// <summary>The source took part at the cut, and its topology row lies before it, never to be dispatched
    /// here.</summary>
    public void OnSnapshotHeader(SnapshotHeader header)
    {
        SetParticipating(true);
    }

    /// <inheritdoc/>
    public void OnSnapshotRecord(IDirectBuffer record, int length, int recordIndex, long formatVersion)
    {
        _listener.OnRestore(record, length, recordIndex, (int)formatVersion);
    }

    /// <summary>The Replayer cut <paramref name="round"/>: an instance that takes part answers it until the offer
    /// lands.</summary>
    internal void OnSnapshotRound(long round)
    {
        if (_participating)
        {
            _pendingJoined = round;
        }
    }

    /// <summary>Places what the Replayer is still owed, a declined offer being placed again on the next call. A
    /// confirmation waits until its file is durable, and one whose file never becomes durable is never
    /// placed.</summary>
    /// <returns>signals placed</returns>
    internal int Signal(ISignals signals)
    {
        int placed = 0;
        if (_pendingJoined != 0 && signals.Joined(_pendingJoined))
        {
            _pendingJoined = 0;
            placed++;
        }
        if (_pendingConfirmed != 0 && _store.DurableRound >= _pendingConfirmed)
        {
            _confirmedRound = _pendingConfirmed;
            if (signals.Confirmed(_pendingConfirmed))
            {
                _pendingConfirmed = 0;
                placed++;
            }
        }
        return placed;
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
