using Adaptive.Agrona;

namespace Org.Limitless.Seqeron.App;

/// <summary>
/// What a source that takes part in snapshot rounds serializes, and restores from (doc/snapshot.md §4, §7). Given
/// to a façade's options; a source whose topology row says <c>snapshot="true"</c> takes part only if it has one,
/// and every instance of a source runs the same build, so either all of them do or none does. An instance given one
/// restores its source's latest snapshot on start, if there is one.
/// </summary>
public interface ISnapshotListener
{
    /// <summary>The version of the record format <see cref="OnSnapshot"/> writes, carried in
    /// <c>SnapshotEnd.formatVersion</c> and opaque to seqeron. A restore stops on one its build does not
    /// support.</summary>
    int FormatVersion { get; }

    /// <summary>
    /// Encodes the next record of this instance's state as of the round's cut — every frame up to the
    /// <c>SnapshotStarted</c> dispatched, none after. The façade calls it repeatedly, before it dispatches the next
    /// frame, until it returns 0; it does so on every instance of the source, live or replayed, and every one must
    /// produce the same records for the same state (A-6): no hash-map iteration order, no local time, no node
    /// identity. Must not throw.
    /// </summary>
    /// <param name="buffer">where to encode the record, from offset 0, at most its capacity (65535 bytes); valid
    /// only during this call</param>
    /// <param name="recordIndex">0 on a round's first call, which is where an iteration over the state starts
    /// over</param>
    /// <returns>the record's length, or 0 when the snapshot is complete</returns>
    int OnSnapshot(IMutableDirectBuffer buffer, int recordIndex);

    /// <summary>Decodes the next record of the snapshot this instance restores from, in the order
    /// <see cref="OnSnapshot"/> encoded them, before any frame after its cut is dispatched. A snapshot of no records
    /// makes no call.</summary>
    /// <param name="buffer">holding the record from offset 0; valid only during this call</param>
    /// <param name="length">the record's length</param>
    /// <param name="recordIndex">0 on the first record, which is where the state is cleared: a restore that starts
    /// over begins again at 0</param>
    void OnRestore(IDirectBuffer buffer, int length, int recordIndex);
}
