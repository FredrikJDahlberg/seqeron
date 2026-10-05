using Adaptive.Agrona;
using Org.Limitless.Seqeron.Protocol;

namespace Org.Limitless.Seqeron.Replayer.Client;

/// <summary>
/// Takes a source's snapshot as a restore reads it from this instance's own file, before any frame after its cut is
/// dispatched (doc/snapshot.md §7). Each restore, a restart's included, begins with <see cref="OnSnapshotHeader"/>.
/// </summary>
public interface ISnapshotRestoreHandler
{
    /// <summary>Whether this build reads records of <paramref name="formatVersion"/>; a snapshot it does not read
    /// stops the restore.</summary>
    /// <param name="formatVersion">the round's, as its <c>SnapshotEnd</c> carries it</param>
    bool SupportsFormatVersion(long formatVersion);

    /// <summary>Record 0, the façade's; the first call of each restore.</summary>
    /// <param name="header">the decoded header</param>
    void OnSnapshotHeader(SnapshotHeader header);

    /// <summary>One of the source's own records, in order.</summary>
    /// <param name="record">holding the record from offset 0; valid only during this call</param>
    /// <param name="length">the record's length</param>
    /// <param name="recordIndex">0 for the record after the header</param>
    void OnSnapshotRecord(IDirectBuffer record, int length, int recordIndex);
}
