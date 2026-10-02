package org.limitless.seqeron.replayer.client;

import org.agrona.DirectBuffer;
import org.limitless.seqeron.protocol.SnapshotHeader;

/**
 * Takes a source's latest snapshot as it is replayed, before any frame after its cut is dispatched
 * (doc/snapshot.md §7). A restore that starts over, after a replay lost under it, begins again with {@link
 * #onSnapshotHeader}.
 */
public interface SnapshotRestoreHandler {
    /** Whether this build reads records of {@code formatVersion}; a snapshot it does not read stops the restore. */
    boolean supportsFormatVersion(long formatVersion);

    /** Record 0, the façade's; the first call of a restore, and of each time it starts over. */
    void onSnapshotHeader(SnapshotHeader header);

    /**
     * One of the source's own records, in order.
     * @param record      holding the record from offset 0; valid only during this call
     * @param length      the record's length
     * @param recordIndex 0 for the record after the header
     */
    void onSnapshotRecord(DirectBuffer record, int length, int recordIndex);
}
