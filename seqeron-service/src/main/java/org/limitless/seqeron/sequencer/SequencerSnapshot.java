package org.limitless.seqeron.sequencer;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

/**
 * The Sequencer's state as records, the shape of {@code SnapshotListener} (doc/snapshot.md §10.2).
 * {@link SequencerService} writes them to Aeron's snapshot publication and reads them back in {@code onStart}.
 */
interface SequencerSnapshot {
    /** The format {@link #onSnapshot} writes. */
    int formatVersion();

    /**
     * Encodes record {@code recordIndex}; index 0 starts the iteration afresh.
     * @return the record's length, or 0 once there is none
     */
    int onSnapshot(MutableDirectBuffer buffer, int recordIndex);

    /**
     * Takes the records in the order they were written, into a Sequencer that has sequenced nothing.
     * @param formatVersion the version the snapshot was written in
     * @throws IllegalStateException on a record it cannot read: a foreign schema, a newer version, a count that
     *     differs from the end's
     */
    void onRestore(DirectBuffer buffer, int length, int recordIndex, int formatVersion);
}
