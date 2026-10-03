package org.limitless.seqeron.app;

import java.util.zip.CRC32C;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.limitless.seqeron.protocol.Publish;
import org.limitless.seqeron.protocol.SnapshotFormat;
import org.limitless.seqeron.protocol.SnapshotHeader;
import org.limitless.seqeron.replayer.client.SnapshotRestoreHandler;
import org.limitless.seqeron.replayer.client.SnapshotStore;

/**
 * A façade's side of snapshot rounds, with no Aeron in it (doc/snapshot.md §4): serialize at the cut into this
 * instance's own file, submit the round's {@code SnapshotEnd} if this instance may publish, and compare the
 * source's sequenced {@code SnapshotEnd} with what this instance serialized (A-7). The façade feeds it the frames
 * and drives {@link #submit} from its duty cycle; every outcome is the façade's to act on.
 *
 * <p>No takeover: an instance that is not the publisher at the cut submits nothing for that round, and a
 * publisher that loses the role stops for good.
 *
 * <p>It is also what a restore hands the snapshot's records to (§7), passing the source's own to the listener.
 *
 * <p>A passive instance holds no state until it is activated (§4): it takes part in no round, a restore hands its
 * listener nothing, and the façade dispatches it no payload while {@link #holdsState} is false. The C++ twin is {@code app/detail/SnapshotTaker.hpp}; keep the two in step.
 */
final class SnapshotTaker implements SnapshotRestoreHandler {
    /** How an instance places the end of a round it publishes. */
    interface Actions {
        Publish publishEnd(long round, int recordCount, long length, long crc32c, int formatVersion);
    }

    private final SnapshotListener listener;
    private final SnapshotStore store;
    private final UnsafeBuffer encoding = new UnsafeBuffer(new byte[SnapshotFormat.MAX_RECORD_LENGTH]);
    private final CRC32C crc = new CRC32C();
    private boolean participating;
    private boolean passive;
    private long round = -1;
    private boolean serialized;
    private boolean publishing;
    private int recordCount;
    private long length;

    /**
     * @param listener what serializes the state, or null if this build takes part in no round
     * @param store    where this instance keeps its snapshots; null exactly when {@code listener} is
     * @param passive  whether this instance holds no state until it is activated
     */
    SnapshotTaker(final SnapshotListener listener, final SnapshotStore store, final boolean passive) {
        this.listener = listener;
        this.store = store;
        this.passive = passive;
    }

    /** Whether this instance holds the source's state: false while it is passive. */
    boolean holdsState() {
        return !passive;
    }

    /**
     * Ends passivity once this instance is activated and caught up on the election.
     * @return true exactly then: the façade restarts its recovery, which restores the state this instance now holds
     */
    boolean activate(final boolean activated, final boolean caughtUp) {
        if (!passive || !activated || !caughtUp) {
            return false;
        }
        passive = false;
        return true;
    }

    /**
     * Whether the source's topology row takes part. Without a listener it cannot, whatever the row says.
     * @param snapshot the row's {@code snapshot}
     */
    void participating(final boolean snapshot) {
        participating = snapshot && listener != null;
    }

    boolean isParticipating() {
        return participating;
    }

    /**
     * A {@code SnapshotStarted} was dispatched: supersede any round still open and serialize this one into its
     * file, pulling records from the listener until it returns 0. A length outside {@code 0 … 1302} is the
     * listener's bug; the round is dropped, as it is on every instance of the same build, and so is one whose
     * header outgrows a record. One that no longer takes part still abandons the round it held.
     * @param startedRound its {@code round}
     * @param header       the façade's header as of the cut
     * @param mayPublish   whether this instance is the one that publishes at the cut
     */
    void onSnapshotStarted(final long startedRound, final SnapshotHeader header, final boolean mayPublish) {
        publishing = false;
        serialized = false;
        if (passive || !participating || header.encodedLength() > SnapshotFormat.MAX_RECORD_LENGTH) {
            return;
        }
        round = startedRound;
        crc.reset();
        recordCount = 0;
        length = 0;
        store.begin(round);
        append(header.encode(encoding, 0));
        for (int recordIndex = 0;; recordIndex++) {
            final int recordLength = listener.onSnapshot(encoding, recordIndex);
            if (recordLength == 0) {
                break;
            }
            if (recordLength < 0 || recordLength > SnapshotFormat.MAX_RECORD_LENGTH) {
                store.abandon();
                return;
            }
            append(recordLength);
        }
        store.commit(recordCount, length, crc.getValue(), listener.formatVersion());
        serialized = true;
        publishing = mayPublish;
    }

    /**
     * The source's own {@code SnapshotEnd} was dispatched. One that matches makes this round's file the oldest
     * this instance keeps.
     * @return false if it is for the round this instance serialized and disagrees with it: this instance has
     *     diverged from the log (A-7)
     */
    boolean onSnapshotEnd(final long endRound, final int endRecordCount, final long endLength, final long crc32c) {
        if (!serialized || endRound != round) {
            return true;
        }
        serialized = false;
        publishing = false;
        if (endRecordCount != recordCount || endLength != length || crc32c != crc.getValue()) {
            return false;
        }
        store.deleteBefore(round);
        return true;
    }

    /** This instance may no longer publish this round: another took the role. It does not resume. */
    void stopPublishing() {
        publishing = false;
    }

    boolean isPublishing() {
        return publishing;
    }

    /** The build reads only the format it writes (§9). */
    @Override
    public boolean supportsFormatVersion(final long formatVersion) {
        return formatVersion == (listener.formatVersion() & 0xFFFF_FFFFL);
    }

    /** The source took part at the cut, and its topology row lies before it, never to be dispatched here. */
    @Override
    public void onSnapshotHeader(final SnapshotHeader header) {
        if (!passive) {
            participating(true);
        }
    }

    @Override
    public void onSnapshotRecord(final DirectBuffer record, final int length, final int recordIndex) {
        if (!passive) {
            listener.onRestore(record, length, recordIndex);
        }
    }

    /**
     * Places the end of the round being published. A declined end is placed again on the next call.
     * @return frames placed
     */
    int submit(final Actions actions) {
        if (!publishing || actions.publishEnd(round, recordCount, length, crc.getValue(), listener.formatVersion())
            != Publish.Published) {
            return 0;
        }
        publishing = false;
        return 1;
    }

    /** Takes the record {@link #encoding} holds. */
    private void append(final int recordLength) {
        crc.update(encoding.byteArray(), 0, recordLength);
        recordCount++;
        length += recordLength;
        store.append(encoding, 0, recordLength);
    }
}
