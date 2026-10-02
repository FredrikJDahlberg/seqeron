package org.limitless.seqeron.app;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.limitless.seqeron.protocol.Publish;
import org.limitless.seqeron.protocol.SnapshotFormat;
import org.limitless.seqeron.protocol.SnapshotHeader;
import org.limitless.seqeron.replayer.client.SnapshotRestoreHandler;

/**
 * A façade's side of snapshot rounds, with no Aeron in it (doc/snapshot.md §4): serialize at the cut,
 * submit the records as chunks if this instance may publish, then the end, and compare the source's
 * sequenced {@code SnapshotEnd} with what this instance serialized (A-7). The façade feeds it the frames and
 * drives {@link #submit} from its duty cycle; every outcome is the façade's to act on.
 *
 * <p>No takeover: an instance that is not the publisher at the cut submits nothing for that round, and a
 * publisher that loses the role stops for good.
 *
 * <p>It is also what a restore hands the snapshot's records to (§7), passing the source's own to the listener.
 */
final class SnapshotTaker implements SnapshotRestoreHandler {
    /** Chunks one duty cycle submits at most, so a large snapshot does not starve the tap. */
    static final int MAX_CHUNKS_PER_CYCLE = 16;

    /** How an instance places the frames of a round it publishes. */
    interface Actions {
        Publish publishChunk(long round, int chunkIndex, DirectBuffer record, int offset, int length);

        Publish publishEnd(long round, int chunkCount, long length, long crc32c, int formatVersion);
    }

    private final SnapshotListener listener;
    private final SnapshotRecords records = new SnapshotRecords();
    private final UnsafeBuffer encoding = new UnsafeBuffer(new byte[SnapshotFormat.MAX_RECORD_LENGTH]);
    private final UnsafeBuffer record = new UnsafeBuffer(0, 0);
    private boolean participating;
    private long round = -1;
    private boolean serialized;
    private boolean publishing;
    private int nextChunkIndex;
    private int recordLength = -1;

    /** @param listener what serializes the state, or null if this build takes part in no round */
    SnapshotTaker(final SnapshotListener listener) {
        this.listener = listener;
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
     * A {@code SnapshotStarted} was dispatched: supersede any round still open and serialize this one, pulling
     * records from the listener until it returns 0. A length outside {@code 0 … 1302} is the listener's bug;
     * the round is dropped, as it is on every instance of the same build.
     * @param startedRound its {@code round}
     * @param header       the façade's header as of the cut
     * @param mayPublish   whether this instance is the one that publishes at the cut
     */
    void onSnapshotStarted(final long startedRound, final SnapshotHeader header, final boolean mayPublish) {
        if (!participating) {
            return;
        }
        round = startedRound;
        publishing = false;
        serialized = records.reset(header, mayPublish);
        for (int recordIndex = 0; serialized; recordIndex++) {
            final int length = listener.onSnapshot(encoding, recordIndex);
            if (length == 0) {
                break;
            }
            if (length < 0 || length > SnapshotFormat.MAX_RECORD_LENGTH) {
                serialized = false;
                records.release();
            } else {
                records.append(encoding, 0, length);
            }
        }
        if (!serialized) {
            return;
        }
        publishing = mayPublish;
        nextChunkIndex = 0;
        recordLength = -1;
    }

    /**
     * The source's own {@code SnapshotEnd} was dispatched.
     * @return false if it is for the round this instance serialized and disagrees with it: this instance has
     *     diverged from the log (A-7)
     */
    boolean onSnapshotEnd(final long endRound, final int chunkCount, final long length, final long crc32c) {
        if (!serialized || endRound != round) {
            return true;
        }
        serialized = false;
        publishing = false;
        records.release();
        return chunkCount == records.chunkCount() && length == records.length() && crc32c == records.crc32c();
    }

    /** This instance may no longer publish this round: another took the role. It does not resume. */
    void stopPublishing() {
        if (publishing) {
            publishing = false;
            records.release();
        }
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
        participating(true);
    }

    @Override
    public void onSnapshotRecord(final DirectBuffer record, final int length, final int recordIndex) {
        listener.onRestore(record, length, recordIndex);
    }

    /**
     * Places what it can of the round being published: chunks in order, then the end, at most {@link
     * #MAX_CHUNKS_PER_CYCLE} a call. A declined frame is placed again on the next call.
     * @return frames placed
     */
    int submit(final Actions actions) {
        int placed = 0;
        while (publishing && placed < MAX_CHUNKS_PER_CYCLE) {
            if (recordLength < 0) {
                recordLength = records.nextRecord(record);
            }
            final Publish outcome = recordLength < 0
                ? actions.publishEnd(round, records.chunkCount(), records.length(), records.crc32c(),
                                     listener.formatVersion())
                : actions.publishChunk(round, nextChunkIndex, record, 0, recordLength);
            if (outcome != Publish.Published) {
                return placed;
            }
            placed++;
            if (recordLength < 0) {
                publishing = false;
                records.release();
            } else {
                nextChunkIndex++;
                recordLength = -1;
            }
        }
        return placed;
    }
}
