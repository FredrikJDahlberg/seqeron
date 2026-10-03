package org.limitless.seqeron.replayer.server;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.agrona.DirectBuffer;
import org.limitless.seqeron.protocol.SequencedFrameDecoder;
import org.limitless.seqeron.protocol.SystemFrame;
import org.limitless.seqeron.sbe.frame.MessageHeaderDecoder;
import org.limitless.seqeron.sbe.frame.SnapshotEndDecoder;
import org.limitless.seqeron.sbe.frame.SnapshotStartedDecoder;

/**
 * Each source's sequenced {@code SnapshotEnd}s in this node's active recording, by round, with where each round
 * starts (doc/snapshot.md §5), free of Aeron: {@link ReplayerService} feeds it every frame of the recording in
 * order, from its start, with the frame's position. The snapshots' bytes stay with the instances that serialized
 * them; a restoring one asks for the end of the round it holds, to check its file against. A source's first end
 * of a round is the one kept.
 *
 * <p>Single-threaded: the Replayer's duty cycle.
 */
final class SnapshotIndex {
    /** Rounds whose start is remembered, for an end that arrives after a later round started. */
    static final int REMEMBERED_ROUNDS = 4;

    /** One source's end of one round, and where a restore from it resumes. */
    record Entry(long round, long asOfGlobalSeqNo, long asOfPosition, long formatVersion, int recordCount,
                 long length, long crc32c) { }

    /** What one round's {@code SnapshotStarted} said. */
    private record Start(long round, long globalSeqNo, long position) { }

    /** Learns of a source's newest end; how the Replayer keeps its counters. */
    interface Listener {
        void onIndexed(int sourceId, Entry entry);
    }

    private final Listener listener;
    private final SequencedFrameDecoder view = new SequencedFrameDecoder();
    private final SnapshotStartedDecoder started = new SnapshotStartedDecoder();
    private final SnapshotEndDecoder end = new SnapshotEndDecoder();
    private final Start[] starts = new Start[REMEMBERED_ROUNDS];
    private final Map<Integer, Map<Long, Entry>> entries = new HashMap<>();
    private final Map<Integer, Long> newestRounds = new HashMap<>();

    SnapshotIndex(final Listener listener) {
        this.listener = listener;
    }

    /** Forgets everything, as a replay of the recording from its start begins. */
    void reset() {
        Arrays.fill(starts, null);
        entries.clear();
        newestRounds.clear();
    }

    /** The end of {@code sourceId}'s {@code round}, or null. */
    Entry lookup(final int sourceId, final long round) {
        final Map<Long, Entry> rounds = entries.get(sourceId);
        return rounds == null ? null : rounds.get(round);
    }

    /**
     * Takes the next frame of the recording.
     * @param buffer   holding it
     * @param offset   of its first byte
     * @param length   its length
     * @param position the recording position of its first byte
     */
    void onFrame(final DirectBuffer buffer, final int offset, final int length, final long position) {
        if (!view.wrap(buffer, offset, length) || !view.isSystem()) {
            return;
        }
        switch (view.systemEventType()) {
        case SystemFrame.SNAPSHOT_STARTED -> {
            started.wrap(buffer, view.payloadOffset(), SnapshotStartedDecoder.BLOCK_LENGTH,
                         SnapshotStartedDecoder.SCHEMA_VERSION);
            final long round = started.round();
            starts[(int)(round % REMEMBERED_ROUNDS)] = new Start(round, view.globalSeqNo(), position);
        }
        case SystemFrame.SNAPSHOT_END -> {
            end.wrap(buffer, view.payloadOffset(), SnapshotEndDecoder.BLOCK_LENGTH,
                     MessageHeaderDecoder.SCHEMA_VERSION);
            onEnd(view.sourceId());
        }
        default -> {
            // Nothing else locates a snapshot.
        }
        }
    }

    private void onEnd(final int sourceId) {
        final long round = end.round();
        final Start start = starts[Math.floorMod(round, REMEMBERED_ROUNDS)];
        if (start == null || start.round() != round) {
            return; // the round's start is not in this recording, or too far back: nothing to resume at
        }
        final Map<Long, Entry> rounds = entries.computeIfAbsent(sourceId, source -> new HashMap<>());
        if (rounds.containsKey(round)) {
            return;
        }
        final Entry entry = new Entry(round, start.globalSeqNo(), start.position(), end.formatVersion(),
                                      end.recordCount(), end.length(), end.crc32c());
        rounds.put(round, entry);
        if (round > newestRounds.getOrDefault(sourceId, 0L)) {
            newestRounds.put(sourceId, round);
            listener.onIndexed(sourceId, entry);
        }
    }
}
