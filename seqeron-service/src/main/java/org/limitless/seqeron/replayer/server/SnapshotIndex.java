package org.limitless.seqeron.replayer.server;

import java.util.HashMap;
import java.util.Map;
import org.agrona.DirectBuffer;
import org.limitless.seqeron.protocol.SequencedFrameDecoder;
import org.limitless.seqeron.protocol.SnapshotValidator;
import org.limitless.seqeron.protocol.SystemFrame;
import org.limitless.seqeron.sbe.frame.MessageHeaderDecoder;
import org.limitless.seqeron.sbe.frame.SnapshotChunkDecoder;
import org.limitless.seqeron.sbe.frame.SnapshotEndDecoder;
import org.limitless.seqeron.sbe.frame.SnapshotStartedDecoder;

/**
 * Each source's latest valid snapshot in this node's active recording (doc/snapshot.md §5), free of Aeron:
 * {@link ReplayerService} feeds it every frame of the recording in order, from its start, with the frame's
 * position. A snapshot is valid once its chunks pass {@link SnapshotValidator}; one that fails is never
 * indexed, and the source's previous one stays.
 *
 * <p>Single-threaded: the Replayer's duty cycle.
 */
final class SnapshotIndex {
    /** Rounds whose start is remembered, for a chunk 0 that arrives after a later round started. */
    static final int REMEMBERED_ROUNDS = 4;

    /** One source's latest valid snapshot: where a restore replays from, to, and resumes. */
    record Entry(long round, long asOfGlobalSeqNo, long asOfPosition, long endPosition, long formatVersion) { }

    /** What one round's {@code SnapshotStarted} said. */
    private record Start(long round, long globalSeqNo, long position) { }

    /** One source's snapshot in progress. */
    private static final class Pending {
        final SnapshotValidator validator = new SnapshotValidator();
        Start start;
    }

    /** Learns of a source whose latest valid snapshot changed; how the Replayer keeps its counters. */
    interface Listener {
        void onIndexed(int sourceId, Entry entry);
    }

    private final Listener listener;
    private final SequencedFrameDecoder view = new SequencedFrameDecoder();
    private final SnapshotStartedDecoder started = new SnapshotStartedDecoder();
    private final SnapshotChunkDecoder chunk = new SnapshotChunkDecoder();
    private final SnapshotEndDecoder end = new SnapshotEndDecoder();
    private final Start[] starts = new Start[REMEMBERED_ROUNDS];
    private final Map<Integer, Pending> pending = new HashMap<>();
    private final Map<Integer, Entry> entries = new HashMap<>();

    SnapshotIndex(final Listener listener) {
        this.listener = listener;
    }

    /** Forgets everything, as a replay of the recording from its start begins. */
    void reset() {
        java.util.Arrays.fill(starts, null);
        pending.clear();
        entries.clear();
    }

    /** The latest valid snapshot of {@code sourceId}, or null. */
    Entry lookup(final int sourceId) {
        return entries.get(sourceId);
    }

    /**
     * Takes the next frame of the recording.
     * @param buffer        holding it
     * @param offset        of its first byte
     * @param length        its length
     * @param startPosition the recording position of its first byte
     * @param endPosition   the recording position just past it
     */
    void onFrame(final DirectBuffer buffer, final int offset, final int length, final long startPosition,
                 final long endPosition) {
        if (!view.wrap(buffer, offset, length) || !view.isSystem()) {
            return;
        }
        switch (view.systemEventType()) {
        case SystemFrame.SNAPSHOT_STARTED -> {
            started.wrap(buffer, view.payloadOffset(), SnapshotStartedDecoder.BLOCK_LENGTH,
                         SnapshotStartedDecoder.SCHEMA_VERSION);
            final long round = started.round();
            starts[(int)(round % REMEMBERED_ROUNDS)] = new Start(round, view.globalSeqNo(), startPosition);
        }
        case SystemFrame.SNAPSHOT_CHUNK -> {
            chunk.wrap(buffer, view.payloadOffset(), SnapshotChunkDecoder.BLOCK_LENGTH,
                       MessageHeaderDecoder.SCHEMA_VERSION);
            onChunk(view.sourceId(), buffer);
        }
        case SystemFrame.SNAPSHOT_END -> {
            end.wrap(buffer, view.payloadOffset(), SnapshotEndDecoder.BLOCK_LENGTH,
                     MessageHeaderDecoder.SCHEMA_VERSION);
            onEnd(view.sourceId(), endPosition);
        }
        default -> {
            // Nothing else locates a snapshot.
        }
        }
    }

    /** Chunk 0 of a round begins that source's snapshot of it; a source has one in progress at a time. */
    private void onChunk(final int sourceId, final DirectBuffer buffer) {
        final long round = chunk.round();
        Pending source = pending.get(sourceId);
        if (chunk.chunkIndex() == 0 && (source == null || source.start == null || source.start.round() != round)) {
            final Start start = starts[(int)(round % REMEMBERED_ROUNDS)];
            if (start == null || start.round() != round) {
                return; // the round's start is not in this recording, or too far back: nothing to index
            }
            if (source == null) {
                source = new Pending();
                pending.put(sourceId, source);
            }
            source.start = start;
            source.validator.reset(round);
        }
        if (source != null) {
            final int dataOffset = chunk.limit() + SnapshotChunkDecoder.dataHeaderLength();
            source.validator.onChunk(round, chunk.chunkIndex(), buffer, dataOffset, chunk.dataLength());
        }
    }

    private void onEnd(final int sourceId, final long endPosition) {
        final Pending source = pending.get(sourceId);
        if (source == null || source.start == null || source.start.round() != end.round()) {
            return;
        }
        if (source.validator.onEnd(end.round(), end.chunkCount(), end.length(), end.crc32c()) !=
            SnapshotValidator.State.COMPLETE) {
            return;
        }
        final Entry previous = entries.get(sourceId);
        if (previous != null && previous.asOfGlobalSeqNo() >= source.start.globalSeqNo()) {
            return;
        }
        final Entry entry = new Entry(end.round(), source.start.globalSeqNo(), source.start.position(), endPosition,
                                      end.formatVersion());
        entries.put(sourceId, entry);
        listener.onIndexed(sourceId, entry);
    }
}
