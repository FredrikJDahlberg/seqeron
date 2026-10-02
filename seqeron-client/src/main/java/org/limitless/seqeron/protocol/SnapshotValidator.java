package org.limitless.seqeron.protocol;

import java.util.zip.CRC32C;
import org.agrona.DirectBuffer;

/**
 * Checks one source's snapshot for one round as its frames come off the tap or a replay, keeping none of its
 * bytes (doc/snapshot.md §5, §7). Valid means: chunks {@code 0 … chunkCount − 1}, each once and in order, then
 * the {@code SnapshotEnd}, whose {@code length} and {@code crc32c} the chunks match. The caller passes only the
 * source's frames; frames of another round are ignored. A snapshot once invalid stays invalid until {@link
 * #reset}. The C++ twin is {@code protocol/Snapshot.hpp}; keep the two in step.
 *
 * <p>Not thread-safe.
 */
public final class SnapshotValidator {
    /** Where the snapshot stands. */
    public enum State {
        /** Chunks so far are in order; no {@code SnapshotEnd} yet. */
        COLLECTING,
        /** The {@code SnapshotEnd} arrived and every check passed. */
        COMPLETE,
        /** A chunk was out of order, repeated or oversized, or the {@code SnapshotEnd} disagreed with the chunks. */
        INVALID
    }

    private final CRC32C crc = new CRC32C();
    private long round;
    private int nextChunkIndex;
    private long length;
    private State state = State.INVALID;

    /** A validator checking no round until {@link #reset}. */
    public SnapshotValidator() {
    }

    /**
     * Starts checking {@code round}, forgetting anything seen before.
     * @param round the round whose frames to take
     */
    public void reset(final long round) {
        this.round = round;
        nextChunkIndex = 0;
        length = 0;
        crc.reset();
        state = State.COLLECTING;
    }

    /**
     * Takes one {@code SnapshotChunk} of the source. A restore hands the record on only while this answers
     * {@link State#COLLECTING}.
     * @param chunkRound the chunk's {@code round}
     * @param chunkIndex the chunk's {@code chunkIndex}
     * @param data       holding the chunk's {@code data}, one record
     * @param offset     of its first byte
     * @param dataLength of its {@code data}
     * @return where the snapshot stands after it
     */
    public State onChunk(final long chunkRound, final int chunkIndex, final DirectBuffer data, final int offset,
                         final int dataLength) {
        if (state != State.COLLECTING || chunkRound != round) {
            return state;
        }
        if (chunkIndex != nextChunkIndex || dataLength > SnapshotFormat.MAX_RECORD_LENGTH) {
            state = State.INVALID;
            return state;
        }
        SnapshotFormat.update(crc, data, offset, dataLength);
        length += dataLength;
        nextChunkIndex++;
        return state;
    }

    /**
     * Takes the source's {@code SnapshotEnd}.
     * @param endRound   its {@code round}
     * @param chunkCount its {@code chunkCount}
     * @param endLength  its {@code length}
     * @param crc32c     its {@code crc32c}
     * @return where the snapshot stands after it
     */
    public State onEnd(final long endRound, final int chunkCount, final long endLength, final long crc32c) {
        if (state != State.COLLECTING || endRound != round) {
            return state;
        }
        state = chunkCount == nextChunkIndex && endLength == length && crc32c == crc.getValue() ? State.COMPLETE
                                                                                                  : State.INVALID;
        return state;
    }

    /** Where the snapshot stands. */
    public State state() {
        return state;
    }

    /** The round being checked. */
    public long round() {
        return round;
    }

    /** How many bytes the chunks so far carried. */
    public long length() {
        return length;
    }
}
