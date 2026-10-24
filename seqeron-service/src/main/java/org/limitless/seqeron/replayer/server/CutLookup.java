package org.limitless.seqeron.replayer.server;

import java.util.HashMap;
import org.agrona.collections.Long2LongHashMap;

/**
 * Where a snapshot's cut is in a queue recording, free of Aeron: a restoring client names the cut by its
 * {@code clusterSeqNo} and hints at a position (doc/snapshot.md §10.8). A cut the Replayer has kept is answered from
 * memory; any other is found by reading the recording for the frame, from the hint when it is a frame boundary and
 * otherwise from the recording's start. {@link ReplayerService} drives it, with the archive behind {@link Replayer}.
 *
 * <p>Single-threaded: the Replayer's duty cycle.
 */
final class CutLookup {
    private static final long MISSING = -1;

    /** What a lookup decided. */
    enum Verdict {
        /** The cut starts at {@link Answer#position}. */
        FOUND,
        /** The cut is before the recording's first frame (spec R-2). */
        BELOW,
        /** The recording passes the cut without a frame at it: it is not the log the cut was made in. */
        NOT_IN_LOG,
        /** The recording has not reached the cut yet. */
        PENDING
    }

    /** A verdict, with the cut's position when it is {@link Verdict#FOUND}. */
    record Answer(Verdict verdict, long position) {
        private static final Answer BELOW = new Answer(Verdict.BELOW, -1);
        private static final Answer NOT_IN_LOG = new Answer(Verdict.NOT_IN_LOG, -1);
        private static final Answer PENDING = new Answer(Verdict.PENDING, -1);
    }

    /** Reads a recording's frames in order for {@link #find}; {@link Replayer#scanQueueRecording} is one. */
    @FunctionalInterface
    interface Scanner {
        Replayer.ScanEnd scan(long recordingId, long fromPosition, long tip, Replayer.FrameVisitor visitor);
    }

    private final HashMap<Long, Long2LongHashMap> kept = new HashMap<>();

    /**
     * Remembers where a cut was recorded.
     * @param recordingId       the recording of the queue it is on
     * @param cutClusterSeqNo   the cut
     * @param position          where its frame starts in that recording
     */
    void keep(final long recordingId, final long cutClusterSeqNo, final long position) {
        kept.computeIfAbsent(recordingId, id -> new Long2LongHashMap(MISSING)).put(cutClusterSeqNo, position);
    }

    /**
     * Forgets the cuts of a recording that lie below its new start, which a purge moved.
     * @param recordingId the recording
     * @param startPosition where it starts now
     */
    void forgetBelow(final long recordingId, final long startPosition) {
        final Long2LongHashMap cuts = kept.get(recordingId);
        if (cuts != null) {
            cuts.entrySet().removeIf(entry -> entry.getValue() < startPosition);
        }
    }

    /**
     * Finds a cut in a recording.
     *
     * @param recordingId     the recording
     * @param startPosition   where it starts
     * @param tip             where it has reached
     * @param hint            the position the client's file names, tried first
     * @param cutClusterSeqNo the cut
     * @param scanner         reads the recording when the cut is not kept
     * @return the verdict
     */
    Answer find(final long recordingId, final long startPosition, final long tip, final long hint,
                final long cutClusterSeqNo, final Scanner scanner) {
        final Long2LongHashMap cuts = kept.get(recordingId);
        final long position = cuts == null ? MISSING : cuts.get(cutClusterSeqNo);
        if (position != MISSING) {
            return position < startPosition ? Answer.BELOW : new Answer(Verdict.FOUND, position);
        }
        if (tip <= startPosition) {
            return Answer.PENDING;
        }
        if (hint > startPosition && hint < tip) {
            try {
                final Answer fromHint = scan(recordingId, hint, tip, cutClusterSeqNo, scanner, false);
                if (fromHint != null) {
                    return fromHint;
                }
            } catch (final RuntimeException refused) {
                // A hint that is no frame boundary may be refused by the archive; the recording's start is not.
            }
        }
        return scan(recordingId, startPosition, tip, cutClusterSeqNo, scanner, true);
    }

    /**
     * One pass over the recording from {@code from}.
     * @param fromStart whether {@code from} is the recording's start: a first frame past the cut then means the cut is
     *     below the recording, and from a hint only that the hint was no help
     * @return the answer, or null when a pass from a hint found nothing to conclude from
     */
    private static Answer scan(final long recordingId, final long from, final long tip, final long cutClusterSeqNo,
                               final Scanner scanner, final boolean fromStart) {
        final long[] found = {MISSING};
        final boolean[] first = {true};
        final boolean[] past = {false};
        final boolean[] pastFirst = {false};
        final Replayer.ScanEnd end = scanner.scan(recordingId, from, tip, (framePosition, clusterSeqNo) -> {
            final boolean firstFrame = first[0];
            first[0] = false;
            if (clusterSeqNo < cutClusterSeqNo) {
                return true;
            }
            if (clusterSeqNo == cutClusterSeqNo) {
                found[0] = framePosition;
            } else {
                past[0] = true;
                pastFirst[0] = firstFrame;
            }
            return false;
        });
        if (found[0] != MISSING) {
            return new Answer(Verdict.FOUND, found[0]);
        }
        if (past[0]) {
            if (pastFirst[0] && fromStart) {
                return Answer.BELOW;
            }
            return pastFirst[0] ? null : Answer.NOT_IN_LOG;
        }
        if (end == Replayer.ScanEnd.UNREADABLE) {
            return fromStart ? Answer.NOT_IN_LOG : null;
        }
        return Answer.PENDING;
    }
}
