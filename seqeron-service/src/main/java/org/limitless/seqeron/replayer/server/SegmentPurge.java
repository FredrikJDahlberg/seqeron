package org.limitless.seqeron.replayer.server;

import io.aeron.archive.client.AeronArchive;

/**
 * Where an archive recording may be purged to (doc/snapshot.md §10.6). {@code purgeSegments} takes a segment boundary
 * at or past the end of the recording's first segment, and nothing else.
 */
public final class SegmentPurge {
    /** {@link #target}'s answer when there is nothing to purge. */
    public static final long NOTHING = -1;

    private SegmentPurge() {
    }

    /**
     * The new start of a recording whose segments below {@code floor} are to go.
     *
     * @param startPosition     where the recording starts, which may be mid-term
     * @param termBufferLength  its term length
     * @param segmentFileLength its segment length
     * @param floor             the lowest position that must stay
     * @return the segment boundary at or below the floor, or {@link #NOTHING} while that is not past the end of the
     *     first segment
     */
    public static long target(final long startPosition, final int termBufferLength, final int segmentFileLength,
                              final long floor) {
        final long lowerBound =
            AeronArchive.segmentFileBasePosition(startPosition, startPosition, termBufferLength, segmentFileLength) +
            segmentFileLength;
        final long target =
            AeronArchive.segmentFileBasePosition(startPosition, floor, termBufferLength, segmentFileLength);
        return target < lowerBound ? NOTHING : target;
    }
}
