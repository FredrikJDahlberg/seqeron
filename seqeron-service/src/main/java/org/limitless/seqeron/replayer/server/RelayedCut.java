package org.limitless.seqeron.replayer.server;

import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.BitUtil;

/**
 * A snapshot marker a gateway host's relay has published onto its local queue (doc/snapshot-gateway-purge-plan.md).
 *
 * @param round        the round's number
 * @param clusterSeqNo the marker's {@code clusterSeqNo}: the cut
 * @param groupId      the group whose queue it was published on
 * @param position     where the marker starts in the host's own recording of that queue
 * @param live         whether the relay was caught up with its member's recording when it published the marker,
 *                     rather than replaying history that existed when the relay connected
 */
public record RelayedCut(long round, long clusterSeqNo, int groupId, long position, boolean live) {
    /**
     * Where a frame starts on a queue, from the position {@code offer} returned for it. A term-boundary padding frame
     * precedes the frame when it does not fit the term, and the result is past the frame alone, so the subtraction
     * holds for any frame that is not fragmented.
     *
     * @param end    the offer result: the position just past the frame
     * @param length the offered message's length
     * @return the position the frame starts at
     */
    static long startOf(final long end, final int length) {
        return end - BitUtil.align(length + DataHeaderFlyweight.HEADER_LENGTH, FrameDescriptor.FRAME_ALIGNMENT);
    }
}
