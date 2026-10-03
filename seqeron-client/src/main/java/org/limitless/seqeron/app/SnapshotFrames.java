package org.limitless.seqeron.app;

import java.util.function.IntSupplier;
import org.agrona.ExpandableArrayBuffer;
import org.limitless.seqeron.protocol.FrameLayer;
import org.limitless.seqeron.protocol.Publish;
import org.limitless.seqeron.protocol.SystemFrame;
import org.limitless.seqeron.sbe.frame.SnapshotEndEncoder;

/**
 * The frame of a round a façade publishes, its {@code SnapshotEnd} (doc/snapshot.md §4), under its {@code sourceId}
 * and belonging to no connection. The C++ twin is {@code app/detail/SnapshotFrames.hpp}; keep the two in step.
 */
final class SnapshotFrames implements SnapshotTaker.Actions {
    private static final int NO_CONNECTION = -1;

    private final ExpandableArrayBuffer body = new ExpandableArrayBuffer(FrameLayer.MAX_PAYLOAD_LENGTH);
    private final SnapshotEndEncoder end = new SnapshotEndEncoder();
    private final Session session;
    private final IntSupplier sourceId;

    /**
     * @param session  what places the frames
     * @param sourceId the façade's {@code sourceId}, read at each frame: a gateway's resolves from its row
     */
    SnapshotFrames(final Session session, final IntSupplier sourceId) {
        this.session = session;
        this.sourceId = sourceId;
    }

    @Override
    public Publish publishEnd(final long round, final int recordCount, final long length, final long crc32c,
                              final int formatVersion) {
        end.wrap(body, 0).round(round).recordCount(recordCount).length(length).crc32c(crc32c)
            .formatVersion(formatVersion & 0xFFFF_FFFFL);
        return placed(session.publishSystem(sourceId.getAsInt(), NO_CONNECTION, SystemFrame.SNAPSHOT_END, body,
                                            end.encodedLength()));
    }

    /** A refused end is this class's own bug, never a condition to wait out. */
    private static Publish placed(final Publish outcome) {
        if (outcome == Publish.Refused) {
            throw new IllegalStateException("a SnapshotEnd the sequencer would reject "
                                            + "(doc/seqeron-protocol-spec.md §9.2)");
        }
        return outcome;
    }
}
