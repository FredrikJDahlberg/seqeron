package org.limitless.seqeron.replayer.server;

import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.SourceLocation;
import java.util.ArrayDeque;
import org.agrona.concurrent.UnsafeBuffer;
import org.limitless.seqeron.util.Logger;

/**
 * Publishes a member's {@link DerivationAnnouncement}s on the stream its archive records, which a gateway host starts
 * from: the first checkpoint the {@link LogFollower} keeps, again each time a purge releases it.
 *
 * <p>Not thread-safe: every call is from the Replayer's duty-cycle thread.
 */
final class DerivationAnnouncer {
    private final Aeron aeron;
    private final AeronArchive archive;
    private final int memberId;
    private LogFollower follower;
    private int commitCounterId;
    private ExclusivePublication publication;
    /** Whether the archive records the announcements, and whether it last refused to. */
    private boolean recording;
    private boolean refused;
    private final ArrayDeque<byte[]> unannounced = new ArrayDeque<>();
    private final UnsafeBuffer buffer = new UnsafeBuffer();
    /** The checkpoint announced last: the first kept. */
    private LogFollower.Checkpoint announced;

    DerivationAnnouncer(final Aeron aeron, final AeronArchive archive, final int memberId) {
        this.aeron = aeron;
        this.archive = archive;
        this.memberId = memberId;
    }

    /** Whether the archive records the announcements. */
    boolean recording() {
        return recording;
    }

    /**
     * Opens the stream and asks the archive to record it.
     * @param follower        whose first checkpoint is announced
     * @param commitCounterId the counter of the log's commit position, which a reader of the announcement follows
     */
    void start(final LogFollower follower, final int commitCounterId) {
        this.follower = follower;
        this.commitCounterId = commitCounterId;
        publication = aeron.addExclusivePublication(DerivationAnnouncement.CHANNEL, DerivationAnnouncement.STREAM_ID);
        record();
    }

    /**
     * Records this run's announcements, and purges the ones earlier runs left. A refusal, such as a purge the archive
     * declines while a gateway host replays the recording, is tried again at the next attempt.
     */
    void record() {
        try {
            for (final ArchiveRecording earlier :
                 ArchiveRecording.ofStream(archive, DerivationAnnouncement.STREAM_ID)) {
                if (!earlier.active()) {
                    archive.purgeRecording(earlier.recordingId());
                }
            }
            archive.startRecording(DerivationAnnouncement.CHANNEL, DerivationAnnouncement.STREAM_ID,
                                   SourceLocation.LOCAL, true);
            recording = true;
            if (refused) {
                Logger.info(Logger.CoreComponent.ReplayerService, memberId, "recording this member's announcements");
            }
        } catch (final RuntimeException ex) {
            if (!refused) {
                refused = true;
                Logger.error(Logger.CoreComponent.ReplayerService, Logger.CoreEventCode.ServiceError, memberId,
                             "the archive did not record this member's announcements, which a gateway host starts "
                                 + "from (%s); trying again every %dms",
                             ex.getMessage(), DerivingReplayer.FOLLOW_RETRY_MS);
            }
        }
    }

    /** @return the announcements offered */
    int doWork() {
        if (recording) {
            announceFirstCheckpoint();
        }
        int work = 0;
        while (!unannounced.isEmpty()) {
            buffer.wrap(unannounced.peek());
            if (publication.offer(buffer, 0, buffer.capacity()) <= 0) {
                break;
            }
            unannounced.poll();
            work++;
        }
        return work;
    }

    /** Announces the first checkpoint kept once a purge has released the one announced before it. */
    private void announceFirstCheckpoint() {
        final LogFollower.Checkpoint first = follower.checkpoints().getFirst();
        if (first != announced && unannounced.isEmpty()) {
            announced = first;
            unannounced.addAll(new DerivationAnnouncement(
                                   new DerivationStart(first.logPosition(), first.state(), first.reemitMarker(),
                                                       first.queuePositions(), follower.termLength(), follower.mtu()),
                                   commitCounterId)
                                   .encode());
        }
    }
}
