package org.limitless.seqeron.replayer.server;

import io.aeron.logbuffer.FragmentHandler;
import java.util.List;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.status.AtomicCounter;

/**
 * Everything {@link ReplayerService} needs from the node it runs on — the local archive, the two
 * node-local IPC streams it answers co-located apps over, its operator counters, and the clock —
 * behind one seam, so the replay protocol itself can be exercised without an Aeron runtime (see
 * {@code ReplayerServiceTest}).
 */
public interface Replayer {
    /**
     * One queue recording as read off an archive listing. {@code startPosition} is carried because a replay
     * must start at or after it. A queue recorded from its first frame starts at 0, one recorded from a snapshot's
     * cut elsewhere (doc/snapshot.md §10.3).
     */
    record RecordingSpan(long recordingId, long startPosition, boolean active, int termBufferLength) { }

    /** Takes one frame of a queue recording in a scan. */
    @FunctionalInterface
    interface FrameVisitor {
        /**
         * @param position      where the frame starts in the recording
         * @param clusterSeqNo  its {@code clusterSeqNo}
         * @return whether to go on to the next frame
         */
        boolean onFrame(long position, long clusterSeqNo);
    }

    /** How {@link #scanQueueRecording} ended. */
    enum ScanEnd {
        /** The visitor asked to stop. */
        STOPPED,
        /** The scan reached the position it was bounded to. */
        TIP,
        /** The recording does not hold a frame where the scan began or went on: not a frame boundary. */
        UNREADABLE
    }

    /**
     * How far a recording trails the queue it records, and the window it may trail by before the queue's publisher
     * blocks on it.
     */
    record QueueBacklog(long bytes, long window) { }

    /**
     * Every recording of one group's queue the local archive holds, in whatever order it listed them; which one is
     * served is {@code ReplayerService}'s decision.
     * @param groupId the queue's group
     * @return the recordings, possibly empty
     */
    List<RecordingSpan> listQueueRecordings(int groupId);

    /**
     * Reads a queue recording's frames in order, from a position to another, or until the visitor stops.
     * @param recordingId archive recording id
     * @param fromPosition where to begin, which must be a frame boundary
     * @param endPosition where to stop, the recording's tip
     * @param visitor takes each frame
     * @return how the scan ended
     */
    ScanEnd scanQueueRecording(long recordingId, long fromPosition, long endPosition, FrameVisitor visitor);

    /**
     * Where a still-recording recording has reached. On every request's path, so read off the node's counters
     * rather than asked of the archive.
     * @param recordingId archive recording id
     * @return the live position, or a negative value if this recording is no longer being written
     */
    long recordingPosition(long recordingId);

    /**
     * Where a stopped recording ended.
     * @param recordingId archive recording id
     * @return the stop position, or a negative value if it has not been written yet
     */
    long stopPosition(long recordingId);

    /**
     * How far an active queue recording trails its queue, read off this node's counters.
     * @param groupId the queue's group
     * @param span the queue's active recording
     * @return its backlog, or {@code null} when the counters show no queue publication it records
     */
    QueueBacklog queueBacklog(int groupId, RecordingSpan span);

    /**
     * Starts a bounded replay onto a node-local IPC stream.
     * @param recordingId archive recording id
     * @param position where to start replaying from
     * @param length how much to replay
     * @param streamId {@code REPLAY_STREAM_ID}
     * @return the archive's replay session id
     */
    long startReplay(long recordingId, long position, long length, int streamId);

    /**
     * Stops a replay. Bounded replays end on their own, so this may fail on a session that is already
     * gone; callers treat that as harmless.
     * @param replaySessionId the session returned by {@link #startReplay}
     */
    void stopReplay(long replaySessionId);

    /** {@link #pollArchive}'s answer once the archive has closed the control session. */
    String ARCHIVE_SESSION_CLOSED = "the control session is closed";

    /**
     * Reads one response off the local archive's control session between requests. The archive pings the
     * session once a second and closes it once those go unread, so this is called every duty cycle.
     * @return the error the archive answered one request with, such as a replay whose client left before it
     *         connected; {@link #ARCHIVE_SESSION_CLOSED} once the session is closed; or {@code null}
     */
    String pollArchive();

    /**
     * Polls the apps → Replayer request stream.
     * @param handler receives each fragment
     * @param fragmentLimit maximum fragments to read
     * @return fragments read
     */
    int pollRequests(FragmentHandler handler, int fragmentLimit);

    /**
     * Offers one encoded control reply to the Replayer → apps stream.
     * @param buffer encoded reply
     * @param offset buffer offset
     * @param length encoded length
     * @return the raw Aeron offer result (see {@code io.aeron.Publication})
     */
    long offerControl(DirectBuffer buffer, int offset, int length);

    /**
     * Whether a replay's publication is still open: until the archive has ended the replay and its subscriber
     * has let it go, or the subscriber's client has died.
     * @param replaySessionId the session returned by {@link #startReplay}
     * @return false once it is gone
     */
    boolean isReplayLive(long replaySessionId);

    /**
     * Allocates one operator counter (see {@code SeqeronCounters}); the member id is the
     * implementation's.
     * @param typeId seqeron counter type id
     * @param label human-readable label
     * @return the counter
     */
    AtomicCounter newCounter(int typeId, String label);

    /**
     * @return wall-clock milliseconds, for archive-retry pacing and error episodes
     */
    long epochMillis();
}
