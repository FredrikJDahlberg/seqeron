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
     * must start at or after it, though it is always 0 today.
     */
    record RecordingSpan(long recordingId, long startPosition, boolean active, int termBufferLength) { }

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

    /**
     * Reads one response off the local archive's control session between requests. The archive pings the
     * session once a second and closes it once those go unread, so this is called every duty cycle.
     * @return the archive's error, or {@code null}; once the session is closed, why
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
