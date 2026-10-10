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
     * One tap recording as read off an archive listing. {@code startPosition} is carried because a replay
     * must start at or after it, though it is always 0 today.
     */
    record RecordingSpan(long recordingId, long startPosition, boolean active, int termBufferLength) { }

    /**
     * How far a recording trails the tap it records, and the window it may trail by before the tap's publisher
     * blocks on it.
     */
    record TapBacklog(long bytes, long window) { }

    /**
     * Every tap recording the local archive holds, in whatever order it listed them; which one is served is
     * {@code ReplayerService}'s decision.
     * @return the recordings, possibly empty
     */
    List<RecordingSpan> listTapRecordings();

    /**
     * Where a still-recording recording has reached.
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
     * How far the active tap recording trails the tap, read off this node's counters.
     * @param span the active tap recording
     * @return its backlog, or {@code null} when the counters show no tap publication it records
     */
    TapBacklog tapBacklog(RecordingSpan span);

    /**
     * Starts a bounded replay onto a node-local IPC stream.
     * @param recordingId archive recording id
     * @param position where to start replaying from
     * @param length how much to replay
     * @param streamId {@code REPLAY_STREAM_ID} or {@code SELF_CHECK_STREAM_ID}
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
     * Opens a subscription to the self-check stream, filtered to one replay session: an earlier check's
     * replay may linger on the same stream and would otherwise read as the next span's first frame.
     * @param replaySessionId the session returned by {@link #startReplay}
     * @return the stream, to be {@link SelfCheckStream#close}d when the check ends
     */
    SelfCheckStream openSelfCheckStream(long replaySessionId);

    /**
     * Opens a replay of a recording that follows it live, onto an internal stream only the snapshot index
     * reads.
     * @param recordingId the active tap recording
     * @param position    where to start, its start position
     * @return the stream, to be {@link IndexStream#close}d when the index stops
     */
    IndexStream openIndexStream(long recordingId, long position);

    /**
     * Allocates one operator counter for one source, keyed on {@code {memberId, sourceId}}.
     * @param typeId   seqeron counter type id
     * @param label    human-readable label
     * @param sourceId the source it counts
     * @return the counter
     */
    AtomicCounter newSourceCounter(int typeId, String label, int sourceId);

    /**
     * Allocates one operator counter (see {@code SeqeronCounters}); the member id is the
     * implementation's.
     * @param typeId seqeron counter type id
     * @param label human-readable label
     * @return the counter
     */
    AtomicCounter newCounter(int typeId, String label);

    /**
     * @return wall-clock milliseconds, for slot ageing and stall-retry pacing
     */
    long epochMillis();

    /**
     * @return monotonic milliseconds, for the self-check deadline
     */
    long nowMs();

    /** The snapshot index's replay of the active recording. */
    interface IndexStream {
        /**
         * @param handler receives each frame with its recording positions
         * @param fragmentLimit maximum fragments to read
         * @return fragments read
         */
        int poll(IndexFrameHandler handler, int fragmentLimit);

        /** Whether the replay has ended: the recording stopped, or the archive dropped the replay. */
        boolean isEnded();

        void close();
    }

    /** One frame of the index's replay. */
    @FunctionalInterface
    interface IndexFrameHandler {
        /**
         * @param buffer        holding the frame
         * @param offset        of its first byte
         * @param length        its length
         * @param position the recording position of its first byte
         */
        void onFrame(DirectBuffer buffer, int offset, int length, long position);
    }

    /** The internal stream a startup self-check replay is read back over. */
    interface SelfCheckStream {
        /**
         * @param handler receives each fragment
         * @param fragmentLimit maximum fragments to read
         * @return fragments read
         */
        int poll(FragmentHandler handler, int fragmentLimit);

        void close();
    }
}
