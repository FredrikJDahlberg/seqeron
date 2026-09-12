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
     * Every tap recording the local archive holds, in whatever order it listed them — ordering and
     * active-span selection are {@code ReplayerService}'s decisions ({@link ReplayRecordings#stitch}).
     * @return the recordings, possibly empty
     */
    List<ReplayRecordings.RecordingSpan> listTapRecordings();

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
     * Starts a bounded replay onto a node-local IPC stream.
     * @param recordingId archive recording id
     * @param position where to start replaying from
     * @param length how much to replay
     * @param streamId {@link ReplayerService#REPLAY_STREAM_ID} or {@code SELF_CHECK_STREAM_ID}
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
     * Opens a fresh subscription to the internal self-check stream, filtered to one replay session.
     * Called after {@link #startReplay} so the session id is known: a fresh subscription alone does not
     * isolate a check, because an earlier check's replay publication lingers on the same stream and a new
     * subscription joins its image mid-replay — delivering that replay's second frame as if it were the
     * next span's first.
     * @param replaySessionId the session returned by {@link #startReplay}
     * @return the stream, to be {@link SelfCheckStream#close}d when the check ends
     */
    SelfCheckStream openSelfCheckStream(long replaySessionId);

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
     * @return monotonic nanoseconds, for the self-check deadline
     */
    long nanoTime();

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
