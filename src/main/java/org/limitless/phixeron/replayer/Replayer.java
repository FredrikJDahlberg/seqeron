package org.limitless.phixeron.replayer;

import io.aeron.logbuffer.FragmentHandler;
import java.util.List;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.status.AtomicCounter;

/**
 * Everything {@link ReplayerService} needs from the node it runs on — the local archive, the two
 * node-local IPC streams it answers co-located apps over, its operator counters, and the clock —
 * behind one seam, so the replay protocol itself can be exercised without an Aeron runtime (see
 * {@code ReplayerServiceTest}).
 *
 * <p>Deliberately <b>one</b> interface rather than one per dependency: there are exactly two
 * implementations, {@link AeronReplayer} and the test fake, and splitting the same eleven
 * calls across three interfaces would only mean writing three fakes instead of one. Everything above
 * this line — which recording to serve, when the tip is unknown, when to hold, when to refuse — stays
 * in {@code ReplayerService} where it can be asserted on; everything below it is a one-line forward to
 * {@code Aeron}/{@code AeronArchive}.
 *
 * <p>Narrower than the calls it wraps: the archive's callback-driven recording listing arrives as a
 * plain {@code List}, and the channel every stream runs over ({@link ReplayerService#IPC_CHANNEL}) is
 * fixed by the implementation rather than passed at each call.
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
     * Opens a fresh subscription to the internal self-check stream. One per check, never pooled: a
     * previous check's abandoned replay must not be able to deliver into the next one.
     * @return the stream, to be {@link SelfCheckStream#close}d when the check ends
     */
    SelfCheckStream openSelfCheckStream();

    /**
     * Allocates one operator counter (see {@code PhixeronCounters}); the member id is the
     * implementation's.
     * @param typeId phixeron counter type id
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
