package org.limitless.phixeron.replayer.server;

import io.aeron.logbuffer.FragmentHandler;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersManager;
import org.agrona.concurrent.status.CountersReader;

/**
 * An in-memory node for {@link ReplayerService} to serve: a scripted archive, the two app-facing IPC
 * streams as plain queues, real Agrona counters, and a clock the test moves by hand. No media driver,
 * no archive, no cluster — {@code ReplayerServiceTest} runs in milliseconds like the rest of the Java
 * suite.
 *
 * <p>Aeron types appear only where they are pure interfaces or constants ({@link FragmentHandler},
 * the {@code Publication} offer results); nothing here starts or connects to anything. Fragments are
 * delivered with a null {@code Header}, which both of the service's handlers ignore.
 */
final class FakeReplayer implements Replayer {
    /** Counter slots; the service allocates nine. */
    private static final int COUNTER_CAPACITY = 16;

    /** One {@code startReplay} the service asked for. */
    record StartedReplay(long recordingId, long position, long length, int streamId, long replaySessionId) { }

    private final List<ReplayRecordings.RecordingSpan> recordings = new ArrayList<>();
    private final Map<Long, Long> recordingPositions = new HashMap<>();
    private final Map<Long, Long> stopPositions = new HashMap<>();

    private final List<StartedReplay> startedReplays = new ArrayList<>();
    private final List<Long> stoppedReplays = new ArrayList<>();
    private long nextReplaySessionId = 900;
    private RuntimeException replayFailure;
    private RuntimeException archiveFailure;
    private int replayAttempts;

    private final Deque<byte[]> requestQueue = new ArrayDeque<>();
    private final List<byte[]> controlReplies = new ArrayList<>();
    private long controlOfferResult = 1; // > 0 is a successful offer

    private final Deque<byte[]> selfCheckFrames = new ArrayDeque<>();
    private int selfCheckStreamsOpened;

    // Direct buffers, not byte[]: CountersManager requires 8-byte alignment.
    private final CountersManager countersManager = new CountersManager(
        new UnsafeBuffer(ByteBuffer.allocateDirect(COUNTER_CAPACITY * CountersReader.METADATA_LENGTH)),
        new UnsafeBuffer(ByteBuffer.allocateDirect(COUNTER_CAPACITY* CountersReader.COUNTER_LENGTH)));
    private final Map<Integer, AtomicCounter> countersByTypeId = new HashMap<>();

    private long epochMillis = 1_000_000L;
    private long nanoTime = 5_000_000_000L;

    // ── Scripting the node ──────────────────────────────────────────────────────

    /** Adds one tap recording. A negative tip means the archive cannot say where it ends. */
    void addRecording(final long recordingId, final long startPosition, final boolean active, final long tip) {
        recordings.add(new ReplayRecordings.RecordingSpan(recordingId, startPosition, active));
        if (active) {
            recordingPositions.put(recordingId, tip);
        } else {
            stopPositions.put(recordingId, tip);
        }
    }

    /** Marks a recording stopped, as an operator repairing an unclean shutdown's leftover does. */
    void stopRecording(final long recordingId) {
        recordings.replaceAll(span
                              -> span.recordingId() == recordingId
                                  ? new ReplayRecordings.RecordingSpan(recordingId, span.startPosition(), false)
                                  : span);
        final Long tip = recordingPositions.remove(recordingId);
        if (tip != null) {
            stopPositions.put(recordingId, tip);
        }
    }

    /** Leaves neither position counter able to report {@code recordingId}'s tip, as a rotation transiently does. */
    void hideTip(final long recordingId) {
        recordingPositions.remove(recordingId);
        stopPositions.remove(recordingId);
    }

    /** Makes every subsequent {@code startReplay} throw, as an archive that will not serve a replay does. */
    void failReplays(final RuntimeException failure) {
        replayFailure = failure;
    }

    void serveReplaysAgain() {
        replayFailure = null;
    }

    /**
     * Makes every subsequent archive call throw, listings included — an archive that has stopped
     * answering at all, as opposed to one that answers and refuses a particular replay ({@link
     * #failReplays}). What separates a real outage from a bad request position.
     */
    void failArchive(final RuntimeException failure) {
        archiveFailure = failure;
    }

    void answerArchiveAgain() {
        archiveFailure = null;
    }

    /** Queues one encoded app → Replayer message for the next {@code pollRequests}. */
    void enqueueRequest(final byte[] message) {
        requestQueue.add(message);
    }

    /** Queues one fragment for the in-flight self-check replay to read back. */
    void enqueueSelfCheckFrame(final byte[] frame) {
        selfCheckFrames.add(frame);
    }

    /** What every subsequent control offer returns; a negative value is an Aeron offer failure. */
    void controlOfferResult(final long result) {
        controlOfferResult = result;
    }

    void advanceMillis(final long millis) {
        epochMillis += millis;
        nanoTime += millis * 1_000_000L;
    }

    // ── Reading back what the service did ───────────────────────────────────────

    List<StartedReplay> startedReplays() {
        return startedReplays;
    }

    List<Long> stoppedReplays() {
        return stoppedReplays;
    }

    /** Every {@code startReplay} the service tried, including the ones a failing archive refused. */
    int replayAttempts() {
        return replayAttempts;
    }

    List<byte[]> controlReplies() {
        return controlReplies;
    }

    int selfCheckStreamsOpened() {
        return selfCheckStreamsOpened;
    }

    long counter(final int typeId) {
        return countersByTypeId.get(typeId).get();
    }

    // ── Replayer ─────────────────────────────────────────────────────

    @Override
    public List<ReplayRecordings.RecordingSpan> listTapRecordings() {
        throwIfArchiveDown();
        return new ArrayList<>(recordings);
    }

    @Override
    public long recordingPosition(final long recordingId) {
        throwIfArchiveDown();
        return recordingPositions.getOrDefault(recordingId, -1L);
    }

    @Override
    public long stopPosition(final long recordingId) {
        throwIfArchiveDown();
        return stopPositions.getOrDefault(recordingId, -1L);
    }

    @Override
    public long startReplay(final long recordingId, final long position, final long length, final int streamId) {
        ++replayAttempts; // counted before the failure, so a refused probe is still visible as a probe
        throwIfArchiveDown();
        if (replayFailure != null) {
            throw replayFailure;
        }
        final long replaySessionId = ++nextReplaySessionId;
        startedReplays.add(new StartedReplay(recordingId, position, length, streamId, replaySessionId));
        return replaySessionId;
    }

    @Override
    public void stopReplay(final long replaySessionId) {
        stoppedReplays.add(replaySessionId);
    }

    @Override
    public int pollRequests(final FragmentHandler handler, final int fragmentLimit) {
        int read = 0;
        while (read < fragmentLimit && !requestQueue.isEmpty()) {
            final byte[] message = requestQueue.poll();
            handler.onFragment(new UnsafeBuffer(message), 0, message.length, null);
            ++read;
        }
        return read;
    }

    @Override
    public long offerControl(final DirectBuffer buffer, final int offset, final int length) {
        if (controlOfferResult < 0) {
            return controlOfferResult;
        }
        final byte[] reply = new byte[length];
        buffer.getBytes(offset, reply);
        controlReplies.add(reply);
        return controlOfferResult;
    }

    @Override
    public SelfCheckStream openSelfCheckStream() {
        ++selfCheckStreamsOpened;
        return new SelfCheckStream() {
            @Override
            public int poll(final FragmentHandler handler, final int fragmentLimit) {
                int read = 0;
                while (read < fragmentLimit && !selfCheckFrames.isEmpty()) {
                    final byte[] frame = selfCheckFrames.poll();
                    handler.onFragment(new UnsafeBuffer(frame), 0, frame.length, null);
                    ++read;
                }
                return read;
            }

            @Override
            public void close() {
                // A fresh subscription per check means an abandoned one's data is gone with it.
                selfCheckFrames.clear();
            }
        };
    }

    @Override
    public AtomicCounter newCounter(final int typeId, final String label) {
        final AtomicCounter counter = countersManager.newCounter(label, typeId);
        countersByTypeId.put(typeId, counter);
        return counter;
    }

    private void throwIfArchiveDown() {
        if (archiveFailure != null) {
            throw archiveFailure;
        }
    }

    @Override
    public long epochMillis() {
        return epochMillis;
    }

    @Override
    public long nanoTime() {
        return nanoTime;
    }
}
