package org.limitless.seqeron.replayer.server;

import io.aeron.logbuffer.FragmentHandler;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
    /** Counter slots; the service allocates seven. */
    private static final int COUNTER_CAPACITY = 16;

    /** Every recording's term length: the driver's default IPC term, as a member's tap has. */
    static final int TERM_BUFFER_LENGTH = 16 * 1024 * 1024;

    /** One {@code startReplay} the service asked for. */
    record StartedReplay(long recordingId, long position, long length, int streamId, long replaySessionId) { }

    private final List<Replayer.RecordingSpan> recordings = new ArrayList<>();
    private final Map<Long, Long> recordingPositions = new HashMap<>();
    private final Map<Long, Long> stopPositions = new HashMap<>();
    private Replayer.TapBacklog tapBacklog;

    private final List<StartedReplay> startedReplays = new ArrayList<>();
    private final List<Long> stoppedReplays = new ArrayList<>();
    private long nextReplaySessionId = 900;
    private RuntimeException replayFailure;
    private RuntimeException archiveFailure;
    private String archiveSessionError;
    private int archivePolls;
    private int listings;
    private int replayAttempts;

    private final Deque<byte[]> requestQueue = new ArrayDeque<>();
    private final List<byte[]> controlReplies = new ArrayList<>();
    private long controlOfferResult = 1; // > 0 is a successful offer

    private final Set<Long> endedReplays = new HashSet<>();


    // Direct buffers, not byte[]: CountersManager requires 8-byte alignment.
    private final CountersManager countersManager = new CountersManager(
        new UnsafeBuffer(ByteBuffer.allocateDirect(COUNTER_CAPACITY * CountersReader.METADATA_LENGTH)),
        new UnsafeBuffer(ByteBuffer.allocateDirect(COUNTER_CAPACITY* CountersReader.COUNTER_LENGTH)));
    private final Map<Integer, AtomicCounter> countersByTypeId = new HashMap<>();

    private long epochMillis = 1_000_000L;

    // ── Scripting the node ──────────────────────────────────────────────────────

    /** Adds one tap recording. A negative tip means the archive cannot say where it ends. */
    void addRecording(final long recordingId, final long startPosition, final boolean active, final long tip) {
        recordings.add(new Replayer.RecordingSpan(recordingId, startPosition, active, TERM_BUFFER_LENGTH));
        if (active) {
            recordingPositions.put(recordingId, tip);
        } else {
            stopPositions.put(recordingId, tip);
        }
    }

    /** Marks a recording stopped, as an operator repairing an unclean shutdown's leftover does. */
    void stopRecording(final long recordingId) {
        recordings.replaceAll(
            span
            -> span.recordingId() == recordingId
                ? new Replayer.RecordingSpan(recordingId, span.startPosition(), false, span.termBufferLength())
                : span);
        final Long tip = recordingPositions.remove(recordingId);
        if (tip != null) {
            stopPositions.put(recordingId, tip);
        }
    }

    /**
     * Leaves a recording listed as recording with nothing writing it, as an unclean shutdown does: its position
     * counter went with the process, and the archive's restart wrote its stop position.
     */
    void orphanRecording(final long recordingId) {
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

    /**
     * What the counters say the active recording trails the tap by; {@code null}, the default, is no tap
     * publication found.
     */
    void tapBacklog(final Replayer.TapBacklog backlog) {
        tapBacklog = backlog;
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

    /** Makes every subsequent {@code pollArchive} report {@code error}, as a session the archive closed does. */
    void closeArchiveSession(final String error) {
        archiveSessionError = error;
    }

    /** How many times the archive's tap recordings were listed. */
    int listings() {
        return listings;
    }

    int archivePolls() {
        return archivePolls;
    }

    /** Queues one encoded app → Replayer message for the next {@code pollRequests}. */
    void enqueueRequest(final byte[] message) {
        requestQueue.add(message);
    }

    /** Ends a replay, as the archive reaching its bound and its client letting it go does. */
    void endReplay(final long replaySessionId) {
        endedReplays.add(replaySessionId);
    }

    /** What every subsequent control offer returns; a negative value is an Aeron offer failure. */
    void controlOfferResult(final long result) {
        controlOfferResult = result;
    }

    void advanceMillis(final long millis) {
        epochMillis += millis;
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

    long counter(final int typeId) {
        return countersByTypeId.get(typeId).get();
    }

    // ── Replayer ─────────────────────────────────────────────────────

    @Override
    public List<Replayer.RecordingSpan> listTapRecordings() {
        ++listings;
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
    public Replayer.TapBacklog tapBacklog(final Replayer.RecordingSpan span) {
        return tapBacklog;
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
    public String pollArchive() {
        ++archivePolls;
        return archiveSessionError;
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
    public boolean isReplayLive(final long replaySessionId) {
        return !endedReplays.contains(replaySessionId);
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
}
