package org.limitless.seqeron.tools;

import io.aeron.Publication;
import io.aeron.logbuffer.FragmentHandler;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.limitless.seqeron.replayer.server.Replayer;

/**
 * A member's {@link Replayer} over its real archive and streams, made to fail or stall on cue, so that a timer boundary
 * in {@code ReplayerService} is crossed exactly and not by the luck of a script's timing. It outlives a restart of its
 * member, as a member's clock does.
 */
final class FaultableReplayer implements Replayer {
    private volatile Replayer delegate;
    private volatile boolean controlBackPressured;
    private volatile boolean requestsStarved;
    private volatile long startReplayDelayMs;
    private volatile boolean startReplayFails;
    private volatile boolean replayProgressHeld;
    private volatile boolean replaysEnded;
    private final Map<Long, Long> heldProgress = new ConcurrentHashMap<>();

    /** The decoration {@code SequencerServer.launch} takes: this over the member's Replayer. */
    Replayer wrap(final Replayer replayer) {
        delegate = replayer;
        return this;
    }

    /** {@link #offerControl} answers {@code BACK_PRESSURED} without offering, as a control stream nobody reads does. */
    void backPressureControl(final boolean on) {
        controlBackPressured = on;
    }

    /** {@link #pollRequests} reads nothing, so no request is served until it is let go. */
    void starveRequests(final boolean on) {
        requestsStarved = on;
    }

    /** {@link #startReplay} takes {@code millis} longer, on the Replayer's own thread. */
    void slowStartReplay(final long millis) {
        startReplayDelayMs = millis;
    }

    /** {@link #startReplay} throws, as an archive that refuses the replay does. */
    void failStartReplay(final boolean on) {
        startReplayFails = on;
    }

    /** {@link #replayProgress} stays where each replay stood when this began, as a subscriber that reads no more. */
    void holdReplayProgress(final boolean on) {
        replayProgressHeld = on;
        if (!on) {
            heldProgress.clear();
        }
    }

    /** {@link #replayProgress} answers {@link #REPLAY_ENDED}: every replay's publication is gone. */
    void endReplays(final boolean on) {
        replaysEnded = on;
    }

    @Override
    public List<RecordingSpan> listQueueRecordings(final int groupId) {
        return delegate.listQueueRecordings(groupId);
    }

    @Override
    public ScanEnd scanQueueRecording(final long recordingId, final long fromPosition, final long endPosition,
                                      final FrameVisitor visitor) {
        return delegate.scanQueueRecording(recordingId, fromPosition, endPosition, visitor);
    }

    @Override
    public long recordingPosition(final long recordingId) {
        return delegate.recordingPosition(recordingId);
    }

    @Override
    public long stopPosition(final long recordingId) {
        return delegate.stopPosition(recordingId);
    }

    @Override
    public QueueBacklog queueBacklog(final int groupId, final RecordingSpan span) {
        return delegate.queueBacklog(groupId, span);
    }

    @Override
    public boolean purgeSegments(final long recordingId, final long newStartPosition) {
        return delegate.purgeSegments(recordingId, newStartPosition);
    }

    @Override
    public long startReplay(final long recordingId, final long position, final long length, final int streamId) {
        final long delayMs = startReplayDelayMs;
        if (delayMs > 0) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(delayMs));
        }
        if (startReplayFails) {
            throw new IllegalStateException("startReplay refused by FaultableReplayer");
        }
        return delegate.startReplay(recordingId, position, length, streamId);
    }

    @Override
    public void stopReplay(final long replaySessionId) {
        delegate.stopReplay(replaySessionId);
    }

    @Override
    public String pollArchive() {
        return delegate.pollArchive();
    }

    @Override
    public int pollRequests(final FragmentHandler handler, final int fragmentLimit) {
        return requestsStarved ? 0 : delegate.pollRequests(handler, fragmentLimit);
    }

    @Override
    public long offerControl(final DirectBuffer buffer, final int offset, final int length) {
        return controlBackPressured ? Publication.BACK_PRESSURED : delegate.offerControl(buffer, offset, length);
    }

    @Override
    public long replayProgress(final long replaySessionId) {
        if (replaysEnded) {
            return REPLAY_ENDED;
        }
        final long progress = delegate.replayProgress(replaySessionId);
        if (!replayProgressHeld || progress == REPLAY_ENDED) {
            return progress;
        }
        return heldProgress.computeIfAbsent(replaySessionId, id -> progress);
    }

    @Override
    public AtomicCounter newCounter(final int typeId, final String label) {
        return delegate.newCounter(typeId, label);
    }

    @Override
    public long monotonicMs() {
        return delegate.monotonicMs();
    }
}
