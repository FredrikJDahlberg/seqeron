package org.limitless.seqeron.replayer.server;

import io.aeron.Aeron;
import io.aeron.ChannelUri;
import io.aeron.ControlledFragmentAssembler;
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ConsensusModule;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import java.util.ArrayDeque;
import java.util.List;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersReader;
import org.limitless.seqeron.protocol.FrameLayer;
import org.limitless.seqeron.protocol.ReplayProtocol;
import org.limitless.seqeron.sequencer.LogDeriver;
import org.limitless.seqeron.sequencer.QueuePositions;
import org.limitless.seqeron.sequencer.Sequencer;
import org.limitless.seqeron.util.Logger;

/**
 * A member's {@link Replayer} that serves a replay from a queue's first frame off the Raft log's recording instead of
 * the queue's: a {@link LogDeriver} over the log, published at the queue's term length and MTU, puts each frame at the
 * position the queue's recording holds it at (doc/disk-load-plan.md, step 2a). Prototype: every other call, and a
 * replay it cannot derive, goes to the Replayer it decorates. It cannot derive a replay from past position 0, on a
 * member whose archive holds a cluster snapshot, or with the log's start trimmed.
 *
 * <p>Not thread-safe: every call is from the Replayer's duty-cycle thread.
 */
public final class DerivingReplayer implements Replayer {
    /** The stream the archive replays the log on to this, beside the replay protocol's. */
    public static final int LOG_REPLAY_STREAM_ID = 204;

    private static final String LOG_REPLAY_CHANNEL = "aeron:ipc?alias=seqeron-log-derive";
    private static final String DERIVED_CHANNEL = "aeron:ipc?alias=seqeron-replay-derived";
    /** How long a derived replay waits for its subscriber, as the archive's replay does. */
    private static final long CONNECT_TIMEOUT_MS = 5_000;
    private static final int FRAGMENT_LIMIT = 64;

    private final Replayer delegate;
    private final Aeron aeron;
    private final AeronArchive archive;
    private final int memberId;
    private final Long2ObjectHashMap<DerivedReplay> replays = new Long2ObjectHashMap<>();

    /** The queue a recording is of. */
    private record Queue(int groupId, int termLength, int mtu) { }

    public DerivingReplayer(final Replayer delegate, final Aeron aeron, final AeronArchive archive,
                            final int memberId) {
        this.delegate = delegate;
        this.aeron = aeron;
        this.archive = archive;
        this.memberId = memberId;
    }

    @Override
    public long startReplay(final long recordingId, final long position, final long length, final int streamId) {
        final long logRecordingId = position == 0 ? wholeLogRecording() : Aeron.NULL_VALUE;
        final int commitCounterId = commitPositionCounterId();
        if (logRecordingId == Aeron.NULL_VALUE || commitCounterId == CountersReader.NULL_COUNTER_ID) {
            return delegate.startReplay(recordingId, position, length, streamId);
        }
        final Queue queue = queueOf(recordingId);
        final long logSessionId = archive.startBoundedReplay(logRecordingId, 0, AeronArchive.NULL_LENGTH,
                                                             commitCounterId, LOG_REPLAY_CHANNEL, LOG_REPLAY_STREAM_ID);
        final Subscription log =
            aeron.addSubscription(ChannelUri.addSessionId(LOG_REPLAY_CHANNEL, (int)logSessionId), LOG_REPLAY_STREAM_ID);
        final ExclusivePublication publication = aeron.addExclusivePublication(
            DERIVED_CHANNEL + "|term-length=" + queue.termLength() + "|mtu=" + queue.mtu(), streamId);
        // What the client subscribes by, as an archive's replay session id is: its low 32 bits.
        final long replaySessionId = publication.sessionId() & 0xFFFF_FFFFL;
        replays.put(replaySessionId, new DerivedReplay(queue, length, logSessionId, log, publication));
        Logger.info(Logger.CoreComponent.ReplayerService, memberId,
                    "replay session %d derives group %d's queue [0,%d) from log recording %d", replaySessionId,
                    queue.groupId(), length, logRecordingId);
        return replaySessionId;
    }

    /** The log's active recording when it starts at 0 and no cluster snapshot could have been restored from. */
    private long wholeLogRecording() {
        final MutableInteger snapshots = new MutableInteger();
        archive.listRecordingsForUri(0, 1, "", ConsensusModule.Configuration.snapshotStreamId(),
                                     (controlSessionId, correlationId, recordingId, startTimestamp, stopTimestamp,
                                      startPosition, stopPosition, initialTermId, segmentFileLength, termBufferLength,
                                      mtuLength, sessionId, streamId, strippedChannel, originalChannel,
                                      sourceIdentity) -> snapshots.increment());
        if (snapshots.get() > 0) {
            return Aeron.NULL_VALUE;
        }
        final long[] found = { Aeron.NULL_VALUE };
        archive.listRecordingsForUri(0, Integer.MAX_VALUE, "", ConsensusModule.Configuration.logStreamId(),
                                     (controlSessionId, correlationId, recordingId, startTimestamp, stopTimestamp,
                                      startPosition, stopPosition, initialTermId, segmentFileLength, termBufferLength,
                                      mtuLength, sessionId, streamId, strippedChannel, originalChannel,
                                      sourceIdentity) -> {
                                         if (stopTimestamp == AeronArchive.NULL_TIMESTAMP && startPosition == 0) {
                                             found[0] = recordingId;
                                         }
                                     });
        return found[0];
    }

    private int commitPositionCounterId() {
        final MutableInteger id = new MutableInteger(CountersReader.NULL_COUNTER_ID);
        aeron.countersReader().forEach((counterId, typeId, keyBuffer, label) -> {
            if (typeId == ConsensusModule.Configuration.COMMIT_POSITION_TYPE_ID) {
                id.set(counterId);
            }
        });
        return id.get();
    }

    private Queue queueOf(final long queueRecordingId) {
        final Queue[] queue = { null };
        archive.listRecording(
            queueRecordingId,
            (controlSessionId, correlationId, recordingId, startTimestamp, stopTimestamp, startPosition, stopPosition,
             initialTermId, segmentFileLength, termBufferLength, mtuLength, sessionId, streamId, strippedChannel,
             originalChannel, sourceIdentity)
                -> queue[0] = new Queue(streamId - FrameLayer.queueStreamId(0), termBufferLength, mtuLength));
        if (queue[0] == null) {
            throw new IllegalStateException("the archive holds no recording " + queueRecordingId);
        }
        return queue[0];
    }

    @Override
    public int doWork() {
        int work = delegate.doWork();
        for (final DerivedReplay replay : replays.values()) {
            work += replay.doWork();
        }
        return work;
    }

    @Override
    public void stopReplay(final long replaySessionId) {
        final DerivedReplay replay = replays.remove(replaySessionId);
        if (replay == null) {
            delegate.stopReplay(replaySessionId);
        } else {
            replay.close();
        }
    }

    @Override
    public long replayProgress(final long replaySessionId) {
        final long progress = delegate.replayProgress(replaySessionId);
        if (progress == REPLAY_ENDED) {
            final DerivedReplay replay = replays.remove(replaySessionId);
            if (replay != null) {
                replay.close();
            }
        }
        return progress;
    }

    /** One replay: the archive's replay of the log in, the requested queue's frames out. */
    private final class DerivedReplay implements Sequencer.FrameSink {
        private final Queue queue;
        private final long endPosition;
        /** Where the queue stands after the last frame derived, offered or pending. */
        private long derivedPosition;
        private final long logSessionId;
        private final Subscription log;
        private final ExclusivePublication publication;
        private final LogDeriver deriver = new LogDeriver(memberId, this);
        /** The frames of one log entry the publication has not taken yet. */
        private final ArrayDeque<byte[]> pending = new ArrayDeque<>();
        private final UnsafeBuffer pendingBuffer = new UnsafeBuffer();
        private final ControlledFragmentHandler handler = new ControlledFragmentAssembler(this::onLogEntry);
        private final long connectDeadlineMs = delegate.monotonicMs() + CONNECT_TIMEOUT_MS;
        private boolean connected;
        private boolean closed;

        DerivedReplay(final Queue queue, final long endPosition, final long logSessionId, final Subscription log,
                      final ExclusivePublication publication) {
            this.queue = queue;
            this.endPosition = endPosition;
            this.logSessionId = logSessionId;
            this.log = log;
            this.publication = publication;
        }

        int doWork() {
            if (closed) {
                return 0;
            }
            if (!publication.isConnected()) {
                if (connected || delegate.monotonicMs() > connectDeadlineMs) {
                    close(); // its subscriber left, or never came
                }
                return 0;
            }
            connected = true;
            int work = 0;
            while (!pending.isEmpty()) {
                final byte[] frame = pending.peek();
                pendingBuffer.wrap(frame);
                if (!offer(pendingBuffer, frame.length)) {
                    return work;
                }
                pending.poll();
                work++;
            }
            if (publication.position() >= endPosition) {
                if (publication.position() > endPosition) {
                    Logger.info(Logger.CoreComponent.ReplayerService, memberId,
                                "group %d's derived replay ended at %d, past the queue's %d", queue.groupId(),
                                publication.position(), endPosition);
                }
                close();
                return work + 1;
            }
            final Image image = log.imageBySessionId((int)logSessionId);
            return image == null ? work : work + image.controlledPoll(handler, FRAGMENT_LIMIT);
        }

        private ControlledFragmentHandler.Action onLogEntry(final DirectBuffer buffer, final int offset,
                                                            final int length, final Header header) {
            deriver.onLogEntry(buffer, offset, length);
            return pending.isEmpty() && derivedPosition < endPosition ? ControlledFragmentHandler.Action.CONTINUE
                                                                      : ControlledFragmentHandler.Action.BREAK;
        }

        @Override
        public void activate(final int[] groupIds) {
        }

        @Override
        public void publish(final int group, final DirectBuffer buffer, final int length) {
            if (group != queue.groupId() || closed || derivedPosition >= endPosition) {
                return;
            }
            derivedPosition = QueuePositions.frameStart(derivedPosition, length, queue.termLength(), queue.mtu()) +
                QueuePositions.framedLength(length, queue.mtu());
            if (!pending.isEmpty() || !offer(buffer, length)) {
                final byte[] frame = new byte[length];
                buffer.getBytes(0, frame);
                pending.add(frame);
            }
        }

        /** @return whether the publication took the frame; a publication that is gone closes the replay */
        private boolean offer(final DirectBuffer buffer, final int length) {
            long result;
            while ((result = publication.offer(buffer, 0, length)) == Publication.ADMIN_ACTION) {
                Thread.onSpinWait(); // the term rotated
            }
            if (result == Publication.NOT_CONNECTED || result == Publication.CLOSED ||
                result == Publication.MAX_POSITION_EXCEEDED) {
                close();
            }
            return result > 0;
        }

        void close() {
            if (closed) {
                return;
            }
            closed = true;
            pending.clear();
            CloseHelper.quietCloseAll(publication, log);
            try {
                archive.stopReplay(logSessionId);
            } catch (final RuntimeException ex) {
                // the archive has ended it already
            }
        }
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
    public String pollArchive() {
        return delegate.pollArchive();
    }

    @Override
    public int pollRequests(final FragmentHandler handler, final int fragmentLimit) {
        return delegate.pollRequests(handler, fragmentLimit);
    }

    @Override
    public long offerControl(final DirectBuffer buffer, final int offset, final int length) {
        return delegate.offerControl(buffer, offset, length);
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
