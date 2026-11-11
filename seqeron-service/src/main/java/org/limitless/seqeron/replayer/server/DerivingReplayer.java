package org.limitless.seqeron.replayer.server;

import io.aeron.Aeron;
import io.aeron.ChannelUri;
import io.aeron.ControlledFragmentAssembler;
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.cluster.ConsensusModule;
import io.aeron.logbuffer.BufferClaim;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.logbuffer.Header;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersReader;
import org.limitless.seqeron.protocol.FrameLayer;
import org.limitless.seqeron.protocol.SequencedFrameDecoder;
import org.limitless.seqeron.sequencer.LogDeriver;
import org.limitless.seqeron.sequencer.QueuePositions;
import org.limitless.seqeron.sequencer.Sequencer;
import org.limitless.seqeron.util.Logger;

/**
 * A member's {@link Replayer} whose queues are not recorded: a queue's history is a {@link LogDeriver} over the Raft
 * log's recording, published at the queue's term length and MTU, which puts each frame at the position the queue's
 * publication put it at (doc/disk-load-plan.md, step 2a).
 *
 * <p>To {@link ReplayerService} each queue is one active recording whose id is its group's, which starts where the
 * queue did and has reached where the derivation has brought it.
 *
 * <p>One derivation follows the log for as long as this runs and keeps a checkpoint of it every {@link
 * #CHECKPOINT_LOG_BYTES}: the deriver's state, and where each queue stood. A replay or a scan continues from the last
 * checkpoint at or before the position it starts at. The derivation starts where the member's service says it did
 * ({@link DerivationStart}): at the log's first entry, or at the cluster snapshot it restored, each queue at the
 * position the service created it at.
 *
 * <p>No queue is listed until the derivation has caught up with the log's commit position as it stood when it
 * started. The checkpoints are held in memory, rebuilt at each start, and released below a purge. Past {@link
 * #MAX_CHECKPOINTS} every other one is released and the log between two doubles, so a log nothing purges costs a
 * bounded number of them and a longer derivation before a replay's first frame. The replay protocol's streams, the
 * counters and the clock are the Replayer's it decorates.
 *
 * <p>Not thread-safe: every call is from the Replayer's duty-cycle thread.
 */
public final class DerivingReplayer implements Replayer {
    /** The stream the archive replays the log, and a cluster snapshot, on to this, beside the replay protocol's. */
    private static final int LOG_REPLAY_STREAM_ID = 204;

    /** How much log lies between two checkpoints at first: the most a replay derives before its first frame. */
    public static final long CHECKPOINT_LOG_BYTES = 8L * 1024 * 1024;
    /** The most checkpoints kept: 8 GiB of log at {@link #CHECKPOINT_LOG_BYTES}, before the log between two doubles. */
    public static final int MAX_CHECKPOINTS = 1024;

    private static final String LOG_REPLAY_CHANNEL = "aeron:ipc?alias=seqeron-log-derive";
    /** The alias of a derived replay's publication, which an archive's replay does not carry. */
    public static final String DERIVED_ALIAS = "seqeron-replay-derived";

    private static final String DERIVED_CHANNEL = "aeron:ipc?alias=" + DERIVED_ALIAS;
    /** How long a derived replay waits for its subscriber, as the archive's replay does. */
    private static final long CONNECT_TIMEOUT_MS = 5_000;
    /** How long a derived scan may go without a log entry, as the archive's replay of a recorded one. */
    private static final long SCAN_STALL_TIMEOUT_MS = 2_000;
    private static final long FOLLOW_RETRY_MS = 1_000;
    private static final int FRAGMENT_LIMIT = 64;
    private static final int FOLLOW_FRAGMENT_LIMIT = 256;

    private final Replayer delegate;
    private final Aeron aeron;
    private final AeronArchive archive;
    private final int memberId;
    private final Supplier<DerivationStart> derivationStart;
    private final long checkpointLogBytes;
    private final int maxCheckpoints;
    private final Long2ObjectHashMap<DerivedReplay> replays = new Long2ObjectHashMap<>();
    /** The derivation that keeps the checkpoints, or null until the service has started and the log is recorded. */
    private Follower follower;
    /** Whether a start was found for the follower, or none can be. */
    private boolean followerDecided;
    /** Where a gateway host reads this member's {@link DerivationAnnouncement}, and what is still to be published. */
    private ExclusivePublication announcement;
    private final ArrayDeque<byte[]> unannounced = new ArrayDeque<>();
    /** The checkpoint announced last: the first kept, which a gateway host may start from. */
    private Checkpoint announced;
    private long nextFollowAttemptMs;

    /** A queue as it is published: every queue of a node has its term length and MTU. */
    private record Queue(int groupId, int termLength, int mtu) { }

    /**
     * Where a derivation continues from.
     *
     * @param logPosition    of the log entry to apply next
     * @param state          the deriver's before that entry
     * @param reemitMarker   whether the state is a cluster snapshot's, whose marker starts each queue
     * @param queuePositions where each queue stood, slot {@code groupId - 1}
     */
    private record Checkpoint(long logPosition, LogDeriver.State state, boolean reemitMarker, long[] queuePositions) { }

    /**
     * @param derivationStart where the member's service started, or null until it has
     */
    public DerivingReplayer(final Replayer delegate, final Aeron aeron, final AeronArchive archive, final int memberId,
                            final Supplier<DerivationStart> derivationStart) {
        this(delegate, aeron, archive, memberId, derivationStart, CHECKPOINT_LOG_BYTES, MAX_CHECKPOINTS);
    }

    /**
     * As above, for a test that cannot write gigabytes of log.
     *
     * @param checkpointLogBytes the log between two checkpoints at first
     * @param maxCheckpoints     the most kept
     */
    public DerivingReplayer(final Replayer delegate, final Aeron aeron, final AeronArchive archive, final int memberId,
                            final Supplier<DerivationStart> derivationStart, final long checkpointLogBytes,
                            final int maxCheckpoints) {
        this.checkpointLogBytes = checkpointLogBytes;
        this.maxCheckpoints = maxCheckpoints;
        this.derivationStart = derivationStart;
        this.delegate = delegate;
        this.aeron = aeron;
        this.archive = archive;
        this.memberId = memberId;
    }

    /** The checkpoints kept, the derivation's start among them; 0 until the queues are listed. */
    public int checkpointCount() {
        return follower != null && follower.isValid() ? follower.checkpoints.size() : 0;
    }

    @Override
    public long startReplay(final long recordingId, final long position, final long length, final int streamId) {
        final Queue queue = queueOf(recordingId);
        final Checkpoint from = checkpointAt(queue, position);
        final long logSessionId = replayLog(follower.logRecordingId, from.logPosition(), follower.commitCounterId);
        final Subscription log = logSubscription(logSessionId);
        final int termId = (int)(position >> Integer.numberOfTrailingZeros(queue.termLength()));
        final ExclusivePublication publication = aeron.addExclusivePublication(
            DERIVED_CHANNEL + "|term-length=" + queue.termLength() + "|mtu=" + queue.mtu() +
                "|init-term-id=0|term-id=" + termId + "|term-offset=" + (position & (queue.termLength() - 1)),
            streamId);
        // What the client subscribes by, as an archive's replay session id is: its low 32 bits.
        final long replaySessionId = publication.sessionId() & 0xFFFF_FFFFL;
        replays.put(replaySessionId,
                    new DerivedReplay(queue, from, position, position + length, logSessionId, log, publication));
        Logger.info(
            Logger.CoreComponent.ReplayerService, memberId,
            "replay session %d derives group %d's queue [%d,%d) from the log at %d, where the queue stood at %d",
            replaySessionId, queue.groupId(), position, position + length, from.logPosition(),
            from.queuePositions()[queue.groupId() - 1]);
        return replaySessionId;
    }

    /** The queue a recording id names, once the queues are listed. */
    private Queue queueOf(final long recordingId) {
        if (follower == null || !follower.isValid() || recordingId < 1 || recordingId > FrameLayer.MAX_GROUPS) {
            throw new IllegalArgumentException("no queue is derived as recording " + recordingId);
        }
        return new Queue((int)recordingId, follower.termLength, follower.mtu);
    }

    /** The last checkpoint at which a queue stood at or before {@code position}, which its start is the first of. */
    private Checkpoint checkpointAt(final Queue queue, final long position) {
        final Checkpoint from = follower.checkpointAt(queue.groupId(), position);
        if (from == null) {
            throw new IllegalArgumentException("group " + queue.groupId() + "'s queue is not derived from " + position +
                                               ", before " + follower.queueStarts[queue.groupId() - 1]);
        }
        return from;
    }

    private long replayLog(final long logRecordingId, final long logPosition, final int commitCounterId) {
        return archive.startBoundedReplay(logRecordingId, logPosition, AeronArchive.NULL_LENGTH, commitCounterId,
                                          LOG_REPLAY_CHANNEL, LOG_REPLAY_STREAM_ID);
    }

    private Subscription logSubscription(final long sessionId) {
        return aeron.addSubscription(ChannelUri.addSessionId(LOG_REPLAY_CHANNEL, (int)sessionId), LOG_REPLAY_STREAM_ID);
    }

    /** Starts the follower once the service has handed over its start and the log is being recorded. */
    private void startFollower() {
        final DerivationStart start = derivationStart.get();
        final long[] log = start == null ? null : activeLogRecording();
        final int commitCounterId = commitPositionCounterId();
        if (log == null || commitCounterId == CountersReader.NULL_COUNTER_ID) {
            return;
        }
        followerDecided = true;
        if (start.logPosition() < log[1]) {
            Logger.info(Logger.CoreComponent.ReplayerService, memberId,
                        "replays are not derived: the log's recording starts at %d, after the service's start at %d",
                        log[1], start.logPosition());
            return;
        }
        try {
            follower = new Follower(start, log[0], commitCounterId);
            recordAnnouncements();
        } catch (final RuntimeException ex) {
            Logger.info(Logger.CoreComponent.ReplayerService, memberId, "replays are not derived: %s", ex.toString());
        }
    }

    /** Records this run's announcements, and purges the ones earlier runs left. */
    private void recordAnnouncements() {
        final List<Long> earlier = new ArrayList<>();
        archive.listRecordingsForUri(0, Integer.MAX_VALUE, "", DerivationAnnouncement.STREAM_ID,
                                     (controlSessionId, correlationId, recordingId, startTimestamp, stopTimestamp,
                                      startPosition, stopPosition, initialTermId, segmentFileLength, termBufferLength,
                                      mtuLength, sessionId, streamId, strippedChannel, originalChannel,
                                      sourceIdentity) -> {
                                         if (stopTimestamp != AeronArchive.NULL_TIMESTAMP) {
                                             earlier.add(recordingId);
                                         }
                                     });
        for (final long recordingId : earlier) {
            archive.purgeRecording(recordingId);
        }
        archive.startRecording(DerivationAnnouncement.CHANNEL, DerivationAnnouncement.STREAM_ID, SourceLocation.LOCAL,
                               true);
        announcement = aeron.addExclusivePublication(DerivationAnnouncement.CHANNEL, DerivationAnnouncement.STREAM_ID);
    }

    /** Announces the first checkpoint kept once a purge has released the one announced before it. */
    private void announceFirstCheckpoint() {
        final Checkpoint first = follower.checkpoints.getFirst();
        if (first != announced && unannounced.isEmpty()) {
            announced = first;
            unannounced.addAll(new DerivationAnnouncement(
                                   new DerivationStart(first.logPosition(), first.state(), first.reemitMarker(),
                                                       first.queuePositions(), follower.termLength, follower.mtu),
                                   follower.commitCounterId)
                                   .encode());
        }
    }

    /** The id and the start position of the log's active recording, or null. */
    private long[] activeLogRecording() {
        final long[][] active = { null };
        archive.listRecordingsForUri(0, Integer.MAX_VALUE, "", ConsensusModule.Configuration.logStreamId(),
                                     (controlSessionId, correlationId, recordingId, startTimestamp, stopTimestamp,
                                      startPosition, stopPosition, initialTermId, segmentFileLength, termBufferLength,
                                      mtuLength, sessionId, streamId, strippedChannel, originalChannel,
                                      sourceIdentity) -> {
                                         if (stopTimestamp == AeronArchive.NULL_TIMESTAMP) {
                                             active[0] = new long[] { recordingId, startPosition };
                                         }
                                     });
        return active[0];
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

    @Override
    public int doWork() {
        int work = delegate.doWork();
        if (announcement != null) {
            announceFirstCheckpoint();
        }
        while (!unannounced.isEmpty() &&
               announcement.offer(new UnsafeBuffer(unannounced.peek()), 0, unannounced.peek().length) > 0) {
            unannounced.poll();
            work++;
        }
        if (follower != null) {
            work += follower.doWork();
        } else if (!followerDecided && delegate.monotonicMs() >= nextFollowAttemptMs) {
            nextFollowAttemptMs = delegate.monotonicMs() + FOLLOW_RETRY_MS;
            startFollower();
        }
        for (final DerivedReplay replay : replays.values()) {
            work += replay.doWork();
        }
        return work;
    }

    @Override
    public void stopReplay(final long replaySessionId) {
        final DerivedReplay replay = replays.remove(replaySessionId);
        if (replay != null) {
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

    /**
     * The derivation that follows the log from the member's start to its commit position and keeps the checkpoints.
     */
    private final class Follower implements Sequencer.FrameSink {
        private final int termLength;
        private final int mtu;
        private final long logRecordingId;
        private final int commitCounterId;
        private final List<Checkpoint> checkpoints = new ArrayList<>();
        private final long[] queuePositions;
        /** Where each queue's history starts: where the service created it, or where it was purged to. */
        private final long[] queueStarts;
        private final long logSessionId;
        private final Subscription log;
        private final LogDeriver deriver = new LogDeriver(memberId, this);
        private final ControlledFragmentAssembler entries =
            new ControlledFragmentAssembler((buffer, offset, length, header) -> {
                deriver.onLogEntry(buffer, offset, length);
                return ControlledFragmentHandler.Action.CONTINUE;
            });
        /** Where the last whole entry applied ends in the log. */
        private long entryEnd;
        /**
         * Ends a poll at the entry a checkpoint is due after: a poll's limit may fall inside an entry above the MTU.
         */
        private final ControlledFragmentHandler handler = (buffer, offset, length, header) -> {
            entries.onFragment(buffer, offset, length, header);
            if ((header.flags() & FrameDescriptor.END_FRAG_FLAG) == 0) {
                return ControlledFragmentHandler.Action.CONTINUE;
            }
            entryEnd = header.position();
            return checkpointDue() ? ControlledFragmentHandler.Action.BREAK : ControlledFragmentHandler.Action.CONTINUE;
        };
        /** The commit position when this started: its queue positions are no tip before it has derived that far. */
        private final long caughtUpAt;
        private boolean caughtUp;
        /** The log between two checkpoints, which doubles each time they are thinned. */
        private long checkpointSpacing = checkpointLogBytes;

        Follower(final DerivationStart start, final long logRecordingId, final int commitCounterId) {
            this.termLength = start.termLength();
            this.mtu = start.mtu();
            this.logRecordingId = logRecordingId;
            this.commitCounterId = commitCounterId;
            this.queuePositions = start.queueStarts().clone();
            this.queueStarts = start.queueStarts().clone();
            this.caughtUpAt = aeron.countersReader().getCounterValue(commitCounterId);
            this.caughtUp = caughtUpAt <= start.logPosition();
            checkpoints.add(
                new Checkpoint(start.logPosition(), start.state(), start.restored(), start.queueStarts().clone()));
            logSessionId = replayLog(logRecordingId, start.logPosition(), commitCounterId);
            log = logSubscription(logSessionId);
            deriver.restore(start.state(), start.restored());
        }

        /** Whether the derivation has caught up with the log, so that where it has brought a queue is its tip. */
        boolean isValid() {
            return caughtUp;
        }

        /** The last checkpoint at which group {@code groupId}'s queue stood at or before {@code position}. */
        Checkpoint checkpointAt(final int groupId, final long position) {
            for (int i = checkpoints.size() - 1; i >= 0; i--) {
                if (checkpoints.get(i).queuePositions()[groupId - 1] <= position) {
                    return checkpoints.get(i);
                }
            }
            return null;
        }

        /** Moves a queue's start, and releases the checkpoints no replay can continue from any more. */
        void purge(final int groupId, final long newStart) {
            queueStarts[groupId - 1] = newStart;
            while (checkpoints.size() > 1 && isBelowStarts(checkpoints.get(1))) {
                checkpoints.removeFirst();
            }
        }

        private boolean isBelowStarts(final Checkpoint checkpoint) {
            for (int i = 0; i < queueStarts.length; i++) {
                if (checkpoint.queuePositions()[i] > queueStarts[i]) {
                    return false;
                }
            }
            return true;
        }

        private boolean checkpointDue() {
            return entryEnd - checkpoints.getLast().logPosition() >= checkpointSpacing;
        }

        /** Releases every other checkpoint, keeping the first, which a queue's start may be continued from alone. */
        private void thin() {
            int kept = 0;
            for (int i = 0; i < checkpoints.size(); i += 2) {
                checkpoints.set(kept++, checkpoints.get(i));
            }
            checkpoints.subList(kept, checkpoints.size()).clear();
            checkpointSpacing *= 2;
        }

        int doWork() {
            final Image image = log.imageBySessionId((int)logSessionId);
            if (image == null) {
                return 0;
            }
            final int fragments = image.controlledPoll(handler, FOLLOW_FRAGMENT_LIMIT);
            if (checkpointDue() && entryEnd == image.position()) {
                checkpoints.add(new Checkpoint(entryEnd, deriver.state(), false, queuePositions.clone()));
                if (checkpoints.size() > maxCheckpoints) {
                    thin();
                }
            }
            caughtUp |= entryEnd >= caughtUpAt;
            return fragments;
        }

        @Override
        public void activate(final int[] groupIds) {
        }

        @Override
        public void publish(final int groupId, final DirectBuffer buffer, final int length) {
            final long start = QueuePositions.frameStart(queuePositions[groupId - 1], length, termLength, mtu);
            queuePositions[groupId - 1] = start + QueuePositions.framedLength(length, mtu);
        }
    }

    /** One replay: the archive's replay of the log in, the requested queue's frames out. */
    private final class DerivedReplay implements Sequencer.FrameSink {
        private final Queue queue;
        private final long startPosition;
        private final long endPosition;
        /** Where the queue stands after the last frame derived, offered or pending or before the replay's start. */
        private long derivedPosition;
        private final long logSessionId;
        private final Subscription log;
        private final ExclusivePublication publication;
        private final LogDeriver deriver = new LogDeriver(memberId, this);
        /** The frames the publication has not taken yet. */
        private final ArrayDeque<byte[]> pending = new ArrayDeque<>();
        /** How much of the first pending frame is not to be published: a replay may start inside a fragmented one. */
        private int pendingOffset;
        private final UnsafeBuffer pendingBuffer = new UnsafeBuffer();
        private final BufferClaim claim = new BufferClaim();
        private final ControlledFragmentHandler handler = new ControlledFragmentAssembler(this::onLogEntry);
        private final long connectDeadlineMs = delegate.monotonicMs() + CONNECT_TIMEOUT_MS;
        private boolean connected;
        private boolean started;
        private boolean closed;

        DerivedReplay(final Queue queue, final Checkpoint from, final long startPosition, final long endPosition,
                      final long logSessionId, final Subscription log, final ExclusivePublication publication) {
            this.queue = queue;
            this.startPosition = startPosition;
            this.endPosition = endPosition;
            this.derivedPosition = from.queuePositions()[queue.groupId() - 1];
            this.logSessionId = logSessionId;
            this.log = log;
            this.publication = publication;
            deriver.restore(from.state(), from.reemitMarker());
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
                if (!(pendingOffset == 0 ? offer(pendingBuffer, frame.length) : offerRest(frame))) {
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
            final long start = QueuePositions.frameStart(derivedPosition, length, queue.termLength(), queue.mtu());
            derivedPosition = start + QueuePositions.framedLength(length, queue.mtu());
            if (derivedPosition <= startPosition) {
                return; // before the replay
            }
            if (!started && start < startPosition) {
                // The archive replays from a fragment's start too, and a client stopped inside a frame resumes there.
                final long skipped = startPosition - start;
                if (skipped % queue.mtu() != 0) {
                    Logger.info(
                        Logger.CoreComponent.ReplayerService, memberId,
                        "group %d's derived replay from %d is closed: it is inside a fragment of the frame at %d",
                        queue.groupId(), startPosition, start);
                    close();
                    return;
                }
                started = true;
                pendingOffset = (int)(skipped / queue.mtu()) * publication.maxPayloadLength();
                final byte[] frame = new byte[length];
                buffer.getBytes(0, frame);
                pending.add(frame);
                return;
            }
            started = true;
            if (!connected || !pending.isEmpty() || !offer(buffer, length)) {
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
            return taken(result);
        }

        /**
         * Publishes a frame's fragments from {@link #pendingOffset} on, as the queue's publication cut them.
         * @return whether the publication took the last of them
         */
        private boolean offerRest(final byte[] frame) {
            while (pendingOffset < frame.length) {
                final int length = Math.min(publication.maxPayloadLength(), frame.length - pendingOffset);
                long result;
                while ((result = publication.tryClaim(length, claim)) == Publication.ADMIN_ACTION) {
                    Thread.onSpinWait();
                }
                if (!taken(result)) {
                    return false;
                }
                claim.buffer().putBytes(claim.offset(), frame, pendingOffset, length);
                pendingOffset += length;
                claim.flags(pendingOffset == frame.length ? FrameDescriptor.END_FRAG_FLAG : 0).commit();
            }
            pendingOffset = 0;
            return true;
        }

        private boolean taken(final long result) {
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

    /** One scan: the queue's frames from a position, derived from a checkpoint at or before it. */
    private final class DerivedScan implements Sequencer.FrameSink {
        private final Queue queue;
        private final long fromPosition;
        private final long endPosition;
        private final FrameVisitor visitor;
        private final LogDeriver deriver = new LogDeriver(memberId, this);
        private final SequencedFrameDecoder frame = new SequencedFrameDecoder();
        private long derivedPosition;
        private ScanEnd ended;

        DerivedScan(final Queue queue, final Checkpoint from, final long fromPosition, final long endPosition,
                    final FrameVisitor visitor) {
            this.queue = queue;
            this.fromPosition = fromPosition;
            this.endPosition = endPosition;
            this.visitor = visitor;
            this.derivedPosition = from.queuePositions()[queue.groupId() - 1];
            deriver.restore(from.state(), from.reemitMarker());
        }

        /** Reads the log until the scan ends, or delivers nothing for {@link #SCAN_STALL_TIMEOUT_MS}. */
        ScanEnd run(final Subscription log, final int logSessionId) {
            final ControlledFragmentHandler handler =
                new ControlledFragmentAssembler((buffer, offset, length, header) -> {
                    deriver.onLogEntry(buffer, offset, length);
                    return ended == null ? ControlledFragmentHandler.Action.CONTINUE
                                         : ControlledFragmentHandler.Action.BREAK;
                });
            long deadlineMs = delegate.monotonicMs() + SCAN_STALL_TIMEOUT_MS;
            while (ended == null) {
                if (derivedPosition >= endPosition) {
                    return ScanEnd.TIP;
                }
                final Image image = log.imageBySessionId(logSessionId);
                if (image != null && image.controlledPoll(handler, FRAGMENT_LIMIT) > 0) {
                    deadlineMs = delegate.monotonicMs() + SCAN_STALL_TIMEOUT_MS;
                } else if (delegate.monotonicMs() > deadlineMs) {
                    return ScanEnd.STALLED;
                } else {
                    Thread.yield();
                }
            }
            return ended;
        }

        @Override
        public void activate(final int[] groupIds) {
        }

        @Override
        public void publish(final int group, final DirectBuffer buffer, final int length) {
            if (group != queue.groupId() || ended != null) {
                return;
            }
            final long start = QueuePositions.frameStart(derivedPosition, length, queue.termLength(), queue.mtu());
            derivedPosition = start + QueuePositions.framedLength(length, queue.mtu());
            if (derivedPosition <= fromPosition) {
                return; // before the scan
            }
            if (start >= endPosition) {
                ended = ScanEnd.TIP;
            } else if (start < fromPosition || !frame.wrap(buffer, 0, length)) {
                ended = ScanEnd.UNREADABLE;
            } else if (!visitor.onFrame(start, frame.clusterSeqNo())) {
                ended = ScanEnd.STOPPED;
            }
        }
    }

    /** One recording per queue, once the derivation has caught up: nothing tells an inactive group's from another. */
    @Override
    public List<RecordingSpan> listQueueRecordings(final int groupId) {
        return follower != null && follower.isValid()
            ? List.of(new RecordingSpan(groupId, follower.queueStarts[groupId - 1], true, follower.termLength,
                                        follower.termLength))
            : List.of();
    }

    @Override
    public ScanEnd scanQueueRecording(final long recordingId, final long fromPosition, final long endPosition,
                                      final FrameVisitor visitor) {
        final Queue queue = queueOf(recordingId);
        final Checkpoint from = checkpointAt(queue, fromPosition);
        final long logSessionId = replayLog(follower.logRecordingId, from.logPosition(), follower.commitCounterId);
        try (Subscription log = logSubscription(logSessionId)) {
            return new DerivedScan(queue, from, fromPosition, endPosition, visitor).run(log, (int)logSessionId);
        } finally {
            try {
                archive.stopReplay(logSessionId);
            } catch (final RuntimeException ex) {
                // the archive has ended it already
            }
        }
    }

    /** Where the derivation has brought the queue: the committed log's, which the queue reaches. */
    @Override
    public long recordingPosition(final long recordingId) {
        return follower != null && follower.isValid() ? follower.queuePositions[(int)recordingId - 1]
                                                      : AeronArchive.NULL_POSITION;
    }

    /** Never: the derivation runs for as long as this does. */
    @Override
    public long stopPosition(final long recordingId) {
        return AeronArchive.NULL_POSITION;
    }

    /** None: nothing records the queue, so no replay competes with its recorder. */
    @Override
    public QueueBacklog queueBacklog(final int groupId, final RecordingSpan span) {
        return null;
    }

    /** Moves the queue's start: the log below it is the operator's to trim. */
    @Override
    public boolean purgeSegments(final long recordingId, final long newStartPosition) {
        follower.purge(queueOf(recordingId).groupId(), newStartPosition);
        return true;
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
