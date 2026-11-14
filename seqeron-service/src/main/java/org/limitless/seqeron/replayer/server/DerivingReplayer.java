package org.limitless.seqeron.replayer.server;

import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import io.aeron.Publication;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.cluster.ConsensusModule;
import io.aeron.logbuffer.BufferClaim;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.FrameDescriptor;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayDeque;
import java.util.List;
import java.util.function.BooleanSupplier;
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
import org.limitless.seqeron.sequencer.FramePredictor;
import org.limitless.seqeron.sequencer.LogDeriver;
import org.limitless.seqeron.sequencer.QueuePositions;
import org.limitless.seqeron.sequencer.Sequencer;
import org.limitless.seqeron.util.Logger;

/**
 * A member's {@link Replayer} whose queues are not recorded: a queue's history is a {@link LogDeriver} over the Raft
 * log's recording, read from its segment files up to the commit position ({@link RecordingReader}) and published at the
 * queue's term length and MTU, which puts each frame at the position the queue's publication put it at
 * (doc/disk-load-plan.md, step 2a).
 *
 * <p>To {@link ReplayerService} each queue is one active recording whose id is its group's, which starts where the
 * queue did and has reached where the derivation has brought it.
 *
 * <p>One derivation, a {@link LogFollower}, follows the log for as long as this runs and keeps a checkpoint of it every
 * {@link #CHECKPOINT_LOG_BYTES}: the deriver's state, and where each queue stood. It also compares each entry's frames
 * with a {@link FramePredictor}'s and keeps a {@link LogFollower.Run} where they differ, so a replay or a scan runs no
 * sequencer: it continues from the last run at or before the position it starts at, with the predictor between runs and
 * the frames each run kept. The runs are held in memory up to {@link #MAX_RUN_BYTES}; past it a run that an entry the
 * predictor does not derive starts holds the deriver's state, and a replay runs a sequencer over it up to the next
 * checkpoint. The derivation starts where the member's service says it did
 * ({@link DerivationStart}): at the log's first entry, or at the cluster snapshot it restored, each queue at the
 * position the service created it at.
 *
 * <p>No queue is listed until the derivation has caught up with the log's commit position as it stood when it
 * started. The checkpoints are held in memory, rebuilt at each start, and released below a purge. Past {@link
 * #MAX_CHECKPOINTS} every other one is released and the log between two doubles, so a log nothing purges costs a
 * bounded number of them; the runs kept at them stay, so a replay's first frame is no further off. The replay
 * protocol's streams, the counters and the clock are the Replayer's it decorates.
 *
 * <p>Not thread-safe: every call is from the Replayer's duty-cycle thread.
 */
public final class DerivingReplayer implements Replayer {
    /** How much log lies between two checkpoints at first: the most a replay derives before its first frame. */
    public static final long CHECKPOINT_LOG_BYTES = 8L * 1024 * 1024;
    /** The most checkpoints kept: 8 GiB of log at {@link #CHECKPOINT_LOG_BYTES}, before the log between two doubles. */
    public static final int MAX_CHECKPOINTS = 1024;
    /**
     * The most the runs may hold before a stretch of log with an entry the predictor does not derive runs a sequencer.
     */
    public static final long MAX_RUN_BYTES = 64L * 1024 * 1024;

    /** The alias of a derived replay's publication, which an archive's replay does not carry. */
    public static final String DERIVED_ALIAS = "seqeron-replay-derived";

    private static final String DERIVED_CHANNEL = "aeron:ipc?alias=" + DERIVED_ALIAS;
    /** How long a derived replay waits for its subscriber, as the archive's replay does. */
    private static final long CONNECT_TIMEOUT_MS = 5_000;
    private static final long FOLLOW_RETRY_MS = 1_000;
    private static final int ENTRY_LIMIT = 64;

    private final Replayer delegate;
    private final Aeron aeron;
    private final AeronArchive archive;
    private final File archiveDir;
    private final int memberId;
    private final Supplier<DerivationStart> derivationStart;
    private final long checkpointLogBytes;
    private final int maxCheckpoints;
    private final long maxRunBytes;
    private final Long2ObjectHashMap<DerivedReplay> replays = new Long2ObjectHashMap<>();
    /** The derivation that keeps the checkpoints, or null until the service has started and the log is recorded. */
    private LogFollower follower;
    /** The counter of the log's commit position, which the follower reads up to. */
    private int commitCounterId;
    /** Where a gateway host reads this member's {@link DerivationAnnouncement}, and what is still to be published. */
    private ExclusivePublication announcement;
    /** Whether the archive records the announcements, and whether it last refused to. */
    private boolean announcing;
    private boolean announcingRefused;
    private final ArrayDeque<byte[]> unannounced = new ArrayDeque<>();
    /** The checkpoint announced last: the first kept, which a gateway host may start from. */
    private LogFollower.Checkpoint announced;
    /** When the follower's start, or the announcements' recording, is next tried. */
    private long nextAttemptMs;

    /** A queue as it is published: every queue of a node has its term length and MTU. */
    private record Queue(int groupId, int termLength, int mtu) { }

    /**
     * @param archiveDir      where the member's archive keeps the log's segment files
     * @param derivationStart where the member's service started, or null until it has
     */
    public DerivingReplayer(final Replayer delegate, final Aeron aeron, final AeronArchive archive,
                            final File archiveDir, final int memberId,
                            final Supplier<DerivationStart> derivationStart) {
        this(delegate, aeron, archive, archiveDir, memberId, derivationStart, CHECKPOINT_LOG_BYTES, MAX_CHECKPOINTS,
             MAX_RUN_BYTES);
    }

    /**
     * As above, for a test that cannot write gigabytes of log.
     *
     * @param checkpointLogBytes the log between two checkpoints at first
     * @param maxCheckpoints     the most kept
     * @param maxRunBytes        the most the runs may hold
     */
    public DerivingReplayer(final Replayer delegate, final Aeron aeron, final AeronArchive archive,
                            final File archiveDir, final int memberId, final Supplier<DerivationStart> derivationStart,
                            final long checkpointLogBytes, final int maxCheckpoints, final long maxRunBytes) {
        this.checkpointLogBytes = checkpointLogBytes;
        this.maxCheckpoints = maxCheckpoints;
        this.maxRunBytes = maxRunBytes;
        this.derivationStart = derivationStart;
        this.delegate = delegate;
        this.aeron = aeron;
        this.archive = archive;
        this.archiveDir = archiveDir;
        this.memberId = memberId;
    }

    /** Where replays need the log from: the first checkpoint, or {@link LogTrimmer#HOLD} until the follower starts. */
    long firstCheckpointLogPosition() {
        return follower == null ? LogTrimmer.HOLD : follower.checkpoints().getFirst().logPosition();
    }

    /** The checkpoints kept, the derivation's start among them; 0 until the queues are listed. */
    public int checkpointCount() {
        return follower != null && follower.isValid() ? follower.checkpoints().size() : 0;
    }

    /** Whether this member's archive records its announcements, which a gateway host starts from. */
    public boolean announces() {
        return announcing;
    }

    /** The runs kept, the derivation's start among them: one more for each entry the predictor did not derive. */
    public int runCount() {
        return follower == null ? 0 : follower.runCount();
    }

    /** How many of the runs kept a {@link LogDeriver} derives, the runs having been past their bound. */
    public int sequencedRunCount() {
        return follower == null ? 0 : follower.sequencedRunCount();
    }

    @Override
    public long startReplay(final long recordingId, final long position, final long length, final int streamId) {
        final Queue queue = queueOf(recordingId);
        if ((position & (FrameDescriptor.FRAME_ALIGNMENT - 1)) != 0) {
            throw new IllegalArgumentException("position " + position + " is not on a frame boundary");
        }
        final long from = runAt(queue, position);
        final int termId = (int)(position >> Integer.numberOfTrailingZeros(queue.termLength()));
        final ExclusivePublication publication = aeron.addExclusivePublication(
            DERIVED_CHANNEL + "|term-length=" + queue.termLength() + "|mtu=" + queue.mtu() +
                "|init-term-id=0|term-id=" + termId + "|term-offset=" + (position & (queue.termLength() - 1)),
            streamId);
        // What the client subscribes by, as an archive's replay session id is: its low 32 bits.
        final long replaySessionId = publication.sessionId() & 0xFFFF_FFFFL;
        replays.put(replaySessionId, new DerivedReplay(queue, from, position, position + length, publication));
        Logger.info(
            Logger.CoreComponent.ReplayerService, memberId,
            "replay session %d derives group %d's queue [%d,%d) from the log at %d, where the queue stood at %d",
            replaySessionId, queue.groupId(), position, position + length, follower.run(from).entriesFrom(),
            follower.run(from).queuePositions()[queue.groupId() - 1]);
        return replaySessionId;
    }

    /** The queue a recording id names, once the queues are listed. */
    private Queue queueOf(final long recordingId) {
        if (follower == null || !follower.isValid() || recordingId < 1 || recordingId > FrameLayer.MAX_GROUPS) {
            throw new IllegalArgumentException("no queue is derived as recording " + recordingId);
        }
        return new Queue((int)recordingId, follower.termLength(), follower.mtu());
    }

    /** The number of the last run before which a queue stood at or before {@code position}. */
    private long runAt(final Queue queue, final long position) {
        final long from = follower.runAt(queue.groupId(), position);
        if (from < 0) {
            throw new IllegalArgumentException("group " + queue.groupId() + "'s queue is not derived from " + position +
                                               ", before " + follower.queueStart(queue.groupId()));
        }
        return from;
    }

    /**
     * Starts the follower once the service has handed over its start and the log is being recorded. A member has no
     * recorded path to fall back on, so a start the log cannot serve, or a follower that cannot be built, ends the duty
     * cycle and with it the node.
     */
    private void startFollower() {
        final DerivationStart start = derivationStart.get();
        final ArchiveRecording log =
            start == null ? null : ArchiveRecording.activeOf(archive, ConsensusModule.Configuration.logStreamId());
        final int counterId = commitPositionCounterId();
        if (log == null || counterId == CountersReader.NULL_COUNTER_ID) {
            return;
        }
        if (start.logPosition() < log.startPosition()) {
            throw new IllegalStateException("the log's recording starts at " + log.startPosition() +
                                            ", after the service's start at " + start.logPosition() +
                                            ", so no queue can be derived");
        }
        final CountersReader counters = aeron.countersReader();
        follower =
            new LogFollower(memberId, start, archiveDir, log,
                            () -> counters.getCounterValue(counterId), checkpointLogBytes, maxCheckpoints, maxRunBytes);
        commitCounterId = counterId;
        announcement = aeron.addExclusivePublication(DerivationAnnouncement.CHANNEL, DerivationAnnouncement.STREAM_ID);
        recordAnnouncements();
    }

    /**
     * Records this run's announcements, and purges the ones earlier runs left. A refusal, such as a purge the archive
     * declines while a gateway host replays the recording, is tried again at the next attempt.
     */
    private void recordAnnouncements() {
        try {
            for (final ArchiveRecording earlier :
                 ArchiveRecording.ofStream(archive, DerivationAnnouncement.STREAM_ID)) {
                if (!earlier.active()) {
                    archive.purgeRecording(earlier.recordingId());
                }
            }
            archive.startRecording(DerivationAnnouncement.CHANNEL, DerivationAnnouncement.STREAM_ID,
                                   SourceLocation.LOCAL, true);
            announcing = true;
            if (announcingRefused) {
                Logger.info(Logger.CoreComponent.ReplayerService, memberId, "recording this member's announcements");
            }
        } catch (final RuntimeException ex) {
            if (!announcingRefused) {
                announcingRefused = true;
                Logger.error(Logger.CoreComponent.ReplayerService, Logger.CoreEventCode.ServiceError, memberId,
                             "the archive did not record this member's announcements, which a gateway host starts "
                                 + "from (%s); trying again every %dms",
                             ex.getMessage(), FOLLOW_RETRY_MS);
            }
        }
    }

    /** Announces the first checkpoint kept once a purge has released the one announced before it. */
    private void announceFirstCheckpoint() {
        final LogFollower.Checkpoint first = follower.checkpoints().getFirst();
        if (first != announced && unannounced.isEmpty()) {
            announced = first;
            unannounced.addAll(new DerivationAnnouncement(
                                   new DerivationStart(first.logPosition(), first.state(), first.reemitMarker(),
                                                       first.queuePositions(), follower.termLength(), follower.mtu()),
                                   commitCounterId)
                                   .encode());
        }
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
        if (announcing) {
            announceFirstCheckpoint();
        }
        while (!unannounced.isEmpty() &&
               announcement.offer(new UnsafeBuffer(unannounced.peek()), 0, unannounced.peek().length) > 0) {
            unannounced.poll();
            work++;
        }
        if ((follower == null || !announcing) && delegate.monotonicMs() >= nextAttemptMs) {
            nextAttemptMs = delegate.monotonicMs() + FOLLOW_RETRY_MS;
            if (follower == null) {
                startFollower();
            } else {
                recordAnnouncements();
            }
        }
        if (follower != null) {
            work += follower.doWork();
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
     * One group's frames from a run on: the run's own, then a {@link FramePredictor}'s for the log behind it, as far as
     * the follower has compared them, or a {@link LogDeriver}'s where the run holds its state.
     */
    private final class RunWalker implements AutoCloseable {
        private final int groupId;
        private final Sequencer.FrameSink sink;
        private final BooleanSupplier wantsMore;
        private final FramePredictor predictor = new FramePredictor();
        private LogDeriver deriver;
        private final UnsafeBuffer leadFrame = new UnsafeBuffer();
        private final RecordingReader log;
        private long runNumber;
        private LogFollower.Run run;
        /** Where the next entry starts in the log. */
        private long entryStart;
        private boolean entered;
        private final RecordingReader.MessageHandler handler = this::onLogEntry;

        /**
         * @param wantsMore whether the sink takes another entry's frames in this poll
         */
        RunWalker(final int groupId, final long runNumber, final Sequencer.FrameSink sink,
                  final BooleanSupplier wantsMore) {
            this.groupId = groupId;
            this.runNumber = runNumber;
            this.sink = sink;
            this.wantsMore = wantsMore;
            this.run = follower.run(runNumber);
            this.entryStart = run.entriesFrom();
            this.log = follower.logReader(entryStart);
        }

        /**
         * @return the entries read
         * @throws UncheckedIOException if the log is gone, or the runs below this were released
         */
        int poll(final int entryLimit) {
            if (follower.run(runNumber) == null) {
                throw new UncheckedIOException(new IOException("the queue is purged past the run"));
            }
            if (!entered) {
                entered = true;
                enter();
                if (!wantsMore.getAsBoolean()) {
                    return 1;
                }
            }
            return log.poll(handler, follower.entryEnd(), entryLimit);
        }

        private boolean onLogEntry(final DirectBuffer buffer, final int offset, final int length,
                                   final long endPosition) {
            for (LogFollower.Run next;
                 (next = follower.run(runNumber + 1)) != null && next.logPosition() <= entryStart;) {
                runNumber++;
                run = next;
                enter();
            }
            entryStart = endPosition;
            if (endPosition > run.entriesFrom()) {
                if (run.state() != null) {
                    deriver.onLogEntry(buffer, offset, length);
                } else {
                    predictor.onLogEntry(buffer, offset, length, 1 << (groupId - 1), sink);
                }
            }
            return wantsMore.getAsBoolean();
        }

        private void enter() {
            for (final LogFollower.Frame frame : run.lead()) {
                if (frame.groupId() == groupId) {
                    leadFrame.wrap(frame.bytes());
                    sink.publish(groupId, leadFrame, frame.bytes().length);
                }
            }
            if (run.state() != null) {
                if (deriver == null) {
                    deriver = new LogDeriver(memberId, sink); // the sink takes its own group's frames alone
                }
                deriver.restore(run.state(), false);
            } else {
                predictor.restore(run.clusterSeqNo(), run.activeGroupMask(), run.groupSeqNos());
            }
        }

        @Override
        public void close() {
            log.close();
        }
    }

    /** One replay: the log's entries in, the requested queue's frames out. */
    private final class DerivedReplay implements Sequencer.FrameSink {
        private final Queue queue;
        private final long startPosition;
        private final long endPosition;
        /** Where the queue stands after the last frame derived, offered or pending or before the replay's start. */
        private long derivedPosition;
        private final RunWalker walker;
        private final ExclusivePublication publication;
        /** The frames the publication has not taken yet. */
        private final ArrayDeque<byte[]> pending = new ArrayDeque<>();
        /** How much of the first pending frame is not to be published: a replay may start inside a fragmented one. */
        private int pendingOffset;
        private final UnsafeBuffer pendingBuffer = new UnsafeBuffer();
        private final BufferClaim claim = new BufferClaim();
        private final long connectDeadlineMs = delegate.monotonicMs() + CONNECT_TIMEOUT_MS;
        private boolean connected;
        private boolean started;
        private boolean closed;

        DerivedReplay(final Queue queue, final long from, final long startPosition, final long endPosition,
                      final ExclusivePublication publication) {
            this.queue = queue;
            this.startPosition = startPosition;
            this.endPosition = endPosition;
            this.derivedPosition = follower.run(from).queuePositions()[queue.groupId() - 1];
            this.publication = publication;
            this.walker = new RunWalker(queue.groupId(), from, this,
                                        () -> pending.isEmpty() && derivedPosition < this.endPosition && !closed);
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
            try {
                return work + walker.poll(ENTRY_LIMIT);
            } catch (final UncheckedIOException ex) {
                Logger.info(Logger.CoreComponent.ReplayerService, memberId, "group %d's derived replay is closed: %s",
                            queue.groupId(), ex.getMessage());
                close();
                return work;
            }
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
            CloseHelper.quietCloseAll(publication, walker);
        }
    }

    /** One scan: the queue's frames from a position, derived from a run at or before it. */
    private final class DerivedScan implements Sequencer.FrameSink {
        private final Queue queue;
        private final long fromPosition;
        private final long endPosition;
        private final FrameVisitor visitor;
        private final SequencedFrameDecoder frame = new SequencedFrameDecoder();
        private long derivedPosition;
        private ScanEnd ended;

        DerivedScan(final Queue queue, final long from, final long fromPosition, final long endPosition,
                    final FrameVisitor visitor) {
            this.queue = queue;
            this.fromPosition = fromPosition;
            this.endPosition = endPosition;
            this.visitor = visitor;
            this.derivedPosition = follower.run(from).queuePositions()[queue.groupId() - 1];
        }

        /**
         * Reads the log until the scan ends, or until it reaches where the follower has, which no wait moves: the
         * follower runs on this thread.
         */
        ScanEnd run(final RunWalker log) {
            while (ended == null) {
                if (derivedPosition >= endPosition) {
                    return ScanEnd.TIP;
                }
                if (log.poll(ENTRY_LIMIT) == 0) {
                    return ScanEnd.STALLED;
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
            ? List.of(new RecordingSpan(groupId, follower.queueStart(groupId), true, follower.termLength(),
                                        follower.termLength()))
            : List.of();
    }

    @Override
    public ScanEnd scanQueueRecording(final long recordingId, final long fromPosition, final long endPosition,
                                      final FrameVisitor visitor) {
        final Queue queue = queueOf(recordingId);
        final long from = runAt(queue, fromPosition);
        final DerivedScan scan = new DerivedScan(queue, from, fromPosition, endPosition, visitor);
        try (RunWalker log = new RunWalker(queue.groupId(), from, scan, () -> scan.ended == null)) {
            return scan.run(log);
        } catch (final UncheckedIOException ex) {
            return ScanEnd.UNREADABLE;
        }
    }

    /** Where the derivation has brought the queue: the committed log's, which the queue reaches. */
    @Override
    public long recordingPosition(final long recordingId) {
        return follower != null && follower.isValid() ? follower.queuePosition((int)recordingId)
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
