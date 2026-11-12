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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.collections.IntArrayList;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersReader;
import org.limitless.seqeron.protocol.FrameLayer;
import org.limitless.seqeron.protocol.SequencedFrameDecoder;
import org.limitless.seqeron.sbe.frame.MessageHeaderDecoder;
import org.limitless.seqeron.sbe.frame.SequencedHeaderDecoder;
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
 * <p>One derivation follows the log for as long as this runs and keeps a checkpoint of it every {@link
 * #CHECKPOINT_LOG_BYTES}: the deriver's state, and where each queue stood. It also compares each entry's frames with
 * a {@link FramePredictor}'s and keeps a {@link Run} where they differ, so a replay or a scan runs no sequencer: it
 * continues from the last run at or before the position it starts at, with the predictor between runs and the
 * frames each run kept. The runs are held in memory up to {@link #MAX_RUN_BYTES}; past it a run that an entry the
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
    /** What a run is counted as holding besides its frames. */
    private static final int RUN_BYTES = 512;

    /** The alias of a derived replay's publication, which an archive's replay does not carry. */
    public static final String DERIVED_ALIAS = "seqeron-replay-derived";

    private static final String DERIVED_CHANNEL = "aeron:ipc?alias=" + DERIVED_ALIAS;
    /** How long a derived replay waits for its subscriber, as the archive's replay does. */
    private static final long CONNECT_TIMEOUT_MS = 5_000;
    /** How long a derived scan may go without a log entry, as a scan of a recorded queue. */
    private static final long SCAN_STALL_TIMEOUT_MS = 2_000;
    private static final long FOLLOW_RETRY_MS = 1_000;
    private static final int ENTRY_LIMIT = 64;
    private static final int FOLLOW_ENTRY_LIMIT = 256;

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

    /** A frame as the sequencer published it. */
    private record Frame(int groupId, byte[] bytes) { }

    /**
     * A stretch of log a {@link FramePredictor} derives as the sequencer did, behind the frames of the entry before
     * it, which it does not: the derivation's start, an entry the two differed on, or no entry where a checkpoint
     * was due.
     *
     * @param logPosition     of that entry, or {@code entriesFrom}
     * @param entriesFrom     where the predicted entries start
     * @param lead            that entry's frames, or the ones the derivation's start published
     * @param queuePositions  where each queue stood before {@code lead}, slot {@code groupId - 1}
     * @param clusterSeqNo    the predictor's state after {@code lead}, with the next two
     * @param activeGroupMask the groups a frame has reached
     * @param groupSeqNos     of the last frame on each queue, slot {@code groupId - 1}
     * @param state           null, or the deriver's after {@code lead}: the runs were past {@link #MAX_RUN_BYTES}, so
     *                        this one keeps no run for the entries the predictor does not derive up to the next
     *                        checkpoint, and a {@link LogDeriver} derives it
     */
    private record Run(long logPosition, long entriesFrom, List<Frame> lead, long[] queuePositions, long clusterSeqNo,
                       int activeGroupMask, long[] groupSeqNos, LogDeriver.State state) {
        long bytes() {
            long bytes = RUN_BYTES;
            for (final Frame frame : lead) {
                bytes += frame.bytes().length;
            }
            return bytes;
        }
    }

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

    /**
     * The log position replays need the log from: the first checkpoint's, {@link LogTrimmer#HOLD} until the derivation
     * has started, or {@link Long#MAX_VALUE} if it never will.
     */
    long firstCheckpointLogPosition() {
        if (follower == null) {
            return followerDecided ? Long.MAX_VALUE : LogTrimmer.HOLD;
        }
        return follower.checkpoints.getFirst().logPosition();
    }

    /** The checkpoints kept, the derivation's start among them; 0 until the queues are listed. */
    public int checkpointCount() {
        return follower != null && follower.isValid() ? follower.checkpoints.size() : 0;
    }

    /** The runs kept, the derivation's start among them: one more for each entry the predictor did not derive. */
    public int runCount() {
        return follower == null ? 0 : follower.runs.size();
    }

    /** How many of the runs kept a {@link LogDeriver} derives, the runs having been past their bound. */
    public int sequencedRunCount() {
        return follower == null ? 0 : (int)follower.runs.stream().filter(run -> run.state() != null).count();
    }

    @Override
    public long startReplay(final long recordingId, final long position, final long length, final int streamId) {
        final Queue queue = queueOf(recordingId);
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
        return new Queue((int)recordingId, follower.termLength, follower.mtu);
    }

    /** The number of the last run before which a queue stood at or before {@code position}. */
    private long runAt(final Queue queue, final long position) {
        final long from = follower.runAt(queue.groupId(), position);
        if (from < 0) {
            throw new IllegalArgumentException("group " + queue.groupId() + "'s queue is not derived from " + position +
                                               ", before " + follower.queueStarts[queue.groupId() - 1]);
        }
        return from;
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
            follower = new Follower(start, log, commitCounterId);
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

    /** The id, start position, term length and segment file length of the log's active recording, or null. */
    private long[] activeLogRecording() {
        final long[][] active = { null };
        archive.listRecordingsForUri(
            0, Integer.MAX_VALUE, "", ConsensusModule.Configuration.logStreamId(),
            (controlSessionId, correlationId, recordingId, startTimestamp, stopTimestamp, startPosition, stopPosition,
             initialTermId, segmentFileLength, termBufferLength, mtuLength, sessionId, streamId, strippedChannel,
             originalChannel, sourceIdentity) -> {
                if (stopTimestamp == AeronArchive.NULL_TIMESTAMP) {
                    active[0] = new long[] { recordingId, startPosition, termBufferLength, segmentFileLength };
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
        /** The log's recording: its id, start position, term length and segment file length. */
        private final long[] logRecording;
        private final int commitCounterId;
        private final List<Checkpoint> checkpoints = new ArrayList<>();
        private final long[] queuePositions;
        /** Where each queue's history starts: where the service created it, or where it was purged to. */
        private final long[] queueStarts;
        private final RecordingReader log;
        private final LogDeriver deriver = new LogDeriver(memberId, this);
        /** The runs kept, and how many a purge has released before them: a run's number counts those. */
        private final List<Run> runs = new ArrayList<>();
        private long runsReleased;
        private long runBytes;
        /** Whether the last run is one a {@link LogDeriver} derives, which no run is kept behind. */
        private boolean sequencedRun;
        private final FramePredictor predictor = new FramePredictor();
        private final SequencedHeaderDecoder queueHeader = new SequencedHeaderDecoder();
        /** Of the last frame on each queue, and the groups a frame has reached. */
        private final long[] groupSeqNos = new long[FrameLayer.MAX_GROUPS];
        private int activeGroupMask;
        /** The frames of the entry being applied, and where each queue stood before it. */
        private final ExpandableArrayBuffer entryFrames = new ExpandableArrayBuffer();
        private final IntArrayList entryFrameGroups = new IntArrayList();
        private final IntArrayList entryFrameEnds = new IntArrayList();
        private final long[] entryQueuePositions = new long[FrameLayer.MAX_GROUPS];
        /** How many of the entry's frames the predictor has published, and whether one differed. */
        private int predicted;
        private boolean differed;
        private final Sequencer.FrameSink comparison = new Sequencer.FrameSink() {
            @Override
            public void activate(final int[] groupIds) {
            }

            @Override
            public void publish(final int groupId, final DirectBuffer buffer, final int length) {
                final int i = predicted++;
                if (i >= entryFrameGroups.size() || entryFrameGroups.getInt(i) != groupId) {
                    differed = true;
                    return;
                }
                final int from = i == 0 ? 0 : entryFrameEnds.getInt(i - 1);
                differed |= !Arrays.equals(entryFrames.byteArray(), from, entryFrameEnds.getInt(i), buffer.byteArray(),
                                           0, length);
            }
        };
        /** Where the last entry applied ends in the log. */
        private long entryEnd;
        /** Ends a poll at the entry a checkpoint is due after. */
        private final RecordingReader.MessageHandler handler = (buffer, offset, length, endPosition) -> {
            startEntry();
            deriver.onLogEntry(buffer, offset, length);
            if (!sequencedRun) {
                predicted = 0;
                differed = false;
                predictor.onLogEntry(buffer, offset, length, -1, comparison);
                if (differed || predicted != entryFrameGroups.size()) {
                    addRun(entryEnd, endPosition);
                }
            }
            entryEnd = endPosition;
            return !checkpointDue();
        };
        /** The commit position when this started: its queue positions are no tip before it has derived that far. */
        private final long caughtUpAt;
        private boolean caughtUp;
        /** The log between two checkpoints, which doubles each time they are thinned. */
        private long checkpointSpacing = checkpointLogBytes;

        Follower(final DerivationStart start, final long[] logRecording, final int commitCounterId) {
            this.termLength = start.termLength();
            this.mtu = start.mtu();
            this.logRecording = logRecording;
            this.commitCounterId = commitCounterId;
            this.queuePositions = start.queueStarts().clone();
            this.queueStarts = start.queueStarts().clone();
            this.caughtUpAt = aeron.countersReader().getCounterValue(commitCounterId);
            this.caughtUp = caughtUpAt <= start.logPosition();
            checkpoints.add(
                new Checkpoint(start.logPosition(), start.state(), start.restored(), start.queueStarts().clone()));
            entryEnd = start.logPosition();
            log = logReader(entryEnd);
            startEntry();
            deriver.restore(start.state(), start.restored());
            addRun(entryEnd, entryEnd);
        }

        private void startEntry() {
            entryFrameGroups.clear();
            entryFrameEnds.clear();
            System.arraycopy(queuePositions, 0, entryQueuePositions, 0, queuePositions.length);
        }

        /** Keeps a run behind the frames collected since {@link #startEntry}, from where the sequencer stands. */
        private void addRun(final long logPosition, final long entriesFrom) {
            final List<Frame> lead = new ArrayList<>(entryFrameGroups.size());
            for (int i = 0, from = 0; i < entryFrameGroups.size(); from = entryFrameEnds.getInt(i++)) {
                final byte[] bytes = new byte[entryFrameEnds.getInt(i) - from];
                entryFrames.getBytes(from, bytes);
                lead.add(new Frame(entryFrameGroups.getInt(i), bytes));
            }
            sequencedRun = entriesFrom > logPosition && runBytes > maxRunBytes;
            runs.add(new Run(logPosition, entriesFrom, lead, entryQueuePositions.clone(), deriver.clusterSeqNo(),
                             activeGroupMask, groupSeqNos.clone(), sequencedRun ? deriver.state() : null));
            runBytes += runs.getLast().bytes();
            predictor.restore(deriver.clusterSeqNo(), activeGroupMask, groupSeqNos);
        }

        /** The run of a number, or null if it is released or not kept yet. */
        Run run(final long number) {
            return number < runsReleased || number >= runsReleased + runs.size()
                ? null
                : runs.get((int)(number - runsReleased));
        }

        /**
         * The number of the last run before which group {@code groupId}'s queue stood at or before {@code position}.
         */
        long runAt(final int groupId, final long position) {
            for (int i = runs.size() - 1; i >= 0; i--) {
                if (runs.get(i).queuePositions()[groupId - 1] <= position) {
                    return runsReleased + i;
                }
            }
            return -1;
        }

        /** A reader of the log's recording from an entry's start. */
        RecordingReader logReader(final long position) {
            return new RecordingReader(archiveDir, logRecording[0], logRecording[1], (int)logRecording[2],
                                       (int)logRecording[3], position);
        }

        /** Whether the derivation has caught up with the log, so that where it has brought a queue is its tip. */
        boolean isValid() {
            return caughtUp;
        }

        /** Moves a queue's start, and releases the checkpoints no replay can continue from any more. */
        void purge(final int groupId, final long newStart) {
            queueStarts[groupId - 1] = newStart;
            while (checkpoints.size() > 1 && isBelowStarts(checkpoints.get(1).queuePositions())) {
                checkpoints.removeFirst();
            }
            int released = 0;
            while (released + 1 < runs.size() && isBelowStarts(runs.get(released + 1).queuePositions())) {
                runBytes -= runs.get(released++).bytes();
            }
            runs.subList(0, released).clear();
            runsReleased += released;
        }

        private boolean isBelowStarts(final long[] positions) {
            for (int i = 0; i < queueStarts.length; i++) {
                if (positions[i] > queueStarts[i]) {
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
            final int entries =
                log.poll(handler, aeron.countersReader().getCounterValue(commitCounterId), FOLLOW_ENTRY_LIMIT);
            if (checkpointDue()) {
                checkpoints.add(new Checkpoint(entryEnd, deriver.state(), false, queuePositions.clone()));
                startEntry();
                addRun(entryEnd, entryEnd); // so that a replay's first frame is no further off than from a checkpoint
                if (checkpoints.size() > maxCheckpoints) {
                    thin();
                }
            }
            caughtUp |= entryEnd >= caughtUpAt;
            return entries;
        }

        @Override
        public void activate(final int[] groupIds) {
        }

        @Override
        public void publish(final int groupId, final DirectBuffer buffer, final int length) {
            final long start = QueuePositions.frameStart(queuePositions[groupId - 1], length, termLength, mtu);
            queuePositions[groupId - 1] = start + QueuePositions.framedLength(length, mtu);
            groupSeqNos[groupId - 1] = queueHeader.wrap(buffer, MessageHeaderDecoder.ENCODED_LENGTH).groupSeqNo();
            activeGroupMask |= 1 << (groupId - 1);
            final int from = entryFrameEnds.isEmpty() ? 0 : entryFrameEnds.getInt(entryFrameEnds.size() - 1);
            entryFrames.putBytes(from, buffer, 0, length);
            entryFrameGroups.addInt(groupId);
            entryFrameEnds.addInt(from + length);
        }
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
        private Run run;
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
            return log.poll(handler, follower.entryEnd, entryLimit);
        }

        private boolean onLogEntry(final DirectBuffer buffer, final int offset, final int length,
                                   final long endPosition) {
            for (Run next; (next = follower.run(runNumber + 1)) != null && next.logPosition() <= entryStart;) {
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
            for (final Frame frame : run.lead()) {
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

        /** Reads the log until the scan ends, or delivers nothing for {@link #SCAN_STALL_TIMEOUT_MS}. */
        ScanEnd run(final RunWalker log) {
            long deadlineMs = delegate.monotonicMs() + SCAN_STALL_TIMEOUT_MS;
            while (ended == null) {
                if (derivedPosition >= endPosition) {
                    return ScanEnd.TIP;
                }
                if (log.poll(ENTRY_LIMIT) > 0) {
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
