package org.limitless.seqeron.replayer.server;

import io.aeron.Aeron;
import io.aeron.ChannelUri;
import io.aeron.ControlledFragmentAssembler;
import io.aeron.ExclusivePublication;
import io.aeron.FragmentAssembler;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.codecs.ClientSessionDecoder;
import io.aeron.cluster.codecs.MessageHeaderDecoder;
import io.aeron.cluster.codecs.SnapshotMark;
import io.aeron.cluster.codecs.SnapshotMarkerDecoder;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.logbuffer.BufferClaim;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.logbuffer.Header;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.collections.LongArrayList;
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
 * A member's {@link Replayer} that serves a replay off the Raft log's recording instead of the queue's: a {@link
 * LogDeriver} over the log, published at the queue's term length and MTU, puts each frame at the position the queue's
 * recording holds it at (doc/disk-load-plan.md, step 2a).
 *
 * <p>One derivation follows the log for as long as this runs and keeps a checkpoint of it every {@link
 * #CHECKPOINT_LOG_BYTES}: the deriver's state, and where each queue stood. A replay continues from the last
 * checkpoint at or before the position it starts at. The derivation itself starts where the member's service did: at
 * the log's first entry, or at the cluster snapshot whose marker is the first frame of the queues' recordings.
 *
 * <p>A queue's tip and a scan of it are the derivation's too, so that only a queue's identity, its start and its
 * first frame are read off its recording.
 *
 * <p>Prototype: every other call goes to the Replayer it decorates, and so do these until the derivation has
 * placed the first frame of each queue's recording where the recording holds it, and for good if it places one
 * elsewhere. It finds no start for a queue recording purged past its first frame on a member that started from a
 * snapshot. The checkpoints are held in memory and rebuilt at each start.
 *
 * <p>Not thread-safe: every call is from the Replayer's duty-cycle thread.
 */
public final class DerivingReplayer implements Replayer {
    /** The stream the archive replays the log, and a cluster snapshot, on to this, beside the replay protocol's. */
    private static final int LOG_REPLAY_STREAM_ID = 204;

    /** How much log lies between two checkpoints: the most a replay derives before its first frame. */
    public static final long CHECKPOINT_LOG_BYTES = 8L * 1024 * 1024;

    private static final String LOG_REPLAY_CHANNEL = "aeron:ipc?alias=seqeron-log-derive";
    /** The alias of a derived replay's publication, which an archive's replay does not carry. */
    public static final String DERIVED_ALIAS = "seqeron-replay-derived";

    private static final String DERIVED_CHANNEL = "aeron:ipc?alias=" + DERIVED_ALIAS;
    /** How long a derived replay waits for its subscriber, as the archive's replay does. */
    private static final long CONNECT_TIMEOUT_MS = 5_000;
    private static final long SNAPSHOT_READ_TIMEOUT_MS = 2_000;
    /** How long a derived scan may go without a log entry, as the archive's replay of a recorded one. */
    private static final long SCAN_STALL_TIMEOUT_MS = 2_000;
    private static final long FOLLOW_RETRY_MS = 1_000;
    private static final int FRAGMENT_LIMIT = 64;
    private static final int FOLLOW_FRAGMENT_LIMIT = 256;

    private final Replayer delegate;
    private final Aeron aeron;
    private final AeronArchive archive;
    private final int memberId;
    private final Long2ObjectHashMap<DerivedReplay> replays = new Long2ObjectHashMap<>();
    /** The derivation that keeps the checkpoints, or null until the queues' recordings have a first frame. */
    private Follower follower;
    /** Whether a start was found for the follower, or none can be. */
    private boolean followerDecided;
    private long nextFollowAttemptMs;

    /** A queue's active recording, and the {@code clusterSeqNo} of the frame it starts with. */
    private record
        Queue(int groupId, long recordingId, long startPosition, int termLength, int mtu, long firstClusterSeqNo) { }

    /**
     * Where a derivation continues from.
     *
     * @param logPosition    of the log entry to apply next
     * @param state          the deriver's before that entry
     * @param reemitMarker   whether the state is a cluster snapshot's, whose marker starts each queue
     * @param queuePositions where each queue stood, slot {@code groupId - 1}
     */
    private record Checkpoint(long logPosition, LogDeriver.State state, boolean reemitMarker, long[] queuePositions) { }

    public DerivingReplayer(final Replayer delegate, final Aeron aeron, final AeronArchive archive,
                            final int memberId) {
        this.delegate = delegate;
        this.aeron = aeron;
        this.archive = archive;
        this.memberId = memberId;
    }

    /** The checkpoints kept so far, the derivation's start among them; 0 while replays are not derived. */
    public int checkpointCount() {
        return follower != null && follower.isValid() ? follower.checkpoints.size() : 0;
    }

    /** Whether a replay is derived from the log, and not the archive's of the queue's recording. */
    public boolean isDerived(final long replaySessionId) {
        return replays.containsKey(replaySessionId);
    }

    @Override
    public long startReplay(final long recordingId, final long position, final long length, final int streamId) {
        final Queue queue = follower != null && follower.isValid() ? follower.queueOf(recordingId) : null;
        final Checkpoint from = queue == null ? null : follower.checkpointAt(queue.groupId(), position);
        if (from == null) {
            return delegate.startReplay(recordingId, position, length, streamId);
        }
        final long logSessionId;
        try {
            logSessionId = replayLog(follower.logRecordingId, from.logPosition(), follower.commitCounterId);
        } catch (final RuntimeException ex) {
            Logger.info(Logger.CoreComponent.ReplayerService, memberId,
                        "group %d's replay from %d is not derived, the log does not replay from %d: %s",
                        queue.groupId(), position, from.logPosition(), ex.getMessage());
            return delegate.startReplay(recordingId, position, length, streamId);
        }
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

    private long replayLog(final long logRecordingId, final long logPosition, final int commitCounterId) {
        return archive.startBoundedReplay(logRecordingId, logPosition, AeronArchive.NULL_LENGTH, commitCounterId,
                                          LOG_REPLAY_CHANNEL, LOG_REPLAY_STREAM_ID);
    }

    private Subscription logSubscription(final long sessionId) {
        return aeron.addSubscription(ChannelUri.addSessionId(LOG_REPLAY_CHANNEL, (int)sessionId), LOG_REPLAY_STREAM_ID);
    }

    /**
     * Starts the follower once each active queue recording has a first frame and a start explains it: the cluster
     * snapshot whose cut that frame is, else the log's first entry.
     */
    private void startFollower() {
        final Queue[] queues = new Queue[FrameLayer.MAX_GROUPS];
        int found = 0;
        for (int groupId = 1; groupId <= FrameLayer.MAX_GROUPS; groupId++) {
            final Queue listed = activeRecording(FrameLayer.queueStreamId(groupId), groupId);
            if (listed == null) {
                continue;
            }
            final long[] first = { Aeron.NULL_VALUE };
            final long tip = delegate.recordingPosition(listed.recordingId());
            if (tip <= listed.startPosition() ||
                delegate.scanQueueRecording(listed.recordingId(), listed.startPosition(), tip,
                                            (framePosition, clusterSeqNo) -> {
                                                first[0] = clusterSeqNo;
                                                return false;
                                            }) != ScanEnd.STOPPED) {
                return; // nothing recorded yet, or not readable now
            }
            queues[groupId - 1] = new Queue(groupId, listed.recordingId(), listed.startPosition(), listed.termLength(),
                                            listed.mtu(), first[0]);
            found++;
        }
        final Queue log = activeRecording(ConsensusModule.Configuration.logStreamId(), 0);
        final int commitCounterId = commitPositionCounterId();
        if (found == 0 || log == null || commitCounterId == CountersReader.NULL_COUNTER_ID) {
            return;
        }
        followerDecided = true;
        Checkpoint start = snapshotStart(queues);
        if (start == null && log.startPosition() == 0) {
            start =
                new Checkpoint(0, new LogDeriver.State(List.of(), new long[0]), false, new long[FrameLayer.MAX_GROUPS]);
        }
        if (start == null || start.logPosition() < log.startPosition()) {
            Logger.info(Logger.CoreComponent.ReplayerService, memberId,
                        "replays are not derived: the log's recording starts at %d and no cluster snapshot starts the "
                            + "queues' recordings",
                        log.startPosition());
            return;
        }
        try {
            follower = new Follower(queues, log.recordingId(), commitCounterId, start);
        } catch (final RuntimeException ex) {
            Logger.info(Logger.CoreComponent.ReplayerService, memberId, "replays are not derived: %s", ex.toString());
        }
    }

    /** The active recording of a stream, its first frame's {@code clusterSeqNo} unset. */
    private Queue activeRecording(final int recordedStreamId, final int groupId) {
        final Queue[] active = { null };
        archive.listRecordingsForUri(
            0, Integer.MAX_VALUE, "", recordedStreamId,
            (controlSessionId, correlationId, recordingId, startTimestamp, stopTimestamp, startPosition, stopPosition,
             initialTermId, segmentFileLength, termBufferLength, mtuLength, sessionId, streamId, strippedChannel,
             originalChannel, sourceIdentity) -> {
                if (stopTimestamp == AeronArchive.NULL_TIMESTAMP) {
                    active[0] =
                        new Queue(groupId, recordingId, startPosition, termBufferLength, mtuLength, Aeron.NULL_VALUE);
                }
            });
        return active[0];
    }

    /**
     * The cluster snapshot the member's service started from, as a derivation's start: the one whose cut is the first
     * frame of the queues' recordings, each queue standing at its recording's start. Null when there is none.
     */
    private Checkpoint snapshotStart(final Queue[] queues) {
        final long[] starts = new long[FrameLayer.MAX_GROUPS];
        long cut = Aeron.NULL_VALUE;
        for (final Queue queue : queues) {
            if (queue != null) {
                starts[queue.groupId() - 1] = queue.startPosition();
                cut = queue.firstClusterSeqNo();
            }
        }
        final LongArrayList snapshots = new LongArrayList();
        archive.listRecordingsForUri(
            0, Integer.MAX_VALUE, "", ClusteredServiceContainer.Configuration.snapshotStreamId(),
            (controlSessionId, correlationId, recordingId, startTimestamp, stopTimestamp, startPosition, stopPosition,
             initialTermId, segmentFileLength, termBufferLength, mtuLength, sessionId, streamId, strippedChannel,
             originalChannel, sourceIdentity) -> {
                if (stopTimestamp != AeronArchive.NULL_TIMESTAMP) {
                    snapshots.addLong(recordingId);
                }
            });
        snapshots.sort(Comparator.reverseOrder());
        final Sequencer.FrameSink nowhere = new Sequencer.FrameSink() {
            @Override
            public void activate(final int[] groupIds) {
            }

            @Override
            public void publish(final int groupId, final DirectBuffer buffer, final int length) {
            }
        };
        for (final long recordingId : snapshots) {
            try {
                final Checkpoint snapshot = readSnapshot(recordingId, starts);
                final LogDeriver probe = new LogDeriver(memberId, nowhere);
                probe.restore(snapshot.state(), false);
                if (probe.clusterSeqNo() == cut) {
                    return snapshot;
                }
            } catch (final RuntimeException ex) {
                Logger.info(Logger.CoreComponent.ReplayerService, memberId,
                            "cluster snapshot recording %d is no start for a derivation: %s", recordingId,
                            ex.toString());
            }
        }
        return null;
    }

    /**
     * Reads a service snapshot's recording: the log position it was taken at, the sessions open there, and the
     * {@link Sequencer}'s records between the container's own.
     */
    private Checkpoint readSnapshot(final long recordingId, final long[] queueStarts) {
        final MessageHeaderDecoder header = new MessageHeaderDecoder();
        final SnapshotMarkerDecoder marker = new SnapshotMarkerDecoder();
        final ClientSessionDecoder session = new ClientSessionDecoder();
        final List<byte[]> records = new ArrayList<>();
        final LongArrayList sessions = new LongArrayList();
        final long[] logPosition = { Aeron.NULL_VALUE };
        final boolean[] ended = { false };
        final FragmentHandler handler = new FragmentAssembler((buffer, offset, length, frameHeader) -> {
            header.wrap(buffer, offset);
            if (header.schemaId() != MessageHeaderDecoder.SCHEMA_ID) {
                final byte[] record = new byte[length];
                buffer.getBytes(offset, record);
                records.add(record);
            } else if (header.templateId() == SnapshotMarkerDecoder.TEMPLATE_ID) {
                marker.wrapAndApplyHeader(buffer, offset, header);
                logPosition[0] = marker.logPosition();
                ended[0] = marker.mark() == SnapshotMark.END;
            } else if (header.templateId() == ClientSessionDecoder.TEMPLATE_ID) {
                sessions.addLong(session.wrapAndApplyHeader(buffer, offset, header).clusterSessionId());
            }
        });
        final long sessionId =
            archive.startReplay(recordingId, 0, AeronArchive.NULL_LENGTH, LOG_REPLAY_CHANNEL, LOG_REPLAY_STREAM_ID);
        final long deadlineMs = delegate.monotonicMs() + SNAPSHOT_READ_TIMEOUT_MS;
        try (Subscription subscription = logSubscription(sessionId)) {
            while (!ended[0]) {
                if (subscription.poll(handler, FRAGMENT_LIMIT) == 0) {
                    if (delegate.monotonicMs() > deadlineMs) {
                        throw new IllegalStateException("its replay stalled before the snapshot's end");
                    }
                    Thread.yield();
                }
            }
        } finally {
            try {
                archive.stopReplay(sessionId);
            } catch (final RuntimeException ex) {
                // the archive has ended it already
            }
        }
        return new Checkpoint(logPosition[0], new LogDeriver.State(records, sessions.toLongArray()), true,
                              queueStarts.clone());
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

    /**
     * The derivation that follows the log from the member's start to its commit position, checks that it places the
     * first frame of each queue's recording where the recording holds it, and keeps the checkpoints.
     */
    private final class Follower implements Sequencer.FrameSink {
        private final Queue[] queues;
        private final long logRecordingId;
        private final int commitCounterId;
        private final List<Checkpoint> checkpoints = new ArrayList<>();
        private final long[] queuePositions;
        private final long logSessionId;
        private final Subscription log;
        private final LogDeriver deriver = new LogDeriver(memberId, this);
        private final SequencedFrameDecoder frame = new SequencedFrameDecoder();
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
        /** The queues whose recording's first frame is still to be derived. */
        private int unplaced;
        private boolean misplaced;

        Follower(final Queue[] queues, final long logRecordingId, final int commitCounterId, final Checkpoint start) {
            this.queues = queues;
            this.logRecordingId = logRecordingId;
            this.commitCounterId = commitCounterId;
            this.queuePositions = start.queuePositions().clone();
            for (final Queue queue : queues) {
                unplaced += queue == null ? 0 : 1;
            }
            checkpoints.add(start);
            logSessionId = replayLog(logRecordingId, start.logPosition(), commitCounterId);
            log = logSubscription(logSessionId);
            deriver.restore(start.state(), start.reemitMarker());
        }

        /** Whether every queue's first recorded frame has been derived, each at its recording's position. */
        boolean isValid() {
            return unplaced == 0 && !misplaced;
        }

        Queue queueOf(final long recordingId) {
            for (final Queue queue : queues) {
                if (queue != null && queue.recordingId() == recordingId) {
                    return queue;
                }
            }
            return null;
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

        private boolean checkpointDue() {
            return isValid() && entryEnd - checkpoints.getLast().logPosition() >= CHECKPOINT_LOG_BYTES;
        }

        int doWork() {
            final Image image = misplaced ? null : log.imageBySessionId((int)logSessionId);
            if (image == null) {
                return 0;
            }
            final int fragments = image.controlledPoll(handler, FOLLOW_FRAGMENT_LIMIT);
            if (checkpointDue() && entryEnd == image.position()) {
                checkpoints.add(new Checkpoint(entryEnd, deriver.state(), false, queuePositions.clone()));
            }
            return fragments;
        }

        @Override
        public void activate(final int[] groupIds) {
        }

        @Override
        public void publish(final int groupId, final DirectBuffer buffer, final int length) {
            final Queue queue = queues[groupId - 1];
            if (queue == null) {
                return;
            }
            final long start =
                QueuePositions.frameStart(queuePositions[groupId - 1], length, queue.termLength(), queue.mtu());
            queuePositions[groupId - 1] = start + QueuePositions.framedLength(length, queue.mtu());
            if (unplaced == 0 || misplaced || !frame.wrap(buffer, 0, length) ||
                frame.clusterSeqNo() != queue.firstClusterSeqNo()) {
                return;
            }
            unplaced--;
            if (start != queue.startPosition()) {
                misplaced = true;
                Logger.info(Logger.CoreComponent.ReplayerService, memberId,
                            "replays are not derived: the log puts clusterSeqNo %d of group %d's queue at %d, and its "
                                + "recording starts with it at %d",
                            frame.clusterSeqNo(), groupId, start, queue.startPosition());
            }
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

    @Override
    public List<RecordingSpan> listQueueRecordings(final int groupId) {
        return delegate.listQueueRecordings(groupId);
    }

    /** Derived from the last checkpoint at or before {@code fromPosition}, once replays are. */
    @Override
    public ScanEnd scanQueueRecording(final long recordingId, final long fromPosition, final long endPosition,
                                      final FrameVisitor visitor) {
        final Queue queue = follower != null && follower.isValid() ? follower.queueOf(recordingId) : null;
        final Checkpoint from = queue == null ? null : follower.checkpointAt(queue.groupId(), fromPosition);
        if (from == null) {
            return delegate.scanQueueRecording(recordingId, fromPosition, endPosition, visitor);
        }
        final long logSessionId;
        try {
            logSessionId = replayLog(follower.logRecordingId, from.logPosition(), follower.commitCounterId);
        } catch (final RuntimeException ex) {
            Logger.info(Logger.CoreComponent.ReplayerService, memberId,
                        "group %d's scan from %d is not derived, the log does not replay from %d: %s", queue.groupId(),
                        fromPosition, from.logPosition(), ex.getMessage());
            return delegate.scanQueueRecording(recordingId, fromPosition, endPosition, visitor);
        }
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

    /**
     * Where the derivation has brought the queue, once replays are derived and while its recording is being written:
     * the committed log's, which the recording reaches.
     */
    @Override
    public long recordingPosition(final long recordingId) {
        final long recorded = delegate.recordingPosition(recordingId);
        final Queue queue =
            recorded >= 0 && follower != null && follower.isValid() ? follower.queueOf(recordingId) : null;
        return queue == null ? recorded : follower.queuePositions[queue.groupId() - 1];
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
