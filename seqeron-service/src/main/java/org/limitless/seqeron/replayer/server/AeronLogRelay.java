package org.limitless.seqeron.replayer.server;

import static io.aeron.Aeron.NULL_VALUE;

import io.aeron.Aeron;
import io.aeron.ControlledFragmentAssembler;
import io.aeron.ExclusivePublication;
import io.aeron.FragmentAssembler;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveException;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.archive.status.RecordingPos;
import io.aeron.cluster.ConsensusModule;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import io.aeron.logbuffer.FrameDescriptor;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.NoOpLock;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;
import org.limitless.seqeron.protocol.FrameLayer;
import org.limitless.seqeron.protocol.PortLayout;
import org.limitless.seqeron.protocol.SequencedFrameDecoder;
import org.limitless.seqeron.protocol.SystemFrame;
import org.limitless.seqeron.sbe.frame.SnapshotStartedDecoder;
import org.limitless.seqeron.sequencer.LogDeriver;
import org.limitless.seqeron.sequencer.Sequencer;
import org.limitless.seqeron.util.Clocks;
import org.limitless.seqeron.util.Logger;

/**
 * The Aeron adapter for {@link LogRelay}: on a gateway host whose members record no queue, reads a member's Raft log
 * over UDP with one replay bounded by the member's commit position, runs a {@link LogDeriver} over it, and publishes
 * the frames of each relayed group onto this host's own queue, which the local archive records and
 * {@link ReplayerService} serves (doc/disk-load-plan.md, step 2a). It does for every relayed group what an
 * {@link AeronQueueRelay} does for one.
 *
 * <p>The derivation starts where the first member read says its service did ({@link DerivationAnnouncement}), so the
 * host's queues start at {@code groupSeqNo} 1 or at that member's cut. A frame a local queue does not take is kept and
 * offered again next cycle, and the log is not read while one is kept.
 */
final class AeronLogRelay implements AutoCloseable, Sequencer.FrameSink {
    private static final long ARCHIVE_MESSAGE_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(1);
    private static final long RECORDING_START_TIMEOUT_MS = 10_000;
    private static final long RESOLVE_TIMEOUT_MS = 1_000;
    private static final long ANNOUNCEMENT_TIMEOUT_MS = 1_000;
    private static final long RECORDING_CHECK_INTERVAL_MS = 1_000;
    private static final int FRAGMENT_LIMIT = 64;

    /** One relayed group's queue on this host. */
    private record LocalQueue(int groupId, ExclusivePublication publication, int recordingCounterId, long recordingId) {
    }

    /** A frame a local queue has not taken yet. */
    private record Pending(LocalQueue queue, byte[] frame) { }

    private final Aeron aeron;
    private final AeronArchive localArchive;
    private final List<String> archiveEndpoints;
    private final String host;
    private final int nodeId;
    private final Queue<byte[]> topologyRows;
    private final Queue<RelayedCut> cutReports;
    private final IdleStrategy idleStrategy;
    private final Runnable fatalHandler;
    private final LogRelay relay;
    private final CountersReader counters;
    private final LocalQueue[] queues = new LocalQueue[FrameLayer.MAX_GROUPS];
    private final int defaultGroupId;
    private final LogDeriver deriver;
    private final ArrayDeque<Pending> pending = new ArrayDeque<>();
    private final UnsafeBuffer pendingBuffer = new UnsafeBuffer();
    private final SequencedFrameDecoder view = new SequencedFrameDecoder();
    private final SnapshotStartedDecoder marker = new SnapshotStartedDecoder();

    private AeronArchive source;
    private Subscription replay;
    private long replaySessionId = NULL_VALUE;
    private ControlledFragmentHandler handler;
    private boolean imageSeen;
    private boolean restored;
    private long nowMs;
    private long nextRecordingCheckMs;
    private boolean fatal;

    /**
     * Creates the local queues and starts the local archive recording them, as {@link AeronQueueRelay} does for one.
     *
     * @param groups       the groups whose queues this relays, ascending: the first is the host's default queue
     * @param topologyRows where to hand a copy of each topology row the default queue carries
     * @param cutReports   where to report each snapshot marker once it is on a local queue
     */
    AeronLogRelay(final Aeron aeron, final AeronArchive localArchive, final List<String> archiveEndpoints,
                  final String host, final int nodeId, final List<Integer> groups, final Queue<byte[]> topologyRows,
                  final Queue<RelayedCut> cutReports, final IdleStrategy idleStrategy, final Runnable fatalHandler,
                  final long stallTimeoutMs) {
        this.aeron = aeron;
        this.localArchive = localArchive;
        this.archiveEndpoints = archiveEndpoints;
        this.host = host;
        this.nodeId = nodeId;
        this.topologyRows = topologyRows;
        this.cutReports = cutReports;
        this.idleStrategy = idleStrategy;
        this.fatalHandler = fatalHandler;
        this.relay = new LogRelay(archiveEndpoints.size(), stallTimeoutMs);
        this.counters = aeron.countersReader();
        this.defaultGroupId = groups.getFirst();
        this.deriver = new LogDeriver(nodeId, this);
        for (final int groupId : groups) {
            queues[groupId - 1] = openQueue(groupId);
        }
    }

    private LocalQueue openQueue(final int groupId) {
        final int streamId = FrameLayer.queueStreamId(groupId);
        final ExclusivePublication queue = aeron.addExclusivePublication(FrameLayer.QUEUE_CHANNEL, streamId);
        localArchive.startRecording(FrameLayer.QUEUE_CHANNEL, streamId, SourceLocation.LOCAL);
        final long deadlineMs = Clocks.monotonicMs() + RECORDING_START_TIMEOUT_MS;
        int counterId;
        while ((counterId = RecordingPos.findCounterIdBySession(
                    counters, queue.sessionId(), localArchive.archiveId())) == CountersReader.NULL_COUNTER_ID) {
            if (Clocks.monotonicMs() >= deadlineMs) {
                throw new IllegalStateException("the local archive did not start recording group " + groupId +
                                                "'s queue within " + RECORDING_START_TIMEOUT_MS + "ms");
            }
            idleStrategy.idle();
        }
        final long recordingId = RecordingPos.getRecordingId(counters, counterId);
        // An earlier run's recordings: this one starts again from a member's start (doc/snapshot.md §10.3).
        final List<Long> older = new ArrayList<>();
        localArchive.listRecordingsForUri(0, Integer.MAX_VALUE, "", streamId,
                                          (controlSessionId, correlationId, id, startTimestamp, stopTimestamp,
                                           startPosition, stopPosition, initialTermId, segmentFileLength,
                                           termBufferLength, mtuLength, sessionId, stream, strippedChannel,
                                           originalChannel, sourceIdentity) -> {
                                              if (id < recordingId) {
                                                  older.add(id);
                                              }
                                          });
        for (final long id : older) {
            try {
                localArchive.purgeRecording(id);
            } catch (final ArchiveException ex) {
                Logger.error(Logger.CoreComponent.QueueRelay, Logger.CoreEventCode.ServiceError, nodeId,
                             "could not purge group %d's earlier queue recording %d: %s", groupId, id, ex.getMessage());
            }
        }
        return new LocalQueue(groupId, queue, counterId, recordingId);
    }

    /**
     * Runs the duty cycle on the calling thread until {@code running} goes false or the relay gives up. An uncaught
     * exception brings the process down like a lost recording does.
     */
    void run(final AtomicBoolean running, final IdleStrategy idle) {
        Logger.info(Logger.CoreComponent.QueueRelay, nodeId,
                    "deriving the local queues from the Raft log of member archives %s", archiveEndpoints);
        try {
            while (running.get() && !fatal) {
                idle.idle(doWork());
            }
        } catch (final RuntimeException ex) {
            fatal = true;
            Logger.fault(Logger.CoreComponent.QueueRelay, Logger.CoreEventCode.ReplayDutyCycleFailure, nodeId,
                         "FATAL: the log relay's duty cycle terminated by an uncaught exception (%s)", ex.toString());
            fatalHandler.run();
        }
    }

    int doWork() {
        nowMs = Clocks.monotonicMs();
        checkRecordings();
        int work = 0;
        while (!fatal && !pending.isEmpty()) {
            final Pending next = pending.peek();
            pendingBuffer.wrap(next.frame());
            if (!offer(next.queue(), pendingBuffer, next.frame().length)) {
                return work;
            }
            pending.poll();
            work++;
        }
        if (fatal) {
            return work;
        }
        if (source == null) {
            if (relay.mayAttempt(nowMs)) {
                connect();
                return work + 1;
            }
            return work;
        }
        // The member's archive pings this session once a second and closes it once those go unread.
        final String archiveError = source.pollForErrorResponse();
        if (archiveError != null) {
            dropSource("its archive's control session failed: " + archiveError);
            return work + 1;
        }
        final int fragments = replay.controlledPoll(handler, FRAGMENT_LIMIT);
        if (fragments == 0 && !fatal) {
            imageSeen |= replay.imageCount() > 0;
            if (imageSeen && replay.imageCount() == 0) {
                dropSource("its replay ended");
            } else if (relay.isSourceStalled(nowMs)) {
                dropSource("nothing received for " + LogRelay.SOURCE_TIMEOUT_MS + "ms");
            }
        }
        return work + fragments;
    }

    @Override
    public void activate(final int[] groupIds) {
    }

    @Override
    public void publish(final int groupId, final DirectBuffer buffer, final int length) {
        final LocalQueue queue = queues[groupId - 1];
        if (queue == null || fatal) {
            return;
        }
        if (!pending.isEmpty() || !offer(queue, buffer, length)) {
            final byte[] frame = new byte[length];
            buffer.getBytes(0, frame);
            pending.add(new Pending(queue, frame));
        }
    }

    /** @return whether the queue took the frame */
    private boolean offer(final LocalQueue queue, final DirectBuffer buffer, final int length) {
        final long result = queue.publication().offer(buffer, 0, length);
        if (result > 0) {
            relay.onOffered();
            if (!view.wrap(buffer, 0, length)) {
                return true;
            }
            if (queue.groupId() == defaultGroupId && QueueResolver.isTopologyRow(buffer, 0, length)) {
                final byte[] row = new byte[length];
                buffer.getBytes(0, row);
                topologyRows.add(row);
            }
            if (view.isSystem() && view.systemEventType() == SystemFrame.SNAPSHOT_STARTED) {
                marker.wrap(view.buffer(), view.payloadOffset(), SnapshotStartedDecoder.BLOCK_LENGTH,
                            SnapshotStartedDecoder.SCHEMA_VERSION);
                cutReports.add(new RelayedCut(marker.round(), view.clusterSeqNo(), queue.groupId(),
                                              RelayedCut.startOf(result, length), relay.isLive()));
            }
            return true;
        }
        if (result == Publication.CLOSED || result == Publication.MAX_POSITION_EXCEEDED) {
            fatal("group " + queue.groupId() + "'s local queue refused a frame for good (" + result + ")");
        } else if (result != Publication.ADMIN_ACTION &&
                   relay.onBackPressured(nowMs, counters.getCounterValue(queue.recordingCounterId()))) {
            fatal("group " + queue.groupId() + "'s local recording made no progress for " + relay.stallTimeoutMs() +
                  "ms of back-pressure");
        }
        return false;
    }

    /** A stopped local recording back-pressures nothing, so it is checked for rather than waited on. */
    private void checkRecordings() {
        if (fatal || nowMs < nextRecordingCheckMs) {
            return;
        }
        nextRecordingCheckMs = nowMs + RECORDING_CHECK_INTERVAL_MS;
        for (final LocalQueue queue : queues) {
            if (queue != null && !RecordingPos.isActive(counters, queue.recordingCounterId(), queue.recordingId())) {
                fatal("the local archive stopped recording group " + queue.groupId() + "'s queue (recording " +
                      queue.recordingId() + ")");
                return;
            }
        }
    }

    private void connect() {
        final String endpoint = archiveEndpoints.get(relay.sourceIndex());
        try {
            source = AeronArchive.connect(new AeronArchive.Context()
                                              .aeron(aeron)
                                              .ownsAeronClient(false)
                                              .controlRequestChannel("aeron:udp?endpoint=" + endpoint)
                                              .controlRequestStreamId(PortLayout.ARCHIVE_CONTROL_STREAM_ID)
                                              .controlResponseChannel("aeron:udp?endpoint=" + host + ":0")
                                              .controlResponseStreamId(NodeDriver.RELAY_ARCHIVE_RESPONSE_STREAM_ID)
                                              .messageTimeoutNs(ARCHIVE_MESSAGE_TIMEOUT_NS)
                                              .lock(NoOpLock.INSTANCE));
            startReplay();
        } catch (final RuntimeException ex) {
            dropSource(ex.getMessage());
        }
    }

    /** Replays the member's log from where {@link LogRelay#replayFrom} says, bounded by its commit position. */
    private void startReplay() {
        final DerivationAnnouncement announcement = readAnnouncement();
        if (announcement == null) {
            dropSource("it announces no derivation start: it records its queues, or has not started");
            return;
        }
        final long[] log = activeRecording(ConsensusModule.Configuration.logStreamId());
        if (log == null) {
            dropSource("it is not recording a log");
            return;
        }
        final DerivationStart start = announcement.start();
        final long from = relay.replayFrom(start.logPosition(), log[1], source.getRecordingPosition(log[0]), nowMs);
        if (from == LogRelay.HISTORY_LOST) {
            closeSource();
            fatal("every member archive's log recording starts past position " + relay.position() +
                  ", which they trimmed while this relay was away; a restart derives from a member's start into new "
                  + "recordings");
            return;
        }
        if (from == LogRelay.NOT_SUITABLE) {
            dropSource("its log's recording, from " + log[1] + ", does not hold position " +
                       (relay.position() == LogRelay.NOT_SUITABLE ? start.logPosition() : relay.position()));
            return;
        }
        if (!restored) {
            restored = true;
            deriver.restore(start.state(), start.restored());
            deriver.replayTopologyRows(topologyRows::add);
        }
        replay = aeron.addSubscription("aeron:udp?endpoint=" + host + ":0", AeronQueueRelay.REPLAY_STREAM_ID);
        imageSeen = false;
        final ControlledFragmentAssembler entries =
            new ControlledFragmentAssembler((buffer, offset, length, header) -> {
                deriver.onLogEntry(buffer, offset, length);
                return Action.CONTINUE;
            });
        handler = (buffer, offset, length, header) -> {
            final boolean ends = (header.flags() & FrameDescriptor.END_FRAG_FLAG) != 0;
            if (ends) {
                relay.onEntry(header.position(), nowMs);
            }
            entries.onFragment(buffer, offset, length, header);
            return ends && !pending.isEmpty() ? Action.BREAK : Action.CONTINUE;
        };
        replaySessionId =
            source.startBoundedReplay(log[0], from, AeronArchive.NULL_LENGTH, announcement.commitCounterId(),
                                      resolvedChannel(replay), AeronQueueRelay.REPLAY_STREAM_ID);
        Logger.info(Logger.CoreComponent.QueueRelay, nodeId,
                    "following member archive %s: its log's recording %d from position %d",
                    archiveEndpoints.get(relay.sourceIndex()), log[0], from);
    }

    /** The member's last announcement off its newest active recording of them, or null. */
    private DerivationAnnouncement readAnnouncement() {
        final long[] recording = activeRecording(DerivationAnnouncement.STREAM_ID);
        if (recording == null) {
            return null;
        }
        final long length = source.getRecordingPosition(recording[0]) - recording[1];
        if (length <= 0) {
            return null;
        }
        final List<byte[]> messages = new ArrayList<>();
        final FragmentAssembler assembler = new FragmentAssembler((buffer, offset, messageLength, header) -> {
            final byte[] message = new byte[messageLength];
            buffer.getBytes(offset, message);
            messages.add(message);
        });
        try (Subscription subscription =
                 aeron.addSubscription("aeron:udp?endpoint=" + host + ":0", AeronQueueRelay.REPLAY_STREAM_ID)) {
            final long sessionId = source.startReplay(recording[0], recording[1], length, resolvedChannel(subscription),
                                                      AeronQueueRelay.REPLAY_STREAM_ID);
            final long deadlineMs = Clocks.monotonicMs() + ANNOUNCEMENT_TIMEOUT_MS;
            final long end = recording[1] + length;
            while ((subscription.imageCount() == 0 || subscription.imageAtIndex(0).position() < end) &&
                   Clocks.monotonicMs() < deadlineMs) {
                if (subscription.poll(assembler, FRAGMENT_LIMIT) == 0) {
                    idleStrategy.idle();
                }
            }
            CloseHelper.quietClose(() -> source.stopReplay(sessionId));
            return DerivationAnnouncement.decode(messages);
        }
    }

    /** The id and the start position of the member's newest active recording of a stream, or null. */
    private long[] activeRecording(final int streamId) {
        final long[][] newest = { null };
        source.listRecordingsForUri(0, Integer.MAX_VALUE, "", streamId,
                                    (controlSessionId, correlationId, id, startTimestamp, stopTimestamp, start,
                                     stopPosition, initialTermId, segmentFileLength, termBufferLength, mtuLength,
                                     sessionId, stream, strippedChannel, originalChannel, sourceIdentity) -> {
                                        if (stopTimestamp == AeronArchive.NULL_TIMESTAMP &&
                                            (newest[0] == null || id > newest[0][0])) {
                                            newest[0] = new long[] { id, start };
                                        }
                                    });
        return newest[0];
    }

    /** The subscription's channel with the port the driver bound, which is where the member replays to. */
    private String resolvedChannel(final Subscription subscription) {
        final long deadlineMs = Clocks.monotonicMs() + RESOLVE_TIMEOUT_MS;
        String channel;
        while ((channel = subscription.tryResolveChannelEndpointPort()) == null) {
            if (Clocks.monotonicMs() >= deadlineMs) {
                throw new IllegalStateException("the driver did not bind the replay subscription within " +
                                                RESOLVE_TIMEOUT_MS + "ms");
            }
            idleStrategy.idle();
        }
        return channel;
    }

    private void dropSource(final String reason) {
        Logger.error(Logger.CoreComponent.QueueRelay, Logger.CoreEventCode.RelaySourceLost, nodeId,
                     "the log relay left member archive %s: %s", archiveEndpoints.get(relay.sourceIndex()), reason);
        closeSource();
        relay.onSourceLost(nowMs);
    }

    private void closeSource() {
        if (replaySessionId != NULL_VALUE && source != null) {
            final long sessionId = replaySessionId;
            CloseHelper.quietClose(() -> source.stopReplay(sessionId));
        }
        replaySessionId = NULL_VALUE;
        CloseHelper.quietClose(replay);
        replay = null;
        CloseHelper.quietClose(source);
        source = null;
    }

    private void fatal(final String reason) {
        fatal = true;
        Logger.fault(Logger.CoreComponent.QueueRelay, Logger.CoreEventCode.QueueRecordingFailure, nodeId,
                     "FATAL: %s at log position %d, so this host can no longer keep the history it serves", reason,
                     relay.position());
        fatalHandler.run();
    }

    @Override
    public void close() {
        closeSource();
        for (final LocalQueue queue : queues) {
            if (queue != null) {
                CloseHelper.quietClose(()
                                           -> localArchive.stopRecording(FrameLayer.QUEUE_CHANNEL,
                                                                         FrameLayer.queueStreamId(queue.groupId())));
                CloseHelper.quietClose(queue.publication());
            }
        }
    }
}
