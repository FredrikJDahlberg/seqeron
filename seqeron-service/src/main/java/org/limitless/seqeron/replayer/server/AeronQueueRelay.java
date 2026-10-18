package org.limitless.seqeron.replayer.server;

import static io.aeron.Aeron.NULL_VALUE;

import io.aeron.Aeron;
import io.aeron.ControlledFragmentAssembler;
import io.aeron.ExclusivePublication;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.archive.status.RecordingPos;
import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import io.aeron.logbuffer.Header;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.NoOpLock;
import org.agrona.concurrent.status.CountersReader;
import org.limitless.seqeron.protocol.FrameLayer;
import org.limitless.seqeron.protocol.PortLayout;
import org.limitless.seqeron.protocol.SequencedFrameDecoder;
import org.limitless.seqeron.util.Clocks;
import org.limitless.seqeron.util.Logger;

/**
 * The Aeron adapter for {@link QueueRelay}: on a gateway host, reads a member's recording of one group's queue
 * over UDP with one replay that follows it live, and republishes each frame onto this host's own queue of the
 * same stream, which the local archive records and {@link ReplayerService} serves exactly as it does on a
 * member. Nothing on the member changes: its archive already takes remote clients, and a replay costs its
 * sequencer nothing. A host runs one per group it relays, all on one thread ({@link #run}).
 *
 * <p>A back-pressured local queue is not spun on: the fragment is aborted and offered again next cycle, and
 * the member's replay waits behind it under Aeron's flow control.
 */
final class AeronQueueRelay implements AutoCloseable {
    /** Stream the member's archive replays to this relay on, on an ephemeral port of its own. */
    static final int REPLAY_STREAM_ID = 206;

    /** How long one request to a member's archive may go unanswered; a dead member costs this much. */
    private static final long ARCHIVE_MESSAGE_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(1);

    /** How long the local archive may take to attach to the local queue. */
    private static final long RECORDING_START_TIMEOUT_MS = 10_000;

    /** How long the driver may take to bind the replay subscription's ephemeral port. */
    private static final long RESOLVE_TIMEOUT_MS = 1_000;

    private static final long RECORDING_CHECK_INTERVAL_MS = 1_000;

    private static final int FRAGMENT_LIMIT = 64;

    private final Aeron aeron;
    private final AeronArchive localArchive;
    private final List<String> archiveEndpoints;
    private final String host;
    private final int nodeId;
    private final int groupId;
    private final int streamId;
    private final Queue<byte[]> topologyRows;
    private final IdleStrategy idleStrategy;
    private final Runnable fatalHandler;
    private final QueueRelay relay;
    private final ExclusivePublication queue;
    private final CountersReader counters;
    private final int recordingCounterId;
    private final long recordingId;

    private final SequencedFrameDecoder view = new SequencedFrameDecoder();
    private final ControlledFragmentAssembler assembler = new ControlledFragmentAssembler(this::onFragment);

    private AeronArchive source;
    private Subscription replay;
    private long replaySessionId = NULL_VALUE;
    private boolean imageSeen;
    private QueueRelay.Verdict pending;
    private long nowMs;
    private long nextRecordingCheckMs;
    private boolean fatal;

    /**
     * Creates the local queue and starts the local archive recording it. The local archive client is used
     * here and in {@link #close} only, both on the launching thread while no duty cycle runs.
     *
     * @param aeron            this host's own media driver
     * @param localArchive     this host's own archive
     * @param archiveEndpoints the members' archive control endpoints, {@code host:port}, tried in order
     * @param host             this host's name as the members reach it
     * @param nodeId           this host's id, for the log
     * @param groupId          the group whose queue this relays
     * @param topologyRows     where to hand a copy of each topology row it publishes, or {@code null}: the
     *                         relay of the host's default queue feeds its {@link ReplayerService}
     * @param idleStrategy     what a wait on the driver or the archive does between checks
     * @param fatalHandler     run once, from the duty cycle, when the relay can no longer keep the local
     *                         history; it must signal a shutdown and return
     */
    AeronQueueRelay(final Aeron aeron, final AeronArchive localArchive, final List<String> archiveEndpoints,
                    final String host, final int nodeId, final int groupId, final Queue<byte[]> topologyRows,
                    final IdleStrategy idleStrategy, final Runnable fatalHandler) {
        this.aeron = aeron;
        this.localArchive = localArchive;
        this.archiveEndpoints = archiveEndpoints;
        this.host = host;
        this.nodeId = nodeId;
        this.groupId = groupId;
        this.streamId = FrameLayer.queueStreamId(groupId);
        this.topologyRows = topologyRows;
        this.idleStrategy = idleStrategy;
        this.fatalHandler = fatalHandler;
        this.relay = new QueueRelay(archiveEndpoints.size());
        this.counters = aeron.countersReader();

        queue = aeron.addExclusivePublication(FrameLayer.QUEUE_CHANNEL, streamId);
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
        recordingCounterId = counterId;
        recordingId = RecordingPos.getRecordingId(counters, counterId);
    }

    /**
     * Runs every relay's duty cycle on the calling thread until {@code running} goes false or one gives up. An
     * uncaught exception brings the process down like a lost recording does: supervision restarts it.
     *
     * @param relays       one per relayed group
     * @param running      true while running
     * @param idleStrategy the shared duty cycle's
     */
    static void run(final List<AeronQueueRelay> relays, final AtomicBoolean running, final IdleStrategy idleStrategy) {
        for (final AeronQueueRelay relay : relays) {
            Logger.info(Logger.CoreComponent.QueueRelay, relay.nodeId,
                        "relaying group %d's queue from member archives %s onto local recording %d", relay.groupId,
                        relay.archiveEndpoints, relay.recordingId);
        }
        AeronQueueRelay current = null;
        try {
            while (running.get()) {
                int work = 0;
                for (final AeronQueueRelay relay : relays) {
                    current = relay;
                    work += relay.doWork();
                    if (relay.fatal) {
                        return;
                    }
                }
                idleStrategy.idle(work);
            }
        } catch (final RuntimeException ex) {
            current.fatal = true;
            Logger.fault(Logger.CoreComponent.QueueRelay, Logger.CoreEventCode.ReplayDutyCycleFailure, current.nodeId,
                         "FATAL: group %d's relay duty cycle terminated by an uncaught exception (%s)", current.groupId,
                         ex.toString());
            current.fatalHandler.run();
        }
    }

    /** One duty-cycle iteration; returns a work count for the idle strategy. */
    int doWork() {
        nowMs = Clocks.monotonicMs();
        checkRecording();
        if (fatal) {
            return 0;
        }
        if (source == null) {
            if (relay.mayAttempt(nowMs)) {
                connect();
                return 1;
            }
            return 0;
        }
        // The member's archive pings this session once a second and closes it once those go unread.
        final String archiveError = source.pollForErrorResponse();
        if (archiveError != null) {
            dropSource("its archive's control session failed: " + archiveError);
            return 1;
        }

        final int work = replay.controlledPoll(assembler, FRAGMENT_LIMIT);
        if (fatal) {
            return work;
        }
        if (pending == QueueRelay.Verdict.REPLAY_FROM_START) {
            pending = null;
            restartReplay();
        } else if (pending == QueueRelay.Verdict.DROP_SOURCE) {
            pending = null;
            dropSource("its queue recording has a hole where this relay needs groupSeqNo " +
                       (relay.lastGroupSeqNo() + 1));
        } else if (work == 0) {
            imageSeen |= replay.imageCount() > 0;
            if (imageSeen && replay.imageCount() == 0) {
                dropSource("its replay ended");
            } else if (relay.isSourceStalled(nowMs)) {
                dropSource("nothing received for " + QueueRelay.SOURCE_TIMEOUT_MS + "ms");
            }
        }
        return work;
    }

    @Override
    public void close() {
        closeSource();
        CloseHelper.quietClose(() -> localArchive.stopRecording(FrameLayer.QUEUE_CHANNEL, streamId));
        CloseHelper.quietClose(queue);
    }

    private Action onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header) {
        final QueueRelay.Verdict verdict =
            view.wrap(buffer, offset, length) ? relay.onFrame(view.groupSeqNo(), nowMs) : relay.onUnreadable();
        switch (verdict) {
        case SKIP:
            return Action.CONTINUE;
        case PUBLISH:
            return publish(buffer, offset, length, header);
        default:
            pending = verdict;
            return Action.BREAK;
        }
    }

    private Action publish(final DirectBuffer buffer, final int offset, final int length, final Header header) {
        final long result = queue.offer(buffer, offset, length);
        if (result > 0) {
            relay.onPublished(view.groupSeqNo(), header.position());
            if (topologyRows != null && QueueResolver.isTopologyRow(buffer, offset, length)) {
                final byte[] row = new byte[length];
                buffer.getBytes(offset, row);
                topologyRows.add(row);
            }
            return Action.CONTINUE;
        }
        if (result == Publication.CLOSED || result == Publication.MAX_POSITION_EXCEEDED) {
            fatal("the local queue refused groupSeqNo " + view.groupSeqNo() + " for good (" + result + ")");
            return Action.BREAK;
        }
        if (relay.onBackPressured(nowMs, counters.getCounterValue(recordingCounterId))) {
            fatal("the local recording made no progress for " + QueueRelay.RECORDING_STALL_FATAL_MS +
                  "ms of back-pressure");
            return Action.BREAK;
        }
        return Action.ABORT;
    }

    /** A stopped local recording back-pressures nothing, so it is checked for rather than waited on. */
    private void checkRecording() {
        if (fatal || nowMs < nextRecordingCheckMs) {
            return;
        }
        nextRecordingCheckMs = nowMs + RECORDING_CHECK_INTERVAL_MS;
        if (!RecordingPos.isActive(counters, recordingCounterId, recordingId)) {
            fatal("the local archive stopped recording the queue (recording " + recordingId + ")");
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

    private void restartReplay() {
        closeReplay();
        try {
            startReplay();
        } catch (final RuntimeException ex) {
            dropSource(ex.getMessage());
        }
    }

    /** Replays the member's active queue recording from where {@link QueueRelay#replayFrom} says, following it live. */
    private void startReplay() {
        // The newest recording still recording; an unclean shutdown can leave an older one looking active too.
        final long[] newest = { NULL_VALUE, 0 };
        source.listRecordingsForUri(0, Integer.MAX_VALUE, "", streamId,
                                    (controlSessionId, correlationId, id, startTimestamp, stopTimestamp, start,
                                     stopPosition, initialTermId, segmentFileLength, termBufferLength, mtuLength,
                                     sessionId, streamId, strippedChannel, originalChannel, sourceIdentity) -> {
                                        if (stopTimestamp == AeronArchive.NULL_TIMESTAMP && id > newest[0]) {
                                            newest[0] = id;
                                            newest[1] = start;
                                        }
                                    });
        final long activeId = newest[0];
        final long startPosition = newest[1];
        if (activeId == NULL_VALUE) {
            dropSource("it is not recording group " + groupId + "'s queue");
            return;
        }

        final long from = relay.replayFrom(startPosition, source.getRecordingPosition(activeId), nowMs);
        if (from == QueueRelay.NOT_SUITABLE) {
            dropSource("its recording is behind this relay's groupSeqNo " + relay.lastGroupSeqNo());
            return;
        }

        replay = aeron.addSubscription("aeron:udp?endpoint=" + host + ":0", REPLAY_STREAM_ID);
        imageSeen = false;
        final String channel = resolvedChannel(replay);
        replaySessionId =
            source.startReplay(activeId, from, AeronArchive.REPLAY_ALL_AND_FOLLOW, channel, REPLAY_STREAM_ID);
        Logger.info(Logger.CoreComponent.QueueRelay, nodeId,
                    "following member archive %s: group %d's recording %d from position %d, after groupSeqNo %d",
                    archiveEndpoints.get(relay.sourceIndex()), groupId, activeId, from, relay.lastGroupSeqNo());
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
                     "group %d's relay left member archive %s: %s", groupId, archiveEndpoints.get(relay.sourceIndex()),
                     reason);
        closeSource();
        relay.onSourceLost(nowMs);
    }

    private void closeReplay() {
        if (replaySessionId != NULL_VALUE && source != null) {
            final long sessionId = replaySessionId;
            CloseHelper.quietClose(() -> source.stopReplay(sessionId));
        }
        replaySessionId = NULL_VALUE;
        CloseHelper.quietClose(replay);
        replay = null;
    }

    private void closeSource() {
        closeReplay();
        CloseHelper.quietClose(source);
        source = null;
    }

    private void fatal(final String reason) {
        fatal = true;
        Logger.fault(Logger.CoreComponent.QueueRelay, Logger.CoreEventCode.QueueRecordingFailure, nodeId,
                     "FATAL: %s at group %d's groupSeqNo=%d, so this host can no longer keep the history it serves",
                     reason, groupId, relay.lastGroupSeqNo());
        fatalHandler.run();
    }
}
