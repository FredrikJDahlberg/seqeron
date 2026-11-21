package org.limitless.seqeron.replayer.server;

import io.aeron.Aeron;
import io.aeron.ChannelUri;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ConsensusModule;
import io.aeron.logbuffer.BlockHandler;
import java.io.File;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.NoOpLock;
import org.agrona.concurrent.status.CountersReader;
import org.limitless.seqeron.protocol.FrameLayer;
import org.limitless.seqeron.protocol.PortLayout;
import org.limitless.seqeron.sequencer.DerivationStart;
import org.limitless.seqeron.sequencer.LogDeriver;
import org.limitless.seqeron.sequencer.NodeDriver;
import org.limitless.seqeron.util.Clocks;
import org.limitless.seqeron.util.Logger;

/**
 * Publishes a member's queues from its committed log on a thread of its own, beside the queues its service publishes,
 * on streams of their own (doc/plans/queue-off-duty-cycle-plan.md, step 2): a run that compares the two, frame for
 * frame and in latency, before the service stops publishing. Off unless {@value #PROP_ENABLED} is true.
 *
 * <p>It starts where the service did ({@link DerivationStart}), reads the log's recording from the archive's segment
 * files up to the commit position, and runs a {@link LogDeriver} over it into a {@link QueueEmitter}. A frame a queue
 * does not take is kept, and no log entry is read while one is. It holds a subscriber of its own on each queue and
 * reads it past every frame it offers, as the service does. A failure stops it and nothing else.
 *
 * <p>Single-threaded: its own duty cycle.
 */
public final class ShadowQueueAgent implements AutoCloseable {
    /** The system property that turns it on, on a member. */
    public static final String PROP_ENABLED = "sequencer.shadowQueues";

    /** The shadow queues' channel: {@link FrameLayer#QUEUE_CHANNEL}'s, under another alias. */
    public static final String CHANNEL = "aeron:ipc?alias=seqeron-shadow-queue";

    /** Group {@code groupId}'s shadow queue is stream {@code STREAM_ID_BASE + groupId}. */
    public static final int STREAM_ID_BASE = 3000;

    private static final int ENTRY_LIMIT = 256;
    private static final int INITIAL_TERM_ID = 0;
    private static final BlockHandler DISCARD = (buffer, offset, length, sessionId, termId) -> { };

    private final Aeron aeron;
    private final File archiveDir;
    private final int memberId;
    private final Supplier<DerivationStart> derivationStart;
    private final CountersReader counters;
    private final ExclusivePublication[] publications = new ExclusivePublication[FrameLayer.MAX_GROUPS];
    private final Subscription[] drainSubs = new Subscription[FrameLayer.MAX_GROUPS];
    private final Image[] drains = new Image[FrameLayer.MAX_GROUPS];
    private final RecordingReader.MessageHandler handler = this::onLogEntry;

    private DerivationStart start;
    private QueueEmitter emitter;
    private LogDeriver deriver;
    private RecordingReader log;
    private int commitCounterId;
    private long nextAttemptMs;
    private boolean failed;

    /**
     * @param aeron           the member's Aeron client
     * @param archiveDir      where the member's archive keeps the log's segment files
     * @param derivationStart where the member's service started, or null until it has
     */
    public ShadowQueueAgent(final Aeron aeron, final File archiveDir, final int memberId,
                            final Supplier<DerivationStart> derivationStart) {
        this.aeron = aeron;
        this.archiveDir = archiveDir;
        this.memberId = memberId;
        this.derivationStart = derivationStart;
        this.counters = aeron.countersReader();
    }

    /** Group {@code groupId}'s shadow queue's stream. */
    public static int streamId(final int groupId) {
        return STREAM_ID_BASE + groupId;
    }

    /** Runs the duty cycle on the calling thread until {@code running} goes false or it fails. */
    void run(final AtomicBoolean running, final IdleStrategy idle) {
        try {
            while (running.get() && !failed) {
                idle.idle(doWork());
            }
        } catch (final RuntimeException ex) {
            fail("its duty cycle threw " + ex);
        }
    }

    int doWork() {
        if (log == null) {
            return tryStart() ? 1 : 0;
        }
        int work = emitter.flush();
        if (!emitter.isHolding()) {
            work += log.poll(handler, counters.getCounterValue(commitCounterId), ENTRY_LIMIT);
        }
        if (emitter.failure() != null) {
            fail(emitter.failure());
        }
        return work;
    }

    /** Ends a poll at an entry whose frames a queue did not all take. */
    private boolean onLogEntry(final DirectBuffer buffer, final int offset, final int length, final long endPosition) {
        deriver.onLogEntry(buffer, offset, length);
        return !emitter.isHolding();
    }

    /** Starts once the service has handed over its start and the log is being recorded, trying once a second. */
    private boolean tryStart() {
        final long nowMs = Clocks.monotonicMs();
        final DerivationStart start = derivationStart.get();
        if (start == null || nowMs < nextAttemptMs) {
            return false;
        }
        nextAttemptMs = nowMs + DerivingReplayer.FOLLOW_RETRY_MS;
        final int counterId = DerivingReplayer.commitPositionCounterId(counters);
        final ArchiveRecording recording;
        try (AeronArchive archive = connectArchive()) {
            recording = ArchiveRecording.activeOf(archive, ConsensusModule.Configuration.logStreamId());
        }
        if (recording == null || counterId == CountersReader.NULL_COUNTER_ID) {
            return false;
        }
        if (start.logPosition() < recording.startPosition()) {
            fail("the log's recording starts at " + recording.startPosition() + ", after the service's start at " +
                 start.logPosition());
            return false;
        }
        this.start = start;
        commitCounterId = counterId;
        emitter = new QueueEmitter(new Queues(), start.queueStarts(), start.termLength(), start.mtu());
        deriver = new LogDeriver(memberId, emitter);
        log = new RecordingReader(archiveDir, recording.recordingId(), recording.startPosition(),
                                  recording.termLength(), recording.segmentLength(), start.logPosition());
        deriver.restore(start.state(), start.restored());
        Logger.info(Logger.CoreComponent.ReplayerServer, memberId,
                    "shadow queues follow the log from position %d on streams %d+groupId", start.logPosition(),
                    STREAM_ID_BASE);
        return true;
    }

    private AeronArchive connectArchive() {
        return AeronArchive.connect(new AeronArchive.Context()
                                        .aeron(aeron)
                                        .ownsAeronClient(false)
                                        .controlRequestChannel(PortLayout.ARCHIVE_CONTROL_CHANNEL)
                                        .controlRequestStreamId(PortLayout.ARCHIVE_CONTROL_STREAM_ID)
                                        .controlResponseChannel(PortLayout.ARCHIVE_CONTROL_CHANNEL)
                                        .controlResponseStreamId(NodeDriver.REPLAYER_ARCHIVE_RESPONSE_STREAM_ID)
                                        .lock(NoOpLock.INSTANCE));
    }

    private void fail(final String reason) {
        failed = true;
        Logger.error(Logger.CoreComponent.ReplayerServer, Logger.CoreEventCode.QueueRecordingFailure, memberId,
                     "the shadow queues stop: %s; the member carries on", reason);
    }

    @Override
    public void close() {
        CloseHelper.quietClose(log);
        CloseHelper.quietCloseAll(drainSubs);
        CloseHelper.quietCloseAll(publications);
    }

    /** The Aeron half of {@link QueueEmitter}: a queue is created at its first frame, and takes none until drained. */
    private final class Queues implements QueueEmitter.Queues {
        @Override
        public long offer(final int groupId, final DirectBuffer buffer, final int length) {
            final int i = groupId - 1;
            if (publications[i] == null) {
                final String channel = new ChannelUriStringBuilder(CHANNEL)
                                           .initialPosition(start.queueStarts()[i], INITIAL_TERM_ID, start.termLength())
                                           .mtu(start.mtu())
                                           .build();
                publications[i] = aeron.addExclusivePublication(channel, streamId(groupId));
                drainSubs[i] = aeron.addSubscription(ChannelUri.addSessionId(CHANNEL, publications[i].sessionId()),
                                                     streamId(groupId));
            }
            if (drains[i] == null && (drains[i] = drainSubs[i].imageBySessionId(publications[i].sessionId())) == null) {
                return Publication.NOT_CONNECTED;
            }
            final long result = publications[i].offer(buffer, 0, length);
            while (result > 0 && drains[i].position() < result && !drains[i].isClosed()) {
                drains[i].blockPoll(DISCARD, Integer.MAX_VALUE); // a block ends with its term
            }
            return result;
        }

        @Override
        public boolean isUnrecoverable(final long offerResult) {
            return offerResult == Publication.CLOSED || offerResult == Publication.MAX_POSITION_EXCEEDED;
        }
    }
}
