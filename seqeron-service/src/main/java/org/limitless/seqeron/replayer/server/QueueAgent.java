package org.limitless.seqeron.replayer.server;

import io.aeron.Aeron;
import io.aeron.AeronCounters;
import io.aeron.ChannelUri;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.Counter;
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ConsensusModule;
import io.aeron.driver.status.StreamCounter;
import io.aeron.logbuffer.BlockHandler;
import java.io.File;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.NoOpLock;
import org.agrona.concurrent.status.CountersReader;
import org.limitless.seqeron.protocol.FrameLayer;
import org.limitless.seqeron.protocol.PortLayout;
import org.limitless.seqeron.protocol.SeqeronCounters;
import org.limitless.seqeron.sequencer.DerivationStart;
import org.limitless.seqeron.sequencer.LogDeriver;
import org.limitless.seqeron.sequencer.NodeDriver;
import org.limitless.seqeron.util.Clocks;
import org.limitless.seqeron.util.Logger;

/**
 * Publishes a member's queues from its committed log, on a thread of its own, so the service's duty cycle never waits
 * on a queue's subscribers (doc/records/plans/queue-off-duty-cycle-plan.md, steps 1 to 3).
 *
 * <p>It starts where the service did ({@link DerivationStart}), reads the log's recording from the archive's segment
 * files up to the commit position, and runs a {@link LogDeriver} over it into a {@link QueueEmitter}. A frame a queue
 * does not take is kept, and no log entry is read while one is. It holds a subscriber of its own on each queue and
 * reads it past every frame it offers, so every offer lands with no application subscribed. A queue that stays
 * back-pressured for the stall timeout is revoked, after it logs the subscribers holding it, and reopened at the
 * commit position. A frame that lands off the log's position, a subscriber of its own the driver closed, or a duty
 * cycle that throws, runs the member's fatal handler.
 *
 * <p>Single-threaded: its own duty cycle. {@link #logPosition} is read from the Replayer's.
 */
final class QueueAgent implements AutoCloseable {
    private static final int ENTRY_LIMIT = 256;
    private static final int INITIAL_TERM_ID = 0;
    private static final long QUEUE_CHECK_INTERVAL_NS = TimeUnit.SECONDS.toNanos(1);
    /** Short: until it starts, the member's queues carry nothing. */
    private static final long START_RETRY_MS = 50;
    private static final BlockHandler DISCARD = (buffer, offset, length, sessionId, termId) -> { };

    private final Aeron aeron;
    private final File archiveDir;
    private final int memberId;
    private final Supplier<DerivationStart> derivationStart;
    private final long stallTimeoutNs;
    private final Runnable fatalHandler;
    private final CountersReader counters;
    private final Counter backPressureAlerts;
    private final Counter stalled;
    private final ExclusivePublication[] publications = new ExclusivePublication[FrameLayer.MAX_GROUPS];
    private final Subscription[] drainSubs = new Subscription[FrameLayer.MAX_GROUPS];
    private final Image[] drains = new Image[FrameLayer.MAX_GROUPS];
    private final RecordingReader.MessageHandler handler = this::onLogEntry;

    private DerivationStart start;
    private QueueEmitter emitter;
    private LogDeriver deriver;
    private RecordingReader log;
    /** Held while it waits to start, so a retry costs one listing. */
    private AeronArchive archive;
    private int commitCounterId;
    private long nextAttemptMs;
    private long nextQueueCheckNs;
    private boolean failed;
    /** Where the next log entry it reads starts, or {@link LogTrimmer#HOLD} until it has started. */
    private volatile long logPosition = LogTrimmer.HOLD;

    /**
     * @param aeron           the member's Aeron client
     * @param archiveDir      where the member's archive keeps the log's segment files
     * @param derivationStart where the member's service started, or null until it has
     * @param stallTimeoutNs  how long a queue may stay back-pressured before the member terminates
     * @param fatalHandler    the member's: run once, when the queues cannot be published
     */
    QueueAgent(final Aeron aeron, final File archiveDir, final int memberId,
               final Supplier<DerivationStart> derivationStart, final long stallTimeoutNs,
               final Runnable fatalHandler) {
        this.aeron = aeron;
        this.archiveDir = archiveDir;
        this.memberId = memberId;
        this.derivationStart = derivationStart;
        this.stallTimeoutNs = stallTimeoutNs;
        this.fatalHandler = fatalHandler;
        this.counters = aeron.countersReader();
        this.backPressureAlerts = counter(SeqeronCounters.SEQUENCER_QUEUE_BACKPRESSURE_ALERTS_TYPE_ID,
                                          "queueBackPressureAlerts");
        this.stalled = counter(SeqeronCounters.SEQUENCER_QUEUE_STALLED_TYPE_ID, "queueStalled");
    }

    private Counter counter(final int typeId, final String name) {
        return SeqeronCounters.addCounter(aeron, typeId, "seqeron.sequencer." + name + " member=" + memberId, memberId);
    }

    /** Where the log must be kept from for this agent, or {@link LogTrimmer#HOLD} until it has started. */
    long logPosition() {
        return logPosition;
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
        final long nowNs = System.nanoTime();
        int work = emitter.isHolding() ? emitter.flush(nowNs) : 0;
        if (!emitter.isHolding()) {
            final long commitPosition = counters.getCounterValue(commitCounterId);
            work += log.poll(handler, commitPosition, ENTRY_LIMIT);
            logPosition = log.position();
            if (logPosition >= commitPosition) {
                emitter.onCaughtUp();
            }
        }
        if (emitter.failure() != null) {
            fail(emitter.failure());
        } else if (nowNs - nextQueueCheckNs >= 0) {
            nextQueueCheckNs = nowNs + QUEUE_CHECK_INTERVAL_NS;
            checkQueues();
        }
        return work;
    }

    /** Ends a poll at an entry whose frames a queue did not all take. */
    private boolean onLogEntry(final DirectBuffer buffer, final int offset, final int length, final long endPosition) {
        deriver.onLogEntry(buffer, offset, length);
        return !emitter.isHolding();
    }

    /**
     * A subscriber of its own the driver closed back-pressures nothing while applications keep the queue connected, so
     * the stall bound would never see it.
     */
    private void checkQueues() {
        for (int i = 0; i < FrameLayer.MAX_GROUPS; i++) {
            if (drains[i] != null && drains[i].isClosed()) {
                fail("the media driver closed this member's own subscriber to group " + (i + 1) + "'s queue");
                return;
            }
        }
    }

    /** Starts once the service has handed over its start and the log is being recorded. */
    private boolean tryStart() {
        final long nowMs = Clocks.monotonicMs();
        final DerivationStart start = derivationStart.get();
        if (start == null || nowMs < nextAttemptMs) {
            return false;
        }
        nextAttemptMs = nowMs + START_RETRY_MS;
        final int counterId = DerivingReplayer.commitPositionCounterId(counters);
        if (archive == null) {
            archive = connectArchive();
        }
        final ArchiveRecording recording =
            ArchiveRecording.activeOf(archive, ConsensusModule.Configuration.logStreamId());
        if (recording == null || counterId == CountersReader.NULL_COUNTER_ID) {
            return false;
        }
        archive.close();
        archive = null;
        if (start.logPosition() < recording.startPosition()) {
            fail("the log's recording starts at " + recording.startPosition() + ", after the service's start at " +
                 start.logPosition());
            return false;
        }
        this.start = start;
        commitCounterId = counterId;
        emitter = new QueueEmitter(new Queues(), start.queueStarts(), start.termLength(), start.mtu(), stallTimeoutNs,
                                   memberId);
        deriver = new LogDeriver(memberId, emitter);
        log = new RecordingReader(archiveDir, recording.recordingId(), recording.startPosition(),
                                  recording.termLength(), recording.segmentLength(), start.logPosition());
        logPosition = log.position();
        deriver.restore(start.state(), start.restored());
        Logger.info(Logger.CoreComponent.ReplayerServer, memberId, "queues follow the log from position %d",
                    start.logPosition());
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

    /** Latched: the gauge stays raised on the way out. */
    private void fail(final String reason) {
        if (failed) {
            return;
        }
        failed = true;
        stalled.set(1);
        Logger.fault(Logger.CoreComponent.ReplayerServer, Logger.CoreEventCode.QueueRecordingFailure, memberId,
                     "FATAL: %s, so this member cannot publish its queues — terminating it; its peers keep quorum and "
                         + "its restart derives its queues again",
                     reason);
        fatalHandler.run();
    }

    @Override
    public void close() {
        CloseHelper.quietClose(archive);
        CloseHelper.quietClose(log);
        CloseHelper.quietCloseAll(drainSubs);
        CloseHelper.quietCloseAll(publications);
        CloseHelper.quietCloseAll(backPressureAlerts, stalled);
    }

    /** The Aeron half of {@link QueueEmitter}: a queue is created at its first frame, and takes none until drained. */
    private final class Queues implements QueueEmitter.Queues {
        @Override
        public long offer(final int groupId, final DirectBuffer buffer, final int length) {
            final int i = groupId - 1;
            if (publications[i] == null) {
                open(groupId, start.queueStarts()[i]);
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

        @Override
        public boolean isBackPressure(final long offerResult) {
            return offerResult == Publication.BACK_PRESSURED;
        }

        @Override
        public void revoke(final int groupId) {
            final int i = groupId - 1;
            logHolders(groupId, publications[i].sessionId());
            CloseHelper.quietClose(drainSubs[i]);
            publications[i].revoke();
            publications[i] = null;
            drainSubs[i] = null;
            drains[i] = null;
        }

        @Override
        public void open(final int groupId, final long position) {
            final int i = groupId - 1;
            // The driver's term length and MTU, which the start's are: a queue at 0 is on the bare channel,
            // which AeronReplayer.queueBacklog looks the publication up by.
            final String channel = position == 0
                ? FrameLayer.QUEUE_CHANNEL
                : new ChannelUriStringBuilder(FrameLayer.QUEUE_CHANNEL)
                      .initialPosition(position, INITIAL_TERM_ID, start.termLength())
                      .build();
            publications[i] = aeron.addExclusivePublication(channel, FrameLayer.queueStreamId(groupId));
            drainSubs[i] = aeron.addSubscription(
                ChannelUri.addSessionId(FrameLayer.QUEUE_CHANNEL, publications[i].sessionId()),
                FrameLayer.queueStreamId(groupId));
            Logger.info(Logger.CoreComponent.ReplayerServer, memberId, "group %d's queue opened at position %d",
                        groupId, position);
        }

        /** Names each subscriber at the queue's limit by the registration id its driver's counters show. */
        private void logHolders(final int groupId, final int sessionId) {
            final DirectBuffer metaData = counters.metaDataBuffer();
            long lowest = Long.MAX_VALUE;
            for (int pass = 0; pass < 2; pass++) {
                for (int id = 0, maxId = counters.maxCounterId(); id <= maxId; id++) {
                    if (counters.getCounterState(id) != CountersReader.RECORD_ALLOCATED ||
                        counters.getCounterTypeId(id) != AeronCounters.DRIVER_SUBSCRIBER_POSITION_TYPE_ID) {
                        continue;
                    }
                    final int key = CountersReader.metaDataOffset(id) + CountersReader.KEY_OFFSET;
                    if (metaData.getInt(key + StreamCounter.SESSION_ID_OFFSET) != sessionId ||
                        metaData.getInt(key + StreamCounter.STREAM_ID_OFFSET) != FrameLayer.queueStreamId(groupId)) {
                        continue;
                    }
                    final long position = counters.getCounterValue(id);
                    if (pass == 0) {
                        lowest = Math.min(lowest, position);
                    } else if (position == lowest) {
                        Logger.error(Logger.CoreComponent.ReplayerServer, Logger.CoreEventCode.QueueBackpressure,
                                     memberId, "group %d's queue is held at position %d by subscription "
                                         + "registrationId=%d", groupId, position,
                                     metaData.getLong(key + StreamCounter.REGISTRATION_ID_OFFSET));
                    }
                }
            }
        }

        @Override
        public void backPressureAlert() {
            backPressureAlerts.increment();
        }

        @Override
        public void stalled(final boolean stalled) {
            QueueAgent.this.stalled.set(stalled ? 1 : 0);
        }
    }
}
