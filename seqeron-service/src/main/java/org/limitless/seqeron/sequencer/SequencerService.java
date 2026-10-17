package org.limitless.seqeron.sequencer;

import io.aeron.Counter;
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.archive.status.RecordingPos;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.logbuffer.Header;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.status.CountersReader;
import org.limitless.seqeron.protocol.FrameLayer;
import org.limitless.seqeron.protocol.SeqeronCounters;
import org.limitless.seqeron.replayer.server.NodeDriver;
import org.limitless.seqeron.util.Clocks;
import org.limitless.seqeron.util.Logger;

/**
 * The Aeron adapter over {@link Sequencer}: it decides when to call the state machine and publishes what
 * comes back on this node's queue, which the co-located archive records. It holds no replicated state.
 *
 * <p>Every node publishes and records its own queue; the queue publication and its recording are created once
 * in {@link #onStart} and span every leader tenure.
 *
 * <p>Snapshots are refused ({@link #onTakeSnapshot} throws, {@link #onStart} rejects a snapshot image):
 * recovery is always full-log replay, which is what keeps each node's recording complete.
 */
public final class SequencerService implements ClusteredService {
    /** How long {@link #onStart} waits for the archive to start recording the queue before refusing to start. */
    private static final long QUEUE_RECORDING_START_TIMEOUT_MS = 5_000;

    /** Set at launch to enable the test-only fault below; unset in production. */
    private static final String FAULT_INJECTION_ENV = "SEQERON_FAULT_INJECTION";

    /** Touch this file in the cluster directory to arm the fault. See {@link #injectQueueRecordingFault}. */
    private static final String QUEUE_FAULT_TRIGGER_FILE = "queue-stall-fault";

    /** Correlation id of the one repeating heartbeat timer; rescheduling it moves its deadline. */
    private static final long HEARTBEAT_TIMER_CORRELATION_ID = 0x7100_0000_0000_0001L;

    private final Sequencer sequencer;

    /** Everything here that can back-pressure, and the decision to terminate rather than wait. */
    private final QueuePublisher queue = new QueuePublisher(new QueueActions());

    /** Brings the whole node down; wired by {@link SequencerServer}. See {@link QueuePublisher}. */
    private final Runnable fatalHandler;

    private final int memberId;

    /** The co-located archive's client settings; {@link #onStart} supplies the cluster's Aeron client. */
    private final AeronArchive.Context archiveContext;

    // Aeron runtime
    private Cluster cluster;
    private ExclusivePublication queuePub;
    private AeronArchive aeronArchive;
    private CountersReader counters;

    // Queue recording liveness
    private int queueRecordingCounterId = CountersReader.NULL_COUNTER_ID;
    private long queueRecordingId = RecordingPos.NULL_RECORDING_ID;
    /** Test-only fault trigger; null unless fault injection is enabled. See {@link #injectQueueRecordingFault}. */
    private Path queueFaultTrigger;

    // Operator counters
    private Counter globalSeqNoCounter;
    private Counter queueBackPressureAlertCounter;
    private Counter queueStalledCounter;
    private Counter rejectedIngressCounter;
    private Counter leadershipChangeCounter;
    private Counter currentLeaderMemberIdCounter;
    private Counter lastHeartbeatTimestampCounter;
    private Counter gatewayPromotionCounter;
    private Counter gatewayPromotionFailedCounter;
    private Counter bootstrapActivatedCounter;
    private Counter messagesSequencedCounter;

    /**
     * @param memberId       this node's cluster memberId
     * @param archiveContext the co-located archive's client settings; this service owns it
     * @param fatalHandler   run once, from a cluster callback, when this node can no longer record its own
     *                       queue. Must not block: it signals a shutdown and returns.
     */
    public SequencerService(final int memberId, final AeronArchive.Context archiveContext,
                            final Runnable fatalHandler) {
        this.sequencer = new Sequencer(memberId, queue::emit);
        this.memberId = memberId;
        this.archiveContext = archiveContext;
        this.fatalHandler = fatalHandler;
    }

    /**
     * Cluster start handler
     * @param cluster       with which the service can interact.
     * @param snapshotImage from which the service can load its archived state which can be null when no snapshot.
     */
    @Override
    public void onStart(final Cluster cluster, final Image snapshotImage) {
        // Refused before anything is acquired, so nothing is left behind.
        if (snapshotImage != null) {
            throw refuseStart("[SequencerService] Refusing to start from a snapshot: recovery is full-log replay from "
                              + "globalSeqNo 1 (see the class javadoc). Remove the snapshot from the cluster directory "
                              + "so the log replays in full.");
        }

        this.cluster = cluster;
        this.counters = cluster.context().aeron().countersReader();
        if (null != System.getenv(FAULT_INJECTION_ENV)) {
            queueFaultTrigger = cluster.context().clusterDir().toPath().resolve(QUEUE_FAULT_TRIGGER_FILE);
        }

        try {
            aeronArchive = AeronArchive.connect(archiveContext.aeron(cluster.context().aeron()));
            queuePub =
                cluster.context().aeron().addExclusivePublication(FrameLayer.QUEUE_CHANNEL, FrameLayer.QUEUE_STREAM_ID);
            aeronArchive.startRecording(FrameLayer.QUEUE_CHANNEL, FrameLayer.QUEUE_STREAM_ID, SourceLocation.LOCAL);
            if (!awaitQueueRecordingActive()) {
                throw new IllegalStateException("the co-located archive did not start recording the queue within " +
                                                QUEUE_RECORDING_START_TIMEOUT_MS + "ms");
            }
        } catch (final RuntimeException ex) {
            CloseHelper.quietCloseAll(aeronArchive, queuePub);
            throw refuseStart("[SequencerService] Refusing to start, the queue cannot be recorded: " + ex);
        }
        createCounters();
    }

    /**
     * Blocks until the archive's recording has attached to the queue publication, and keeps its counter for
     * {@link QueuePublisher#checkRecordingAlive}.
     * @return whether the recording attached before the deadline; the caller refuses the start if not
     */

    private boolean awaitQueueRecordingActive() {
        final long archiveId = aeronArchive.archiveId();
        final long deadlineMs = Clocks.monotonicMs() + QUEUE_RECORDING_START_TIMEOUT_MS;
        int counterId;
        while ((counterId = RecordingPos.findCounterIdBySession(counters, queuePub.sessionId(), archiveId)) ==
               CountersReader.NULL_COUNTER_ID) {
            if (Clocks.monotonicMs() >= deadlineMs) {
                return false;
            }
            cluster.idleStrategy().idle();
        }
        queueRecordingCounterId = counterId;
        queueRecordingId = RecordingPos.getRecordingId(counters, counterId);
        return true;
    }

    /**
     * Refuses to bring this node up. A throw alone only stops the service agent and leaves a headless node
     * running, so the fatal handler brings the process down too.
     * @param message why start-up was refused
     * @return the exception for the caller to throw
     */
    private IllegalStateException refuseStart(final String message) {
        fatalHandler.run();
        return new IllegalStateException(message);
    }

    /** Creates this node's operator counters. */
    private void createCounters() {
        globalSeqNoCounter = counter(SeqeronCounters.SEQUENCER_GLOBAL_SEQ_NO_TYPE_ID, "globalSeqNo");
        queueBackPressureAlertCounter =
            counter(SeqeronCounters.SEQUENCER_QUEUE_BACKPRESSURE_ALERTS_TYPE_ID, "queueBackPressureAlerts");
        queueStalledCounter = counter(SeqeronCounters.SEQUENCER_QUEUE_STALLED_TYPE_ID, "queueStalled");
        rejectedIngressCounter =
            counter(SeqeronCounters.SEQUENCER_REJECTED_INGRESS_COUNT_TYPE_ID, "rejectedIngressCount");
        leadershipChangeCounter =
            counter(SeqeronCounters.SEQUENCER_LEADERSHIP_CHANGE_COUNT_TYPE_ID, "leadershipChangeCount");
        currentLeaderMemberIdCounter =
            counter(SeqeronCounters.SEQUENCER_CURRENT_LEADER_MEMBER_ID_TYPE_ID, "currentLeaderMemberId");
        lastHeartbeatTimestampCounter =
            counter(SeqeronCounters.SEQUENCER_LAST_CLUSTER_HEARTBEAT_TIMESTAMP_TYPE_ID, "lastHeartbeatTimestamp");
        gatewayPromotionCounter =
            counter(SeqeronCounters.SEQUENCER_GATEWAY_PROMOTION_COUNT_TYPE_ID, "gatewayPromotionCount");
        gatewayPromotionFailedCounter =
            counter(SeqeronCounters.SEQUENCER_GATEWAY_PROMOTION_FAILED_COUNT_TYPE_ID, "gatewayPromotionFailedCount");
        bootstrapActivatedCounter =
            counter(SeqeronCounters.SEQUENCER_BOOTSTRAP_ACTIVATED_TYPE_ID, "bootstrapActivated");
        messagesSequencedCounter = counter(SeqeronCounters.SEQUENCER_INGRESS_MESSAGES_TYPE_ID, "ingressMessages");
    }

    /**
     * One operator counter, labelled {@code seqeron.sequencer.<name> member=<memberId>}.
     * @param typeId which counter, from {@link SeqeronCounters}
     * @param name   its name within the {@code seqeron.sequencer} namespace
     */
    private Counter counter(final int typeId, final String name) {
        return SeqeronCounters.addCounter(cluster.context().aeron(), typeId,
                                          "seqeron.sequencer." + name + " member=" + memberId, memberId);
    }

    /**
     * Cluster client session open handler.
     * @param session   for the client which have been opened.
     * @param timestamp at which the session was opened.
     */
    @Override
    public void onSessionOpen(final ClientSession session, final long timestamp) {
    }

    /**
     * Cluster client session close handler.
     * @param session     that has been closed.
     * @param timestamp   at which the session was closed.
     * @param closeReason the session was closed.
     */
    @Override
    public void onSessionClose(final ClientSession session, final long timestamp, final CloseReason closeReason) {
        if (sequencer.sessionClosed(session.id(), timestamp)) {
            Logger.info(Logger.CoreComponent.SequencerService, memberId,
                        "gateway session %d closed (%s) — promoting standby", session.id(), closeReason);
        }
        updateCounters();
    }

    /**
     * Client session message handler
     * @param session   for the client which sent the message. This can be null if the client was a service.
     * @param timestamp for when the message was received.
     * @param buffer    containing the message.
     * @param offset    in the buffer at which the message is encoded.
     * @param length    of the encoded message.
     * @param header    aeron header for the incoming message.
     */
    @Override
    public void onSessionMessage(final ClientSession session, final long timestamp, final DirectBuffer buffer,
                                 final int offset, final int length, final Header header) {
        if (session == null) {
            rejectedIngressCounter.increment();
            Logger.error(Logger.CoreComponent.SequencerService, Logger.CoreEventCode.MalformedIngressMessage,
                         memberId, "skipping ingress message with no client session (globalSeqNo stays %d)",
                         sequencer.globalSeqNo());
            return;
        }
        if (sequencer.sequenceMessage(buffer, offset, length, session.id(), timestamp)) {
            messagesSequencedCounter.increment();
        } else {
            rejectedIngressCounter.increment();
        }
        updateCounters();
    }

    /**
     * Timer event handler
     * @param correlationId for the expired timer.
     * @param timestamp     at which the timer expired.
     */
    @Override
    public void onTimerEvent(final long correlationId, final long timestamp) {
        if (correlationId == HEARTBEAT_TIMER_CORRELATION_ID) {
            sequencer.clusterHeartbeat(timestamp);
            updateCounters();
            lastHeartbeatTimestampCounter.set(TimeUnit.NANOSECONDS.toMillis(timestamp)); // exported as ms
            injectQueueRecordingFault();
            queue.checkRecordingAlive();
            scheduleHeartbeat();
        }
    }

    /**
     * Test-only fault injection for chaos-runner.sh, inert unless {@link #FAULT_INJECTION_ENV} is set:
     * touching {@code <clusterDir>/queue-stall-fault} stops this node's queue recording, the one way to provoke
     * {@link QueuePublisher#checkRecordingAlive} from outside the JVM. One-shot per process.
     */
    private void injectQueueRecordingFault() {
        if (queueFaultTrigger == null || !Files.exists(queueFaultTrigger)) {
            return;
        }
        queueFaultTrigger = null;
        Logger.info(Logger.CoreComponent.SequencerService, memberId,
                    "fault injection: stopping this node's queue recording");
        aeronArchive.stopRecording(FrameLayer.QUEUE_CHANNEL, FrameLayer.QUEUE_STREAM_ID);
    }

    /** Re-arms the cluster clock one heartbeat ahead; see {@link QueuePublisher#scheduleHeartbeat}. */
    private void scheduleHeartbeat() {
        queue.scheduleHeartbeat(cluster.time() + FrameLayer.CLUSTER_HEARTBEAT_INTERVAL_NS);
    }

    /**
     * Unsupported by design (see the class javadoc).
     * @param snapshotPublication to which the state should be recorded.
     */
    @Override
    public void onTakeSnapshot(final ExclusivePublication snapshotPublication) {
        throw new UnsupportedOperationException(
            "[SequencerService] Snapshots are not supported: recovery is full-log replay from globalSeqNo 1, "
            + "which is what keeps each node's queue recording complete. Stop the cluster with clusterctl "
            + "shutdown (ABORT), not a SNAPSHOT/SHUTDOWN toggle.");
    }

    /**
     * New leadership term received
     * @param leadershipTermId identity for the new leadership term.
     * @param logPosition position the log has reached as the result of this message.
     * @param timestamp for the new leadership term.
     * @param termBaseLogPosition position at the beginning of the leadership term.
     * @param leaderMemberId who won the election.
     * @param logSessionId session id for the publication of the log.
     * @param timeUnit for the timestamps in the coming leadership term.
     * @param appVersion for the application configured in the consensus module.
     */
    @Override
    public void onNewLeadershipTermEvent(final long leadershipTermId, final long logPosition, final long timestamp,
                                         final long termBaseLogPosition, final int leaderMemberId,
                                         final int logSessionId, final TimeUnit timeUnit, final int appVersion) {
        applyLeadership(leadershipTermId, leaderMemberId, timestamp);
        scheduleHeartbeat(); // Arm (or re-arm) the internal cluster clock here
    }

    /**
     * Role change handler
     * @param newRole that the node has assumed.
     */
    @Override
    public void onRoleChange(final Cluster.Role newRole) {
        // Deliberately a no-op: onNewLeadershipTermEvent already fires on every node
    }

    /**
     * Called when a new leadership term begins.
     * @param leadershipTermId the new term
     * @param leaderMemberId leader member identity
     * @param timestamp now
     */
    private void applyLeadership(final long leadershipTermId, final int leaderMemberId, final long timestamp) {
        sequencer.leadershipChanged(leadershipTermId, leaderMemberId, timestamp);
        updateCounters();
        leadershipChangeCounter.increment();
        currentLeaderMemberIdCounter.set(leaderMemberId);
        final boolean leader = leaderMemberId == memberId;
        Logger.info(Logger.CoreComponent.SequencerService, memberId,
                    "leadership change: term %d, new leader is memberId=%d (isLeader=%b)", leadershipTermId,
                    leaderMemberId, leader);
    }

    /**
     * Termination handler
     * @param cluster with which the service can interact.
     */
    @Override
    public void onTerminate(final Cluster cluster) {
        CloseHelper.quietCloseAll(aeronArchive, queuePub);
        closeCounters();
    }

    /** Closes the operator counters; a node whose start was refused created none. */
    private void closeCounters() {
        CloseHelper.quietCloseAll(globalSeqNoCounter, queueBackPressureAlertCounter, queueStalledCounter,
                                  rejectedIngressCounter, leadershipChangeCounter, currentLeaderMemberIdCounter,
                                  lastHeartbeatTimestampCounter, gatewayPromotionCounter, gatewayPromotionFailedCounter,
                                  bootstrapActivatedCounter, messagesSequencedCounter);
    }

    /** Mirrors the sequencer's state onto the operator counters after a callback. */
    private void updateCounters() {
        globalSeqNoCounter.set(sequencer.globalSeqNo());
        gatewayPromotionCounter.set(sequencer.gatewayPromotionCount());
        gatewayPromotionFailedCounter.set(sequencer.gatewayPromotionFailedCount());
        bootstrapActivatedCounter.set(sequencer.bootstrapActivationEmitted() ? 1 : 0);
    }

    /** The Aeron half of {@link QueuePublisher}: calls through to the runtime, no decisions. */
    private final class QueueActions implements QueuePublisher.Actions {
        @Override
        public long offerFrame(final DirectBuffer buffer, final int length) {
            return queuePub.offer(buffer, 0, length);
        }

        @Override
        public boolean isUnrecoverable(final long offerResult) {
            return offerResult == ExclusivePublication.CLOSED
                || offerResult == ExclusivePublication.MAX_POSITION_EXCEEDED;
        }

        @Override
        public boolean scheduleTimer(final long deadline) {
            return cluster.scheduleTimer(HEARTBEAT_TIMER_CORRELATION_ID, deadline);
        }

        @Override
        public void idle() {
            cluster.idleStrategy().idle();
        }

        @Override
        public long nanoTime() {
            return System.nanoTime();
        }

        @Override
        public boolean recordingActive() {
            return RecordingPos.isActive(counters, queueRecordingCounterId, queueRecordingId);
        }

        @Override
        public long recordedPosition() {
            return counters.getCounterValue(queueRecordingCounterId);
        }

        @Override
        public long recordingId() {
            return queueRecordingId;
        }

        @Override
        public void queueBackPressureAlert() {
            queueBackPressureAlertCounter.increment();
        }

        @Override
        public void queueStalled(final boolean stalled) {
            queueStalledCounter.set(stalled ? 1 : 0);
        }

        @Override
        public void signalFatal() {
            fatalHandler.run();
        }

        @Override
        public void halt() {
            Runtime.getRuntime().halt(NodeDriver.EXIT_FATAL);
        }

        @Override
        public Integer memberId() {
            return memberId;
        }

        @Override
        public long globalSeqNo() {
            return sequencer.globalSeqNo();
        }
    }
}
