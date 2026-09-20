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
import org.agrona.concurrent.NoOpLock;
import org.agrona.concurrent.status.CountersReader;
import org.limitless.seqeron.protocol.FrameLayer;
import org.limitless.seqeron.protocol.SeqeronCounters;
import org.limitless.seqeron.util.Clocks;
import org.limitless.seqeron.util.Logger;

/**
 * The Aeron adapter over {@link Sequencer}: it decides when to call the state machine and publishes what
 * comes back on this node's tap, which the co-located archive records. It holds no replicated state.
 *
 * <p>Every node publishes and records its own tap; the tap publication and its recording are created once
 * in {@link #onStart} and span every leader tenure.
 *
 * <p>Snapshots are refused ({@link #onTakeSnapshot} throws, {@link #onStart} rejects a snapshot image):
 * recovery is always full-log replay, which is what keeps each node's recording complete.
 */
public final class SequencerService implements ClusteredService {
    /**
     * Control-response stream of this service's archive client. Must not be 101, the cluster's IPC ingress
     * stream, or archive replies misdecode as ingress; nor ReplayerServer's 120 on the same driver. Sharing
     * SequencerServer's 121 is fine: the archive demuxes clients by controlSessionId.
     */
    private static final int ARCHIVE_CONTROL_RESPONSE_STREAM_ID = 121;

    /** How long {@link #onStart} waits for the archive to start recording the tap before refusing to start. */
    private static final long TAP_RECORDING_START_TIMEOUT_MS = 5_000;

    /** Set at launch to enable the test-only fault below; unset in production. */
    private static final String FAULT_INJECTION_ENV = "SEQERON_FAULT_INJECTION";

    /** Touch this file in the cluster directory to arm the fault. See {@link #injectTapRecordingFault}. */
    private static final String TAP_FAULT_TRIGGER_FILE = "tap-stall-fault";

    /** Correlation id of the one repeating heartbeat timer; rescheduling it moves its deadline. */
    private static final long HEARTBEAT_TIMER_CORRELATION_ID = 0x7100_0000_0000_0001L;

    private final Sequencer sequencer;

    /** Everything here that can back-pressure, and the decision to terminate rather than wait. */
    private final TapPublisher tap = new TapPublisher(new TapActions());

    /** Brings the whole node down; wired by {@link SequencerServer}. See {@link TapPublisher}. */
    private final Runnable fatalHandler;

    // Aeron runtime
    private Cluster cluster;
    private ExclusivePublication tapPub;
    private AeronArchive aeronArchive;
    private CountersReader counters;

    // Tap recording liveness
    private int tapRecordingCounterId = CountersReader.NULL_COUNTER_ID;
    private long tapRecordingId = RecordingPos.NULL_RECORDING_ID;
    /** Test-only fault trigger; null unless fault injection is enabled. See {@link #injectTapRecordingFault}. */
    private Path tapFaultTrigger;

    // Operator counters
    private Counter globalSeqNoCounter;
    private Counter tapBackPressureAlertCounter;
    private Counter tapStalledCounter;
    private Counter rejectedIngressCounter;
    private Counter leadershipChangeCounter;
    private Counter currentLeaderMemberIdCounter;
    private Counter lastHeartbeatTimestampCounter;
    private Counter gatewayPromotionCounter;
    private Counter gatewayPromotionFailedCounter;
    private Counter bootstrapActivatedCounter;
    private Counter connectedClientsCounter;
    private Counter messagesSequencedCounter;

    /**
     * @param fatalHandler run once, from a cluster callback, when this node can no longer record its own
     *                     tap. Must not block: it signals a shutdown and returns.
     */
    public SequencerService(final Runnable fatalHandler) {
        this.sequencer = new Sequencer();
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
            tapFaultTrigger = cluster.context().clusterDir().toPath().resolve(TAP_FAULT_TRIGGER_FILE);
        }

        try {
            aeronArchive = AeronArchive.connect(new AeronArchive.Context()
                                                    .aeron(cluster.context().aeron())
                                                    .controlRequestChannel("aeron:ipc")
                                                    .controlRequestStreamId(100)
                                                    .controlResponseChannel("aeron:ipc")
                                                    .controlResponseStreamId(ARCHIVE_CONTROL_RESPONSE_STREAM_ID)
                                                    .lock(NoOpLock.INSTANCE));
            tapPub = cluster.context().aeron().addExclusivePublication(FrameLayer.FEEDER_CHANNEL, FrameLayer.FEEDER_STREAM_ID);
            aeronArchive.startRecording(FrameLayer.FEEDER_CHANNEL, FrameLayer.FEEDER_STREAM_ID, SourceLocation.LOCAL);
            if (!awaitTapRecordingActive()) {
                throw new IllegalStateException("the co-located archive did not start recording the tap within "
                                                + TAP_RECORDING_START_TIMEOUT_MS + "ms");
            }
        } catch (final RuntimeException ex) {
            CloseHelper.quietCloseAll(aeronArchive, tapPub);
            throw refuseStart("[SequencerService] Refusing to start, the tap cannot be recorded: " + ex);
        }
        // Counters are NOT created here: cluster.memberId() is still NULL_VALUE during onStart
    }

    /**
     * Blocks until the archive's recording has attached to the tap publication, and keeps its counter for
     * {@link TapPublisher#checkRecordingAlive}.
     * @return whether the recording attached before the deadline; the caller refuses the start if not
     */

    private boolean awaitTapRecordingActive() {
        final long archiveId = aeronArchive.archiveId();
        final long deadlineMs = Clocks.monotonicMs() + TAP_RECORDING_START_TIMEOUT_MS;
        int counterId;
        while ((counterId = RecordingPos.findCounterIdBySession(counters, tapPub.sessionId(), archiveId)) ==
               CountersReader.NULL_COUNTER_ID) {
            if (Clocks.monotonicMs() >= deadlineMs) {
                return false;
            }
            cluster.idleStrategy().idle();
        }
        tapRecordingCounterId = counterId;
        tapRecordingId = RecordingPos.getRecordingId(counters, counterId);
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

    /**
     * Creates this node's operator counters on the first callback that needs one, labelled with the
     * memberId — which is still {@code NULL_VALUE} during {@link #onStart}.
     */
    private void ensureCounters() {
        if (globalSeqNoCounter != null) {
            return;
        }
        final int memberId = cluster.memberId();
        sequencer.memberId(memberId);
        globalSeqNoCounter = counter(SeqeronCounters.SEQUENCER_GLOBAL_SEQ_NO_TYPE_ID, "globalSeqNo", memberId);
        tapBackPressureAlertCounter =
            counter(SeqeronCounters.SEQUENCER_TAP_BACKPRESSURE_ALERTS_TYPE_ID, "tapBackPressureAlerts", memberId);
        tapStalledCounter = counter(SeqeronCounters.SEQUENCER_TAP_STALLED_TYPE_ID, "tapStalled", memberId);
        rejectedIngressCounter =
            counter(SeqeronCounters.SEQUENCER_REJECTED_INGRESS_COUNT_TYPE_ID, "rejectedIngressCount", memberId);
        leadershipChangeCounter =
            counter(SeqeronCounters.SEQUENCER_LEADERSHIP_CHANGE_COUNT_TYPE_ID, "leadershipChangeCount", memberId);
        currentLeaderMemberIdCounter =
            counter(SeqeronCounters.SEQUENCER_CURRENT_LEADER_MEMBER_ID_TYPE_ID, "currentLeaderMemberId", memberId);
        lastHeartbeatTimestampCounter =
            counter(SeqeronCounters.SEQUENCER_LAST_CLUSTER_HEARTBEAT_TIMESTAMP_TYPE_ID, "lastHeartbeatTimestamp",
                    memberId);
        gatewayPromotionCounter =
            counter(SeqeronCounters.SEQUENCER_GATEWAY_PROMOTION_COUNT_TYPE_ID, "gatewayPromotionCount", memberId);
        gatewayPromotionFailedCounter =
            counter(SeqeronCounters.SEQUENCER_GATEWAY_PROMOTION_FAILED_COUNT_TYPE_ID, "gatewayPromotionFailedCount",
                    memberId);
        bootstrapActivatedCounter =
            counter(SeqeronCounters.SEQUENCER_BOOTSTRAP_ACTIVATED_TYPE_ID, "bootstrapActivated", memberId);
        connectedClientsCounter =
            counter(SeqeronCounters.SEQUENCER_CONNECTED_CLIENTS_TYPE_ID, "connectedClients", memberId);
        messagesSequencedCounter =
            counter(SeqeronCounters.SEQUENCER_INGRESS_MESSAGES_TYPE_ID, "ingressMessages", memberId);
    }

    /**
     * One operator counter, labelled {@code seqeron.sequencer.<name> member=<memberId>}.
     * @param typeId   which counter, from {@link SeqeronCounters}
     * @param name     its name within the {@code seqeron.sequencer} namespace
     * @param memberId this node
     */
    private Counter counter(final int typeId, final String name, final int memberId) {
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
        ensureCounters();
        publishPromotion(sequencer.sessionClosed(session.id(), timestamp),
                         "gateway session " + session.id() + " closed (" + closeReason + ")");
    }

    /**
     * Publishes whatever a promotion decision came back with, counting both outcomes.
     * @param activation what {@link Sequencer#sessionClosed} or {@link
     *     Sequencer#pendingGatewayActivationTimeout} returned
     * @param reason what triggered it, for the log line
     */
    private void publishPromotion(final int activation, final String reason) {
        if (activation == Sequencer.NO_PROMOTION_TARGET) {
            // A gateway lost its active instance but nothing replaced it.
            gatewayPromotionFailedCounter.increment();
        } else if (activation != Sequencer.NO_FRAME) {
            gatewayPromotionCounter.increment();
            Logger.info(Logger.CoreComponent.SequencerService, cluster.memberId(), "%s — promoting standby", reason);
            emit(activation);
        }
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
        ensureCounters();
        if (session == null) {
            rejectedIngressCounter.increment();
            Logger.error(Logger.CoreComponent.SequencerService, Logger.CoreEventCode.MalformedIngressMessage,
                         cluster.memberId(), "skipping ingress message with no client session (globalSeqNo stays %d)",
                         sequencer.globalSeqNo());
            return;
        }
        final int sequenced = sequencer.sequenceMessage(buffer, offset, length, session.id(), timestamp);
        if (sequenced != Sequencer.NO_FRAME) {
            messagesSequencedCounter.increment();
            emit(sequenced);
        } else {
            rejectedIngressCounter.increment();
        }

        int activation;
        while ((activation = sequencer.pendingGatewayActivation(timestamp)) != Sequencer.NO_FRAME) {
            emit(activation);
            if (sequencer.bootstrapActivationEmitted()) {
                bootstrapActivatedCounter.set(1);
            }
        }
    }

    /**
     * Timer event handler
     * @param correlationId for the expired timer.
     * @param timestamp     at which the timer expired.
     */
    @Override
    public void onTimerEvent(final long correlationId, final long timestamp) {
        if (correlationId == HEARTBEAT_TIMER_CORRELATION_ID) {
            ensureCounters();
            emit(sequencer.clusterHeartbeat(timestamp));

            int overdue;
            while ((overdue = sequencer.pendingGatewayActivationTimeout(timestamp)) != Sequencer.NO_FRAME) {
                publishPromotion(overdue, "a designated gateway instance never declared itself started");
            }
            lastHeartbeatTimestampCounter.set(TimeUnit.NANOSECONDS.toMillis(timestamp)); // exported as ms
            injectTapRecordingFault();
            tap.checkRecordingAlive();
            scheduleHeartbeat();
        }
    }

    /**
     * Test-only fault injection for chaos-runner.sh, inert unless {@link #FAULT_INJECTION_ENV} is set:
     * touching {@code <clusterDir>/tap-stall-fault} stops this node's tap recording, the one way to provoke
     * {@link TapPublisher#checkRecordingAlive} from outside the JVM. One-shot per process.
     */
    private void injectTapRecordingFault() {
        if (tapFaultTrigger == null || !Files.exists(tapFaultTrigger)) {
            return;
        }
        tapFaultTrigger = null;
        Logger.info(Logger.CoreComponent.SequencerService, cluster.memberId(),
                    "fault injection: stopping this node's tap recording");
        aeronArchive.stopRecording(FrameLayer.FEEDER_CHANNEL, FrameLayer.FEEDER_STREAM_ID);
    }

    /** Re-arms the cluster clock one heartbeat ahead; see {@link TapPublisher#scheduleHeartbeat}. */
    private void scheduleHeartbeat() {
        tap.scheduleHeartbeat(cluster.time() + FrameLayer.CLUSTER_HEARTBEAT_INTERVAL_NS);
    }

    /**
     * Unsupported by design (see the class javadoc).
     * @param snapshotPublication to which the state should be recorded.
     */
    @Override
    public void onTakeSnapshot(final ExclusivePublication snapshotPublication) {
        throw new UnsupportedOperationException(
            "[SequencerService] Snapshots are not supported: recovery is full-log replay from globalSeqNo 1, "
            + "which is what keeps each node's tap recording complete. Stop the cluster with clusterctl "
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
        ensureCounters();

        final int length = sequencer.leadershipChanged(leadershipTermId, leaderMemberId, timestamp);
        leadershipChangeCounter.increment();
        currentLeaderMemberIdCounter.set(leaderMemberId);
        final boolean leader = leaderMemberId == cluster.memberId();
        Logger.info(Logger.CoreComponent.SequencerService, cluster.memberId(),
                    "leadership change: term %d, new leader is memberId=%d (isLeader=%b)", leadershipTermId,
                    leaderMemberId, leader);
        emit(length);
    }

    /**
     * Termination handler
     * @param cluster with which the service can interact.
     */
    @Override
    public void onTerminate(final Cluster cluster) {
        CloseHelper.quietCloseAll(aeronArchive, tapPub);
        closeCounters();
    }

    /** Closes the operator counters; a node that never reached its first callback created none. */
    private void closeCounters() {
        CloseHelper.quietCloseAll(globalSeqNoCounter, tapBackPressureAlertCounter, tapStalledCounter,
                                  rejectedIngressCounter, leadershipChangeCounter, currentLeaderMemberIdCounter,
                                  lastHeartbeatTimestampCounter, gatewayPromotionCounter,
                                  gatewayPromotionFailedCounter, bootstrapActivatedCounter, connectedClientsCounter,
                                  messagesSequencedCounter);
    }

    /**
     * Publishes the frame in the sequencer's buffer onto the tap; see {@link TapPublisher#emit}.
     * @param length of the encoded frame at offset 0 of the sequencer's buffer
     */
    private void emit(final int length) {
        tap.emit(length);
        globalSeqNoCounter.set(sequencer.globalSeqNo());
        connectedClientsCounter.set(sequencer.connectedClientCount());
    }

    /** The Aeron half of {@link TapPublisher}: calls through to the runtime, no decisions. */
    private final class TapActions implements TapPublisher.Actions {
        @Override
        public long offerFrame(final int length) {
            return tapPub.offer(sequencer.buffer(), 0, length);
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
            return RecordingPos.isActive(counters, tapRecordingCounterId, tapRecordingId);
        }

        @Override
        public long recordedPosition() {
            return counters.getCounterValue(tapRecordingCounterId);
        }

        @Override
        public long recordingId() {
            return tapRecordingId;
        }

        @Override
        public void tapBackPressureAlert() {
            tapBackPressureAlertCounter.increment();
        }

        @Override
        public void tapStalled(final boolean stalled) {
            if (tapStalledCounter != null) { // null before the first callback creates the counters
                tapStalledCounter.set(stalled ? 1 : 0);
            }
        }

        @Override
        public void signalFatal() {
            fatalHandler.run();
        }

        @Override
        public void halt() {
            Runtime.getRuntime().halt(SequencerServer.EXIT_TAP_FATAL);
        }

        @Override
        public Integer memberId() {
            return cluster.memberId();
        }

        @Override
        public long globalSeqNo() {
            return sequencer.globalSeqNo();
        }
    }
}
