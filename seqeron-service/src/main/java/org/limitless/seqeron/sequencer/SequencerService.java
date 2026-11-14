package org.limitless.seqeron.sequencer;

import io.aeron.ChannelUri;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.Counter;
import io.aeron.ExclusivePublication;
import io.aeron.FragmentAssembler;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.cluster.ClusterControl;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.logbuffer.BlockHandler;
import io.aeron.logbuffer.Header;
import io.aeron.protocol.DataHeaderFlyweight;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.TimeUnit;
import org.agrona.BitUtil;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersReader;
import org.limitless.seqeron.protocol.FrameLayer;
import org.limitless.seqeron.protocol.SeqeronCounters;
import org.limitless.seqeron.replayer.server.CutHandover;
import org.limitless.seqeron.replayer.server.DefaultQueueRows;
import org.limitless.seqeron.replayer.server.DerivationStart;
import org.limitless.seqeron.replayer.server.NodeDriver;
import org.limitless.seqeron.replayer.server.RestartSafeLog;
import org.limitless.seqeron.sbe.sequencer.SequencerStateDecoder;
import org.limitless.seqeron.util.Logger;

/**
 * The Aeron adapter over {@link Sequencer}: it decides when to call the state machine and publishes what
 * comes back on this node's queues, one per group. It holds no replicated state.
 *
 * <p>Every node publishes its own queues and records none: its Replayer derives a queue's history from the log
 *. Each queue's publication and drain are created once, when the log activates the
 * groups (§7.4), and span every leader tenure.
 *
 * <p>A cluster snapshot is also an application round: {@link #onTakeSnapshot} publishes the
 * round's marker on every queue and writes the {@link Sequencer}'s state, and {@link #onStart} restores that state and
 * starts each queue again at the cut. It is refused while no {@code <snapshots>} policy is loaded, and recovery is then
 * full-log replay.
 */
public final class SequencerService implements ClusteredService {
    /** Correlation id of the one repeating heartbeat timer; rescheduling it moves its deadline. */
    static final long HEARTBEAT_TIMER_CORRELATION_ID = 0x7100_0000_0000_0001L;

    /** Where a restored queue's publication starts in its term ids; any value serves, positions being what count. */
    private static final int INITIAL_TERM_ID = 0;

    private final Sequencer sequencer;

    /** Everything here that can back-pressure, and the decision to terminate rather than wait. */
    private final QueuePublisher queue;

    /** Brings the whole node down; wired by {@link SequencerServer}. See {@link QueuePublisher}. */
    private final Runnable fatalHandler;

    private final int memberId;

    /** What the Replayer resolves queues from; a restored node feeds it the rows its log no longer replays. */
    private final DefaultQueueRows rows;
    private final Queue<byte[]> topologyRows;

    /** This member's record of its snapshots, and where the cuts it takes are handed to its Replayer. */
    private final RestartSafeLog safeLog;
    private final Queue<CutHandover> cuts;
    private final AtomicReference<DerivationStart> derivationStart;

    /** The term length and MTU the node publishes its queues with, which a restored queue must match to keep its positions. */
    private final int nodeTermLength;
    private final int nodeMtu;

    /** The cluster's snapshot toggle, or null until found. */
    private AtomicCounter controlToggle;

    /** What each queue's last offer returned, and the length it offered, slot groupId - 1. */
    private final long[] lastOfferResults = new long[FrameLayer.MAX_GROUPS];
    private final int[] lastOfferLengths = new int[FrameLayer.MAX_GROUPS];

    // Aeron runtime
    private Cluster cluster;
    private CountersReader counters;

    // Each group's queue, slot groupId - 1; null until activation opens it.
    private final ExclusivePublication[] queuePubs = new ExclusivePublication[FrameLayer.MAX_GROUPS];

    private static final BlockHandler DISCARD = (buffer, offset, length, sessionId, termId) -> { };

    // Each queue's own subscriber, slot groupId - 1: it keeps the queue connected, so that every offer lands and the
    // queue's positions are the ones the log gives.
    private final Subscription[] queueDrainSubs = new Subscription[FrameLayer.MAX_GROUPS];
    private final Image[] queueDrains = new Image[FrameLayer.MAX_GROUPS];

    // Operator counters
    private Counter clusterSeqNoCounter;
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
     * @param topologyRows   receives a copy of each topology row the default queue carries, which the co-located
     *                       replayer resolves instances' queues from (spec R-6)
     * @param fatalHandler   run once, from a cluster callback, when this node can no longer record its own
     *                       queue. Must not block: it signals a shutdown and returns.
     * @param safeLog        this member's {@code restart-safe.log}
     * @param cuts           receives each cluster snapshot this member takes, for its Replayer
     * @param derivationStart receives where this service starts, for its Replayer
     * @param nodeTermLength the IPC term length of the node's driver, which its queues are published with
     * @param nodeMtu        the IPC MTU of the node's driver
     * @param stallTimeoutMs how long one queue offer may stay back-pressured before this node terminates
     */
    public SequencerService(final int memberId, final Queue<byte[]> topologyRows, final Runnable fatalHandler,
                            final RestartSafeLog safeLog, final Queue<CutHandover> cuts,
                            final AtomicReference<DerivationStart> derivationStart, final int nodeTermLength,
                            final int nodeMtu, final long stallTimeoutMs) {
        this.queue = new QueuePublisher(new QueueActions(), TimeUnit.MILLISECONDS.toNanos(stallTimeoutMs));
        this.topologyRows = topologyRows;
        this.safeLog = safeLog;
        this.cuts = cuts;
        this.derivationStart = derivationStart;
        this.nodeTermLength = nodeTermLength;
        this.nodeMtu = nodeMtu;
        this.rows = new DefaultQueueRows(topologyRows::add);
        this.sequencer = new Sequencer(memberId, new Sequencer.FrameSink() {
            @Override
            public void activate(final int[] groupIds) {
                rows.onActivate(groupIds);
                queue.activate(groupIds);
            }

            @Override
            public void publish(final int groupId, final DirectBuffer buffer, final int length) {
                lastOfferResults[groupId - 1] = queue.emit(groupId, buffer, length);
                lastOfferLengths[groupId - 1] = length;
                rows.onPublish(groupId, buffer, length);
            }
        });
        this.memberId = memberId;
        this.fatalHandler = fatalHandler;
    }

    /**
     * Cluster start handler
     * @param cluster       with which the service can interact.
     * @param snapshotImage from which the service can load its archived state which can be null when no snapshot.
     */
    @Override
    public void onStart(final Cluster cluster, final Image snapshotImage) {
        this.cluster = cluster;
        this.counters = cluster.context().aeron().countersReader();
        createCounters();
        if (snapshotImage != null) {
            try {
                restore(snapshotImage);
            } catch (final RuntimeException ex) {
                throw refuseStart("[SequencerService] Refusing to start from the snapshot: " + ex + ". Invalidate it "
                                  + "with clusterctl invalidate-latest-snapshot to start from the one before.");
            }
        } else {
            derivationStart.set(new DerivationStart(0, new LogDeriver.State(List.of(), new long[0]), false,
                                                    new long[FrameLayer.MAX_GROUPS], nodeTermLength, nodeMtu));
        }
    }

    /**
     * Restores the {@link Sequencer} from the snapshot, then starts each queue again at the cut: the marker of the round is the queue's first frame.
     */
    private void restore(final Image snapshotImage) {
        final RuntimeException[] failure = {null};
        final int[] state = {0, 0}; // the next record's index, and the format version
        final FragmentAssembler assembler = new FragmentAssembler((buffer, offset, length, header) -> {
            try {
                final UnsafeBuffer record = new UnsafeBuffer(buffer, offset, length);
                if (state[0] == 0) {
                    state[1] = new SequencerStateDecoder()
                                   .wrapAndApplyHeader(record, 0, new org.limitless.seqeron.sbe.sequencer.MessageHeaderDecoder())
                                   .formatVersion();
                }
                sequencer.onRestore(record, length, state[0]++, state[1]);
            } catch (final RuntimeException ex) {
                failure[0] = ex; // Image.poll would hand it to the error handler and carry on
            }
        });
        while (!sequencer.isRestored() && failure[0] == null) {
            if (snapshotImage.poll(assembler, 10) == 0) {
                if (snapshotImage.isClosed()) {
                    throw new IllegalStateException("the snapshot ended before the Sequencer's state did");
                }
                cluster.idleStrategy().idle();
            }
        }
        if (failure[0] != null) {
            throw failure[0];
        }
        updateCounters();
        final LogDeriver.State restored = LogDeriver.stateOf(
            sequencer, cluster.clientSessions().stream().mapToLong(ClientSession::id).toArray());
        final long[] queueStarts = new long[FrameLayer.MAX_GROUPS];
        if (sequencer.isActive()) {
            final int[] groupIds = sequencer.activeGroups();
            rows.onActivate(groupIds);
            sequencer.replayTopologyRows(topologyRows::add);
            queue.activate(groupIds, groupId -> {
                final QueueStart.Decision start =
                    QueueStart.decide(true, sequencer.markerPosition(groupId), sequencer.queueTermLength(groupId),
                                      sequencer.queueMtu(groupId), nodeTermLength, nodeMtu);
                queueStarts[groupId - 1] = start.position();
                return start;
            });
            sequencer.reemitMarker();
        }
        derivationStart.set(
            new DerivationStart(cluster.logPosition(), restored, true, queueStarts, nodeTermLength, nodeMtu));
        Logger.info(Logger.CoreComponent.SequencerService, memberId,
                    "restored the Sequencer at clusterSeqNo=%d, round %d", sequencer.clusterSeqNo(),
                    sequencer.snapshotRound());
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
        clusterSeqNoCounter = counter(SeqeronCounters.SEQUENCER_CLUSTER_SEQ_NO_TYPE_ID, "clusterSeqNo");
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
        requestRound();
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
                         memberId, "skipping ingress message with no client session (clusterSeqNo stays %d)",
                         sequencer.clusterSeqNo());
            return;
        }
        if (sequencer.sequenceMessage(buffer, offset, length, session.id(), timestamp)) {
            messagesSequencedCounter.increment();
        } else {
            rejectedIngressCounter.increment();
        }
        updateCounters();
        requestRound();
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
            requestRound();
            lastHeartbeatTimestampCounter.set(TimeUnit.NANOSECONDS.toMillis(timestamp)); // exported as ms
            queue.checkRecordingAlive();
            scheduleHeartbeat();
        }
    }

    /** Re-arms the cluster clock one heartbeat ahead; see {@link QueuePublisher#scheduleHeartbeat}. */
    private void scheduleHeartbeat() {
        queue.scheduleHeartbeat(cluster.time() + FrameLayer.CLUSTER_HEARTBEAT_INTERVAL_NS);
    }

    /**
     * Asks the cluster for a snapshot while a round is due and this node leads. The consensus
     * module reads its toggle only on the leader, outside an election, so a request set elsewhere would wait and take an
     * extra snapshot once that member leads; and it switches only from neutral, by compare-and-set, so a request that
     * meets an operator's or one in progress fails and is made again at the next callback.
     */
    private void requestRound() {
        if (!sequencer.snapshotDue() || cluster.role() != Cluster.Role.LEADER) {
            return;
        }
        if (controlToggle == null) {
            controlToggle = ClusterControl.findControlToggle(counters, cluster.context().clusterId());
        }
        if (controlToggle != null) {
            ClusterControl.ToggleState.SNAPSHOT.toggle(controlToggle);
        }
    }

    /**
     * A cluster snapshot, which is an application round: the {@link Sequencer} publishes the
     * round's marker on every queue, then its state is written, which acks. Nothing waits for an application. Refused
     * while no {@code <snapshots>} policy is loaded, so a cluster without one keeps its full-log replay.
     *
     * <p>A node writes the cut to its {@code restart-safe.log} before it acks, and hands it to its Replayer. A snapshot
     * action replayed from the log finds its cut already there and hands nothing over: whether it was restart-safe was
     * decided in the run that took it.
     * @param snapshotPublication to which the state should be recorded.
     */
    @Override
    public void onTakeSnapshot(final ExclusivePublication snapshotPublication) {
        if (!sequencer.hasSnapshotPolicy()) {
            throw new IllegalStateException(
                "[SequencerService] Refusing to take a snapshot: no <snapshots> policy is loaded, so recovery is "
                + "full-log replay from clusterSeqNo 1. Load a topology with <snapshots>.");
        }
        final long logPosition = cluster.logPosition();
        final boolean live = !safeLog.isCut(logPosition);
        if (live) {
            safeLog.appendCut(logPosition);
        }
        sequencer.takeSnapshot(cluster.time());
        recordQueueCuts();
        writeState(snapshotPublication);
        if (live && sequencer.isActive()) {
            final long[] markerPositions = new long[FrameLayer.MAX_GROUPS];
            for (int groupId = 1; groupId <= FrameLayer.MAX_GROUPS; groupId++) {
                markerPositions[groupId - 1] = queuePubs[groupId - 1] == null ? -1 : sequencer.markerPosition(groupId);
            }
            cuts.add(new CutHandover(sequencer.snapshotRound(), sequencer.clusterSeqNo(), logPosition, markerPositions));
        }
        updateCounters();
    }

    /** Tells the {@link Sequencer} where each queue's marker started, and what the queue was published with. */
    private void recordQueueCuts() {
        for (final int groupId : sequencer.activeGroups()) {
            final ExclusivePublication publication = queuePubs[groupId - 1];
            final long end = lastOfferResults[groupId - 1];
            final int frame = BitUtil.align(lastOfferLengths[groupId - 1] + DataHeaderFlyweight.HEADER_LENGTH,
                                            io.aeron.logbuffer.FrameDescriptor.FRAME_ALIGNMENT);
            sequencer.queueCut(groupId, end - frame, publication.termBufferLength(),
                               publication.maxPayloadLength() + DataHeaderFlyweight.HEADER_LENGTH);
        }
    }

    /** Offers the {@link Sequencer}'s records to the snapshot publication, waiting out back-pressure. */
    private void writeState(final ExclusivePublication publication) {
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(4096);
        for (int recordIndex = 0;; recordIndex++) {
            final int length = sequencer.onSnapshot(buffer, recordIndex);
            if (length == 0) {
                return;
            }
            long result;
            while ((result = publication.offer(buffer, 0, length)) < 0) {
                if (result == Publication.CLOSED || result == Publication.MAX_POSITION_EXCEEDED
                    || result == Publication.NOT_CONNECTED) {
                    throw new IllegalStateException("the snapshot publication refused record " + recordIndex + ": " + result);
                }
                cluster.idleStrategy().idle();
            }
        }
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
        requestRound();
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
        CloseHelper.quietCloseAll(queueDrainSubs);
        CloseHelper.quietCloseAll(queuePubs);
        closeCounters();
    }

    /** Closes the operator counters; a node whose start was refused created none. */
    private void closeCounters() {
        CloseHelper.quietCloseAll(clusterSeqNoCounter, queueBackPressureAlertCounter, queueStalledCounter,
                                  rejectedIngressCounter, leadershipChangeCounter, currentLeaderMemberIdCounter,
                                  lastHeartbeatTimestampCounter, gatewayPromotionCounter, gatewayPromotionFailedCounter,
                                  bootstrapActivatedCounter, messagesSequencedCounter);
    }

    /** Mirrors the sequencer's state onto the operator counters after a callback. */
    private void updateCounters() {
        clusterSeqNoCounter.set(sequencer.clusterSeqNo());
        gatewayPromotionCounter.set(sequencer.gatewayPromotionCount());
        gatewayPromotionFailedCounter.set(sequencer.gatewayPromotionFailedCount());
        bootstrapActivatedCounter.set(sequencer.bootstrapActivationEmitted() ? 1 : 0);
    }

    /** The Aeron half of {@link QueuePublisher}: calls through to the runtime, no decisions. */
    private final class QueueActions implements QueuePublisher.Actions {
        @Override
        public void openQueue(final int groupId, final QueueStart.Decision start) {
            final int streamId = FrameLayer.queueStreamId(groupId);
            final String channel = start.kind() == QueueStart.Kind.AT_POSITION
                ? new ChannelUriStringBuilder(FrameLayer.QUEUE_CHANNEL)
                      .initialPosition(start.position(), INITIAL_TERM_ID, nodeTermLength)
                      .build()
                : FrameLayer.QUEUE_CHANNEL;
            queuePubs[groupId - 1] = cluster.context().aeron().addExclusivePublication(channel, streamId);
            queueDrainSubs[groupId - 1] = cluster.context().aeron().addSubscription(
                ChannelUri.addSessionId(FrameLayer.QUEUE_CHANNEL, queuePubs[groupId - 1].sessionId()), streamId);
        }

        @Override
        public boolean attachRecording(final int groupId) {
            queueDrains[groupId - 1] = queueDrainSubs[groupId - 1].imageBySessionId(queuePubs[groupId - 1].sessionId());
            return queueDrains[groupId - 1] != null;
        }

        @Override
        public long offerFrame(final int groupId, final DirectBuffer buffer, final int length) {
            final long result = queuePubs[groupId - 1].offer(buffer, 0, length);
            if (result > 0) {
                final Image drain = queueDrains[groupId - 1];
                while (drain.position() < result && !drain.isClosed()) {
                    drain.blockPoll(DISCARD, Integer.MAX_VALUE); // a block ends with its term
                }
            }
            return result;
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
        public boolean recordingActive(final int groupId) {
            return !queueDrains[groupId - 1].isClosed();
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
        public long clusterSeqNo() {
            return sequencer.clusterSeqNo();
        }
    }
}
