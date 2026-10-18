package org.limitless.seqeron.app;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.cluster.client.EgressListener;
import io.aeron.cluster.codecs.EventCode;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.limitless.seqeron.protocol.FrameLayer;
import org.limitless.seqeron.protocol.Publish;
import org.limitless.seqeron.protocol.ReplayProtocol;
import org.limitless.seqeron.protocol.SystemFrame;
import org.limitless.seqeron.replayer.client.ReplayerStreamReceiver;
import org.limitless.seqeron.replayer.client.SequencedEvent;
import org.limitless.seqeron.replayer.client.SnapshotRestoreHandler;
import org.limitless.seqeron.replayer.client.SnapshotStore;
import org.limitless.seqeron.sbe.replay.MessageHeaderEncoder;
import org.limitless.seqeron.sbe.replay.QueueRequestEncoder;
import org.limitless.seqeron.sequencer.client.ClusterStreamSender;
import org.limitless.seqeron.sequencer.client.IngressPublisher;
import org.limitless.seqeron.sequencer.client.PendingSends;
import org.limitless.seqeron.util.Clocks;
import org.limitless.seqeron.util.Logger;

/**
 * What every seqeron client does the same way: the cluster session it submits on, the co-located queue it
 * follows, confirmed ingress across a failover, and the fences that say when it may no longer act — wired
 * together into one {@link #doWork()} whose ordering is not the caller's to get right.
 *
 * <p>Not API. {@link Gateway} is the façade over it; it holds no election and no leader gate of its own,
 * so a co-located application's façade can sit on the same core.
 */
final class Session implements AutoCloseable {
    /** The deployment policy every producer had been copying: the queue may be silent for 20 heartbeat periods. */
    static final long DEFAULT_QUEUE_STALL_TIMEOUT_MS = 20 * FrameLayer.CLUSTER_HEARTBEAT_INTERVAL_MS;

    /** Longer than the queue's, since a re-walk is slower than the live stream it is catching up to. */
    static final long DEFAULT_RECOVERY_STALL_TIMEOUT_MS = 3 * DEFAULT_QUEUE_STALL_TIMEOUT_MS;

    /** Frames in flight between a publish and the queue; far above what one round trip holds. */
    static final int DEFAULT_PENDING_CAPACITY = 1024;

    /** How often an attempt to replace a lost session begins while the façade allows it. */
    static final long RECONNECT_INTERVAL_MS = 1_000;

    /** No group is resolved yet (spec A-9), so a frame addresses none and §9.2 condition 14 refuses it. */
    private static final int UNRESOLVED_GROUP_MASK = 0;

    /** Untethered, as the receiver's is: a reply this instance does not read must not hold up the Replayer. */
    private static final String CONTROL_CHANNEL = ReplayProtocol.IPC_CHANNEL + "|tether=false";

    private static final int CONTROL_FRAGMENT_LIMIT = 16;

    /** What the façade above does with what comes off the queue, and with a fence. */
    interface Dispatch {
        /** A system frame this core does not consume itself. {@code LeadershipChanged} never arrives here. */
        void onSystem(SequencedEvent event);

        /** A {@code LeadershipChanged} was applied; confirmed ingress has already taken the new term. */
        void onLeadershipChanged();

        void onPayload(Payload payload);

        /** Every transition to caught-up, the first included. */
        void onCaughtUp(long clusterSeqNo);

        /** The cluster clock's tick, once a second. */
        void onClusterHeartbeat(long clusterTimeNs, long receiveTimeNs);

        /** Once, latched: this producer may no longer act. */
        void onFenced(ClusterError fence, String detail);

        /**
         * Whether a session the cluster closed may be replaced rather than fenced: the instance holds nothing the
         * loss of that session changed. Asked on every cycle without one.
         */
        boolean mayReconnect();

        /** Whether this instance acts for its producer right now: a stall fences only one that does. */
        boolean isActing();
    }

    private final Dispatch dispatch;
    private final PendingSends pending;
    private final IngressPublisher publisher;
    private final ClusterStreamSender sender = new ClusterStreamSender();
    private final ReplayerStreamReceiver receiver;
    private final QueueResolution queueResolution = new QueueResolution(new QueueResolutionActions());
    private final RecoveryStallFence recoveryStall;
    private final QueueStallFence queueStall;
    private final Payload payload = new Payload();

    private final long recoveryStallTimeoutMs;
    private final long queueStallTimeoutMs;

    /** The stall an instance that does not act has been alarmed for, or null. */
    private ClusterError stallAlarm;

    /** The cluster's own account of why this session ended, kept for the fence's detail. */
    private String sessionFault;

    private boolean caughtUp;
    private boolean fenced;

    /** Monotonic ms when the session was found lost; -1 while one is open. */
    private long sessionLostSinceMs = -1;
    private long nextReconnectMs;

    /** The term of the last {@code LeadershipChanged} applied; -1 before the first. */
    private long leadershipTermId = -1;

    /** What the {@code QueueRequest} names: the instance's topology row (spec R-6). */
    private final int queueSourceId;
    private final String queueGatewayName;

    // The queue's resolution, until the receiver starts on it.
    private Aeron aeron;
    private int memberId;
    private long queueClientId = -1;
    private Publication queueRequestPublication;
    private Subscription queueControlSubscription;
    private final FragmentHandler queueControlHandler =
        (buffer, offset, length, header) -> queueResolution.onControl(buffer, offset, length);
    private final MessageHeaderEncoder queueRequestHeader = new MessageHeaderEncoder();
    private final QueueRequestEncoder queueRequest = new QueueRequestEncoder();
    private final UnsafeBuffer queueRequestBuffer =
        new UnsafeBuffer(new byte[MessageHeaderEncoder.ENCODED_LENGTH + QueueRequestEncoder.BLOCK_LENGTH]);

    /**
     * @param sourceId    the application's {@code sourceId} its row names, or {@link ReplayProtocol#NO_SOURCE_ID}
     *                    for a gateway instance
     * @param gatewayName the gateway instance's row name, or null for an application
     */
    Session(final int clientId, final int pendingCapacity, final long queueStallTimeoutMs,
            final long recoveryStallTimeoutMs, final int sourceId, final String gatewayName, final Dispatch dispatch) {
        this.dispatch = dispatch;
        this.queueSourceId = sourceId;
        this.queueGatewayName = gatewayName == null ? "" : gatewayName;
        this.queueStallTimeoutMs = queueStallTimeoutMs;
        this.recoveryStallTimeoutMs = recoveryStallTimeoutMs;
        this.pending = new PendingSends(pendingCapacity);
        this.publisher = new IngressPublisher(pending);
        this.recoveryStall = new RecoveryStallFence(recoveryStallTimeoutMs);
        this.queueStall = new QueueStallFence(queueStallTimeoutMs);
        this.receiver = new ReplayerStreamReceiver(clientId, this::onSequenced, this::onLeadershipChanged, null);
        sender.setIngressHold(pending);
    }

    /** Restores the newest snapshot in {@code store} first; call before starting. */
    void restoreFrom(final SnapshotStore store, final SnapshotRestoreHandler handler) {
        receiver.restoreFrom(store, handler);
    }

    /** Opens the cluster session over UDP and asks this node's Replayer which queue to follow. */
    void start(final Aeron aeron, final int memberId, final String egressChannel, final String ingressEndpoints) {
        sender.connect(aeron, egressChannel, ingressEndpoints, new SessionListener());
        resolveQueue(aeron, memberId);
    }

    /**
     * The same for a client sharing this member's media driver: ingress over its own {@code aeron:ipc} while
     * that member leads, the UDP endpoint set when it does not.
     */
    void startColocated(final Aeron aeron, final int memberId, final long ipcConnectTimeoutMs,
                        final String egressChannel, final String ingressEndpoints) {
        sender.connectColocated(aeron, memberId, ipcConnectTimeoutMs, egressChannel, ingressEndpoints,
                                new SessionListener());
        resolveQueue(aeron, memberId);
    }

    /** The receiver starts once the Replayer has said which queue this instance reads (spec R-6). */
    private void resolveQueue(final Aeron aeron, final int memberId) {
        this.aeron = aeron;
        this.memberId = memberId;
        queueClientId = aeron.nextCorrelationId();
        queueControlSubscription = aeron.addSubscription(CONTROL_CHANNEL, ReplayProtocol.CONTROL_STREAM_ID);
        queueRequestPublication = aeron.addPublication(ReplayProtocol.IPC_CHANNEL, ReplayProtocol.REQUEST_STREAM_ID);
        queueResolution.start();
    }

    /** Waits for the Replayer's answer, unfenced, then starts the receiver on that queue. */
    private int pollQueueResolution() {
        if (queueControlSubscription == null) {
            return 0;
        }
        int work = queueControlSubscription.poll(queueControlHandler, CONTROL_FRAGMENT_LIMIT);
        queueResolution.doTimers(!queueRequestPublication.isConnected());
        final int groupId = queueResolution.groupId();
        if (groupId != QueueResolution.UNRESOLVED) {
            receiver.start(aeron, memberId, groupId);
            closeQueueResolution();
            ++work;
        }
        return work;
    }

    private void closeQueueResolution() {
        if (queueControlSubscription != null) {
            queueControlSubscription.close();
            queueControlSubscription = null;
        }
        if (queueRequestPublication != null) {
            queueRequestPublication.close();
            queueRequestPublication = null;
        }
    }

    /**
     * One duty cycle, in the order the parts require: the queue first, so a fence is judged on what this cycle
     * saw; the resend last, so nothing new goes out ahead of it.
     * @return units of work done
     */
    int doWork() {
        if (fenced) {
            return 0;
        }
        int work = queueResolution.groupId() == QueueResolution.UNRESOLVED ? pollQueueResolution() : receiver.poll();
        checkCaughtUp();
        checkFences();
        if (fenced) {
            // Here rather than at the fence, which may be raised from inside that poll. Nothing polls the receiver
            // again, and a replay left open would hold one of the member's Replayer slots.
            receiver.close();
            closeQueueResolution();
            return work;
        }
        work += sender.pollEgress();
        sender.keepAlive();
        work += pending.resendMissing(sender);
        return work;
    }

    Publish publishPayload(final int sourceId, final int connectionId, final int payloadId,
                           final DirectBuffer body, final int bodyLength) {
        return fenced ? Publish.Declined
                      : publisher.publishPayload(sender, sourceId, connectionId, payloadId, UNRESOLVED_GROUP_MASK,
                                                 body, bodyLength);
    }

    Publish publishSystem(final int sourceId, final int connectionId, final int systemEventType,
                          final DirectBuffer body, final int bodyLength) {
        return fenced ? Publish.Declined
                      : publisher.publishSystem(sender, sourceId, connectionId, systemEventType, body, bodyLength);
    }

    /**
     * Tells the cluster this client is alive, for a façade whose consumer spins inside a callback and would
     * otherwise starve the one in {@link #doWork()}. Self-throttling, so calling it per spin costs nothing.
     */
    void keepAlive() {
        if (!fenced) {
            sender.keepAlive();
        }
    }

    /** Send nothing new while this holds: an older term's frames are still unseen or unresent. */
    boolean isHolding() {
        return pending.isHolding();
    }

    boolean isCaughtUp() {
        return receiver.isCaughtUp();
    }

    boolean isFenced() {
        return fenced;
    }

    long lastClusterSeqNo() {
        return receiver.lastClusterSeqNo();
    }

    int currentLeaderMemberId() {
        return receiver.currentLeaderMemberId();
    }

    long leadershipTermId() {
        return leadershipTermId;
    }

    @Override
    public void close() {
        receiver.close();
        closeQueueResolution();
        sender.close();
    }

    /** Polled rather than taken off the receiver's callback: neither fence may wait on the next frame to land. */
    private void checkCaughtUp() {
        final boolean now = receiver.isCaughtUp();
        if (now == caughtUp) {
            return;
        }
        caughtUp = now;
        if (!now) {
            return;
        }
        final long nowMs = Clocks.monotonicMs();
        recoveryStall.onCaughtUp();
        queueStall.onCaughtUp(nowMs);
        dispatch.onCaughtUp(receiver.lastClusterSeqNo());
    }

    private void checkFences() {
        if (!hasSession()) {
            final String why = sessionFault != null ? sessionFault : "closed";
            if (!dispatch.mayReconnect()) {
                fence(ClusterError.CLUSTER_SESSION_LOST, why);
                return;
            }
            replaceSession(why);
            if (fenced) {
                return;
            }
        }
        if (pending.isFaulted()) {
            fence(ClusterError.INGRESS_CONFIRM_FAULTED,
                  "an own frame came back differing from the oldest pending one, so what reached the log can no "
                      + "longer be counted");
            return;
        }
        if (receiver.restoreFailure() != null) {
            fence(ClusterError.SNAPSHOT_UNRESTORABLE, receiver.restoreFailure());
            return;
        }
        final long nowMs = Clocks.monotonicMs();
        if (!receiver.isCaughtUp()) {
            if (recoveryStall.onNotCaughtUp(nowMs, receiver.lastClusterSeqNo())) {
                stalled(ClusterError.RECOVERY_STALLED, Logger.CoreEventCode.RecoveryStalled,
                        "recovery has dispatched nothing for >" + recoveryStallTimeoutMs +
                            "ms (clusterSeqNo stuck at " + receiver.lastClusterSeqNo() + ")");
                return;
            }
        } else if (queueStall.isStalled(nowMs)) {
            stalled(ClusterError.QUEUE_STALLED, Logger.CoreEventCode.QueueStalled,
                    "no ClusterHeartbeat for >" + queueStallTimeoutMs + "ms");
            return;
        }
        if (stallAlarm != null) {
            Logger.info(Logger.CoreComponent.Cluster, null, "%s cleared", stallAlarm);
            stallAlarm = null;
        }
    }

    /** Fences an instance that acts; one that does not raises an alarm once and keeps following the queue. */
    private void stalled(final ClusterError stall, final Logger.CoreEventCode code, final String detail) {
        if (dispatch.isActing()) {
            fence(stall, detail);
            return;
        }
        if (stallAlarm != stall) {
            stallAlarm = stall;
            Logger.error(Logger.CoreComponent.Cluster, code, null,
                         "%s, not fenced while this instance does not act: %s", stall, detail);
        }
    }

    /**
     * Whether a cluster session is open. The recorded fault and {@code isConnected()} as well as the sender's
     * latch: {@code AeronCluster} also closes itself, with no event at all, when a new leader does not arrive
     * before its timeout. And the queue, whose {@code SessionClosed} is the cluster's own word.
     */
    boolean hasSession() {
        return sessionFault == null && !sender.isSessionLost() && sender.isConnected() &&
               !pending.isClosed(sender.clusterSessionId());
    }

    /**
     * Opens a session in place of the lost one, an attempt at most once a second and each advanced a step per cycle,
     * until one opens or the queue could have gone silent for as long: past that the cluster is not coming back for
     * this process, and the session's loss is a fence. An attempt under way then is let finish, since one whose
     * request an election swallowed spends its whole timeout.
     */
    private void replaceSession(final String why) {
        final long nowMs = Clocks.monotonicMs();
        if (sessionLostSinceMs < 0) {
            sessionLostSinceMs = nowMs;
            nextReconnectMs = nowMs;
        }
        if (nowMs - sessionLostSinceMs > queueStallTimeoutMs && !sender.isReconnecting()) {
            fence(ClusterError.CLUSTER_SESSION_LOST,
                  why + "; no session replaced it within " + queueStallTimeoutMs + "ms");
            return;
        }
        if (!sender.isReconnecting()) {
            if (nowMs < nextReconnectMs) {
                return;
            }
            nextReconnectMs = nowMs + RECONNECT_INTERVAL_MS;
        }
        if (!sender.reconnect()) {
            return;
        }
        sessionFault = null;
        sessionLostSinceMs = -1;
        if (pending.size() > 0) {
            Logger.info(Logger.CoreComponent.Cluster, null,
                        "%d unconfirmed frames hold new ones until the queue settles them: the session they went out "
                            + "on was lost (%s)", pending.size(), why);
        }
    }

    /** Forgets every frame not yet seen on the queue, for a façade that knows none of them will be sequenced. */
    void discardUnconfirmed(final String why) {
        final int dropped = pending.discardUnconfirmed();
        if (dropped > 0) {
            Logger.info(Logger.CoreComponent.Cluster, null, "%d unconfirmed frames are not resent: %s", dropped, why);
        }
    }

    /**
     * Latches the first fence and releases the cluster session, so a designated instance's sibling is promoted now
     * rather than when the application exits; a façade raises its own, such as a diverged snapshot, through this too.
     */
    void fence(final ClusterError reason, final String detail) {
        if (fenced) {
            return;
        }
        fenced = true;
        sender.close();
        dispatch.onFenced(reason, detail);
    }

    /** Confirmed ingress takes the term first: nothing new may go out before the hold it may place is on. */
    private void onLeadershipChanged(final int leaderMemberId, final long leadershipTermId,
                                     final long clusterSeqNo) {
        pending.onLeadershipChanged(leadershipTermId);
        this.leadershipTermId = leadershipTermId;
        dispatch.onLeadershipChanged();
    }

    private void onSequenced(final SequencedEvent event) {
        pending.onSequenced(event);
        if (event.isSystem()) {
            if (event.systemEventType() == SystemFrame.CLUSTER_HEARTBEAT) {
                queueStall.onClusterHeartbeat(Clocks.monotonicMs());
                dispatch.onClusterHeartbeat(event.clusterTimestampNs(), event.receiveTimeNs());
            }
            dispatch.onSystem(event);
            return;
        }
        payload.wrap(event);
        dispatch.onPayload(payload);
    }

    /** The Aeron half of {@link QueueResolution}. */
    private final class QueueResolutionActions implements QueueResolution.Actions {
        @Override
        public void sendQueueRequest(final long requestId) {
            if (queueRequestPublication == null || !queueRequestPublication.isConnected()) {
                return; // Replayer not up yet; the resend timer retries
            }
            queueRequest.wrapAndApplyHeader(queueRequestBuffer, 0, queueRequestHeader)
                .clientId(queueClientId)
                .requestId(requestId)
                .sourceId(queueSourceId)
                .gatewayName(queueGatewayName);
            queueRequestPublication.offer(queueRequestBuffer, 0,
                                          MessageHeaderEncoder.ENCODED_LENGTH + queueRequest.encodedLength());
        }

        @Override
        public long clientId() {
            return queueClientId;
        }

        @Override
        public long nowMs() {
            return Clocks.monotonicMs();
        }
    }

    /** Records why the cluster ended this session; {@link #checkFences} is where it becomes a fence. */
    private final class SessionListener implements EgressListener {
        @Override
        public void onMessage(final long clusterSessionId, final long timestamp, final DirectBuffer buffer,
                              final int offset, final int length, final Header header) {
            // Nothing is addressed to a producer on egress: everything it publishes comes back on the queue.
        }

        @Override
        public void onSessionEvent(final long correlationId, final long clusterSessionId,
                                   final long leadershipTermId, final int leaderMemberId, final EventCode code,
                                   final String detail) {
            if (code == EventCode.ERROR || code == EventCode.CLOSED) {
                sessionFault = code + ": " + detail;
            }
        }
    }
}
