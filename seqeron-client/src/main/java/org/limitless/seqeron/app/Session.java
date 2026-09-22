package org.limitless.seqeron.app;

import io.aeron.Aeron;
import io.aeron.cluster.client.EgressListener;
import io.aeron.cluster.codecs.EventCode;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.limitless.seqeron.protocol.FrameLayer;
import org.limitless.seqeron.protocol.Publish;
import org.limitless.seqeron.protocol.SystemFrame;
import org.limitless.seqeron.replayer.client.ReplayerStreamReceiver;
import org.limitless.seqeron.replayer.client.SequencedEvent;
import org.limitless.seqeron.sequencer.client.ClusterStreamSender;
import org.limitless.seqeron.sequencer.client.IngressPublisher;
import org.limitless.seqeron.sequencer.client.PendingSends;
import org.limitless.seqeron.util.Clocks;

/**
 * What every seqeron client does the same way: the cluster session it submits on, the co-located tap it
 * follows, confirmed ingress across a failover, and the fences that say when it may no longer act — wired
 * together into one {@link #doWork()} whose ordering is not the caller's to get right.
 *
 * <p>Not API. {@link Gateway} is the façade over it; it holds no election and no leader gate of its own,
 * so a co-located application's façade can sit on the same core.
 */
final class Session implements AutoCloseable {
    /** The deployment policy every producer had been copying: the tap may be silent for 20 heartbeat periods. */
    static final long DEFAULT_TAP_STALL_TIMEOUT_MS = 20 * FrameLayer.CLUSTER_HEARTBEAT_INTERVAL_MS;

    /** Longer than the tap's, since a re-walk is slower than the live stream it is catching up to. */
    static final long DEFAULT_RECOVERY_STALL_TIMEOUT_MS = 3 * DEFAULT_TAP_STALL_TIMEOUT_MS;

    /** Frames in flight between a publish and the tap; far above what one round trip holds. */
    static final int DEFAULT_PENDING_CAPACITY = 1024;

    /**
     * The lag at which the tap is called stale. The same span as the tap-silence timeout: a tap a whole
     * stall window behind is as good as silent to anything reading it.
     */
    static final long DEFAULT_TAP_LAG_THRESHOLD_MS = DEFAULT_TAP_STALL_TIMEOUT_MS;

    /** What the façade above does with what comes off the tap, and with a fence. */
    interface Dispatch {
        /** A system frame this core does not consume itself. {@code LeadershipChanged} never arrives here. */
        void onSystem(SequencedEvent event);

        /** A {@code LeadershipChanged} was applied; confirmed ingress has already taken the new term. */
        void onLeadershipChanged();

        void onPayload(Payload payload);

        /** Every transition to caught-up, the first included. */
        void onCaughtUp(long globalSeqNo);

        /** The cluster clock's tick, once a second. */
        void onClusterHeartbeat(long clusterTimeNs, long receiveTimeNs);

        /** Once, latched: this producer may no longer act. */
        void onFenced(ClusterError fence, String detail);
    }

    private final Dispatch dispatch;
    private final PendingSends pending;
    private final IngressPublisher publisher;
    private final ClusterStreamSender sender = new ClusterStreamSender();
    private final ReplayerStreamReceiver receiver;
    private final RecoveryStallFence recoveryStall;
    private final TapStallFence tapStall;
    private final TapLagMonitor tapLag;
    private final Payload payload = new Payload();

    private final long recoveryStallTimeoutMs;
    private final long tapStallTimeoutMs;

    /** The cluster's own account of why this session ended, kept for the fence's detail. */
    private String sessionFault;

    private boolean caughtUp;
    private boolean fenced;

    Session(final int clientId, final int pendingCapacity, final long tapStallTimeoutMs,
            final long recoveryStallTimeoutMs, final long tapLagThresholdMs, final Dispatch dispatch) {
        this.dispatch = dispatch;
        this.tapStallTimeoutMs = tapStallTimeoutMs;
        this.recoveryStallTimeoutMs = recoveryStallTimeoutMs;
        this.pending = new PendingSends(pendingCapacity);
        this.publisher = new IngressPublisher(pending);
        this.recoveryStall = new RecoveryStallFence(recoveryStallTimeoutMs);
        this.tapStall = new TapStallFence(tapStallTimeoutMs);
        this.tapLag = new TapLagMonitor(tapLagThresholdMs * 1_000_000L);
        this.receiver = new ReplayerStreamReceiver(clientId, this::onSequenced, this::onLeadershipChanged, null);
        sender.setIngressHold(pending);
    }

    /** Opens the cluster session over UDP and starts following this node's tap. */
    void start(final Aeron aeron, final int memberId, final String egressChannel, final String ingressEndpoints) {
        sender.connect(aeron, egressChannel, ingressEndpoints, new SessionListener());
        receiver.start(aeron, memberId);
    }

    /**
     * The same for a client sharing this member's media driver: ingress over its own {@code aeron:ipc} while
     * that member leads, the UDP endpoint set when it does not.
     */
    void startColocated(final Aeron aeron, final int memberId, final long ipcConnectTimeoutMs,
                        final String egressChannel, final String ingressEndpoints) {
        sender.connectColocated(aeron, memberId, ipcConnectTimeoutMs, egressChannel, ingressEndpoints,
                                new SessionListener());
        receiver.start(aeron, memberId);
    }

    /**
     * One duty cycle, in the order the parts require: the tap first, so a fence is judged on what this cycle
     * saw; the resend last, so nothing new goes out ahead of it.
     * @return units of work done
     */
    int doWork() {
        if (fenced) {
            return 0;
        }
        int work = receiver.poll();
        checkCaughtUp();
        checkFences();
        if (fenced) {
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
                      : publisher.publishPayload(sender, sourceId, connectionId, payloadId, body, bodyLength);
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

    long lastGlobalSeqNo() {
        return receiver.lastGlobalSeqNo();
    }

    /** Observation only, for a consumer that reports lag itself; nothing here raises a fence. */
    TapLagMonitor tapLag() {
        return tapLag;
    }

    int currentLeaderMemberId() {
        return receiver.currentLeaderMemberId();
    }

    @Override
    public void close() {
        receiver.close();
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
        tapStall.onCaughtUp(nowMs);
        dispatch.onCaughtUp(receiver.lastGlobalSeqNo());
    }

    private void checkFences() {
        // isConnected() as well as the recorded fault: AeronCluster also closes itself, with no event at all,
        // when a new leader does not arrive before its timeout.
        if (sessionFault != null || sender.isSessionLost() || !sender.isConnected()) {
            fence(ClusterError.CLUSTER_SESSION_LOST, sessionFault != null ? sessionFault : "closed");
            return;
        }
        if (pending.isFaulted()) {
            fence(ClusterError.INGRESS_CONFIRM_FAULTED,
                  "an own frame came back differing from the oldest pending one, so what reached the log can no "
                      + "longer be counted");
            return;
        }
        final long nowMs = Clocks.monotonicMs();
        if (!receiver.isCaughtUp()) {
            if (recoveryStall.onNotCaughtUp(nowMs, receiver.lastGlobalSeqNo())) {
                fence(ClusterError.RECOVERY_STALLED, "recovery has dispatched nothing for >" + recoveryStallTimeoutMs
                                                  + "ms (globalSeqNo stuck at " + receiver.lastGlobalSeqNo() + ")");
            }
            return;
        }
        if (tapStall.isStalled(nowMs)) {
            fence(ClusterError.TAP_STALLED, "no ClusterHeartbeat for >" + tapStallTimeoutMs + "ms");
        }
    }

    private void fence(final ClusterError reason, final String detail) {
        fenced = true;
        dispatch.onFenced(reason, detail);
    }

    /** Confirmed ingress takes the term first: nothing new may go out before the hold it may place is on. */
    private void onLeadershipChanged(final int leaderMemberId, final long leadershipTermId,
                                     final long globalSeqNo) {
        pending.onLeadershipChanged(leadershipTermId);
        dispatch.onLeadershipChanged();
    }

    private void onSequenced(final SequencedEvent event) {
        pending.onSequenced(event);
        if (event.isSystem()) {
            if (event.systemEventType() == SystemFrame.CLUSTER_HEARTBEAT) {
                tapStall.onClusterHeartbeat(Clocks.monotonicMs());
                // Lag is judged on the live stream only: a replayed heartbeat is arbitrarily late by
                // construction and says nothing about how far behind the leader this node is now.
                if (receiver.isCaughtUp()) {
                    tapLag.onClusterHeartbeat(event.clusterTimestampNs(), event.receiveTimeNs());
                }
                dispatch.onClusterHeartbeat(event.clusterTimestampNs(), event.receiveTimeNs());
            }
            dispatch.onSystem(event);
            return;
        }
        payload.wrap(event);
        dispatch.onPayload(payload);
    }

    /** Records why the cluster ended this session; {@link #checkFences} is where it becomes a fence. */
    private final class SessionListener implements EgressListener {
        @Override
        public void onMessage(final long clusterSessionId, final long timestamp, final DirectBuffer buffer,
                              final int offset, final int length, final Header header) {
            // Nothing is addressed to a producer on egress: everything it publishes comes back on the tap.
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
