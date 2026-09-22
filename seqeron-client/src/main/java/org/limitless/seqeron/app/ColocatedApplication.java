package org.limitless.seqeron.app;

import io.aeron.Aeron;
import java.util.Objects;
import org.agrona.DirectBuffer;
import org.limitless.seqeron.protocol.PortLayout;
import org.limitless.seqeron.protocol.Publish;
import org.limitless.seqeron.replayer.client.SequencedEvent;

/**
 * One replica of a co-located application — the kind of producer nothing elects. One runs per node, the
 * topology's {@code <applications>} section names it, and {@code LeadershipChanged} already picks the
 * replica that submits: this one publishes only while its own node leads. What that takes is behind it —
 * the cluster session over the node's own {@code aeron:ipc}, the tap it follows, the leader gate,
 * confirmed ingress across a failover, and the fences that say it may no longer act.
 *
 * <p><b>What is left to the consumer is its own work</b>: the payloads it reads, the state it keeps, the
 * payloads it submits. Every replica reads the same ordered stream and so holds the same state, which is
 * what makes {@link OutstandingWork} — fed that same stream — the way work survives the gate closing
 * under it.
 *
 * <p>Single-threaded: every method belongs to the caller's one duty-cycle thread, which calls
 * {@link #doWork()} each iteration. The C++ twin is {@code app/ColocatedApplication.hpp}; keep the two in
 * step.
 */
public final class ColocatedApplication implements AutoCloseable {
    /** How long the tap may be silent before this replica is fenced — 20 heartbeat periods. */
    public static final long DEFAULT_TAP_STALL_TIMEOUT_MS = Session.DEFAULT_TAP_STALL_TIMEOUT_MS;

    /** How long recovery may dispatch nothing, once caught up before; longer, as a re-walk is slower. */
    public static final long DEFAULT_RECOVERY_STALL_TIMEOUT_MS = Session.DEFAULT_RECOVERY_STALL_TIMEOUT_MS;

    /** The lag at which this node's tap is called stale — the same span as the tap-silence timeout. */
    public static final long DEFAULT_TAP_LAG_THRESHOLD_MS = Session.DEFAULT_TAP_LAG_THRESHOLD_MS;

    /** Frames in flight between a publish and the tap; far above what one round trip holds. */
    public static final int DEFAULT_PENDING_CAPACITY = Session.DEFAULT_PENDING_CAPACITY;

    /** How long ingress is tried on this member's own {@code aeron:ipc}: short, as a follower never answers. */
    public static final long DEFAULT_IPC_CONNECT_TIMEOUT_MS = 500;

    /** A payload of this application's own belongs to no connection. */
    private static final int NO_CONNECTION = -1;

    /** What an application does that this class cannot do for it. */
    public interface Listener {
        /**
         * The leader gate crossed an edge: true when this replica may do leader-only work — caught up, and
         * its own node leads — false when it may not. <b>Every leadership change closes an open gate</b>,
         * so false is where {@link OutstandingWork#onNotLeader()} belongs: a reply this node submitted
         * during the election may have gone with it, and the next opening dispatches it again.
         */
        void onLeadershipChanged(boolean leading);

        /** One application payload off this node's tap, in {@code globalSeqNo} order. */
        void onSequenced(Payload payload);

        /** Every transition to caught-up, the first included. */
        void onCaughtUp(long globalSeqNo);

        /**
         * The cluster clock's tick (spec §7), once a second. It is the one time source that keeps advancing
         * while every producer is silent, which is exactly when a watchdog must still fire, and it is
         * identical on every node — so a timer driven by it decides the same thing everywhere.
         */
        void onClusterHeartbeat(long clusterTimeNs, long receiveTimeNs);

        /** Once, latched: this replica may no longer act. Exiting is the usual way — its restart re-walks. */
        void onFenced(ClusterError fence, String detail);
    }

    private final Session session;
    private final LeaderGate gate;
    private final Listener listener;
    private final int sourceId;
    private final int memberId;
    private final long ipcConnectTimeoutMs;
    private final String egressChannel;
    private final String ingressEndpoints;

    private ColocatedApplication(final Builder builder) {
        this.listener = builder.listener;
        this.sourceId = builder.sourceId;
        this.memberId = builder.memberId;
        this.ipcConnectTimeoutMs = builder.ipcConnectTimeoutMs;
        this.egressChannel = builder.egressChannel;
        this.ingressEndpoints = builder.ingressEndpoints;
        this.gate = new LeaderGate(builder.memberId);
        this.session = new Session(builder.clientId, builder.pendingCapacity, builder.tapStallTimeoutMs,
                                   builder.recoveryStallTimeoutMs, builder.tapLagThresholdMs,
                                   new SessionDispatch());
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Opens the cluster session on this node and starts following its tap; the gate stays shut until caught up. */
    public void start(final Aeron aeron) {
        session.startColocated(aeron, memberId, ipcConnectTimeoutMs, egressChannel, ingressEndpoints);
    }

    /**
     * One duty-cycle iteration: the cluster session and the tap, then the gate over what they left.
     * @return units of work done, for the caller's idle strategy
     */
    public int doWork() {
        final int work = session.doWork();
        final LeaderGate.Transition transition = gate.update(session.isCaughtUp(), session.currentLeaderMemberId());
        if (transition == LeaderGate.Transition.NONE) {
            return work;
        }
        listener.onLeadershipChanged(transition == LeaderGate.Transition.OPENED);
        return work + 1;
    }

    /** Whether leader-only work may reach ingress right now: the gate is open, and ingress is not held. */
    public boolean canPublish() {
        return gate.isOpen() && !session.isHolding();
    }

    /** Whether this replica may do leader-only work at all; {@link #canPublish()} is what a publish needs. */
    public boolean isLeading() {
        return gate.isOpen();
    }

    /**
     * Submits one payload of this application's own, stamped with its {@code sourceId} and belonging to no
     * connection.
     *
     * @param payload the payload's bytes, its own {@code MessageHeader} included
     * @return {@code Declined} while the gate is shut, ingress is held or the transport is back-pressured —
     *     retry it; {@code Refused} is permanent
     */
    public Publish publish(final int payloadId, final DirectBuffer payload, final int length) {
        return submit(sourceId, NO_CONNECTION, payloadId, payload, length);
    }

    /**
     * The same on behalf of the producer that asked for it: a reply carries the <b>requester's</b>
     * {@code sourceId} and {@code connectionId}, which is how the gateway that took the request routes the
     * answer back out of it. Keep those two off the request rather than the request itself — a
     * {@link Payload} is valid only during its callback, and a reply is usually dispatched later.
     */
    public Publish reply(final int requesterSourceId, final int connectionId, final int payloadId,
                         final DirectBuffer payload, final int length) {
        return submit(requesterSourceId, connectionId, payloadId, payload, length);
    }

    /** This replica's own {@code sourceId}, the one its topology row gives it. */
    public int sourceId() {
        return sourceId;
    }

    public boolean isCaughtUp() {
        return session.isCaughtUp();
    }

    public long lastGlobalSeqNo() {
        return session.lastGlobalSeqNo();
    }

    /**
     * How far behind the leader this node's tap is running. Observation only — nothing here raises a
     * fence; a consumer that wants to report staleness polls it.
     */
    public TapLagMonitor tapLag() {
        return session.tapLag();
    }

    /** Closes the cluster session and the tap. The Aeron client is the caller's and is left open. */
    @Override
    public void close() {
        session.close();
    }

    /** A shut gate declines rather than submits: only the leading replica's copy of the work is the one sent. */
    private Publish submit(final int frameSourceId, final int connectionId, final int payloadId,
                           final DirectBuffer payload, final int length) {
        if (!gate.isOpen()) {
            return Publish.Declined;
        }
        return session.publishPayload(frameSourceId, connectionId, payloadId, payload, length);
    }

    /** What comes off the tap, and the one frame the gate is driven by. */
    private final class SessionDispatch implements Session.Dispatch {
        @Override
        public void onSystem(final SequencedEvent event) {
            // Seqeron's own vocabulary says nothing to a producer nothing elects; the leadership the gate
            // turns on arrives below rather than here.
        }

        @Override
        public void onLeadershipChanged() {
            gate.onLeadershipChanged();
        }

        @Override
        public void onPayload(final Payload payload) {
            listener.onSequenced(payload);
        }

        @Override
        public void onCaughtUp(final long globalSeqNo) {
            listener.onCaughtUp(globalSeqNo);
        }

        @Override
        public void onClusterHeartbeat(final long clusterTimeNs, final long receiveTimeNs) {
            listener.onClusterHeartbeat(clusterTimeNs, receiveTimeNs);
        }

        @Override
        public void onFenced(final ClusterError fence, final String detail) {
            listener.onFenced(fence, detail);
        }
    }

    /** Everything one replica needs to join its deployment. */
    public static final class Builder {
        private int sourceId;
        private int clientId;
        private int memberId;
        private String egressChannel;
        private String ingressEndpoints = PortLayout.ingressEndpoints();
        private Listener listener;
        private int pendingCapacity = DEFAULT_PENDING_CAPACITY;
        private long tapStallTimeoutMs = DEFAULT_TAP_STALL_TIMEOUT_MS;
        private long recoveryStallTimeoutMs = DEFAULT_RECOVERY_STALL_TIMEOUT_MS;
        private long tapLagThresholdMs = DEFAULT_TAP_LAG_THRESHOLD_MS;
        private long ipcConnectTimeoutMs = DEFAULT_IPC_CONNECT_TIMEOUT_MS;

        /**
         * This application's {@code sourceId}, the one its {@code <applications>} row declares — required,
         * because spec §5 is one id space, and never −1, which ingress refuses.
         */
        public Builder sourceId(final int sourceId) {
            this.sourceId = sourceId;
            return this;
        }

        /** This replica's Replayer client id, unique among the co-located apps on its node. */
        public Builder clientId(final int clientId) {
            this.clientId = clientId;
            return this;
        }

        /** The node this replica runs on: whose tap it follows, and whose leadership opens its gate. */
        public Builder memberId(final int memberId) {
            this.memberId = memberId;
            return this;
        }

        /** This client's own egress endpoint; two media drivers on one host cannot both bind a port. */
        public Builder egressChannel(final String egressChannel) {
            this.egressChannel = egressChannel;
            return this;
        }

        /** The members to reach when this node is not leading, {@code PortLayout.ingressEndpoints()} by default. */
        public Builder ingressEndpoints(final String ingressEndpoints) {
            this.ingressEndpoints = ingressEndpoints;
            return this;
        }

        public Builder listener(final Listener listener) {
            this.listener = listener;
            return this;
        }

        public Builder pendingCapacity(final int pendingCapacity) {
            this.pendingCapacity = pendingCapacity;
            return this;
        }

        public Builder tapStallTimeoutMs(final long tapStallTimeoutMs) {
            this.tapStallTimeoutMs = tapStallTimeoutMs;
            return this;
        }

        public Builder recoveryStallTimeoutMs(final long recoveryStallTimeoutMs) {
            this.recoveryStallTimeoutMs = recoveryStallTimeoutMs;
            return this;
        }

        public Builder tapLagThresholdMs(final long tapLagThresholdMs) {
            this.tapLagThresholdMs = tapLagThresholdMs;
            return this;
        }

        public Builder ipcConnectTimeoutMs(final long ipcConnectTimeoutMs) {
            this.ipcConnectTimeoutMs = ipcConnectTimeoutMs;
            return this;
        }

        public ColocatedApplication build() {
            Objects.requireNonNull(egressChannel, "egressChannel");
            Objects.requireNonNull(ingressEndpoints, "ingressEndpoints");
            Objects.requireNonNull(listener, "listener");
            return new ColocatedApplication(this);
        }
    }
}
