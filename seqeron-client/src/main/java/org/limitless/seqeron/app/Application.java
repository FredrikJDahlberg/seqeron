package org.limitless.seqeron.app;

import io.aeron.Aeron;
import io.aeron.Counter;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import org.agrona.DirectBuffer;
import org.limitless.seqeron.protocol.PortLayout;
import org.limitless.seqeron.protocol.Publish;
import org.limitless.seqeron.protocol.SeqeronCounters;
import org.limitless.seqeron.protocol.SnapshotHeader;
import org.limitless.seqeron.protocol.SystemFrame;
import org.limitless.seqeron.replayer.client.SequencedEvent;
import org.limitless.seqeron.replayer.client.SnapshotStore;
import org.limitless.seqeron.sbe.frame.ApplicationRegisteredDecoder;
import org.limitless.seqeron.sbe.frame.MessageHeaderDecoder;
import org.limitless.seqeron.sbe.frame.SnapshotEndDecoder;
import org.limitless.seqeron.sbe.frame.SnapshotStartedDecoder;

/**
 * One replica of a co-located application — the kind of producer nothing elects. One runs per node, the
 * topology's {@code <applications>} section names it, and {@code LeadershipChanged} already picks the
 * replica that submits: this one publishes only while its own node leads. What that takes is behind it —
 * the cluster session over the node's own {@code aeron:ipc}, the queue it follows, the leader gate,
 * confirmed ingress across a failover, and the fences that say it may no longer act.
 *
 * <p><b>Off the cluster</b> ({@link Builder#offCluster}), on a gateway host, it is the one instance rather
 * than one of three: its gate opens once caught up, whoever leads, and it submits over UDP. Nothing stops a
 * second instance publishing too; an application that needs a standby off the cluster is a {@link Gateway}.
 *
 * <p><b>What is left to the consumer is its own work</b>: the payloads it reads, the state it keeps, the
 * payloads it submits. Every replica reads the same ordered stream and so holds the same state, which is
 * what makes {@link OutstandingWork} — fed that same stream — the way work survives the gate closing
 * under it.
 *
 * <p><b>Snapshots</b> (doc/snapshot.md §4): given a {@link SnapshotListener} and a topology row with {@code
 * snapshot="true"}, every replica serializes its state at each round's cut into its own directory, and the one
 * whose gate is open then submits the round's {@code SnapshotEnd}. A replica whose snapshot differs from the one
 * sequenced is fenced with {@link ClusterError#SNAPSHOT_DIVERGED}. On start, a replica given a listener restores
 * the newest snapshot of its own that the log confirms and resumes after its cut, or replays from {@code
 * clusterSeqNo} 1 without one; one it cannot restore is fenced with {@link ClusterError#SNAPSHOT_UNRESTORABLE}.
 *
 * <p>Single-threaded: every method belongs to the caller's one duty-cycle thread, which calls
 * {@link #doWork()} each iteration. The C++ twin is {@code app/Application.hpp}; keep the two in
 * step.
 */
public final class Application implements AutoCloseable {
    /** How long the queue may be silent before this replica is fenced — 20 heartbeat periods. */
    public static final long DEFAULT_QUEUE_STALL_TIMEOUT_MS = Session.DEFAULT_QUEUE_STALL_TIMEOUT_MS;

    /** How long recovery may dispatch nothing, once caught up before; longer, as a re-walk is slower. */
    public static final long DEFAULT_RECOVERY_STALL_TIMEOUT_MS = Session.DEFAULT_RECOVERY_STALL_TIMEOUT_MS;

    /** Frames in flight between a publish and the queue; far above what one round trip holds. */
    public static final int DEFAULT_PENDING_CAPACITY = Session.DEFAULT_PENDING_CAPACITY;

    /** How long ingress is tried on this member's own {@code aeron:ipc}: short, as a follower never answers. */
    public static final long DEFAULT_IPC_CONNECT_TIMEOUT_MS = 500;

    /** A payload of this application's own belongs to no connection. */
    private static final int NO_CONNECTION = -1;

    /** What an application does that this class cannot do for it. */
    public interface Listener {
        /**
         * The log's group table, once, before the gate first opens: what {@link #publish(int, int, DirectBuffer,
         * int)} takes its masks from.
         */
        void onGroupsResolved(GroupDirectory directory);

        /**
         * The leader gate crossed an edge: true when this replica may do leader-only work — caught up, and
         * its own node leads, or off the cluster caught up alone — false when it may not. <b>Every
         * leadership change closes an open gate</b>, so false is where {@link OutstandingWork#onNotLeader()}
         * belongs: a reply this node submitted during the election may have gone with it, and the next
         * opening dispatches it again. False also comes before {@link #onFenced} when this replica fences with
         * its gate open.
         */
        void onLeadershipChanged(boolean leading);

        /** One application payload off this node's queue, in {@code clusterSeqNo} order. */
        void onSequenced(Payload payload);

        /** Every transition to caught-up, the first included. */
        void onCaughtUp(long clusterSeqNo);

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
    private final SnapshotTaker snapshots;
    private final SnapshotFrames snapshotFrames;
    private final int sourceId;
    private final int memberId;
    private final int clientId;
    private final boolean offCluster;
    private final long ipcConnectTimeoutMs;
    private final String egressChannel;
    private final String ingressEndpoints;
    private Counter snapshotRoundCounter;

    private Application(final Builder builder) {
        this.listener = builder.listener;
        this.sourceId = builder.sourceId;
        this.memberId = builder.memberId;
        this.clientId = builder.clientId;
        this.offCluster = builder.offCluster;
        this.ipcConnectTimeoutMs = builder.ipcConnectTimeoutMs;
        this.egressChannel = builder.egressChannel;
        this.ingressEndpoints = builder.ingressEndpoints;
        this.gate = new LeaderGate(builder.memberId, builder.offCluster);
        final SnapshotStore store =
            builder.snapshotListener == null ? null : new SnapshotStore(builder.snapshotDirectory);
        this.snapshots = new SnapshotTaker(builder.snapshotListener, store, this::keepAlive);
        this.session = new Session(builder.clientId, builder.pendingCapacity, builder.queueStallTimeoutMs,
                                   builder.recoveryStallTimeoutMs, builder.sourceId, null, builder.addressedGroups,
                                   new SessionDispatch());
        this.snapshotFrames = new SnapshotFrames(session, () -> sourceId);
        if (store != null) {
            session.restoreFrom(store, snapshots);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Opens the cluster session on this node and starts following its queue; the gate stays shut until caught up. */
    public void start(final Aeron aeron) {
        if (offCluster) {
            session.start(aeron, memberId, egressChannel, ingressEndpoints);
        } else {
            session.startColocated(aeron, memberId, ipcConnectTimeoutMs, egressChannel, ingressEndpoints);
        }
        if (snapshots.isListening()) {
            snapshotRoundCounter = SeqeronCounters.addAppCounter(
                aeron, SeqeronCounters.APP_SNAPSHOT_ROUND_TYPE_ID,
                "seqeron.app.snapshotRound source=" + sourceId + " member=" + memberId + " client=" + clientId,
                memberId, clientId);
        }
    }

    /**
     * One duty-cycle iteration: the cluster session and the queue, then the gate over what they left. Nothing once
     * fenced.
     * @return units of work done, for the caller's idle strategy
     */
    public int doWork() {
        int work = session.doWork();
        if (snapshotRoundCounter != null) {
            snapshotRoundCounter.set(snapshots.confirmedRound());
        }
        if (session.isFenced()) {
            return work;
        }
        // Shut while a lost session is replaced: a reply sent meanwhile may not land, so the next opening redispatches.
        final LeaderGate.Transition transition =
            gate.update(session.isCaughtUp() && session.hasSession(), session.currentLeaderMemberId());
        if (transition != LeaderGate.Transition.NONE) {
            listener.onLeadershipChanged(transition == LeaderGate.Transition.OPENED);
            work++;
        }
        if (gate.isOpen()) {
            work += snapshots.submit(snapshotFrames);
        }
        return work;
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
     * Submits one payload of this application's own to its own group, stamped with its {@code sourceId} and
     * belonging to no connection.
     *
     * @param payload the payload's bytes, its own {@code MessageHeader} included
     * @return {@code Declined} while the gate is shut, ingress is held or the transport is back-pressured —
     *     retry it; {@code Refused} is permanent
     */
    public Publish publish(final int payloadId, final DirectBuffer payload, final int length) {
        return submit(sourceId, NO_CONNECTION, 0, payloadId, payload, length);
    }

    /**
     * The same to the groups {@code groupMask} names as well as its own.
     *
     * @param groupMask bits from the {@link GroupDirectory}; one the table lacks is {@code Refused}
     */
    public Publish publish(final int groupMask, final int payloadId, final DirectBuffer payload, final int length) {
        return submit(sourceId, NO_CONNECTION, groupMask, payloadId, payload, length);
    }

    /**
     * The same on behalf of the producer that asked for it: a reply carries the <b>requester's</b>
     * {@code sourceId}, {@code connectionId} and {@code groupMask}, which is how the gateway that took the request
     * routes the answer back out of it, and how every group that saw the request sees the answer. Keep those three
     * off the request rather than the request itself — a {@link Payload} is valid only during its callback, and a
     * reply is usually dispatched later.
     */
    public Publish reply(final int requesterSourceId, final int connectionId, final int groupMask, final int payloadId,
                         final DirectBuffer payload, final int length) {
        return submit(requesterSourceId, connectionId, groupMask, payloadId, payload, length);
    }

    /** This replica's own {@code sourceId}, the one its topology row gives it. */
    public int sourceId() {
        return sourceId;
    }

    public boolean isCaughtUp() {
        return session.isCaughtUp();
    }

    public long lastClusterSeqNo() {
        return session.lastClusterSeqNo();
    }

    /** Closes the cluster session and the queue. The Aeron client is the caller's and is left open. */
    @Override
    public void close() {
        session.close();
        if (snapshotRoundCounter != null) {
            snapshotRoundCounter.close();
        }
    }

    /** The session's keep-alive, for a round serialized inside a dispatch. */
    private void keepAlive() {
        session.keepAlive();
    }

    /** A shut gate declines rather than submits: only the leading replica's copy of the work is the one sent. */
    private Publish submit(final int frameSourceId, final int connectionId, final int groupMask, final int payloadId,
                           final DirectBuffer payload, final int length) {
        if (!gate.isOpen()) {
            return Publish.Declined;
        }
        return session.publishPayload(frameSourceId, connectionId, groupMask, payloadId, payload, length);
    }

    /** What comes off the queue, and the one frame the gate is driven by. */
    private final class SessionDispatch implements Session.Dispatch {
        private final ApplicationRegisteredDecoder row = new ApplicationRegisteredDecoder();
        private final SnapshotStartedDecoder started = new SnapshotStartedDecoder();
        private final SnapshotEndDecoder end = new SnapshotEndDecoder();

        /**
         * Of seqeron's own vocabulary, this replica reads its topology row and the snapshot rounds; the
         * leadership the gate turns on arrives below rather than here. A submitted payload carries no
         * MessageHeader, so each decode takes this build's constants (spec §7, V-3).
         */
        @Override
        public void onSystem(final SequencedEvent event) {
            switch (event.systemEventType()) {
            case SystemFrame.APPLICATION_REGISTERED -> {
                row.wrap(event.buffer(), event.payloadOffset(), ApplicationRegisteredDecoder.BLOCK_LENGTH,
                         MessageHeaderDecoder.SCHEMA_VERSION);
                if (row.applicationSourceId() == sourceId) {
                    snapshots.participating(row.snapshot() == 1);
                }
            }
            case SystemFrame.SNAPSHOT_STARTED -> {
                started.wrap(event.buffer(), event.payloadOffset(), SnapshotStartedDecoder.BLOCK_LENGTH,
                             SnapshotStartedDecoder.SCHEMA_VERSION);
                snapshots.onSnapshotStarted(
                    started.round(), event.clusterSeqNo(), event.position(), event.clusterTimestampNs(),
                    new SnapshotHeader(session.leadershipTermId(), session.currentLeaderMemberId(),
                                       session.groupTable(), null),
                    gate.isOpen());
            }
            case SystemFrame.SNAPSHOT_END -> {
                if (event.sourceId() != sourceId) {
                    return;
                }
                end.wrap(event.buffer(), event.payloadOffset(), SnapshotEndDecoder.BLOCK_LENGTH,
                         MessageHeaderDecoder.SCHEMA_VERSION);
                if (!snapshots.onSnapshotEnd(end.round(), end.recordCount(), end.length(), end.crc32c())) {
                    session.fence(ClusterError.SNAPSHOT_DIVERGED, "round " + end.round() + "'s sequenced "
                        + "snapshot at clusterSeqNo " + event.clusterSeqNo() + " differs from this replica's");
                }
            }
            default -> {
                // Nothing else says anything to a producer nothing elects.
            }
            }
        }

        /** A term won by another member ends this replica's part in the round it is publishing. */
        @Override
        public void onLeadershipChanged() {
            gate.onLeadershipChanged();
            if (!offCluster && session.currentLeaderMemberId() != memberId) {
                snapshots.stopPublishing();
            }
        }

        @Override
        public void onPayload(final Payload payload) {
            listener.onSequenced(payload);
        }

        @Override
        public void onGroupsResolved(final GroupDirectory directory) {
            listener.onGroupsResolved(directory);
        }

        @Override
        public void onCaughtUp(final long clusterSeqNo) {
            listener.onCaughtUp(clusterSeqNo);
        }

        @Override
        public void onClusterHeartbeat(final long clusterTimeNs, final long receiveTimeNs) {
            listener.onClusterHeartbeat(clusterTimeNs, receiveTimeNs);
        }

        /** An open gate closes first, so the listener has given up leader-only work by the time it hears why. */
        @Override
        public void onFenced(final ClusterError fence, final String detail) {
            if (gate.onFenced()) {
                listener.onLeadershipChanged(false);
            }
            listener.onFenced(fence, detail);
        }

        /** Nothing elects a replica, and its work stays outstanding on the log until replied to. */
        @Override
        public boolean mayReconnect() {
            return true;
        }

        /** The leader's replica, caught up or not: no other holds the leader-only work. */
        @Override
        public boolean isActing() {
            return offCluster || session.currentLeaderMemberId() == memberId;
        }
    }

    /** Everything one replica needs to join its deployment. */
    public static final class Builder {
        private int sourceId;
        private List<String> addressedGroups = List.of();
        private int clientId;
        private int memberId;
        private boolean offCluster;
        private String egressChannel;
        private String ingressEndpoints = PortLayout.ingressEndpoints();
        private Listener listener;
        private SnapshotListener snapshotListener;
        private Path snapshotDirectory;
        private int pendingCapacity = DEFAULT_PENDING_CAPACITY;
        private long queueStallTimeoutMs = DEFAULT_QUEUE_STALL_TIMEOUT_MS;
        private long recoveryStallTimeoutMs = DEFAULT_RECOVERY_STALL_TIMEOUT_MS;
        private long ipcConnectTimeoutMs = DEFAULT_IPC_CONNECT_TIMEOUT_MS;

        /**
         * This application's {@code sourceId}, the one its {@code <applications>} row declares — required,
         * because spec §5 is one id space, and never −1, which ingress refuses.
         */
        public Builder sourceId(final int sourceId) {
            this.sourceId = sourceId;
            return this;
        }

        /**
         * The names of the groups this application publishes to besides its own; one the log's group table lacks
         * fences it with {@link ClusterError#GROUP_UNKNOWN}.
         */
        public Builder addressedGroups(final String... addressedGroups) {
            this.addressedGroups = List.of(addressedGroups);
            return this;
        }

        /** This replica's client id, its counters' label; unique among the co-located apps on its node. */
        public Builder clientId(final int clientId) {
            this.clientId = clientId;
            return this;
        }

        /** The node this replica runs on: whose queue it follows, and whose leadership opens its gate. */
        public Builder memberId(final int memberId) {
            this.memberId = memberId;
            return this;
        }

        /**
         * Whether this node is a gateway host, running no member, rather than a member: the gate then opens
         * whoever leads, and ingress is UDP alone. Run one instance, as nothing elects between two.
         */
        public Builder offCluster(final boolean offCluster) {
            this.offCluster = offCluster;
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

        /**
         * What serializes this application's state for snapshot rounds and restores it on start; without one it
         * takes part in none, whatever its topology row says, and recovers from {@code clusterSeqNo} 1.
         */
        public Builder snapshotListener(final SnapshotListener snapshotListener) {
            this.snapshotListener = snapshotListener;
            return this;
        }

        /**
         * Where this replica keeps its snapshots, one file per round; required with a {@link #snapshotListener}.
         * Its own: no other instance may write it. A restart restores from what it holds, so it must outlive the
         * process.
         */
        public Builder snapshotDirectory(final Path snapshotDirectory) {
            this.snapshotDirectory = snapshotDirectory;
            return this;
        }

        public Builder pendingCapacity(final int pendingCapacity) {
            this.pendingCapacity = pendingCapacity;
            return this;
        }

        public Builder queueStallTimeoutMs(final long queueStallTimeoutMs) {
            this.queueStallTimeoutMs = queueStallTimeoutMs;
            return this;
        }

        public Builder recoveryStallTimeoutMs(final long recoveryStallTimeoutMs) {
            this.recoveryStallTimeoutMs = recoveryStallTimeoutMs;
            return this;
        }

        public Builder ipcConnectTimeoutMs(final long ipcConnectTimeoutMs) {
            this.ipcConnectTimeoutMs = ipcConnectTimeoutMs;
            return this;
        }

        public Application build() {
            Objects.requireNonNull(egressChannel, "egressChannel");
            Objects.requireNonNull(ingressEndpoints, "ingressEndpoints");
            Objects.requireNonNull(listener, "listener");
            if (snapshotListener != null && snapshotDirectory == null) {
                throw new IllegalStateException("snapshotDirectory is required with a snapshotListener");
            }
            return new Application(this);
        }
    }
}
