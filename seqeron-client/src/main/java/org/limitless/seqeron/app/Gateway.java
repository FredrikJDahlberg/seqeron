package org.limitless.seqeron.app;

import io.aeron.Aeron;
import java.util.ArrayDeque;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.limitless.seqeron.protocol.PortLayout;
import org.limitless.seqeron.protocol.Publish;
import org.limitless.seqeron.protocol.SnapshotHeader;
import org.limitless.seqeron.protocol.SystemFrame;
import org.limitless.seqeron.replayer.client.SequencedEvent;
import org.limitless.seqeron.replayer.client.SnapshotRestoreHandler;
import org.limitless.seqeron.replayer.client.SnapshotStore;
import org.limitless.seqeron.sbe.frame.ConnectionClosedEncoder;
import org.limitless.seqeron.sbe.frame.ConnectionOpenedDecoder;
import org.limitless.seqeron.sbe.frame.ConnectionOpenedEncoder;
import org.limitless.seqeron.sbe.frame.GatewayActiveDecoder;
import org.limitless.seqeron.sbe.frame.GatewayRegisteredDecoder;
import org.limitless.seqeron.sbe.frame.GatewayStartedEncoder;
import org.limitless.seqeron.sbe.frame.MessageHeaderDecoder;
import org.limitless.seqeron.sbe.frame.SnapshotEndDecoder;
import org.limitless.seqeron.sbe.frame.SnapshotStartedDecoder;

/**
 * One instance of an elected active/standby producer pair. Everything this tier defines about being a
 * gateway is behind it: the list row that names the instance, the designation that makes it serve, the
 * {@code GatewayStarted} that binds its session, the connection id space it resumes from its predecessor,
 * the connection lifecycle frames, confirmed ingress across a failover, and the fences that stand it down.
 *
 * <p><b>What is left to the consumer is its edge</b> — a socket, a dialler, a codec. It opens that edge in
 * {@link Listener#onActivated}, closes it in {@link Listener#onStandby}, and otherwise exchanges payloads:
 * nothing of the frame layer or of seqeron's system vocabulary appears in its code.
 *
 * <p><b>Snapshots</b> (doc/snapshot.md §4): given a {@link SnapshotListener} and topology rows with {@code
 * snapshot="true"}, every instance serializes its state at each round's cut into its own directory, under a header
 * carrying the pair's election state, and the active one then submits the round's {@code SnapshotEnd}. An instance
 * whose snapshot differs from the one sequenced is fenced with {@link ClusterError#SNAPSHOT_DIVERGED}. On start, an
 * instance given a listener restores the newest snapshot of its own that the log confirms and resumes after its
 * cut, or replays from {@code globalSeqNo} 1 without one; one it cannot restore is fenced with {@link
 * ClusterError#SNAPSHOT_UNRESTORABLE}. A {@linkplain Builder#passive passive} instance holds no state until it is
 * activated: it follows the tap for the election alone, then restores and catches up before it serves.
 *
 * <p>Single-threaded: every method belongs to the caller's one duty-cycle thread, which calls
 * {@link #doWork()} each iteration. The C++ twin is {@code app/Gateway.hpp}; keep the two in step.
 */
public final class Gateway implements AutoCloseable {
    /**
     * How long the tap may be silent before this instance is fenced — 20 heartbeat periods. The deployment
     * policy every gateway had been copying.
     */
    public static final long DEFAULT_TAP_STALL_TIMEOUT_MS = Session.DEFAULT_TAP_STALL_TIMEOUT_MS;

    /** How long recovery may dispatch nothing, once caught up before; longer than the tap's, as a re-walk is slower. */
    public static final long DEFAULT_RECOVERY_STALL_TIMEOUT_MS = Session.DEFAULT_RECOVERY_STALL_TIMEOUT_MS;

    /** Frames in flight between a publish and the tap; far above what one round trip holds. */
    public static final int DEFAULT_PENDING_CAPACITY = Session.DEFAULT_PENDING_CAPACITY;

    /**
     * A frame that belongs to the gateway rather than to one of its connections, and what
     * {@link #openConnection} answers when there is none to give.
     */
    public static final int NO_CONNECTION = -1;

    /**
     * What {@link #sourceId()} and {@link #gatewayId()} read until a {@code GatewayRegistered} row names this
     * instance. Nothing may be published under an unresolved identity, so a consumer that stamps its own
     * records with {@link #sourceId()} checks for this first.
     */
    public static final int UNRESOLVED = GatewayLifecycle.UNRESOLVED;

    private static final byte[] NO_CONNECTION_DATA = new byte[0];

    /** What a gateway does that this class cannot do for it. */
    public interface Listener {
        /**
         * This instance has been designated, has announced itself and may now serve: open the edge.
         *
         * @param firstConnectionId the id {@link #openConnection} will allocate first, resumed past whatever
         *                          a predecessor issued — a gateway that numbers its own sessions starts here
         * @return whether the edge opened; false is retried on the next {@link #doWork()}
         */
        boolean onActivated(int firstConnectionId);

        /** Stand down: close the edge and drop every connection it let in. Publishes nothing. */
        void onStandby();

        /** One application payload off this node's tap, in {@code globalSeqNo} order. */
        void onSequenced(Payload payload);

        /**
         * This logical gateway took a connection, whichever instance issued it. A consumer that keeps
         * per-connection state rebuilds it from these while it replays. The buffer is valid only during
         * the call.
         */
        void onConnectionOpened(int connectionId, DirectBuffer connectionData, int offset, int length);

        /**
         * That connection has gone. A client that drops its socket without logging out produces no payload
         * at all, so this is the only notice of it.
         */
        void onConnectionClosed(int connectionId);

        /** Every transition to caught-up, the first included. */
        void onCaughtUp(long globalSeqNo);

        /**
         * The cluster clock's tick (spec §7), once a second. It is the one time source that keeps advancing
         * while every producer is silent, which is exactly when a watchdog must still fire, and it is
         * identical on every node — so a timer driven by it decides the same thing everywhere.
         */
        void onClusterHeartbeat(long clusterTimeNs, long receiveTimeNs);

        /** Once, latched: release the cluster session — exiting is the usual way — so a standby takes over. */
        void onFenced(ClusterError fence, String detail);
    }

    private final Session session;
    private final GatewayLifecycle lifecycle;
    private final Listener listener;
    private final SnapshotTaker snapshots;
    private final SnapshotFrames snapshotFrames;
    private final int memberId;
    private final String egressChannel;
    private final String ingressEndpoints;

    private final ExpandableArrayBuffer body = new ExpandableArrayBuffer(256);
    private final GatewayStartedEncoder gatewayStarted = new GatewayStartedEncoder();
    private final ConnectionOpenedEncoder connectionOpened = new ConnectionOpenedEncoder();
    private final ConnectionClosedEncoder connectionClosed = new ConnectionClosedEncoder();
    private final ConnectionOpenedDecoder connectionOpenedIn = new ConnectionOpenedDecoder();
    private final GatewayRegisteredDecoder gatewayRow = new GatewayRegisteredDecoder();
    private final GatewayActiveDecoder gatewayActive = new GatewayActiveDecoder();
    private final SnapshotStartedDecoder snapshotStarted = new SnapshotStartedDecoder();
    private final SnapshotEndDecoder snapshotEnd = new SnapshotEndDecoder();

    /** Connection lifecycle frames still to be placed, in the order they were asked for. */
    private final ArrayDeque<Lifecycle> lifecycleQueue = new ArrayDeque<>();

    /** Connections whose {@code ConnectionOpened} has not landed: nothing may be published on one yet. */
    private final Set<Integer> unopened = new HashSet<>();

    /** The highest {@code connectionId} this logical gateway's history holds; the resume point for §7's row. */
    private int highestConnectionId = NO_CONNECTION;
    private int nextConnectionId;

    /** Holding no state: until activated, this instance follows the tap for the election alone. */
    private boolean passive;

    private Gateway(final Builder builder) {
        this.listener = builder.listener;
        this.memberId = builder.memberId;
        this.egressChannel = builder.egressChannel;
        this.ingressEndpoints = builder.ingressEndpoints;
        this.passive = builder.passive;
        this.lifecycle = new GatewayLifecycle(builder.gatewayName, new LifecycleActions());
        final SnapshotStore store =
            builder.snapshotListener == null ? null : new SnapshotStore(builder.snapshotDirectory);
        this.snapshots = new SnapshotTaker(builder.snapshotListener, store);
        this.session = new Session(builder.clientId, builder.pendingCapacity, builder.tapStallTimeoutMs,
                                   builder.recoveryStallTimeoutMs, new SessionDispatch());
        this.snapshotFrames = new SnapshotFrames(session, lifecycle::gatewaySourceId);
        if (store != null) {
            session.restoreFrom(builder.sourceId, store, new Restore());
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Opens the cluster session and starts following this node's tap; the gate stays shut until designated. */
    public void start(final Aeron aeron) {
        session.start(aeron, memberId, egressChannel, ingressEndpoints);
    }

    /**
     * One duty-cycle iteration: the cluster session and the tap, then whatever the connection lifecycle, the
     * election and a snapshot round still owe. A passive instance that has been activated starts over here.
     * @return units of work done, for the caller's idle strategy
     */
    public int doWork() {
        int work = session.doWork();
        if (passive && lifecycle.isActivated() && session.isCaughtUp()) {
            passive = false;
            session.restart();
            return work + 1;
        }
        work += drainLifecycle();
        work += session.isCaughtUp() ? lifecycle.advance() : 0;
        work += snapshots.submit(snapshotFrames);
        return work;
    }

    /**
     * The edge closed without being asked to — a dial that failed, a counterparty that hung up. The
     * designation stands, so {@link #doWork()} opens it again through {@link Listener#onActivated}, and
     * without a second {@code GatewayStarted}: this instance is still the one the cluster designated.
     *
     * <p>An acceptor whose listen socket stays bound never calls this. An initiator does: its edge is one
     * dial, and a dial that fails or a session that drops is the gate closing under it.
     */
    public void gateClosed() {
        lifecycle.onGateClosed();
    }

    /**
     * Tells the cluster this instance is alive. {@link #doWork()} already does it once a cycle; call this as
     * well from inside a {@link Listener} callback that spins — writing to an edge that is back-pressured,
     * say — since {@code doWork()} cannot run again until that callback returns, and the cluster drops a
     * session that goes quiet for {@code sequencer.sessionTimeoutMs}. Self-throttling, so a call per spin
     * costs nothing.
     */
    public void keepAlive() {
        session.keepAlive();
    }

    /**
     * Whether a connection may be taken right now: this instance is serving, and ingress is not held behind a
     * failover's resend. Check it before accepting or dialling — a connection taken while ingress is held
     * could not have its {@code ConnectionOpened} placed.
     */
    public boolean canAccept() {
        return lifecycle.isServing() && !session.isHolding();
    }

    /**
     * Takes one connection into the cluster's view of this gateway: allocates its id from the resume point
     * and queues its {@code ConnectionOpened}, which {@link #doWork()} places and retries.
     *
     * @return the connection's id, or {@link #NO_CONNECTION} if this instance is not serving
     */
    public int openConnection(final DirectBuffer connectionData, final int length) {
        final byte[] data = new byte[length];
        connectionData.getBytes(0, data, 0, length);
        return open(data);
    }

    /** The same for a connection whose only identity is its id. */
    public int openConnection() {
        return open(NO_CONNECTION_DATA);
    }

    private int open(final byte[] connectionData) {
        if (!lifecycle.onSessionAcquired()) {
            return NO_CONNECTION;
        }
        final int connectionId = nextConnectionId++;
        lifecycleQueue.add(new Lifecycle(connectionId, SystemFrame.CONNECTION_OPENED, connectionData));
        unopened.add(connectionId);
        return connectionId;
    }

    /** The same for a connection that has gone. One the cluster never heard of is dropped rather than announced. */
    public void closeConnection(final int connectionId) {
        if (!lifecycle.isServing()) {
            // Stood down: the connections went with the edge, and a successor's GatewayStarted releases them.
            return;
        }
        if (unopened.remove(connectionId)) {
            lifecycleQueue.removeIf(queued -> queued.connectionId == connectionId);
            return;
        }
        lifecycleQueue.add(new Lifecycle(connectionId, SystemFrame.CONNECTION_CLOSED, NO_CONNECTION_DATA));
    }

    /**
     * Submits one application payload on a connection, stamped with this gateway's {@code sourceId}.
     *
     * @param payload the payload's bytes, its own {@code MessageHeader} included
     * @return {@code Declined} while ingress is held, back-pressured, or the connection's
     *     {@code ConnectionOpened} has not landed yet — retry it; {@code Refused} is permanent
     */
    public Publish publish(final int connectionId, final int payloadId, final DirectBuffer payload,
                           final int length) {
        if (unopened.contains(connectionId)) {
            return Publish.Declined;
        }
        return session.publishPayload(lifecycle.gatewaySourceId(), connectionId, payloadId, payload, length);
    }

    /** Whether the last {@code GatewayActive} for this pair named this instance. */
    public boolean isActivated() {
        return lifecycle.isActivated();
    }

    /** Whether the edge is open — {@link Listener#onActivated} has returned true and nothing has stood it down. */
    public boolean isServing() {
        return lifecycle.isServing();
    }

    /** Whether this instance still holds no state: passive and not yet activated. */
    public boolean isPassive() {
        return passive;
    }

    /**
     * This logical gateway's {@code sourceId}, shared with its standby; {@link #UNRESOLVED} until a
     * {@code GatewayRegistered} row names it.
     */
    public int sourceId() {
        return lifecycle.gatewaySourceId();
    }

    /** This instance's list row, or {@link #UNRESOLVED} until a row names it. */
    public int gatewayId() {
        return lifecycle.gatewayId();
    }

    public boolean isCaughtUp() {
        return session.isCaughtUp();
    }

    public long lastGlobalSeqNo() {
        return session.lastGlobalSeqNo();
    }

    /** Closes the cluster session and the tap. The Aeron client is the caller's and is left open. */
    @Override
    public void close() {
        session.close();
    }

    /** Places what the connection lifecycle owes, oldest first, stopping at the first frame that is declined. */
    private int drainLifecycle() {
        int work = 0;
        while (!lifecycleQueue.isEmpty()) {
            final Lifecycle next = lifecycleQueue.peek();
            if (!published(session.publishSystem(lifecycle.gatewaySourceId(), next.connectionId,
                                                 next.systemEventType, body, encode(next)))) {
                break;
            }
            lifecycleQueue.poll();
            unopened.remove(next.connectionId);
            work++;
        }
        return work;
    }

    private int encode(final Lifecycle queued) {
        if (queued.systemEventType == SystemFrame.CONNECTION_OPENED) {
            connectionOpened.wrap(body, 0);
            connectionOpened.putConnectionData(queued.connectionData, 0, queued.connectionData.length);
            return connectionOpened.encodedLength();
        }
        connectionClosed.wrap(body, 0);
        return connectionClosed.encodedLength();
    }

    /** The resume point, read off every frame this logical gateway's history holds, whichever instance issued it. */
    /**
     * A {@code ConnectionOpened}'s opaque tail, or nothing. §7.1 lets {@code connectionData} be absent, and a
     * producer that takes the option encodes no var-data header at all — so a payload too short to hold one is
     * that case, not a short read.
     */
    private void dispatchConnectionOpened(final SequencedEvent event) {
        if (event.payloadLength() < ConnectionOpenedDecoder.connectionDataHeaderLength()) {
            listener.onConnectionOpened(event.connectionId(), event.buffer(), event.payloadOffset(), 0);
            return;
        }
        connectionOpenedIn.wrap(event.buffer(), event.payloadOffset(), ConnectionOpenedDecoder.BLOCK_LENGTH,
                                MessageHeaderDecoder.SCHEMA_VERSION);
        final int length = connectionOpenedIn.connectionDataLength();
        listener.onConnectionOpened(event.connectionId(), event.buffer(),
                                    connectionOpenedIn.limit() + ConnectionOpenedDecoder.connectionDataHeaderLength(),
                                    length);
    }

    private void observeConnectionId(final int sourceId, final int connectionId) {
        if (lifecycle.gatewaySourceId() != GatewayLifecycle.UNRESOLVED && sourceId == lifecycle.gatewaySourceId()
            && connectionId > highestConnectionId) {
            highestConnectionId = connectionId;
        }
    }

    /** A refused frame is this class's own bug, never a condition to wait out. */
    private static boolean published(final Publish result) {
        if (result == Publish.Refused) {
            throw new IllegalStateException("a frame the sequencer would reject (doc/seqeron-protocol-spec.md §9.2)");
        }
        return result == Publish.Published;
    }

    /** What this instance derives from the frames before a cut, for the round it starts (§6). */
    private SnapshotHeader snapshotHeader() {
        return new SnapshotHeader(session.leadershipTermId(), session.currentLeaderMemberId(),
                                  new SnapshotHeader.GatewayState(lifecycle.gatewaySourceId(),
                                                                  lifecycle.activeGatewayId(), highestConnectionId,
                                                                  lifecycle.pairRows()));
    }

    /** One connection lifecycle frame waiting to be placed. */
    private static final class Lifecycle {
        private final int connectionId;
        private final int systemEventType;
        private final byte[] connectionData;

        private Lifecycle(final int connectionId, final int systemEventType, final byte[] connectionData) {
            this.connectionId = connectionId;
            this.systemEventType = systemEventType;
            this.connectionData = connectionData;
        }
    }

    /** The election's side effects. */
    private final class LifecycleActions implements GatewayLifecycle.Actions {
        @Override
        public void identityResolved(final int gatewayId, final int gatewaySourceId, final int preferenceRank) {
            // Nothing to do: the consumer reads the identity off the accessors when it opens its edge.
        }

        @Override
        public boolean publishGatewayStarted(final int gatewayId) {
            nextConnectionId = highestConnectionId + 1;
            gatewayStarted.wrap(body, 0);
            gatewayStarted.gatewayId(gatewayId).firstConnectionId(nextConnectionId);
            return published(session.publishSystem(lifecycle.gatewaySourceId(), NO_CONNECTION,
                                                   SystemFrame.GATEWAY_STARTED, body,
                                                   gatewayStarted.encodedLength()));
        }

        @Override
        public boolean openGate() {
            return listener.onActivated(nextConnectionId);
        }

        @Override
        public void closeGate() {
            // The connections go with the edge, and a successor's GatewayStarted is what releases them.
            lifecycleQueue.clear();
            unopened.clear();
            listener.onStandby();
        }
    }

    /**
     * What a restore hands over in place of the frames before the cut: the header's election state and connection id
     * resume point, then, unless passive, the application's records.
     */
    private final class Restore implements SnapshotRestoreHandler {
        @Override
        public boolean supportsFormatVersion(final long formatVersion) {
            return snapshots.supportsFormatVersion(formatVersion);
        }

        @Override
        public void onSnapshotHeader(final SnapshotHeader header) {
            if (header.gateway() != null) {
                lifecycle.onSnapshotHeader(header.gateway());
                highestConnectionId = header.gateway().highestConnectionId();
            }
            if (!passive) {
                snapshots.onSnapshotHeader(header);
            }
        }

        @Override
        public void onSnapshotRecord(final DirectBuffer record, final int length, final int recordIndex) {
            if (!passive) {
                snapshots.onSnapshotRecord(record, length, recordIndex);
            }
        }
    }

    /** What comes off the tap, split into what the election reads and what the consumer does. */
    private final class SessionDispatch implements Session.Dispatch {
        /**
         * A submitted system payload carries no MessageHeader, so each decode takes this build's own block length and
         * version (doc/seqeron-protocol-spec.md §7, V-3). A passive instance reads the election alone.
         */
        @Override
        public void onSystem(final SequencedEvent event) {
            observeConnectionId(event.sourceId(), event.connectionId());
            switch (event.systemEventType()) {
            case SystemFrame.GATEWAY_REGISTERED:
                gatewayRow.wrap(event.buffer(), event.payloadOffset(), GatewayRegisteredDecoder.BLOCK_LENGTH,
                                MessageHeaderDecoder.SCHEMA_VERSION);
                lifecycle.onGatewayRegistered(gatewayRow.gatewayId(), gatewayRow.gatewaySourceId(),
                                              gatewayRow.gatewayName(), gatewayRow.preferenceRank());
                if (gatewayRow.gatewayId() == lifecycle.gatewayId()) {
                    snapshots.participating(gatewayRow.snapshot() == 1);
                }
                break;
            case SystemFrame.CONNECTION_OPENED:
                if (!passive && event.sourceId() == lifecycle.gatewaySourceId()) {
                    dispatchConnectionOpened(event);
                }
                break;
            case SystemFrame.CONNECTION_CLOSED:
                if (!passive && event.sourceId() == lifecycle.gatewaySourceId()) {
                    listener.onConnectionClosed(event.connectionId());
                }
                break;
            case SystemFrame.GATEWAY_ACTIVE:
                gatewayActive.wrap(event.buffer(), event.payloadOffset(), GatewayActiveDecoder.BLOCK_LENGTH,
                                   GatewayActiveDecoder.SCHEMA_VERSION);
                lifecycle.onGatewayActive(gatewayActive.gatewayId());
                if (!lifecycle.isActivated()) {
                    snapshots.stopPublishing();
                }
                break;
            case SystemFrame.SNAPSHOT_STARTED:
                if (!passive && snapshots.isParticipating()) {
                    snapshotStarted.wrap(event.buffer(), event.payloadOffset(), SnapshotStartedDecoder.BLOCK_LENGTH,
                                         SnapshotStartedDecoder.SCHEMA_VERSION);
                    snapshots.onSnapshotStarted(snapshotStarted.round(), snapshotHeader(), lifecycle.isAnnounced());
                }
                break;
            case SystemFrame.SNAPSHOT_END:
                if (event.sourceId() != lifecycle.gatewaySourceId()) {
                    break;
                }
                snapshotEnd.wrap(event.buffer(), event.payloadOffset(), SnapshotEndDecoder.BLOCK_LENGTH,
                                 MessageHeaderDecoder.SCHEMA_VERSION);
                if (!snapshots.onSnapshotEnd(snapshotEnd.round(), snapshotEnd.recordCount(), snapshotEnd.length(),
                                             snapshotEnd.crc32c())) {
                    session.fence(ClusterError.SNAPSHOT_DIVERGED, "round " + snapshotEnd.round() + "'s sequenced "
                        + "snapshot at globalSeqNo " + event.globalSeqNo() + " differs from this instance's");
                }
                break;
            default:
                break;
            }
        }

        @Override
        public void onLeadershipChanged() {
            // A gateway is elected by the cluster rather than gated on its node leading: nothing to do.
        }

        @Override
        public void onPayload(final Payload payload) {
            observeConnectionId(payload.sourceId(), payload.connectionId());
            if (!passive) {
                listener.onSequenced(payload);
            }
        }

        @Override
        public void onCaughtUp(final long globalSeqNo) {
            lifecycle.onCaughtUp();
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

    /** Everything one instance needs to join its pair. */
    public static final class Builder {
        private String gatewayName;
        private int sourceId = UNRESOLVED;
        private SnapshotListener snapshotListener;
        private Path snapshotDirectory;
        private boolean passive;
        private int clientId;
        private int memberId;
        private String egressChannel;
        private String ingressEndpoints = PortLayout.ingressEndpoints();
        private Listener listener;
        private int pendingCapacity = DEFAULT_PENDING_CAPACITY;
        private long tapStallTimeoutMs = DEFAULT_TAP_STALL_TIMEOUT_MS;
        private long recoveryStallTimeoutMs = DEFAULT_RECOVERY_STALL_TIMEOUT_MS;

        /** The {@code GatewayRegistered} row name this instance joins on. */
        public Builder gatewayName(final String gatewayName) {
            this.gatewayName = gatewayName;
            return this;
        }

        /**
         * The logical gateway's {@code sourceId}, the one its rows declare. Required with a {@link
         * #snapshotListener}, as the restore asks for the source's snapshot before any row is dispatched.
         */
        public Builder sourceId(final int sourceId) {
            this.sourceId = sourceId;
            return this;
        }

        /**
         * What serializes this instance's state for snapshot rounds and restores it on start; without one it takes
         * part in none, whatever its rows say, and recovers from {@code globalSeqNo} 1.
         */
        public Builder snapshotListener(final SnapshotListener snapshotListener) {
            this.snapshotListener = snapshotListener;
            return this;
        }

        /**
         * Where this instance keeps its snapshots, one file per round; required with a {@link #snapshotListener}.
         * Its own: no other instance, its pair's included, may write it. A restart restores from what it holds, so
         * it must outlive the process.
         */
        public Builder snapshotDirectory(final Path snapshotDirectory) {
            this.snapshotDirectory = snapshotDirectory;
            return this;
        }

        /**
         * Whether this instance holds no state until it is activated (doc/snapshot.md §4). Until then the listener
         * sees no payload and no connection, and takes part in no round, so it writes no snapshot; on activation the
         * instance restores the newest one its directory holds, or replays from {@code globalSeqNo} 1 without one,
         * before it serves. A failover to it races the activation deadline, so it suits state that replays well
         * inside 5 s.
         */
        public Builder passive(final boolean passive) {
            this.passive = passive;
            return this;
        }

        /** This replica's Replayer client id, unique among the co-located apps on its node. */
        public Builder clientId(final int clientId) {
            this.clientId = clientId;
            return this;
        }

        /** The node whose tap this instance follows. */
        public Builder memberId(final int memberId) {
            this.memberId = memberId;
            return this;
        }

        /** This client's own egress endpoint; two media drivers on one host cannot both bind a port. */
        public Builder egressChannel(final String egressChannel) {
            this.egressChannel = egressChannel;
            return this;
        }

        /** The members to reach, {@code PortLayout.ingressEndpoints()} by default. */
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

        public Gateway build() {
            Objects.requireNonNull(gatewayName, "gatewayName");
            Objects.requireNonNull(egressChannel, "egressChannel");
            Objects.requireNonNull(ingressEndpoints, "ingressEndpoints");
            Objects.requireNonNull(listener, "listener");
            if (snapshotListener != null && sourceId == UNRESOLVED) {
                throw new IllegalStateException("sourceId is required with a snapshotListener");
            }
            if (snapshotListener != null && snapshotDirectory == null) {
                throw new IllegalStateException("snapshotDirectory is required with a snapshotListener");
            }
            return new Gateway(this);
        }
    }
}
