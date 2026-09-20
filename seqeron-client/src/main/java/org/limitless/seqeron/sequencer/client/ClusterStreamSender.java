package org.limitless.seqeron.sequencer.client;

import io.aeron.Aeron;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import io.aeron.cluster.codecs.EventCode;
import io.aeron.exceptions.AeronException;
import io.aeron.logbuffer.Header;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.YieldingIdleStrategy;
import org.limitless.seqeron.protocol.FrameLayer;
import org.limitless.seqeron.protocol.PortLayout;
import org.limitless.seqeron.util.Clocks;
import org.limitless.seqeron.util.Logger;

/**
 * The cluster session a producer submits on — the Java twin of {@code sequencer/client/ClusterStreamSender.hpp},
 * with the same method names and semantics. Far smaller, because {@code AeronCluster} already is the
 * cluster protocol the C++ class implements by hand; this adds only {@link #connectColocated}, a
 * {@link #send} that spins through back-pressure and elections, and a self-throttling {@link #keepAlive}.
 *
 * <p>One divergence: when leadership moves off the co-located member this reconnects (a new cluster session
 * id) where C++ swaps the publication, because {@code AeronCluster} owns its publication.
 *
 * <p>Not thread-safe: every method belongs to the caller's one duty-cycle thread.
 */
public final class ClusterStreamSender implements IngressSender, AutoCloseable {
    /** A sender with no session; {@link #connectColocated} or {@link #connect} opens one. */
    public ClusterStreamSender() {
    }

    /** Ingress channel of a client sharing its member's media driver. Only the leader subscribes to it. */
    private static final String INGRESS_CHANNEL_IPC = "aeron:ipc";

    /** Ingress channel of a client reaching members over the network; endpoints name the members. */
    private static final String INGRESS_CHANNEL_UDP = "aeron:udp";

    /** No co-located member: this sender reaches the cluster over the network. */
    private static final int NO_MEMBER = -1;

    private static final long CONNECT_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(10);

    /**
     * How long the client waits for a {@code NewLeader} before closing itself. The default, 2x the cluster's
     * 200ms {@code leaderHeartbeatTimeoutNs}, is shorter than an election.
     */
    private static final long NEW_LEADER_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(5);

    /** Well inside the cluster's {@code sequencer.sessionTimeoutMs} (1s), as the C++ twin's is. */
    private static final long KEEP_ALIVE_INTERVAL_MS = 200;

    /** How long {@link #send} spins before calling the session what it has become. */
    private static final long INGRESS_STALL_FATAL_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(10);

    private static final long BACKPRESSURE_ALERT_INTERVAL_MS = TimeUnit.SECONDS.toMillis(1);

    private final IdleStrategy idle = new YieldingIdleStrategy();
    private final EgressListener listener = new SessionListener();

    /** Which offer results are terminal, held where a test can drive it. */
    private final IngressStallPolicy stallPolicy =
        new IngressStallPolicy(INGRESS_STALL_FATAL_TIMEOUT_MS, BACKPRESSURE_ALERT_INTERVAL_MS);

    private Aeron aeron;
    private AeronCluster cluster;
    private EgressListener appListener;
    private String egressChannel;
    private String ingressEndpoints;
    private int colocatedMemberId = NO_MEMBER;
    private boolean sessionLost;
    private int newLeaderMemberId = NO_MEMBER;
    /** Whether the open session's ingress is the co-located member's {@code aeron:ipc}. */
    private boolean overIpc;
    /** IPC ingress lost its leader and reaches nobody else, so the session must be replaced. */
    private boolean reconnectDue;
    private IngressTracker hold;
    private boolean newLeaderDuringSend;
    private long lastKeepAliveMs;

    /**
     * Connects a client co-located with no member: UDP ingress against the whole endpoint set.
     *
     * @param egressChannel this client's own egress endpoint; two media drivers on one host cannot both
     *     bind a port (doc/registries.md §2)
     * @param ingressEndpoints the members to reach, {@link PortLayout#ingressEndpoints()} for the default set
     */
    public void connect(final Aeron aeron, final String egressChannel, final String ingressEndpoints) {
        connect(aeron, egressChannel, ingressEndpoints, null);
    }

    /** The same, for a caller that must see session events — a gateway's cluster-session fence. */
    public void connect(final Aeron aeron, final String egressChannel, final String ingressEndpoints,
                        final EgressListener appListener) {
        this.aeron = aeron;
        this.egressChannel = egressChannel;
        this.ingressEndpoints = requireEndpoints(ingressEndpoints);
        this.appListener = appListener;
        cluster = openSession(INGRESS_CHANNEL_UDP, ingressEndpoints, CONNECT_TIMEOUT_NS);
    }

    /**
     * Connects a client sharing a cluster member's Aeron directory: that member's {@code aeron:ipc} first,
     * within the short {@code ipcConnectTimeoutMs} since a follower never answers there, then UDP. If
     * leadership later moves away, {@link #pollEgress} reconnects over UDP; moving back is not chased.
     */
    public void connectColocated(final Aeron aeron, final int memberId, final long ipcConnectTimeoutMs,
                                 final String egressChannel, final String ingressEndpoints) {
        connectColocated(aeron, memberId, ipcConnectTimeoutMs, egressChannel, ingressEndpoints, null);
    }

    /** The same, for a caller that must see session events. */
    public void connectColocated(final Aeron aeron, final int memberId, final long ipcConnectTimeoutMs,
                                 final String egressChannel, final String ingressEndpoints,
                                 final EgressListener appListener) {
        this.aeron = aeron;
        this.colocatedMemberId = memberId;
        this.egressChannel = egressChannel;
        this.ingressEndpoints = requireEndpoints(ingressEndpoints);
        this.appListener = appListener;
        try {
            // No endpoints with IPC ingress: AeronCluster refuses the pair, and there is nothing to name.
            cluster = openSession(INGRESS_CHANNEL_IPC, null, TimeUnit.MILLISECONDS.toNanos(ipcConnectTimeoutMs));
        } catch (final AeronException ex) {
            Logger.error(Logger.CoreComponent.Cluster, Logger.CoreEventCode.ClusterIpcFallback, memberId,
                         "member %d did not answer ingress on %s (%s) — falling back to UDP", memberId,
                         INGRESS_CHANNEL_IPC, ex.getMessage());
            cluster = openSession(INGRESS_CHANNEL_UDP, ingressEndpoints, CONNECT_TIMEOUT_NS);
        }
    }

    /** The UDP fallback and the reconnect both need them, so a connect without them could never recover. */
    private static String requireEndpoints(final String ingressEndpoints) {
        return Objects.requireNonNull(ingressEndpoints, "ingressEndpoints");
    }

    /** Hears every {@code NewLeader}, and gives up a send that met one while it holds. */
    public void setIngressHold(final IngressTracker hold) {
        this.hold = hold;
    }

    /**
     * Offers one pre-encoded frame to cluster ingress, spinning until it lands. {@code CLOSED} is not
     * terminal: during an election {@code AeronCluster} closes the ingress publication and waits for a
     * {@code NewLeader}, which {@code pollEgress} installs — hence the spin polls. Bounded, since losing
     * quorum would otherwise stop the caller's duty cycle. No keep-alive from in here: it would offer on the
     * same stuck publication.
     *
     * @return false when there is no session left to take it, or when a {@code NewLeader} arrived mid-spin
     *     while the {@link IngressTracker} holds: nothing was placed, and the frame goes again once it releases
     */
    @Override
    public boolean send(final DirectBuffer frame, final int length) {
        if (cluster == null || cluster.isClosed() || length == 0) {
            return false;
        }
        if (length > FrameLayer.MAX_INGRESS_LENGTH) {
            throw new IllegalArgumentException("ingress frame " + length + " exceeds MAX_INGRESS_LENGTH "
                                               + FrameLayer.MAX_INGRESS_LENGTH);
        }
        long result;
        newLeaderDuringSend = false;
        while ((result = cluster.offer(frame, 0, length)) < 0) {
            final long now = Clocks.monotonicMs();
            switch (stallPolicy.onOfferFailed(now, result, sessionLost, cluster.isClosed())) {
            case FATAL:
                throw new IllegalStateException("cluster ingress offer failed: " + result);
            case SESSION_GONE:
                return false;
            case STALLED:
                Logger.error(Logger.CoreComponent.Cluster, Logger.CoreEventCode.ClusterOfferFailed, member(),
                             "cluster ingress took no frame for %ds — calling the session lost",
                             TimeUnit.MILLISECONDS.toSeconds(INGRESS_STALL_FATAL_TIMEOUT_MS));
                sessionLost = true;
                return false;
            case ALERT:
                Logger.error(Logger.CoreComponent.Cluster, Logger.CoreEventCode.ClusterOfferFailed, member(),
                             "cluster ingress back-pressured (offer=%d) for %dms", result,
                             stallPolicy.blockedMs(now));
                break;
            case RETRY:
            default:
                break;
            }
            pollEgress();
            if (newLeaderDuringSend && hold != null && hold.isHolding()) {
                return false;
            }
            idle.idle();
        }
        stallPolicy.onOffered();
        return true;
    }

    /** Sends a keep-alive if the interval has elapsed; call it every duty-cycle iteration. */
    public void keepAlive() {
        if (cluster == null || cluster.isClosed()) {
            return;
        }
        final long now = Clocks.monotonicMs();
        if (now - lastKeepAliveMs < KEEP_ALIVE_INTERVAL_MS) {
            return;
        }
        lastKeepAliveMs = now;
        if (!cluster.sendKeepAlive()) {
            Logger.error(Logger.CoreComponent.Cluster, Logger.CoreEventCode.ClusterOfferFailed, member(),
                         "keep-alive offer failed");
        }
    }

    /**
     * Drains cluster egress, then applies any reconnect it called for, outside the poll.
     *
     * @return fragments read
     */
    public int pollEgress() {
        if (cluster == null) {
            return 0;
        }
        final int fragments = cluster.pollEgress();
        if (reconnectDue) {
            reconnectOverUdp();
        }
        return fragments;
    }

    /** Whether a session is open. */
    public boolean isConnected() {
        return cluster != null && !cluster.isClosed();
    }

    /** True once the cluster has closed this session, as opposed to never having opened one. Latched. */
    public boolean isSessionLost() {
        return sessionLost;
    }

    @Override
    public long clusterSessionId() {
        return cluster == null ? Aeron.NULL_VALUE : cluster.clusterSessionId();
    }

    /** {@code AeronCluster} moves its stamp only inside {@link #pollEgress}, which send polls between failed offers. */
    @Override
    public long leadershipTermId() {
        return cluster == null ? Aeron.NULL_VALUE : cluster.leadershipTermId();
    }

    /** Closes the session. The Aeron client is the caller's and is left open. */
    @Override
    public void close() {
        CloseHelper.quietClose(cluster);
        cluster = null;
    }

    private AeronCluster openSession(final String ingressChannel, final String ingressEndpoints,
                                     final long timeoutNs) {
        final AeronCluster session = AeronCluster.connect(new AeronCluster.Context()
            .aeron(aeron)
            .ingressChannel(ingressChannel)
            .ingressEndpoints(ingressEndpoints)
            .egressChannel(egressChannel)
            .egressListener(listener)
            .messageTimeoutNs(timeoutNs)
            .newLeaderTimeoutNs(NEW_LEADER_TIMEOUT_NS));
        overIpc = INGRESS_CHANNEL_IPC.equals(ingressChannel);
        reconnectDue = false;
        return session;
    }

    /**
     * Replaces an IPC session whose leader moved away: with no endpoints, {@code AeronCluster} would wait on
     * the same {@code aeron:ipc} forever. UDP lets the cluster client chase the leader itself.
     */
    private void reconnectOverUdp() {
        Logger.info(Logger.CoreComponent.Cluster, member(),
                    "leadership moved to member %d — reconnecting ingress over UDP", newLeaderMemberId);
        CloseHelper.quietClose(cluster);
        cluster = openSession(INGRESS_CHANNEL_UDP, ingressEndpoints, CONNECT_TIMEOUT_NS);
    }

    private Integer member() {
        return colocatedMemberId == NO_MEMBER ? null : colocatedMemberId;
    }

    /** Watches egress for what this class acts on, and passes every event to the caller's listener. */
    private final class SessionListener implements EgressListener {
        @Override
        public void onMessage(final long clusterSessionId, final long timestamp, final DirectBuffer buffer,
                              final int offset, final int length, final Header header) {
            if (appListener != null) {
                appListener.onMessage(clusterSessionId, timestamp, buffer, offset, length, header);
            }
        }

        @Override
        public void onSessionEvent(final long correlationId, final long clusterSessionId,
                                   final long leadershipTermId, final int leaderMemberId, final EventCode code,
                                   final String detail) {
            if (code == EventCode.CLOSED || code == EventCode.ERROR) {
                sessionLost = true;
            }
            if (appListener != null) {
                appListener.onSessionEvent(correlationId, clusterSessionId, leadershipTermId, leaderMemberId,
                                           code, detail);
            }
        }

        @Override
        public void onNewLeader(final long clusterSessionId, final long leadershipTermId,
                               final int leaderMemberId, final String ingressEndpoints) {
            newLeaderMemberId = leaderMemberId;
            newLeaderDuringSend = true;
            if (hold != null) {
                hold.onNewLeader(leadershipTermId);
            }
            // Leadership coming back is deliberately not chased, unlike the C++ twin: it would cost a session.
            reconnectDue = overIpc && leaderMemberId != colocatedMemberId;
            if (appListener != null) {
                appListener.onNewLeader(clusterSessionId, leadershipTermId, leaderMemberId, ingressEndpoints);
            }
        }
    }
}
