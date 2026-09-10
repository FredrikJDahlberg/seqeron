package org.limitless.seqeron.sequencer;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import io.aeron.cluster.codecs.EventCode;
import io.aeron.exceptions.AeronException;
import io.aeron.logbuffer.Header;
import java.util.concurrent.TimeUnit;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.YieldingIdleStrategy;
import org.limitless.seqeron.util.Logger;

/**
 * The cluster session a producer submits on — the Java twin of {@code sequencer/ClusterStreamSender.hpp},
 * with the same method names and the same semantics.
 *
 * <p><b>The two halves are not the same size, and that is the point.</b> The C++ class implements the
 * cluster wire protocol itself (SessionConnectRequest, SessionEvent, redirect, NewLeaderEvent, the
 * ingress publication) because Aeron's C++ client has no cluster client. Java's {@code AeronCluster} is
 * that protocol, so this class is only what {@code AeronCluster} does not do:
 *
 * <ul>
 *   <li>{@link #connectColocated} — ingress over the co-located member's own {@code aeron:ipc}, falling
 *       back to the UDP endpoint set when that member is not the leader;</li>
 *   <li>{@link #send} — an offer that spins through back-pressure and an election rather than dropping
 *       the frame;</li>
 *   <li>{@link #keepAlive} — self-throttling, so a duty cycle calls it every iteration.</li>
 * </ul>
 *
 * <p><b>One behavioural divergence from the C++ twin</b>, forced by {@code AeronCluster} owning its own
 * ingress publication: when leadership moves off the co-located member, C++ swaps the publication inside
 * the live session, while this class reconnects — the cluster session id changes at that moment. It
 * matters to a producer whose session identity is bound to something (a gateway's {@code GatewayStarted}
 * binding, spec §5 S-6); it does not to a co-located application, whose {@code sessionId} on the wire is
 * advisory and overwritten by the sequencer anyway.
 *
 * <p>Not thread-safe: every method belongs to the caller's one duty-cycle thread.
 */
public final class ClusterStreamSender implements IngressSender, AutoCloseable {
    /** Ingress channel of a client sharing its member's media driver. Only the leader subscribes to it. */
    public static final String INGRESS_CHANNEL_IPC = "aeron:ipc";

    /** Ingress channel of a client reaching members over the network; endpoints name the members. */
    public static final String INGRESS_CHANNEL_UDP = "aeron:udp";

    /** Members the UDP endpoint set names by default — the cluster is bounded at three (registries §2). */
    public static final int DEFAULT_NODE_COUNT = 3;

    /** No co-located member: this sender reaches the cluster over the network. */
    private static final int NO_MEMBER = -1;

    private static final long CONNECT_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(10);

    /**
     * How long the client waits for a {@code NewLeader} event before closing itself. Set explicitly
     * because the default is 2x the cluster's {@code leaderHeartbeatTimeoutNs}, which
     * {@link SequencerServer} tunes to 200ms — leaving a client 400ms of patience for an election that
     * takes closer to a second.
     */
    private static final long NEW_LEADER_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(5);

    /** Well inside the cluster's {@code sequencer.sessionTimeoutMs} (1s), as the C++ twin's is. */
    private static final long KEEP_ALIVE_INTERVAL_NS = TimeUnit.MILLISECONDS.toNanos(200);

    /** How long {@link #send} spins before calling the session what it has become. */
    private static final long INGRESS_STALL_FATAL_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(10);

    private static final long BACKPRESSURE_ALERT_INTERVAL_NS = TimeUnit.SECONDS.toNanos(1);

    private final IdleStrategy idle = new YieldingIdleStrategy();
    private final EgressListener listener = new SessionListener();

    private Aeron aeron;
    private AeronCluster cluster;
    private EgressListener appListener;
    private String egressChannel;
    private String ingressEndpoints;
    private int colocatedMemberId = NO_MEMBER;
    private boolean ipcIngress;
    private boolean sessionLost;
    private boolean reconnectPending;
    private int newLeaderMemberId = NO_MEMBER;
    private int sourceId;
    private long lastKeepAliveNs;

    /**
     * Connects a client that is not co-located with any member: UDP ingress against the whole endpoint
     * set, which is where the cluster's own redirect and leader-chasing take it from.
     *
     * @param egressChannel this client's own egress endpoint; two media drivers on one host cannot both
     *     bind a port, so every client needs one of its own (doc/registries.md §2)
     */
    public void connect(final Aeron aeron, final String egressChannel) {
        connect(aeron, egressChannel, null);
    }

    /** The same, for a caller that must see session events — a gateway's cluster-session fence. */
    public void connect(final Aeron aeron, final String egressChannel, final EgressListener appListener) {
        this.aeron = aeron;
        this.egressChannel = egressChannel;
        this.appListener = appListener;
        cluster = openSession(INGRESS_CHANNEL_UDP, udpEndpoints(), CONNECT_TIMEOUT_NS);
    }

    /**
     * Connects a client deployed co-located with one cluster member, sharing that member's Aeron
     * directory. Ingress goes over that member's own {@code aeron:ipc} first, on the theory that it
     * usually is (or shortly becomes) the leader — a follower opens no IPC ingress subscription at all,
     * so the connect request there simply goes unanswered, which is why the short
     * {@code ipcConnectTimeoutMs} comes before the UDP endpoint set and its full budget.
     *
     * <p>Leadership moving away afterwards is handled too: {@link #pollEgress} sees the new leader is not
     * this member and reconnects over UDP. Moving back is not chased, unlike the C++ twin — a reconnect
     * costs a session where its publication swap does not.
     */
    public void connectColocated(final Aeron aeron, final int memberId, final long ipcConnectTimeoutMs,
                                 final String egressChannel) {
        connectColocated(aeron, memberId, ipcConnectTimeoutMs, egressChannel, null);
    }

    /** The same, for a caller that must see session events. */
    public void connectColocated(final Aeron aeron, final int memberId, final long ipcConnectTimeoutMs,
                                 final String egressChannel, final EgressListener appListener) {
        this.aeron = aeron;
        this.colocatedMemberId = memberId;
        this.egressChannel = egressChannel;
        this.appListener = appListener;
        try {
            // No endpoints with IPC ingress: AeronCluster refuses the pair, and there is nothing to name.
            cluster = openSession(INGRESS_CHANNEL_IPC, null, TimeUnit.MILLISECONDS.toNanos(ipcConnectTimeoutMs));
            ipcIngress = true;
        } catch (final AeronException ex) {
            Logger.error(Logger.Component.Cluster, Logger.EventCode.ClusterIpcFallback, memberId,
                         "member %d did not answer ingress on %s (%s) — falling back to UDP", memberId,
                         INGRESS_CHANNEL_IPC, ex.getMessage());
            cluster = openSession(INGRESS_CHANNEL_UDP, udpEndpoints(), CONNECT_TIMEOUT_NS);
        }
    }

    /**
     * Names the UDP endpoint set explicitly, instead of the {@link #DEFAULT_NODE_COUNT}-member one this
     * class derives from the port formula. For a deployment that is not the default — an operator tool
     * pointed at another cluster. Must be set before connecting.
     *
     * <p>The C++ twin has no equivalent: it resolves one endpoint and lets the cluster's REDIRECT carry it
     * to the leader, where {@code AeronCluster} takes the whole set and chases the leader itself.
     */
    public void setIngressEndpoints(final String ingressEndpoints) {
        this.ingressEndpoints = ingressEndpoints;
    }

    /**
     * Offers one pre-encoded frame to cluster ingress, spinning until it lands.
     *
     * <p><b>{@code CLOSED} is not terminal here.</b> A leader that dies closes the client's egress image,
     * and {@code AeronCluster} responds by closing the ingress publication and waiting for a
     * {@code NewLeader} event — so every offer returns {@code CLOSED} for the length of the election, on a
     * session the cluster still holds. The publication that replaces it is installed by {@code pollEgress},
     * which is why the spin polls.
     *
     * <p>Bounded, and the bound is not a detail: the spin's other two exits both need a leader — one to
     * accept the frame, the other to tell us the session is gone. Lose quorum and neither comes, and an
     * unbounded spin would stop the caller's whole duty cycle with it.
     *
     * <p>No keep-alive is pumped from in here, deliberately, as in the C++ twin: it would offer on the same
     * publication, and one that will not take this frame will not take a keep-alive either.
     *
     * @return false when there is no session left to take it
     */
    @Override
    public boolean send(final DirectBuffer frame, final int length) {
        if (cluster == null || cluster.isClosed() || length == 0) {
            return false;
        }
        if (length > FrameLayer.MAX_INGRESS_LENGTH) {
            // A frame too large to place is a programming error no runtime handling repairs, and silently
            // dropping it would tear the very continuity the caller is submitting to preserve.
            throw new IllegalArgumentException("ingress frame " + length + " exceeds MAX_INGRESS_LENGTH "
                                               + FrameLayer.MAX_INGRESS_LENGTH);
        }
        long blockedSinceNs = 0;
        long nextAlertNs = 0;
        long result;
        while ((result = cluster.offer(frame, 0, length)) < 0) {
            if (result == Publication.MAX_POSITION_EXCEEDED) {
                throw new IllegalStateException("cluster ingress offer failed: " + result);
            }
            final long now = System.nanoTime();
            if (blockedSinceNs == 0) {
                blockedSinceNs = now;
                nextAlertNs = now + BACKPRESSURE_ALERT_INTERVAL_NS;
            }
            if (sessionLost || cluster.isClosed()) {
                return false;
            }
            if (now - blockedSinceNs >= INGRESS_STALL_FATAL_TIMEOUT_NS) {
                Logger.error(Logger.Component.Cluster, Logger.EventCode.ClusterOfferFailed, member(),
                             "cluster ingress took no frame for %ds — calling the session lost",
                             TimeUnit.NANOSECONDS.toSeconds(INGRESS_STALL_FATAL_TIMEOUT_NS));
                sessionLost = true;
                return false;
            }
            if (now >= nextAlertNs) {
                Logger.error(Logger.Component.Cluster, Logger.EventCode.ClusterOfferFailed, member(),
                             "cluster ingress back-pressured (offer=%d) for %dms", result,
                             TimeUnit.NANOSECONDS.toMillis(now - blockedSinceNs));
                nextAlertNs = now + BACKPRESSURE_ALERT_INTERVAL_NS;
            }
            pollEgress();
            idle.idle();
        }
        return true;
    }

    /**
     * Sends a keep-alive if the interval has elapsed. Self-throttling, like the C++ twin's: a duty cycle
     * calls it every iteration and this decides when one is due.
     */
    public void keepAlive() {
        if (cluster == null || cluster.isClosed()) {
            return;
        }
        final long now = System.nanoTime();
        if (now - lastKeepAliveNs < KEEP_ALIVE_INTERVAL_NS) {
            return;
        }
        lastKeepAliveNs = now;
        if (!cluster.sendKeepAlive()) {
            Logger.error(Logger.Component.Cluster, Logger.EventCode.ClusterOfferFailed, member(),
                         "keep-alive offer failed");
        }
    }

    /**
     * Drains cluster egress, then acts on what it held. The reconnect is applied out here rather than from
     * inside the poll, for the reason the C++ twin never builds a publication inside one.
     *
     * @return fragments read
     */
    public int pollEgress() {
        if (cluster == null) {
            return 0;
        }
        final int fragments = cluster.pollEgress();
        if (reconnectPending) {
            reconnectPending = false;
            reconnectOverUdp();
        }
        return fragments;
    }

    /** Whether a session is open. */
    public boolean isConnected() {
        return cluster != null && !cluster.isClosed();
    }

    /**
     * True once the cluster has closed this client's session — as opposed to never having connected one.
     * Latched: there is no re-handshake, so a caller whose work is only valid with a session stops here.
     */
    public boolean isSessionLost() {
        return sessionLost;
    }

    @Override
    public long clusterSessionId() {
        return cluster == null ? Aeron.NULL_VALUE : cluster.clusterSessionId();
    }

    /** This process's fixed identity in spec §5's one id space; the caller stamps it into each frame. */
    public int sourceId() {
        return sourceId;
    }

    public void setSourceId(final int sourceId) {
        this.sourceId = sourceId;
    }

    /** Closes the session. The Aeron client is the caller's and is left open. */
    @Override
    public void close() {
        CloseHelper.quietClose(cluster);
        cluster = null;
    }

    /**
     * The ingress endpoint set for a {@code nodeCount}-member cluster, in {@code AeronCluster}'s
     * {@code id=host:port} form, off {@link SequencerServer}'s port formula rather than a restatement of
     * the numbers.
     */
    public static String ingressEndpoints(final int nodeCount) {
        final StringBuilder endpoints = new StringBuilder();
        for (int id = 0; id < nodeCount; id++) {
            if (id > 0) {
                endpoints.append(',');
            }
            endpoints.append(id).append('=').append(SequencerServer.ingressEndpoint(id));
        }
        return endpoints.toString();
    }

    private String udpEndpoints() {
        return ingressEndpoints != null ? ingressEndpoints : ingressEndpoints(DEFAULT_NODE_COUNT);
    }

    private AeronCluster openSession(final String ingressChannel, final String ingressEndpoints,
                                     final long timeoutNs) {
        return AeronCluster.connect(new AeronCluster.Context()
            .aeron(aeron)
            .ingressChannel(ingressChannel)
            .ingressEndpoints(ingressEndpoints)
            .egressChannel(egressChannel)
            .egressListener(listener)
            .messageTimeoutNs(timeoutNs)
            .newLeaderTimeoutNs(NEW_LEADER_TIMEOUT_NS));
    }

    /**
     * Leadership left the co-located member, and IPC ingress reaches nobody else: with no endpoints to
     * chase, {@code AeronCluster} would re-add the publication on the same {@code aeron:ipc} and wait
     * there. So the session is replaced by one over UDP, where the cluster's own leader chasing works.
     */
    private void reconnectOverUdp() {
        Logger.info(Logger.Component.Cluster, member(),
                    "leadership moved to member %d — reconnecting ingress over UDP", newLeaderMemberId);
        CloseHelper.quietClose(cluster);
        cluster = openSession(INGRESS_CHANNEL_UDP, udpEndpoints(), CONNECT_TIMEOUT_NS);
        ipcIngress = false;
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
            reconnectPending = ipcIngress && leaderMemberId != colocatedMemberId;
            if (appListener != null) {
                appListener.onNewLeader(clusterSessionId, leadershipTermId, leaderMemberId, ingressEndpoints);
            }
        }
    }
}
