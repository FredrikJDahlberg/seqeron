package org.limitless.seqeron.tools;

import io.aeron.Aeron;
import io.aeron.cluster.client.EgressListener;
import io.aeron.cluster.codecs.EventCode;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.limitless.seqeron.app.GatewayLifecycle;
import org.limitless.seqeron.app.PendingSends;
import org.limitless.seqeron.app.RecoveryStallFence;
import org.limitless.seqeron.sequencer.ClusterStreamSender;
import org.limitless.seqeron.sequencer.FrameLayer;
import org.limitless.seqeron.sequencer.IngressPublisher;
import org.limitless.seqeron.sequencer.IngressPublisher.Publish;
import org.limitless.seqeron.replayer.client.ReplayerStreamReceiver;
import org.limitless.seqeron.replayer.client.SequencedEvent;
import org.limitless.seqeron.sbe.frame.ConnectionClosedEncoder;
import org.limitless.seqeron.sbe.frame.ConnectionOpenedEncoder;
import org.limitless.seqeron.sbe.frame.GatewayActiveDecoder;
import org.limitless.seqeron.sbe.frame.GatewayRegisteredDecoder;
import org.limitless.seqeron.sbe.frame.GatewayStartedEncoder;
import org.limitless.seqeron.sbe.frame.MessageHeaderDecoder;
import org.limitless.seqeron.sbe.probe.ProbeMarkerDecoder;
import org.limitless.seqeron.sequencer.SystemFrame;
import org.limitless.seqeron.util.Logger;

/**
 * The edge-neutral probe's gateway — an elected active/standby producer
 * with a real listening socket, owned by the cluster tier and speaking no FIX.
 *
 * <p><b>Harness code, and it lives in the test source set</b> — nothing in a deployment runs it, so it is
 * in no jar. {@code chaos-runner.sh} launches it from {@code cluster/build/classes/java/test} beside the
 * uber jar, the way a test-tier tool is launched, and fails with a message
 * naming {@code ./gradlew :cluster:compileTestJava} if that has not been built. It sits in
 * {@link ClusterProbe}'s package because it reuses that class's cluster-connect and offer plumbing, which
 * is package-private and stays that way.
 *
 * <p>{@link ClusterProbe} gave core's harnesses a load generator and a tap consumer, which is what four of
 * the five needed. {@code chaos-runner.sh} needed a fifth thing they cannot supply: a <b>gateway pair</b>
 * under the faults. Everything that machinery does is the cluster tier's own — {@code GatewayStarted}
 * binding a cluster session to a {@code gatewayId}, the sequencer synthesizing {@code GatewayActive} on all
 * four paths, a standby opening its gate on promotion, and the four fences closing it — and none of it
 * needs a FIX codec or a product binary. This is that, and nothing else.
 *
 * <p><b>Its ingress is confirmed on the tap</b> (spec §16 A-4, A-5): every frame goes through an
 * {@link IngressPublisher} tracking into a {@link PendingSends}, which the sender also holds, so a leader
 * change loses nothing and reorders nothing. A publish the hold declines is retried on a later cycle, and
 * a {@code PendingSends} fault is a fifth fence. It is the reference user of that wiring.
 *
 * <p><b>The invariant it exists to hold</b> is the one both real gateways hold: nothing reaches the socket
 * that has not round-tripped consensus. A client line in becomes a {@code ProbeMarker} stamped with that
 * connection's {@code connectionId}, and the reply is written only when that frame comes back off the
 * co-located tap. That is what makes the accept gate observable from outside the process, which in turn is
 * what lets a harness assert "still accepting TCP with no media driver" — the sharpest thing the old
 * gateway rounds checked.
 *
 * <p>Two modes:
 * <ul>
 *   <li><b>serve</b> — the gateway. Resolves its identity from the {@code GatewayRegistered} row naming
 *       {@code probe.gatewayName}, publishes {@code GatewayStarted} when the cluster designates it, opens
 *       the accept gate, and closes it on any of the four fences.</li>
 *   <li><b>client</b> — a line client for it: connect, send {@code probe.count} lines, require an
 *       {@code ok} for each. Exit 0 iff every one round-tripped. The load generator and the liveness probe
 *       through the gate, so a harness needs no {@code nc}.</li>
 * </ul>
 *
 * <p>What it deliberately is not: no FIX, no session layer, no sequence numbers, no resend cache, no
 * reference data. A client connection is a socket and an int.
 *
 * <p>System properties, on top of {@link ClusterProbe}'s {@code probe.memberId} / {@code probe.aeronDir} /
 * {@code probe.ingressEndpoints}:
 * <pre>
 *   probe.gatewayName  — serve: the Gateway row's name this instance joins on; required
 *   probe.listenPort   — the TCP port: the gateway's to bind, the client's to connect to; default 9200
 *   probe.clientId     — serve: this replica's Replayer client id; default 10 (ClusterProbe follow's is 9)
 *   probe.count        — client: lines to send; default 1
 *   probe.host         — client: host to connect to; default localhost
 * </pre>
 */
public final class TestGateway {
    /** This harness's own {@link Logger} component — core's enum names only core's processes. */
    private enum Component implements Logger.Component {
        TestGateway
    }

    /** No single connection: a gateway-scoped frame, matching {@code ClusterIngress.NO_CONNECTION}. */
    private static final int NO_CONNECTION = -1;

    /**
     * The two fences that are a clock, the product gateways' constants verbatim — this holds the same
     * position they do, so a divergence here would make the harness prove something no gateway does.
     */
    private static final long TAP_STALL_TIMEOUT_MS = 20 * FrameLayer.CLUSTER_HEARTBEAT_INTERVAL_MS;

    private static final long RECOVERY_STALL_TIMEOUT_MS = 3 * TAP_STALL_TIMEOUT_MS;

    /** Exit status of a fenced instance, and of one whose media driver went away. Mirrors ClusterProbe's. */
    private static final int EXIT_FENCED = 70;

    private static final int READ_BUFFER_BYTES = 1024;

    private static final long CLIENT_REPLY_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(15);

    /** Frames in flight between a publish and the tap; far above what one round trip holds. */
    private static final int PENDING_CAPACITY = 1024;

    /**
     * The empty variable-length field, used twice: a probe connection has no identity but its id, so
     * {@code ConnectionOpened} carries no {@code connectionData}, and a request line's marker carries no
     * filler — the line's own bytes are not what is being round-tripped, its {@code seqNo} is.
     */
    private static final byte[] EMPTY = new byte[0];

    private final String gatewayName;
    private final int listenPort;
    private final int clientId;

    private final PendingSends pending = new PendingSends(PENDING_CAPACITY);
    private final IngressPublisher publisher = new IngressPublisher(pending);
    private final ClusterStreamSender sender = new ClusterStreamSender();
    private final ExpandableArrayBuffer body = new ExpandableArrayBuffer(256);
    private final ClusterProbe.MarkerEncoder marker = new ClusterProbe.MarkerEncoder();
    private final GatewayStartedEncoder gatewayStarted = new GatewayStartedEncoder();
    private final ConnectionOpenedEncoder connectionOpened = new ConnectionOpenedEncoder();
    private final ConnectionClosedEncoder connectionClosed = new ConnectionClosedEncoder();
    private final GatewayRegisteredDecoder gatewayRow = new GatewayRegisteredDecoder();
    private final GatewayActiveDecoder gatewayActive = new GatewayActiveDecoder();
    private final ProbeMarkerDecoder probeMarker = new ProbeMarkerDecoder();

    private final RecoveryStallFence recoveryStall = new RecoveryStallFence(RECOVERY_STALL_TIMEOUT_MS);
    private final GatewayLifecycle lifecycle;

    private final Map<Integer, Connection> connections = new HashMap<>();

    private Aeron aeron;
    private ReplayerStreamReceiver tap;
    private ServerSocketChannel acceptor;

    private boolean announcedCaughtUp;

    /** The highest {@code connectionId} this logical gateway's history holds; the resume point for §7's row. */
    private int highestConnectionId = NO_CONNECTION;
    private int nextConnectionId;
    private long markerSeqNo;

    private long lastTapProgressMs = System.currentTimeMillis();

    /**
     * The cluster's own account of why this session ended, kept for the fence's message. Recorded on the
     * egress poll and raised from {@link #checkFences}, never thrown from the listener: {@code Image.poll}
     * hands a fragment handler's exception to the Aeron error handler and advances the subscriber position
     * anyway, so a throw from here would be swallowed and the process would carry on.
     */
    private String sessionFault;

    private TestGateway(final String gatewayName, final int listenPort, final int clientId) {
        this.gatewayName = gatewayName;
        this.listenPort = listenPort;
        this.clientId = clientId;
        this.lifecycle = new GatewayLifecycle(gatewayName, new LifecycleActions());
    }

    public static void main(final String[] args) {
        final String mode = args.length > 0 ? args[0] : "help";
        final int listenPort = Integer.getInteger("probe.listenPort", 9200);
        switch (mode) {
            case "serve" -> {
                final String name = System.getProperty("probe.gatewayName");
                if (name == null || name.isEmpty()) {
                    System.err.println("[TestGateway] serve: -Dprobe.gatewayName is required");
                    System.exit(1);
                }
                System.exit(new TestGateway(name, listenPort, Integer.getInteger("probe.clientId", 10)).serve());
            }
            case "client" -> System.exit(client(System.getProperty("probe.host", "localhost"), listenPort,
                                                Integer.getInteger("probe.count", 1)));
            default -> {
                usage();
                System.exit(mode.equals("help") ? 0 : 1);
            }
        }
    }

    // ── serve ─────────────────────────────────────────────────────────────────────

    private int serve() {
        aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(ClusterProbe.aeronDir()));
        sender.setIngressEndpoints(ClusterProbe.ingressEndpoints());
        sender.setIngressHold(pending);
        sender.connect(aeron, ClusterProbe.egressChannel(), new SessionEventListener());
        tap = new ReplayerStreamReceiver(clientId, this::onSequenced,
                                         (leaderMemberId, leadershipTermId, globalSeqNo) ->
                                             pending.onLeadershipChanged(leadershipTermId), null);
        tap.start(aeron, ClusterProbe.memberId());
        log("standby — following the tap as %s, gate shut until a GatewayActive names this instance",
            gatewayName);

        final AtomicBoolean running = new AtomicBoolean(true);
        final AtomicBoolean fenced = new AtomicBoolean();
        final ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();
        final Thread duty = new Thread(() -> {
            final IdleStrategy idle = ClusterProbe.resolveIdleStrategy();
            try {
                while (running.get()) {
                    idle.idle(dutyCycle());
                }
            } catch (final RuntimeException | IOException ex) {
                // Fatal by design: every one of the three fatal fences (cluster session lost, tap stalled,
                // recovery stalled) arrives here, and so does the media driver going away. The gate is shut
                // and the process exits, which closes the cluster session — and that close is what makes
                // the sequencer promote the standby.
                Logger.error(Component.TestGateway, Logger.CoreEventCode.ClusterSessionError,
                             ClusterProbe.memberId(), "FENCED: %s", ex.getMessage());
                fenced.set(true);
                barrier.signalAll();
            }
        }, "probe-gateway-" + clientId);
        duty.start();

        barrier.await();
        running.set(false);
        try {
            duty.join(TimeUnit.SECONDS.toMillis(5));
        } catch (final InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
        // Sockets before the cluster session, as the product gateways close: a standby must be promoted against
        // an edge that is already free.
        closeGate();
        if (!fenced.get()) {
            tap.close();
            sender.close();
            aeron.close();
        }
        barrier.close();
        return fenced.get() ? EXIT_FENCED : 0;
    }

    /** @return units of work done, for the idle strategy */
    private int dutyCycle() throws IOException {
        int work = tap.poll();
        checkFences();
        work += sender.pollEgress();
        sender.keepAlive();
        work += pending.resendMissing(sender);
        work += announceCaughtUp();
        work += advanceGate();
        work += pollSockets();
        return work;
    }

    /**
     * The two fences that are a clock. The third — the cluster closing this session, with an event or
     * silently on a leader that never arrives — is read the same way the product gateways read it. The
     * fifth is ingress confirmation that can no longer be trusted.
     */
    private void checkFences() {
        if (sessionFault != null || sender.isSessionLost() || !sender.isConnected()) {
            // isConnected() as well as the recorded fault: AeronCluster also closes itself, with no event at
            // all, when a new leader does not arrive before its timeout — and an ERROR event, unlike a
            // CLOSED one, leaves the client open.
            throw new IllegalStateException("cluster session lost (" + (sessionFault != null ? sessionFault : "closed")
                                                + ") — this instance can never be promoted again");
        }
        if (pending.isFaulted()) {
            throw new IllegalStateException(
                "an own frame came back differing from the oldest pending one, so what reached the log can no "
                    + "longer be counted — releasing the cluster session so a standby can take over");
        }
        if (!tap.isCaughtUp()) {
            if (recoveryStall.onNotCaughtUp(System.currentTimeMillis(), tap.lastGlobalSeqNo())) {
                throw new IllegalStateException(
                    "recovery has dispatched nothing for >" + RECOVERY_STALL_TIMEOUT_MS + "ms (globalSeqNo stuck at "
                        + tap.lastGlobalSeqNo() + ") — releasing the cluster session so a standby can take over");
            }
            return;
        }
        if ((System.currentTimeMillis() - lastTapProgressMs) >= TAP_STALL_TIMEOUT_MS) {
            throw new IllegalStateException(
                "co-located tap stalled: no ClusterHeartbeat for >" + TAP_STALL_TIMEOUT_MS + "ms ("
                    + (TAP_STALL_TIMEOUT_MS / FrameLayer.CLUSTER_HEARTBEAT_INTERVAL_MS)
                    + " heartbeat periods) — releasing the cluster session so a standby can take over");
        }
    }

    /** Opens the gate once this instance is designated <em>and</em> caught up, never before. */
    private int advanceGate() {
        return tap.isCaughtUp() ? lifecycle.advance() : 0;
    }

    /** Shuts the listener and every connection; the lifecycle's stand-down and shutdown both come here. */
    private void closeGate() {
        if (acceptor != null) {
            close(acceptor);
            acceptor = null;
        }
        for (final Connection connection : connections.values()) {
            close(connection.channel);
        }
        connections.clear();
    }

    // ── the socket ────────────────────────────────────────────────────────────────

    private int pollSockets() throws IOException {
        if (acceptor == null) {
            return 0;
        }
        int work = 0;
        SocketChannel accepted;
        // Nothing is accepted while the hold is on: its ConnectionOpened could not be published.
        while (!pending.isHolding() && (accepted = acceptor.accept()) != null) {
            accepted.configureBlocking(false);
            accepted.setOption(StandardSocketOptions.TCP_NODELAY, true);
            final int connectionId = nextConnectionId++;
            connections.put(connectionId, new Connection(connectionId, accepted));
            work++;
        }
        // A publish the hold declines is retried next cycle: a connection's lifecycle frames wait on flags,
        // and an unsent line stays in its buffer.
        for (final Iterator<Connection> it = connections.values().iterator(); it.hasNext(); ) {
            final Connection connection = it.next();
            if (!connection.opened) {
                if (!publishConnection(SystemFrame.CONNECTION_OPENED, connection.id)) {
                    continue;
                }
                connection.opened = true;
                work++;
            }
            if (!connection.closing) {
                final int read = connection.channel.read(connection.in);
                if (read < 0) {
                    close(connection.channel);
                    connection.closing = true;
                } else if (read > 0 || connection.in.position() > 0) {
                    work += submitLines(connection);
                }
            }
            if (connection.closing) {
                if (!publishConnection(SystemFrame.CONNECTION_CLOSED, connection.id)) {
                    continue;
                }
                it.remove();
                work++;
            }
        }
        return work;
    }

    /**
     * Submits one {@code ProbeMarker} per complete line read. Nothing is written back here: the reply is
     * this frame's own return off the tap, which is the whole invariant.
     */
    private int submitLines(final Connection connection) {
        int work = 0;
        connection.in.flip();
        int consumed = 0;
        for (int i = connection.in.position(); i < connection.in.limit(); i++) {
            if (connection.in.get(i) != '\n') {
                continue;
            }
            final int length = marker.encodePayload(markerSeqNo + 1, ClusterProbe.NO_FILLER);
            if (!published(publisher.publishPayload(sender, lifecycle.gatewaySourceId(), connection.id,
                                                    ClusterProbe.PROBE_PAYLOAD_ID, marker.payload(), length))) {
                break;
            }
            markerSeqNo++;
            consumed = i + 1;
            work++;
        }
        connection.in.position(consumed);
        connection.in.compact();
        return work;
    }

    // ── the tap ───────────────────────────────────────────────────────────────────

    /**
     * One frame off the node-local tap. The list rows name this instance, the activations say whether it
     * serves, the heartbeat is the tap-liveness clock, and a {@code ProbeMarker} carrying this gateway's
     * own {@code sourceId} is a request that has been through consensus and may now be answered.
     */
    private void onSequenced(final SequencedEvent event) {
        pending.onSequenced(event);
        observeConnectionId(event);
        if (event.isSystem()) {
            switch (event.systemEventType()) {
                case SystemFrame.GATEWAY_REGISTERED -> onGatewayRow(event);
                case SystemFrame.GATEWAY_ACTIVE -> onGatewayActive(event);
                case SystemFrame.CLUSTER_HEARTBEAT -> lastTapProgressMs = System.currentTimeMillis();
                default -> { }
            }
            return;
        }
        if (event.payloadId() != ClusterProbe.PROBE_PAYLOAD_ID ||
            event.templateId() != ProbeMarkerDecoder.TEMPLATE_ID || event.sourceId() != lifecycle.gatewaySourceId()) {
            return;
        }
        probeMarker.wrap(event.buffer(), event.offset() + MessageHeaderDecoder.ENCODED_LENGTH,
                         event.blockLength(), event.version());
        reply(event.connectionId(), probeMarker.seqNo());
    }

    /**
     * The resume point {@code GatewayStarted.firstConnectionId} carries: the highest id this logical
     * gateway's history holds, whichever instance issued it. Read off every frame rather than off
     * {@code ConnectionOpened} alone so a standby's view cannot lag its predecessor's allocation.
     */
    private void observeConnectionId(final SequencedEvent event) {
        if (lifecycle.gatewaySourceId() != GatewayLifecycle.UNRESOLVED &&
            event.sourceId() == lifecycle.gatewaySourceId() && event.connectionId() > highestConnectionId) {
            highestConnectionId = event.connectionId();
        }
    }

    /** Writes the answer this request round-tripped for. A standby holds no sockets, so this is a no-op there. */
    private void reply(final int connectionId, final long seqNo) {
        final Connection connection = connections.get(connectionId);
        if (connection == null) {
            return;
        }
        try {
            connection.channel.write(ByteBuffer.wrap(("ok " + seqNo + "\n").getBytes(StandardCharsets.US_ASCII)));
        } catch (final IOException ex) {
            // The client went away between submitting and its frame coming back. pollSockets sees the
            // close on its next read and publishes ConnectionClosed; there is nothing to do here.
            log("connection %d: reply dropped (%s)", connectionId, ex.getMessage());
        }
    }

    private void onGatewayRow(final SequencedEvent event) {
        // A submitted system body carries no MessageHeader, so its block length and version come from this
        // build's own constants (doc/seqeron-protocol-spec.md §7, V-3).
        gatewayRow.wrap(event.buffer(), event.offset(), GatewayRegisteredDecoder.BLOCK_LENGTH,
                                MessageHeaderDecoder.SCHEMA_VERSION);
        lifecycle.onGatewayRegistered(gatewayRow.gatewayId(), gatewayRow.gatewaySourceId(), gatewayRow.gatewayName(),
                                      gatewayRow.preferenceRank());
    }

    private void onGatewayActive(final SequencedEvent event) {
        gatewayActive.wrap(event.buffer(), event.offset(), event.blockLength(), event.version());
        final int target = gatewayActive.gatewayId();
        final boolean wasActivated = lifecycle.isActivated();
        lifecycle.onGatewayActive(target);
        if (lifecycle.isActivated() != wasActivated) {
            log("GatewayActive(gatewayId=%d) — this instance (gatewayId=%d) is now %s", target, lifecycle.gatewayId(),
                wasActivated ? "standby" : "active");
        }
    }

    /**
     * The one line the harnesses wait on before they may drive this instance, and the arming of the
     * recovery fence — which is why it is a duty-cycle step rather than something a frame triggers: neither
     * may wait on the next {@code ClusterHeartbeat} to land.
     * @return units of work done
     */
    private int announceCaughtUp() {
        if (announcedCaughtUp || !tap.isCaughtUp()) {
            return 0;
        }
        announcedCaughtUp = true;
        recoveryStall.onCaughtUp();
        lifecycle.onCaughtUp();
        log("Caught up — following live at globalSeqNo %d", tap.lastGlobalSeqNo());
        return 1;
    }

    // ── ingress ───────────────────────────────────────────────────────────────────

    /** {@link GatewayLifecycle}'s side effects: the listener is the gate. */
    private final class LifecycleActions implements GatewayLifecycle.Actions {
        @Override
        public void identityResolved(final int gatewayId, final int gatewaySourceId, final int preferenceRank) {
            log("resolved: gatewayId=%d gatewaySourceId=%d rank=%d", gatewayId, gatewaySourceId, preferenceRank);
        }

        @Override
        public boolean publishGatewayStarted(final int gatewayId) {
            nextConnectionId = highestConnectionId + 1;
            gatewayStarted.wrap(body, 0);
            gatewayStarted.gatewayId(gatewayId).firstConnectionId(nextConnectionId);
            return publishSystem(SystemFrame.GATEWAY_STARTED, NO_CONNECTION, gatewayStarted.encodedLength());
        }

        @Override
        public boolean openGate() {
            try {
                acceptor = ServerSocketChannel.open();
                acceptor.configureBlocking(false);
                acceptor.setOption(StandardSocketOptions.SO_REUSEADDR, true);
                acceptor.bind(new InetSocketAddress(listenPort));
            } catch (final IOException ex) {
                throw new UncheckedIOException(ex); // fatal, as it was before
            }
            log("gate OPEN on port %d — gatewayId=%d gatewaySourceId=%d, connectionIds resume at %d", listenPort,
                lifecycle.gatewayId(), lifecycle.gatewaySourceId(), nextConnectionId);
            return true;
        }

        @Override
        public void closeGate() {
            TestGateway.this.closeGate();
        }
    }

    /** @return whether it was published; false is the hold or back-pressure, to retry next cycle */
    private boolean publishConnection(final int systemEventType, final int connectionId) {
        final int length;
        if (systemEventType == SystemFrame.CONNECTION_OPENED) {
            connectionOpened.wrap(body, 0);
            connectionOpened.putConnectionData(EMPTY, 0, 0);
            length = connectionOpened.encodedLength();
        } else {
            connectionClosed.wrap(body, 0);
            length = connectionClosed.encodedLength();
        }
        return publishSystem(systemEventType, connectionId, length);
    }

    private boolean publishSystem(final int systemEventType, final int connectionId, final int bodyLength) {
        return published(publisher.publishSystem(sender, lifecycle.gatewaySourceId(), connectionId, systemEventType,
                                                 body, bodyLength));
    }

    /** A refusal is this harness's own bug, never a condition to wait out. */
    private static boolean published(final Publish result) {
        if (result == Publish.Refused) {
            throw new IllegalStateException("a frame the sequencer would reject (doc/seqeron-protocol-spec.md §9.2)");
        }
        return result == Publish.Published;
    }

    // ── client ────────────────────────────────────────────────────────────────────

    /**
     * Sends {@code count} lines through the gate and requires an {@code ok} for each. Blocking and
     * one-connection: it is a test client, and every line it sends has to cross consensus, so nothing here
     * is on a path worth pipelining beyond what the socket already does.
     * @return 0 iff every line was answered
     */
    private static int client(final String host, final int port, final int count) {
        try (SocketChannel channel = SocketChannel.open()) {
            channel.connect(new InetSocketAddress(host, port));
            channel.configureBlocking(false);
            for (int i = 1; i <= count; i++) {
                channel.write(ByteBuffer.wrap(("probe " + i + "\n").getBytes(StandardCharsets.US_ASCII)));
            }
            final ByteBuffer in = ByteBuffer.allocate(READ_BUFFER_BYTES * 8);
            final long deadline = System.currentTimeMillis() + CLIENT_REPLY_TIMEOUT_MS;
            int replies = 0;
            while (replies < count && System.currentTimeMillis() < deadline) {
                final int read = channel.read(in);
                if (read < 0) {
                    break;
                }
                if (read == 0) {
                    Thread.onSpinWait();
                    continue;
                }
                for (int i = in.position() - read; i < in.position(); i++) {
                    if (in.get(i) == '\n') {
                        replies++;
                    }
                }
                if (!in.hasRemaining()) {
                    in.clear();
                }
            }
            if (replies < count) {
                System.err.printf("[TestGateway] client: %d/%d line(s) answered within %dms%n", replies, count,
                                  CLIENT_REPLY_TIMEOUT_MS);
                return 1;
            }
            System.out.printf("[TestGateway] client: %d/%d line(s) round-tripped through %s:%d%n", replies, count,
                              host, port);
            return 0;
        } catch (final IOException ex) {
            System.err.println("[TestGateway] client: " + ex);
            return 1;
        }
    }

    // ── plumbing ──────────────────────────────────────────────────────────────────

    /** Records why the cluster ended this session; {@link #checkFences} is where it becomes fatal. */
    private final class SessionEventListener implements EgressListener {
        @Override
        public void onMessage(final long clusterSessionId, final long timestamp,
                              final org.agrona.DirectBuffer buffer, final int offset, final int length,
                              final io.aeron.logbuffer.Header header) {
            // Nothing is addressed to this client on egress: everything it publishes comes back on the tap.
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

    /**
     * One accepted socket and the bytes read from it so far. A request line longer than the buffer stalls
     * that connection; the client sends short ones.
     */
    private static final class Connection {
        private final int id;
        private final SocketChannel channel;
        private final ByteBuffer in = ByteBuffer.allocate(READ_BUFFER_BYTES);
        private boolean opened;
        private boolean closing;

        private Connection(final int id, final SocketChannel channel) {
            this.id = id;
            this.channel = channel;
        }
    }

    private static void close(final java.nio.channels.Channel channel) {
        try {
            channel.close();
        } catch (final IOException ignored) {
            // Closing a socket that is already gone is the normal case on a fence.
        }
    }

    private static void log(final String format, final Object... args) {
        Logger.info(Component.TestGateway, ClusterProbe.memberId(), format, args);
    }

    private static void usage() {
        System.err.println("""
            Usage: TestGateway <serve|client>

              serve   run the gateway: -Dprobe.gatewayName=<row name> -Dprobe.listenPort=<port>
              client  send -Dprobe.count lines through the gate and require an ok for each

            See the class javadoc for the -Dprobe.* properties.""");
    }
}
