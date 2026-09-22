package org.limitless.seqeron.tools;

import io.aeron.Aeron;
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
import org.agrona.DirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.limitless.seqeron.app.Fence;
import org.limitless.seqeron.app.Gateway;
import org.limitless.seqeron.app.Payload;
import org.limitless.seqeron.protocol.Publish;
import org.limitless.seqeron.sbe.probe.ProbeMarkerDecoder;
import org.limitless.seqeron.util.IdleStrategies;
import org.limitless.seqeron.util.Logger;

/**
 * The edge-neutral probe's gateway — an elected active/standby producer with a real listening socket, owned
 * by the cluster tier and speaking no FIX.
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
 * four paths, a standby opening its gate on promotion, and the fences closing it — and none of it
 * needs a FIX codec or a product binary. This is that, and nothing else.
 *
 * <p><b>It is the reference consumer of {@link Gateway}</b>, the client tier's façade for one instance of an
 * elected pair: the election, the connection id space, the connection lifecycle frames, confirmed ingress
 * (spec §16 A-4, A-5) and the fences are all behind it, so what is left here is a socket and a line
 * protocol. Nothing of the frame layer or of seqeron's system vocabulary appears below.
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
 *   <li><b>serve</b> — the gateway. Joins the {@code GatewayRegistered} row naming {@code probe.gatewayName},
 *       opens the accept gate when the cluster designates it, and closes it on a stand-down or a fence.</li>
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

    /** Exit status of a fenced instance, and of one whose media driver went away. Mirrors ClusterProbe's. */
    private static final int EXIT_FENCED = 70;

    private static final int READ_BUFFER_BYTES = 1024;

    private static final long CLIENT_REPLY_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(15);

    private final String gatewayName;
    private final int listenPort;
    private final int clientId;

    private final ClusterProbe.MarkerEncoder marker = new ClusterProbe.MarkerEncoder();
    private final ProbeMarkerDecoder probeMarker = new ProbeMarkerDecoder();

    private final Map<Integer, Connection> connections = new HashMap<>();
    private final AtomicBoolean fenced = new AtomicBoolean();
    private final ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();

    private Aeron aeron;
    private Gateway gateway;
    private ServerSocketChannel acceptor;

    /** Mirrors {@code Gateway.isActivated()}, so the designation is logged on its edge and only there. */
    private boolean activated;

    private long markerSeqNo;

    private TestGateway(final String gatewayName, final int listenPort, final int clientId) {
        this.gatewayName = gatewayName;
        this.listenPort = listenPort;
        this.clientId = clientId;
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
        gateway = Gateway.builder()
            .gatewayName(gatewayName)
            .clientId(clientId)
            .memberId(ClusterProbe.memberId())
            .egressChannel(ClusterProbe.egressChannel())
            .ingressEndpoints(ClusterProbe.ingressEndpoints())
            .listener(new GateListener())
            .build();
        gateway.start(aeron);
        log("standby — following the tap as %s, gate shut until a GatewayActive names this instance",
            gatewayName);

        final AtomicBoolean running = new AtomicBoolean(true);
        final Thread duty = new Thread(() -> {
            final IdleStrategy idle = IdleStrategies.fromProperty(ClusterProbe.IDLE_STRATEGY_PROPERTY).get();
            try {
                while (running.get()) {
                    idle.idle(dutyCycle());
                }
            } catch (final RuntimeException | IOException ex) {
                // The media driver going away, or the edge failing to open. Every fence the cluster tier
                // raises arrives at onFenced instead.
                fence("FENCED: " + ex.getMessage());
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
            gateway.close();
            aeron.close();
        }
        barrier.close();
        return fenced.get() ? EXIT_FENCED : 0;
    }

    /** @return units of work done, for the idle strategy */
    private int dutyCycle() throws IOException {
        int work = gateway.doWork();
        work += logActivation();
        work += pollSockets();
        return work;
    }

    /**
     * The designation, logged on the edge where it changes. {@code chaos-runner.sh} reads this line to tell
     * which instance of the pair is live, so it tracks the activation rather than the gate.
     * @return units of work done
     */
    private int logActivation() {
        if (gateway.isActivated() == activated) {
            return 0;
        }
        activated = !activated;
        log("GatewayActive — this instance (gatewayId=%d) is now %s", gateway.gatewayId(),
            activated ? "active" : "standby");
        return 1;
    }

    /** Shuts the listener and every connection; the stand-down and the shutdown both come here. */
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

    /** Records the fence and releases the main thread; the process exits, which closes the cluster session. */
    private void fence(final String message) {
        Logger.error(Component.TestGateway, Logger.CoreEventCode.ClusterSessionError, ClusterProbe.memberId(),
                     "%s", message);
        fenced.set(true);
        barrier.signalAll();
    }

    // ── the socket ────────────────────────────────────────────────────────────────

    private int pollSockets() throws IOException {
        if (acceptor == null) {
            return 0;
        }
        int work = 0;
        SocketChannel accepted;
        // Nothing is accepted while ingress is held: the connection's ConnectionOpened could not be placed.
        while (gateway.canAccept() && (accepted = acceptor.accept()) != null) {
            final int connectionId = gateway.openConnection();
            if (connectionId == Gateway.NO_CONNECTION) {
                close(accepted);
                break;
            }
            accepted.configureBlocking(false);
            accepted.setOption(StandardSocketOptions.TCP_NODELAY, true);
            connections.put(connectionId, new Connection(connectionId, accepted));
            work++;
        }
        for (final Iterator<Connection> it = connections.values().iterator(); it.hasNext(); ) {
            final Connection connection = it.next();
            final int read = connection.channel.read(connection.in);
            if (read < 0) {
                close(connection.channel);
                gateway.closeConnection(connection.id);
                it.remove();
                work++;
                continue;
            }
            if (read > 0 || connection.in.position() > 0) {
                work += submitLines(connection);
            }
        }
        return work;
    }

    /**
     * Submits one {@code ProbeMarker} per complete line read. Nothing is written back here: the reply is
     * this frame's own return off the tap, which is the whole invariant. A publish the gateway declines —
     * ingress held, back-pressured, or the connection not yet announced — leaves the line in its buffer.
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
            if (!published(gateway.publish(connection.id, ClusterProbe.PROBE_PAYLOAD_ID, marker.payload(),
                                           length))) {
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
            // close on its next read and closes the connection; there is nothing to do here.
            log("connection %d: reply dropped (%s)", connectionId, ex.getMessage());
        }
    }

    /** A refusal is this harness's own bug, never a condition to wait out. */
    private static boolean published(final Publish result) {
        if (result == Publish.Refused) {
            throw new IllegalStateException("a frame the sequencer would reject (doc/seqeron-protocol-spec.md §9.2)");
        }
        return result == Publish.Published;
    }

    // ── the gateway ───────────────────────────────────────────────────────────────

    /** The edge: what {@link Gateway} cannot do for a gateway that owns a socket. */
    private final class GateListener implements Gateway.Listener {
        @Override
        public boolean onActivated(final int firstConnectionId) {
            try {
                acceptor = ServerSocketChannel.open();
                acceptor.configureBlocking(false);
                acceptor.setOption(StandardSocketOptions.SO_REUSEADDR, true);
                acceptor.bind(new InetSocketAddress(listenPort));
            } catch (final IOException ex) {
                throw new UncheckedIOException(ex); // fatal, as it was before
            }
            log("gate OPEN on port %d — gatewayId=%d gatewaySourceId=%d, connectionIds resume at %d", listenPort,
                gateway.gatewayId(), gateway.sourceId(), firstConnectionId);
            return true;
        }

        @Override
        public void onStandby() {
            closeGate();
        }

        @Override
        public void onConnectionOpened(final int connectionId, final DirectBuffer connectionData, final int offset,
                                       final int length) {
            // This harness keeps no session state, so a predecessor's connections are nothing to rebuild.
        }

        @Override
        public void onConnectionClosed(final int connectionId) {
            // As above.
        }

        @Override
        public void onClusterHeartbeat(final long clusterTimeNs, final long receiveTimeNs) {
            // This harness holds no session state, so it has no timer to drive off the cluster clock.
        }

        /**
         * A {@code ProbeMarker} carrying this gateway's own {@code sourceId} is a request that has been
         * through consensus and may now be answered.
         */
        @Override
        public void onSequenced(final Payload payload) {
            if (payload.payloadId() != ClusterProbe.PROBE_PAYLOAD_ID ||
                payload.templateId() != ProbeMarkerDecoder.TEMPLATE_ID || payload.sourceId() != gateway.sourceId()) {
                return;
            }
            probeMarker.wrap(payload.buffer(), payload.bodyOffset(), payload.blockLength(), payload.version());
            reply(payload.connectionId(), probeMarker.seqNo());
        }

        /** The one line the harnesses wait on before they may drive this instance. */
        @Override
        public void onCaughtUp(final long globalSeqNo) {
            log("Caught up — following live at globalSeqNo %d", globalSeqNo);
        }

        @Override
        public void onFenced(final Fence reason, final String detail) {
            fence("FENCED: " + reason + " — " + detail
                      + " — releasing the cluster session so a standby can take over");
        }
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

    /**
     * One accepted socket and the bytes read from it so far. A request line longer than the buffer stalls
     * that connection; the client sends short ones.
     */
    private static final class Connection {
        private final int id;
        private final SocketChannel channel;
        private final ByteBuffer in = ByteBuffer.allocate(READ_BUFFER_BYTES);

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
