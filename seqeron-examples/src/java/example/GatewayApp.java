package example;

import io.aeron.Aeron;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.limitless.seqeron.app.ClusterError;
import org.limitless.seqeron.app.Gateway;
import org.limitless.seqeron.app.Payload;
import org.limitless.seqeron.protocol.Publish;

/**
 * One instance of an elected active/standby pair, written against {@link Gateway} alone — the gateway twin of
 * {@link ColocatedApp}, and the Java twin of {@code GatewayApp.cpp}, under the same import check.
 *
 * <p>A gateway is the producer kind the cluster elects: the topology names both instances, the cluster
 * designates one, and only that one serves. What is left here is the edge. A real gateway opens a socket in
 * {@link #onActivated}; this one takes a single simulated client connection instead, and pings the cluster on
 * it once a second, reading each ping back off its own tap.
 *
 * <p>It runs the C++ example's pair, so either instance may be either language. Load
 * {@code seqeron-examples/topology.xml}, then run one instance or both; stop the first and the second takes
 * over. Properties: {@code -Dgateway.name} (default {@code GW-EX-A}), {@code -Dgateway.member} (default 0),
 * {@code -Dgateway.clientId} (default 15 for A, 16 for B), {@code -Dgateway.aeronDir}.
 */
public final class GatewayApp implements Gateway.Listener {
    /** The examples' `payloadId` — eight raw bytes, no schema, which spec §13.2 admits. */
    private static final int PING_PAYLOAD_ID = 6;

    private static final long PING_INTERVAL_NS = TimeUnit.SECONDS.toNanos(1);

    /** Ephemeral: one session, and no port of its own to allocate. */
    private static final String EGRESS_CHANNEL = "aeron:udp?endpoint=localhost:0";

    private static final DirectBuffer CLIENT_LABEL =
        new UnsafeBuffer("example-client".getBytes(StandardCharsets.US_ASCII));

    private final MutableDirectBuffer pingBody = new UnsafeBuffer(new byte[Long.BYTES]);

    private Gateway gateway;
    private boolean open;
    private int connectionId = Gateway.NO_CONNECTION;
    private long pingSentNs;
    private String fence;

    public static void main(final String[] args) {
        final String gatewayName = System.getProperty("gateway.name", "GW-EX-A");
        final int instance = gatewayName.equals("GW-EX-B") ? 1 : 0;
        final int memberId = Integer.getInteger("gateway.member", 0);
        final int clientId = Integer.getInteger("gateway.clientId", 15 + instance);
        final String aeronDir = System.getProperty(
            "gateway.aeronDir", System.getProperty("java.io.tmpdir") + "/seqeron-seq-aeron-" + memberId);

        final AtomicBoolean running = new AtomicBoolean(true);
        final Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            running.set(false);
            try {
                mainThread.join(TimeUnit.SECONDS.toMillis(5));
            } catch (final InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        }));

        final GatewayApp edge = new GatewayApp();
        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDir));
             Gateway gateway = Gateway.builder()
                                   .gatewayName(gatewayName)
                                   .clientId(clientId)
                                   .memberId(memberId)
                                   .egressChannel(EGRESS_CHANNEL)
                                   .listener(edge)
                                   .build()) {
            edge.gateway = gateway;
            gateway.start(aeron);
            System.out.printf("# %s on member %d via %s%n", gatewayName, memberId, aeronDir);
            edge.run(running);
        }
        // System.out is buffered when it is not a console, and nothing flushes it on exit.
        System.out.flush();
        if (edge.fence != null) {
            System.err.println("# " + edge.fence);
            System.exit(1);
        }
    }

    /** The one duty cycle. Every method on the façade belongs to this thread, callbacks included. */
    private void run(final AtomicBoolean running) {
        final IdleStrategy idle = new BackoffIdleStrategy();
        long nextPingNs = 0;
        while (running.get() && fence == null) {
            final int work = gateway.doWork();
            // canAccept() is serving and not held behind a failover's resend: the moment a real gateway accepts.
            if (open && connectionId == Gateway.NO_CONNECTION && gateway.canAccept()) {
                connectionId = gateway.openConnection(CLIENT_LABEL, CLIENT_LABEL.capacity());
            }
            final long now = System.nanoTime();
            if (connectionId != Gateway.NO_CONNECTION && now >= nextPingNs) {
                // Declined until the connection's ConnectionOpened has landed, and while ingress is held or
                // back-pressured. Next second's ping is the retry.
                pingSentNs = now;
                pingBody.putLong(0, now, ByteOrder.LITTLE_ENDIAN);
                if (gateway.publish(connectionId, PING_PAYLOAD_ID, pingBody, Long.BYTES) != Publish.Published) {
                    pingSentNs = 0;
                }
                nextPingNs = now + PING_INTERVAL_NS;
            }
            idle.idle(work);
        }
    }

    /** Designated: open the edge. A real gateway binds its listen socket here; false would be retried. */
    @Override
    public boolean onActivated(final int firstConnectionId) {
        System.out.printf("# designated — serving, connection ids from %d%n", firstConnectionId);
        open = true;
        return true;
    }

    /** Stood down or fenced: close the edge and drop every connection it let in. */
    @Override
    public void onStandby() {
        System.out.println("# standing by");
        open = false;
        connectionId = Gateway.NO_CONNECTION;
        pingSentNs = 0;
    }

    /** Every application payload on this node's tap, in {@code globalSeqNo} order, history and live alike. */
    @Override
    public void onSequenced(final Payload payload) {
        if (pingSentNs != 0 && payload.connectionId() == connectionId && payload.payloadId() == PING_PAYLOAD_ID &&
            payload.payloadLength() == Long.BYTES &&
            payload.buffer().getLong(payload.payloadOffset(), ByteOrder.LITTLE_ENDIAN) == pingSentNs) {
            System.out.printf("%d ping echoed on connection %d, round trip %dus%n", payload.globalSeqNo(),
                              payload.connectionId(), (System.nanoTime() - pingSentNs) / 1_000L);
        }
    }

    /**
     * This logical gateway's connections, whichever instance opened them: how a standby that keeps
     * per-connection state rebuilds it while it replays.
     */
    @Override
    public void onConnectionOpened(final int connectionId, final DirectBuffer connectionData, final int offset,
                                   final int length) {
        System.out.printf("# connection %d opened (%s)%n", connectionId,
                          connectionData.getStringWithoutLengthAscii(offset, length));
    }

    @Override
    public void onConnectionClosed(final int connectionId) {
        System.out.printf("# connection %d closed%n", connectionId);
    }

    @Override
    public void onCaughtUp(final long globalSeqNo) {
        System.out.printf("# caught up at %d — following the tap live%n", globalSeqNo);
    }

    @Override
    public void onClusterHeartbeat(final long clusterTimeNs, final long receiveTimeNs) {
        // Nothing to time here; a gateway with a watchdog runs it off this.
    }

    /** Latched, once: this instance may no longer act. Exiting releases its session, so the standby takes over. */
    @Override
    public void onFenced(final ClusterError fence, final String detail) {
        this.fence = fence + ": " + detail;
    }
}
