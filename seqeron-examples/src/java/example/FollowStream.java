package example;

import io.aeron.Aeron;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.limitless.seqeron.protocol.PortLayout;
import org.limitless.seqeron.protocol.SystemFrame;
import org.limitless.seqeron.replayer.client.ReplayerStreamReceiver;
import org.limitless.seqeron.replayer.client.SequencedEvent;
import org.limitless.seqeron.sbe.frame.ClusterHeartbeatDecoder;
import org.limitless.seqeron.sbe.frame.ConnectionClosedEncoder;
import org.limitless.seqeron.sbe.frame.ConnectionOpenedDecoder;
import org.limitless.seqeron.sbe.frame.ConnectionOpenedEncoder;
import org.limitless.seqeron.sbe.frame.GatewayActiveDecoder;
import org.limitless.seqeron.sequencer.client.ClusterStreamSender;
import org.limitless.seqeron.sequencer.client.IngressPublisher;

/**
 * Follows one node's ordered stream end to end: history replayed through that node's co-located
 * ReplayerService, then the live tap, with the switch between them handled by the receiver. Once caught
 * up it also produces — one ping a second at cluster ingress, whose echo comes back through
 * {@link #onSequenced} with everything else.
 *
 * <p>Both families are exercised in both directions: the ping is an application payload, and the
 * connection this example announces is a system event, submitted with {@code publishSystem} and decoded
 * off the tap in {@link #printSystem}.
 *
 * <p>Start a node first ({@code seqeron-service/src/main/scripts/start-cluster.sh} in the seqeron repo), then
 * {@code ./gradlew -p seqeron-examples run}. Properties: {@code -Dfollow.member} (default 0),
 * {@code -Dfollow.clientId} (default 7), {@code -Dfollow.aeronDir}.
 */
public final class FollowStream {
    /** The examples' own payloadId and sourceId. */
    private static final int PING_PAYLOAD_ID = 6;
    private static final int PING_SOURCE_ID = 10;

    /** The one connection this example models: announced at start-up, and what every ping rides. */
    private static final int CONNECTION_ID = 1;

    /** Whatever identity a producer's connections have; opaque to the cluster tier, and MAY be empty. */
    private static final byte[] CONNECTION_LABEL = "follow-example".getBytes(StandardCharsets.US_ASCII);

    private static final long PING_INTERVAL_NS = TimeUnit.SECONDS.toNanos(1);

    /** Ingress is tried on this member's own aeron:ipc first; a follower answers there on neither. */
    private static final long IPC_CONNECT_TIMEOUT_MS = 500;

    /** Ephemeral: one session, and no port of its own to allocate. */
    private static final String EGRESS_CHANNEL = "aeron:udp?endpoint=localhost:0";

    private static final IngressPublisher PUBLISHER = new IngressPublisher();
    private static final MutableDirectBuffer PING_BODY = new UnsafeBuffer(new byte[Long.BYTES]);
    private static final MutableDirectBuffer SYSTEM_BODY = new UnsafeBuffer(new byte[64]);
    private static final ConnectionOpenedEncoder CONNECTION_OPENED = new ConnectionOpenedEncoder();
    private static final ConnectionClosedEncoder CONNECTION_CLOSED = new ConnectionClosedEncoder();
    private static final ConnectionOpenedDecoder OPENED_DECODER = new ConnectionOpenedDecoder();
    private static final ClusterHeartbeatDecoder HEARTBEAT_DECODER = new ClusterHeartbeatDecoder();
    private static final GatewayActiveDecoder GATEWAY_ACTIVE_DECODER = new GatewayActiveDecoder();

    private static long lastGlobalSeqNo;
    private static String fault;
    private static long pingSentNs;

    public static void main(final String[] args) {
        final int memberId = Integer.getInteger("follow.member", 0);
        final int clientId = Integer.getInteger("follow.clientId", 7);
        final String aeronDir = System.getProperty(
            "follow.aeronDir", System.getProperty("java.io.tmpdir") + "/seqeron-seq-aeron-" + memberId);

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

        // connectColocated is what a co-located producer wants: ingress over its own member's aeron:ipc —
        // no endpoints, no ports — falling back to the UDP endpoint set when that member is not the leader,
        // which is the only member that subscribes to IPC ingress.
        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDir));
             ClusterStreamSender sender = new ClusterStreamSender();
             ReplayerStreamReceiver receiver = new ReplayerStreamReceiver(
                 clientId, FollowStream::onSequenced, FollowStream::onLeadershipChanged,
                 () -> System.out.println("# caught up — following the tap live"))) {

            sender.connectColocated(aeron, memberId, IPC_CONNECT_TIMEOUT_MS, EGRESS_CHANNEL,
                                    PortLayout.ingressEndpoints());
            receiver.start(aeron, memberId);
            System.out.printf("# following member %d via %s%n", memberId, aeronDir);

            // The one duty cycle. Every receiver and sender method belongs to this thread.
            final IdleStrategy idle = new BackoffIdleStrategy();
            boolean announced = false;
            long nextPingNs = 0;
            while (running.get() && fault == null) {
                final int work = receiver.poll() + sender.pollEgress();
                // Self-throttling: the sender decides when a keep-alive is due, so this just says when it
                // had the chance to send one.
                sender.keepAlive();
                if (!announced) {
                    announced = announceConnection(sender);
                }
                // Only once caught up: a ping submitted during the replay walk would be echoed behind the
                // history still being read, and the round trip would measure the walk rather than the path.
                final long now = System.nanoTime();
                if (announced && receiver.isCaughtUp() && now >= nextPingNs) {
                    ping(sender);
                    nextPingNs = now + PING_INTERVAL_NS;
                }
                idle.idle(work);
            }
            if (announced) {
                closeConnection(sender);
            }
        }
        // System.out is buffered when it is not a console, and nothing flushes it on exit.
        System.out.flush();
        if (fault != null) {
            System.err.println("# " + fault);
            System.exit(1);
        }
    }

    /**
     * This example's one connection, as a {@code ConnectionOpened} system event: a system body goes through
     * {@code publishSystem}, and carries no {@code MessageHeader} because {@code systemEventType} names it.
     *
     * @return whether it was placed; a {@code Declined} is retried on the next duty cycle
     */
    private static boolean announceConnection(final ClusterStreamSender sender) {
        CONNECTION_OPENED.wrap(SYSTEM_BODY, 0).putConnectionData(CONNECTION_LABEL, 0, CONNECTION_LABEL.length);
        return PUBLISHER.publishSystem(sender, PING_SOURCE_ID, CONNECTION_ID, SystemFrame.CONNECTION_OPENED,
                                       SYSTEM_BODY, CONNECTION_OPENED.encodedLength())
            == IngressPublisher.Publish.Published;
    }

    /**
     * The matching {@code ConnectionClosed}, which has no fields: {@code header.connectionId} names a connection
     * every consumer already saw open. Best effort — with no session left to take it, the connection stays open
     * in the sequencer's set, exactly as it would had this process crashed.
     */
    private static void closeConnection(final ClusterStreamSender sender) {
        CONNECTION_CLOSED.wrap(SYSTEM_BODY, 0);
        PUBLISHER.publishSystem(sender, PING_SOURCE_ID, CONNECTION_ID, SystemFrame.CONNECTION_CLOSED, SYSTEM_BODY,
                                CONNECTION_CLOSED.encodedLength());
    }

    /**
     * One ping at cluster ingress: the examples' own {@code payloadId}, and a body of eight raw bytes
     * holding the {@code nanoTime} it left on. Not SBE, and it need not be — the cluster tier decodes no
     * {@code payloadId} at all, so a payload is copied through unopened whatever it holds.
     *
     * <p>Nothing waits here for the echo: the consumer this process already is picks it up off the tap
     * like every other frame.
     */
    private static void ping(final ClusterStreamSender sender) {
        pingSentNs = System.nanoTime();
        PING_BODY.putLong(0, pingSentNs, ByteOrder.LITTLE_ENDIAN);
        if (PUBLISHER.publishPayload(sender, PING_SOURCE_ID, CONNECTION_ID, PING_PAYLOAD_ID, PING_BODY, Long.BYTES)
            != IngressPublisher.Publish.Published) {
            // Declined: the sender spun through back-pressure and an election and found no session at the
            // end of it. Next second's ping is the retry.
            pingSentNs = 0;
        }
    }

    /**
     * Every other frame arrives here in globalSeqNo order, history and live alike — the receiver requests
     * a replay for anything the live tap dropped and dispatches nothing out of order in the meantime.
     */
    private static void onSequenced(final SequencedEvent event) {
        if (!inOrder(event.globalSeqNo())) {
            return;
        }
        if (event.isSystem()) {
            printSystem(event);
        } else if (isOwnPing(event)) {
            System.out.printf("%d ping echoed, round trip %dus%n",
                              event.globalSeqNo(), (System.nanoTime() - pingSentNs) / 1_000L);
        } else {
            System.out.printf("%d payloadId=%d template=%d length=%d%n",
                              event.globalSeqNo(), event.payloadId(), event.templateId(), event.payloadLength());
        }
    }

    /**
     * A system body decoded, one case per wrap rule. The body carries no {@code MessageHeader}, so the decoder
     * supplies what one would have said: the nine submitted events take their decoder's own
     * {@code BLOCK_LENGTH} and {@code SCHEMA_VERSION}, since {@link SequencedEvent#blockLength()} is 0 for
     * them, and the three synthesized ones take the event's own.
     *
     * <p>Every allocated event arrives whether a consumer handles it or not, so the default arm is where a
     * consumer of one protocol spends its time.
     */
    private static void printSystem(final SequencedEvent event) {
        switch (event.systemEventType()) {
            case SystemFrame.CONNECTION_OPENED -> {
                OPENED_DECODER.wrap(event.buffer(), event.payloadOffset(), ConnectionOpenedDecoder.BLOCK_LENGTH,
                                    ConnectionOpenedDecoder.SCHEMA_VERSION);
                System.out.printf("%d ConnectionOpened connection=%d label=%d bytes%n", event.globalSeqNo(),
                                  event.connectionId(), OPENED_DECODER.connectionDataLength());
            }
            case SystemFrame.CLUSTER_HEARTBEAT -> {
                HEARTBEAT_DECODER.wrap(event.buffer(), event.payloadOffset(), event.blockLength(), event.version());
                System.out.printf("%d ClusterHeartbeat cluster clock %dns%n", event.globalSeqNo(),
                                  HEARTBEAT_DECODER.header().timestamp());
            }
            case SystemFrame.GATEWAY_ACTIVE -> {
                // Only after clusterctl load-topology: this frame is the cluster designating one gateway
                // instance, and its gatewayId is in the body and nowhere else.
                GATEWAY_ACTIVE_DECODER.wrap(event.buffer(), event.payloadOffset(), event.blockLength(), event.version());
                System.out.printf("%d GatewayActive gatewayId=%d%n", event.globalSeqNo(),
                                  GATEWAY_ACTIVE_DECODER.gatewayId());
            }
            default -> System.out.printf("%d system eventType=%d%n", event.globalSeqNo(), event.systemEventType());
        }
    }

    /** This process's own ping, told from any other producer's by the timestamp it carries. */
    private static boolean isOwnPing(final SequencedEvent event) {
        return pingSentNs != 0 && event.payloadId() == PING_PAYLOAD_ID && event.payloadLength() == Long.BYTES
            && event.buffer().getLong(event.payloadOffset(), ByteOrder.LITTLE_ENDIAN) == pingSentNs;
    }

    /** The one frame family that reaches a consumer here instead of through onSequenced. */
    private static void onLeadershipChanged(final int newLeaderMemberId, final long leadershipTermId,
                                            final long globalSeqNo) {
        if (!inOrder(globalSeqNo)) {
            return;
        }
        System.out.printf("%d leader=member %d term %d%n", globalSeqNo, newLeaderMemberId, leadershipTermId);
    }

    /**
     * The invariant the whole tier exists for: one frame per globalSeqNo, no holes, replay and live alike.
     * Recorded rather than thrown — this runs inside a fragment handler, and Image.poll advances the
     * subscriber position regardless of what a handler raises, so the duty cycle above raises it instead.
     */
    private static boolean inOrder(final long globalSeqNo) {
        if (fault != null) {
            return false;
        }
        if (globalSeqNo != lastGlobalSeqNo + 1) {
            fault = "gap: " + lastGlobalSeqNo + " -> " + globalSeqNo;
            return false;
        }
        lastGlobalSeqNo = globalSeqNo;
        return true;
    }
}
