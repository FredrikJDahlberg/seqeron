package example;

import io.aeron.Aeron;
import io.aeron.cluster.client.AeronCluster;
import java.nio.ByteOrder;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.limitless.seqeron.replayer.client.ReplayerStreamReceiver;
import org.limitless.seqeron.replayer.client.SequencedEvent;
import org.limitless.seqeron.sequencer.SystemFrame;

/**
 * Follows one node's ordered stream end to end: history replayed through that node's co-located
 * ReplayerService, then the live tap, with the switch between them handled by the receiver. Once caught
 * up it also produces — one ping a second at cluster ingress, whose echo comes back through
 * {@link #onSequenced} with everything else.
 *
 * <p>Start a node first ({@code src/main/scripts/start-cluster.sh} in the seqeron repo), then
 * {@code ./gradlew run}. Properties: {@code -Dfollow.member} (default 0), {@code -Dfollow.clientId}
 * (default 7), {@code -Dfollow.aeronDir}.
 */
public final class FollowStream {
    /** The examples' own payloadId and sourceId, allocated in the spec's §6.1 and §5 tables. */
    private static final int PING_PAYLOAD_ID = 6;
    private static final int PING_SOURCE_ID = 10;

    /** The ping belongs to no connection and no session of its own; the sequencer overwrites the latter. */
    private static final int NO_ID = -1;

    private static final long PING_INTERVAL_NS = TimeUnit.SECONDS.toNanos(1);

    /** Well inside the cluster's 1s sessionTimeoutMs, which one ping a second does not meet on its own. */
    private static final long KEEP_ALIVE_INTERVAL_NS = TimeUnit.MILLISECONDS.toNanos(200);

    private static final ExpandableArrayBuffer PING_FRAME = new ExpandableArrayBuffer();
    private static final MutableDirectBuffer PING_BODY = new UnsafeBuffer(new byte[Long.BYTES]);

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

        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDir));
             AeronCluster cluster = AeronCluster.connect(new AeronCluster.Context()
                 .aeron(aeron)
                 .ingressChannel("aeron:ipc")
                 .egressChannel("aeron:udp?endpoint=localhost:0"));
             ReplayerStreamReceiver receiver = new ReplayerStreamReceiver(
                 clientId, FollowStream::onSequenced, FollowStream::onLeadershipChanged,
                 () -> System.out.println("# caught up — following the tap live"))) {

            receiver.start(aeron, memberId);
            System.out.printf("# following member %d via %s%n", memberId, aeronDir);

            // The one duty cycle. Every receiver and cluster method belongs to this thread.
            final IdleStrategy idle = new BackoffIdleStrategy();
            long nextKeepAliveNs = 0;
            long nextPingNs = 0;
            while (running.get() && fault == null) {
                final int work = receiver.poll() + cluster.pollEgress();
                final long now = System.nanoTime();
                if (now >= nextKeepAliveNs) {
                    cluster.sendKeepAlive();
                    nextKeepAliveNs = now + KEEP_ALIVE_INTERVAL_NS;
                }
                // Only once caught up: a ping submitted during the replay walk would be echoed behind the
                // history still being read, and the round trip would measure the walk rather than the path.
                if (receiver.isCaughtUp() && now >= nextPingNs) {
                    ping(cluster);
                    nextPingNs = now + PING_INTERVAL_NS;
                }
                idle.idle(work);
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
     * One ping at cluster ingress: the examples' own {@code payloadId}, and a body of eight raw bytes
     * holding the {@code nanoTime} it left on. Not SBE, and it need not be — the cluster tier decodes no
     * {@code payloadId} at all, so a payload is copied through unopened whatever it holds.
     *
     * <p>Nothing waits here for the echo: the consumer this process already is picks it up off the tap
     * like every other frame.
     */
    private static void ping(final AeronCluster cluster) {
        pingSentNs = System.nanoTime();
        PING_BODY.putLong(0, pingSentNs, ByteOrder.LITTLE_ENDIAN);
        final int length = SystemFrame.wrapPayload(PING_FRAME, PING_SOURCE_ID, NO_ID, NO_ID, PING_PAYLOAD_ID,
                                                   PING_BODY, Long.BYTES);
        if (cluster.offer(PING_FRAME, 0, length) < 0) {
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
            System.out.printf("%d system eventType=%d%n", event.globalSeqNo(), event.systemEventType());
        } else if (isOwnPing(event)) {
            System.out.printf("%d ping echoed, round trip %dus%n",
                              event.globalSeqNo(), (System.nanoTime() - pingSentNs) / 1_000L);
        } else {
            System.out.printf("%d payloadId=%d template=%d length=%d%n",
                              event.globalSeqNo(), event.payloadId(), event.templateId(), event.length());
        }
    }

    /** This process's own ping, told from any other producer's by the timestamp it carries. */
    private static boolean isOwnPing(final SequencedEvent event) {
        return pingSentNs != 0 && event.payloadId() == PING_PAYLOAD_ID && event.length() == Long.BYTES
            && event.buffer().getLong(event.offset(), ByteOrder.LITTLE_ENDIAN) == pingSentNs;
    }

    /** The one frame family that reaches a consumer here instead of through onSequenced. */
    private static void onLeadershipChanged(final int newLeaderMemberId, final long globalSeqNo) {
        if (!inOrder(globalSeqNo)) {
            return;
        }
        System.out.printf("%d leader=member %d%n", globalSeqNo, newLeaderMemberId);
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
