package example;

import io.aeron.Aeron;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.limitless.seqeron.replayer.client.ReplayerStreamReceiver;
import org.limitless.seqeron.replayer.client.SequencedEvent;

/**
 * Follows one node's ordered stream end to end: history replayed through that node's co-located
 * ReplayerService, then the live tap, with the switch between them handled by the receiver.
 *
 * <p>Start a node first ({@code src/main/scripts/start-cluster.sh} in the seqeron repo), then
 * {@code ./gradlew run}. Properties: {@code -Dfollow.member} (default 0), {@code -Dfollow.clientId}
 * (default 7), {@code -Dfollow.aeronDir}.
 */
public final class FollowStream {
    private static long lastGlobalSeqNo;
    private static String fault;

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

        // Both handlers, not just the sequenced one: a LeadershipChanged frame is delivered on its own
        // callback and on no other, so a consumer that passes null there sees a hole in globalSeqNo
        // wherever the cluster changed leader — including at globalSeqNo 1, which always is one.
        //
        // clientId must be unique among the Replayer's co-located apps: two sharing one supersede each
        // other's replays and neither ever catches up.
        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDir));
             ReplayerStreamReceiver receiver = new ReplayerStreamReceiver(
                 clientId, FollowStream::onSequenced, FollowStream::onLeadershipChanged,
                 () -> System.out.println("# caught up — following the tap live"))) {

            receiver.start(aeron, memberId);
            System.out.printf("# following member %d via %s%n", memberId, aeronDir);

            // The one duty cycle. Every receiver method belongs to this thread.
            final IdleStrategy idle = new BackoffIdleStrategy();
            while (running.get() && fault == null) {
                idle.idle(receiver.poll());
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
     * Every other frame arrives here in globalSeqNo order, history and live alike — the receiver requests
     * a replay for anything the live tap dropped and dispatches nothing out of order in the meantime.
     */
    private static void onSequenced(final SequencedEvent event) {
        if (!inOrder(event.globalSeqNo())) {
            return;
        }
        // Split by family first. A system event is seqeron's own vocabulary, named by systemEventType (the
        // table is in SystemFrame); an application payload stays opaque here, as it is to the cluster tier
        // itself. Never dispatch on a bare template id — it is unique only within one schema.
        if (event.isSystem()) {
            System.out.printf("%d system eventType=%d%n", event.globalSeqNo(), event.systemEventType());
        } else {
            System.out.printf("%d payloadId=%d template=%d length=%d%n",
                              event.globalSeqNo(), event.payloadId(), event.templateId(), event.length());
        }
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
