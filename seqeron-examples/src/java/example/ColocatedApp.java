package example;

import io.aeron.Aeron;
import java.nio.ByteOrder;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.limitless.seqeron.app.ClusterError;
import org.limitless.seqeron.app.ColocatedApplication;
import org.limitless.seqeron.app.Payload;
import org.limitless.seqeron.protocol.Publish;

/**
 * The same flow as {@link FollowStream}, written against the front door instead of the tiers under it.
 * Look at the imports: {@code app}, {@code protocol.Publish}, Aeron and Agrona, and nothing else. There is
 * no receiver here, no sender, no envelope and no {@code systemEventType} — the façade assembles the
 * cluster session, the tap, confirmed ingress across a failover, the fences and the leader gate, and hands
 * this class {@link Payload}s.
 *
 * <p>A co-located application is the producer kind nothing elects: one replica per node, publishing only
 * while its own node leads. That is why it needs no topology document to run — {@code LeadershipChanged}
 * already picks the replica that submits, so {@link #onLeadershipChanged} is the whole election.
 *
 * <p>Start a node first ({@code seqeron-service/src/main/scripts/start-cluster.sh} in the seqeron repo),
 * then {@code ./gradlew -p seqeron-examples runColocated}. Properties: {@code -Dcolocated.member}
 * (default 0), {@code -Dcolocated.clientId} (default 13), {@code -Dcolocated.aeronDir}.
 */
public final class ColocatedApp implements ColocatedApplication.Listener {
    /** The examples' `payloadId` — eight raw bytes, no schema, which spec §13.2 admits. */
    private static final int PING_PAYLOAD_ID = 6;

    /** This example's own `sourceId` (spec §5); the low-level example is 10. */
    private static final int SOURCE_ID = 11;

    private static final long PING_INTERVAL_NS = TimeUnit.SECONDS.toNanos(1);

    /** Ephemeral: one session, and no port of its own to allocate. */
    private static final String EGRESS_CHANNEL = "aeron:udp?endpoint=localhost:0";

    private final MutableDirectBuffer pingBody = new UnsafeBuffer(new byte[Long.BYTES]);

    private ColocatedApplication app;
    private long pingSentNs;
    private String fence;

    public static void main(final String[] args) {
        final int memberId = Integer.getInteger("colocated.member", 0);
        final int clientId = Integer.getInteger("colocated.clientId", 13);
        final String aeronDir = System.getProperty(
            "colocated.aeronDir", System.getProperty("java.io.tmpdir") + "/seqeron-seq-aeron-" + memberId);

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

        final ColocatedApp application = new ColocatedApp();
        // ingressEndpoints is not passed: it defaults to the cluster's own port block, which is what a
        // co-located replica falls back to on the duty cycles where its node is not the one leading.
        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDir));
             ColocatedApplication app = ColocatedApplication.builder()
                 .sourceId(SOURCE_ID).clientId(clientId).memberId(memberId)
                 .egressChannel(EGRESS_CHANNEL).listener(application)
                 .build()) {

            application.app = app;
            app.start(aeron);
            System.out.printf("# member %d via %s — replica of application sourceId %d%n",
                              memberId, aeronDir, SOURCE_ID);
            application.run(running);
        }
        // System.out is buffered when it is not a console, and nothing flushes it on exit.
        System.out.flush();
        if (application.fence != null) {
            System.err.println("# " + application.fence);
            System.exit(1);
        }
    }

    /** The one duty cycle. Every method on the façade belongs to this thread, callbacks included. */
    private void run(final AtomicBoolean running) {
        final IdleStrategy idle = new BackoffIdleStrategy();
        long nextPingNs = 0;
        while (running.get() && fence == null) {
            final int work = app.doWork();
            final long now = System.nanoTime();
            // canPublish() is the gate and the failover hold in one: shut while this node does not lead,
            // and shut while PendingSends is still resending what the last leader change lost.
            if (app.canPublish() && now >= nextPingNs) {
                ping();
                nextPingNs = now + PING_INTERVAL_NS;
            }
            idle.idle(work);
        }
    }

    /**
     * One payload at cluster ingress, on this application's own `sourceId` and no connection. Nothing waits
     * here for the echo: this process is its own consumer and picks it up off the tap like any other frame.
     */
    private void ping() {
        pingSentNs = System.nanoTime();
        pingBody.putLong(0, pingSentNs, ByteOrder.LITTLE_ENDIAN);
        if (app.publish(PING_PAYLOAD_ID, pingBody, Long.BYTES) != Publish.Published) {
            // Declined: the gate shut, ingress is held behind a resend, or the transport is back-pressured.
            // Next second's ping is the retry.
            pingSentNs = 0;
        }
    }

    /**
     * Every application payload on this node's tap, in {@code globalSeqNo} order, history and live alike —
     * every replica sees the same ones and so holds the same state. System frames never arrive here; the
     * façade acts on them and reports only what an application has a decision to make about.
     */
    @Override
    public void onSequenced(final Payload payload) {
        if (isOwnPing(payload)) {
            System.out.printf("%d ping echoed, round trip %dus%n",
                              payload.globalSeqNo(), (System.nanoTime() - pingSentNs) / 1_000L);
        } else {
            System.out.printf("%d sourceId=%d payloadId=%d length=%d%n", payload.globalSeqNo(),
                              payload.sourceId(), payload.payloadId(), payload.payloadLength());
        }
    }

    /** This process's own ping, told from the other example's by the timestamp it carries. */
    private boolean isOwnPing(final Payload payload) {
        return pingSentNs != 0 && payload.sourceId() == SOURCE_ID && payload.payloadId() == PING_PAYLOAD_ID
            && payload.payloadLength() == Long.BYTES
            && payload.buffer().getLong(payload.payloadOffset(), ByteOrder.LITTLE_ENDIAN) == pingSentNs;
    }

    /**
     * The whole election, for the producer kind nothing elects. False is also where a replica keeping
     * outstanding work calls {@code OutstandingWork.onNotLeader()} — every leadership change shuts an open
     * gate, and a payload submitted during the election may have gone with it.
     */
    @Override
    public void onLeadershipChanged(final boolean leading) {
        System.out.println(leading ? "# leading — publishing" : "# not leading — silent");
    }

    @Override
    public void onCaughtUp(final long globalSeqNo) {
        System.out.printf("# caught up at %d — following the tap live%n", globalSeqNo);
    }

    /**
     * The cluster clock, once a second: the one time source that keeps advancing while every producer is
     * silent, and identical on every node. A deadline belongs on this rather than on a local clock.
     */
    @Override
    public void onClusterHeartbeat(final long clusterTimeNs, final long receiveTimeNs) {
        // Nothing to time here; a producer with a watchdog runs it off this.
    }

    /**
     * Latched, once: this replica may no longer act, and exiting lets its restart re-walk the log.
     * Recorded rather than thrown — this runs inside the façade's duty cycle, which raises nothing of a
     * consumer's on its behalf.
     */
    @Override
    public void onFenced(final ClusterError fence, final String detail) {
        this.fence = fence + ": " + detail;
    }
}
