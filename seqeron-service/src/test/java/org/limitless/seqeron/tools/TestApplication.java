package org.limitless.seqeron.tools;

import io.aeron.Aeron;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.limitless.seqeron.app.Application;
import org.limitless.seqeron.app.ClusterError;
import org.limitless.seqeron.app.Payload;
import org.limitless.seqeron.app.SnapshotListener;
import org.limitless.seqeron.sbe.probe.ProbeMarkerDecoder;
import org.limitless.seqeron.util.IdleStrategies;
import org.limitless.seqeron.util.Logger;

/**
 * One replica of a co-located application taking part in snapshot rounds: the reference consumer of {@link
 * Application} with a {@link SnapshotListener}, as {@link TestGateway} is of {@code Gateway}. Harness code in the
 * test source set, launched by {@code snapshot-test.sh} off the compiled test classes.
 *
 * <p>While its gate is open it submits a {@code ProbeMarker} every {@code probe.intervalMs}; a declined one is not
 * retried. Its state is the count of markers sequenced under its {@code sourceId} and a digest of their {@code
 * globalSeqNo}s in order, so a replica that restored the wrong round, or applied the tail out of order, serializes
 * differently from the publisher and is fenced on the next round.
 *
 * <p>System properties, on top of {@link ClusterProbe}'s {@code probe.memberId} / {@code probe.aeronDir} /
 * {@code probe.ingressEndpoints}:
 * <pre>
 *   probe.clientId     — this replica's Replayer client id; default 11
 *   probe.sourceId     — the application's sourceId; default 16
 *   probe.snapshotDir  — this replica's own snapshot directory; default {tmpdir}/seqeron-snapshots-app-{memberId}
 *   probe.intervalMs   — pause between markers while leading; default 100
 *   probe.ipcConnectTimeoutMs — how long a session waits on its member's IPC ingress; the façade's default
 * </pre>
 */
public final class TestApplication implements Application.Listener, SnapshotListener {
    private enum Component implements Logger.Component {
        TestApplication
    }

    /** Exit status of a fenced replica, and of one whose media driver went away. Mirrors TestGateway's. */
    private static final int EXIT_FENCED = 70;

    /** A gap between cluster heartbeats this long is logged when it ends; they arrive once a second. */
    private static final long HEARTBEAT_SILENCE_NS = TimeUnit.SECONDS.toNanos(3);

    private final long intervalNs = TimeUnit.MILLISECONDS.toNanos(Long.getLong("probe.intervalMs", 100));
    private final int sourceId = Integer.getInteger("probe.sourceId", 16);

    private long sequencedMarkers;
    private long digest;
    private long lastHeartbeatNs;

    private final ClusterProbe.MarkerEncoder marker = new ClusterProbe.MarkerEncoder();
    private Application app;
    private long markerSeqNo;
    private volatile boolean fenced;

    public static void main(final String[] args) {
        System.exit(new TestApplication().run());
    }

    private int run() {
        final int memberId = ClusterProbe.memberId();
        final Path snapshotDir = Path.of(System.getProperty(
            "probe.snapshotDir",
            Path.of(System.getProperty("java.io.tmpdir"), "seqeron-snapshots-app-" + memberId).toString()));
        final AtomicBoolean running = new AtomicBoolean(true);
        final Thread main = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            running.set(false);
            try {
                main.join(TimeUnit.SECONDS.toMillis(5));
            } catch (final InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        }));

        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(ClusterProbe.aeronDir()));
             Application application = Application.builder()
                 .sourceId(sourceId)
                 .clientId(Integer.getInteger("probe.clientId", 11))
                 .memberId(memberId)
                 .ipcConnectTimeoutMs(Long.getLong("probe.ipcConnectTimeoutMs",
                                                   Application.DEFAULT_IPC_CONNECT_TIMEOUT_MS))
                 .egressChannel(ClusterProbe.egressChannel())
                 .ingressEndpoints(ClusterProbe.ingressEndpoints())
                 .listener(this)
                 .snapshotListener(this)
                 .snapshotDirectory(snapshotDir)
                 .build()) {
            app = application;
            app.start(aeron);
            log("following the tap as sourceId %d, snapshots in %s", sourceId, snapshotDir);
            final IdleStrategy idle = IdleStrategies.fromProperty(ClusterProbe.IDLE_STRATEGY_PROPERTY).get();
            long nextMarkerNs = 0;
            while (running.get() && !fenced) {
                int work = app.doWork();
                final long now = System.nanoTime();
                if (app.canPublish() && now >= nextMarkerNs) {
                    final int length = marker.encodePayload(++markerSeqNo, ClusterProbe.NO_FILLER);
                    app.publish(ClusterProbe.PROBE_PAYLOAD_ID, marker.payload(), length);
                    nextMarkerNs = now + intervalNs;
                    work++;
                }
                idle.idle(work);
            }
        } catch (final RuntimeException ex) {
            // The media driver going away; every fence the client tier raises arrives at onFenced instead.
            onFenced(null, ex.getMessage());
        }
        return fenced ? EXIT_FENCED : 0;
    }

    @Override
    public void onLeadershipChanged(final boolean leading) {
        log(leading ? "now leading — submitting markers" : "not leading — silent");
    }

    @Override
    public void onSequenced(final Payload payload) {
        if (payload.payloadId() != ClusterProbe.PROBE_PAYLOAD_ID ||
            payload.templateId() != ProbeMarkerDecoder.TEMPLATE_ID || payload.sourceId() != sourceId) {
            return;
        }
        sequencedMarkers++;
        digest = digest * 31 + payload.globalSeqNo();
    }

    /** The one line the harness waits on before it may drive this replica. */
    @Override
    public void onCaughtUp(final long globalSeqNo) {
        log("Caught up — following live at globalSeqNo %d, %d marker(s) sequenced, digest %016x", globalSeqNo,
            sequencedMarkers, digest);
    }

    /** Timed on dispatch, so the line marks when this duty cycle next ran, not when the frame arrived. */
    @Override
    public void onClusterHeartbeat(final long clusterTimeNs, final long receiveTimeNs) {
        final long now = System.nanoTime();
        if (lastHeartbeatNs != 0 && now - lastHeartbeatNs >= HEARTBEAT_SILENCE_NS) {
            log("cluster heartbeat resumed after %dms of silence",
                TimeUnit.NANOSECONDS.toMillis(now - lastHeartbeatNs));
        }
        lastHeartbeatNs = now;
    }

    @Override
    public void onFenced(final ClusterError fence, final String detail) {
        Logger.error(Component.TestApplication, Logger.CoreEventCode.ClusterSessionError, ClusterProbe.memberId(),
                     "FENCED: %s — %s", fence, detail);
        fenced = true;
    }

    @Override
    public int formatVersion() {
        return 1;
    }

    /** One record: the marker count, then the digest. */
    @Override
    public int onSnapshot(final MutableDirectBuffer buffer, final int recordIndex) {
        if (recordIndex > 0) {
            return 0;
        }
        buffer.putLong(0, sequencedMarkers);
        buffer.putLong(Long.BYTES, digest);
        return 2 * Long.BYTES;
    }

    @Override
    public void onRestore(final DirectBuffer buffer, final int length, final int recordIndex) {
        sequencedMarkers = buffer.getLong(0);
        digest = buffer.getLong(Long.BYTES);
    }

    private static void log(final String format, final Object... args) {
        Logger.info(Component.TestApplication, ClusterProbe.memberId(), format, args);
    }
}
