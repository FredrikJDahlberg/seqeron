package org.limitless.phixeron.tools;

import io.aeron.Aeron;
import io.aeron.FragmentAssembler;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import java.util.Arrays;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.agrona.concurrent.YieldingIdleStrategy;
import org.limitless.phixeron.replayer.client.ReplayerStreamReceiver;
import org.limitless.phixeron.replayer.client.SequencedEvent;
import org.limitless.phixeron.replayer.client.SequencedFrameDecoder;
import org.limitless.phixeron.sbe.probe.MessageHeaderDecoder;
import org.limitless.phixeron.sbe.probe.MessageHeaderEncoder;
import org.limitless.phixeron.sbe.probe.ProbeMarkerDecoder;
import org.limitless.phixeron.sbe.probe.ProbeMarkerEncoder;
import org.limitless.phixeron.sequencer.SequencerService;
import org.limitless.phixeron.sequencer.SystemFrame;
import org.limitless.phixeron.util.Logger;

/**
 * The edge-neutral probe (doc/future-arch.md §11 step 5) — the cluster tier's own load generator and
 * tap consumer, so its end-to-end scripts can drive a cluster with no product binary built.
 *
 * <p>Before this, all five harnesses in {@code cluster/src/test/scripts} generated load with the C++
 * {@code fix_test_server} and consumed the tap with {@code OrderExecServer}. Nothing they assert is
 * about FIX: they grep for a caught-up announcement, the {@link ReplayerStreamReceiver}'s gap and
 * re-walk lines, the Replayer's own segment/recording decisions, and a delivered-frame count. That made
 * this module's e2e suite depend on the C++ edge — a product → cluster edge no build could see, and the
 * thing that would leave seqeron with no runnable e2e at all once it is extracted.
 *
 * <p>Three modes, one message ({@code sbe-probe.xml}, {@code payloadId} 5):
 * <ul>
 *   <li><b>submit</b> — flood {@code probe.count} {@code ProbeMarker}s at cluster ingress. Builds an
 *       archive, or gives a cold-start walk work to do. Opens no tap subscription: a tethered one left
 *       unpolled behind a 400k-frame flood would back-pressure the sequencer's own tap.</li>
 *   <li><b>ping</b> — submit exactly one and wait for its sequenced echo off the co-located tap. The
 *       liveness check: it proves ingress, consensus, and the tap in one round trip, which is what the
 *       FIX round-trip probe it replaces was actually testing.</li>
 *   <li><b>follow</b> — replay history through the co-located Replayer, then follow the tap live,
 *       announcing the transition. The {@code OrderExecServer} replacement.</li>
 * </ul>
 *
 * <p>System properties (mirroring {@code ReplayerServer}'s, which every script already sets this way):
 * <pre>
 *   probe.memberId          — which cluster member this probe co-locates with (0/1/2); default 0
 *   probe.aeronDir          — that member's Aeron directory; default {tmpdir}/phixeron-seq-aeron-{memberId}
 *   probe.ingressEndpoints  — cluster ingress endpoints; default the three-node localhost set
 *   probe.clientId          — follow: this replica's Replayer client id; default 9
 *   probe.count             — submit: frames to send; default 1000
 *   probe.fillerBytes       — submit: bytes of filler per frame; default 0
 *   probe.pacingMicros      — submit: pause between frames; default 0 (as fast as ingress accepts)
 *   probe.latencyStats      — follow: record and report post-catch-up delivery latency; default false
 *   probe.faultInjection    — follow: install the SIGUSR1 tap-drop handler; default false
 *   probe.faultDropCount    — follow: frames dropped per SIGUSR1; default 1
 *   probe.idleStrategy      — follow: backoff (default) or yielding
 * </pre>
 */
public final class ClusterProbe {
    /**
     * The probe protocol's {@code payloadId}. Private in the §6.1 sense — one publisher and one consumer,
     * both this class — so it needs no {@code PayloadIdRegistered} row and no {@code <protocols>} entry.
     * 2, 3 and 4 are the deployment's product protocols.
     */
    public static final int PROBE_PAYLOAD_ID = 5;

    /**
     * The probe's {@code sourceId} in §5's one id space. Claimed by no topology row, like
     * {@code clusterctl}'s 2, so S-6 case 3 leaves its frames unchecked — which is what an application
     * frame gets anyway.
     */
    public static final int PROBE_SOURCE_ID = 8;

    /** No connection and no advisory session: the probe is a producer, not a gateway with sockets. */
    private static final int NO_ID = -1;

    private static final int MEMBER_ID = Integer.getInteger("probe.memberId", 0);

    /** Which cluster member every {@code probe.*} process co-locates with; {@code TestGateway} shares it. */
    static int memberId() {
        return MEMBER_ID;
    }

    private static final String AERON_DIR = System.getProperty(
        "probe.aeronDir", System.getProperty("java.io.tmpdir") + "/phixeron-seq-aeron-" + MEMBER_ID);

    private static final String INGRESS_ENDPOINTS =
        System.getProperty("probe.ingressEndpoints", "0=localhost:9302,1=localhost:9312,2=localhost:9322");

    private static final long CONNECT_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(5);
    private static final long OFFER_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(5);
    private static final long ECHO_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(10);

    /** Exit status of a follower whose duty cycle died — its media driver went away. Mirrors ReplayerServer's. */
    private static final int EXIT_DUTY_CYCLE_FATAL = 70;

    private static final EgressListener NULL_EGRESS = (sessionId, timestamp, buffer, offset, length, header) -> { };

    /** Set from the SIGUSR1 handler, drained on the duty-cycle thread — signals are not queued. */
    private static final AtomicInteger faultDropArmed = new AtomicInteger();

    private ClusterProbe() {
    }

    public static void main(final String[] args) {
        final String mode = args.length > 0 ? args[0] : "help";
        switch (mode) {
            case "submit" -> System.exit(submit(Integer.getInteger("probe.count", 1000)));
            case "ping" -> System.exit(ping());
            case "follow" -> follow();
            default -> {
                usage();
                System.exit(mode.equals("help") ? 0 : 1);
            }
        }
    }

    // ── submit / ping ─────────────────────────────────────────────────────────────

    /**
     * Floods {@code count} {@code ProbeMarker}s at cluster ingress, seqNo 1..count.
     * @param count frames to send
     * @return 0 on success
     */
    private static int submit(final int count) {
        final int fillerBytes = Integer.getInteger("probe.fillerBytes", 0);
        final long pacingMicros = Long.getLong("probe.pacingMicros", 0L);
        try (AeronCluster cluster = connectCluster()) {
            final ExpandableArrayBuffer frame = new ExpandableArrayBuffer();
            final ExpandableArrayBuffer payload = new ExpandableArrayBuffer();
            final byte[] filler = new byte[fillerBytes];
            Arrays.fill(filler, (byte)'x');
            for (long seqNo = 1; seqNo <= count; seqNo++) {
                final int length = encode(frame, payload, seqNo, NO_ID, PROBE_SOURCE_ID, filler);
                offer(cluster, frame, length);
                pause(pacingMicros);
            }
            Logger.info(Logger.Component.ClusterProbe, MEMBER_ID,
                        "submitted %d ProbeMarker(s), filler %d bytes, pacing %dus", count, fillerBytes,
                        pacingMicros);
            return 0;
        } catch (final RuntimeException ex) {
            Logger.error(Logger.Component.ClusterProbe, Logger.EventCode.ClusterSessionError, MEMBER_ID,
                         "submit failed: %s", ex);
            return 1;
        }
    }

    /**
     * Submits one {@code ProbeMarker} and waits for its own sequenced echo off the co-located tap — the
     * whole path (ingress, consensus, tap) in one round trip.
     * @return 0 if the echo arrived, 1 otherwise
     */
    private static int ping() {
        // Distinctive, so a concurrent flood's 1..count cannot be mistaken for this frame's echo.
        final long seqNo = System.currentTimeMillis();
        try (AeronCluster cluster = connectCluster()) {
            final Subscription tap = awaitTap(cluster);
            if (tap == null) {
                return 1;
            }
            final ExpandableArrayBuffer frame = new ExpandableArrayBuffer();
            final ExpandableArrayBuffer payload = new ExpandableArrayBuffer();
            offer(cluster, frame, encode(frame, payload, seqNo, NO_ID, PROBE_SOURCE_ID, new byte[0]));

            final EchoHandler handler = new EchoHandler(seqNo);
            final FragmentAssembler assembler = new FragmentAssembler(handler);
            final IdleStrategy idle = new YieldingIdleStrategy();
            final long deadline = System.nanoTime() + ECHO_TIMEOUT_NS;
            while (!handler.found && System.nanoTime() < deadline) {
                final int fragments = tap.poll(assembler, 10);
                cluster.pollEgress();
                idle.idle(fragments);
            }
            if (!handler.found) {
                Logger.error(Logger.Component.ClusterProbe, Logger.EventCode.ClusterSessionError, MEMBER_ID,
                             "ping seqNo %d was not echoed on the tap within %ds", seqNo,
                             TimeUnit.NANOSECONDS.toSeconds(ECHO_TIMEOUT_NS));
                return 1;
            }
            Logger.info(Logger.Component.ClusterProbe, MEMBER_ID, "ping seqNo %d echoed at globalSeqNo %d",
                        seqNo, handler.globalSeqNo);
            return 0;
        } catch (final RuntimeException ex) {
            Logger.error(Logger.Component.ClusterProbe, Logger.EventCode.ClusterSessionError, MEMBER_ID,
                         "ping failed: %s", ex);
            return 1;
        }
    }

    /**
     * Encodes one {@code ProbeMarker} into {@code payload}, then wraps it as an {@code Unsequenced} frame
     * in {@code frame}. The payload carries its own {@code MessageHeader}, as every payload does.
     * @param connectionId the connection this frame belongs to, or -1 for a producer-scoped one
     * @param sourceId     the producer stamping it: this class's own, or a {@code TestGateway}'s resolved
     *                     {@code gatewaySourceId}
     * @return the frame's length in bytes
     */
    static int encode(final ExpandableArrayBuffer frame, final ExpandableArrayBuffer payload, final long seqNo,
                      final int connectionId, final int sourceId, final byte[] filler) {
        final ProbeMarkerEncoder encoder = new ProbeMarkerEncoder();
        encoder.wrapAndApplyHeader(payload, 0, new MessageHeaderEncoder());
        encoder.seqNo(seqNo);
        encoder.putFiller(filler, 0, filler.length);
        final int payloadLength = MessageHeaderEncoder.ENCODED_LENGTH + encoder.encodedLength();
        final int length = SystemFrame.wrapPayload(frame, sourceId, connectionId, NO_ID, PROBE_PAYLOAD_ID,
                                                   payload, payloadLength);
        if (length == SystemFrame.REFUSED) {
            throw new IllegalArgumentException("probe.fillerBytes makes the payload larger than a frame may carry");
        }
        return length;
    }

    /** Matches this ping's own sequenced echo: our payloadId, our template, our seqNo. */
    private static final class EchoHandler implements FragmentHandler {
        private final long seqNo;
        private final SequencedFrameDecoder view = new SequencedFrameDecoder();
        private final ProbeMarkerDecoder decoder = new ProbeMarkerDecoder();
        private boolean found;
        private long globalSeqNo;

        private EchoHandler(final long seqNo) {
            this.seqNo = seqNo;
        }

        @Override
        public void onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header) {
            if (found || !view.wrap(buffer, offset, length) || view.isSystem()
                || view.payloadId() != PROBE_PAYLOAD_ID || view.templateId() != ProbeMarkerDecoder.TEMPLATE_ID) {
                return;
            }
            // payloadOffset() is the payload's own MessageHeader; the decoder sits past it, and
            // blockLength/version come off that header via the view.
            decoder.wrap(view.buffer(), view.payloadOffset() + MessageHeaderDecoder.ENCODED_LENGTH,
                         view.blockLength(), view.version());
            if (decoder.seqNo() == seqNo) {
                found = true;
                globalSeqNo = view.globalSeqNo();
            }
        }
    }

    // ── follow ────────────────────────────────────────────────────────────────────

    private static void follow() {
        final int clientId = Integer.getInteger("probe.clientId", 9);
        final boolean latencyStats = Boolean.getBoolean("probe.latencyStats");
        final boolean faultInjection = Boolean.getBoolean("probe.faultInjection");
        final int dropPerSignal = Integer.getInteger("probe.faultDropCount", 1);

        final DeliveryStats stats = new DeliveryStats(latencyStats);
        if (latencyStats) {
            Logger.info(Logger.Component.ClusterProbe, MEMBER_ID, "delivery-latency stats ENABLED");
        }

        final Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(AERON_DIR));
        Logger.info(Logger.Component.ClusterProbe, MEMBER_ID,
                    "Connected to co-located Aeron media driver at %s | clientId %d", AERON_DIR, clientId);

        // The receiver is its own callbacks' subject, so it cannot be a constructor argument to them.
        final AtomicReference<ReplayerStreamReceiver> self = new AtomicReference<>();
        final AtomicBoolean announcedLive = new AtomicBoolean();
        final ReplayerStreamReceiver receiver = new ReplayerStreamReceiver(clientId, stats::onSequenced, null, () -> {
            // Fires on every transition to caught-up, not only the first: after a gap the consumer
            // re-converges, and which frames were delivered live rather than by the healing replay is
            // exactly what these tests measure.
            if (announcedLive.compareAndSet(false, true)) {
                Logger.info(Logger.Component.ClusterProbe, MEMBER_ID, "Caught up — following live");
            } else {
                Logger.info(Logger.Component.ClusterProbe, MEMBER_ID, "re-converged at globalSeqNo %d",
                            self.get().lastGlobalSeqNo());
            }
        });
        self.set(receiver);
        stats.receiver = receiver;
        receiver.start(aeron, MEMBER_ID);
        Logger.info(Logger.Component.ClusterProbe, MEMBER_ID,
                    "Following the node tap — replaying history via the Replayer, then live");

        if (faultInjection) {
            receiver.enableFaultInjection();
            sun.misc.Signal.handle(new sun.misc.Signal("USR1"), signal -> faultDropArmed.addAndGet(dropPerSignal));
            Logger.info(Logger.Component.ClusterProbe, MEMBER_ID,
                        "fault injection ENABLED — SIGUSR1 drops %d live tap frame(s)", dropPerSignal);
        }

        final AtomicBoolean running = new AtomicBoolean(true);
        final AtomicBoolean fatal = new AtomicBoolean();
        final ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();
        final Thread dutyThread = new Thread(() -> {
            final IdleStrategy idle = resolveIdleStrategy();
            try {
                while (running.get()) {
                    if (faultInjection && faultDropArmed.get() > 0) {
                        final int armed = faultDropArmed.getAndSet(0);
                        receiver.injectTapDrop(armed);
                        Logger.info(Logger.Component.ClusterProbe, MEMBER_ID, "fault injection: armed %d tap drop(s)",
                                    armed);
                    }
                    idle.idle(receiver.poll());
                }
            } catch (final RuntimeException ex) {
                // The media driver going away closes the Aeron client under us, and every poll past that
                // throws. Failing fast is the invariant replayer-restart-test.sh phase 2 asserts: a
                // co-located process must die with its node rather than spin against a dead driver.
                Logger.error(Logger.Component.ClusterProbe, Logger.EventCode.ReplayDutyCycleFailure, MEMBER_ID,
                             "duty cycle failed — media driver gone? %s", ex);
                fatal.set(true);
                barrier.signalAll();
            }
        }, "probe-" + clientId);
        dutyThread.start();

        barrier.await();
        running.set(false);
        try {
            dutyThread.join(TimeUnit.SECONDS.toMillis(5));
        } catch (final InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
        stats.report();
        if (!fatal.get()) {
            receiver.close();
            aeron.close();
        }
        barrier.close();
        if (fatal.get()) {
            System.exit(EXIT_DUTY_CYCLE_FATAL);
        }
    }

    /**
     * Counts delivered frames and, when enabled, the cluster-commit → tap delivery latency of the ones
     * delivered while caught up. Replayed history is excluded: its "latency" is the age of the archive.
     */
    private static final class DeliveryStats {
        private final boolean enabled;
        private long[] samplesUs = new long[1 << 16];
        private int sampleCount;
        private long delivered;

        /**
         * Set once, straight after construction — the receiver cannot be a constructor argument because
         * it takes this object's handler. Read at dispatch time rather than once per duty cycle: a poll
         * dispatches up to a fragment limit of frames, and a replay-to-live seam can fall inside one.
         */
        private ReplayerStreamReceiver receiver;

        private DeliveryStats(final boolean enabled) {
            this.enabled = enabled;
        }

        private void onSequenced(final SequencedEvent event) {
            delivered++;
            if (!enabled || !receiver.isCaughtUp()) {
                return;
            }
            if (sampleCount == samplesUs.length) {
                samplesUs = Arrays.copyOf(samplesUs, samplesUs.length * 2);
            }
            samplesUs[sampleCount++] = (event.receiveTimeNs() - event.clusterTimestamp() * 1_000_000L) / 1_000L;
        }

        private void report() {
            Logger.info(Logger.Component.ClusterProbe, MEMBER_ID, "frames delivered in order: %d", delivered);
            if (!enabled) {
                return;
            }
            if (sampleCount == 0) {
                Logger.info(Logger.Component.ClusterProbe, MEMBER_ID,
                            "delivery latency: no caught-up (live) samples recorded");
                return;
            }
            final long[] sorted = Arrays.copyOf(samplesUs, sampleCount);
            Arrays.sort(sorted);
            Logger.info(Logger.Component.ClusterProbe, MEMBER_ID,
                        "delivery latency (cluster-commit -> tap, n=%d): p50=%dus p90=%dus p99=%dus p99.9=%dus "
                            + "max=%dus (the receive stamp is nanosecond-resolution, but the cluster commit "
                            + "timestamp it is measured from is milliseconds, so every sample carries up to 1ms "
                            + "of quantisation on top of the true latency)",
                        sampleCount, pct(sorted, 0.50), pct(sorted, 0.90), pct(sorted, 0.99), pct(sorted, 0.999),
                        sorted[sampleCount - 1]);
        }

        private static long pct(final long[] sorted, final double q) {
            return sorted[(int)(q * (sorted.length - 1))];
        }
    }

    // ── plumbing ──────────────────────────────────────────────────────────────────

    static AeronCluster connectCluster() {
        return connectCluster(NULL_EGRESS);
    }

    /** The same, for a caller that must see session events — {@code TestGateway}'s cluster-session fence. */
    static AeronCluster connectCluster(final EgressListener egressListener) {
        return AeronCluster.connect(new AeronCluster.Context()
            .aeronDirectoryName(AERON_DIR)
            .ingressChannel("aeron:udp")
            .ingressEndpoints(INGRESS_ENDPOINTS)
            .egressChannel("aeron:udp?endpoint=localhost:0")
            .egressListener(egressListener)
            .messageTimeoutNs(CONNECT_TIMEOUT_NS));
    }

    /**
     * Subscribes this node's co-located tap and waits for it to connect. Untethered, like every consumer
     * addresses it: a probe that falls behind must be dropped rather than back-pressure the sequencer.
     */
    private static Subscription awaitTap(final AeronCluster cluster) {
        final Subscription tap = cluster.context().aeron()
            .addSubscription(ReplayerStreamReceiver.FEEDER_CONSUMER_CHANNEL, SequencerService.FEEDER_STREAM_ID);
        final IdleStrategy idle = new YieldingIdleStrategy();
        final long deadline = System.nanoTime() + CONNECT_TIMEOUT_NS;
        while (!tap.isConnected()) {
            if (System.nanoTime() >= deadline) {
                Logger.error(Logger.Component.ClusterProbe, Logger.EventCode.ClusterSessionError, MEMBER_ID,
                             "tap (aeron:ipc/%d) not available — co-located with a SequencerServer?",
                             SequencerService.FEEDER_STREAM_ID);
                return null;
            }
            cluster.pollEgress();
            idle.idle();
        }
        return tap;
    }

    static void offer(final AeronCluster cluster, final DirectBuffer buffer, final int length) {
        final IdleStrategy idle = new YieldingIdleStrategy();
        final long deadline = System.nanoTime() + OFFER_TIMEOUT_NS;
        long result;
        while ((result = cluster.offer(buffer, 0, length)) < 0) {
            if (result == Publication.CLOSED || result == Publication.MAX_POSITION_EXCEEDED) {
                throw new IllegalStateException("cluster ingress offer failed: " + result);
            }
            if (System.nanoTime() >= deadline) {
                throw new IllegalStateException("cluster ingress offer timed out (back-pressure / not connected)");
            }
            cluster.pollEgress();
            idle.idle();
        }
    }

    /** Sleeps {@code micros}, or returns at once if it is not positive. */
    private static void pause(final long micros) {
        if (micros <= 0) {
            return;
        }
        final long deadline = System.nanoTime() + micros * 1_000L;
        while (System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
    }

    static IdleStrategy resolveIdleStrategy() {
        final String name = System.getProperty("probe.idleStrategy", "backoff");
        return switch (name.toLowerCase(Locale.ROOT)) {
            case "yielding" -> new YieldingIdleStrategy();
            case "backoff" -> new BackoffIdleStrategy();
            default -> throw new IllegalArgumentException("Unknown probe.idleStrategy=" + name);
        };
    }

    private static void usage() {
        System.err.println("""
            Usage: ClusterProbe <submit|ping|follow>

              submit  flood -Dprobe.count frames at cluster ingress (default 1000)
              ping    submit one frame and wait for its sequenced echo on the tap
              follow  replay history via the co-located Replayer, then follow the tap live

            See the class javadoc for the -Dprobe.* properties.""");
    }
}
