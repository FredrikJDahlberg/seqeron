package org.limitless.seqeron.tools;

import io.aeron.Aeron;
import io.aeron.FragmentAssembler;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.collections.LongArrayList;
import org.agrona.collections.LongHashSet;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.agrona.concurrent.YieldingIdleStrategy;
import org.limitless.seqeron.app.PendingSends;
import org.limitless.seqeron.sequencer.ClusterStreamSender;
import org.limitless.seqeron.sequencer.FrameLayer;
import org.limitless.seqeron.replayer.client.ReplayerStreamReceiver;
import org.limitless.seqeron.replayer.client.SequencedEvent;
import org.limitless.seqeron.replayer.client.SequencedFrameDecoder;
import org.limitless.seqeron.replayer.client.TapFaultInjector;
import org.limitless.seqeron.sbe.probe.MessageHeaderDecoder;
import org.limitless.seqeron.sbe.probe.MessageHeaderEncoder;
import org.limitless.seqeron.sbe.probe.ProbeMarkerDecoder;
import org.limitless.seqeron.sbe.probe.ProbeMarkerEncoder;
import org.limitless.seqeron.sequencer.SequencerService;
import org.limitless.seqeron.sequencer.SystemFrame;
import org.limitless.seqeron.util.IdleStrategies;
import org.limitless.seqeron.util.Logger;

/**
 * The cluster tier's own load generator and tap consumer, so its end-to-end scripts need no product
 * binary. Four modes, one message ({@code sbe-probe.xml}, {@code payloadId} 5):
 * <ul>
 *   <li><b>submit</b> — flood {@code probe.count} {@code ProbeMarker}s at ingress. Opens no tap
 *       subscription, which an unpolled flood would back-pressure.</li>
 *   <li><b>ping</b> — submit one and wait for its sequenced echo off the co-located tap: ingress,
 *       consensus and tap in one round trip.</li>
 *   <li><b>follow</b> — replay history through the co-located Replayer, then follow the tap live.</li>
 *   <li><b>confirm</b> — send {@code probe.count} through {@link ClusterStreamSender} and a
 *       {@link PendingSends} while following its own tap; exit 0 only if the tap shows seqNo 1..count
 *       exactly once, in order. {@code -Dprobe.pendingSends=false} is the control.</li>
 * </ul>
 *
 * <p>System properties (mirroring {@code ReplayerServer}'s, which every script already sets this way):
 * <pre>
 *   probe.memberId          — which cluster member this probe co-locates with (0/1/2); default 0
 *   probe.aeronDir          — that member's Aeron directory; default {tmpdir}/seqeron-seq-aeron-{memberId}
 *   probe.ingressEndpoints  — cluster ingress endpoints; default the three-node localhost set on
 *                             {@code SEQERON_PORT_BASE}'s ports
 *   probe.egressHost        — hostname the leader sends this client's egress to; default localhost,
 *                             which fails when the leader is on another host
 *   probe.clientId          — follow, confirm: this replica's Replayer client id; default 9
 *   probe.count             — submit, confirm: frames to send; default 1000
 *   probe.fillerBytes       — submit: bytes of filler per frame; default 0
 *   probe.pacingMicros      — submit, confirm: pause between frames; default 0 (as fast as ingress accepts)
 *   probe.pendingSends      — confirm: hold and resend across a leader change; default true
 *   probe.latencyStats      — follow: record and report post-catch-up delivery latency; default false
 *   probe.faultInjection    — follow: install the SIGUSR1 tap-drop handler; default false
 *   probe.faultDropCount    — follow: frames dropped per SIGUSR1; default 1
 *   probe.idleStrategy      — follow: backoff (default), yielding or busyspin
 * </pre>
 */
public final class ClusterProbe {
    /** The probe's {@code payloadId}; private (§6.1), so it needs no {@code PayloadIdRegistered} row. */
    public static final int PROBE_PAYLOAD_ID = 5;

    /** The probe's {@code sourceId} (§5), claimed by no topology row, so S-6 leaves its frames unchecked. */
    public static final int PROBE_SOURCE_ID = 8;

    /** No connection and no advisory session: the probe is a producer, not a gateway with sockets. */
    private static final int NO_ID = -1;

    /** {@code ping} and {@code TestGateway} carry no filler; only {@code submit} pads a frame. */
    static final byte[] NO_FILLER = new byte[0];

    private static final int MEMBER_ID = Integer.getInteger("probe.memberId", 0);

    /** The duty-cycle idle strategy {@code follow} and {@code TestGateway} poll with. */
    static final String IDLE_STRATEGY_PROPERTY = "probe.idleStrategy";

    /** Which cluster member every {@code probe.*} process co-locates with; {@code TestGateway} shares it. */
    static int memberId() {
        return MEMBER_ID;
    }

    private static final String AERON_DIR = System.getProperty(
        "probe.aeronDir", System.getProperty("java.io.tmpdir") + "/seqeron-seq-aeron-" + MEMBER_ID);

    private static final String INGRESS_ENDPOINTS = System.getProperty(
        "probe.ingressEndpoints", ClusterStreamSender.ingressEndpoints(ClusterStreamSender.DEFAULT_NODE_COUNT));

    private static final String EGRESS_HOST = System.getProperty("probe.egressHost", "localhost");

    /** The co-located member's Aeron directory; {@code TestGateway} connects through it too. */
    static String aeronDir() {
        return AERON_DIR;
    }

    static String ingressEndpoints() {
        return INGRESS_ENDPOINTS;
    }

    /** This client's own egress endpoint, on an ephemeral port. */
    static String egressChannel() {
        return "aeron:udp?endpoint=" + EGRESS_HOST + ":0";
    }

    private static final long CONNECT_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(5);
    private static final long OFFER_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(5);

    /**
     * How often {@link #offer} keeps its own session alive while it spins. Must stay well inside the
     * cluster's {@code sessionTimeoutNs} (1s by default, {@code sequencer.sessionTimeoutMs}).
     */
    private static final long KEEP_ALIVE_INTERVAL_NS = TimeUnit.MILLISECONDS.toNanos(200);

    /**
     * How long the client waits for a {@code NewLeader} before closing itself. The default, 2x the cluster's
     * 200ms {@code leaderHeartbeatTimeoutNs}, is shorter than an election.
     */
    private static final long NEW_LEADER_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(5);
    private static final long ECHO_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(10);

    /** confirm: frames in flight between send and the tap; far above what one round trip holds. */
    private static final int PENDING_CAPACITY = 4096;

    /** confirm: how long the run may go without sending, resending or seeing an own frame before it is judged. */
    private static final long DRAIN_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(15);

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
            case "confirm" -> System.exit(confirm(Integer.getInteger("probe.count", 1000)));
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
            final MarkerEncoder marker = new MarkerEncoder();
            final byte[] filler = new byte[fillerBytes];
            Arrays.fill(filler, (byte)'x');
            for (long seqNo = 1; seqNo <= count; seqNo++) {
                offer(cluster, marker.frame(), marker.encode(seqNo, NO_ID, PROBE_SOURCE_ID, filler));
                pause(pacingMicros);
            }
            Logger.info(Logger.CoreComponent.ClusterProbe, MEMBER_ID,
                        "submitted %d ProbeMarker(s), filler %d bytes, pacing %dus", count, fillerBytes,
                        pacingMicros);
            return 0;
        } catch (final RuntimeException ex) {
            Logger.error(Logger.CoreComponent.ClusterProbe, Logger.CoreEventCode.ClusterSessionError, MEMBER_ID,
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
            final MarkerEncoder marker = new MarkerEncoder();
            offer(cluster, marker.frame(), marker.encode(seqNo, NO_ID, PROBE_SOURCE_ID, NO_FILLER));

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
                Logger.error(Logger.CoreComponent.ClusterProbe, Logger.CoreEventCode.ClusterSessionError, MEMBER_ID,
                             "ping seqNo %d was not echoed on the tap within %ds", seqNo,
                             TimeUnit.NANOSECONDS.toSeconds(ECHO_TIMEOUT_NS));
                return 1;
            }
            Logger.info(Logger.CoreComponent.ClusterProbe, MEMBER_ID, "ping seqNo %d echoed at globalSeqNo %d",
                        seqNo, handler.globalSeqNo);
            return 0;
        } catch (final RuntimeException ex) {
            Logger.error(Logger.CoreComponent.ClusterProbe, Logger.CoreEventCode.ClusterSessionError, MEMBER_ID,
                         "ping failed: %s", ex);
            return 1;
        }
    }

    /**
     * Everything one producing thread needs to encode {@code ProbeMarker} frames, held rather than
     * allocated per frame. Not thread-safe.
     */
    static final class MarkerEncoder {
        private final SystemFrame envelope = new SystemFrame();
        private final ExpandableArrayBuffer frame = new ExpandableArrayBuffer();
        private final ExpandableArrayBuffer payload = new ExpandableArrayBuffer();
        private final MessageHeaderEncoder payloadHeader = new MessageHeaderEncoder();
        private final ProbeMarkerEncoder marker = new ProbeMarkerEncoder();

        /** The frame the last {@link #encode} wrote, valid up to the length it returned. */
        ExpandableArrayBuffer frame() {
            return frame;
        }

        /** The payload the last {@link #encodePayload} wrote, its own {@code MessageHeader} included. */
        ExpandableArrayBuffer payload() {
            return payload;
        }

        /** Encodes one {@code ProbeMarker} payload alone, for a caller whose publisher adds the envelope. */
        int encodePayload(final long seqNo, final byte[] filler) {
            marker.wrapAndApplyHeader(payload, 0, payloadHeader);
            marker.seqNo(seqNo);
            marker.putFiller(filler, 0, filler.length);
            return MessageHeaderEncoder.ENCODED_LENGTH + marker.encodedLength();
        }

        /**
         * Encodes one {@code ProbeMarker} into the payload buffer, then wraps it as an {@code Unsequenced}
         * frame. The payload carries its own {@code MessageHeader}, as every payload does.
         * @param connectionId the connection this frame belongs to, or -1 for a producer-scoped one
         * @param sourceId     the producer stamping it
         * @return the frame's length in bytes
         */
        int encode(final long seqNo, final int connectionId, final int sourceId, final byte[] filler) {
            final int payloadLength = encodePayload(seqNo, filler);
            final int length = envelope.wrapPayload(frame, sourceId, connectionId, NO_ID, PROBE_PAYLOAD_ID,
                                                    payload, payloadLength);
            if (length == SystemFrame.REFUSED) {
                throw new IllegalArgumentException(
                    "probe.fillerBytes makes the payload larger than a frame may carry");
            }
            return length;
        }
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

    // ── confirm ───────────────────────────────────────────────────────────────────

    /**
     * Sends seqNo 1..count while following this node's tap, then judges what the tap showed from this
     * process's own sessions. The frames go out as fast as the pacing allows, so a leader killed meanwhile
     * has some in flight.
     * @return 0 if the tap showed every frame exactly once, in order
     */
    private static int confirm(final int count) {
        final boolean tracked = Boolean.parseBoolean(System.getProperty("probe.pendingSends", "true"));
        final int clientId = Integer.getInteger("probe.clientId", 9);
        final long pacingNs = Long.getLong("probe.pacingMicros", 0L) * 1_000L;
        final PendingSends pending = new PendingSends(PENDING_CAPACITY);
        final OwnFrames own = new OwnFrames();
        final AtomicBoolean caughtUp = new AtomicBoolean();
        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(AERON_DIR));
             ClusterStreamSender sender = new ClusterStreamSender()) {
            final ReplayerStreamReceiver receiver = new ReplayerStreamReceiver(clientId, event -> {
                if (tracked) {
                    pending.onSequenced(event);
                }
                own.onSequenced(event);
            }, (leaderMemberId, leadershipTermId, globalSeqNo) -> pending.onLeadershipChanged(leadershipTermId),
                () -> caughtUp.set(true));
            receiver.start(aeron, MEMBER_ID);
            final IdleStrategy idle = new YieldingIdleStrategy();
            while (!caughtUp.get()) {
                idle.idle(receiver.poll());
            }
            sender.setIngressEndpoints(INGRESS_ENDPOINTS);
            if (tracked) {
                sender.setIngressHold(pending);
            }
            sender.connect(aeron, egressChannel());
            Logger.info(Logger.CoreComponent.ClusterProbe, MEMBER_ID, "confirm: sending %d frame(s)%s", count,
                        tracked ? " through PendingSends" : " untracked (control)");

            final MarkerEncoder marker = new MarkerEncoder();
            long next = 1;
            long nextSendNs = 0;
            long lastProgressNs = System.nanoTime();
            int resent = 0;
            while (true) {
                final long now = System.nanoTime();
                final int seen = own.seqNos.size();
                final int work = receiver.poll() + sender.pollEgress();
                sender.keepAlive();
                if (sender.isSessionLost() || pending.isFaulted()) {
                    Logger.error(Logger.CoreComponent.ClusterProbe, Logger.CoreEventCode.ClusterSessionError,
                                 MEMBER_ID, "confirm: %s", pending.isFaulted() ? "PendingSends faulted"
                                                                              : "cluster session lost");
                    return 1;
                }
                final int resentNow = tracked ? pending.resendMissing(sender) : 0;
                if (resentNow > 0) {
                    resent += resentNow;
                    own.sessions.add(sender.clusterSessionId());
                }
                final boolean gated = tracked && (pending.isHolding() || pending.isFull());
                if (next <= count && !gated && now >= nextSendNs) {
                    final int length = marker.encode(next, NO_ID, PROBE_SOURCE_ID, NO_FILLER);
                    if (sender.send(marker.frame(), length)) {
                        own.sessions.add(sender.clusterSessionId());
                        if (tracked) {
                            pending.track(marker.frame(), length, sender.clusterSessionId(),
                                          sender.leadershipTermId());
                        }
                        next++;
                        nextSendNs = now + pacingNs;
                        lastProgressNs = now;
                    }
                }
                if (resentNow > 0 || own.seqNos.size() > seen) {
                    lastProgressNs = now;
                }
                final boolean drained = next > count && (tracked ? pending.size() == 0 : own.last() == count);
                if (drained || now - lastProgressNs > DRAIN_TIMEOUT_NS) {
                    break;
                }
                idle.idle(work + resentNow);
            }
            receiver.close();
            return own.judge(count, resent);
        } catch (final RuntimeException ex) {
            Logger.error(Logger.CoreComponent.ClusterProbe, Logger.CoreEventCode.ClusterSessionError, MEMBER_ID,
                         "confirm failed: %s", ex);
            return 1;
        }
    }

    /** The seqNos this process's own sessions put on the tap, in tap order. */
    private static final class OwnFrames {
        private final LongHashSet sessions = new LongHashSet();
        private final LongArrayList seqNos = new LongArrayList();
        private final ProbeMarkerDecoder decoder = new ProbeMarkerDecoder();

        private void onSequenced(final SequencedEvent event) {
            if (event.isSystem() || event.payloadId() != PROBE_PAYLOAD_ID ||
                event.templateId() != ProbeMarkerDecoder.TEMPLATE_ID || !sessions.contains(event.sourceSessionId())) {
                return;
            }
            decoder.wrap(event.buffer(), event.offset() + MessageHeaderDecoder.ENCODED_LENGTH, event.blockLength(),
                         event.version());
            seqNos.addLong(decoder.seqNo());
        }

        private long last() {
            return seqNos.isEmpty() ? 0 : seqNos.getLong(seqNos.size() - 1);
        }

        /** 0 if the tap showed 1..count exactly once, in order; logs the tally either way. */
        private int judge(final int count, final int resent) {
            final LongHashSet distinct = new LongHashSet();
            int outOfOrder = 0;
            for (int i = 0; i < seqNos.size(); i++) {
                distinct.add(seqNos.getLong(i));
                if (i > 0 && seqNos.getLong(i) <= seqNos.getLong(i - 1)) {
                    outOfOrder++;
                }
            }
            final int missing = count - distinct.size();
            final int duplicated = seqNos.size() - distinct.size();
            final boolean exact = missing == 0 && duplicated == 0 && outOfOrder == 0;
            Logger.info(Logger.CoreComponent.ClusterProbe, MEMBER_ID,
                        "confirm: %s — tap showed %d of %d: missing %d, duplicated %d, out of order %d; resent %d",
                        exact ? "EXACT" : "NOT EXACT", seqNos.size(), count, missing, duplicated, outOfOrder, resent);
            return exact ? 0 : 1;
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
            Logger.info(Logger.CoreComponent.ClusterProbe, MEMBER_ID, "delivery-latency stats ENABLED");
        }

        final Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(AERON_DIR));
        Logger.info(Logger.CoreComponent.ClusterProbe, MEMBER_ID,
                    "Connected to co-located Aeron media driver at %s | clientId %d", AERON_DIR, clientId);

        // The receiver is its own callbacks' subject, so it cannot be a constructor argument to them.
        final AtomicReference<ReplayerStreamReceiver> self = new AtomicReference<>();
        final AtomicBoolean announcedLive = new AtomicBoolean();
        final TapFaultInjector tapFaults = faultInjection ? new TapFaultInjector() : null;
        final ReplayerStreamReceiver receiver = new ReplayerStreamReceiver(clientId, stats::onSequenced, null, () -> {
            // Fires on every transition to caught-up, including re-convergence after a gap.
            if (announcedLive.compareAndSet(false, true)) {
                Logger.info(Logger.CoreComponent.ClusterProbe, MEMBER_ID, "Caught up — following live");
            } else {
                Logger.info(Logger.CoreComponent.ClusterProbe, MEMBER_ID, "re-converged at globalSeqNo %d",
                            self.get().lastGlobalSeqNo());
            }
        }, tapFaults);
        self.set(receiver);
        stats.receiver = receiver;
        receiver.start(aeron, MEMBER_ID);
        Logger.info(Logger.CoreComponent.ClusterProbe, MEMBER_ID,
                    "Following the node tap — replaying history via the Replayer, then live");

        if (faultInjection) {
            sun.misc.Signal.handle(new sun.misc.Signal("USR1"), signal -> faultDropArmed.addAndGet(dropPerSignal));
            Logger.info(Logger.CoreComponent.ClusterProbe, MEMBER_ID,
                        "fault injection ENABLED — SIGUSR1 drops %d live tap frame(s)", dropPerSignal);
        }

        final AtomicBoolean running = new AtomicBoolean(true);
        final AtomicBoolean fatal = new AtomicBoolean();
        final ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();
        final Thread dutyThread = new Thread(() -> {
            final IdleStrategy idle = IdleStrategies.fromProperty(IDLE_STRATEGY_PROPERTY).get();
            try {
                while (running.get()) {
                    if (faultInjection && faultDropArmed.get() > 0) {
                        final int armed = faultDropArmed.getAndSet(0);
                        tapFaults.arm(armed);
                        Logger.info(Logger.CoreComponent.ClusterProbe, MEMBER_ID,
                                    "fault injection: armed %d tap drop(s)",
                                    armed);
                    }
                    idle.idle(receiver.poll());
                }
            } catch (final RuntimeException ex) {
                // The driver going away closes the Aeron client and every poll throws: a co-located
                // process must die with its node (replayer-restart-test.sh phase 2).
                Logger.error(Logger.CoreComponent.ClusterProbe, Logger.CoreEventCode.ReplayDutyCycleFailure, MEMBER_ID,
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
         * Set once after construction, since the receiver takes this object's handler. Read per dispatch:
         * a replay-to-live seam can fall inside one poll.
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
            samplesUs[sampleCount++] = (event.receiveTimeNs() - event.clusterTimestampNs()) / 1_000L;
        }

        private void report() {
            Logger.info(Logger.CoreComponent.ClusterProbe, MEMBER_ID, "frames delivered in order: %d", delivered);
            if (!enabled) {
                return;
            }
            if (sampleCount == 0) {
                Logger.info(Logger.CoreComponent.ClusterProbe, MEMBER_ID,
                            "delivery latency: no caught-up (live) samples recorded");
                return;
            }
            final long[] sorted = Arrays.copyOf(samplesUs, sampleCount);
            Arrays.sort(sorted);
            Logger.info(Logger.CoreComponent.ClusterProbe, MEMBER_ID,
                        "delivery latency (cluster-commit -> tap, n=%d): p50=%dus p90=%dus p99=%dus p99.9=%dus "
                            + "max=%dus (both stamps are epoch nanoseconds, but the commit timestamp is the "
                            + "leader's clock, so a cross-host sample carries the clock offset between the two)",
                        sampleCount, pct(sorted, 0.50), pct(sorted, 0.90), pct(sorted, 0.99), pct(sorted, 0.999),
                        sorted[sampleCount - 1]);
        }

        private static long pct(final long[] sorted, final double q) {
            return sorted[(int)(q * (sorted.length - 1))];
        }
    }

    // ── plumbing ──────────────────────────────────────────────────────────────────

    static AeronCluster connectCluster() {
        return AeronCluster.connect(new AeronCluster.Context()
            .aeronDirectoryName(AERON_DIR)
            .ingressChannel("aeron:udp")
            .ingressEndpoints(INGRESS_ENDPOINTS)
            .egressChannel(egressChannel())
            .egressListener(NULL_EGRESS)
            .messageTimeoutNs(CONNECT_TIMEOUT_NS)
            .newLeaderTimeoutNs(NEW_LEADER_TIMEOUT_NS));
    }

    /** Subscribes this node's tap, untethered, and waits for it to connect. */
    private static Subscription awaitTap(final AeronCluster cluster) {
        final Subscription tap = cluster.context().aeron()
            .addSubscription(ReplayerStreamReceiver.FEEDER_CONSUMER_CHANNEL, FrameLayer.FEEDER_STREAM_ID);
        final IdleStrategy idle = new YieldingIdleStrategy();
        final long deadline = System.nanoTime() + CONNECT_TIMEOUT_NS;
        while (!tap.isConnected()) {
            if (System.nanoTime() >= deadline) {
                Logger.error(Logger.CoreComponent.ClusterProbe, Logger.CoreEventCode.ClusterSessionError, MEMBER_ID,
                             "tap (aeron:ipc/%d) not available — co-located with a SequencerServer?",
                             FrameLayer.FEEDER_STREAM_ID);
                return null;
            }
            cluster.pollEgress();
            idle.idle();
        }
        return tap;
    }

    /**
     * Offers to cluster ingress, spinning through back-pressure and a leadership change. {@code CLOSED} is
     * not terminal: during an election the ingress publication is closed until {@code pollEgress} installs
     * the new leader's, so only the client closing itself ends the spin. It sends keep-alives too, so a
     * long spin against a back-pressuring leader does not time the session out.
     */
    static void offer(final AeronCluster cluster, final DirectBuffer buffer, final int length) {
        final IdleStrategy idle = new YieldingIdleStrategy();
        final long start = System.nanoTime();
        final long deadline = start + OFFER_TIMEOUT_NS;
        long nextKeepAlive = start + KEEP_ALIVE_INTERVAL_NS;
        long result;
        while ((result = cluster.offer(buffer, 0, length)) < 0) {
            if (result == Publication.MAX_POSITION_EXCEEDED || cluster.isClosed()) {
                throw new IllegalStateException("cluster ingress offer failed: " + result);
            }
            final long now = System.nanoTime();
            if (now >= deadline) {
                throw new IllegalStateException("cluster ingress offer timed out (back-pressure / no leader)");
            }
            if (now >= nextKeepAlive) {
                cluster.sendKeepAlive();
                nextKeepAlive = now + KEEP_ALIVE_INTERVAL_NS;
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

    private static void usage() {
        System.err.println("""
            Usage: ClusterProbe <submit|ping|follow|confirm>

              submit  flood -Dprobe.count frames at cluster ingress (default 1000)
              ping    submit one frame and wait for its sequenced echo on the tap
              follow  replay history via the co-located Replayer, then follow the tap live
              confirm send -Dprobe.count frames and check the tap shows each once, in order

            See the class javadoc for the -Dprobe.* properties.""");
    }
}
