package org.limitless.phixeron.sequencer;

import io.aeron.Aeron;
import io.aeron.Counter;
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.archive.status.RecordingPos;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.logbuffer.Header;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.NoOpLock;
import org.agrona.concurrent.status.CountersReader;
import org.limitless.phixeron.metrics.PhixeronCounters;
import org.limitless.phixeron.replayer.ReplayerService;
import org.limitless.phixeron.util.Logger;

/**
 * Aeron Cluster service that imposes a total order on messages arriving from multiple clients.
 *
 * <p>For every committed {@link #onSessionMessage} the service assigns a
 * <b>globalSeqNo</b> — a cluster-wide monotone counter shared across all sources and
 * lifecycle events (connect / disconnect / leadership change) — and stamps it, together with the cluster
 * consensus timestamp, into the message's {@code header} composite before republishing it.
 *
 * <p><b>This class is the Aeron adapter, not the state machine.</b> All sequencing state and every
 * frame encode — including the {@code sbe-unsequenced.xml} (schema 200) → {@code sbe-sequenced.xml}
 * (schema 202) copy-through trick that lets the sequencer re-stamp any FIX message type without
 * knowing about it — live in {@link Sequencer}, which has no Aeron dependency and is unit-tested
 * directly. What remains here is the cluster-facing half: the tap publication and its recording,
 * timer scheduling, and the reliable-offer discipline in {@link #emit}.
 *
 * <p>The decorated message is published on the node-local <em>tap</em>
 * ({@link #FEEDER_CHANNEL} / {@link #FEEDER_STREAM_ID}), an {@code aeron:ipc} stream that this node's
 * co-located Aeron Archive records. Co-located app replicas follow it live directly, and the
 * co-located {@link ReplayerService} serves history/gap replay of this
 * recording to those apps on startup.
 *
 * <p><b>Every node records its own tap (no leader/follower asymmetry on the stream path):</b> all
 * cluster nodes maintain identical sequencing state (updated on every callback) and each one
 * publishes and records its own tap. Because every node processes the same committed log in the
 * same order, the taps are byte-identical across nodes, so every node's local archive independently
 * holds a complete copy of the sequenced history — no cross-node replication is needed, and any node
 * a client is co-located with can serve full history/gap replay. The tap {@link
 * ExclusivePublication} and its recording are created once in {@link #onStart} and live for the whole
 * process, continuous across leadership changes (an {@code aeron:ipc} publication has no fixed port to
 * collide on across a failover, unlike the retired UDP global stream), so a given node's recording is a
 * single continuous run spanning every leader tenure rather than one recording per tenure.
 *
 * <p><b>Durability:</b> {@link #emit} is <em>reliable</em> (it spins until the offer lands), because
 * the tap recording is the authoritative history — a dropped frame would be an unrecoverable gap. This
 * cannot wedge structurally the way the retired UDP global stream did (audit.md S4, where {@code
 * MaxMulticastFlowControl} never advanced the sender limit with zero network subscribers): the only
 * tethered subscriber of the tap is the co-located archive recording, so {@link #emit} blocks only on
 * real local-archive write back-pressure, which clears as the archive drains to disk. The app replicas'
 * own tap subscriptions are untethered, so a slow app is dropped (and heals via the ReplayerService replay
 * protocol) rather than back-pressuring the recording.
 *
 * <p><b>…but reliable is not unbounded: a node that cannot record its tap terminates.</b> Spinning is
 * right for an archive that is merely busy and wrong for one that is dead, and the two are told apart by
 * whether the recording behind the tap is still there and still advancing ({@link TapStallPolicy}, driven
 * from {@link #emit} on back-pressure and from the 1 Hz tick on liveness — {@link
 * #checkTapRecordingAlive} covers the case that never back-pressures at all). Once the archive is
 * provably not recording, this node cannot do the job it exists to do, so {@link #fatalTapFailure} takes
 * it down: the peers hold identical complete recordings and keep quorum, and the restart rebuilds this
 * node's recording from {@code globalSeqNo} 1 over the full-log replay it performs anyway. Note that
 * <em>throwing</em> is not an option in any of the callbacks below — see {@link #emit}.
 *
 * <p><b>No snapshots — both hooks refuse.</b> Recovery here is always full-log replay from {@code
 * globalSeqNo} 1, and that is what makes a node's tap recording a complete copy of history rather
 * than one beginning wherever a snapshot left off. A snapshot would also have to carry all of {@link
 * Sequencer}'s replicated state — the gateway topology, the standby-promotion session map, the
 * bootstrap-activation latch — and one that silently dropped any of it would diverge the restored
 * node from its peers, breaking the byte-identical-taps invariant above. So {@link #onTakeSnapshot}
 * throws rather than persisting a partial state, and {@link #onStart} refuses a snapshot image
 * rather than restoring from one. Nothing in normal operation reaches either: {@code clusterctl
 * shutdown} uses {@code ABORT}, which takes no snapshot. See doc/review-2026-07-25.md #5.
 */
public final class SequencerService implements ClusteredService {
    /**
     * Node-local IPC channel and stream the sequenced stream is tapped onto. Every node — leader
     * <em>and</em> follower — republishes each sequenced frame here in {@code globalSeqNo} order (the
     * taps are byte-identical across nodes, since every node processes the same committed log in the
     * same order) and records it into its own co-located archive. The co-located app replicas follow it
     * directly as their live feed, and the co-located {@link ReplayerService}
     * serves history/gap replay of this recording — the same node-local archive serves both.
     *
     * <p>Created and recorded once in {@link #onStart} and continuous per node across leadership changes
     * ({@code aeron:ipc} has no fixed port to collide on across a failover, unlike the retired UDP global
     * stream), so a node's recording is one continuous run spanning every leader tenure that consumers
     * never re-resolve. Reliable, not lossy ({@link #emit} spins until the offer lands): the recording
     * is the authoritative history, so a dropped frame would be an unrecoverable gap.
     */
    public static final String FEEDER_CHANNEL = "aeron:ipc";
    public static final int FEEDER_STREAM_ID = 205;

    /**
     * Control-response stream for this service's own archive client (startRecording/stopRecording).
     * Must not be 101: that's Aeron Cluster's default {@code ingressStreamId}, and
     * isIpcIngressAllowed(true) makes the leader subscribe to ingress on aeron:ipc/101 too — sharing
     * it here means every archive reply misdecodes as an ingress frame (and vice versa). Also distinct
     * from ReplayerNode's ARCHIVE_CONTROL_RESPONSE_STREAM_ID (120), which shares this member's
     * aeron:ipc driver. Matches SequencerNode's own ARCHIVE_CONTROL_RESPONSE_STREAM_ID (121) — sharing
     * a value is fine, since the archive protocol demuxes concurrent clients on one response stream by
     * controlSessionId/correlationId.
     */
    private static final int ARCHIVE_CONTROL_RESPONSE_STREAM_ID = 121;

    /**
     * How long {@link #awaitTapRecordingActive} waits for the co-located archive's recording of the tap
     * to become active before failing start-up. Bounded so a wedged/absent local archive fails fast at
     * onStart rather than hanging the node.
     */
    private static final long TAP_RECORDING_START_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(5);

    /**
     * How often, in wall time, back-pressure in {@link #emit} is alerted on and {@link #stallPolicy}
     * re-evaluated. This used to be a spin count (1,000,000, documented as "~10 ms at ~10 ns/spin"), which
     * is only true when the loop spins on an idle core: the container idles with a {@code
     * YieldingIdleStrategy}, and on a loaded host a yield costs microseconds, so the same count took over
     * ten seconds — long enough that a node whose archive had died sat there spinning without ever
     * reaching the evaluation that would have terminated it. The stall thresholds below are wall-clock
     * durations, so what samples them has to be too.
     */
    private static final long BACK_PRESSURE_ALERT_INTERVAL_NS = TimeUnit.MILLISECONDS.toNanos(10);

    /**
     * Spins between clock reads while back-pressured. {@code System.nanoTime} is cheap but not free, and
     * the idle strategy may be a busy-spin one, so the clock is not read on every iteration.
     */
    private static final int SPINS_PER_CLOCK_CHECK = 1024;

    /**
     * How long tap-emit back-pressure must persist, continuously, before {@link #emit} treats it as a
     * genuine local-archive stall.
     */
    private static final long SUSTAINED_BACKPRESSURE_THRESHOLD_NS = TimeUnit.MILLISECONDS.toNanos(200);

    /**
     * How long the tap recording may make <em>zero</em> progress, while {@link #emit} is back-pressured,
     * before this node gives up on the local archive and terminates (see {@link #fatalTapFailure}). Not a
     * back-pressure timeout: an archive draining slowly under load back-pressures continuously and keeps
     * advancing, and is left alone however long that lasts — this bounds only an archive that has stopped
     * draining. Kept at 5x the stall gauge, the same margin {@code sessionTimeoutNs} keeps over the
     * keep-alive interval, and lands this node's own fatal judgement in the same order of magnitude as
     * {@code sessionTimeoutNs}'s 1s — the other threshold governing how long this cluster tolerates a
     * dependency going quiet. Re-tune against real production storage before trusting it off loopback.
     */
    private static final long TAP_STALL_FATAL_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(1);

    /**
     * How long the consensus module may refuse the cluster-clock timer, continuously, before {@link
     * #scheduleTick} gives up on this node. The same judgement {@link #TAP_STALL_FATAL_TIMEOUT_NS} makes
     * about the archive, applied to the other end of the service: back-pressure on the consensus-module
     * proxy is ordinary and self-clearing, and a full second of it without a single accepted timer is not
     * a busy module but a wedged one. Matched to that constant deliberately — both bound the same
     * question, "is the thing this node depends on still draining?", and there is no reason for the two
     * answers to differ.
     */
    private static final long TICK_SCHEDULE_FATAL_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(1);

    /**
     * How long after signalling a fatal tap failure the process may still be alive before it is halted
     * outright. The graceful path has to close the very archive that may be the thing wedged, so it can
     * hang; by this point the node is committed to dying and nothing is lost by skipping the niceties.
     * Well above the ~7s a healthy teardown takes when {@link #emit} is the wedged party — the container's
     * close has to wait out its own retry timeout and then interrupt this thread out of the spin — so the
     * backstop cannot pre-empt a shutdown that was about to succeed. It is a backstop, not a deadline.
     */
    private static final long FATAL_SHUTDOWN_BACKSTOP_NS = TimeUnit.SECONDS.toNanos(30);

    /**
     * Period of the internal cluster clock ({@link Sequencer#tick}): the leader fires this timer once per
     * second and every node emits a header-only {@code Tick} carrying the consensus timestamp. It exists
     * so every consumer has a cluster-driven clock that keeps advancing even while an individual FIX
     * session is silent — which is exactly when the gateway's keepalive watchdog must probe/disconnect
     * (the sequenced-header timestamp is the only clock the watchdog is allowed to trust, since only the
     * leader assigns real time). 1 Hz gives ±1 s resolution, ample for the watchdog's tens-of-seconds
     * thresholds. Trade-off: every tick appends a timer event + a tick frame to the replicated
     * log/recording, so full-log-replay recovery grows with uptime; this constant is the single knob to
     * trade watchdog resolution against that cost. (A tighter win — gating clock emission on active FIX
     * sessions — is noted in doc/gap.md; 1 Hz is the low-risk interim.)
     */
    private static final long TICK_INTERVAL_MS = 1000;

    /** Set at launch to enable the test-only fault below; unset in production. */
    private static final String FAULT_INJECTION_ENV = "PHIXERON_FAULT_INJECTION";

    /** Touch this file in the cluster directory to arm the fault. See {@link #injectTapRecordingFault}. */
    private static final String TAP_FAULT_TRIGGER_FILE = "tap-stall-fault";

    /**
     * Correlation id of the single repeating tick timer. There is only one service timer, so a fixed
     * constant is safe; rescheduling with the same id simply moves the one timer's deadline.
     */
    private static final long TICK_TIMER_CORRELATION_ID = 0x7100_0000_0000_0001L;

    /**
     * The replicated state machine: owns {@code globalSeqNo} and every frame encode. This class is
     * only its Aeron adapter — it decides <em>when</em> to call the sequencer and publishes what
     * comes back, and holds no replicated state of its own.
     */
    private final Sequencer sequencer;

    /** When to stop waiting on a back-pressured tap and terminate instead. Pure; see {@link TapStallPolicy}. */
    private final TapStallPolicy stallPolicy =
        new TapStallPolicy(SUSTAINED_BACKPRESSURE_THRESHOLD_NS, TAP_STALL_FATAL_TIMEOUT_NS);

    /** Brings the whole node down; wired by {@link SequencerNode}. See {@link #fatalTapFailure}. */
    private final Runnable fatalHandler;

    // ── Aeron runtime ────────────────────────────────────────────────────────

    private Cluster cluster;
    private ExclusivePublication tapPub;
    private AeronArchive aeronArchive;
    private CountersReader counters;

    // ── Tap recording liveness (the archive's own counter for this node's recording) ──
    private int tapRecordingCounterId = CountersReader.NULL_COUNTER_ID;
    private long tapRecordingId = RecordingPos.NULL_RECORDING_ID;
    private boolean fatalSignalled;
    private long fatalSignalledNs;
    /** Test-only fault trigger; null unless fault injection is enabled. See {@link #injectTapRecordingFault}. */
    private Path tapFaultTrigger;

    // ── Operator counters (see PhixeronCounters) — created once in onStart, closed in onTerminate ──
    private Counter globalSeqNoCounter;
    private Counter tapBackPressureAlertCounter;
    private Counter tapStalledCounter;
    private Counter rejectedIngressCounter;
    private Counter leadershipChangeCounter;
    private Counter currentLeaderMemberIdCounter;
    private Counter lastTickTimestampCounter;
    private Counter gatewayPromotionCounter;
    private Counter gatewayPromotionFailedCounter;
    private Counter bootstrapActivatedCounter;
    private Counter connectedClientsCounter;
    private Counter messagesSequencedCounter;

    /**
     * Gateway topology is derived from the sequenced Gateway rows (see {@link Sequencer}), not configured.
     * @param fatalHandler run once, from a cluster callback, when this node can no longer record its own
     *                     tap — see {@link #fatalTapFailure}. Must not block: it is expected to signal a
     *                     shutdown and return, not to perform one.
     */
    public SequencerService(final Runnable fatalHandler) {
        this.sequencer = new Sequencer();
        this.fatalHandler = fatalHandler;
    }

    /**
     * Cluster start handler
     * @param cluster       with which the service can interact.
     * @param snapshotImage from which the service can load its archived state which can be null when no snapshot.
     */
    @Override
    public void onStart(final Cluster cluster, final Image snapshotImage) {
        this.cluster = cluster;
        this.counters = cluster.context().aeron().countersReader();
        if (null != System.getenv(FAULT_INJECTION_ENV)) {
            tapFaultTrigger = cluster.context().clusterDir().toPath().resolve(TAP_FAULT_TRIGGER_FILE);
        }

        // Connect to the co-located archive via IPC — NoOpLock is safe on the single conductor thread.
        aeronArchive = AeronArchive.connect(new AeronArchive.Context()
                                                .aeron(cluster.context().aeron())
                                                .controlRequestChannel("aeron:ipc")
                                                .controlRequestStreamId(100)
                                                .controlResponseChannel("aeron:ipc")
                                                .controlResponseStreamId(ARCHIVE_CONTROL_RESPONSE_STREAM_ID)
                                                .lock(NoOpLock.INSTANCE));
        // Node-local live tap of the sequenced stream, created and recorded on every node (leader and
        // follower alike). Every node re-publishes each sequenced frame here and records it into its own
        // co-located archive, so every node independently holds a complete copy of the sequenced history
        // Co-located app replicas follow this live directly; the co-located ReplayerService serves history/gap replay
        // of this recording.
        tapPub = cluster.context().aeron().addExclusivePublication(FEEDER_CHANNEL, FEEDER_STREAM_ID);
        aeronArchive.startRecording(FEEDER_CHANNEL, FEEDER_STREAM_ID, SourceLocation.LOCAL);
        awaitTapRecordingActive();
        // Counters are NOT created here: cluster.memberId() is still NULL_VALUE during onStart (Aeron
        // assigns it only once this service has joined the active log, after onStart returns) — see
        // ensureCounters(), called instead from the first callback that needs one.

        // snapshots are not supported
        if (snapshotImage != null) {
            throw refuseStart(
                "[SequencerService] Refusing to start from a snapshot: recovery is full-log replay from "
                + "globalSeqNo 1 (see the class javadoc). Remove the snapshot from the cluster directory "
                + "so the log replays in full.");
        }
    }

    /**
     * Blocks until the co-located archive's recording subscription has attached to the tap publication, and
     * keeps its counter so {@link #tapRecordingActive} can tell later whether that recording is still there.
     * Bounded by TAP_RECORDING_START_TIMEOUT_NS so an absent local archive fails start-up fast rather than hanging.
     */

    private void awaitTapRecordingActive() {
        final long archiveId = aeronArchive.archiveId();
        final long deadlineNs = System.nanoTime() + TAP_RECORDING_START_TIMEOUT_NS;
        int counterId;
        while ((counterId = RecordingPos.findCounterIdBySession(counters, tapPub.sessionId(), archiveId))
               == CountersReader.NULL_COUNTER_ID) {
            if (System.nanoTime() >= deadlineNs) {
                throw refuseStart("[SequencerService] replayer recording did not start within timeout");
            }
            cluster.idleStrategy().idle();
        }
        tapRecordingCounterId = counterId;
        tapRecordingId = RecordingPos.getRecordingId(counters, counterId);
    }

    /**
     * Refuses to bring this node up. Throwing alone would only kill the service agent thread — {@code
     * AgentRunner} reports an {@code onStart} failure and stops, but nothing exits the JVM — leaving a
     * headless node whose media driver and consensus module keep running without a service behind them.
     * So the fatal handler brings the process down and the caller's throw stops the agent from proceeding.
     * @param message why start-up was refused
     * @return the exception for the caller to throw
     */
    private IllegalStateException refuseStart(final String message) {
        fatalHandler.run();
        return new IllegalStateException(message);
    }

    /**
     * Lazily creates this node's operator counters (see {@link PhixeronCounters}) on the first
     * callback that needs one, labelled with the memberId so {@code aeron-stat}/{@code clusterctl
     * counters} disambiguate nodes sharing one host. Not created eagerly in {@link #onStart}: {@code
     * cluster.memberId()} is still {@code NULL_VALUE} there — Aeron assigns it only once this service
     * has joined the active log, which happens after {@code onStart} returns but before any of the
     * callbacks below can fire.
     */
    private void ensureCounters() {
        if (globalSeqNoCounter != null) {
            return;
        }
        final int memberId = cluster.memberId();
        // Same reason the counters are labelled here rather than in onStart: this is the first point at
        // which Aeron has assigned the id. The sequencer names itself with it when it rejects an ingress
        // message — it has no other member context, and used to log the *leader's* id in that slot.
        sequencer.memberId(memberId);
        final Aeron aeron = cluster.context().aeron();
        globalSeqNoCounter = PhixeronCounters.addCounter(aeron, PhixeronCounters.SEQUENCER_GLOBAL_SEQ_NO_TYPE_ID,
            "phixeron.sequencer.globalSeqNo member=" + memberId, memberId);
        tapBackPressureAlertCounter = PhixeronCounters.addCounter(aeron,
            PhixeronCounters.SEQUENCER_TAP_BACKPRESSURE_ALERTS_TYPE_ID,
            "phixeron.sequencer.tapBackPressureAlerts member=" + memberId, memberId);
        tapStalledCounter = PhixeronCounters.addCounter(aeron, PhixeronCounters.SEQUENCER_TAP_STALLED_TYPE_ID,
            "phixeron.sequencer.tapStalled member=" + memberId, memberId);
        rejectedIngressCounter = PhixeronCounters.addCounter(aeron, PhixeronCounters.SEQUENCER_REJECTED_INGRESS_COUNT_TYPE_ID,
            "phixeron.sequencer.rejectedIngressCount member=" + memberId, memberId);
        leadershipChangeCounter = PhixeronCounters.addCounter(aeron,
            PhixeronCounters.SEQUENCER_LEADERSHIP_CHANGE_COUNT_TYPE_ID,
            "phixeron.sequencer.leadershipChangeCount member=" + memberId, memberId);
        currentLeaderMemberIdCounter = PhixeronCounters.addCounter(aeron,
            PhixeronCounters.SEQUENCER_CURRENT_LEADER_MEMBER_ID_TYPE_ID,
            "phixeron.sequencer.currentLeaderMemberId member=" + memberId, memberId);
        lastTickTimestampCounter = PhixeronCounters.addCounter(aeron, PhixeronCounters.SEQUENCER_LAST_TICK_TIMESTAMP_TYPE_ID,
            "phixeron.sequencer.lastTickTimestamp member=" + memberId, memberId);
        gatewayPromotionCounter = PhixeronCounters.addCounter(aeron, PhixeronCounters.SEQUENCER_GATEWAY_PROMOTION_COUNT_TYPE_ID,
            "phixeron.sequencer.gatewayPromotionCount member=" + memberId, memberId);
        gatewayPromotionFailedCounter = PhixeronCounters.addCounter(aeron,
            PhixeronCounters.SEQUENCER_GATEWAY_PROMOTION_FAILED_COUNT_TYPE_ID,
            "phixeron.sequencer.gatewayPromotionFailedCount member=" + memberId, memberId);
        bootstrapActivatedCounter = PhixeronCounters.addCounter(aeron, PhixeronCounters.SEQUENCER_BOOTSTRAP_ACTIVATED_TYPE_ID,
            "phixeron.sequencer.bootstrapActivated member=" + memberId, memberId);
        connectedClientsCounter = PhixeronCounters.addCounter(aeron, PhixeronCounters.SEQUENCER_CONNECTED_CLIENTS_TYPE_ID,
            "phixeron.sequencer.connectedClients member=" + memberId, memberId);
        messagesSequencedCounter = PhixeronCounters.addCounter(aeron, PhixeronCounters.SEQUENCER_INGRESS_MESSAGES_TYPE_ID,
            "phixeron.sequencer.ingressMessages member=" + memberId, memberId);
    }

    /**
     * Cluster client session open handler.
     * @param session   for the client which have been opened.
     * @param timestamp at which the session was opened.
     */
    @Override
    public void onSessionOpen(final ClientSession session, final long timestamp) {
    }

    /**
     * Cluster client session close handler.
     * @param session     that has been closed.
     * @param timestamp   at which the session was closed.
     * @param closeReason the session was closed.
     */
    @Override
    public void onSessionClose(final ClientSession session, final long timestamp, final CloseReason closeReason) {
        ensureCounters();
        final int activation = sequencer.sessionClosed(session.id(), timestamp);
        if (activation == Sequencer.NO_PROMOTION_TARGET) {
            // A gateway session closed but nothing replaced it — the cluster is gateway-less for this
            // logical gateway until an instance starts. Sequencer already logged the specifics; this side
            // only needs to surface it as an operator-visible counter, distinct from gatewayPromotionCounter.
            gatewayPromotionFailedCounter.increment();
        } else if (activation != Sequencer.NO_FRAME) {
            gatewayPromotionCounter.increment();
            Logger.info(Logger.Component.SequencerService, cluster.memberId(),
                    "gateway session %d closed (%s) — promoting standby", session.id(), closeReason);
            emit(activation);
        }
    }

    /**
     * Client session message handler
     * @param session   for the client which sent the message. This can be null if the client was a service.
     * @param timestamp for when the message was received.
     * @param buffer    containing the message.
     * @param offset    in the buffer at which the message is encoded.
     * @param length    of the encoded message.
     * @param header    aeron header for the incoming message.
     */
    @Override
    public void onSessionMessage(final ClientSession session,
                                 final long timestamp,
                                 final DirectBuffer buffer,
                                 final int offset,
                                 final int length,
                                 final Header header) {
        ensureCounters();
        if (session == null) {
            // Aeron looks the session up by clusterSessionId and passes the result straight through, so
            // this is null whenever that id has no live session (the interface contract above says so).
            // sessionId is the one header field sequenceMessage cannot copy from the ingress frame, and
            // it is exactly what is missing, so the frame is unsequenceable — count it like any other
            // malformed ingress. Dereferencing would throw, and a throw here is swallowed after the log
            // position has already advanced (see emit), which drops the frame anyway but silently.
            rejectedIngressCounter.increment();
            Logger.error(Logger.Component.SequencerService, Logger.EventCode.MalformedIngressMessage,
                    cluster.memberId(), "skipping ingress message with no client session (globalSeqNo stays %d)",
                    sequencer.globalSeqNo());
            return;
        }
        final int sequenced = sequencer.sequenceMessage(buffer, offset, length, session.id(), timestamp);
        if (sequenced != Sequencer.NO_FRAME) {
            messagesSequencedCounter.increment();
            emit(sequenced);
        } else {
            rejectedIngressCounter.increment();
        }
        // The first EndBasicData opens the trading day: the cluster designates the primary FIX
        // gateway by synthesizing a bootstrap GatewayActive right behind it, on the next globalSeqNo.
        final int activation = sequencer.pendingGatewayBootstrapActivation(timestamp);
        if (activation != Sequencer.NO_FRAME) {
            bootstrapActivatedCounter.set(1);
            emit(activation);
        }
    }

    /**
     * Timer event handler
     * @param correlationId for the expired timer.
     * @param timestamp     at which the timer expired.
     */
    @Override
    public void onTimerEvent(final long correlationId, final long timestamp) {
        if (correlationId == TICK_TIMER_CORRELATION_ID) {
            ensureCounters();
            emit(sequencer.tick(timestamp));
            lastTickTimestampCounter.set(timestamp);
            injectTapRecordingFault();
            checkTapRecordingAlive();
            scheduleTick();
        }
    }

    /**
     * 1 Hz liveness check on the co-located archive's recording of the tap — and the reason {@link #emit}'s
     * back-pressure bound is not enough on its own: <b>a recording that stops does not back-pressure
     * anything.</b> The tap publication still has the app replicas attached (untethered), so offers keep
     * landing and frames keep flowing live while nothing at all is being recorded — this node silently
     * losing the history it is responsible for, discovered only when someone later asks it for a replay.
     * The recording counter going away is the only symptom, so it is checked on the cluster's own clock.
     */
    private void checkTapRecordingAlive() {
        if (fatalSignalled) {
            haltIfShutdownStalled(System.nanoTime());
        } else if (!tapRecordingActive()) {
            fatalTapFailure("the local archive stopped recording the tap (recording " + tapRecordingId + ")");
        }
    }

    /**
     * Test-only fault injection (src/test/scripts/chaos-runner.sh), inert unless {@link
     * #FAULT_INJECTION_ENV} was set at launch: touching {@code <clusterDir>/tap-stall-fault} makes this node
     * stop recording its own tap, which is the only way to provoke {@link #checkTapRecordingAlive} from
     * outside the process — the archive runs inside this JVM, so its recorder cannot be paused or killed on
     * its own. A file trigger rather than a signal: no unsupported JDK signal API, and none of the
     * coalescing caveats the C++ side's SIGUSR1 injector carries. One-shot per process.
     */
    private void injectTapRecordingFault() {
        if (tapFaultTrigger == null || !Files.exists(tapFaultTrigger)) {
            return;
        }
        tapFaultTrigger = null;
        Logger.info(Logger.Component.SequencerService, cluster.memberId(),
                "fault injection: stopping this node's tap recording");
        aeronArchive.stopRecording(FEEDER_CHANNEL, FEEDER_STREAM_ID);
    }

    /**
     * Re-arms the cluster clock ({@link #TICK_INTERVAL_MS} ahead of current cluster time), spinning until
     * the consensus module accepts it — bounded, for the same reason {@link #emit} is. Returning with the
     * timer unscheduled would stop the clock outright: nothing else re-arms it until the next leadership
     * term, so every consumer's session clock would silently stop advancing. Spinning forever is no better
     * — {@code onTimerEvent} would never return and this node would go dark with none of the failure paths
     * below ever running. So a consensus module that has not accepted a timer for {@link
     * #TICK_SCHEDULE_FATAL_TIMEOUT_NS} is treated as wedged and this node terminates, as visibly as it does
     * when it cannot record its own tap. (The only false return is back-pressure: Aeron throws for a
     * closed/disconnected proxy publication rather than returning.)
     */
    private void scheduleTick() {
        final long deadline = cluster.time() + TICK_INTERVAL_MS;
        int spins = 0;
        long backPressuredSinceNs = 0;
        while (!cluster.scheduleTimer(TICK_TIMER_CORRELATION_ID, deadline)) {
            if (++spins >= SPINS_PER_CLOCK_CHECK) {
                spins = 0;
                final long nowNs = System.nanoTime();
                if (backPressuredSinceNs == 0) {
                    backPressuredSinceNs = nowNs;             // first read only anchors the period
                } else if (fatalSignalled) {
                    haltIfShutdownStalled(nowNs);             // keep the backstop alive: no tick reaches it now
                } else if (nowNs - backPressuredSinceNs >= TICK_SCHEDULE_FATAL_TIMEOUT_NS) {
                    fatalFailure(Logger.EventCode.ServiceError,
                            "the consensus module did not accept the cluster-clock timer for "
                            + TimeUnit.NANOSECONDS.toSeconds(TICK_SCHEDULE_FATAL_TIMEOUT_NS)
                            + "s of continuous back-pressure");
                }
            }
            cluster.idleStrategy().idle();
        }
    }

    /**
     * Snapshot handler Unsupported by design (see the class javadoc).
     * @param snapshotPublication to which the state should be recorded.
     */
    @Override
    public void onTakeSnapshot(final ExclusivePublication snapshotPublication) {
        throw new UnsupportedOperationException(
            "[SequencerService] Snapshots are not supported: recovery is full-log replay from globalSeqNo 1, "
            + "which is what keeps each node's tap recording complete. Stop the cluster with clusterctl "
            + "shutdown (ABORT), not a SNAPSHOT/SHUTDOWN toggle.");
    }

    /**
     * New leadership term received
     * @param logPosition identity for the new leadership term.
     * @param leadershipTermId position the log has reached as the result of this message.
     * @param timestamp for the new leadership term.
     * @param termBaseLogPosition position at the beginning of the leadership term.
     * @param leaderMemberId who won the election.
     * @param logSessionId session id for the publication of the log.
     * @param timeUnit for the timestamps in the coming leadership term.
     * @param appVersion for the application configured in the consensus module.
     */
    @Override
    public void onNewLeadershipTermEvent(final long logPosition,
                                         final long leadershipTermId,
                                         final long timestamp,
                                         final long termBaseLogPosition,
                                         final int leaderMemberId,
                                         final int logSessionId,
                                         final TimeUnit timeUnit,
                                         final int appVersion) {
        applyLeadership(leaderMemberId, timestamp);
        scheduleTick();         // Arm (or re-arm) the internal cluster clock here
    }

    /**
     * Role change handler
     * @param newRole that the node has assumed.
     */
    @Override
    public void onRoleChange(final Cluster.Role newRole) {
        // Deliberately a no-op: onNewLeadershipTermEvent already fires on every node
    }

    /**
     * Called when a new leadership term begins.
     * @param leaderMemberId leader member identity
     * @param timestamp now
     */
    private void applyLeadership(final int leaderMemberId, final long timestamp) {
        ensureCounters();
        // Encoded and emitted on every node, so each node's replayer (and its recording) carries this
        // globalSeqNo gap-free. The sequencer suppresses a repeat of the leader already on record.
        final int length = sequencer.leadershipChanged(leaderMemberId, timestamp);
        if (length == Sequencer.NO_FRAME) {
            return;
        }
        leadershipChangeCounter.increment();
        currentLeaderMemberIdCounter.set(leaderMemberId);
        final boolean leader = leaderMemberId == cluster.memberId();
        Logger.info(Logger.Component.SequencerService, cluster.memberId(),
                "leadership change: new leader is memberId=%d (isLeader=%b)", leaderMemberId, leader);
        emit(length);
    }

    /**
     * Termination handler
     * @param cluster with which the service can interact.
     */
    @Override
    public void onTerminate(final Cluster cluster) {
        if (aeronArchive != null) {
            aeronArchive.close();
        }
        if (tapPub != null) {
            // Closing the publication ends the recording's source image, so the archive stops the tap
            // recording (sets its stopPosition) without an explicit stopRecording call.
            tapPub.close();
        }
        closeCounters();
    }

    /**
     * Closes this node's operator counters, freeing their slots in the CnC counters file.
     */
    private void closeCounters() {
        final Counter[] counters = {
            globalSeqNoCounter, tapBackPressureAlertCounter, tapStalledCounter, rejectedIngressCounter,
            leadershipChangeCounter, currentLeaderMemberIdCounter, lastTickTimestampCounter, gatewayPromotionCounter,
            gatewayPromotionFailedCounter, bootstrapActivatedCounter, connectedClientsCounter, messagesSequencedCounter
        };
        for (final Counter counter : counters) {
            if (counter != null) {
                counter.close();
            }
        }
    }

    /**
     * Publishes the frame in encodeBuffer[0, length) onto the node-local tap. Spins on back-pressure — see
     * the class javadoc on why this must be reliable rather than lossy — but not blindly: {@link
     * TapStallPolicy} watches the recording behind the tap, and once it is provably not draining (or gone),
     * {@link #fatalTapFailure} takes the node down rather than wait out a failure that will not clear.
     * {@code tapStalledCounter} still separates a sustained stall from ordinary transient back-pressure
     * (see {@link #SUSTAINED_BACKPRESSURE_THRESHOLD_NS}) for the operator watching it happen.
     *
     * <p><b>The only two exits are a landed offer and process death</b> — never a return with the frame
     * unpublished, and in particular never an exception. An exception raised in any callback below is
     * caught by {@code Image.boundedControlledPoll}, which has <em>already advanced the log position past
     * the message</em> in its {@code finally}, and {@code AgentRunner} keeps the agent running for anything
     * that is not an {@code AgentTerminationException}: the service would resume at the next message,
     * around a hole in its own recording, with {@code globalSeqNo} already consumed. That is precisely the
     * unrecoverable gap all of this exists to prevent, so the failure paths signal and keep spinning. (The
     * spin does unwind on the way out — closing the container interrupts this thread, and the idle strategy
     * raises {@code AgentTerminationException("interrupted")} — but only once the process is already going
     * down, which is why that stack trace appears in the log after a fatal.)
     * @param length of the encoded frame at offset 0 of the sequencer's buffer
     */
    private void emit(final int length) {
        int spins = 0;
        long nextAlertNs = 0;
        long result;
        while ((result = tapPub.offer(sequencer.buffer(), 0, length)) < 0) {
            if (result == ExclusivePublication.CLOSED || result == ExclusivePublication.MAX_POSITION_EXCEEDED) {
                fatalTapFailure("tap publication failed: " + result);
            }
            if (++spins >= SPINS_PER_CLOCK_CHECK) {
                spins = 0;
                final long nowNs = System.nanoTime();
                if (nextAlertNs == 0) {
                    nextAlertNs = nowNs + BACK_PRESSURE_ALERT_INTERVAL_NS;   // first read only anchors the period
                } else if (nowNs - nextAlertNs >= 0) {
                    nextAlertNs = nowNs + BACK_PRESSURE_ALERT_INTERVAL_NS;
                    onBackPressureThreshold(nowNs);
                }
            }
            cluster.idleStrategy().idle();
        }
        // onEmitted() always runs (it resets the policy); the gauge stays latched on a node that is dying,
        // so an operator watching it does not see the stall "clear" on the way out.
        if (stallPolicy.onEmitted() && !fatalSignalled) {
            tapStalledCounter.set(0);
            Logger.info(Logger.Component.Sequencer, cluster.memberId(),
                    "RECOVERED: tap back-pressure cleared at globalSeqNo=%d", sequencer.globalSeqNo());
        }
        globalSeqNoCounter.set(sequencer.globalSeqNo());
        connectedClientsCounter.set(sequencer.connectedClientCount());
    }

    /**
     * One {@link #BACK_PRESSURE_ALERT_INTERVAL_NS} of continuous back-pressure has elapsed in {@link #emit}:
     * alert, then ask {@link TapStallPolicy} whether this is an archive that is merely busy or one that has
     * stopped draining. All of the per-period work lives here rather than in the spin, so the normal emit —
     * where the first offer lands — pays none of it.
     * @param nowNs the clock reading that triggered this period, reused rather than read again
     */
    private void onBackPressureThreshold(final long nowNs) {
        if (fatalSignalled) {
            haltIfShutdownStalled(nowNs);
            return;
        }
        Logger.error(Logger.Component.Sequencer, Logger.EventCode.ReplayerBackpressure, cluster.memberId(),
                "ALERT: replayer back-pressure at globalSeqNo=%d", sequencer.globalSeqNo());
        tapBackPressureAlertCounter.increment();
        switch (stallPolicy.onBackPressure(nowNs, tapRecordingActive(), tapRecordedPosition())) {
            case STALLED -> {
                tapStalledCounter.set(1);
                Logger.error(Logger.Component.Sequencer, Logger.EventCode.ReplayerBackpressure, cluster.memberId(),
                        "STALLED: tap back-pressure sustained beyond %dms with no recording progress at globalSeqNo=%d",
                        TimeUnit.NANOSECONDS.toMillis(SUSTAINED_BACKPRESSURE_THRESHOLD_NS), sequencer.globalSeqNo());
            }
            case FATAL_RECORDING_GONE -> fatalTapFailure(
                    "the local archive stopped recording the tap (recording " + tapRecordingId + ")");
            case FATAL_NO_PROGRESS -> fatalTapFailure("the tap recording made no progress for "
                    + TimeUnit.NANOSECONDS.toMillis(TAP_STALL_FATAL_TIMEOUT_NS) + "ms of continuous back-pressure");
            case CONTINUE -> {
            }
        }
    }

    /**
     * Gives up on this node, because its archive is the authoritative copy of the sequenced history and a
     * frame that cannot be recorded is a hole that no later work can fill: a node that cannot record is no
     * longer doing the job it exists to do, and is better dead than silently incomplete. Also latches the
     * stall gauge, so an operator watching it does not see the stall clear on the way out.
     * @param reason what failed, for the operator
     */
    private void fatalTapFailure(final String reason) {
        if (!fatalSignalled && tapStalledCounter != null) {   // null before the first callback creates the counters
            tapStalledCounter.set(1);
        }
        fatalFailure(Logger.EventCode.TapRecordingFailure, reason
                + ", so it can no longer record the history it is responsible for");
    }

    /**
     * Brings this node down: the peers hold identical, complete recordings and keep quorum without it, and
     * its restart rebuilds everything it held from {@code globalSeqNo} 1 over the full-log replay it
     * performs anyway — so failing loudly and early costs the cluster nothing and costs a silently degraded
     * node everything. Latched: the first call decides, and later ones only fall through to {@link
     * #haltIfShutdownStalled}.
     * @param code   which failure class this is, for the operator's log
     * @param reason what failed
     */
    private void fatalFailure(final Logger.EventCode code, final String reason) {
        if (fatalSignalled) {
            return;
        }
        fatalSignalled = true;
        fatalSignalledNs = System.nanoTime();
        Logger.fault(Logger.Component.SequencerService, code, cluster.memberId(),
                "FATAL: %s at globalSeqNo=%d — terminating this node; its peers keep quorum and its restart "
                + "replays the full log", reason, sequencer.globalSeqNo());
        fatalHandler.run();
    }

    /**
     * Backstop for the graceful teardown {@link #fatalHandler} kicks off: that path has to close the very
     * archive that may be what is wedged, so it can hang. By this point the node is committed to dying and
     * everything it holds is either replicated or replayable, so stop waiting and halt.
     * @param nowNs monotonic clock reading
     */
    private void haltIfShutdownStalled(final long nowNs) {
        if (nowNs - fatalSignalledNs >= FATAL_SHUTDOWN_BACKSTOP_NS) {
            Runtime.getRuntime().halt(SequencerNode.EXIT_TAP_FATAL);
        }
    }

    /** Whether the co-located archive is still recording this node's tap onto {@link #tapRecordingId}. */
    private boolean tapRecordingActive() {
        return RecordingPos.isActive(counters, tapRecordingCounterId, tapRecordingId);
    }

    /** How far the archive has recorded the tap; only meaningful while {@link #tapRecordingActive}. */
    private long tapRecordedPosition() {
        return counters.getCounterValue(tapRecordingCounterId);
    }
}
