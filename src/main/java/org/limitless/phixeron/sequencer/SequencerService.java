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
import java.util.concurrent.TimeUnit;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.NoOpLock;
import org.agrona.concurrent.status.CountersReader;
import org.limitless.phixeron.PhixeronCounters;
import org.limitless.phixeron.replayer.ReplayerService;

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
     * How long {@link #awaitTapRecordingActive} waits for the co-located archive's recording of the tap
     * to become active before failing start-up. Bounded so a wedged/absent local archive fails fast at
     * onStart rather than hanging the node.
     */
    private static final long TAP_RECORDING_START_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(5);

    /**
     * Maximum consecutive back-pressure spins in {@link #emit} before printing an alert.
     * At ~10 ns/spin this is ~10 ms per alert period.
     */
    private static final int MAX_BACK_PRESSURE_SPINS = 1_000_000;

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

    // ── Aeron runtime ────────────────────────────────────────────────────────

    private Cluster cluster;
    private ExclusivePublication tapPub;
    private AeronArchive aeronArchive;

    // ── Operator counters (see PhixeronCounters) — created once in onStart, closed in onTerminate ──
    private Counter globalSeqNoCounter;
    private Counter tapBackPressureAlertCounter;
    private Counter rejectedIngressCounter;
    private Counter leadershipChangeCounter;
    private Counter currentLeaderMemberIdCounter;
    private Counter lastTickTimestampCounter;
    private Counter gatewayPromotionCounter;
    private Counter bootstrapActivatedCounter;

    /** Gateway topology is derived from the sequenced Gateway rows (see {@link Sequencer}), not configured. */
    public SequencerService() {
        this.sequencer = new Sequencer();
    }

    /**
     * Cluster start handler
     * @param cluster       with which the service can interact.
     * @param snapshotImage from which the service can load its archived state which can be null when no snapshot.
     */
    @Override
    public void onStart(final Cluster cluster, final Image snapshotImage) {
        this.cluster = cluster;

        // Connect to the co-located archive via IPC — NoOpLock is safe on the single conductor thread.
        aeronArchive = AeronArchive.connect(new AeronArchive.Context()
                                                .aeron(cluster.context().aeron())
                                                .controlRequestChannel("aeron:ipc")
                                                .controlRequestStreamId(100)
                                                .controlResponseChannel("aeron:ipc")
                                                .controlResponseStreamId(101)
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
            throw new IllegalStateException(
                "[SequencerService] Refusing to start from a snapshot: recovery is full-log replay from "
                + "globalSeqNo 1 (see the class javadoc). Remove the snapshot from the cluster directory "
                + "so the log replays in full.");
        }
    }

    /**
     * Blocks until the co-located archive's recording subscription has attached to the tap publication.
     * Bounded by TAP_RECORDING_START_TIMEOUT_NS so an absent local archive fails start-up fast rather than hanging.
     */

    private void awaitTapRecordingActive() {
        final CountersReader counters = cluster.context().aeron().countersReader();
        final long archiveId = aeronArchive.archiveId();
        final long deadlineNs = System.nanoTime() + TAP_RECORDING_START_TIMEOUT_NS;
        while (RecordingPos.findCounterIdBySession(counters, tapPub.sessionId(), archiveId)
               == CountersReader.NULL_COUNTER_ID) {
            if (System.nanoTime() >= deadlineNs) {
                throw new IllegalStateException("[SequencerService] replayer recording did not start within timeout");
            }
            cluster.idleStrategy().idle();
        }
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
        final Aeron aeron = cluster.context().aeron();
        globalSeqNoCounter = PhixeronCounters.addCounter(aeron, PhixeronCounters.SEQUENCER_GLOBAL_SEQ_NO_TYPE_ID,
            "phixeron.sequencer.globalSeqNo member=" + memberId, memberId);
        tapBackPressureAlertCounter = PhixeronCounters.addCounter(aeron,
            PhixeronCounters.SEQUENCER_TAP_BACKPRESSURE_ALERTS_TYPE_ID,
            "phixeron.sequencer.tapBackPressureAlerts member=" + memberId, memberId);
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
        bootstrapActivatedCounter = PhixeronCounters.addCounter(aeron, PhixeronCounters.SEQUENCER_BOOTSTRAP_ACTIVATED_TYPE_ID,
            "phixeron.sequencer.bootstrapActivated member=" + memberId, memberId);
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
        if (activation != Sequencer.NO_FRAME) {
            gatewayPromotionCounter.increment();
            System.out.printf("[SequencerService/%d] gateway session %d closed (%s) — promoting standby%n",
                              cluster.memberId(), session.id(), closeReason);
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
        final int sequenced = sequencer.sequenceMessage(buffer, offset, length, session.id(), timestamp);
        if (sequenced != Sequencer.NO_FRAME) {
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
            scheduleTick();
        }
    }

    /**
     * Schedule heartbeat every TICK_INTERVAL_MS ahead of current cluster time.
     * Spins until the schedule lands
     */
    private void scheduleTick() {
        final long deadline = cluster.time() + TICK_INTERVAL_MS;
        while (!cluster.scheduleTimer(TICK_TIMER_CORRELATION_ID, deadline)) {
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
        System.out.printf("[SequencerService/%d] leadership change: new leader is memberId=%d (isLeader=%b)%n",
                          cluster.memberId(), leaderMemberId, leader);
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
            globalSeqNoCounter, tapBackPressureAlertCounter, rejectedIngressCounter, leadershipChangeCounter,
            currentLeaderMemberIdCounter, lastTickTimestampCounter, gatewayPromotionCounter, bootstrapActivatedCounter
        };
        for (final Counter counter : counters) {
            if (counter != null) {
                counter.close();
            }
        }
    }

    /**
     * Publishes the frame in encodeBuffer[0, length) onto the node-local tap.
     * @param length
     */
    private void emit(final int length) {
        int idleSpins = 0;
        long result;
        while ((result = tapPub.offer(sequencer.buffer(), 0, length)) < 0) {
            if (result == ExclusivePublication.CLOSED || result == ExclusivePublication.MAX_POSITION_EXCEEDED) {
                throw new IllegalStateException("[SequencerService] replayer publication failed: " + result);
            }
            if (++idleSpins >= MAX_BACK_PRESSURE_SPINS) {
                System.err.printf("[SequencerService] ALERT: replayer back-pressure at globalSeqNo=%d%n",
                                  sequencer.globalSeqNo());
                tapBackPressureAlertCounter.increment();
                idleSpins = 0;
            }
            cluster.idleStrategy().idle();
        }
        globalSeqNoCounter.set(sequencer.globalSeqNo());
    }
}
