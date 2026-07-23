package org.limitless.phixeron.sequencer;

import io.aeron.ExclusivePublication;
import io.aeron.FragmentAssembler;
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
import org.agrona.ExpandableDirectByteBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.NoOpLock;
import org.agrona.concurrent.status.CountersReader;
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
 * timer scheduling, snapshot I/O, and the reliable-offer discipline in {@link #emit}.
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
 * <p><b>Snapshot format</b> (little-endian binary, single fragment):
 * <pre>
 *   int64  globalSeqNo
 * </pre>
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

    private static final int SNAPSHOT_POLL_BATCH = 10;

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

    /** Snapshot scratch; separate from the sequencer's encode buffer so the two never alias. */
    private final MutableDirectBuffer snapshotBuffer = new ExpandableDirectByteBuffer(Long.BYTES);

    // ── Aeron runtime (not snapshotted) ──────────────────────────────────────

    private Cluster cluster;
    private ExclusivePublication tapPub;
    private AeronArchive aeronArchive;

    // ── Construction ────────────────────────────────────────────────────────────

    /** Single-gateway topology defaults; see {@link #SequencerService(int, int)}. */
    public SequencerService() {
        this(Sequencer.DEFAULT_GATEWAY_SOURCE_ID, Sequencer.DEFAULT_PRIMARY_GATEWAY_ID);
    }

    /**
     * @param gatewaySourceId  sourceId identifying a FIX gateway session (the standby-promotion trigger)
     * @param primaryGatewayId designated-primary {@code gatewayId} named by the bootstrap {@code GatewayActive}
     */
    public SequencerService(final int gatewaySourceId, final int primaryGatewayId) {
        this.sequencer = new Sequencer(gatewaySourceId, primaryGatewayId);
    }

    // ── ClusteredService lifecycle ────────────────────────────────────────────

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
        // — no cross-node replication needed. Co-located app replicas follow this live directly; the
        // co-located ReplayerService serves history/gap replay of this recording. See FEEDER_CHANNEL. Recording
        // must be active before the first
        // frame is published, so await it here (onStart runs before any onSessionMessage, so this waits on
        // start-up alone, never on live traffic).
        tapPub = cluster.context().aeron().addExclusivePublication(FEEDER_CHANNEL, FEEDER_STREAM_ID);
        aeronArchive.startRecording(FEEDER_CHANNEL, FEEDER_STREAM_ID, SourceLocation.LOCAL);
        awaitTapRecordingActive();

        if (snapshotImage != null) {
            loadSnapshot(snapshotImage);
        }
        // NB: the internal clock timer is armed in onNewLeadershipTermEvent, not here — Aeron forbids
        // scheduling timers (or sending messages) from onStart.
    }

    // Blocks until the co-located archive's recording subscription has attached to the tap publication,
    // so no frame is published before the recording begins (which would leave an unrecoverable hole in
    // the authoritative history). Bounded by TAP_RECORDING_START_TIMEOUT_NS so an absent/wedged local
    // archive fails start-up fast rather than hanging.
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

    // An Aeron Cluster session opening or closing is a transport event between the cluster and one
    // of its clients — a gateway or an OrderExecClient attaching and detaching. It is not a FIX
    // session lifecycle: the FIX client's TCP connection to the gateway is carried by
    // ClientConnected/ClientDisconnected, which the gateway observes directly and publishes on
    // ingress, so they arrive through onSessionMessage below like every other message and carry the
    // connectionId they refer to. onSessionOpen therefore synthesizes nothing.
    //
    // onSessionClose is the one exception, and only for a FIX *gateway's* cluster session: when the
    // primary gateway detaches (crash or shutdown) that is the promotion trigger for a hot standby.
    // The sequencer synthesizes a GatewayActive so the standby — which has been rebuilding the
    // primary's session state off the tap — opens its accept gate. A non-gateway session closing
    // still produces nothing. See Sequencer.sessionClosed and doc/todo.md item 18.

    @Override
    public void onSessionOpen(final ClientSession session, final long timestamp) {}

    @Override
    public void onSessionClose(final ClientSession session, final long timestamp, final CloseReason closeReason) {
        final int activation = sequencer.sessionClosed(session.id(), timestamp);
        if (activation != Sequencer.NO_FRAME) {
            System.out.printf("[SequencerService/%d] gateway session %d closed (%s) — promoting standby%n",
                              cluster.memberId(), session.id(), closeReason);
            emit(activation);
        }
    }

    @Override
    public void onSessionMessage(final ClientSession session, final long timestamp, final DirectBuffer buffer,
                                 final int offset, final int length, final Header header) {
        emit(sequencer.sequenceMessage(buffer, offset, length, session.id(), timestamp));
        // The first EndBasicData opens the trading day: the cluster designates the primary FIX
        // gateway by synthesizing a bootstrap GatewayActive right behind it, on the next globalSeqNo.
        final int activation = sequencer.pendingBootstrapActivation(timestamp);
        if (activation != Sequencer.NO_FRAME) {
            emit(activation);
        }
    }

    @Override
    public void onTimerEvent(final long correlationId, final long timestamp) {
        if (correlationId == TICK_TIMER_CORRELATION_ID) {
            emit(sequencer.tick(timestamp));
            scheduleTick();
        }
    }

    // Arms the single repeating tick timer for one TICK_INTERVAL_MS ahead of current cluster time. Spins
    // until the schedule lands, mirroring emit()'s reliable-offer discipline: a dropped reschedule would
    // stop the cluster clock. Deadlines are in the cluster's time unit (milliseconds — the Aeron default,
    // not overridden in SequencerNode), matching cluster.time().
    private void scheduleTick() {
        final long deadline = cluster.time() + TICK_INTERVAL_MS;
        while (!cluster.scheduleTimer(TICK_TIMER_CORRELATION_ID, deadline)) {
            cluster.idleStrategy().idle();
        }
    }

    // ── Snapshot ──────────────────────────────────────────────────────────────

    @Override
    public void onTakeSnapshot(final ExclusivePublication snapshotPublication) {
        snapshotBuffer.putLong(0, sequencer.globalSeqNo());
        long offerResult;
        do {
            offerResult = snapshotPublication.offer(snapshotBuffer, 0, Long.BYTES);
            if (offerResult == ExclusivePublication.CLOSED
                || offerResult == ExclusivePublication.MAX_POSITION_EXCEEDED) {
                throw new IllegalStateException("[SequencerService] Snapshot publication failed: " + offerResult);
            }
            if (offerResult < 0) {
                cluster.idleStrategy().idle();
            }
        } while (offerResult < 0);
    }

    private void loadSnapshot(final Image snapshotImage) {
        final FragmentAssembler handler =
            new FragmentAssembler((buf, off, len, hdr) -> sequencer.globalSeqNo(buf.getLong(off)));
        while (!snapshotImage.isClosed()) {
            cluster.idleStrategy().idle(snapshotImage.poll(handler, SNAPSHOT_POLL_BATCH));
        }
    }

    // ── Leadership ────────────────────────────────────────────────────────────

    @Override
    public void onNewLeadershipTermEvent(final long logPosition, final long leadershipTermId, final long timestamp,
                                         final long termBaseLogPosition, final int leaderMemberId,
                                         final int logSessionId, final TimeUnit timeUnit, final int appVersion) {
        applyLeadership(leaderMemberId, timestamp);
        // Arm (or re-arm) the internal cluster clock here rather than in onStart, where Aeron forbids
        // scheduling timers. Fires on every node when a term begins (cold start and every failover);
        // scheduleTick is idempotent by correlation id, and the timer then re-arms itself in
        // onTimerEvent, so the clock runs continuously across leadership changes.
        scheduleTick();
    }

    @Override
    public void onRoleChange(final Cluster.Role newRole) {
        // Deliberately a no-op: onNewLeadershipTermEvent already fires on every node (leader and
        // followers alike) with an explicit leaderMemberId, which is what applyLeadership needs to stamp
        // newLeaderMemberId onto the LeadershipChanged event. Keying everything off one authoritative
        // event (rather than also reacting here) removes a class of double-firing/idempotency bugs.
    }

    // Called on every node whenever a new leadership term begins (including this node's own promotion).
    // With every node recording its own tap there is no leader-only publication or standby-follow to
    // manage here any more (both retired in the tap-recording change) — the only per-leadership work is
    // synthesizing a LeadershipChanged event (ReplayerService design §3). Every node consumes
    // onNewLeadershipTermEvent in the same log order, so ++globalSeqNo here (on every node, exactly like
    // onSessionOpen/onSessionMessage) keeps the counter identical across nodes, and each node stamps that
    // same globalSeqNo onto a LeadershipChanged it emits onto its own tap — the per-node replicas use it
    // to track the current leader at one exact point in the ordered stream.
    private void applyLeadership(final int leaderMemberId, final long timestamp) {
        // Encoded and emitted on every node, so each node's replayer (and its recording) carries this
        // globalSeqNo gap-free. The sequencer suppresses a repeat of the leader already on record.
        final int length = sequencer.leadershipChanged(leaderMemberId, timestamp);
        if (length == Sequencer.NO_FRAME) {
            return;
        }
        final boolean leader = leaderMemberId == cluster.memberId();
        System.out.printf("[SequencerService/%d] leadership change: new leader is memberId=%d (isLeader=%b)%n",
                          cluster.memberId(), leaderMemberId, leader);
        emit(length);
    }

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
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    // Publishes the frame in encodeBuffer[0, length) onto the node-local tap, which every node records
    // into its own local archive as the authoritative sequenced history. Reliable: spins until the offer
    // lands, because a dropped frame would be an unrecoverable hole in the recording. Unlike the retired
    // UDP global stream this cannot wedge structurally — the only tethered subscriber of the tap is the
    // co-located archive recording (the app replicas' tap subscriptions are untethered, so a slow app is
    // dropped, not back-pressuring), so this blocks only on real local-archive write back-pressure, which
    // clears as the archive drains to disk.
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
                idleSpins = 0;
            }
            cluster.idleStrategy().idle();
        }
    }

}
