package org.limitless.phixeron.replayer;

import static io.aeron.Aeron.NULL_VALUE;

import io.aeron.Aeron;
import io.aeron.Counter;
import io.aeron.ExclusivePublication;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.logbuffer.FragmentHandler;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.limitless.phixeron.PhixeronCounters;
import org.limitless.phixeron.sbe.unsequenced.MessageHeaderDecoder;
import org.limitless.phixeron.sbe.unsequenced.MessageHeaderEncoder;
import org.limitless.phixeron.sbe.unsequenced.ReplayCompleteDecoder;
import org.limitless.phixeron.sbe.unsequenced.ReplayPendingEncoder;
import org.limitless.phixeron.sbe.unsequenced.ReplayRequestDecoder;
import org.limitless.phixeron.sbe.unsequenced.ReplayingEncoder;
import org.limitless.phixeron.sequencer.SequencerService;
import org.limitless.phixeron.util.Logger;

/**
 * Per-node archive <b>replay server</b> for co-located application replicas (ReplayerService design,
 * {@code doc/router-design.md}). It is deliberately <em>not</em> on the live delivery path: every
 * app reads the co-located {@code SequencerService}'s node-local IPC tap ({@link
 * SequencerService#FEEDER_CHANNEL} / {@link SequencerService#FEEDER_STREAM_ID}) <b>directly</b> for the
 * live feed, so the sequencer has no live network data subscribers (the UDP multi-destination-cast
 * global stream is retired) and audit.md S4 (sequencer liveness coupled to its slowest consumer)
 * dissolves structurally. The apps' tap subscriptions are untethered, so a slow app is dropped (and
 * heals via the replay protocol below) rather than back-pressuring the sequencer.
 *
 * <p>This process exists only so that <b>one</b> reader per node touches the archive on behalf of
 * every co-located replica (design §0/§5): N apps do not each open an archive control session and
 * run their own replays. Its whole job is the on-demand replay protocol, all single-threaded on the
 * duty-cycle thread:
 * <ul>
 *   <li><b>Request</b> ({@link #REQUEST_STREAM_ID}) — an app that is cold-starting, or that detects a
 *       {@code globalSeqNo} gap on the live tap, sends {@code ReplayRequest(clientId, segmentIndex,
 *       fromPosition)}.
 *   <li><b>Control</b> ({@link #CONTROL_STREAM_ID}) — the ReplayerService answers {@code
 *       Replaying(clientId, replaySessionId, catchUpPosition)} or {@code ReplayPending(clientId)}.
 *   <li><b>Replay</b> ({@link #REPLAY_STREAM_ID}) — it serves at most {@link #MAX_CONCURRENT_REPLAYS}
 *       archive replays at once onto this {@code aeron:ipc} stream; the requesting app attaches to the
 *       replay image by session id.
 * </ul>
 *
 * <p><b>Replay is bounded to the current recording tip, not open-ended.</b> This is a deliberate
 * divergence from the design's literal "open-ended replay that becomes the live feed": a bounded
 * replay <em>ends by itself</em> once it reaches the tip, at which point the requesting app switches
 * to the live tap (de-duping the seam by {@code globalSeqNo}) — the exact "replay reaches its tip →
 * live sub" handoff {@code ClusterStreamClient} already implements, with the tap playing the role of
 * the live sub. The app detects that tip by <em>position</em> (the {@code catchUpPosition} carried in
 * {@link ReplayingEncoder}), not by the replay image closing: a bounded replay of an <em>active</em>
 * recording does not close its image at the bound. That needs no {@code globalSeqNo→position} index,
 * and any residual gap after the handoff is healed by the same gap-detect → re-request loop (design
 * §5).
 *
 * <p><b>Strictly stateless</b> (design §6): the ReplayerService holds nothing not re-derivable from the
 * archive — only the ephemeral in-flight replay slots. A crash is a fast reconnect; apps treat
 * "ReplayerService gone" as they treat a gap and re-request on its return.
 *
 * <p><b>Startup integrity check.</b> Before ever declaring readiness, {@link #checkReady} verifies the
 * oldest tap recording's first frame is actually {@code globalSeqNo} 1 (see {@link
 * #peekFirstGlobalSeqNo}), not just that some recording exists. This should always hold — {@code
 * SequencerService} arms and confirms its recording before it can emit a single frame, and this
 * project's cluster membership is static, never joining mid-history — so a failure here means this
 * node's own recording has been deleted, corrupted, or partially restored: a broken node. {@code
 * ready} then never becomes true for this process's lifetime ({@link PhixeronCounters#REPLAYER_INTEGRITY_FAILURE_TYPE_ID}
 * latches instead), refusing once at the source rather than leaving every consumer that would ask this
 * node for history to independently hit the same wall.
 *
 * <p><b>Local-archive resilience.</b> The ReplayerService is off the live path entirely, so a transient
 * failure of the node's local archive degrades only history/gap <em>replay</em> — steady-state
 * delivery keeps flowing over the tap the apps read directly. It does not crash the ReplayerService either:
 * an archive control call that throws in the replay path flips it to a STALLED state and keeps the
 * duty cycle running, paces its replay retries, and answers any replay request with {@code
 * ReplayPending} — which the app already treats as "hold at the gap and re-request" — until the
 * archive returns (doc/router-archive.md).
 *
 * <p><b>Cross-failover replay.</b> A cold-starting app walks the recording chain ({@link
 * #resolveSegments}, the same oldest-first stitching {@code ClusterStreamClient} uses). With every node
 * recording its own continuous tap this is normally a single recording spanning every leader failover,
 * so the walk sees full history rather than only the currently-active tenure. Steady-state gap recovery
 * resumes the active recording at a position ({@code ReplayRequest.segmentIndex < 0}); see {@link
 * #startReplayForClient}.
 *
 * <p><b>Slot reclamation.</b> A slot frees as soon as its app is done with it — a cold-start walk
 * supersedes each slot on its next segment request (and frees the last via its walk-terminating
 * request), and a steady-state gap resume ends with a {@code ReplayComplete} message — and a freed
 * slot is handed to any waiting app immediately ({@link #drainPending} on release/termination), not
 * at the next sweep. The idle-TTL ({@link #REPLAY_SLOT_TTL_MS}) is now only a backstop for an app
 * that died or lost its release, not the primary path (design §5's untether/timeout is a stronger
 * version of the same idea).
 *
 * <p><b>Slice scope.</b> The shared bootstrap replay for many co-starting replicas (design §4/§8) is
 * a documented follow-up — each app still gets its own replay.
 */
public final class ReplayerService {
    /** Node-local IPC channel every ReplayerService↔app stream runs over. */
    public static final String IPC_CHANNEL = "aeron:ipc";

    /** ReplayerService → apps: on-demand archive replays (one Aeron session per in-flight replay). */
    public static final int REPLAY_STREAM_ID = 201;

    /** Apps → ReplayerService: {@code ReplayRequest}. */
    public static final int REQUEST_STREAM_ID = 202;

    /** ReplayerService → apps: {@code Replaying} / {@code ReplayPending}. */
    public static final int CONTROL_STREAM_ID = 203;

    /**
     * Internal-only IPC stream {@link #peekFirstGlobalSeqNo} replays onto to read back the oldest tap
     * recording's first frame at startup (see {@link #checkReady}). Never used by any app-facing
     * protocol — distinct from {@link #REPLAY_STREAM_ID} purely so this one-shot self-check can never
     * cross-talk with a real client replay.
     */
    private static final int SELF_CHECK_STREAM_ID = 204;

    /**
     * Bounds {@link #peekFirstGlobalSeqNo}'s wait for the self-check replay's first fragment. The
     * replay is a few bytes over local IPC, so this is generous headroom, not an expected duration;
     * a miss just means {@link #checkReady} retries on the next {@link #poll()} cycle.
     */
    private static final long SELF_CHECK_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(2);

    /**
     * Archive-IO parallelism cap on concurrent replays (design §4/§8) — not a fairness knob. The
     * only multi-replay event that matters is node start/restart, and the node emits nothing until
     * every replica is caught up (the readiness barrier), so this is a makespan bound, not a
     * starvation one.
     */
    private static final int MAX_CONCURRENT_REPLAYS = 2;

    /**
     * A {@code Replaying.replaySessionId} of this value means "you are already at the tip; there is
     * nothing to replay — just follow the live tap." Sent instead of starting a zero-length replay.
     */
    public static final long NO_REPLAY_NEEDED = NULL_VALUE;

    /** Idle-TTL slot reclamation (see class Javadoc): a slot untouched this long is reclaimed. */
    private static final long REPLAY_SLOT_TTL_MS = 60_000;

    /**
     * While STALLED, probe the local archive (for replay) at most this often, so a dead archive is not
     * hammered every duty cycle.
     */
    private static final long STALL_RETRY_INTERVAL_MS = 1_000;

    private static final int FRAGMENT_LIMIT = 16;

    private final Aeron aeron;
    private final AeronArchive archive;
    private final int memberId;
    private final IdleStrategy idleStrategy;

    // ── ReplayerService → apps control + apps → ReplayerService requests ────────────────────
    private final ExclusivePublication controlPub;
    private final Subscription requestSub;

    // Proactive readiness marker: set once the co-located SequencerService's tap recording is visible
    // on the local archive AND has passed the startup integrity check (see checkReady). The launch
    // scripts wait on the readiness log before starting apps.
    private boolean ready = false;

    // Latched once the oldest tap recording's first frame fails the gseq-1 integrity check (see
    // checkReady/peekFirstGlobalSeqNo): this node's own recording doesn't reach the start of the log
    // (deleted, corrupted, or a partial restore), so there is no valid history to serve. `ready` must
    // never become true once this is set — every consumer that would otherwise ask this node for
    // history would independently hit the same wall, so it fails here instead, once, loudly.
    private boolean integrityFailed = false;

    // ── Replay protocol state ─────────────────────────────────────────────────
    // Admission control and pending-queue bookkeeping is a pure function of client ids/tokens (see
    // ReplaySlotAllocator's Javadoc) — split out so it's unit-testable without an archive.
    private final ReplaySlotAllocator replaySlots = new ReplaySlotAllocator(MAX_CONCURRENT_REPLAYS, REPLAY_SLOT_TTL_MS);

    // Local-archive resilience (doc/router-archive.md): a transient local-archive failure must not kill
    // the duty-cycle thread. The ReplayerService is off the live path (apps read the tap directly), so a stall
    // only affects replay. While STALLED it keeps its duty cycle running, paces its replay retries, and
    // holds replay-requesting apps with ReplayPending until the archive returns. Surfaced via
    // stalledCounter (PhixeronCounters.REPLAYER_STALLED_TYPE_ID).
    private boolean stalled = false;
    private long lastStallRetryMs = 0;

    // ── Operator counters (see PhixeronCounters), created in the constructor ────────────────────
    private final Counter stalledCounter;
    private final Counter readyCounter;
    private final Counter activeReplaySlotsCounter;
    private final Counter pendingRequestsCounter;
    private final Counter replaysServedCounter;
    private final Counter idleTtlReclaimedCounter;
    private final Counter integrityFailureCounter;

    private final MessageHeaderDecoder inHeaderDecoder = new MessageHeaderDecoder();
    private final ReplayRequestDecoder replayRequestDecoder = new ReplayRequestDecoder();
    private final ReplayCompleteDecoder replayCompleteDecoder = new ReplayCompleteDecoder();
    private final MessageHeaderEncoder outHeaderEncoder = new MessageHeaderEncoder();
    private final ReplayingEncoder replayingEncoder = new ReplayingEncoder();
    private final ReplayPendingEncoder pendingEncoder = new ReplayPendingEncoder();
    private final MutableDirectBuffer controlBuffer = new ExpandableArrayBuffer(64);

    // Decode the sequenced (not unsequenced) schema's outer header + header composite — the tap
    // recording's own on-wire format — used only by peekFirstGlobalSeqNo. Fully qualified at the point
    // of use instead of imported: the simple names MessageHeaderDecoder/HeaderDecoder are already taken
    // by this class's own unsequenced-schema request/control protocol decoders above.
    private final org.limitless.phixeron.sbe.sequenced.MessageHeaderDecoder selfCheckMsgHeaderDecoder =
        new org.limitless.phixeron.sbe.sequenced.MessageHeaderDecoder();
    private final org.limitless.phixeron.sbe.sequenced.HeaderDecoder selfCheckHeaderDecoder =
        new org.limitless.phixeron.sbe.sequenced.HeaderDecoder();

    private final FragmentHandler requestHandler =
        (buffer, offset, length, header) -> onRequest(buffer, offset, length);

    public ReplayerService(final Aeron aeron, final AeronArchive archive, final int memberId,
                           final IdleStrategy idleStrategy) {
        this.aeron = aeron;
        this.archive = archive;
        this.memberId = memberId;
        this.idleStrategy = idleStrategy;

        this.controlPub = aeron.addExclusivePublication(IPC_CHANNEL, CONTROL_STREAM_ID);
        this.requestSub = aeron.addSubscription(IPC_CHANNEL, REQUEST_STREAM_ID);

        this.stalledCounter = PhixeronCounters.addCounter(aeron,
            PhixeronCounters.REPLAYER_STALLED_TYPE_ID, "phixeron.replayer.stalled member=" + memberId, memberId);
        this.readyCounter = PhixeronCounters.addCounter(aeron,
            PhixeronCounters.REPLAYER_READY_TYPE_ID, "phixeron.replayer.ready member=" + memberId, memberId);
        this.activeReplaySlotsCounter = PhixeronCounters.addCounter(aeron,
            PhixeronCounters.REPLAYER_ACTIVE_SLOTS_TYPE_ID, "phixeron.replayer.activeSlots member=" + memberId, memberId);
        this.pendingRequestsCounter = PhixeronCounters.addCounter(aeron,
            PhixeronCounters.REPLAYER_PENDING_REQUESTS_TYPE_ID, "phixeron.replayer.pendingRequests member=" + memberId, memberId);
        this.replaysServedCounter = PhixeronCounters.addCounter(aeron,
            PhixeronCounters.REPLAYER_REPLAYS_SERVED_COUNT_TYPE_ID,
            "phixeron.replayer.replaysServedCount member=" + memberId, memberId);
        this.idleTtlReclaimedCounter = PhixeronCounters.addCounter(aeron, PhixeronCounters.REPLAYER_IDLE_TTL_RECLAIMED_COUNT_TYPE_ID,
            "phixeron.replayer.idleTtlReclaimedCount member=" + memberId, memberId);
        this.integrityFailureCounter = PhixeronCounters.addCounter(aeron, PhixeronCounters.REPLAYER_INTEGRITY_FAILURE_TYPE_ID,
            "phixeron.replayer.integrityFailure member=" + memberId, memberId);
    }

    /**
     * Blocks running the duty cycle until {@code running} goes false. Serves the replay protocol from
     * the local archive; the live feed is the tap the apps read directly, not this process.
     * @param running true while running
     */
    public void run(final AtomicBoolean running) {
        Logger.info(Logger.Component.ReplayerService, memberId,
                "starting; serving replay from the co-located archive…");
        while (running.get()) {
            final int work = poll();
            idleStrategy.idle(work);
        }
        Logger.info(Logger.Component.ReplayerService, memberId, "shutting down");
        stopAllReplays();
    }

    /** One duty-cycle iteration. Returns a work count for the idle strategy. */
    public int poll() {
        if (!ready) {
            checkReady();
        }
        final int work = requestSub.poll(requestHandler, FRAGMENT_LIMIT);
        reclaimIdleSlots();
        activeReplaySlotsCounter.set(replaySlots.activeCount());
        pendingRequestsCounter.set(replaySlots.pendingCount());
        return work;
    }

    /**
     * Waits for the tap recording to be visible, then verifies its history actually reaches back to
     * the start of the log before ever declaring readiness. Under correct operation this always holds
     * (SequencerService arms and confirms the recording before it can emit a single frame — see its
     * onStart/awaitTapRecordingActive — and this node's own static, fixed cluster membership never
     * joins mid-history), so a failure here means this node's oldest tap recording has been deleted,
     * corrupted, or partially restored out from under it: a broken node, not a transient condition.
     * That is a permanent state (retrying reads the same on-disk bytes), so once {@link
     * #integrityFailed} latches, {@code ready} must never become true for this process's lifetime —
     * every consumer that would otherwise ask this node for history and independently hit the same
     * wall is better served by this one node-level refusal than by each of them failing on their own.
     */
    private void checkReady() {
        if (integrityFailed) {
            return;
        }
        final long recordingId;
        try {
            recordingId = findActiveRecordingId();
        } catch (final RuntimeException ex) {
            return;  // archive not answering yet; retry next cycle (scripts time out and proceed)
        }
        if (recordingId == NULL_VALUE) {
            return;  // nothing recorded yet; retry next cycle
        }

        final List<Long> segments = resolveSegments();
        if (segments.isEmpty()) {
            return;  // retry next cycle
        }
        final long oldestRecordingId = segments.get(0);
        final Long firstGlobalSeqNo;
        try {
            firstGlobalSeqNo = peekFirstGlobalSeqNo(oldestRecordingId);
        } catch (final RuntimeException ex) {
            return;  // archive not answering yet; retry next cycle
        }
        if (firstGlobalSeqNo == null) {
            return;  // oldest recording has nothing written yet; retry next cycle
        }
        if (firstGlobalSeqNo != 1L) {
            integrityFailed = true;
            integrityFailureCounter.set(1);
            Logger.fault(Logger.Component.ReplayerService, Logger.EventCode.ArchiveIntegrityFailure, memberId,
                    "FATAL: oldest tap recording %d's first frame has globalSeqNo=%d, expected "
                            + "1 — this node's recording does not reach the start of the log (deleted, corrupted, or a "
                            + "partial restore?); refusing to mark ready", oldestRecordingId, firstGlobalSeqNo);
            return;
        }

        ready = true;
        readyCounter.set(1);
        Logger.info(Logger.Component.ReplayerService, memberId, "ready — tap recording %d live; serving replay",
                recordingId);
    }

    /**
     * Replays just the first frame of {@code recordingId} (position 0) and returns its globalSeqNo.
     * Replay is the only way to read recorded content back, so this opens a short-lived one on the
     * internal {@link #SELF_CHECK_STREAM_ID}, reads one fragment, and tears both down.
     * @param recordingId the recording to peek (the oldest tap recording — see checkReady)
     * @return the first frame's globalSeqNo, or null if nothing is written yet ({@code tip <= 0}) or
     *         the replay didn't deliver within {@link #SELF_CHECK_TIMEOUT_NS} (transient — checkReady
     *         retries on the next poll() cycle rather than treating a null as failure)
     */
    private Long peekFirstGlobalSeqNo(final long recordingId) {
        long tip = archive.getRecordingPosition(recordingId);
        if (tip < 0) {
            tip = archive.getStopPosition(recordingId);
        }
        if (tip <= 0) {
            return null;
        }

        final long replaySessionId = archive.startReplay(recordingId, 0, tip, IPC_CHANNEL, SELF_CHECK_STREAM_ID);
        try (Subscription sub = aeron.addSubscription(IPC_CHANNEL, SELF_CHECK_STREAM_ID)) {
            final long[] globalSeqNo = {NULL_VALUE};
            final FragmentHandler handler = (buffer, offset, length, header) -> {
                selfCheckMsgHeaderDecoder.wrap(buffer, offset);
                final int bodyOffset =
                    offset + org.limitless.phixeron.sbe.sequenced.MessageHeaderDecoder.ENCODED_LENGTH;
                selfCheckHeaderDecoder.wrap(buffer, bodyOffset);
                globalSeqNo[0] = selfCheckHeaderDecoder.globalSeqNo();
            };
            final long deadlineNs = System.nanoTime() + SELF_CHECK_TIMEOUT_NS;
            while (globalSeqNo[0] == NULL_VALUE && System.nanoTime() < deadlineNs) {
                if (sub.poll(handler, 1) == 0) {
                    idleStrategy.idle();
                }
            }
            return globalSeqNo[0] == NULL_VALUE ? null : globalSeqNo[0];
        } finally {
            stopReplay(replaySessionId);
        }
    }

    // ── Replay protocol ─────────────────────────────────────────────────────────

    /**
     * Request handler
     * @param buffer message buffer
     * @param offset buffer offset
     * @param length message length
     */
    private void onRequest(final DirectBuffer buffer, final int offset, final int length) {
        inHeaderDecoder.wrap(buffer, offset);
        if (inHeaderDecoder.schemaId() != ReplayRequestDecoder.SCHEMA_ID) {
            return;
        }

        // ReplayComplete: the app caught up and is now on the live tap. Free its slot immediately and
        // hand it to a waiting app rather than leaving it to the idle-TTL (design §5). No reply.
        if (inHeaderDecoder.templateId() == ReplayCompleteDecoder.TEMPLATE_ID) {
            replayCompleteDecoder.wrap(buffer, offset + MessageHeaderDecoder.ENCODED_LENGTH,
                                       inHeaderDecoder.blockLength(), inHeaderDecoder.version());
            stopReplayForClient(replayCompleteDecoder.clientId());
            drainPending();
            return;
        }
        if (inHeaderDecoder.templateId() != ReplayRequestDecoder.TEMPLATE_ID) {
            return;
        }
        replayRequestDecoder.wrap(buffer, offset + MessageHeaderDecoder.ENCODED_LENGTH, inHeaderDecoder.blockLength(),
                                  inHeaderDecoder.version());
        final int clientId = replayRequestDecoder.clientId();
        final long fromPosition = replayRequestDecoder.fromPosition();
        final int segmentIndex = replayRequestDecoder.segmentIndex();

        // Supersede any in-flight replay for this client (it re-requested — a new gap position or the
        // next segment of its cold-start walk).
        stopReplayForClient(clientId);

        if (!replaySlots.hasCapacity()) {
            replaySlots.enqueue(clientId, segmentIndex, fromPosition);
            sendPending(clientId);
            Logger.info(Logger.Component.ReplayerService, memberId,
                    "client %d queued: no free replay slot (active=%d/%d, pending=%d)", clientId,
                    replaySlots.activeCount(), MAX_CONCURRENT_REPLAYS, replaySlots.pendingCount());
            return;
        }
        startReplayForClient(clientId, segmentIndex, fromPosition);
        // A walk-terminating request (segmentIndex past the chain) frees this client's slot via the
        // supersede above without taking a new one; hand that freed slot to a waiting app now rather
        // than at the next idle-TTL sweep (design §5).
        drainPending();
    }

    /**
     * Serves one replay request, guarding every archive control call.
     * @param clientId client identity
     * @param segmentIndex segment index
     * @param fromPosition start replay position
     */
    private void startReplayForClient(final int clientId, final int segmentIndex, final long fromPosition) {
        if (stalled) {
            final long now = System.currentTimeMillis();
            if ((now - lastStallRetryMs) < STALL_RETRY_INTERVAL_MS) {
                sendPending(clientId);
                return;
            }
            lastStallRetryMs = now;  // this attempt is the paced probe
        }
        try {
            serveReplay(clientId, segmentIndex, fromPosition);
            if (stalled) {
                stalled = false;
                stalledCounter.set(0);
                Logger.info(Logger.Component.ReplayerService, memberId,
                        "RECOVERED: local archive reachable again");
            }
        } catch (final RuntimeException error) {
            onArchiveStalled("serving replay for client " + clientId, error);
            sendPending(clientId);
        }
    }

    /**
     * Serves one replay to a client. segmentIndex < 0 resumes the current active recording at
     * fromPosition (steady-state gap recovery); segmentIndex >= 0 is one step of a cold-start walk over
     * the per-leader-tenure recording chain — serving the segmentIndex-th recording from position 0, or
     * NO_REPLAY_NEEDED once the walk runs past the last tenure (which is what marks the app caught up).
     * @param clientId client identity
     * @param segmentIndex segment index
     * @param fromPosition start position
     */
    private void serveReplay(final int clientId, final int segmentIndex, final long fromPosition) {
        final long recordingId;
        final long replayFrom;
        if (segmentIndex < 0) {
            recordingId = findActiveRecordingId();
            replayFrom = fromPosition;
            if (recordingId == NULL_VALUE) {
                // No recording to replay from yet; ask the app to hold and retry.
                replaySlots.enqueue(clientId, segmentIndex, fromPosition);
                sendPending(clientId);
                return;
            }
        } else {
            final List<Long> segments = resolveSegments();
            if (segments.isEmpty()) {
                // No tap recording on the local archive yet; hold and retry.
                replaySlots.enqueue(clientId, segmentIndex, fromPosition);
                sendPending(clientId);
                return;
            }
            if (segmentIndex >= segments.size()) {
                // Walked past the last tenure: the app has replayed all history and is at the live tip.
                sendReplaying(clientId, NO_REPLAY_NEEDED, 0);
                return;
            }
            recordingId = segments.get(segmentIndex);
            replayFrom = 0;
        }

        long tip = archive.getRecordingPosition(recordingId);
        if (tip < 0) {
            tip = archive.getStopPosition(recordingId);
        }
        final long boundedLength = tip - replayFrom;
        if (boundedLength <= 0) {
            // Already at (or past) the tip — nothing historical to serve. Tell the app to just
            // follow the live tap; no slot consumed.
            sendReplaying(clientId, NO_REPLAY_NEEDED, tip);
            return;
        }

        final long replaySessionId = archive.startReplay(recordingId, replayFrom, boundedLength, IPC_CHANNEL,
            REPLAY_STREAM_ID);
        replaysServedCounter.increment();
        replaySlots.activate(clientId, replaySessionId, System.currentTimeMillis());
        Logger.info(Logger.Component.ReplayerService, memberId,
                "replay for client %d: segment %d recording %d [%d,%d) session %d", clientId,
                segmentIndex, recordingId, replayFrom, tip, replaySessionId);
        // catchUpPosition = tip: the app follows the replay image until it reaches this, then advances
        // (next segment, or the live tap). A bounded replay of an active recording does not close its
        // image at the bound, so the app detects completion by position (see Replaying / ReplayerClient).
        sendReplaying(clientId, replaySessionId, tip);
    }

    /**
     * Stops an active replay
     * @param clientId client identity
     */
    private void stopReplayForClient(final int clientId) {
        final long token = replaySlots.supersede(clientId);
        if (token != ReplaySlotAllocator.NO_SLOT) {
            stopReplay(token);
        }
    }

    /**
     * Reclaims unused replay slots.
     */
    private void reclaimIdleSlots() {
        final List<Long> reclaimed = replaySlots.reclaimIdle(System.currentTimeMillis());
        for (final long token : reclaimed) {
            stopReplay(token);
            idleTtlReclaimedCounter.increment();
        }
        if (!reclaimed.isEmpty()) {
            drainPending();
        }
    }

    /**
     * Drain pending requests
     */
    private void drainPending() {
        // Bounded to the current queue length: startReplayForClient may re-queue a request (e.g. no
        // recording yet), so retry each waiting request at most once per call rather than spinning on
        // one that cannot yet make progress.
        int budget = replaySlots.pendingCount();
        while (budget-- > 0) {
            final ReplaySlotAllocator.PendingRequest req = replaySlots.pollPending();
            if (req == null) {
                break;
            }
            startReplayForClient(req.clientId(), req.segmentIndex(), req.fromPosition());
        }
    }

    /**
     * Stop a reply
     * @param replaySessionId replay session identity
     */
    private void stopReplay(final long replaySessionId) {
        try {
            archive.stopReplay(replaySessionId);
        } catch (final RuntimeException ex) {
            // Bounded replays end on their own, so the session may already be gone — harmless.
        }
    }

    /**
     * Stop all replays
     */
    private void stopAllReplays() {
        for (final long token : replaySlots.clear()) {
            stopReplay(token);
        }
    }

    /**
     * Send replaying message
     * @param clientId client identity
     * @param replaySessionId replay session identity
     * @param catchUpPosition catchup positon
     */
    private void sendReplaying(final int clientId, final long replaySessionId, final long catchUpPosition) {
        replayingEncoder.wrapAndApplyHeader(controlBuffer, 0, outHeaderEncoder)
            .clientId(clientId)
            .replaySessionId(replaySessionId)
            .catchUpPosition(catchUpPosition);
        offerControl(MessageHeaderEncoder.ENCODED_LENGTH + replayingEncoder.encodedLength());
    }

    /**
     * Send pending message
     * @param clientId
     */
    private void sendPending(final int clientId) {
        pendingEncoder.wrapAndApplyHeader(controlBuffer, 0, outHeaderEncoder).clientId(clientId);
        offerControl(MessageHeaderEncoder.ENCODED_LENGTH + pendingEncoder.encodedLength());
    }

    /**
     * Offer message limit
     * @param length limit
     */
    private void offerControl(final int length) {
        long result;
        int spins = 0;
        while ((result = controlPub.offer(controlBuffer, 0, length)) < 0) {
            if (result == ExclusivePublication.CLOSED || result == ExclusivePublication.MAX_POSITION_EXCEEDED) {
                throw new IllegalStateException("[ReplayerService] control publication failed: " + result);
            }
            if (result == ExclusivePublication.NOT_CONNECTED && ++spins > 1000) {
                return;  // no app listening for control replies; give up on this one
            }
            idleStrategy.idle();
        }
    }

    // ── Local-archive resilience ────────────────────────────────────────────────

    /**
     * A local-archive control call threw: enter STALLED (idempotently, logging once per episode).
     * @param message error text
     * @param exception runtime exception
     */
    private void onArchiveStalled(final String message, final RuntimeException exception) {
        if (!stalled) {
            stalled = true;
            stalledCounter.set(1);
            Logger.info(Logger.Component.ReplayerService, memberId,
                    "STALLED: %s — local archive unreachable (%s); live delivery unaffected "
                            + "(apps read the tap directly), retrying replay", message, exception.getMessage());
        }
    }

    // Finds the currently-active tap recording on the local archive (stopTimestamp unset). Normally
    // there is exactly one (each node records its own continuous tap); a member restart can leave an
    // earlier, stopped recording alongside it, and this returns the active one.
    private long findActiveRecordingId() {
        final long[] found = {NULL_VALUE};
        archive.listRecordingsForUri(0, Integer.MAX_VALUE, "", SequencerService.FEEDER_STREAM_ID,
                                     (controlSessionId, correlationId, recordingId, startTimestamp, stopTimestamp,
                                      startPosition, stopPosition, initialTermId, segmentFileLength, termBufferLength,
                                      mtuLength, sessionId, streamId, strippedChannel, originalChannel,
                                      sourceIdentity) -> {
                                         if (stopTimestamp == AeronArchive.NULL_TIMESTAMP) {
                                             found[0] = recordingId;
                                         }
                                     });
        return found[0];
    }

    // Ordered oldest→newest list of tap recordings on the local archive. With every node recording its
    // own continuous tap this is normally a single recording spanning every leader tenure, so there is usually nothing
    // to stitch. A member restart can leave an earlier, stopped recording plus the post-restart one (overlapping
    // globalSeqNo ranges); a cold-starting app replays them in order and de-duplicates by globalSeqNo, so the overlap
    // is harmless. Mirrors ClusterStreamClient.resolveClusterStreamSegments.
    private List<Long> resolveSegments() {
        final List<ReplayChain.RecordingSpan> spans = new ArrayList<>();
        archive.listRecordingsForUri(0, Integer.MAX_VALUE, "", SequencerService.FEEDER_STREAM_ID,
                                     (controlSessionId, correlationId, recordingId, startTimestamp, stopTimestamp,
                                      startPosition, stopPosition, initialTermId, segmentFileLength, termBufferLength,
                                      mtuLength, sessionId, streamId, strippedChannel, originalChannel,
                                      sourceIdentity) -> spans.add(new ReplayChain.RecordingSpan(recordingId,
                                          startTimestamp, stopTimestamp == AeronArchive.NULL_TIMESTAMP)));
        return ReplayChain.stitch(spans);
    }
}
