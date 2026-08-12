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
import org.limitless.phixeron.sbe.unsequenced.ReplayHeartbeatDecoder;
import org.limitless.phixeron.sbe.unsequenced.ReplayPendingEncoder;
import org.limitless.phixeron.sbe.unsequenced.ReplayRequestDecoder;
import org.limitless.phixeron.sbe.unsequenced.ReplayUnavailableEncoder;
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
 *       Untethered and bounded ({@link #MAX_CONTROL_OFFER_SPINS}) for the same reason the tap is: one
 *       thread answers every app here too, so a wedged app must not be able to hold it.
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
 * oldest tap recording's first frame is actually {@code globalSeqNo} 1 (see {@link #startSelfCheck} /
 * {@link #pollSelfCheck}, which spread that read across duty cycles rather than blocking one), not just
 * that some recording exists. This should always hold — {@code
 * SequencerService} arms and confirms its recording before it can emit a single frame, and this
 * project's cluster membership is static, never joining mid-history — so a failure here means this
 * node's own recording has been deleted, corrupted, or partially restored: a broken node. {@code
 * ready} then never becomes true for this process's lifetime ({@link PhixeronCounters#REPLAYER_INTEGRITY_FAILURE_TYPE_ID}
 * latches instead), and {@link #onRequest} answers every replay request {@code ReplayUnavailable} — the
 * refusal is what contains the broken archive here, rather than leaving every consumer that asks this
 * node for history to independently hit the same wall (and each app's wall is an abort on its own
 * first-frame-must-be-1 check, so one bad archive would take down all of them). Until the check has
 * passed, requests are answered {@code ReplayPending}: history this node has not proven good is not
 * served.
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
 * <p><b>Scope.</b> Shared bootstrap replay for many co-starting replicas is deliberately not
 * implemented — each app gets its own walk. See design §2.4 for why it was designed and rejected.
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
     * Internal-only IPC stream the startup self-check replays onto to read back the oldest tap
     * recording's first frame (see {@link #checkReady}). Never used by any app-facing
     * protocol — distinct from {@link #REPLAY_STREAM_ID} purely so this one-shot self-check can never
     * cross-talk with a real client replay.
     */
    private static final int SELF_CHECK_STREAM_ID = 204;

    /**
     * How long a self-check replay may go unanswered before it is abandoned and started over. Spread
     * across duty cycles, never spun on: the check reads at most one fragment per {@link #poll()}, so
     * this bounds an <em>elapsed</em> wait, not a blocked one. The replay is a few bytes over local
     * IPC, so it is generous headroom rather than an expected duration; expiring just means
     * {@link #checkReady} starts a fresh one.
     */
    private static final long SELF_CHECK_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(2);

    /**
     * Bounds the length the self-check asks the archive to replay. It reads exactly one fragment, so
     * this only has to cover the first frame — comfortably over a default MTU, and orders of magnitude
     * under the whole recording it used to request.
     */
    private static final long SELF_CHECK_REPLAY_LENGTH = 4096;

    /**
     * Archive-IO parallelism cap on concurrent replays (design §4/§8) — not a fairness knob. The
     * only multi-replay event that matters is node start/restart, and the node emits nothing until
     * every replica is caught up (the readiness barrier), so this is a makespan bound, not a
     * starvation one.
     */
    private static final int MAX_CONCURRENT_REPLAYS = 4;

    /**
     * A {@code Replaying.replaySessionId} of this value means "you are already at the tip; there is
     * nothing to replay — just follow the live tap." Sent instead of starting a zero-length replay.
     */
    public static final long NO_REPLAY_NEEDED = NULL_VALUE;

    /**
     * Idle-TTL slot reclamation (see class Javadoc): a slot untouched this long is reclaimed. Genuinely
     * an IDLE timeout — a client refreshes its slot with {@code ReplayHeartbeat} for as long as it is
     * riding the replay image, so this bounds how long a slot survives its client going away, not how
     * long a replay may legitimately take. It must not be the latter: the design mandates full-log
     * replay with no snapshots, so replay duration grows with the trading day and no fixed lifetime is
     * correct. Reclaiming mid-flight stops the archive replay under a healthy client.
     */
    private static final long REPLAY_SLOT_TTL_MS = 60_000;

    /**
     * While STALLED, probe the local archive (for replay) at most this often, so a dead archive is not
     * hammered every duty cycle.
     */
    private static final long STALL_RETRY_INTERVAL_MS = 1_000;

    /**
     * Bounds {@link #offerControl}'s retry spin, whatever the reason the offer failed. Every app is
     * answered from this one duty-cycle thread, so an app that stops draining the control stream must
     * not be able to hold it — that would couple every other app's replays to the slowest one, the
     * audit.md S4 pattern this design exists to dissolve, reintroduced on the control plane. A reply
     * that will not go out is dropped instead; the app's own resend timer is the retry. The apps
     * subscribe untethered, so this bound is a backstop for a burst, not the primary defence.
     */
    private static final int MAX_CONTROL_OFFER_SPINS = 1_000;

    /**
     * Window {@link ReplayClientIdCollisions} counts {@code requestId} regressions over. Wide enough
     * that two colliding clients — each resending on its own ~500ms timer — cross the threshold well
     * inside it, short enough that unrelated restarts spread over a trading day never accumulate.
     */
    private static final long CLIENT_ID_COLLISION_WINDOW_MS = 10_000;

    private static final int FRAGMENT_LIMIT = 16;

    private final Aeron aeron;
    private final AeronArchive archive;
    private final int memberId;
    private final IdleStrategy idleStrategy;

    /** Brings the whole process down; wired by {@link ReplayerNode}. See {@link #fatalDutyCycleFailure}. */
    private final Runnable fatalHandler;

    // ── ReplayerService → apps control + apps → ReplayerService requests ────────────────────
    private final ExclusivePublication controlPub;
    private final Subscription requestSub;

    // Proactive readiness marker: set once the co-located SequencerService's tap recording is visible
    // on the local archive AND has passed the startup integrity check (see checkReady). The launch
    // scripts wait on the readiness log before starting apps.
    private boolean ready = false;

    // Latched once the oldest tap recording's first frame fails the gseq-1 integrity check (see
    // checkReady/pollSelfCheck): this node's own recording doesn't reach the start of the log
    // (deleted, corrupted, or a partial restore), so there is no valid history to serve. `ready` must
    // never become true once this is set — every consumer that would otherwise ask this node for
    // history would independently hit the same wall, so it fails here instead, once, loudly.
    private boolean integrityFailed = false;

    // ── Startup integrity self-check (see checkReady) ──────────────────────────────────────────
    // The check spans duty cycles instead of blocking one: it starts a short replay, then reads it a
    // fragment at a time on later cycles until it answers or SELF_CHECK_TIMEOUT_NS elapses. Waiting
    // inside a single cycle starved every co-located app of even a ReplayPending for as long as the
    // wait — and the archive being slow to answer is exactly when they are asking.
    private Subscription selfCheckSub;
    private long selfCheckReplaySessionId = NULL_VALUE;
    private long selfCheckRecordingId = NULL_VALUE;      // the oldest recording being peeked
    private long selfCheckActiveRecordingId = NULL_VALUE;  // the live one, for the readiness line
    private long selfCheckGlobalSeqNo = NULL_VALUE;      // what the first fragment carried, once read
    private long selfCheckDeadlineNs = 0;

    // Latches once the ">1 active tap recording" anomaly has been reported (see resolveSegments): the
    // condition lasts as long as the stale recording is on disk, and resolveSegments runs per replay
    // request, so without this it would repeat the fault line on every one.
    private boolean staleActiveRecordingLogged = false;

    // Latches once a dropped control reply has been reported (see offerControl): a wedged app is
    // answered on every one of its resends, so without this it would repeat the fault line at the
    // resend rate. The counter keeps carrying the rate.
    private boolean controlReplyDropLogged = false;

    // ── Replay protocol state ─────────────────────────────────────────────────
    // Admission control and pending-queue bookkeeping is a pure function of client ids/tokens (see
    // ReplaySlotAllocator's Javadoc) — split out so it's unit-testable without an archive.
    private final ReplaySlotAllocator replaySlots = new ReplaySlotAllocator(MAX_CONCURRENT_REPLAYS, REPLAY_SLOT_TTL_MS);

    // Two co-located apps launched with the same PHIXERON_REPLAYER_CLIENT_ID supersede each other's
    // replays on every request and neither ever catches up. Also a pure function of what arrives on the
    // request stream, so it too is split out and unit-tested without an archive.
    private final ReplayClientIdCollisions clientIdCollisions =
        new ReplayClientIdCollisions(CLIENT_ID_COLLISION_WINDOW_MS);

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
    private final Counter controlRepliesDroppedCounter;
    private final Counter clientIdCollisionCounter;

    private final MessageHeaderDecoder inHeaderDecoder = new MessageHeaderDecoder();
    private final ReplayRequestDecoder replayRequestDecoder = new ReplayRequestDecoder();
    private final ReplayCompleteDecoder replayCompleteDecoder = new ReplayCompleteDecoder();
    private final ReplayHeartbeatDecoder replayHeartbeatDecoder = new ReplayHeartbeatDecoder();
    private final MessageHeaderEncoder outHeaderEncoder = new MessageHeaderEncoder();
    private final ReplayingEncoder replayingEncoder = new ReplayingEncoder();
    private final ReplayPendingEncoder pendingEncoder = new ReplayPendingEncoder();
    private final ReplayUnavailableEncoder unavailableEncoder = new ReplayUnavailableEncoder();
    private final MutableDirectBuffer controlBuffer = new ExpandableArrayBuffer(64);

    // Decode the sequenced (not unsequenced) schema's outer header + header composite — the tap
    // recording's own on-wire format — used only by the self-check. Fully qualified at the point
    // of use instead of imported: the simple names MessageHeaderDecoder/HeaderDecoder are already taken
    // by this class's own unsequenced-schema request/control protocol decoders above.
    private final org.limitless.phixeron.sbe.sequenced.MessageHeaderDecoder selfCheckMsgHeaderDecoder =
        new org.limitless.phixeron.sbe.sequenced.MessageHeaderDecoder();
    private final org.limitless.phixeron.sbe.sequenced.HeaderDecoder selfCheckHeaderDecoder =
        new org.limitless.phixeron.sbe.sequenced.HeaderDecoder();

    private final FragmentHandler requestHandler =
        (buffer, offset, length, header) -> onRequest(buffer, offset, length);

    private final FragmentHandler selfCheckHandler =
        (buffer, offset, length, header) -> onSelfCheckFragment(buffer, offset);

    /**
     * @param fatalHandler run once, from the duty-cycle thread, when the duty cycle dies on an uncaught
     *                     exception — see {@link #fatalDutyCycleFailure}. Must not block: it is expected
     *                     to signal a shutdown and return, not to perform one.
     */
    public ReplayerService(final Aeron aeron, final AeronArchive archive, final int memberId,
                           final IdleStrategy idleStrategy, final Runnable fatalHandler) {
        this.aeron = aeron;
        this.archive = archive;
        this.memberId = memberId;
        this.idleStrategy = idleStrategy;
        this.fatalHandler = fatalHandler;

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
        this.controlRepliesDroppedCounter = PhixeronCounters.addCounter(aeron,
            PhixeronCounters.REPLAYER_CONTROL_REPLIES_DROPPED_COUNT_TYPE_ID,
            "phixeron.replayer.controlRepliesDroppedCount member=" + memberId, memberId);
        this.clientIdCollisionCounter = PhixeronCounters.addCounter(aeron,
            PhixeronCounters.REPLAYER_CLIENT_ID_COLLISION_TYPE_ID,
            "phixeron.replayer.clientIdCollision member=" + memberId, memberId);
    }

    /**
     * Blocks running the duty cycle until {@code running} goes false. Serves the replay protocol from
     * the local archive; the live feed is the tap the apps read directly, not this process.
     * @param running true while running
     */
    public void run(final AtomicBoolean running) {
        Logger.info(Logger.Component.ReplayerService, memberId,
                "starting; serving replay from the co-located archive…");
        try {
            while (running.get()) {
                final int work = poll();
                idleStrategy.idle(work);
            }
        } catch (final RuntimeException ex) {
            fatalDutyCycleFailure(ex);
        }
        Logger.info(Logger.Component.ReplayerService, memberId, "shutting down");
        stopAllReplays();
        closeSelfCheck();  // a check still in flight owns an archive replay and a subscription
    }

    /**
     * The duty-cycle loop must never die silently: an uncaught exception here (e.g. {@link
     * #offerControl}'s CLOSED/MAX_POSITION_EXCEEDED) would otherwise unwind this thread while the process
     * keeps running with {@code ready} still latched at 1 — every co-located app would keep resending
     * into a ReplayerService that has stopped polling its requests, with nothing to tell it apart from a
     * merely slow one. Strictly stateless (class Javadoc): dying loudly and letting process supervision
     * restart costs nothing a fast reconnect and full-log replay does not already pay for.
     * @param ex what killed the duty cycle
     */
    private void fatalDutyCycleFailure(final RuntimeException ex) {
        ready = false;
        readyCounter.set(0);
        Logger.fault(Logger.Component.ReplayerService, Logger.EventCode.ReplayDutyCycleFailure, memberId,
                "FATAL: replay duty cycle terminated by an uncaught exception (%s) — clearing readiness "
                        + "and exiting; process supervision should restart this node", ex.getMessage());
        fatalHandler.run();
    }

    /** One duty-cycle iteration. Returns a work count for the idle strategy. */
    public int poll() {
        // Requests first, integrity check second. A not-ready ReplayerService still owes every request an
        // answer — ReplayPending, or ReplayUnavailable once the check has failed — and answering costs
        // nothing, whereas checkReady below makes archive control calls that block this thread for as
        // long as the archive takes. The cost of this order is that a request arriving in the very cycle
        // readiness flips is answered ReplayPending and served on the app's next resend instead.
        int work = requestSub.poll(requestHandler, FRAGMENT_LIMIT);
        if (!ready) {
            work += checkReady();
        }
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
     *
     * <p>One step per duty cycle: open the check, or read at most one fragment of it. It never waits.
     * @return work count, so the idle strategy does not park a cycle that is actively reading
     */
    private int checkReady() {
        if (integrityFailed) {
            return 0;
        }
        if (selfCheckSub == null) {
            startSelfCheck();
            return 0;
        }
        return pollSelfCheck();
    }

    /**
     * Opens the self-check replay: the oldest tap recording's first frame, on the internal {@link
     * #SELF_CHECK_STREAM_ID}. Replay is the only way to read recorded content back, so there is no
     * cheaper way to see that frame.
     *
     * <p>Bounded to {@link #SELF_CHECK_REPLAY_LENGTH}, not to the whole recording: only the first
     * fragment is ever read, and asking the archive to replay a trading day's worth of log to look at
     * eight bytes is work it would do until {@link #closeSelfCheck} stopped it. Every early return here
     * is transient — nothing recorded yet, or the archive not answering — and simply retries on the
     * next cycle.
     */
    private void startSelfCheck() {
        final ReplayRecordings.RecordingSpan active;
        try {
            active = findActiveRecording();
        } catch (final RuntimeException ex) {
            return;  // archive not answering yet; retry next cycle (scripts time out and proceed)
        }
        if (active == null) {
            return;  // nothing recorded yet; retry next cycle
        }
        final List<ReplayRecordings.RecordingSpan> segments = resolveSegments();
        if (segments.isEmpty()) {
            return;  // retry next cycle
        }

        final ReplayRecordings.RecordingSpan oldest = segments.get(0);
        try {
            long position = archive.getRecordingPosition(oldest.recordingId());
            if (position < 0) {
                position = archive.getStopPosition(oldest.recordingId());
            }
            final long replayLength = Math.min(position - oldest.startPosition(), SELF_CHECK_REPLAY_LENGTH);
            if (replayLength <= 0) {
                return;  // oldest recording has nothing written yet; retry next cycle
            }
            selfCheckRecordingId = oldest.recordingId();
            selfCheckActiveRecordingId = active.recordingId();
            selfCheckGlobalSeqNo = NULL_VALUE;
            selfCheckReplaySessionId = archive.startReplay(oldest.recordingId(), oldest.startPosition(), replayLength,
                                                          IPC_CHANNEL, SELF_CHECK_STREAM_ID);
            selfCheckSub = aeron.addSubscription(IPC_CHANNEL, SELF_CHECK_STREAM_ID);
            selfCheckDeadlineNs = System.nanoTime() + SELF_CHECK_TIMEOUT_NS;
        } catch (final RuntimeException ex) {
            closeSelfCheck();  // archive not answering yet; retry next cycle
        }
    }

    /**
     * Reads at most one fragment off an in-flight self-check and acts on it. Returns a work count.
     *
     * <p>A first frame at {@code globalSeqNo} 1 is what makes this node ready. Anything else latches
     * {@link #integrityFailed} permanently — retrying reads the same on-disk bytes, so there is nothing
     * to wait for. Delivering nothing before the deadline is neither: that is transient, so the check is
     * torn down and started fresh on a later cycle.
     */
    private int pollSelfCheck() {
        final int work = selfCheckSub.poll(selfCheckHandler, 1);
        if (selfCheckGlobalSeqNo == NULL_VALUE) {
            if (System.nanoTime() > selfCheckDeadlineNs) {
                closeSelfCheck();  // no fragment in time; start over next cycle
            }
            return work;
        }

        final long firstGlobalSeqNo = selfCheckGlobalSeqNo;
        final long oldestRecordingId = selfCheckRecordingId;
        final long activeRecordingId = selfCheckActiveRecordingId;
        closeSelfCheck();
        if (firstGlobalSeqNo != 1L) {
            integrityFailed = true;
            integrityFailureCounter.set(1);
            Logger.fault(Logger.Component.ReplayerService, Logger.EventCode.ArchiveIntegrityFailure, memberId,
                    "FATAL: oldest tap recording %d's first frame has globalSeqNo=%d, expected "
                            + "1 — this node's recording does not reach the start of the log (deleted, corrupted, or a "
                            + "partial restore?); refusing to mark ready", oldestRecordingId, firstGlobalSeqNo);
            return work;
        }

        ready = true;
        readyCounter.set(1);
        Logger.info(Logger.Component.ReplayerService, memberId, "ready — tap recording %d live; serving replay",
                activeRecordingId);
        return work;
    }

    /**
     * Reads globalSeqNo off the self-check replay's first fragment.
     * @param buffer fragment buffer
     * @param offset fragment offset
     */
    private void onSelfCheckFragment(final DirectBuffer buffer, final int offset) {
        selfCheckMsgHeaderDecoder.wrap(buffer, offset);
        // Only the sequenced schema carries the header composite this reads globalSeqNo out of;
        // anything else on this stream would decode to a number with no meaning. The templateId is
        // deliberately not checked — every sequenced message carries the same header, which is the whole
        // point of the schema (see Sequencer.sequenceMessage).
        if (selfCheckMsgHeaderDecoder.schemaId()
                != org.limitless.phixeron.sbe.sequenced.MessageHeaderDecoder.SCHEMA_ID) {
            return;
        }
        final int bodyOffset = offset + org.limitless.phixeron.sbe.sequenced.MessageHeaderDecoder.ENCODED_LENGTH;
        selfCheckHeaderDecoder.wrap(buffer, bodyOffset);
        selfCheckGlobalSeqNo = selfCheckHeaderDecoder.globalSeqNo();
    }

    /** Tears down an in-flight self-check, whether it answered, expired, or never opened. */
    private void closeSelfCheck() {
        if (selfCheckReplaySessionId != NULL_VALUE) {
            stopReplay(selfCheckReplaySessionId);
            selfCheckReplaySessionId = NULL_VALUE;
        }
        if (selfCheckSub != null) {
            selfCheckSub.close();
            selfCheckSub = null;
        }
        selfCheckGlobalSeqNo = NULL_VALUE;
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
        // ReplayHeartbeat: the app is still riding its replay image. Refresh its slot so the TTL ages
        // from the client's last sign of life, not from when the replay started (see ReplaySlotAllocator
        // .touch — a full-log replay outlives any fixed lifetime). No reply.
        if (inHeaderDecoder.templateId() == ReplayHeartbeatDecoder.TEMPLATE_ID) {
            replayHeartbeatDecoder.wrap(buffer, offset + MessageHeaderDecoder.ENCODED_LENGTH,
                                        inHeaderDecoder.blockLength(), inHeaderDecoder.version());
            replaySlots.touch(replayHeartbeatDecoder.clientId(), System.currentTimeMillis());
            return;
        }
        if (inHeaderDecoder.templateId() != ReplayRequestDecoder.TEMPLATE_ID) {
            return;
        }
        replayRequestDecoder.wrap(buffer, offset + MessageHeaderDecoder.ENCODED_LENGTH, inHeaderDecoder.blockLength(),
                                  inHeaderDecoder.version());
        final int clientId = replayRequestDecoder.clientId();
        final long requestId = replayRequestDecoder.requestId();
        final long fromPosition = replayRequestDecoder.fromPosition();
        final int segmentIndex = replayRequestDecoder.segmentIndex();

        if (clientIdCollisions.onRequest(clientId, requestId, System.currentTimeMillis())) {
            onClientIdCollision(clientId);
        }

        // Answer from this node's own health before touching a slot or the archive — see checkReady.
        // Serving history the integrity check rejected is worse than refusing it: the replay's first
        // frame would not be globalSeqNo 1, and every co-located app would abort on its own baseline
        // check, so one broken archive would take down every replica instead of just this process.
        if (integrityFailed) {
            sendUnavailable(clientId, requestId);
            return;
        }
        // Not yet proven good either. Not enqueued: the app's own resend timer is the retry, and a
        // request queued here would only be re-tried against the same not-yet-ready state.
        if (!ready) {
            sendPending(clientId, requestId);
            return;
        }

        // Supersede any in-flight replay for this client (it re-requested — a new gap position or the
        // next segment of its cold-start walk).
        stopReplayForClient(clientId);

        if (!replaySlots.hasCapacity()) {
            replaySlots.enqueue(clientId, requestId, segmentIndex, fromPosition);
            sendPending(clientId, requestId);
            Logger.info(Logger.Component.ReplayerService, memberId,
                    "client %d queued: no free replay slot (active=%d/%d, pending=%d)", clientId,
                    replaySlots.activeCount(), MAX_CONCURRENT_REPLAYS, replaySlots.pendingCount());
            return;
        }
        startReplayForClient(clientId, requestId, segmentIndex, fromPosition);
        // A walk-terminating request (segmentIndex past the chain) frees this client's slot via the
        // supersede above without taking a new one; hand that freed slot to a waiting app now rather
        // than at the next idle-TTL sweep (design §5).
        drainPending();
    }

    /**
     * Serves one replay request, guarding every archive control call.
     * @param clientId client identity
     * @param requestId the request being answered, echoed in every reply
     * @param segmentIndex segment index
     * @param fromPosition start replay position
     */
    private void startReplayForClient(final int clientId, final long requestId, final int segmentIndex,
                                      final long fromPosition) {
        if (stalled) {
            final long now = System.currentTimeMillis();
            if ((now - lastStallRetryMs) < STALL_RETRY_INTERVAL_MS) {
                sendPending(clientId, requestId);
                return;
            }
            lastStallRetryMs = now;  // this attempt is the paced probe
        }
        try {
            serveReplay(clientId, requestId, segmentIndex, fromPosition);
            if (stalled) {
                stalled = false;
                stalledCounter.set(0);
                Logger.info(Logger.Component.ReplayerService, memberId,
                        "RECOVERED: local archive reachable again");
            }
        } catch (final RuntimeException error) {
            if (segmentIndex < 0) {
                // A resume replays from a position the CLIENT supplied, and the startPosition check in
                // serveReplay cannot catch the remaining way it can be wrong: in range, but not on a
                // frame boundary of a recording that has since rotated. Calling that an archive stall
                // answers ReplayPending to a request that can never succeed — forever. Steering the app
                // onto the walk costs it one round trip, and if the archive really is sick the walk
                // that follows says so through this same path.
                rejectResume(clientId, requestId, "archive refused it: " + error.getMessage());
                return;
            }
            onArchiveStalled("serving replay for client " + clientId, error);
            sendPending(clientId, requestId);
        }
    }

    /**
     * Refuses a resume-by-position request: answers NO_REPLAY_NEEDED, which an app that resumed only
     * because it has an open hole reads as "that position is no good here" and falls back to walking the
     * recording chain — the path that needs no position to be sound.
     * @param clientId client identity
     * @param requestId the request being refused
     * @param reason what was wrong with the position, for the log
     */
    private void rejectResume(final int clientId, final long requestId, final String reason) {
        Logger.info(Logger.Component.ReplayerService, memberId,
                "client %d's resume refused (%s) — answering NO_REPLAY_NEEDED so it re-walks the chain",
                clientId, reason);
        sendReplaying(clientId, requestId, NO_REPLAY_NEEDED, 0, NULL_VALUE);
    }

    /**
     * Serves one replay to a client. segmentIndex < 0 resumes the current active recording at
     * fromPosition (steady-state gap recovery); segmentIndex >= 0 is one step of a cold-start walk over
     * the per-leader-tenure recording chain — serving the segmentIndex-th recording from position 0, or
     * NO_REPLAY_NEEDED once the walk runs past the last tenure (which is what marks the app caught up).
     * @param clientId client identity
     * @param requestId the request being answered, echoed in every reply
     * @param segmentIndex segment index
     * @param fromPosition start position
     */
    private void serveReplay(final int clientId, final long requestId, final int segmentIndex,
                             final long fromPosition) {
        // This request is the client's current one, so anything it still has queued is stale — drop it
        // before any path below can queue a fresh one (see ReplaySlotAllocator.cancelPending).
        replaySlots.cancelPending(clientId);

        final long recordingId;
        final long replayFrom;
        if (segmentIndex < 0) {
            final ReplayRecordings.RecordingSpan active = findActiveRecording();
            if (active == null) {
                // No recording to replay from yet; ask the app to hold and retry.
                replaySlots.enqueue(clientId, requestId, segmentIndex, fromPosition);
                sendPending(clientId, requestId);
                return;
            }
            if (fromPosition < active.startPosition()) {
                // The app is resuming at a position from a recording this one replaced: it predates
                // anything we hold. Steer it onto the chain walk (see rejectResume) instead of handing
                // the archive a position it will refuse.
                rejectResume(clientId, requestId, "position " + fromPosition + " predates recording "
                        + active.recordingId() + "'s startPosition " + active.startPosition());
                return;
            }
            recordingId = active.recordingId();
            replayFrom = fromPosition;
        } else {
            final List<ReplayRecordings.RecordingSpan> segments = resolveSegments();
            if (segments.isEmpty()) {
                // No tap recording on the local archive yet; hold and retry.
                replaySlots.enqueue(clientId, requestId, segmentIndex, fromPosition);
                sendPending(clientId, requestId);
                return;
            }
            if (segmentIndex >= segments.size()) {
                // Walked past the last tenure: the app has replayed all history and is at the live tip.
                // No specific recording to name here — NULL_VALUE, matching NO_REPLAY_NEEDED's own sentinel,
                // and load-bearing: this is the ONLY reply that ends a walk. The empty-segment case below
                // sends the same sentinel while naming its recording, which is how the app tells "skip this
                // segment" from "the chain is exhausted, you are caught up".
                sendReplaying(clientId, requestId, NO_REPLAY_NEEDED, 0, NULL_VALUE);
                return;
            }
            // The recording's own startPosition, not a hardcoded 0: a walk step means "this whole
            // segment from its beginning", and the archive is the authority on where that is.
            final ReplayRecordings.RecordingSpan segment = segments.get(segmentIndex);
            recordingId = segment.recordingId();
            replayFrom = segment.startPosition();
        }

        long tip = archive.getRecordingPosition(recordingId);
        if (tip < 0) {
            tip = archive.getStopPosition(recordingId);
        }
        if (tip < 0) {
            // Neither counter could say where this recording ends: its RecordingPos counter is already
            // gone and its stopPosition is not written yet. That is a transient read, not an answer —
            // reported as a tip it would tell a walking app this segment is empty and a resuming one
            // that its position is stale. Hold and retry, exactly as for a recording not yet listed.
            replaySlots.enqueue(clientId, requestId, segmentIndex, fromPosition);
            sendPending(clientId, requestId);
            return;
        }
        final long boundedLength = tip - replayFrom;
        if (boundedLength <= 0) {
            // Already at (or past) the tip — nothing historical to serve. Naming the recording is what
            // separates this from the walk-terminating reply above: for a resume it means "you are at
            // the tip", but for a walk step it means only that THIS segment is empty (an unclean restart
            // can leave a recording created before anything was published to it), and the app must skip
            // it and carry on rather than end its walk with the rest of the chain unreplayed.
            sendReplaying(clientId, requestId, NO_REPLAY_NEEDED, tip, recordingId);
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
        // image at the bound, so the app detects completion by position (see Replaying / ReplayerStreamReceiver).
        sendReplaying(clientId, requestId, replaySessionId, tip, recordingId);
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
            final ReplaySlotAllocator.PendingRequest request = replaySlots.pollPending();
            if (request != null) {
                startReplayForClient(request.clientId(), request.requestId(), request.segmentIndex(),
                                     request.fromPosition());
            }
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
     * @param requestId the request being answered
     * @param replaySessionId replay session identity
     * @param catchUpPosition catchup positon
     * @param recordingId archive recordingId the reply was served from, or NULL_VALUE (see Replaying.recordingId)
     */
    private void sendReplaying(final int clientId, final long requestId, final long replaySessionId,
                               final long catchUpPosition, final long recordingId) {
        replayingEncoder.wrapAndApplyHeader(controlBuffer, 0, outHeaderEncoder)
            .clientId(clientId)
            .requestId(requestId)
            .replaySessionId(replaySessionId)
            .catchUpPosition(catchUpPosition)
            .recordingId(recordingId);
        offerControl(MessageHeaderEncoder.ENCODED_LENGTH + replayingEncoder.encodedLength());
    }

    /**
     * Send pending message
     * @param clientId client identity
     * @param requestId the request being answered
     */
    private void sendPending(final int clientId, final long requestId) {
        pendingEncoder.wrapAndApplyHeader(controlBuffer, 0, outHeaderEncoder).clientId(clientId).requestId(requestId);
        offerControl(MessageHeaderEncoder.ENCODED_LENGTH + pendingEncoder.encodedLength());
    }

    /**
     * Refuses a replay request outright: this node failed its integrity check and has no valid history
     * to serve. Not logged per refusal — checkReady already reported the fault once, and the apps resend
     * on a 500ms timer.
     * @param clientId client identity
     * @param requestId the request being refused
     */
    private void sendUnavailable(final int clientId, final long requestId) {
        unavailableEncoder.wrapAndApplyHeader(controlBuffer, 0, outHeaderEncoder).clientId(clientId)
            .requestId(requestId);
        offerControl(MessageHeaderEncoder.ENCODED_LENGTH + unavailableEncoder.encodedLength());
    }

    /**
     * Offers one control reply, dropping it rather than spinning without bound (see {@link
     * #MAX_CONTROL_OFFER_SPINS}). Dropping is safe here in a way it is not on the tap: a reply answers
     * a request the app resends on a timer, so the cost is one resend interval, whereas an unsent
     * sequenced frame is a hole nothing can fill.
     * @param length encoded length
     */
    private void offerControl(final int length) {
        long result;
        int spins = 0;
        while ((result = controlPub.offer(controlBuffer, 0, length)) < 0) {
            if (result == ExclusivePublication.CLOSED || result == ExclusivePublication.MAX_POSITION_EXCEEDED) {
                throw new IllegalStateException("[ReplayerService] control publication failed: " + result);
            }
            if (++spins > MAX_CONTROL_OFFER_SPINS) {
                // NOT_CONNECTED is ordinary: an app's control subscription need not be registered yet
                // when its first request lands, and the resend covers it. Back-pressure at the bound is
                // not ordinary — that is an app that subscribed and stopped reading.
                if (result != ExclusivePublication.NOT_CONNECTED) {
                    onControlReplyDropped(result);
                }
                return;
            }
            idleStrategy.idle();
        }
    }

    /**
     * Two co-located apps are using one client id (see {@link ReplayClientIdCollisions}). Reported, not
     * refused: both are already livelocked — each request stops the other's replay — so refusing changes
     * nothing they experience, while a false positive would stop a healthy replica from ever recovering.
     * What was missing is the diagnosis, and the fix is a launch-configuration change.
     * @param clientId the id being used twice
     */
    private void onClientIdCollision(final int clientId) {
        clientIdCollisionCounter.set(1);
        Logger.error(Logger.Component.ReplayerService, Logger.EventCode.ReplayClientIdCollision, memberId,
                "two co-located apps are both using PHIXERON_REPLAYER_CLIENT_ID=%d — their requestId "
                        + "sequences interleave, so each request stops the other's replay and NEITHER will "
                        + "ever catch up; give them distinct ids and restart them", clientId);
    }

    /**
     * A control reply was dropped at the spin bound: report it once, count every one.
     * @param result the failing offer result
     */
    private void onControlReplyDropped(final long result) {
        controlRepliesDroppedCounter.increment();
        if (!controlReplyDropLogged) {
            controlReplyDropLogged = true;
            Logger.error(Logger.Component.ReplayerService, Logger.EventCode.ControlReplyDropped, memberId,
                    "dropped a control reply (offer=%d): an app subscribed to stream %d and stopped "
                            + "reading it. Its replays are delayed by a resend; every other app is "
                            + "unaffected — see phixeron.replayer.controlRepliesDroppedCount", result,
                    CONTROL_STREAM_ID);
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
    // earlier, stopped recording alongside it, and this returns the active one. An unclean shutdown can
    // leave an earlier one unstopped too, so this picks the highest recordingId rather than trusting
    // listing order — the same span ReplayRecordings.stitch keeps, so the two selection rules agree.
    // Returns null when there is none.
    private ReplayRecordings.RecordingSpan findActiveRecording() {
        final ReplayRecordings.RecordingSpan[] found = {null};
        archive.listRecordingsForUri(0, Integer.MAX_VALUE, "", SequencerService.FEEDER_STREAM_ID,
                                     (controlSessionId, correlationId, recordingId, startTimestamp, stopTimestamp,
                                      startPosition, stopPosition, initialTermId, segmentFileLength, termBufferLength,
                                      mtuLength, sessionId, streamId, strippedChannel, originalChannel,
                                      sourceIdentity) -> {
                                         if (stopTimestamp == AeronArchive.NULL_TIMESTAMP
                                                 && (found[0] == null || recordingId > found[0].recordingId())) {
                                             found[0] = new ReplayRecordings.RecordingSpan(recordingId, startPosition,
                                                                                           true);
                                         }
                                     });
        return found[0];
    }

    // Ordered oldest→newest list of tap recordings on the local archive. With every node recording its
    // own continuous tap this is normally a single recording spanning every leader tenure, so there is usually nothing
    // to stitch. A member restart can leave an earlier, stopped recording plus the post-restart one (overlapping
    // globalSeqNo ranges); a cold-starting app replays them in order and de-duplicates by globalSeqNo, so the overlap
    // is harmless. Mirrors ClusterStreamClient.resolveClusterStreamSegments.
    private List<ReplayRecordings.RecordingSpan> resolveSegments() {
        final List<ReplayRecordings.RecordingSpan> spans = new ArrayList<>();
        archive.listRecordingsForUri(0, Integer.MAX_VALUE, "", SequencerService.FEEDER_STREAM_ID,
                                     (controlSessionId, correlationId, recordingId, startTimestamp, stopTimestamp,
                                      startPosition, stopPosition, initialTermId, segmentFileLength, termBufferLength,
                                      mtuLength, sessionId, streamId, strippedChannel, originalChannel,
                                      sourceIdentity) -> spans.add(new ReplayRecordings.RecordingSpan(recordingId,
                                          startPosition, stopTimestamp == AeronArchive.NULL_TIMESTAMP)));
        final long activeCount = spans.stream().filter(ReplayRecordings.RecordingSpan::active).count();
        if (activeCount > 1 && !staleActiveRecordingLogged) {
            staleActiveRecordingLogged = true;
            Logger.error(Logger.Component.ReplayerService, Logger.EventCode.StaleActiveRecording, memberId,
                    "%d tap recordings report as still recording — an unclean shutdown left an older one "
                            + "unstopped; serving the newest and skipping the stale one(s)", activeCount);
        }
        return ReplayRecordings.stitch(spans);
    }
}
