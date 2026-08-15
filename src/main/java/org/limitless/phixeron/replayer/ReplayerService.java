package org.limitless.phixeron.replayer;

import static io.aeron.Aeron.NULL_VALUE;

import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import io.aeron.archive.client.AeronArchive;
import io.aeron.logbuffer.FragmentHandler;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.status.AtomicCounter;
import org.limitless.phixeron.metrics.PhixeronCounters;
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
 * <p><b>Startup integrity check.</b> Before ever declaring readiness, {@link #checkReady} verifies that
 * <em>every</em> recording in this node's chain begins at {@code globalSeqNo} 1 (see {@link
 * #startSelfCheck} / {@link #pollSelfCheck}, which sweep the chain one span per duty cycle rather than
 * blocking one), not just that some recording exists. Every span, not only the oldest: recovery is
 * always full-log replay with no snapshots, so a healthy recording starts at {@code globalSeqNo} 1
 * however late it was created ({@link ReplayRecordings#stitch}), and one that starts higher resumed
 * mid-history — which IS a hole at its join with the span before it. Nothing else here can see that
 * hole; it otherwise surfaces only as a co-located app that walks the chain and never converges
 * (review-3.md finding 6). A <em>stopped</em> recording with nothing in it is skipped instead: an
 * unclean restart can create one before anything is published to it, it can never gain a first frame,
 * and {@link #serveReplay} already skips it by name. This should always hold — {@code
 * SequencerService} arms and confirms its recording before it can emit a single frame, and this
 * project's cluster membership is static, never joining mid-history — so a failure here means this
 * node's own recording has been deleted, corrupted, or partially restored: a broken node. {@code
 * ready} then never becomes true for this process's lifetime ({@link
 * PhixeronCounters#REPLAYER_INTEGRITY_FAILURE_TYPE_ID} latches instead), and {@link #onRequest} answers every replay
 * request {@code ReplayUnavailable} — the refusal is what contains the broken archive here, rather than leaving every
 * consumer that asks this node for history to independently hit the same wall (and each app's wall is an abort on its
 * own first-frame-must-be-1 check, so one bad archive would take down all of them). Until the check has passed,
 * requests are answered {@code ReplayPending}: history this node has not proven good is not served.
 *
 * <p><b>Local-archive resilience.</b> The ReplayerService is off the live path entirely, so a transient
 * failure of the node's local archive degrades only history/gap <em>replay</em> — steady-state
 * delivery keeps flowing over the tap the apps read directly. It does not crash the ReplayerService either:
 * an archive control call that throws — from any path, the startup self-check included — flips it to a
 * STALLED state and keeps the duty cycle running, paces its replay retries, and answers any replay request
 * with {@code ReplayPending} — which the app already treats as "hold at the gap and re-request" — until
 * the archive returns (doc/router-archive.md). The state is the node's own view of its archive and nothing
 * else, so it is set wherever the archive refuses and cleared wherever it answers, {@link #probeArchive}
 * included: an idle Replayer must not go on reporting a fault that has been over for hours.
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
     * Internal-only IPC stream the startup self-check replays onto to read back each tap recording's
     * first frame (see {@link #checkReady}). Never used by any app-facing
     * protocol — distinct from {@link #REPLAY_STREAM_ID} purely so this one-shot self-check can never
     * cross-talk with a real client replay. Package-private rather than private: {@link
     * AeronReplayer} subscribes to it on this class's behalf.
     */
    static final int SELF_CHECK_STREAM_ID = 204;

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
     * starvation one. Package-private so {@code ReplayerServiceTest} fills exactly this many slots
     * rather than hardcoding the number a second time.
     */
    static final int MAX_CONCURRENT_REPLAYS = 4;

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

    /**
     * Quiet period that ends a control-reply-drop episode (see {@link #onControlReplyDropped}). Drops
     * come in bursts — a wedged app is answered on every one of its ~500ms resends — so the report is
     * per burst, not per drop; a drop this long after the last one is a new episode and is reported
     * again. Without it the first burst of the process is the only one ever named.
     */
    private static final long CONTROL_DROP_QUIET_MS = 60_000;

    private static final int FRAGMENT_LIMIT = 16;

    /** The node this serves: its archive, its two app-facing IPC streams, its counters, its clock. */
    private final Replayer replayer;
    private final int memberId;
    private final IdleStrategy idleStrategy;

    /** Brings the whole process down; wired by {@link ReplayerNode}. See {@link #fatalDutyCycleFailure}. */
    private final Runnable fatalHandler;

    // Proactive readiness marker: set once the co-located SequencerService's tap recording is visible
    // on the local archive AND has passed the startup integrity check (see checkReady). The launch
    // scripts wait on the readiness log before starting apps.
    private boolean ready = false;

    // Latched once a tap recording's first frame fails the gseq-1 integrity check (see
    // checkReady/pollSelfCheck): this node's recording chain doesn't cover the log from the start
    // (deleted, corrupted, or a partial restore), so there is no valid history to serve. `ready` must
    // never become true once this is set — every consumer that would otherwise ask this node for
    // history would independently hit the same wall, so it fails here instead, once, loudly.
    private boolean integrityFailed = false;

    // Startup integrity self-check (see checkReady)
    private Replayer.SelfCheckStream selfCheckSub;
    private List<ReplayRecordings.RecordingSpan> selfCheckSpans;
    private int selfCheckIndex;
    private long selfCheckReplaySessionId = NULL_VALUE;
    private long selfCheckRecordingId = NULL_VALUE; // the span being peeked
    private long selfCheckActiveRecordingId = NULL_VALUE; // the live one, for the readiness line
    private long selfCheckGlobalSeqNo = NULL_VALUE; // what the first fragment carried, once read
    private long selfCheckDeadlineNs = 0;

    // Latches while the ">1 active tap recording" anomaly is reported (see resolveSegments): the
    // condition lasts as long as the stale recording is on disk, and resolveSegments runs per replay
    // request, so without this it would repeat the fault line on every one. Cleared when the count
    // returns to 1 — an operator purge followed by a later unclean shutdown is a second episode, and
    // this anomaly has no counter, so an un-re-armed latch makes it silent rather than merely quiet.
    private boolean staleActiveRecordingLogged = false;

    // When the last dropped control reply was (see onControlReplyDropped), 0 = none this process.
    // Drops are reported per episode rather than per drop: a wedged app is answered on every one of its
    // resends, so without this the fault line would repeat at the resend rate. The counter carries the
    // rate in between.
    private long lastControlDropMs = 0;

    // Replay protocol state
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

    // Operator counters (see PhixeronCounters)
    private final AtomicCounter stalledCounter;
    private final AtomicCounter readyCounter;
    private final AtomicCounter activeReplaySlotsCounter;
    private final AtomicCounter pendingRequestsCounter;
    private final AtomicCounter replaysServedCounter;
    private final AtomicCounter idleTtlReclaimedCounter;
    private final AtomicCounter integrityFailureCounter;
    private final AtomicCounter controlRepliesDroppedCounter;
    private final AtomicCounter clientIdCollisionCounter;

    private final MessageHeaderDecoder inHeaderDecoder = new MessageHeaderDecoder();
    private final ReplayRequestDecoder replayRequestDecoder = new ReplayRequestDecoder();
    private final ReplayCompleteDecoder replayCompleteDecoder = new ReplayCompleteDecoder();
    private final ReplayHeartbeatDecoder replayHeartbeatDecoder = new ReplayHeartbeatDecoder();
    private final MessageHeaderEncoder outHeaderEncoder = new MessageHeaderEncoder();
    private final ReplayingEncoder replayingEncoder = new ReplayingEncoder();
    private final ReplayPendingEncoder pendingEncoder = new ReplayPendingEncoder();
    private final ReplayUnavailableEncoder unavailableEncoder = new ReplayUnavailableEncoder();
    private final MutableDirectBuffer controlBuffer = new ExpandableArrayBuffer(64);

    private final org.limitless.phixeron.sbe.sequenced.MessageHeaderDecoder selfCheckMsgHeaderDecoder =
        new org.limitless.phixeron.sbe.sequenced.MessageHeaderDecoder();
    private final org.limitless.phixeron.sbe.sequenced.HeaderDecoder selfCheckHeaderDecoder =
        new org.limitless.phixeron.sbe.sequenced.HeaderDecoder();

    private final FragmentHandler requestHandler =
        (buffer, offset, length, header) -> onRequest(buffer, offset, length);

    private final FragmentHandler selfCheckHandler =
        (buffer, offset, length, header) -> onSelfCheckFragment(buffer, offset);

    /**
     * Production constructor: serves member {@code memberId}'s own Aeron client and archive.
     * @param fatalHandler run once, from the duty-cycle thread, when the duty cycle dies on an uncaught
     *                     exception — see {@link #fatalDutyCycleFailure}. Must not block: it is expected
     *                     to signal a shutdown and return, not to perform one.
     */
    public ReplayerService(final Aeron aeron, final AeronArchive archive, final int memberId,
                           final IdleStrategy idleStrategy, final Runnable fatalHandler) {
        this(new AeronReplayer(aeron, archive, memberId), memberId, idleStrategy, fatalHandler);
    }

    /**
     * Serves whatever {@link Replayer} it is handed — the seam a test substitutes a fake
     * node for.
     * @param replayer the node's archive, app-facing streams, counters and clock
     * @param memberId which cluster member this ReplayerService co-locates with
     * @param idleStrategy duty-cycle and control-offer idle strategy
     * @param fatalHandler see the production constructor
     */
    ReplayerService(final Replayer replayer,
                    final int memberId,
                    final IdleStrategy idleStrategy,
                    final Runnable fatalHandler) {
        this.replayer = replayer;
        this.memberId = memberId;
        this.idleStrategy = idleStrategy;
        this.fatalHandler = fatalHandler;

        this.stalledCounter = replayer.newCounter(PhixeronCounters.REPLAYER_STALLED_TYPE_ID,
                                                  "phixeron.replayer.stalled member=" + memberId);
        this.readyCounter =
            replayer.newCounter(PhixeronCounters.REPLAYER_READY_TYPE_ID, "phixeron.replayer.ready member=" + memberId);
        this.activeReplaySlotsCounter = replayer.newCounter(PhixeronCounters.REPLAYER_ACTIVE_SLOTS_TYPE_ID,
                                                            "phixeron.replayer.activeSlots member=" + memberId);
        this.pendingRequestsCounter = replayer.newCounter(PhixeronCounters.REPLAYER_PENDING_REQUESTS_TYPE_ID,
                                                          "phixeron.replayer.pendingRequests member=" + memberId);
        this.replaysServedCounter = replayer.newCounter(PhixeronCounters.REPLAYER_REPLAYS_SERVED_COUNT_TYPE_ID,
                                                        "phixeron.replayer.replaysServedCount member=" + memberId);
        this.idleTtlReclaimedCounter =
            replayer.newCounter(PhixeronCounters.REPLAYER_IDLE_TTL_RECLAIMED_COUNT_TYPE_ID,
                                "phixeron.replayer.idleTtlReclaimedCount member=" + memberId);
        this.integrityFailureCounter = replayer.newCounter(PhixeronCounters.REPLAYER_INTEGRITY_FAILURE_TYPE_ID,
                                                           "phixeron.replayer.integrityFailure member=" + memberId);
        this.controlRepliesDroppedCounter =
            replayer.newCounter(PhixeronCounters.REPLAYER_CONTROL_REPLIES_DROPPED_COUNT_TYPE_ID,
                                "phixeron.replayer.controlRepliesDroppedCount member=" + memberId);
        this.clientIdCollisionCounter = replayer.newCounter(PhixeronCounters.REPLAYER_CLIENT_ID_COLLISION_TYPE_ID,
                                                            "phixeron.replayer.clientIdCollision member=" + memberId);
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
        closeSelfCheck(); // a check still in flight owns an archive replay and a subscription
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
                         + "and exiting; process supervision should restart this node",
                     ex.getMessage());
        fatalHandler.run();
    }

    /** One duty-cycle iteration. Returns a work count for the idle strategy. */
    public int poll() {
        int work = replayer.pollRequests(requestHandler, FRAGMENT_LIMIT);
        if (!ready) {
            work += checkReady();
        } else if (stalled) {
            probeArchive();
        }
        reclaimIdleSlots();
        activeReplaySlotsCounter.set(replaySlots.activeCount());
        pendingRequestsCounter.set(replaySlots.pendingCount());
        return work;
    }

    /**
     * Waits for the tap recording to be visible, then verifies its history actually reaches back to
     * the start of the log before ever declaring readiness — every recording in the chain, one per
     * duty cycle (see the class Javadoc for why the newest counts as much as the oldest). Under correct
     * operation this always holds
     * (SequencerService arms and confirms the recording before it can emit a single frame — see its
     * onStart/awaitTapRecordingActive — and this node's own static, fixed cluster membership never
     * joins mid-history), so a failure here means a tap recording has been deleted,
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
     * Opens the self-check replay for the span the sweep is on: its first frame, on the internal {@link
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
        try {
            final ReplayRecordings.RecordingSpan active = findActiveRecording();
            if (active == null) {
                return; // nothing recorded yet; retry next cycle
            }
            if (selfCheckSpans == null) {
                final List<ReplayRecordings.RecordingSpan> segments = resolveSegments();
                if (segments.isEmpty()) {
                    return; // retry next cycle
                }
                selfCheckSpans = segments;
                selfCheckIndex = 0;
            }
            selfCheckActiveRecordingId = active.recordingId();

            final ReplayRecordings.RecordingSpan span = selfCheckSpans.get(selfCheckIndex);
            long position = replayer.recordingPosition(span.recordingId());
            if (position < 0) {
                position = replayer.stopPosition(span.recordingId());
            }
            final long replayLength = Math.min(position - span.startPosition(), SELF_CHECK_REPLAY_LENGTH);
            if (replayLength <= 0) {
                if (span.active()) {
                    return; // nothing written to the live recording yet; retry next cycle
                }
                completeSelfCheckSpan();
                return;
            }
            selfCheckRecordingId = span.recordingId();
            selfCheckGlobalSeqNo = NULL_VALUE;
            selfCheckReplaySessionId =
                replayer.startReplay(span.recordingId(), span.startPosition(), replayLength, SELF_CHECK_STREAM_ID);
            selfCheckSub = replayer.openSelfCheckStream();
            selfCheckDeadlineNs = replayer.nanoTime() + SELF_CHECK_TIMEOUT_NS;
            onArchiveRecovered(); // it served a replay: whatever refused one earlier is over
        } catch (final RuntimeException ex) {
            closeSelfCheck();
            onArchiveStalled("running the startup self-check", ex);
        }
    }

    /**
     * Reads at most one fragment off an in-flight self-check and acts on it. Returns a work count.
     *
     * <p>A first frame at {@code globalSeqNo} 1 proves that span; the node is ready once every span in
     * the chain has been proved. Anything else latches {@link #integrityFailed} permanently — retrying
     * reads the same on-disk bytes, so there is nothing to wait for. Delivering nothing before the
     * deadline is neither: that is transient, so the check is torn down and the same span started fresh
     * on a later cycle.
     */
    private int pollSelfCheck() {
        final int work = selfCheckSub.poll(selfCheckHandler, 1);
        if (selfCheckGlobalSeqNo == NULL_VALUE) {
            if (replayer.nanoTime() > selfCheckDeadlineNs) {
                closeSelfCheck(); // no fragment in time; start over next cycle
            }
            return work;
        }

        final long firstGlobalSeqNo = selfCheckGlobalSeqNo;
        final long recordingId = selfCheckRecordingId;
        closeSelfCheck();
        if (firstGlobalSeqNo != 1L) {
            integrityFailed = true;
            integrityFailureCounter.set(1);
            Logger.fault(Logger.Component.ReplayerService, Logger.EventCode.ArchiveIntegrityFailure, memberId,
                         "FATAL: tap recording %d (%d of %d in this node's chain) has first frame globalSeqNo=%d, "
                             + "expected 1 — this node's recording chain does not cover the log from the start "
                             + "(deleted, corrupted, or a partial restore?); refusing to mark ready",
                         recordingId, selfCheckIndex + 1, selfCheckSpans.size(), firstGlobalSeqNo);
            return work;
        }
        completeSelfCheckSpan();
        return work;
    }

    /**
     * One span is behind the sweep — proved at {@code globalSeqNo} 1, or a stopped empty one there is
     * nothing to prove. Readiness waits for the whole chain.
     */
    private void completeSelfCheckSpan() {
        ++selfCheckIndex;
        if (selfCheckIndex < selfCheckSpans.size()) {
            return; // the next cycle opens the next span's check
        }
        ready = true;
        readyCounter.set(1);
        Logger.info(Logger.Component.ReplayerService, memberId,
                    "ready — tap recording %d live, %d-recording chain verified from globalSeqNo 1; serving replay",
                    selfCheckActiveRecordingId, selfCheckSpans.size());
        selfCheckSpans = null; // the sweep is over; a later one resolves the chain again rather than resuming this
    }

    /**
     * Reads globalSeqNo off the self-check replay's first fragment.
     * @param buffer fragment buffer
     * @param offset fragment offset
     */
    private void onSelfCheckFragment(final DirectBuffer buffer, final int offset) {
        selfCheckMsgHeaderDecoder.wrap(buffer, offset);
        if (selfCheckMsgHeaderDecoder.schemaId() !=
            org.limitless.phixeron.sbe.sequenced.MessageHeaderDecoder.SCHEMA_ID) {
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

    // Replay protocol
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

        // ReplayComplete: the app caught up and is now on the live tap.
        if (inHeaderDecoder.templateId() == ReplayCompleteDecoder.TEMPLATE_ID) {
            replayCompleteDecoder.wrap(buffer, offset + MessageHeaderDecoder.ENCODED_LENGTH,
                                       inHeaderDecoder.blockLength(), inHeaderDecoder.version());
            stopReplayForClient(replayCompleteDecoder.clientId());
            drainPending();
            return;
        }
        // ReplayHeartbeat: the app is still riding its replay image. Refresh its slot so the TTL ages.
        if (inHeaderDecoder.templateId() == ReplayHeartbeatDecoder.TEMPLATE_ID) {
            replayHeartbeatDecoder.wrap(buffer, offset + MessageHeaderDecoder.ENCODED_LENGTH,
                                        inHeaderDecoder.blockLength(), inHeaderDecoder.version());
            replaySlots.touch(replayHeartbeatDecoder.clientId(), replayer.epochMillis());
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

        if (clientIdCollisions.onRequest(clientId, requestId, replayer.epochMillis())) {
            onClientIdCollision(clientId);
        }

        // Answer from this node's own health before touching a slot or the archive — see checkReady.
        if (integrityFailed) {
            sendUnavailable(clientId, requestId);
            return;
        }
        if (!ready) {
            sendPending(clientId, requestId);
            return;
        }
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
            final long now = replayer.epochMillis();
            if ((now - lastStallRetryMs) < STALL_RETRY_INTERVAL_MS) {
                sendPending(clientId, requestId);
                return;
            }
            lastStallRetryMs = now; // this attempt is the paced probe
        }
        try {
            serveReplay(clientId, requestId, segmentIndex, fromPosition);
            onArchiveRecovered();
        } catch (final RuntimeException error) {
            if (segmentIndex < 0 && archiveAnswers()) {
                rejectResume(clientId, requestId, "archive refused it: " + error.getMessage());
                return;
            }
            onArchiveStalled("serving " + (segmentIndex < 0 ? "a resume" : "walk segment " + segmentIndex) +
                                 " for client " + clientId,
                             error);
            sendPending(clientId, requestId);
        }
    }

    /** Whether the local archive still answers at all — what separates a bad request from an outage. */
    private boolean archiveAnswers() {
        try {
            replayer.listTapRecordings();
            return true;
        } catch (final RuntimeException ex) {
            return false;
        }
    }

    /**
     * Re-probes the local archive while STALLED, sharing {@link #STALL_RETRY_INTERVAL_MS} with the
     * request-driven probe so the two together still make at most one attempt a second at a dead archive.
     *
     * <p>The state used to clear only on a client's replay succeeding, which made it a fault report
     * nothing could retract: a Replayer whose apps have all caught up is asked for nothing, so {@code
     * phixeron.replayer.stalled} stayed at 1 for the rest of the process however healthy the archive had
     * since become. Probes with a bounded replay rather than a listing, because serving a replay is what
     * the state claims the archive cannot do — one that lists recordings and still refuses to replay them
     * is stalled exactly as this describes.
     */
    private void probeArchive() {
        final long now = replayer.epochMillis();
        if ((now - lastStallRetryMs) < STALL_RETRY_INTERVAL_MS) {
            return;
        }
        lastStallRetryMs = now;
        try {
            final ReplayRecordings.RecordingSpan active = findActiveRecording();
            if (active != null) {
                stopReplay(replayer.startReplay(active.recordingId(), active.startPosition(), SELF_CHECK_REPLAY_LENGTH,
                                                SELF_CHECK_STREAM_ID));
            }
            onArchiveRecovered();
        } catch (final RuntimeException ex) {
            // Still refusing: stays STALLED and probes again on the next interval.
        }
    }

    /**
     * Refuses a resume request: answers NO_REPLAY_NEEDED, which an app that resumed only
     * because it has an open hole reads as "that position is no good here" and falls back to walking the
     * recording chain — the path that needs no position to be sound.
     * @param clientId client identity
     * @param requestId the request being refused
     * @param reason what was wrong with the position, for the log
     */
    private void rejectResume(final int clientId, final long requestId, final String reason) {
        Logger.info(Logger.Component.ReplayerService, memberId,
                    "client %d's resume refused (%s) — answering NO_REPLAY_NEEDED so it re-walks the chain", clientId,
                    reason);
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
                rejectResume(clientId, requestId,
                             "position " + fromPosition + " predates recording " + active.recordingId() +
                                 "'s startPosition " + active.startPosition());
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
                // The app has replayed all history.
                sendReplaying(clientId, requestId, NO_REPLAY_NEEDED, 0, NULL_VALUE);
                return;
            }
            // The recording's own startPosition, not a hardcoded 0: a walk step means "this whole
            // segment from its beginning", and the archive is the authority on where that is.
            final ReplayRecordings.RecordingSpan segment = segments.get(segmentIndex);
            recordingId = segment.recordingId();
            replayFrom = segment.startPosition();
        }

        long tip = replayer.recordingPosition(recordingId);
        if (tip < 0) {
            tip = replayer.stopPosition(recordingId);
        }
        if (tip < 0) {
            // Neither counter could say where this recording ends: its RecordingPos counter is already
            // gone and its stopPosition is not written yet.
            replaySlots.enqueue(clientId, requestId, segmentIndex, fromPosition);
            sendPending(clientId, requestId);
            return;
        }
        final long boundedLength = tip - replayFrom;
        if (boundedLength <= 0) {
            // Already at (or past) the tip — nothing historical to serve.
            sendReplaying(clientId, requestId, NO_REPLAY_NEEDED, tip, recordingId);
            return;
        }

        final long replaySessionId = replayer.startReplay(recordingId, replayFrom, boundedLength, REPLAY_STREAM_ID);
        replaysServedCounter.increment();
        replaySlots.activate(clientId, replaySessionId, replayer.epochMillis());
        Logger.info(Logger.Component.ReplayerService, memberId,
                    "replay for client %d: segment %d recording %d [%d,%d) session %d", clientId, segmentIndex,
                    recordingId, replayFrom, tip, replaySessionId);
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
        final List<Long> reclaimed = replaySlots.reclaimIdle(replayer.epochMillis());
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
            replayer.stopReplay(replaySessionId);
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
        unavailableEncoder.wrapAndApplyHeader(controlBuffer, 0, outHeaderEncoder)
            .clientId(clientId)
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
        while ((result = replayer.offerControl(controlBuffer, 0, length)) < 0) {
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
                         + "ever catch up; give them distinct ids and restart them",
                     clientId);
    }

    /**
     * A control reply was dropped at the spin bound: report it once per episode, count every one.
     * @param result the failing offer result
     */
    private void onControlReplyDropped(final long result) {
        controlRepliesDroppedCounter.increment();
        final long nowMs = replayer.epochMillis();
        final boolean newEpisode = lastControlDropMs == 0 || nowMs - lastControlDropMs >= CONTROL_DROP_QUIET_MS;
        lastControlDropMs = nowMs;
        if (newEpisode) {
            Logger.error(Logger.Component.ReplayerService, Logger.EventCode.ControlReplyDropped, memberId,
                         "dropped a control reply (offer=%d): an app subscribed to stream %d and stopped "
                             + "reading it. Its replays are delayed by a resend; every other app is "
                             + "unaffected — see phixeron.replayer.controlRepliesDroppedCount",
                         result, CONTROL_STREAM_ID);
        }
    }

    // Local-archive resilience

    /**
     * A local-archive control call threw: enter STALLED (idempotently, logging once per episode).
     * @param message what was being attempted
     * @param exception runtime exception
     */
    private void onArchiveStalled(final String message, final RuntimeException exception) {
        if (!stalled) {
            stalled = true;
            stalledCounter.set(1);
            Logger.info(Logger.Component.ReplayerService, memberId,
                        "STALLED: the local archive refused %s (%s); live delivery unaffected "
                            + "(apps read the tap directly), probing until it answers",
                        message, exception.getMessage());
        }
    }

    /** The local archive answered: leave STALLED (idempotently, logging once per episode). */
    private void onArchiveRecovered() {
        if (stalled) {
            stalled = false;
            stalledCounter.set(0);
            Logger.info(Logger.Component.ReplayerService, memberId, "RECOVERED: local archive reachable again");
        }
    }

    // Finds the currently-active tap recording on the local archive (stopTimestamp unset).
    private ReplayRecordings.RecordingSpan findActiveRecording() {
        ReplayRecordings.RecordingSpan found = null;
        for (final ReplayRecordings.RecordingSpan span : replayer.listTapRecordings()) {
            if (span.active() && (found == null || span.recordingId() > found.recordingId())) {
                found = span;
            }
        }
        return found;
    }

    // Ordered oldest→newest list of tap recordings on the local archive. With every node recording its
    // own continuous tap this is normally a single recording spanning every leader tenure, so there is usually nothing
    // to stitch.
    private List<ReplayRecordings.RecordingSpan> resolveSegments() {
        final List<ReplayRecordings.RecordingSpan> spans = replayer.listTapRecordings();
        final long activeCount = spans.stream().filter(ReplayRecordings.RecordingSpan::active).count();
        if (activeCount > 1) {
            if (!staleActiveRecordingLogged) {
                staleActiveRecordingLogged = true;
                Logger.error(Logger.Component.ReplayerService, Logger.EventCode.StaleActiveRecording, memberId,
                             "%d tap recordings report as still recording — an unclean shutdown left an older one "
                                 + "unstopped; serving the newest and skipping the stale one(s)",
                             activeCount);
            }
        } else {
            staleActiveRecordingLogged = false;
        }
        return ReplayRecordings.stitch(spans);
    }
}
