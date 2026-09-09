package org.limitless.seqeron.replayer.server;

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
import org.limitless.seqeron.metrics.SeqeronCounters;
import org.limitless.seqeron.sbe.replay.MessageHeaderDecoder;
import org.limitless.seqeron.sbe.replay.MessageHeaderEncoder;
import org.limitless.seqeron.sbe.replay.ReplayCompleteDecoder;
import org.limitless.seqeron.sbe.replay.ReplayHeartbeatDecoder;
import org.limitless.seqeron.sbe.replay.ReplayPendingEncoder;
import org.limitless.seqeron.sbe.replay.ReplayRequestDecoder;
import org.limitless.seqeron.sbe.replay.ReplayUnavailableEncoder;
import org.limitless.seqeron.sbe.replay.ReplayingEncoder;
import org.limitless.seqeron.sequencer.SequencerService;
import org.limitless.seqeron.util.Logger;

/**
 * Per-node archive <b>replay server</b> for co-located application replicas (ReplayerService design,
 * {@code doc/router-design.md}). It is deliberately <em>not</em> on the live delivery path: every
 * app reads the co-located {@code SequencerService}'s node-local IPC tap ({@link
 * SequencerService#FEEDER_CHANNEL} / {@link SequencerService#FEEDER_STREAM_ID}) <b>directly</b> for the
 * live feed, so the sequencer has no live network data subscribers (the UDP multi-destination-cast
 * global stream is retired) and audit.md S4 (sequencer liveness coupled to its slowest consumer)
 * dissolves structurally. The apps' tap subscriptions are untethered, so a slow app is dropped (and
 * heals via the replay protocol below) rather than back-pressuring the sequencer.
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

    // How long a self-check replay may go unanswered before it is abandoned and started over.
    private static final long SELF_CHECK_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(2);

    // Bounds the length the self-check asks the archive to replay.
    private static final long SELF_CHECK_REPLAY_LENGTH = 4096;

    static final int MAX_CONCURRENT_REPLAYS = 4;
    public static final long NO_REPLAY_NEEDED = NULL_VALUE;

    // Idle-TTL slot reclamation (see class Javadoc): a slot untouched this long is reclaimed.
    static final long REPLAY_SLOT_TTL_MS = 5_000;

    // While STALLED, probe the local archive (for replay).
    private static final long STALL_RETRY_INTERVAL_MS = 1_000;

    // Bounds {@link #offerControl}'s retry spin,
    private static final int MAX_CONTROL_OFFER_SPINS = 1_000;

    // Window {@link ReplayClientIdCollisions} counts {@code requestId} regressions over
    private static final long CLIENT_ID_COLLISION_WINDOW_MS = 10_000;

    // Quiet period that ends a control-reply-drop episode (see {@link #onControlReplyDropped}).
    private static final long CONTROL_DROP_QUIET_MS = 60_000;

    private static final int FRAGMENT_LIMIT = 16;

    /** The node this serves: its archive, its two app-facing IPC streams, its counters, its clock. */
    private final Replayer replayer;
    private final int memberId;
    private final IdleStrategy idleStrategy;

    /** Brings the whole process down; wired by {@link ReplayerServer}. See {@link #fatalDutyCycleFailure}. */
    private final Runnable fatalHandler;

    // The SequencerService's tap recording is visible on the local archive AND has passed the startup integrity check.
    private boolean ready = false;

    private boolean integrityFailed = false;
    private boolean staleActiveRecordingLogged = false;
    private boolean stalled = false;

    private long lastStallRetryMs = 0;

    // Startup integrity self-check (see checkReady)
    private Replayer.SelfCheckStream selfCheckSub;
    private List<ReplayRecordings.RecordingSpan> selfCheckSpans;
    private int selfCheckIndex;
    private long selfCheckReplaySessionId = NULL_VALUE;
    private long selfCheckRecordingId = NULL_VALUE; // the span being peeked
    private long selfCheckActiveRecordingId = NULL_VALUE; // the live one, for the readiness line
    private long selfCheckGlobalSeqNo = NULL_VALUE; // what the first fragment carried, once read
    private long selfCheckDeadlineNs = 0;


    // When the last dropped control reply was (see onControlReplyDropped), 0 = none this process.
    private long lastControlDropMs = 0;

    // Replay protocol state
    private final ReplaySlotAllocator replaySlots = new ReplaySlotAllocator(MAX_CONCURRENT_REPLAYS, REPLAY_SLOT_TTL_MS);

    private final ReplayClientIdCollisions clientIdCollisions =
        new ReplayClientIdCollisions(CLIENT_ID_COLLISION_WINDOW_MS);

    // Operator counters (see SeqeronCounters)
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

    private final org.limitless.seqeron.replayer.client.SequencedFrameDecoder selfCheckView =
        new org.limitless.seqeron.replayer.client.SequencedFrameDecoder();

    private final FragmentHandler requestHandler =
        (buffer, offset, length, header) -> onRequest(buffer, offset, length);

    private final FragmentHandler selfCheckHandler =
        (buffer, offset, length, header) -> onSelfCheckFragment(buffer, offset, length);

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

        this.stalledCounter = replayer.newCounter(SeqeronCounters.REPLAYER_STALLED_TYPE_ID,
                                                  "seqeron.replayer.stalled member=" + memberId);
        this.readyCounter =
            replayer.newCounter(SeqeronCounters.REPLAYER_READY_TYPE_ID, "seqeron.replayer.ready member=" + memberId);
        this.activeReplaySlotsCounter = replayer.newCounter(SeqeronCounters.REPLAYER_ACTIVE_SLOTS_TYPE_ID,
                                                            "seqeron.replayer.activeSlots member=" + memberId);
        this.pendingRequestsCounter = replayer.newCounter(SeqeronCounters.REPLAYER_PENDING_REQUESTS_TYPE_ID,
                                                          "seqeron.replayer.pendingRequests member=" + memberId);
        this.replaysServedCounter = replayer.newCounter(SeqeronCounters.REPLAYER_REPLAYS_SERVED_COUNT_TYPE_ID,
                                                        "seqeron.replayer.replaysServedCount member=" + memberId);
        this.idleTtlReclaimedCounter =
            replayer.newCounter(SeqeronCounters.REPLAYER_IDLE_TTL_RECLAIMED_COUNT_TYPE_ID,
                                "seqeron.replayer.idleTtlReclaimedCount member=" + memberId);
        this.integrityFailureCounter = replayer.newCounter(SeqeronCounters.REPLAYER_INTEGRITY_FAILURE_TYPE_ID,
                                                           "seqeron.replayer.integrityFailure member=" + memberId);
        this.controlRepliesDroppedCounter =
            replayer.newCounter(SeqeronCounters.REPLAYER_CONTROL_REPLIES_DROPPED_COUNT_TYPE_ID,
                                "seqeron.replayer.controlRepliesDroppedCount member=" + memberId);
        this.clientIdCollisionCounter = replayer.newCounter(SeqeronCounters.REPLAYER_CLIENT_ID_COLLISION_TYPE_ID,
                                                            "seqeron.replayer.clientIdCollision member=" + memberId);
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
            selfCheckSub = replayer.openSelfCheckStream();
            selfCheckReplaySessionId =
                replayer.startReplay(span.recordingId(), span.startPosition(), replayLength, SELF_CHECK_STREAM_ID);
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
     * @param length fragment length
     */
    private void onSelfCheckFragment(final DirectBuffer buffer, final int offset, final int length) {
        // Any of the five sequenced shapes will do: the check only asks what globalSeqNo the recording
        // starts at, and every one of them carries it at the same offset (F-3).
        if (selfCheckView.wrap(buffer, offset, length)) {
            selfCheckGlobalSeqNo = selfCheckView.globalSeqNo();
        }
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
     * seqeron.replayer.stalled} stayed at 1 for the rest of the process however healthy the archive had
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
                     "two co-located apps are both using SEQERON_REPLAYER_CLIENT_ID=%d — their requestId "
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
                             + "unaffected — see seqeron.replayer.controlRepliesDroppedCount",
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
