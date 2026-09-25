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
import org.limitless.seqeron.protocol.ReplayProtocol;
import org.limitless.seqeron.protocol.SeqeronCounters;
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
 * Per-node archive <b>replay server</b> for co-located app replicas. Not on the live path: apps read the
 * tap directly, untethered, and heal a gap through the replay protocol this serves.
 */
public final class ReplayerService {
    /**
     * Internal IPC stream the startup self-check replays onto, distinct from the app-facing replay stream so
     * the two can never cross-talk. Package-private: {@link AeronReplayer} subscribes to it.
     */
    static final int SELF_CHECK_STREAM_ID = 204;

    // How long a self-check replay may go unanswered before it is abandoned and started over.
    private static final long SELF_CHECK_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(2);

    // Bounds the length the self-check asks the archive to replay.
    private static final long SELF_CHECK_REPLAY_LENGTH = 4096;

    static final int MAX_CONCURRENT_REPLAYS = 4;

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
    private long selfCheckDeadlineMs = 0;


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

    private final org.limitless.seqeron.protocol.SequencedFrameDecoder selfCheckView =
        new org.limitless.seqeron.protocol.SequencedFrameDecoder();

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
     * Serves the {@link Replayer}.
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

        this.stalledCounter = counter(SeqeronCounters.REPLAYER_STALLED_TYPE_ID, "stalled");
        this.readyCounter = counter(SeqeronCounters.REPLAYER_READY_TYPE_ID, "ready");
        this.activeReplaySlotsCounter = counter(SeqeronCounters.REPLAYER_ACTIVE_SLOTS_TYPE_ID, "activeSlots");
        this.pendingRequestsCounter = counter(SeqeronCounters.REPLAYER_PENDING_REQUESTS_TYPE_ID, "pendingRequests");
        this.replaysServedCounter =
            counter(SeqeronCounters.REPLAYER_REPLAYS_SERVED_COUNT_TYPE_ID, "replaysServedCount");
        this.idleTtlReclaimedCounter =
            counter(SeqeronCounters.REPLAYER_IDLE_TTL_RECLAIMED_COUNT_TYPE_ID, "idleTtlReclaimedCount");
        this.integrityFailureCounter = counter(SeqeronCounters.REPLAYER_INTEGRITY_FAILURE_TYPE_ID, "integrityFailure");
        this.controlRepliesDroppedCounter =
            counter(SeqeronCounters.REPLAYER_CONTROL_REPLIES_DROPPED_COUNT_TYPE_ID, "controlRepliesDroppedCount");
        this.clientIdCollisionCounter =
            counter(SeqeronCounters.REPLAYER_CLIENT_ID_COLLISION_TYPE_ID, "clientIdCollision");
    }

    /**
     * One operator counter, labelled {@code seqeron.replayer.<name> member=<memberId>} so a reader
     * disambiguates nodes sharing one host.
     * @param typeId which counter, from {@link SeqeronCounters}
     * @param name   its name within the {@code seqeron.replayer} namespace
     */
    private AtomicCounter counter(final int typeId, final String name) {
        return replayer.newCounter(typeId, "seqeron.replayer." + name + " member=" + memberId);
    }

    /**
     * Blocks running the duty cycle until {@code running} goes false. Serves the replay protocol from
     * the local archive; the live feed is the tap the apps read directly, not this process.
     * @param running true while running
     */
    public void run(final AtomicBoolean running) {
        Logger.info(Logger.CoreComponent.ReplayerService, memberId,
                    "starting; serving replay from the co-located archive…");
        try {
            while (running.get()) {
                final int work = poll();
                idleStrategy.idle(work);
            }
        } catch (final RuntimeException ex) {
            fatalDutyCycleFailure(ex);
        }
        Logger.info(Logger.CoreComponent.ReplayerService, memberId, "shutting down");
        stopAllReplays();
        closeSelfCheck(); // a check still in flight owns an archive replay and a subscription
    }

    /**
     * The duty cycle must never die silently: the process would keep running with {@code ready} latched at 1
     * while nothing polls requests. It dies loudly and supervision restarts it; this server holds no state.
     * @param ex what killed the duty cycle
     */
    private void fatalDutyCycleFailure(final RuntimeException ex) {
        ready = false;
        readyCounter.set(0);
        Logger.fault(Logger.CoreComponent.ReplayerService, Logger.CoreEventCode.ReplayDutyCycleFailure, memberId,
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
     * Waits for the tap recording to be visible, then proves every recording in the chain starts at
     * globalSeqNo 1 before declaring readiness. A failure means a recording was deleted, corrupted or
     * partially restored — permanent, since a retry reads the same bytes — so {@link #integrityFailed}
     * latches and {@code ready} never becomes true for this process.
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
     * Opens the self-check replay of the current span's first frame, bounded to {@link
     * #SELF_CHECK_REPLAY_LENGTH} since only one fragment is read. Every early return is transient and
     * retries next cycle.
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
            selfCheckSub = replayer.openSelfCheckStream(selfCheckReplaySessionId);
            selfCheckDeadlineMs = replayer.nowMs() + SELF_CHECK_TIMEOUT_MS;
            onArchiveRecovered(); // it served a replay: whatever refused one earlier is over
        } catch (final RuntimeException ex) {
            closeSelfCheck();
            onArchiveStalled("running the startup self-check", ex);
        }
    }

    /**
     * Reads at most one fragment off an in-flight self-check. A first frame at globalSeqNo 1 proves that
     * span; anything else latches {@link #integrityFailed}. Nothing before the deadline is transient: the
     * check is torn down and restarted on a later cycle.
     */
    private int pollSelfCheck() {
        final int work = selfCheckSub.poll(selfCheckHandler, 1);
        if (selfCheckGlobalSeqNo == NULL_VALUE) {
            if (replayer.nowMs() > selfCheckDeadlineMs) {
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
            Logger.fault(Logger.CoreComponent.ReplayerService, Logger.CoreEventCode.ArchiveIntegrityFailure, memberId,
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
        Logger.info(Logger.CoreComponent.ReplayerService, memberId,
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
        // Any of the five sequenced messages will do: the check only asks what globalSeqNo the recording
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

        if (inHeaderDecoder.templateId() == ReplayCompleteDecoder.TEMPLATE_ID) {
            replayCompleteDecoder.wrap(buffer, offset + MessageHeaderDecoder.ENCODED_LENGTH,
                                       inHeaderDecoder.blockLength(), inHeaderDecoder.version());
            stopReplayForClient(replayCompleteDecoder.clientId());
            drainPending();
            return;
        }
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
            Logger.info(Logger.CoreComponent.ReplayerService, memberId,
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
     * Re-probes the local archive while STALLED, so the state clears even when no app is asking for a
     * replay. Probes with a bounded replay, since serving one is what STALLED says the archive cannot do,
     * and shares {@link #STALL_RETRY_INTERVAL_MS} with the request path: at most one attempt a second.
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
     * Refuses a resume request with NO_REPLAY_NEEDED, which the app reads as "that position is no good
     * here" and falls back to walking the chain.
     * @param clientId client identity
     * @param requestId the request being refused
     * @param reason what was wrong with the position, for the log
     */
    private void rejectResume(final int clientId, final long requestId, final String reason) {
        Logger.info(Logger.CoreComponent.ReplayerService, memberId,
                    "client %d's resume refused (%s) — answering ReplayProtocol.NO_REPLAY_NEEDED so it re-walks the chain", clientId,
                    reason);
        sendReplaying(clientId, requestId, ReplayProtocol.NO_REPLAY_NEEDED, 0, NULL_VALUE);
    }

    /**
     * Serves one replay. {@code segmentIndex < 0} resumes the active recording at {@code fromPosition};
     * otherwise it is one step of a cold-start walk over the recording chain, answered NO_REPLAY_NEEDED
     * once the walk runs past the last recording.
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
                replaySlots.enqueue(clientId, requestId, segmentIndex, fromPosition);
                sendPending(clientId, requestId);
                return;
            }
            if (fromPosition < active.startPosition()) {
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
                replaySlots.enqueue(clientId, requestId, segmentIndex, fromPosition);
                sendPending(clientId, requestId);
                return;
            }
            if (segmentIndex >= segments.size()) {
                sendReplaying(clientId, requestId, ReplayProtocol.NO_REPLAY_NEEDED, 0, NULL_VALUE);
                return;
            }

            final ReplayRecordings.RecordingSpan segment = segments.get(segmentIndex);
            recordingId = segment.recordingId();
            replayFrom = segment.startPosition();
        }

        long tip = replayer.recordingPosition(recordingId);
        if (tip < 0) {
            tip = replayer.stopPosition(recordingId);
        }
        if (tip < 0) {
            replaySlots.enqueue(clientId, requestId, segmentIndex, fromPosition);
            sendPending(clientId, requestId);
            return;
        }
        final long boundedLength = tip - replayFrom;
        if (boundedLength <= 0) {
            sendReplaying(clientId, requestId, ReplayProtocol.NO_REPLAY_NEEDED, tip, recordingId);
            return;
        }

        final long replaySessionId = replayer.startReplay(recordingId, replayFrom, boundedLength, ReplayProtocol.REPLAY_STREAM_ID);
        replaysServedCounter.increment();
        replaySlots.activate(clientId, replaySessionId, replayer.epochMillis());
        Logger.info(Logger.CoreComponent.ReplayerService, memberId,
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
     * Refuses a replay request: this node failed its integrity check. Not logged per refusal — checkReady
     * reported it once, and apps resend every 500ms.
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
     * Offers one control reply, dropping it at {@link #MAX_CONTROL_OFFER_SPINS} rather than spinning without
     * bound. Safe, unlike on the tap: the app resends on a timer, so a drop costs one resend interval.
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
                if (result != ExclusivePublication.NOT_CONNECTED) {
                    onControlReplyDropped(result);
                }
                return;
            }
            idleStrategy.idle();
        }
    }

    /**
     * Two co-located apps share one client id (see {@link ReplayClientIdCollisions}). Reported, not refused:
     * they already livelock each other, and a false positive must not stop a healthy replica.
     * @param clientId the id being used twice
     */
    private void onClientIdCollision(final int clientId) {
        clientIdCollisionCounter.set(1);
        Logger.error(Logger.CoreComponent.ReplayerService, Logger.CoreEventCode.ReplayClientIdCollision, memberId,
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
            Logger.error(Logger.CoreComponent.ReplayerService, Logger.CoreEventCode.ControlReplyDropped, memberId,
                         "dropped a control reply (offer=%d): an app subscribed to stream %d and stopped "
                             + "reading it. Its replays are delayed by a resend; every other app is "
                             + "unaffected — see seqeron.replayer.controlRepliesDroppedCount",
                         result, ReplayProtocol.CONTROL_STREAM_ID);
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
            Logger.info(Logger.CoreComponent.ReplayerService, memberId,
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
            Logger.info(Logger.CoreComponent.ReplayerService, memberId, "RECOVERED: local archive reachable again");
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

    // Oldest-to-newest tap recordings on the local archive; normally one, spanning every leader tenure.
    private List<ReplayRecordings.RecordingSpan> resolveSegments() {
        final List<ReplayRecordings.RecordingSpan> spans = replayer.listTapRecordings();
        final long activeCount = spans.stream().filter(ReplayRecordings.RecordingSpan::active).count();
        if (activeCount > 1 && !staleActiveRecordingLogged) {
            Logger.error(Logger.CoreComponent.ReplayerService, Logger.CoreEventCode.StaleActiveRecording, memberId,
                         "%d tap recordings report as still recording — an unclean shutdown left an older one "
                             + "unstopped; serving the newest and skipping the stale one(s)",
                         activeCount);
        }
        staleActiveRecordingLogged = activeCount > 1;
        return ReplayRecordings.stitch(spans);
    }
}
