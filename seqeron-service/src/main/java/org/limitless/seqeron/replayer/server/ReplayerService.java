package org.limitless.seqeron.replayer.server;

import static io.aeron.Aeron.NULL_VALUE;

import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import io.aeron.archive.client.AeronArchive;
import io.aeron.logbuffer.FragmentHandler;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.limitless.seqeron.sbe.replay.ReplayClientIdInUseEncoder;
import org.limitless.seqeron.sbe.replay.ReplayCompleteDecoder;
import org.limitless.seqeron.sbe.replay.ReplayHeartbeatDecoder;
import org.limitless.seqeron.sbe.replay.ReplayPendingEncoder;
import org.limitless.seqeron.sbe.replay.ReplayRequestDecoder;
import org.limitless.seqeron.sbe.replay.ReplayUnavailableEncoder;
import org.limitless.seqeron.sbe.replay.ReplayingEncoder;
import org.limitless.seqeron.sbe.replay.SnapshotLocationEncoder;
import org.limitless.seqeron.sbe.replay.SnapshotQueryDecoder;
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

    /**
     * Internal IPC stream the snapshot index's replay of the active recording arrives on. 206 is {@code
     * AeronTapRelay}'s, which shares a gateway host's driver with this service.
     */
    static final int INDEX_STREAM_ID = 207;

    // Frames the index reads per duty cycle: enough to keep up with the tap, and to rebuild after a restart.
    private static final int INDEX_FRAGMENT_LIMIT = 256;

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
    private long selfCheckReplaySessionId = NULL_VALUE;
    private long selfCheckRecordingId = NULL_VALUE;
    private long selfCheckGlobalSeqNo = NULL_VALUE; // what the first fragment carried, once read
    private long selfCheckDeadlineMs = 0;


    // Each source's sequenced snapshot ends (doc/snapshot.md §5), fed by a replay of the active recording that
    // follows it live; a tap subscription would be untethered and could miss one.
    private final SnapshotIndex snapshotIndex = new SnapshotIndex(this::onSnapshotIndexed);
    private final Map<Integer, AtomicCounter> snapshotRoundCounters = new HashMap<>();
    private Replayer.IndexStream indexStream;

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
    private final ReplayClientIdInUseEncoder clientIdInUseEncoder = new ReplayClientIdInUseEncoder();
    private final SnapshotQueryDecoder snapshotQueryDecoder = new SnapshotQueryDecoder();
    private final SnapshotLocationEncoder snapshotLocationEncoder = new SnapshotLocationEncoder();
    private final MutableDirectBuffer controlBuffer = new ExpandableArrayBuffer(64);

    private final org.limitless.seqeron.protocol.SequencedFrameDecoder selfCheckView =
        new org.limitless.seqeron.protocol.SequencedFrameDecoder();

    private final FragmentHandler requestHandler =
        (buffer, offset, length, header) -> onRequest(buffer, offset, length);

    private final FragmentHandler selfCheckHandler =
        (buffer, offset, length, header) -> onSelfCheckFragment(buffer, offset, length);

    private final Replayer.IndexFrameHandler indexHandler = snapshotIndex::onFrame;

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
                    "starting on the co-located archive…");
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
        closeIndex();
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
        final String archiveError = replayer.pollArchive();
        if (archiveError != null) {
            // The session is the archive's to close and cannot be reopened, so no replay could be served again.
            throw new IllegalStateException("the local archive's control session failed: " + archiveError);
        }
        int work = replayer.pollRequests(requestHandler, FRAGMENT_LIMIT);
        if (!ready) {
            work += checkReady();
        } else if (stalled) {
            probeArchive();
        } else {
            work += pollIndex();
        }
        reclaimIdleSlots();
        activeReplaySlotsCounter.set(replaySlots.activeCount());
        pendingRequestsCounter.set(replaySlots.pendingCount());
        return work;
    }

    /**
     * Waits for the tap recording to be visible, then proves it starts at globalSeqNo 1 before declaring
     * readiness: it alone is served, so it must hold the whole log. A failure means the recording was deleted,
     * corrupted or partially restored — permanent, since a retry reads the same bytes — so {@link
     * #integrityFailed} latches and {@code ready} never becomes true for this process.
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
     * Opens the self-check replay of the active recording's first frame, bounded to {@link
     * #SELF_CHECK_REPLAY_LENGTH} since only one fragment is read. Every early return is transient and
     * retries next cycle.
     */
    private void startSelfCheck() {
        try {
            final Replayer.RecordingSpan active = findActiveRecording();
            if (active == null) {
                return; // nothing recorded yet; retry next cycle
            }
            final long replayLength =
                Math.min(tipOf(active.recordingId()) - active.startPosition(), SELF_CHECK_REPLAY_LENGTH);
            if (replayLength <= 0) {
                return; // nothing written to it yet; retry next cycle
            }
            selfCheckRecordingId = active.recordingId();
            selfCheckGlobalSeqNo = NULL_VALUE;
            selfCheckReplaySessionId =
                replayer.startReplay(active.recordingId(), active.startPosition(), replayLength, SELF_CHECK_STREAM_ID);
            selfCheckSub = replayer.openSelfCheckStream(selfCheckReplaySessionId);
            selfCheckDeadlineMs = replayer.nowMs() + SELF_CHECK_TIMEOUT_MS;
            onArchiveRecovered(); // it served a replay: whatever refused one earlier is over
        } catch (final RuntimeException ex) {
            closeSelfCheck();
            onArchiveStalled("running the startup self-check", ex);
        }
    }

    /**
     * Reads at most one fragment off an in-flight self-check. A first frame at globalSeqNo 1 makes the node
     * ready; anything else latches {@link #integrityFailed}. Nothing before the deadline is transient: the
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
                         "FATAL: tap recording %d has first frame globalSeqNo=%d, expected 1 — this node's recording "
                             + "does not cover the log from the start (deleted, corrupted, or a partial restore?); "
                             + "refusing to mark ready",
                         recordingId, firstGlobalSeqNo);
            return work;
        }
        ready = true;
        readyCounter.set(1);
        Logger.info(Logger.CoreComponent.ReplayerService, memberId,
                    "ready — tap recording %d live and verified from globalSeqNo 1; serving replay", recordingId);
        return work;
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

    // Snapshot index

    /**
     * Reads the next frames of the active recording into the index, opening its replay from the recording's
     * start first. A replay that ends — the archive dropped it — is opened again and the index rebuilt.
     * @return fragments read
     */
    private int pollIndex() {
        if (indexStream == null) {
            try {
                final Replayer.RecordingSpan active = findActiveRecording();
                if (active == null) {
                    return 0;
                }
                snapshotIndex.reset();
                indexStream = replayer.openIndexStream(active.recordingId(), active.startPosition());
                onArchiveRecovered();
            } catch (final RuntimeException ex) {
                onArchiveStalled("starting the snapshot index's replay", ex);
                return 0;
            }
        }
        final int work = indexStream.poll(indexHandler, INDEX_FRAGMENT_LIMIT);
        if (indexStream.isEnded()) {
            closeIndex();
        }
        return work;
    }

    private void closeIndex() {
        if (indexStream != null) {
            indexStream.close();
            indexStream = null;
        }
    }

    /** One source's newest snapshot end changed: its round counter follows. */
    private void onSnapshotIndexed(final int sourceId, final SnapshotIndex.Entry entry) {
        snapshotRoundCounters.computeIfAbsent(sourceId, source -> replayer.newSourceCounter(
            SeqeronCounters.REPLAYER_SNAPSHOT_ROUND_TYPE_ID,
            "seqeron.replayer.snapshotRound source=" + source + " member=" + memberId, source)).set(entry.round());
    }

    /**
     * Answers a {@code SnapshotQuery} from the index as it stands. A node still rebuilding it after a restart
     * answers with none for a round it has not reached, and the client tries an older one; holding the query would
     * be an unbounded wait.
     */
    private void onSnapshotQuery(final int clientId, final long requestId, final int sourceId, final long round) {
        if (integrityFailed) {
            sendUnavailable(clientId, requestId);
            return;
        }
        if (!ready) {
            sendPending(clientId, requestId);
            return;
        }
        final SnapshotIndex.Entry entry = snapshotIndex.lookup(sourceId, round);
        snapshotLocationEncoder.wrapAndApplyHeader(controlBuffer, 0, outHeaderEncoder)
            .clientId(clientId)
            .requestId(requestId)
            .round(entry == null ? NULL_VALUE : entry.round())
            .asOfGlobalSeqNo(entry == null ? NULL_VALUE : entry.asOfGlobalSeqNo())
            .asOfPosition(entry == null ? NULL_VALUE : entry.asOfPosition())
            .formatVersion(entry == null ? 0 : entry.formatVersion())
            .recordCount(entry == null ? 0 : entry.recordCount())
            .length(entry == null ? 0 : entry.length())
            .crc32c(entry == null ? 0 : entry.crc32c());
        offerControl(MessageHeaderEncoder.ENCODED_LENGTH + snapshotLocationEncoder.encodedLength());
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
        if (inHeaderDecoder.templateId() == SnapshotQueryDecoder.TEMPLATE_ID) {
            snapshotQueryDecoder.wrap(buffer, offset + MessageHeaderDecoder.ENCODED_LENGTH,
                                      inHeaderDecoder.blockLength(), inHeaderDecoder.version());
            onSnapshotQuery(snapshotQueryDecoder.clientId(), snapshotQueryDecoder.requestId(),
                            snapshotQueryDecoder.sourceId(), snapshotQueryDecoder.round());
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
            replaySlots.enqueue(clientId, requestId, fromPosition);
            sendPending(clientId, requestId);
            Logger.info(Logger.CoreComponent.ReplayerService, memberId,
                        "client %d queued: no free replay slot (active=%d/%d, pending=%d)", clientId,
                        replaySlots.activeCount(), MAX_CONCURRENT_REPLAYS, replaySlots.pendingCount());
            return;
        }
        startReplayForClient(clientId, requestId, fromPosition);
        drainPending();
    }

    /**
     * Serves one replay request, guarding every archive control call.
     * @param clientId client identity
     * @param requestId the request being answered, echoed in every reply
     * @param fromPosition start replay position, or {@link ReplayProtocol#FROM_START}
     */
    private void startReplayForClient(final int clientId, final long requestId, final long fromPosition) {
        if (stalled) {
            final long now = replayer.epochMillis();
            if ((now - lastStallRetryMs) < STALL_RETRY_INTERVAL_MS) {
                sendPending(clientId, requestId);
                return;
            }
            lastStallRetryMs = now; // this attempt is the paced probe
        }
        final boolean fromStart = fromPosition == ReplayProtocol.FROM_START;
        try {
            serveReplay(clientId, requestId, fromPosition);
            onArchiveRecovered();
        } catch (final RuntimeException error) {
            if (!fromStart && archiveAnswers()) {
                rejectResume(clientId, requestId, "archive refused it: " + error.getMessage());
                return;
            }
            onArchiveStalled("serving " + (fromStart ? "a replay from the start" : "a resume") + " for client " +
                                 clientId,
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
            final Replayer.RecordingSpan active = findActiveRecording();
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
     * here" and falls back to replaying from the start.
     * @param clientId client identity
     * @param requestId the request being refused
     * @param reason what was wrong with the position, for the log
     */
    private void rejectResume(final int clientId, final long requestId, final String reason) {
        Logger.info(Logger.CoreComponent.ReplayerService, memberId,
                    "client %d's resume refused (%s) — answering ReplayProtocol.NO_REPLAY_NEEDED so it replays from the start",
                    clientId, reason);
        sendReplaying(clientId, requestId, ReplayProtocol.NO_REPLAY_NEEDED, 0);
    }

    /**
     * Serves one replay of the active recording: from its start for {@link ReplayProtocol#FROM_START}, else
     * resumed at {@code fromPosition}. Bounded at the recording's tip now; the tap carries the rest.
     * @param clientId client identity
     * @param requestId the request being answered, echoed in every reply
     * @param fromPosition start position, or {@link ReplayProtocol#FROM_START}
     */
    private void serveReplay(final int clientId, final long requestId, final long fromPosition) {
        replaySlots.cancelPending(clientId);

        final Replayer.RecordingSpan active = findActiveRecording();
        if (active == null) {
            replaySlots.enqueue(clientId, requestId, fromPosition);
            sendPending(clientId, requestId);
            return;
        }
        final long replayFrom = fromPosition == ReplayProtocol.FROM_START ? active.startPosition() : fromPosition;
        if (replayFrom < active.startPosition()) {
            rejectResume(clientId, requestId,
                         "position " + fromPosition + " predates recording " + active.recordingId() +
                             "'s startPosition " + active.startPosition());
            return;
        }

        final long recordingId = active.recordingId();
        final long tip = tipOf(recordingId);
        if (tip < 0) {
            replaySlots.enqueue(clientId, requestId, fromPosition);
            sendPending(clientId, requestId);
            return;
        }
        final long boundedLength = tip - replayFrom;
        if (boundedLength <= 0) {
            sendReplaying(clientId, requestId, ReplayProtocol.NO_REPLAY_NEEDED, tip);
            return;
        }

        final long replaySessionId = replayer.startReplay(recordingId, replayFrom, boundedLength, ReplayProtocol.REPLAY_STREAM_ID);
        replaysServedCounter.increment();
        replaySlots.activate(clientId, replaySessionId, replayer.epochMillis());
        Logger.info(Logger.CoreComponent.ReplayerService, memberId,
                    "replay for client %d: recording %d [%d,%d) session %d", clientId, recordingId, replayFrom, tip,
                    replaySessionId);
        sendReplaying(clientId, requestId, replaySessionId, tip);
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
                startReplayForClient(request.clientId(), request.requestId(), request.fromPosition());
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
     */
    private void sendReplaying(final int clientId, final long requestId, final long replaySessionId,
                               final long catchUpPosition) {
        replayingEncoder.wrapAndApplyHeader(controlBuffer, 0, outHeaderEncoder)
            .clientId(clientId)
            .requestId(requestId)
            .replaySessionId(replaySessionId)
            .catchUpPosition(catchUpPosition);
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
     * Two co-located apps share one client id (see {@link ReplayClientIdCollisions}). Their requests are still
     * served — they already livelock each other — but both are told, and a client that is told fails its duty
     * cycle.
     * @param clientId the id being used twice
     */
    private void onClientIdCollision(final int clientId) {
        clientIdInUseEncoder.wrapAndApplyHeader(controlBuffer, 0, outHeaderEncoder).clientId(clientId);
        offerControl(MessageHeaderEncoder.ENCODED_LENGTH + clientIdInUseEncoder.encodedLength());
        clientIdCollisionCounter.set(1);
        Logger.error(Logger.CoreComponent.ReplayerService, Logger.CoreEventCode.ReplayClientIdCollision, memberId,
                     "two co-located apps are both using SEQERON_REPLAYER_CLIENT_ID=%d — their requestId "
                         + "sequences interleave, so each request stops the other's replay and NEITHER will "
                         + "ever catch up; both are told, and fail — give them distinct ids",
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

    /**
     * The newest active tap recording on the local archive, or null. More than one active is an unclean
     * shutdown's leftover, reported once per episode; the newest holds the older one's content.
     */
    private Replayer.RecordingSpan findActiveRecording() {
        Replayer.RecordingSpan found = null;
        int activeCount = 0;
        for (final Replayer.RecordingSpan span : replayer.listTapRecordings()) {
            if (span.active()) {
                ++activeCount;
                if (found == null || span.recordingId() > found.recordingId()) {
                    found = span;
                }
            }
        }
        if (activeCount > 1 && !staleActiveRecordingLogged) {
            Logger.error(Logger.CoreComponent.ReplayerService, Logger.CoreEventCode.StaleActiveRecording, memberId,
                         "%d tap recordings report as still recording — an unclean shutdown left an older one "
                             + "unstopped; serving the newest",
                         activeCount);
        }
        staleActiveRecordingLogged = activeCount > 1;
        return found;
    }

    /** Where a recording has reached: its live position, else where it stopped; negative if neither is known. */
    private long tipOf(final long recordingId) {
        final long position = replayer.recordingPosition(recordingId);
        return position >= 0 ? position : replayer.stopPosition(recordingId);
    }
}
