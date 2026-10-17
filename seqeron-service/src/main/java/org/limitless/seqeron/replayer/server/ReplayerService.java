package org.limitless.seqeron.replayer.server;

import static io.aeron.Aeron.NULL_VALUE;

import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import io.aeron.archive.client.AeronArchive;
import io.aeron.logbuffer.FragmentHandler;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongPredicate;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.status.AtomicCounter;
import org.limitless.seqeron.protocol.ReplayProtocol;
import org.limitless.seqeron.protocol.SeqeronCounters;
import org.limitless.seqeron.sbe.replay.MessageHeaderDecoder;
import org.limitless.seqeron.sbe.replay.MessageHeaderEncoder;
import org.limitless.seqeron.sbe.replay.ReplayPendingEncoder;
import org.limitless.seqeron.sbe.replay.ReplayRequestDecoder;
import org.limitless.seqeron.sbe.replay.ReplayingEncoder;
import org.limitless.seqeron.sequencer.SequencerService;
import org.limitless.seqeron.util.Logger;

/**
 * Per-node archive <b>replay server</b> for co-located app replicas. Not on the live path: apps read the
 * tap directly, untethered, and heal a gap through the replay protocol this serves.
 */
public final class ReplayerService {
    static final int MAX_CONCURRENT_REPLAYS = 4;

    // How long the Replayer's own archive call (readiness) waits after one threw.
    private static final long ARCHIVE_RETRY_INTERVAL_MS = 1_000;

    // Bounds {@link #offerControl}'s retry spin,
    private static final int MAX_CONTROL_OFFER_SPINS = 1_000;

    // Quiet period that ends an episode of dropped control replies or archive errors, each logged once per episode.
    private static final long EPISODE_QUIET_MS = 60_000;

    private static final int FRAGMENT_LIMIT = 16;

    /** The node this serves: its archive, its two app-facing IPC streams, its counters, its clock. */
    private final Replayer replayer;
    private final int memberId;
    private final IdleStrategy idleStrategy;

    /** Brings the whole process down; wired by {@link ReplayerServer}. See {@link #fatalDutyCycleFailure}. */
    private final Runnable fatalHandler;

    // The SequencerService's tap recording is visible on the local archive.
    private boolean ready = false;

    /** The recording served while it was reported alongside a stale active one, else NULL_VALUE. */
    private long staleReportedFor = NULL_VALUE;

    /** The active tap recording, kept while it is still recording; a node starts a new one only on restart. */
    private Replayer.RecordingSpan activeRecording;

    // When the Replayer's own next archive call may be made, after one threw.
    private long archiveRetryAtMs = 0;

    // When the last dropped control reply was (see onControlReplyDropped), 0 = none this process.
    private long lastControlDropMs = 0;

    // When the last archive call threw (see onArchiveError), 0 = none this process.
    private long lastArchiveErrorMs = 0;

    // Replay protocol state
    private final ReplaySlotAllocator replaySlots = new ReplaySlotAllocator(MAX_CONCURRENT_REPLAYS);

    // Operator counters (see SeqeronCounters)
    private final AtomicCounter archiveErrorsCounter;
    private final AtomicCounter readyCounter;
    private final AtomicCounter activeReplaySlotsCounter;
    private final AtomicCounter pendingRequestsCounter;
    private final AtomicCounter replaysServedCounter;
    private final AtomicCounter controlRepliesDroppedCounter;
    private final AtomicCounter throttledRequestsCounter;

    private final MessageHeaderDecoder inHeaderDecoder = new MessageHeaderDecoder();
    private final ReplayRequestDecoder replayRequestDecoder = new ReplayRequestDecoder();
    private final MessageHeaderEncoder outHeaderEncoder = new MessageHeaderEncoder();
    private final ReplayingEncoder replayingEncoder = new ReplayingEncoder();
    private final ReplayPendingEncoder pendingEncoder = new ReplayPendingEncoder();
    private final MutableDirectBuffer controlBuffer = new ExpandableArrayBuffer(64);

    private final FragmentHandler requestHandler =
        (buffer, offset, length, header) -> onRequest(buffer, offset, length);

    private final LongPredicate replayLive;

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
        this.replayLive = replayer::isReplayLive;

        this.archiveErrorsCounter = counter(SeqeronCounters.REPLAYER_ARCHIVE_ERRORS_COUNT_TYPE_ID, "archiveErrorsCount");
        this.readyCounter = counter(SeqeronCounters.REPLAYER_READY_TYPE_ID, "ready");
        this.activeReplaySlotsCounter = counter(SeqeronCounters.REPLAYER_ACTIVE_SLOTS_TYPE_ID, "activeSlots");
        this.pendingRequestsCounter = counter(SeqeronCounters.REPLAYER_PENDING_REQUESTS_TYPE_ID, "pendingRequests");
        this.replaysServedCounter =
            counter(SeqeronCounters.REPLAYER_REPLAYS_SERVED_COUNT_TYPE_ID, "replaysServedCount");
        this.controlRepliesDroppedCounter =
            counter(SeqeronCounters.REPLAYER_CONTROL_REPLIES_DROPPED_COUNT_TYPE_ID, "controlRepliesDroppedCount");
        this.throttledRequestsCounter =
            counter(SeqeronCounters.REPLAYER_THROTTLED_REQUESTS_COUNT_TYPE_ID, "throttledRequestsCount");
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
        if (replayer.epochMillis() < archiveRetryAtMs) {
            // the Replayer's own archive call threw a moment ago; requests are still answered
        } else if (!ready) {
            work += checkReady();
        }
        reclaimEndedSlots();
        activeReplaySlotsCounter.set(replaySlots.activeCount());
        pendingRequestsCounter.set(replaySlots.pendingCount());
        return work;
    }

    /**
     * Waits for the tap recording to be visible on the local archive. It alone is served: every node start
     * replays the log from globalSeqNo 1 into a new one, and a client checks that its first frame is 1.
     * @return work count
     */
    private int checkReady() {
        try {
            final Replayer.RecordingSpan active = findActiveRecording();
            if (active == null) {
                return 0; // nothing recorded yet; retry next cycle
            }
            ready = true;
            readyCounter.set(1);
            Logger.info(Logger.CoreComponent.ReplayerService, memberId,
                        "ready — tap recording %d live; serving replay", active.recordingId());
            return 1;
        } catch (final RuntimeException ex) {
            onOwnArchiveCallFailed("listing the tap recordings", ex);
            return 0;
        }
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

        if (inHeaderDecoder.templateId() != ReplayRequestDecoder.TEMPLATE_ID) {
            return;
        }
        replayRequestDecoder.wrap(buffer, offset + MessageHeaderDecoder.ENCODED_LENGTH, inHeaderDecoder.blockLength(),
                                  inHeaderDecoder.version());
        final long clientId = replayRequestDecoder.clientId();
        final long requestId = replayRequestDecoder.requestId();
        final long fromPosition = replayRequestDecoder.fromPosition();

        if (!ready) {
            sendPending(clientId, requestId);
            return;
        }
        stopReplayForClient(clientId);

        final int limit = replayLimit();
        replaySlots.limit(limit);
        if (!replaySlots.hasCapacity()) {
            if (replaySlots.activeCount() < MAX_CONCURRENT_REPLAYS) {
                throttledRequestsCounter.increment();
            }
            replaySlots.addPending(clientId, requestId, fromPosition);
            sendPending(clientId, requestId);
            Logger.info(Logger.CoreComponent.ReplayerService, memberId,
                        "client %d pending: no free replay slot (active=%d/%d%s, pending=%d)", clientId,
                        replaySlots.activeCount(), limit,
                        limit < MAX_CONCURRENT_REPLAYS ? " while the tap recording is behind" : "",
                        replaySlots.pendingCount());
            return;
        }
        startReplayForClient(clientId, requestId, fromPosition);
        drainPending();
    }

    /**
     * Serves one replay request, guarding every archive control call. An archive that throws is answered
     * {@code ReplayPending}: the client resends, and each resend is the retry.
     * @param clientId client identity
     * @param requestId the request being answered, echoed in every reply
     * @param fromPosition start replay position, or {@link ReplayProtocol#FROM_START}
     */
    private void startReplayForClient(final long clientId, final long requestId, final long fromPosition) {
        final boolean fromStart = fromPosition == ReplayProtocol.FROM_START;
        try {
            serveReplay(clientId, requestId, fromPosition);
        } catch (final RuntimeException error) {
            if (!fromStart && archiveAnswers()) {
                rejectResume(clientId, requestId, "archive refused it: " + error.getMessage());
                return;
            }
            onArchiveError("serving " + (fromStart ? "a replay from the start" : "a resume") + " for client " +
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
     * Refuses a resume request with NO_REPLAY_NEEDED, which the app reads as "that position is no good
     * here" and treats as fatal.
     * @param clientId client identity
     * @param requestId the request being refused
     * @param reason what was wrong with the position, for the log
     */
    private void rejectResume(final long clientId, final long requestId, final String reason) {
        Logger.info(Logger.CoreComponent.ReplayerService, memberId,
                    "client %d's resume refused (%s) — answering ReplayProtocol.NO_REPLAY_NEEDED",
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
    private void serveReplay(final long clientId, final long requestId, final long fromPosition) {
        replaySlots.cancelPending(clientId);

        final Replayer.RecordingSpan active = findActiveRecording();
        if (active == null) {
            replaySlots.addPending(clientId, requestId, fromPosition);
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
            replaySlots.addPending(clientId, requestId, fromPosition);
            sendPending(clientId, requestId);
            return;
        }
        final long boundedLength = tip - replayFrom;
        if (boundedLength <= 0) {
            if (fromPosition == ReplayProtocol.FROM_START) {
                sendReplaying(clientId, requestId, ReplayProtocol.NO_REPLAY_NEEDED, tip);
            } else {
                sendPending(clientId, requestId); // the recording trails the tap the client resumed from
            }
            return;
        }

        final long replaySessionId = replayer.startReplay(recordingId, replayFrom, boundedLength, ReplayProtocol.REPLAY_STREAM_ID);
        replaysServedCounter.increment();
        replaySlots.activate(clientId, replaySessionId);
        Logger.info(Logger.CoreComponent.ReplayerService, memberId,
                    "replay for client %d: recording %d [%d,%d) session %d", clientId, recordingId, replayFrom, tip,
                    replaySessionId);
        sendReplaying(clientId, requestId, replaySessionId, tip);
    }

    /**
     * How many replays may run at once: one while the active recording trails the tap by more than a quarter of
     * the window it may trail by, since a replay competes with the recorder for the disk and the recorder holds the
     * sequencer back.
     */
    private int replayLimit() {
        if (replaySlots.activeCount() == 0) {
            return MAX_CONCURRENT_REPLAYS; // the next replay is the only one either way
        }
        try {
            final Replayer.RecordingSpan active = findActiveRecording();
            final Replayer.TapBacklog backlog = active == null ? null : replayer.tapBacklog(active);
            return backlog != null && backlog.bytes() > backlog.window() / 4 ? 1 : MAX_CONCURRENT_REPLAYS;
        } catch (final RuntimeException ex) {
            return MAX_CONCURRENT_REPLAYS; // serving the replay meets the same archive and reports it
        }
    }

    /**
     * Stops an active replay
     * @param clientId client identity
     */
    private void stopReplayForClient(final long clientId) {
        final long token = replaySlots.supersede(clientId);
        if (token != ReplaySlotAllocator.NO_SLOT) {
            stopReplay(token);
        }
    }

    /**
     * Frees the slots whose replay is over: the archive ended it and its client let it go, or the client died.
     */
    private void reclaimEndedSlots() {
        if (replaySlots.reclaimEnded(replayLive) > 0) {
            drainPending();
        }
    }

    /**
     * Drain pending requests
     */
    private void drainPending() {
        if (replaySlots.pendingCount() == 0) {
            return;
        }
        replaySlots.limit(replayLimit());
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
    private void sendReplaying(final long clientId, final long requestId, final long replaySessionId,
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
    private void sendPending(final long clientId, final long requestId) {
        pendingEncoder.wrapAndApplyHeader(controlBuffer, 0, outHeaderEncoder).clientId(clientId).requestId(requestId);
        offerControl(MessageHeaderEncoder.ENCODED_LENGTH + pendingEncoder.encodedLength());
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
     * A control reply was dropped at the spin bound: report it once per episode, count every one.
     * @param result the failing offer result
     */
    private void onControlReplyDropped(final long result) {
        controlRepliesDroppedCounter.increment();
        final long nowMs = replayer.epochMillis();
        final boolean newEpisode = lastControlDropMs == 0 || nowMs - lastControlDropMs >= EPISODE_QUIET_MS;
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
     * A local-archive control call threw: count every one, report it once per episode.
     * @param message what was being attempted
     * @param exception runtime exception
     */
    private void onArchiveError(final String message, final RuntimeException exception) {
        archiveErrorsCounter.increment();
        final long nowMs = replayer.epochMillis();
        final boolean newEpisode = lastArchiveErrorMs == 0 || nowMs - lastArchiveErrorMs >= EPISODE_QUIET_MS;
        lastArchiveErrorMs = nowMs;
        if (newEpisode) {
            Logger.info(Logger.CoreComponent.ReplayerService, memberId,
                        "the local archive refused %s (%s); live delivery unaffected (apps read the tap directly) "
                            + "— see seqeron.replayer.archiveErrorsCount",
                        message, exception.getMessage());
        }
    }

    /** One of the Replayer's own archive calls threw: as {@link #onArchiveError}, then wait before the next. */
    private void onOwnArchiveCallFailed(final String message, final RuntimeException exception) {
        onArchiveError(message, exception);
        archiveRetryAtMs = replayer.epochMillis() + ARCHIVE_RETRY_INTERVAL_MS;
    }

    /**
     * The newest active tap recording on the local archive, or null. More than one active is an unclean
     * shutdown's leftover, reported once per episode; the newest holds the older one's content. Listed again only
     * once the one found has stopped recording, so a request costs no listing.
     */
    private Replayer.RecordingSpan findActiveRecording() {
        if (activeRecording != null && replayer.recordingPosition(activeRecording.recordingId()) >= 0) {
            return activeRecording;
        }
        activeRecording = null;
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
        if (activeCount > 1 && found.recordingId() != staleReportedFor) {
            Logger.error(Logger.CoreComponent.ReplayerService, Logger.CoreEventCode.StaleActiveRecording, memberId,
                         "%d tap recordings report as still recording — an unclean shutdown left an older one "
                             + "unstopped; serving the newest",
                         activeCount);
        }
        staleReportedFor = activeCount > 1 ? found.recordingId() : NULL_VALUE;
        if (found != null && replayer.recordingPosition(found.recordingId()) >= 0) {
            activeRecording = found;
        }
        return found;
    }

    /** Where a recording has reached: its live position, else where it stopped; negative if neither is known. */
    private long tipOf(final long recordingId) {
        final long position = replayer.recordingPosition(recordingId);
        return position >= 0 ? position : replayer.stopPosition(recordingId);
    }
}
