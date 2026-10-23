package org.limitless.seqeron.replayer.server;

import static io.aeron.Aeron.NULL_VALUE;

import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import io.aeron.archive.client.AeronArchive;
import io.aeron.logbuffer.FragmentHandler;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongPredicate;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.limitless.seqeron.protocol.FrameLayer;
import org.limitless.seqeron.protocol.ReplayProtocol;
import org.limitless.seqeron.protocol.SeqeronCounters;
import org.limitless.seqeron.sbe.replay.MessageHeaderDecoder;
import org.limitless.seqeron.sbe.replay.MessageHeaderEncoder;
import org.limitless.seqeron.sbe.replay.QueueRequestDecoder;
import org.limitless.seqeron.sbe.replay.QueueResolvedEncoder;
import org.limitless.seqeron.sbe.replay.ReplayPendingEncoder;
import org.limitless.seqeron.sbe.replay.ReplayRequestDecoder;
import org.limitless.seqeron.sbe.replay.ReplayingEncoder;
import org.limitless.seqeron.sbe.replay.SnapshotConfirmedDecoder;
import org.limitless.seqeron.sbe.replay.SnapshotJoinedDecoder;
import org.limitless.seqeron.sbe.replay.SnapshotRoundEncoder;
import org.limitless.seqeron.sequencer.SequencerService;
import org.limitless.seqeron.util.Logger;

/**
 * Per-node archive <b>replay server</b> for co-located app replicas. Not on the live path: apps read the
 * queue directly, untethered, and heal a gap through the replay protocol this serves.
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

    // Every served queue's recording is visible on the local archive.
    private boolean ready = false;

    /** The sequenced topology rows, handed over by whoever publishes the default queue. */
    private final Queue<byte[]> topologyRows;
    private final UnsafeBuffer topologyRow = new UnsafeBuffer(0, 0);
    private final QueueResolver resolver;

    /** The sources a QueueRequest could not be answered for, each logged once. */
    private final Set<String> unresolvedLogged = new HashSet<>();

    /** Per queue, slot groupId − 1: the recording served while it was reported alongside a stale active one. */
    private final long[] staleReportedFor = new long[FrameLayer.MAX_GROUPS];

    /** Per queue, slot groupId − 1: its active recording, kept while it is still recording. */
    private final Replayer.RecordingSpan[] activeRecordings = new Replayer.RecordingSpan[FrameLayer.MAX_GROUPS];

    // When the Replayer's own next archive call may be made, after one threw.
    private long archiveRetryAtMs = 0;

    // When the last dropped control reply was (see onControlReplyDropped), 0 = none this process.
    private long lastControlDropMs = 0;

    // When the last archive call threw (see onArchiveError), 0 = none this process.
    private long lastArchiveErrorMs = 0;

    // Replay protocol state
    private final ReplaySlotAllocator replaySlots = new ReplaySlotAllocator(MAX_CONCURRENT_REPLAYS);
    private final CutLookup cutLookup = new CutLookup();

    /** The cuts to track, or null where there are none to: a gateway host. */
    private final CutIntake cutIntake;
    private final RestartSafety restartSafety;

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
    private final QueueRequestDecoder queueRequestDecoder = new QueueRequestDecoder();
    private final SnapshotJoinedDecoder snapshotJoinedDecoder = new SnapshotJoinedDecoder();
    private final SnapshotConfirmedDecoder snapshotConfirmedDecoder = new SnapshotConfirmedDecoder();
    private final SnapshotRoundEncoder snapshotRoundEncoder = new SnapshotRoundEncoder();
    private final MessageHeaderEncoder outHeaderEncoder = new MessageHeaderEncoder();
    private final ReplayingEncoder replayingEncoder = new ReplayingEncoder();
    private final ReplayPendingEncoder pendingEncoder = new ReplayPendingEncoder();
    private final QueueResolvedEncoder queueResolvedEncoder = new QueueResolvedEncoder();
    private final MutableDirectBuffer controlBuffer = new ExpandableArrayBuffer(64);

    private final FragmentHandler requestHandler =
        (buffer, offset, length, header) -> onRequest(buffer, offset, length);

    private final LongPredicate replayLive;

    /**
     * Production constructor: serves member {@code memberId}'s own Aeron client and archive.
     * @param topologyRows  the default queue's topology rows (see {@link QueueResolver}), as copies the publisher
     *                      hands over and this drains
     * @param relayedGroups the groups whose queues this node holds: every group on a member, those it relays on a
     *                      gateway host
     * @param fatalHandler run once, from the duty-cycle thread, when the duty cycle dies on an uncaught
     *                     exception — see {@link #fatalDutyCycleFailure}. Must not block: it is expected
     *                     to signal a shutdown and return, not to perform one.
     */
    public ReplayerService(final Aeron aeron, final AeronArchive archive, final int memberId,
                           final IdleStrategy idleStrategy, final Queue<byte[]> topologyRows, final int relayedGroups,
                           final Runnable fatalHandler) {
        this(new AeronReplayer(aeron, archive, memberId), memberId, idleStrategy, topologyRows, relayedGroups,
             fatalHandler, null);
    }

    /**
     * As above, tracking the cluster snapshots a member takes (doc/snapshot.md §10.8).
     * @param cutIntake the cuts handed over, the member's {@code restart-safe.log} and the join window
     */
    public ReplayerService(final Aeron aeron, final AeronArchive archive, final int memberId,
                           final IdleStrategy idleStrategy, final Queue<byte[]> topologyRows, final int relayedGroups,
                           final Runnable fatalHandler, final CutIntake cutIntake) {
        this(new AeronReplayer(aeron, archive, memberId), memberId, idleStrategy, topologyRows, relayedGroups,
             fatalHandler, cutIntake);
    }

    /**
     * Serves the {@link Replayer}.
     * @param replayer the node's archive, app-facing streams, counters and clock
     * @param memberId which cluster member this ReplayerService co-locates with
     * @param idleStrategy duty-cycle and control-offer idle strategy
     * @param topologyRows see the production constructor
     * @param relayedGroups see the production constructor
     * @param fatalHandler see the production constructor
     */
    ReplayerService(final Replayer replayer, final int memberId, final IdleStrategy idleStrategy,
                    final Queue<byte[]> topologyRows, final int relayedGroups, final Runnable fatalHandler) {
        this(replayer, memberId, idleStrategy, topologyRows, relayedGroups, fatalHandler, null);
    }

    /**
     * @param cutIntake the cuts to track, or null where there are none
     */
    ReplayerService(final Replayer replayer, final int memberId, final IdleStrategy idleStrategy,
                    final Queue<byte[]> topologyRows, final int relayedGroups, final Runnable fatalHandler,
                    final CutIntake cutIntake) {
        this.cutIntake = cutIntake;
        this.restartSafety = cutIntake == null ? null : new RestartSafety(cutIntake.joinWindowMs());
        this.replayer = replayer;
        this.memberId = memberId;
        this.idleStrategy = idleStrategy;
        this.topologyRows = topologyRows;
        this.resolver = new QueueResolver(relayedGroups);
        this.fatalHandler = fatalHandler;
        this.replayLive = replayer::isReplayLive;
        Arrays.fill(staleReportedFor, NULL_VALUE);

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
     * the local archive; the live feed is the queue the apps read directly, not this process.
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
        if (Replayer.ARCHIVE_SESSION_CLOSED.equals(archiveError)) {
            // The session is the archive's to close and cannot be reopened, so no replay could be served again.
            throw new IllegalStateException("the local archive closed its control session");
        }
        if (archiveError != null) {
            // An error answers one request and leaves the session open.
            onArchiveError("a replay", new IllegalStateException(archiveError));
        }
        int work = drainTopologyRows();
        work += cutIntake == null ? 0 : trackCuts();
        work += replayer.pollRequests(requestHandler, FRAGMENT_LIMIT);
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
     * Takes the cuts the service has handed over since the last cycle: each queue keeps where its marker was recorded,
     * every application replica is told of the round, and a cut that every replica that joined has confirmed is written
     * to the member's log as safe to restart from (doc/snapshot.md §10.8).
     */
    private int trackCuts() {
        int work = 0;
        CutHandover cut;
        while ((cut = cutIntake.cuts().poll()) != null) {
            for (int groupId = 1; groupId <= FrameLayer.MAX_GROUPS; groupId++) {
                if (cut.markerPositions()[groupId - 1] >= 0 && resolver.serves(groupId)) {
                    try {
                        keepCut(groupId, cut.clusterSeqNo(), cut.markerPositions()[groupId - 1]);
                    } catch (final RuntimeException ex) {
                        onArchiveError("listing the queue recordings to keep a cut", ex); // it is found by reading
                    }
                }
            }
            restartSafety.onCut(cut.round(), cut.logPosition(), replayer.epochMillis());
            snapshotRoundEncoder.wrapAndApplyHeader(controlBuffer, 0, outHeaderEncoder)
                .round(cut.round())
                .clusterSeqNo(cut.clusterSeqNo());
            offerControl(MessageHeaderEncoder.ENCODED_LENGTH + snapshotRoundEncoder.encodedLength());
            ++work;
        }
        final long safe = restartSafety.poll(replayer.epochMillis());
        if (safe != RestartSafety.NO_POSITION) {
            cutIntake.log().appendSafe(safe);
            Logger.info(Logger.CoreComponent.ReplayerService, memberId,
                        "the snapshot at log position %d may be restarted from", safe);
            ++work;
        }
        return work;
    }

    /** Feeds the resolver what the default queue's publisher has handed over since the last cycle. */
    private int drainTopologyRows() {
        int work = 0;
        byte[] row;
        while ((row = topologyRows.poll()) != null) {
            topologyRow.wrap(row);
            resolver.onTopologyRow(topologyRow, 0, row.length);
            ++work;
        }
        return work;
    }

    /**
     * Waits for the group table, then for each served queue's active recording to be visible on the local
     * archive. Only those are served, whatever their first frame: a recording that starts at a snapshot's cut
     * does not start at groupSeqNo 1, and a client that walks one finds out for itself.
     * @return work count
     */
    private int checkReady() {
        final int served = resolver.servedGroups();
        if (served == 0) {
            return 0; // no group active here yet
        }
        try {
            final StringJoiner recordings = new StringJoiner(", ");
            for (int groupId = 1; groupId <= FrameLayer.MAX_GROUPS; groupId++) {
                if (!resolver.serves(groupId)) {
                    continue;
                }
                final Replayer.RecordingSpan active = findActiveRecording(groupId);
                if (active == null) {
                    return 0; // not recorded yet; retry next cycle
                }
                recordings.add("group " + groupId + "'s queue recording " + active.recordingId() + " live");
            }
            ready = true;
            readyCounter.set(1);
            Logger.info(Logger.CoreComponent.ReplayerService, memberId, "ready — %s; serving replay", recordings);
            return 1;
        } catch (final RuntimeException ex) {
            onOwnArchiveCallFailed("listing the queue recordings", ex);
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

        if (inHeaderDecoder.templateId() == QueueRequestDecoder.TEMPLATE_ID) {
            queueRequestDecoder.wrap(buffer, offset + MessageHeaderDecoder.ENCODED_LENGTH,
                                     inHeaderDecoder.blockLength(), inHeaderDecoder.version());
            onQueueRequest();
            return;
        }
        if (inHeaderDecoder.templateId() == SnapshotJoinedDecoder.TEMPLATE_ID) {
            snapshotJoinedDecoder.wrap(buffer, offset + MessageHeaderDecoder.ENCODED_LENGTH,
                                       inHeaderDecoder.blockLength(), inHeaderDecoder.version());
            if (restartSafety != null) {
                restartSafety.onJoined(snapshotJoinedDecoder.round(), snapshotJoinedDecoder.sourceId());
            }
            return;
        }
        if (inHeaderDecoder.templateId() == SnapshotConfirmedDecoder.TEMPLATE_ID) {
            snapshotConfirmedDecoder.wrap(buffer, offset + MessageHeaderDecoder.ENCODED_LENGTH,
                                          inHeaderDecoder.blockLength(), inHeaderDecoder.version());
            if (restartSafety != null) {
                restartSafety.onConfirmed(snapshotConfirmedDecoder.round(), snapshotConfirmedDecoder.sourceId());
            }
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
        final int queueId = replayRequestDecoder.queueId();
        final long cutClusterSeqNo = replayRequestDecoder.cutClusterSeqNo();

        if (!ready || !resolver.serves(queueId)) {
            sendPending(clientId, requestId); // R-5: no queue here has an active recording by that id
            return;
        }
        stopReplayForClient(clientId);

        final int limit = replayLimit();
        replaySlots.limit(limit);
        if (!replaySlots.hasCapacity()) {
            if (replaySlots.activeCount() < MAX_CONCURRENT_REPLAYS) {
                throttledRequestsCounter.increment();
            }
            replaySlots.addPending(clientId, requestId, fromPosition, queueId, cutClusterSeqNo);
            sendPending(clientId, requestId);
            Logger.info(Logger.CoreComponent.ReplayerService, memberId,
                        "client %d pending: no free replay slot (active=%d/%d%s, pending=%d)", clientId,
                        replaySlots.activeCount(), limit,
                        limit < MAX_CONCURRENT_REPLAYS ? " while a queue recording is behind" : "",
                        replaySlots.pendingCount());
            return;
        }
        startReplayForClient(clientId, requestId, fromPosition, queueId, cutClusterSeqNo);
        drainPending();
    }

    /**
     * Answers which queue an instance reads (R-6), or {@code ReplayPending} until its row is on the log. Holds no
     * slot; a request naming both a gateway and an application is logged and not answered.
     */
    private void onQueueRequest() {
        final long clientId = queueRequestDecoder.clientId();
        final long requestId = queueRequestDecoder.requestId();
        final int sourceId = queueRequestDecoder.sourceId();
        final String gatewayName = queueRequestDecoder.gatewayName();
        final boolean namesGateway = !gatewayName.isEmpty();
        final boolean namesApplication = sourceId != ReplayProtocol.NO_SOURCE_ID;
        if (namesGateway && namesApplication) {
            logUnresolvedOnce("gateway " + gatewayName + " and application " + sourceId,
                              "it names both, so it is not answered");
            return;
        }
        final String source = namesGateway ? "gateway " + gatewayName
            : namesApplication             ? "application " + sourceId
                                           : "the default queue";
        final int groupId = namesGateway ? resolver.gatewayGroup(gatewayName)
            : namesApplication           ? resolver.applicationGroup(sourceId)
                                         : resolver.defaultGroup();
        if (groupId == QueueResolver.UNRESOLVED) {
            logUnresolvedOnce(source, "its row is not on the log yet");
            sendPending(clientId, requestId);
        } else if (!resolver.serves(groupId)) {
            logUnresolvedOnce(source, "its group " + groupId + " is not one this node holds a queue for");
            sendPending(clientId, requestId);
        } else {
            sendQueueResolved(clientId, requestId, groupId);
        }
    }

    /** Logs, once per source, a {@code QueueRequest} this cannot answer with a queue it serves. */
    private void logUnresolvedOnce(final String source, final String reason) {
        if (unresolvedLogged.add(source)) {
            Logger.error(Logger.CoreComponent.ReplayerService, Logger.CoreEventCode.QueueUnresolved, memberId,
                         "cannot tell %s which queue to read: %s; it waits, unfenced, until it can", source, reason);
        }
    }

    /**
     * Serves one replay request, guarding every archive control call. An archive that throws is answered
     * {@code ReplayPending}: the client resends, and each resend is the retry.
     * @param clientId client identity
     * @param requestId the request being answered, echoed in every reply
     * @param fromPosition start replay position, or {@link ReplayProtocol#FROM_START}
     * @param queueId the queue to replay
     * @param cutClusterSeqNo the snapshot cut a restore replays from, else {@link ReplayProtocol#NO_CUT}
     */
    private void startReplayForClient(final long clientId, final long requestId, final long fromPosition,
                                      final int queueId, final long cutClusterSeqNo) {
        final boolean fromStart = fromPosition == ReplayProtocol.FROM_START;
        try {
            serveReplay(clientId, requestId, fromPosition, queueId, cutClusterSeqNo);
        } catch (final RuntimeException error) {
            if (!fromStart && archiveAnswers(queueId)) {
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
    private boolean archiveAnswers(final int queueId) {
        try {
            replayer.listQueueRecordings(queueId);
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
     * Serves one replay of a queue's active recording: from its start for {@link ReplayProtocol#FROM_START}, else
     * resumed at {@code fromPosition}. Bounded at the recording's tip now; the queue carries the rest.
     * @param clientId client identity
     * @param requestId the request being answered, echoed in every reply
     * @param fromPosition start position, or {@link ReplayProtocol#FROM_START}
     * @param queueId the queue to replay
     * @param cutClusterSeqNo the snapshot cut a restore replays from, else {@link ReplayProtocol#NO_CUT}; then {@code
     *     fromPosition} only hints at where it is
     */
    private void serveReplay(final long clientId, final long requestId, final long fromPosition, final int queueId,
                             final long cutClusterSeqNo) {
        replaySlots.cancelPending(clientId);

        final Replayer.RecordingSpan active = findActiveRecording(queueId);
        if (active == null) {
            replaySlots.addPending(clientId, requestId, fromPosition, queueId, cutClusterSeqNo);
            sendPending(clientId, requestId);
            return;
        }
        final long recordingId = active.recordingId();
        final long tip = tipOf(recordingId);
        if (tip < 0) {
            replaySlots.addPending(clientId, requestId, fromPosition, queueId, cutClusterSeqNo);
            sendPending(clientId, requestId);
            return;
        }
        final long replayFrom;
        if (cutClusterSeqNo != ReplayProtocol.NO_CUT) {
            final CutLookup.Answer cut =
                cutLookup.find(recordingId, active.startPosition(), tip, fromPosition, cutClusterSeqNo,
                               replayer::scanQueueRecording);
            if (cut.verdict() == CutLookup.Verdict.PENDING) {
                sendPending(clientId, requestId); // the recording has not reached the cut; the client resends
                return;
            }
            if (cut.verdict() != CutLookup.Verdict.FOUND) {
                rejectResume(clientId, requestId,
                             "cut " + cutClusterSeqNo + " is " +
                                 (cut.verdict() == CutLookup.Verdict.BELOW ? "before recording " + recordingId
                                                                           : "not in recording " + recordingId));
                return;
            }
            replayFrom = cut.position();
        } else {
            replayFrom = fromPosition == ReplayProtocol.FROM_START ? active.startPosition() : fromPosition;
            if (replayFrom < active.startPosition()) {
                rejectResume(clientId, requestId,
                             "position " + fromPosition + " predates recording " + recordingId +
                                 "'s startPosition " + active.startPosition());
                return;
            }
        }
        final long boundedLength = tip - replayFrom;
        if (boundedLength <= 0) {
            if (fromPosition == ReplayProtocol.FROM_START) {
                sendReplaying(clientId, requestId, ReplayProtocol.NO_REPLAY_NEEDED, tip);
            } else {
                sendPending(clientId, requestId); // the recording trails the queue the client resumed from
            }
            return;
        }

        final long replaySessionId = replayer.startReplay(recordingId, replayFrom, boundedLength, ReplayProtocol.REPLAY_STREAM_ID);
        replaysServedCounter.increment();
        replaySlots.activate(clientId, replaySessionId);
        Logger.info(Logger.CoreComponent.ReplayerService, memberId,
                    "replay for client %d: group %d's recording %d [%d,%d) session %d", clientId, queueId, recordingId,
                    replayFrom, tip, replaySessionId);
        sendReplaying(clientId, requestId, replaySessionId, tip);
    }

    /**
     * How many replays may run at once: one while any queue's active recording trails it by more than a quarter of
     * the window it may trail by, since a replay competes with the recorder for the disk and the recorder holds the
     * sequencer back.
     */
    private int replayLimit() {
        if (replaySlots.activeCount() == 0) {
            return MAX_CONCURRENT_REPLAYS; // the next replay is the only one either way
        }
        try {
            for (int groupId = 1; groupId <= FrameLayer.MAX_GROUPS; groupId++) {
                final Replayer.RecordingSpan active = resolver.serves(groupId) ? findActiveRecording(groupId) : null;
                final Replayer.QueueBacklog backlog = active == null ? null : replayer.queueBacklog(groupId, active);
                if (backlog != null && backlog.bytes() > backlog.window() / 4) {
                    return 1;
                }
            }
            return MAX_CONCURRENT_REPLAYS;
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
                startReplayForClient(request.clientId(), request.requestId(), request.fromPosition(),
                                     request.queueId(), request.cutClusterSeqNo());
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
     * Send queue resolved message
     * @param clientId client identity
     * @param requestId the request being answered
     * @param queueId the queue it resolved to
     */
    private void sendQueueResolved(final long clientId, final long requestId, final int queueId) {
        queueResolvedEncoder.wrapAndApplyHeader(controlBuffer, 0, outHeaderEncoder)
            .clientId(clientId)
            .requestId(requestId)
            .queueId(queueId);
        offerControl(MessageHeaderEncoder.ENCODED_LENGTH + queueResolvedEncoder.encodedLength());
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
     * bound. Safe, unlike on the queue: the app resends on a timer, so a drop costs one resend interval.
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
                        "the local archive refused %s (%s); live delivery unaffected (apps read the queue directly) "
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
     * The newest active recording of a queue on the local archive, or null. More than one active is an unclean
     * shutdown's leftover, reported once per episode; the newest holds the older one's content. Listed again only
     * once the one found has stopped recording, so a request costs no listing.
     */
    private Replayer.RecordingSpan findActiveRecording(final int groupId) {
        final Replayer.RecordingSpan cached = activeRecordings[groupId - 1];
        if (cached != null && replayer.recordingPosition(cached.recordingId()) >= 0) {
            return cached;
        }
        activeRecordings[groupId - 1] = null;
        Replayer.RecordingSpan found = null;
        int activeCount = 0;
        for (final Replayer.RecordingSpan span : replayer.listQueueRecordings(groupId)) {
            if (span.active()) {
                ++activeCount;
                if (found == null || span.recordingId() > found.recordingId()) {
                    found = span;
                }
            }
        }
        if (activeCount > 1 && found.recordingId() != staleReportedFor[groupId - 1]) {
            Logger.error(Logger.CoreComponent.ReplayerService, Logger.CoreEventCode.StaleActiveRecording, memberId,
                         "%d recordings of group %d's queue report as still recording — an unclean shutdown left an "
                             + "older one unstopped; serving the newest",
                         activeCount, groupId);
        }
        staleReportedFor[groupId - 1] = activeCount > 1 ? found.recordingId() : NULL_VALUE;
        if (found != null && replayer.recordingPosition(found.recordingId()) >= 0) {
            activeRecordings[groupId - 1] = found;
        }
        return found;
    }

    /**
     * Remembers where a snapshot cut was recorded in a queue's active recording, so a restore naming it is answered
     * without reading the recording. The cluster tier's cut hand-over calls it (doc/snapshot.md §10.8).
     * @param queueId         the queue's group
     * @param cutClusterSeqNo the cut
     * @param position        where its frame starts in the recording
     */
    void keepCut(final int queueId, final long cutClusterSeqNo, final long position) {
        final Replayer.RecordingSpan active = findActiveRecording(queueId);
        if (active != null) {
            cutLookup.keep(active.recordingId(), cutClusterSeqNo, position);
        }
    }

    /** Where a recording has reached: its live position, else where it stopped; negative if neither is known. */
    private long tipOf(final long recordingId) {
        final long position = replayer.recordingPosition(recordingId);
        return position >= 0 ? position : replayer.stopPosition(recordingId);
    }
}
