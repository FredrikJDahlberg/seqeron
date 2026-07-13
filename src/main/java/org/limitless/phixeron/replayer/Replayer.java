package org.limitless.phixeron.replayer;

import static io.aeron.Aeron.NULL_VALUE;

import io.aeron.ExclusivePublication;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.logbuffer.FragmentHandler;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.limitless.phixeron.sbe.unsequenced.MessageHeaderDecoder;
import org.limitless.phixeron.sbe.unsequenced.MessageHeaderEncoder;
import org.limitless.phixeron.sbe.unsequenced.ReplayCompleteDecoder;
import org.limitless.phixeron.sbe.unsequenced.ReplayPendingEncoder;
import org.limitless.phixeron.sbe.unsequenced.ReplayRequestDecoder;
import org.limitless.phixeron.sbe.unsequenced.ReplayingEncoder;
import org.limitless.phixeron.sequencer.SequencerService;

/**
 * Per-node archive <b>replay server</b> for co-located application replicas (Replayer design,
 * {@code doc/router-design.md}). It is deliberately <em>not</em> on the live delivery path: every
 * app reads the co-located {@code SequencerService}'s node-local IPC tap ({@link
 * SequencerService#TAP_CHANNEL} / {@link SequencerService#TAP_STREAM_ID}) <b>directly</b> for the
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
 *   <li><b>Control</b> ({@link #CONTROL_STREAM_ID}) — the Replayer answers {@code
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
 * live sub" handoff {@code GlobalStreamClient} already implements, with the tap playing the role of
 * the live sub. The app detects that tip by <em>position</em> (the {@code catchUpPosition} carried in
 * {@link ReplayingEncoder}), not by the replay image closing: a bounded replay of an <em>active</em>
 * recording does not close its image at the bound. That needs no {@code globalSeqNo→position} index,
 * and any residual gap after the handoff is healed by the same gap-detect → re-request loop (design
 * §5).
 *
 * <p><b>Strictly stateless</b> (design §6): the Replayer holds nothing not re-derivable from the
 * archive — only the ephemeral in-flight replay slots. A crash is a fast reconnect; apps treat
 * "Replayer gone" as they treat a gap and re-request on its return.
 *
 * <p><b>Local-archive resilience.</b> The Replayer is off the live path entirely, so a transient
 * failure of the node's local archive degrades only history/gap <em>replay</em> — steady-state
 * delivery keeps flowing over the tap the apps read directly. It does not crash the Replayer either:
 * an archive control call that throws in the replay path flips it to a STALLED state and keeps the
 * duty cycle running, paces its replay retries, and answers any replay request with {@code
 * ReplayPending} — which the app already treats as "hold at the gap and re-request" — until the
 * archive returns (doc/router-archive.md).
 *
 * <p><b>Cross-failover replay.</b> A cold-starting app walks the recording chain ({@link
 * #resolveSegments}, the same oldest-first stitching {@code GlobalStreamClient} uses). With every node
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
public final class Replayer {
    /** Node-local IPC channel every Replayer↔app stream runs over. */
    public static final String IPC_CHANNEL = "aeron:ipc";

    /** Replayer → apps: on-demand archive replays (one Aeron session per in-flight replay). */
    public static final int REPLAY_STREAM_ID = 201;

    /** Apps → Replayer: {@code ReplayRequest}. */
    public static final int REQUEST_STREAM_ID = 202;

    /** Replayer → apps: {@code Replaying} / {@code ReplayPending}. */
    public static final int CONTROL_STREAM_ID = 203;

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

    private final io.aeron.Aeron aeron;
    private final AeronArchive archive;
    private final int memberId;
    private final IdleStrategy idleStrategy;

    // ── Replayer → apps control + apps → Replayer requests ────────────────────
    private final ExclusivePublication controlPub;
    private final Subscription requestSub;

    // Proactive readiness marker: set once the co-located SequencerService's tap recording is visible
    // on the local archive (see checkReady). The launch scripts wait on the readiness log before
    // starting apps.
    private boolean ready = false;

    // ── Replay protocol state ─────────────────────────────────────────────────
    private static final class ReplaySlot {
        final int clientId;
        long replaySessionId;
        long lastTouchedMs;

        ReplaySlot(final int clientId, final long replaySessionId, final long nowMs) {
            this.clientId = clientId;
            this.replaySessionId = replaySessionId;
            this.lastTouchedMs = nowMs;
        }
    }

    private final List<ReplaySlot> activeReplays = new ArrayList<>(MAX_CONCURRENT_REPLAYS);
    private final Deque<long[]> pendingRequests = new ArrayDeque<>();  // [clientId, segmentIndex, fromPosition]

    // Local-archive resilience (doc/router-archive.md): a transient local-archive failure must not kill
    // the duty-cycle thread. The Replayer is off the live path (apps read the tap directly), so a stall
    // only affects replay. While STALLED it keeps its duty cycle running, paces its replay retries, and
    // holds replay-requesting apps with ReplayPending until the archive returns. (Surfacing STALLED via
    // an Aeron counter is a deferred follow-up — see doc/router-archive.md.)
    private boolean stalled = false;
    private long lastStallRetryMs = 0;

    private final MessageHeaderDecoder inHeaderDecoder = new MessageHeaderDecoder();
    private final ReplayRequestDecoder replayRequestDecoder = new ReplayRequestDecoder();
    private final ReplayCompleteDecoder replayCompleteDecoder = new ReplayCompleteDecoder();
    private final MessageHeaderEncoder outHeaderEncoder = new MessageHeaderEncoder();
    private final ReplayingEncoder replayingEncoder = new ReplayingEncoder();
    private final ReplayPendingEncoder pendingEncoder = new ReplayPendingEncoder();
    private final MutableDirectBuffer controlBuffer = new ExpandableArrayBuffer(64);

    private final FragmentHandler requestHandler =
        (buffer, offset, length, header) -> onRequest(buffer, offset, length);

    public Replayer(final io.aeron.Aeron aeron, final AeronArchive archive, final int memberId,
                    final IdleStrategy idleStrategy) {
        this.aeron = aeron;
        this.archive = archive;
        this.memberId = memberId;
        this.idleStrategy = idleStrategy;

        this.controlPub = aeron.addExclusivePublication(IPC_CHANNEL, CONTROL_STREAM_ID);
        this.requestSub = aeron.addSubscription(IPC_CHANNEL, REQUEST_STREAM_ID);
    }

    /**
     * Blocks running the duty cycle until {@code running} goes false. Serves the replay protocol from
     * the local archive; the live feed is the tap the apps read directly, not this process.
     */
    public void run(final java.util.concurrent.atomic.AtomicBoolean running) {
        System.out.printf("[Replayer/%d] starting; serving replay from the co-located archive…%n", memberId);
        while (running.get()) {
            final int work = poll();
            idleStrategy.idle(work);
        }
        System.out.printf("[Replayer/%d] shutting down%n", memberId);
        stopAllReplays();
    }

    /** One duty-cycle iteration. Returns a work count for the idle strategy. */
    public int poll() {
        if (!ready) {
            checkReady();
        }
        final int work = requestSub.poll(requestHandler, FRAGMENT_LIMIT);
        reclaimIdleSlots();
        return work;
    }

    // Proactive readiness marker, independent of any app connecting. Once the co-located
    // SequencerService's tap recording is visible on the local archive, this Replayer can serve
    // cold-start/gap replays — and, equally, live consumers reading the tap directly are getting frames
    // (you cannot record a stream that was never published). The launch scripts wait on this line before
    // starting apps. findActiveRecordingId is a cheap archive lookup that returns NULL_VALUE until the
    // recording appears (SequencerService.onStart creates it during cluster start-up, before this
    // Replayer connects), so this fires within a cycle or two of startup and never again. Guarded so an
    // archive that is momentarily unresponsive at startup just retries the next cycle rather than killing
    // the thread.
    private void checkReady() {
        final long recordingId;
        try {
            recordingId = findActiveRecordingId();
        } catch (final RuntimeException ex) {
            return;  // archive not answering yet; retry next cycle (scripts time out and proceed)
        }
        if (recordingId != NULL_VALUE) {
            ready = true;
            System.out.printf("[Replayer/%d] ready — tap recording %d live; serving replay%n", memberId, recordingId);
        }
    }

    // ── Replay protocol ─────────────────────────────────────────────────────────

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

        if (activeReplays.size() >= MAX_CONCURRENT_REPLAYS) {
            pendingRequests.addLast(new long[] {clientId, segmentIndex, fromPosition});
            sendPending(clientId);
            return;
        }
        startReplayForClient(clientId, segmentIndex, fromPosition);
        // A walk-terminating request (segmentIndex past the chain) frees this client's slot via the
        // supersede above without taking a new one; hand that freed slot to a waiting app now rather
        // than at the next idle-TTL sweep (design §5).
        drainPending();
    }

    // Serves one replay request, guarding every archive control call. If the local archive is
    // unreachable (a control op throws) the Replayer must not die: it flips to STALLED and holds the
    // client with ReplayPending — which the client already treats as "wait and re-request on its resend
    // timer" — until the archive returns. This is the note's "defer failure until a replay is needed"
    // path (doc/router-archive.md); no slot is consumed on failure (every archive call that can throw
    // runs before activeReplays.add in serveReplay).
    //
    // While STALLED, the archive attempt here is a paced probe: within STALL_RETRY_INTERVAL_MS of the
    // last probe we just hold the client with ReplayPending without touching the archive (an
    // AeronArchive control call against a dead archive can block the duty cycle until its timeout), and
    // rely on the client re-requesting. A probe that succeeds clears the stall (onArchiveHealthy). Live
    // delivery is unaffected throughout — apps read the tap directly, never through the Replayer.
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
            onArchiveHealthy();
        } catch (final RuntimeException ex) {
            onArchiveStalled("serving replay for client " + clientId, ex);
            sendPending(clientId);
        }
    }

    // Serves one replay to a client. segmentIndex < 0 resumes the current active recording at
    // fromPosition (steady-state gap recovery); segmentIndex >= 0 is one step of a cold-start walk over
    // the per-leader-tenure recording chain — serving the segmentIndex-th recording from position 0, or
    // NO_REPLAY_NEEDED once the walk runs past the last tenure (which is what marks the app caught up).
    private void serveReplay(final int clientId, final int segmentIndex, final long fromPosition) {
        final long recordingId;
        final long replayFrom;
        if (segmentIndex < 0) {
            recordingId = findActiveRecordingId();
            replayFrom = fromPosition;
            if (recordingId == NULL_VALUE) {
                // No recording to replay from yet; ask the app to hold and retry.
                pendingRequests.addLast(new long[] {clientId, segmentIndex, fromPosition});
                sendPending(clientId);
                return;
            }
        } else {
            final List<Long> chain = resolveSegments();
            if (chain.isEmpty()) {
                // No tap recording on the local archive yet; hold and retry.
                pendingRequests.addLast(new long[] {clientId, segmentIndex, fromPosition});
                sendPending(clientId);
                return;
            }
            if (segmentIndex >= chain.size()) {
                // Walked past the last tenure: the app has replayed all history and is at the live tip.
                sendReplaying(clientId, NO_REPLAY_NEEDED, 0);
                return;
            }
            recordingId = chain.get(segmentIndex);
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

        final long replaySessionId =
            archive.startReplay(recordingId, replayFrom, boundedLength, IPC_CHANNEL, REPLAY_STREAM_ID);
        activeReplays.add(new ReplaySlot(clientId, replaySessionId, System.currentTimeMillis()));
        System.out.printf("[Replayer/%d] replay for client %d: segment %d recording %d [%d,%d) session %d%n", memberId,
                          clientId, segmentIndex, recordingId, replayFrom, tip, replaySessionId);
        // catchUpPosition = tip: the app follows the replay image until it reaches this, then advances
        // (next segment, or the live tap). A bounded replay of an active recording does not close its
        // image at the bound, so the app detects completion by position (see Replaying / ReplayerClient).
        sendReplaying(clientId, replaySessionId, tip);
    }

    private void stopReplayForClient(final int clientId) {
        for (int i = 0; i < activeReplays.size(); i++) {
            if (activeReplays.get(i).clientId == clientId) {
                stopReplay(activeReplays.remove(i).replaySessionId);
                return;
            }
        }
    }

    private void reclaimIdleSlots() {
        final long now = System.currentTimeMillis();
        boolean freed = false;
        for (int i = activeReplays.size() - 1; i >= 0; i--) {
            if (now - activeReplays.get(i).lastTouchedMs > REPLAY_SLOT_TTL_MS) {
                stopReplay(activeReplays.remove(i).replaySessionId);
                freed = true;
            }
        }
        if (freed) {
            drainPending();
        }
    }

    private void drainPending() {
        // Bounded to the current queue length: startReplayForClient may re-queue a request (e.g. no
        // recording yet), so retry each waiting request at most once per call rather than spinning on
        // one that cannot yet make progress.
        int budget = pendingRequests.size();
        while (budget-- > 0 && !pendingRequests.isEmpty() && activeReplays.size() < MAX_CONCURRENT_REPLAYS) {
            final long[] req = pendingRequests.pollFirst();
            startReplayForClient((int) req[0], (int) req[1], req[2]);
        }
    }

    private void stopReplay(final long replaySessionId) {
        try {
            archive.stopReplay(replaySessionId);
        } catch (final RuntimeException ex) {
            // Bounded replays end on their own, so the session may already be gone — harmless.
        }
    }

    private void stopAllReplays() {
        for (final ReplaySlot slot : activeReplays) {
            stopReplay(slot.replaySessionId);
        }
        activeReplays.clear();
    }

    private void sendReplaying(final int clientId, final long replaySessionId, final long catchUpPosition) {
        replayingEncoder.wrapAndApplyHeader(controlBuffer, 0, outHeaderEncoder)
            .clientId(clientId)
            .replaySessionId(replaySessionId)
            .catchUpPosition(catchUpPosition);
        offerControl(MessageHeaderEncoder.ENCODED_LENGTH + replayingEncoder.encodedLength());
    }

    private void sendPending(final int clientId) {
        pendingEncoder.wrapAndApplyHeader(controlBuffer, 0, outHeaderEncoder).clientId(clientId);
        offerControl(MessageHeaderEncoder.ENCODED_LENGTH + pendingEncoder.encodedLength());
    }

    private void offerControl(final int length) {
        long result;
        int spins = 0;
        while ((result = controlPub.offer(controlBuffer, 0, length)) < 0) {
            if (result == ExclusivePublication.CLOSED || result == ExclusivePublication.MAX_POSITION_EXCEEDED) {
                throw new IllegalStateException("[Replayer] control publication failed: " + result);
            }
            if (result == ExclusivePublication.NOT_CONNECTED && ++spins > 1000) {
                return;  // no app listening for control replies; give up on this one
            }
            idleStrategy.idle();
        }
    }

    // ── Local-archive resilience ────────────────────────────────────────────────

    // A local-archive control call threw: enter STALLED (idempotently, logging once per episode). The
    // caller keeps the duty cycle running and retries; replay-requesting apps were held with
    // ReplayPending. Live delivery is unaffected — apps read the tap directly, not through the Replayer.
    private void onArchiveStalled(final String context, final RuntimeException ex) {
        if (!stalled) {
            stalled = true;
            System.out.printf("[Replayer/%d] STALLED: %s — local archive unreachable (%s); live delivery "
                              + "unaffected (apps read the tap directly), retrying replay%n",
                              memberId, context, ex.getMessage());
        }
    }

    // A local-archive control call succeeded again after a stall: leave STALLED (logging once). Reached
    // from a successful replay serve.
    private void onArchiveHealthy() {
        if (stalled) {
            stalled = false;
            System.out.printf("[Replayer/%d] RECOVERED: local archive reachable again%n", memberId);
        }
    }

    // Finds the currently-active tap recording on the local archive (stopTimestamp unset). Normally
    // there is exactly one (each node records its own continuous tap); a member restart can leave an
    // earlier, stopped recording alongside it, and this returns the active one.
    private long findActiveRecordingId() {
        final long[] found = {NULL_VALUE};
        archive.listRecordingsForUri(0, Integer.MAX_VALUE, "", SequencerService.TAP_STREAM_ID,
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
    // own continuous tap this is normally a single recording spanning every leader tenure (the tap
    // publication is never re-created on failover), so unlike the old per-tenure global-stream recordings
    // there is usually nothing to stitch. A member restart can leave an earlier, stopped recording plus
    // the post-restart one (overlapping globalSeqNo ranges); a cold-starting app replays them in order and
    // de-dupes by globalSeqNo, so the overlap is harmless. Mirrors GlobalStreamClient.resolveGlobalStreamSegments.
    private List<Long> resolveSegments() {
        final List<long[]> entries = new ArrayList<>();  // [recordingId, startTs, stopTs]
        archive.listRecordingsForUri(0, Integer.MAX_VALUE, "", SequencerService.TAP_STREAM_ID,
                                     (controlSessionId, correlationId, recordingId, startTimestamp, stopTimestamp,
                                      startPosition, stopPosition, initialTermId, segmentFileLength, termBufferLength,
                                      mtuLength, sessionId, streamId, strippedChannel, originalChannel,
                                      sourceIdentity) -> entries.add(new long[] {recordingId, startTimestamp,
                                                                                 stopTimestamp}));
        entries.sort((a, b) -> Long.compare(a[1], b[1]));
        final List<Long> chain = new ArrayList<>(entries.size());
        boolean keptActive = false;
        for (final long[] entry : entries) {
            final boolean active = entry[2] == AeronArchive.NULL_TIMESTAMP;
            if (!active || !keptActive) {
                keptActive |= active;
                chain.add(entry[0]);
            }
        }
        return chain;
    }
}
