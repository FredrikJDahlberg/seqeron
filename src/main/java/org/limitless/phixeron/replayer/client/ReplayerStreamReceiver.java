package org.limitless.phixeron.replayer.client;

import io.aeron.Aeron;
import io.aeron.Counter;
import io.aeron.FragmentAssembler;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.LogBufferDescriptor;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.limitless.phixeron.metrics.PhixeronCounters;
import org.limitless.phixeron.replayer.server.ReplayerService;
import org.limitless.phixeron.sbe.sequenced.HeaderDecoder;
import org.limitless.phixeron.sbe.sequenced.LeadershipChangedDecoder;
import org.limitless.phixeron.sbe.sequenced.MessageHeaderDecoder;
import org.limitless.phixeron.sbe.unsequenced.MessageHeaderEncoder;
import org.limitless.phixeron.sbe.unsequenced.ReplayCompleteEncoder;
import org.limitless.phixeron.sbe.unsequenced.ReplayHeartbeatEncoder;
import org.limitless.phixeron.sbe.unsequenced.ReplayPendingDecoder;
import org.limitless.phixeron.sbe.unsequenced.ReplayRequestEncoder;
import org.limitless.phixeron.sbe.unsequenced.ReplayUnavailableDecoder;
import org.limitless.phixeron.sbe.unsequenced.ReplayingDecoder;
import org.limitless.phixeron.sequencer.SequencerService;
import org.limitless.phixeron.util.Logger;

/**
 * App-replica side of the per-node {@link ReplayerService} — the Java twin of the C++
 * {@code replayer/ReplayerStreamReceiver.hpp}, and deliberately a faithful port of it: the two follow the
 * same stream with the same protocol, and any divergence in the state machine is a divergence in what
 * history a node's replicas see.
 *
 * <p>It reads the co-located {@code SequencerService} IPC tap ({@link SequencerService#FEEDER_STREAM_ID})
 * LIVE and only touches the archive indirectly — by asking the node-local Replayer to replay when it
 * detects a gap. That is the whole point of the Replayer: one process per node reads the archive, every
 * replica reads the cheap local tap for live and asks the Replayer for history and gaps.
 *
 * <p>Catch-up is detected by position ({@code Replaying.catchUpPosition}), not by the replay image
 * closing: the Replayer's replay is bounded to an ACTIVE recording, and a bounded replay of an active
 * recording never closes its image at the bound.
 *
 * <p>Gap recovery is anchored on globalSeqNo (load-bearing) and merely accelerated by position. On a tap
 * gap the client asks the Replayer to RESUME the active recording at the position of the frame it last
 * dispatched, so repairing a dropped frame costs a replay of the hole rather than of the whole trading
 * day. A position is not self-validating, though: it denotes a frame only within the recording it was
 * observed in, and a member restart can leave the app holding a position from a recording that is no
 * longer the active one. So the resumed replay is checked where it lands — its first frame must be the
 * frame the position was anchored on — and on any mismatch (or a Replayer answering "nothing to replay"
 * over a hole we know is open) the client falls back to re-walking the chain from segment 0, de-duping
 * every already-seen frame by globalSeqNo until it re-reaches the tip. The walk needs no position to be
 * sound, so it stays the backstop; the resume is only the fast path over it.
 *
 * <p>The replay-to-live seam is closed by the tap itself, not by a round trip. Tap frames are drained AND
 * dispatched while a walk is in flight, and — load-bearing — ones landing beyond the current hole are
 * RETAINED in globalSeqNo order rather than dropped, because the tap must be polled every duty cycle (it
 * is untethered) and an unretained frame is therefore gone for good. When the replay reaches the hole, the
 * retained frames hand straight over and the client is live with no residual. The buffer is bounded and
 * falls back to drop-and-re-walk past the bound, since a re-walk over a full trading day cannot buffer a
 * day of traffic.
 *
 * <p>Two consequences of dispatching the tap mid-walk. A non-contiguous tap frame is then EXPECTED, not a
 * new gap: re-walk is triggered only when not {@link #isRecovering()}, so an in-flight walk runs to
 * completion instead of being superseded by the very frames it is racing. And such a frame may not
 * establish the globalSeqNo baseline before the walk has — otherwise a cold start would adopt whatever the
 * live tap happened to be carrying, which is exactly the mid-stream baseline the first-frame-must-be-1
 * abort exists to prevent.
 *
 * <p>Two things the walk state machine must not conflate, both of which resolve the unsafe way if ignored:
 * a stale reply versus the current one (matched on {@code requestId}, which advances on every send,
 * resends included), and a closed replay image versus a completed segment (only a close AT the bound is
 * completion; a close short of it means the replay was stopped under us).
 *
 * <p>{@link #isCaughtUp()} is a state, not a latch. It is cleared the moment a live-tap gap is detected and
 * re-established when the stream goes contiguous again, because consumers gate real decisions on it.
 *
 * <p><b>Single-threaded.</b> Every method must be called from the one duty-cycle thread.
 */
public final class ReplayerStreamReceiver implements AutoCloseable {
    /** Receives every in-order frame that is not intercepted as a leadership change. */
    @FunctionalInterface
    public interface SequencedHandler {
        void onSequenced(SequencedEvent event);
    }

    /** Receives each {@code LeadershipChanged} as it is dispatched, in log order. */
    @FunctionalInterface
    public interface LeadershipHandler {
        void onLeadershipChanged(int newLeaderMemberId, long globalSeqNo);
    }

    /** Fires on every transition to caught-up, including re-convergence after a gap. */
    @FunctionalInterface
    public interface CaughtUpHandler {
        void onCaughtUp();
    }

    /**
     * The tap as a consumer addresses it: untethered, so a slow app is dropped and heals via replay rather
     * than back-pressuring the sequencer.
     */
    public static final String FEEDER_CONSUMER_CHANNEL = SequencerService.FEEDER_CHANNEL + "?tether=false";

    /**
     * Untethered like the tap, and for the same reason: the Replayer answers every app from one duty-cycle
     * thread, so an app that stops polling must not be able to back-pressure the stream the others are
     * answered on. A dropped reply costs one resend interval, which the resend timer already covers.
     */
    public static final String CONTROL_CHANNEL = ReplayerService.IPC_CHANNEL + "?tether=false";

    private static final int FRAGMENT_LIMIT = 16;
    private static final long RESEND_INTERVAL_MS = 500;

    /**
     * How long an established replay may deliver nothing before it is re-requested. Deliberately far above
     * any legitimate pause: the archive reads local disk, so a replay with anything left to serve is never
     * quiet for seconds. Kept well clear of {@link #RESEND_INTERVAL_MS} too, since a spurious fire costs a
     * whole segment re-replayed.
     */
    private static final long REPLAY_STALL_TIMEOUT_MS = 5_000;

    /**
     * How long recovery may run without dispatching a single frame before it is reported as unconvergent. A
     * different question from {@link #REPLAY_STALL_TIMEOUT_MS}, which asks whether one replay IMAGE is
     * advancing: the re-walk loop this catches keeps starting and finishing healthy replays and dispatches
     * nothing out of any of them.
     */
    private static final long RECOVERY_PROGRESS_TIMEOUT_MS = 30_000;

    /**
     * {@code ReplayRequest.segmentIndex} meaning "resume the active recording at fromPosition" rather than
     * "replay the segmentIndex-th recording of the chain" — see {@code ReplayerService.serveReplay}.
     */
    private static final int RESUME_SEGMENT_INDEX = -1;

    /** {@code LeadershipChanged}, synthesized onto the sequenced stream; intercepted, never dispatched. */
    private static final int LEADERSHIP_CHANGED_TEMPLATE_ID = LeadershipChangedDecoder.TEMPLATE_ID;

    /**
     * Caps on frames retained ahead of a hole — enough to cover a walk over a normal recording, not a whole
     * trading day; past either bound recovery falls back to re-walking.
     */
    private static final int MAX_RETAINED_FRAMES = 65536;

    private static final long MAX_RETAINED_BYTES = 16L * 1024 * 1024;

    /** Scratch for the three control messages this client sends; each is well under 64 bytes. */
    private static final int REQUEST_BUFFER_LENGTH = 64;

    private final int clientId;
    private final SequencedHandler onSequenced;
    private final LeadershipHandler onLeadershipChanged;
    private final CaughtUpHandler onCaughtUp;

    private final MessageHeaderDecoder messageHeader = new MessageHeaderDecoder();
    private final HeaderDecoder header = new HeaderDecoder();
    private final LeadershipChangedDecoder leadershipChanged = new LeadershipChangedDecoder();
    private final org.limitless.phixeron.sbe.unsequenced.MessageHeaderDecoder controlHeader =
        new org.limitless.phixeron.sbe.unsequenced.MessageHeaderDecoder();
    private final ReplayingDecoder replaying = new ReplayingDecoder();
    private final ReplayPendingDecoder replayPending = new ReplayPendingDecoder();
    private final ReplayUnavailableDecoder replayUnavailable = new ReplayUnavailableDecoder();

    private final MessageHeaderEncoder requestHeader = new MessageHeaderEncoder();
    private final ReplayRequestEncoder replayRequest = new ReplayRequestEncoder();
    private final ReplayCompleteEncoder replayComplete = new ReplayCompleteEncoder();
    private final ReplayHeartbeatEncoder replayHeartbeat = new ReplayHeartbeatEncoder();
    private final UnsafeBuffer requestBuffer = new UnsafeBuffer(new byte[REQUEST_BUFFER_LENGTH]);

    private final SequencedEvent event = new SequencedEvent();
    private final RecoveryProgressPolicy recoveryProgress = new RecoveryProgressPolicy(RECOVERY_PROGRESS_TIMEOUT_MS);

    private final FragmentHandler tapHandler =
        new FragmentAssembler((buffer, offset, length, hdr) -> onFrame(buffer, offset, length,
                                                                      frameStartPosition(hdr), false));
    private final FragmentHandler replayHandler =
        new FragmentAssembler((buffer, offset, length, hdr) -> onFrame(buffer, offset, length,
                                                                      frameStartPosition(hdr), true));
    private final FragmentHandler controlFragmentHandler =
        new FragmentAssembler((buffer, offset, length, hdr) -> onControl(buffer, offset, length));

    private Aeron aeron;
    private Integer memberId;
    private Subscription tapSubscription;
    private Subscription replaySubscription;
    private Subscription controlSubscription;
    private Publication requestPublication;
    private Image replayImage;
    private Counter recoveryStalledCounter;

    private boolean awaitingReplay;
    private long replaySessionId = -1;

    /** Position the bounded replay ends at; the segment is done once the image reaches it. */
    private long catchUpPosition;

    /** Cold-start walk position; -1 once caught up (steady/resume mode). */
    private int walkSegmentIndex;

    /** recordingId last served for {@link #walkSegmentIndex}, or -1 if not yet known. */
    private long walkRecordingId = -1;

    /** fromPosition of the current request, for an idempotent resend. */
    private long requestFromPosition;

    private long lastRequestMs;

    /** Advances per send; replies not carrying it are stale — see {@link #onControl}. */
    private long requestId;

    private long lastHeartbeatMs;

    /**
     * A {@code ReplayComplete} that did not land. {@code isConnected()} only rules out NOT_CONNECTED; a
     * healthy publication still returns BACK_PRESSURED/ADMIN_ACTION transiently, and a discarded result
     * made "attempted" indistinguishable from "sent". Only the release needs this — see
     * {@link #requestReplay} for why the request must NOT be retried the same way.
     */
    private boolean completePending;

    private long lastReplayPosition = -1;
    private long lastReplayProgressMs;

    /**
     * The Replayer is refusing to serve us (its integrity check failed). State, not just a log latch: the
     * refusal is resent on every request, so report it once per episode, and
     * {@link #checkRecoveryProgress} prints it as the fact that tells a refusal apart from a Replayer that
     * never answered.
     */
    private boolean replayerUnavailable;

    private long lastGlobalSeqNo;

    /** Where the last dispatched frame starts in the recording; {@link #requestResume}'s anchor. */
    private long lastFramePosition;

    /** globalSeqNo a resume replay must open at, or 0 if not resuming. */
    private long resumeAnchorGlobalSeqNo;

    /** Report a hole in replayed history once per episode, not per frame. */
    private boolean replayGapLogged;

    private boolean caughtUp;
    private int currentLeaderMemberId = -1;

    // Live tap frames from beyond the current hole, retained in arrival order.
    private final Deque<RetainBlock> retained = new ArrayDeque<>();
    private final List<RetainBlock> retainPool = new ArrayList<>();
    private int retainReadOffset;
    private long retainTailGlobalSeqNo;
    private int retainFrameCount;
    private long retainBytes;

    /**
     * Frames were dropped ahead of the hole because the FIFO was full: the frontier this client holds is
     * short of the real one, so it must re-walk rather than declare itself caught up. Cleared only where
     * that re-walk is requested ({@link #endOverflowEpisode}).
     */
    private boolean retainOverflowed;

    private boolean retainOverflowLogged;

    /**
     * @param clientId            this replica's stable id, unique among the Replayer's co-located apps
     *                            ({@code PHIXERON_REPLAYER_CLIENT_ID}); two apps sharing one supersede
     *                            each other's replays and neither ever catches up
     * @param onSequenced         receives every in-order frame
     * @param onLeadershipChanged receives each leadership change, or null
     * @param onCaughtUp          fires on every transition to caught-up, or null
     */
    public ReplayerStreamReceiver(final int clientId, final SequencedHandler onSequenced,
                                  final LeadershipHandler onLeadershipChanged, final CaughtUpHandler onCaughtUp) {
        this.clientId = clientId;
        this.onSequenced = onSequenced;
        this.onLeadershipChanged = onLeadershipChanged;
        this.onCaughtUp = onCaughtUp;
    }

    /**
     * Subscribes the tap and control streams, opens the request publication and the convergence counter,
     * and requests the cold-start replay from segment 0.
     * @param aeron    client sharing the co-located node's media driver
     * @param memberId this app's node — needed only to label the counter, since a node's metrics are
     *                 merged with every other node's
     */
    public void start(final Aeron aeron, final int memberId) {
        this.aeron = aeron;
        this.memberId = memberId;
        recoveryStalledCounter = PhixeronCounters.addAppCounter(
            aeron, PhixeronCounters.APP_RECOVERY_STALLED_TYPE_ID,
            "phixeron.app.recoveryStalled member=" + memberId + " client=" + clientId, memberId, clientId);
        tapSubscription = aeron.addSubscription(FEEDER_CONSUMER_CHANNEL, SequencerService.FEEDER_STREAM_ID);
        // No standing replay subscription — see openReplaySubscription: one is opened per replay episode,
        // filtered to that replay's own session id, and closed when the episode ends.
        controlSubscription = aeron.addSubscription(CONTROL_CHANNEL, ReplayerService.CONTROL_STREAM_ID);
        requestPublication = aeron.addPublication(ReplayerService.IPC_CHANNEL, ReplayerService.REQUEST_STREAM_ID);
        requestReplay(0, 0);
    }

    /**
     * One duty-cycle iteration. Poll ordering: always drain control (to learn Replaying/ReplayPending),
     * ride an attached replay image, and always drain AND dispatch the tap — the contiguity check in
     * {@link #onFrame}, not the poll routing, decides what a tap frame is worth mid-walk, which is what
     * lets the tap itself close the replay-to-live seam.
     * @return fragments consumed
     */
    public int poll() {
        int work = 0;
        if (controlSubscription != null) {
            work += controlSubscription.poll(controlFragmentHandler, FRAGMENT_LIMIT);
        }

        // Re-request if a prior request went unanswered (Replayer still starting, request lost, or Replayer
        // restarted). Covers both "no Replaying yet" and "Replaying seen but the replay image never
        // attached". The request publication connecting is a separate, much shorter race — retry every poll
        // while it is still pending rather than eating a full resend interval of pure cold-start latency.
        final boolean requestPubPending = requestPublication != null && !requestPublication.isConnected();
        if (awaitingReplay && (requestPubPending || (nowMs() - lastRequestMs) > RESEND_INTERVAL_MS)) {
            requestReplay(walkSegmentIndex, requestFromPosition); // re-send the same request verbatim
        }

        // A ReplayComplete that never made it out holds our slot until the TTL, and nothing else re-sends
        // it — the walk supersedes its own slot, but a resume has no follow-up request. Outside the
        // replaySessionId block below on purpose: by the time this is pending we are caught up and back on
        // the live tap, so that block no longer runs.
        if (completePending) {
            sendReplayComplete();
        }

        if (replaySessionId >= 0) {
            // Hold our replay slot for as long as we are actually using it.
            if ((nowMs() - lastHeartbeatMs) > RESEND_INTERVAL_MS) {
                sendHeartbeat();
            }
            if (replayImage == null && replaySubscription != null) {
                replayImage = replaySubscription.imageBySessionId((int)replaySessionId);
            }
            if (replayImage != null) {
                if (!replayImage.isClosed()) {
                    work += replayImage.poll(replayHandler, FRAGMENT_LIMIT);
                    final long position = replayImage.position();
                    if (position >= catchUpPosition) {
                        // Reached this segment's bounded tip. A bounded replay of an ACTIVE (still-
                        // recording) recording does NOT close its image at the bound, so completion is
                        // detected by position.
                        onReplaySegmentComplete();
                    } else if (position != lastReplayPosition) {
                        lastReplayPosition = position;
                        lastReplayProgressMs = nowMs();
                    }
                } else {
                    // A closed image still reports its final position, so this is exact.
                    onReplayImageClosed(replayImage.position());
                }
            }
            // else: Replaying received, image not yet attached — nothing to poll this cycle, fall through
            // to the tap drain below rather than holding the whole duty cycle on it.

            // A replay that goes silent has no other way to surface. The resend timer above only covers "no
            // Replaying yet": once one arrives awaitingReplay is false, and a bounded replay of an active
            // recording never closes its image, so an image that simply stops advancing leaves this client
            // waiting on it forever with nothing retrying.
            if (replaySessionId >= 0 && (nowMs() - lastReplayProgressMs) > REPLAY_STALL_TIMEOUT_MS) {
                onReplayStalled();
            }
        }

        // Always drain the tap, even mid-walk or while merely awaiting the Replayer's answer: it is
        // untethered, so a subscription that goes unpolled falls behind the publisher's log buffer. Always
        // dispatch through the same handler too — frames ahead of an in-flight replay drop on the
        // contiguity check anyway, and the one at the seam must not be thrown away.
        if (tapSubscription != null) {
            work += tapSubscription.poll(tapHandler, FRAGMENT_LIMIT);
        }

        checkRecoveryProgress(nowMs());
        return work;
    }

    /** Whether this client is following the live tail; revoked on a tap gap, re-established at the seam. */
    public boolean isCaughtUp() {
        return caughtUp;
    }

    /**
     * Highest globalSeqNo dispatched in order, 0 before the first. The frontier a consumer measures its own
     * recovery progress by.
     */
    public long lastGlobalSeqNo() {
        return lastGlobalSeqNo;
    }

    /**
     * memberId of the current leader per the last {@code LeadershipChanged} processed, or -1 until one is
     * seen. A replica emits iff its own node is this leader.
     */
    public int currentLeaderMemberId() {
        return currentLeaderMemberId;
    }

    @Override
    public void close() {
        closeReplaySubscription();
        if (tapSubscription != null) {
            tapSubscription.close();
            tapSubscription = null;
        }
        if (controlSubscription != null) {
            controlSubscription.close();
            controlSubscription = null;
        }
        if (requestPublication != null) {
            requestPublication.close();
            requestPublication = null;
        }
        if (recoveryStalledCounter != null) {
            recoveryStalledCounter.close();
            recoveryStalledCounter = null;
        }
    }

    /**
     * Mid-walk (cold start or gap re-walk) or awaiting the Replayer's answer: a non-contiguous live-tap
     * frame is expected while this holds (the tap runs ahead of the replay), so {@link #onFrame} drops it
     * without treating it as a new gap.
     */
    boolean isRecovering() {
        return replaySessionId >= 0 || awaitingReplay;
    }

    // ── Test seams ────────────────────────────────────────────────────────────────────────────────
    // Package-private views into the walk/gap-recovery state machine, and the three transitions poll()
    // takes off a live Aeron image, which the unit suite has no way to fake. The C++ twin exposes the
    // same set as its test* methods; keep them in step.

    /** Cold-start entry point for a receiver the unit suite drives with no Aeron client. */
    void startForTest() {
        requestReplay(0, 0);
    }

    boolean isAwaitingReplay() {
        return awaitingReplay;
    }

    long replaySessionId() {
        return replaySessionId;
    }

    int walkSegmentIndex() {
        return walkSegmentIndex;
    }

    long walkRecordingId() {
        return walkRecordingId;
    }

    /** The id the next reply must carry to be acted on — see the correlation check in {@link #onControl}. */
    long requestId() {
        return requestId;
    }

    /** The fromPosition of the current request, so a test can see a gap ask to RESUME rather than re-walk. */
    long requestFromPosition() {
        return requestFromPosition;
    }

    /** Frames held ahead of the current hole. */
    int retainedFrameCount() {
        return retainFrameCount;
    }

    /** Simulates a replay image reaching its bounded catch-up position. */
    void replaySegmentCompleteForTest() {
        onReplaySegmentComplete();
    }

    /** The decision {@link #poll()} makes when it finds the replay image closed at {@code finalPosition}. */
    void replayImageClosedForTest(final long finalPosition) {
        onReplayImageClosed(finalPosition);
    }

    /**
     * Sends {@code ReplayRequest(clientId, requestId, segmentIndex, fromPosition)} and marks us awaiting
     * the reply. Idempotent on the Replayer side (it supersedes any in-flight replay for this clientId), so
     * the resend timer re-sending the same request is safe.
     *
     * <p>{@code requestId} advances on EVERY send, resends included — that is the point. A resend makes the
     * Replayer stop the in-flight session and start a new one, leaving the stale reply queued ahead of the
     * live one on the shared control stream; only a per-send id lets {@link #onControl} tell them apart.
     */
    private void requestReplay(final int segmentIndex, final long fromPosition) {
        if (segmentIndex >= 0) {
            resumeAnchorGlobalSeqNo = 0; // a walk supersedes any resume in flight
            if (segmentIndex != walkSegmentIndex) {
                // A different segment than the one in flight: nothing to compare its recordingId against yet.
                walkRecordingId = -1;
            }
        }
        walkSegmentIndex = segmentIndex;
        requestFromPosition = fromPosition;
        awaitingReplay = true;
        replaySessionId = -1;
        // Drop any unsent release: ReplayComplete names only the clientId, so one landing late — after this
        // request took a fresh slot — would free the slot this replay is riding. A new request supersedes
        // the old slot on the Replayer side anyway, so there is nothing left to release.
        completePending = false;
        closeReplaySubscription();
        lastRequestMs = nowMs();
        ++requestId;
        if (requestPublication == null || !requestPublication.isConnected()) {
            return; // Replayer not up yet; the resend timer retries
        }

        replayRequest.wrapAndApplyHeader(requestBuffer, 0, requestHeader)
                     .clientId(clientId)
                     .requestId(requestId)
                     .fromPosition(fromPosition)
                     .segmentIndex(segmentIndex);
        // Result deliberately discarded: unlike the two sends below, a request that does not land is already
        // covered — the resend timer re-sends it verbatim. Retrying it any sooner is actively harmful: every
        // send does ++requestId, and onControl only acts on a reply carrying the CURRENT id, so a per-poll
        // retry runs the counter away and every reply that arrives is discarded as stale.
        requestPublication.offer(requestBuffer, 0,
                                 MessageHeaderEncoder.ENCODED_LENGTH + replayRequest.encodedLength());
    }

    /**
     * Steady-state gap recovery: ask for the active recording resumed at the frame we last dispatched,
     * rather than re-walking the whole chain from segment 0. Repairing a one-frame drop then costs a
     * one-frame replay instead of a replay of the entire trading day — during which nothing is dispatched
     * at all, a replay slot is held, and {@link #isCaughtUp()} stays false.
     *
     * <p>A bare position only means anything against the recording it was observed in, and the active
     * recording can rotate under us. Rather than trying to prove that has not happened, the resumed replay
     * is checked where it lands: its first frame must be the very frame the position was anchored on, and
     * if it is not we fall back to the walk, which needs no position to be sound.
     */
    private void requestResume() {
        requestReplay(RESUME_SEGMENT_INDEX, lastFramePosition);
        resumeAnchorGlobalSeqNo = lastGlobalSeqNo;
    }

    /**
     * Subscribes to exactly one replay — this one — for as long as we ride it, and to nothing on the replay
     * stream the rest of the time.
     *
     * <p>Load-bearing, not tidiness. The Replayer answers every app on one shared {@code aeron:ipc} stream,
     * and an Aeron publication is flow-controlled by its slowest TETHERED subscriber. A standing
     * subscription on that stream makes every idle app a subscriber of every other app's replay — one that
     * never polls, because poll() only ever reads the image of its OWN session, so its position stays at 0
     * forever, and the archive's replay then wedges one publication window past the slowest of them. Any
     * cold start with more than a term's worth of history hangs permanently. Filtered to the session id, a
     * replay publication has exactly one subscriber.
     */
    private void openReplaySubscription(final long replaySessionId) {
        closeReplaySubscription();
        if (aeron == null) {
            return; // unit suite drives onControl with no Aeron, exactly as requestReplay tolerates
        }
        // The archive's replaySessionId carries the Aeron image session id in its low 32 bits.
        final String channel = ReplayerService.IPC_CHANNEL + "?session-id=" + (int)replaySessionId;
        replaySubscription = aeron.addSubscription(channel, ReplayerService.REPLAY_STREAM_ID);
    }

    private void closeReplaySubscription() {
        replayImage = null;
        if (replaySubscription != null) {
            replaySubscription.close();
            replaySubscription = null;
        }
    }

    /**
     * Releases our replay slot: we have reached the tip the Replayer bounded us to and are back on the live
     * tap. Only the resume path needs this — every step of a cold-start walk supersedes its own slot with
     * the next segment's request, and the walk's last request frees it via NO_REPLAY_NEEDED, whereas a
     * resume has no follow-up request at all. Unlike a request this has no timer behind it and no follow-up
     * that would supersede it, so a single dropped offer would be the whole release — hence
     * {@link #completePending}, retried from {@link #poll()} until it lands.
     */
    private void sendReplayComplete() {
        completePending = true;
        if (requestPublication == null || !requestPublication.isConnected()) {
            return;
        }
        replayComplete.wrapAndApplyHeader(requestBuffer, 0, requestHeader).clientId(clientId);
        final long result = requestPublication.offer(
            requestBuffer, 0, MessageHeaderEncoder.ENCODED_LENGTH + replayComplete.encodedLength());
        completePending = result < 0;
    }

    /**
     * Refreshes this client's replay slot while it rides an attached image, so the Replayer's idle TTL
     * measures "client stopped using the slot" rather than "the replay took a while" — a replay of a full
     * trading day legitimately outlives any fixed TTL. Best-effort: a lost heartbeat only risks the slot
     * being reclaimed, which the truncated-close path recovers from by re-requesting.
     */
    private void sendHeartbeat() {
        if (requestPublication == null || !requestPublication.isConnected()) {
            return;
        }
        replayHeartbeat.wrapAndApplyHeader(requestBuffer, 0, requestHeader).clientId(clientId);
        // Only a landed heartbeat refreshes the slot, so the clock measures when the Replayer last actually
        // heard from us. Advancing it on a dropped offer burns a whole interval per loss and walks a healthy
        // client toward the TTL in silence.
        if (requestPublication.offer(requestBuffer, 0,
                                     MessageHeaderEncoder.ENCODED_LENGTH + replayHeartbeat.encodedLength()) >= 0) {
            lastHeartbeatMs = nowMs();
        }
    }

    /** Decodes one Replayer control reply. Package-private so the unit suite can drive it without Aeron. */
    void onControl(final DirectBuffer buffer, final int offset, final int length) {
        if (length < org.limitless.phixeron.sbe.unsequenced.MessageHeaderDecoder.ENCODED_LENGTH) {
            return;
        }
        controlHeader.wrap(buffer, offset);
        final int bodyOffset = offset + controlHeader.encodedLength();
        final int blockLength = controlHeader.blockLength();
        final int version = controlHeader.version();

        if (controlHeader.templateId() == ReplayingDecoder.TEMPLATE_ID) {
            replaying.wrap(buffer, bodyOffset, blockLength, version);
            if (replaying.clientId() != clientId) {
                return; // another replica's reply on the shared control stream
            }
            if (replaying.requestId() != requestId) {
                // Answer to a request we have already superseded by re-sending: its replay session was
                // stopped when the Replayer took the newer request. Attaching to it would ride an image that
                // closes short of its bound, and clearing awaitingReplay would stop the resend timer while
                // no live replay exists.
                return;
            }
            onReplaying(replaying.replaySessionId(), replaying.catchUpPosition(), replaying.recordingId());
        } else if (controlHeader.templateId() == ReplayPendingDecoder.TEMPLATE_ID) {
            replayPending.wrap(buffer, bodyOffset, blockLength, version);
            if (replayPending.clientId() == clientId && replayPending.requestId() == requestId) {
                // Replayer has no free slot; keep holding. awaitingReplay stays true so the resend timer
                // keeps us alive if the eventual Replaying is ever lost, but ReplayPending itself is just
                // "wait" — reset the request clock so we don't spam while queued.
                lastRequestMs = nowMs();
                replayerUnavailable = false; // queued, not refused — the episode ended
            }
        } else if (controlHeader.templateId() == ReplayUnavailableDecoder.TEMPLATE_ID) {
            replayUnavailable.wrap(buffer, bodyOffset, blockLength, version);
            if (replayUnavailable.clientId() == clientId && replayUnavailable.requestId() == requestId) {
                onReplayUnavailable();
            }
        }
    }

    private void onReplaying(final long session, final long replayCatchUpPosition, final long recordingId) {
        awaitingReplay = false;
        // The Replayer is serving again: whatever refusal episode was open has ended. Clearing here is what
        // makes a second, distinct outage report itself.
        replayerUnavailable = false;
        if (session == ReplayerService.NO_REPLAY_NEEDED) {
            if (walkSegmentIndex < 0) {
                // We asked to resume at a position we know sits below a hole, and the Replayer says that
                // position is already at the recording's tip — so it is not our recording any more.
                // Declaring ourselves caught up here would close the hole by fiat.
                Logger.log(Logger.Component.ReplayerStreamReceiver, Logger.Severity.Warn, Logger.EventCode.TapGap,
                           memberId,
                           "resume at position %d answered 'nothing to replay' while a hole is open above "
                               + "globalSeqNo=%d — the active recording rotated under us; re-walking the "
                               + "recording chain from segment 0",
                           requestFromPosition, lastGlobalSeqNo);
                requestReplay(0, 0);
                return;
            }
            if (recordingId >= 0) {
                // Not the walk terminator. serveReplay answers NO_REPLAY_NEEDED for two different things and
                // tells them apart by this field: it names the recording it found nothing in when a segment
                // is merely EMPTY, and names none at all only once the request ran past the last recording in
                // the chain. Ending the walk on the former drops every later segment.
                requestReplay(walkSegmentIndex + 1, 0);
                return;
            }
            replaySessionId = -1; // already at the tip — follow the live tap
            walkSegmentIndex = -1; // chain exhausted (or never a walk) → steady/resume mode
            // The chain is exhausted, but the frontier is what the retained-ahead FIFO knows, not what the
            // chain covered: a frame retained during the walk may still sit behind a hole the replay never
            // reached, and an overflow dropped tap frames outright. Same guard, same reason, as the resume
            // path in onReplaySegmentComplete.
            drainRetained();
            if (!retained.isEmpty() || retainOverflowed) {
                endOverflowEpisode();
                requestReplay(0, 0);
                return;
            }
            notifyCaughtUp();
            return;
        }

        if (walkSegmentIndex >= 0) {
            // serveReplay re-resolves the recording chain on every request, and a stale still-recording span
            // can be dropped from it once a newer one supersedes it — shifting which recording this
            // segmentIndex denotes. A retried request for the SAME index must land on the SAME recording it
            // did the first time; anything else means the chain moved under it.
            if (walkRecordingId >= 0 && recordingId != walkRecordingId) {
                Logger.log(Logger.Component.ReplayerStreamReceiver, Logger.Severity.Warn, Logger.EventCode.TapGap,
                           memberId,
                           "walk segment %d now resolves to recording %d, previously %d — the recording chain "
                               + "shifted under us; re-walking from segment 0",
                           walkSegmentIndex, recordingId, walkRecordingId);
                requestReplay(0, 0);
                return;
            }
            walkRecordingId = recordingId;
        }
        replaySessionId = session;
        catchUpPosition = replayCatchUpPosition;
        openReplaySubscription(session);
        // Arm the stall watchdog from here: this is the moment the replay starts existing.
        lastReplayPosition = -1;
        lastReplayProgressMs = nowMs();
    }

    /**
     * The node's Replayer failed its startup integrity check: its archive does not reach globalSeqNo 1, so
     * it has no valid history for anyone and says so instead of serving a mid-stream replay we would abort
     * on. Deliberately NOT fatal here — that is the containment: an operator repairs the archive and
     * restarts the Replayer, and the resend timer picks up where it left off with no app restart. We simply
     * never go caught up, so every consumer gate stays shut.
     */
    private void onReplayUnavailable() {
        if (!replayerUnavailable) {
            replayerUnavailable = true;
            Logger.fault(Logger.Component.ReplayerStreamReceiver, Logger.EventCode.ReplayUnavailable, memberId,
                         "this node's Replayer has no valid history to serve (its archive failed the "
                             + "globalSeqNo-1 integrity check) — holding, not dispatching; repair the node's "
                             + "archive and restart its Replayer");
        }
        // Hold exactly as for ReplayPending: still awaiting, request clock reset so the resend paces at the
        // normal interval rather than spinning on a permanent condition.
        lastRequestMs = nowMs();
    }

    /**
     * Decodes one sequenced frame and applies the contiguity rules. Package-private so the unit suite can
     * drive the identical path with no Aeron subscription or media driver.
     * @param framePosition where this frame starts in the recording — the tap, a replay image and the
     *                      recording itself all count positions in the same space
     * @param fromReplay    whether the frame arrived on a replay image rather than the live tap
     */
    void onFrame(final DirectBuffer buffer, final int offset, final int length, final long framePosition,
                 final boolean fromReplay) {
        final long receiveNs = nowNs();
        if (length < MessageHeaderDecoder.ENCODED_LENGTH + HeaderDecoder.ENCODED_LENGTH) {
            return;
        }

        messageHeader.wrap(buffer, offset);
        if (messageHeader.schemaId() != MessageHeaderDecoder.SCHEMA_ID) {
            return;
        }
        header.wrap(buffer, offset + MessageHeaderDecoder.ENCODED_LENGTH);
        final long globalSeqNo = header.globalSeqNo();

        // First frame off a resume replay: it must be the frame whose position we anchored the request on.
        // Anything else means that position no longer denotes that frame — the active recording rotated
        // under us — so drop back to the walk rather than ride an arbitrary mid-stream point. Checked ahead
        // of the de-dupe below, which would otherwise swallow the anchor frame itself and leave the mismatch
        // invisible.
        if (fromReplay && resumeAnchorGlobalSeqNo != 0) {
            final long anchor = resumeAnchorGlobalSeqNo;
            resumeAnchorGlobalSeqNo = 0;
            if (globalSeqNo != anchor) {
                Logger.log(Logger.Component.ReplayerStreamReceiver, Logger.Severity.Warn, Logger.EventCode.TapGap,
                           memberId,
                           "resume replay opened at globalSeqNo=%d, expected %d — the active recording rotated "
                               + "under us; re-walking the recording chain from segment 0",
                           globalSeqNo, anchor);
                requestReplay(0, 0);
                return;
            }
        }

        // Contiguity / de-duplication: globalSeqNo increments by exactly one per event, so any forward jump
        // is a gap. Drop dups; a frame from beyond the hole is RETAINED rather than dropped, so the replay
        // closing the hole hands straight over to it.
        if (lastGlobalSeqNo != 0) {
            if (globalSeqNo <= lastGlobalSeqNo) {
                return;
            }
            if (globalSeqNo > lastGlobalSeqNo + 1) {
                // Only a steady-state hole is a gap worth re-walking for. Mid-walk the tap legitimately runs
                // ahead of the replay, so isRecovering() suppresses the trigger and lets the in-flight walk
                // finish rather than superseding it with the frames it is racing.
                if (!fromReplay && !isRecovering()) {
                    Logger.log(Logger.Component.ReplayerStreamReceiver, Logger.Severity.Warn,
                               Logger.EventCode.TapGap, memberId,
                               "tap gap: expected globalSeqNo=%d got %d — resuming the recording at "
                                   + "globalSeqNo=%d",
                               lastGlobalSeqNo + 1, globalSeqNo, lastGlobalSeqNo);
                    // No longer following live: consumers gate real decisions on isCaughtUp(), and it must
                    // not hold again until the stream goes contiguous.
                    caughtUp = false;
                    requestResume();
                }
                if (!fromReplay) {
                    retainFrame(globalSeqNo, buffer, offset, length, framePosition, receiveNs);
                } else if (!replayGapLogged) {
                    // A hole in REPLAYED history: either the recording chain itself is discontinuous, or the
                    // tethered IPC replay lost a fragment, which it should not. Nothing unsafe follows — the
                    // frame is dropped and the contiguity invariant still holds — but the walk cannot
                    // converge past this, so it is worth saying once rather than retrying in silence.
                    replayGapLogged = true;
                    Logger.log(Logger.Component.ReplayerStreamReceiver, Logger.Severity.Warn,
                               Logger.EventCode.TapGap, memberId,
                               "gap in REPLAYED history: expected globalSeqNo=%d got %d — this node's recording "
                                   + "chain does not cover the hole; recovery cannot converge until it does",
                               lastGlobalSeqNo + 1, globalSeqNo);
                }
                return;
            }
        } else if (globalSeqNo != 1) {
            if (!fromReplay && isRecovering()) {
                // No baseline yet and the cold-start walk is still in flight: this is just the live tap
                // running ahead of a replay that has not reached globalSeqNo 1 yet. Only the walk may
                // establish the baseline — adopting this frame's would BE the arbitrary mid-stream baseline
                // the abort below exists to prevent — but it is still real data, so retain it.
                retainFrame(globalSeqNo, buffer, offset, length, framePosition, receiveNs);
                return;
            }
            // The very first frame this client ever sees — replayed history, or the live tap right after a
            // cold-start NO_REPLAY_NEEDED, which the Replayer sends at segment 0 only when the recording is
            // empty — must be globalSeqNo 1. Fatal, exactly as the C++ twin's abort() is: dispatching from an
            // arbitrary mid-stream baseline would silently serve a session whose history has a hole in it.
            // Logged before throwing so the reason survives even if the caller only reports the exception.
            Logger.fault(Logger.Component.ReplayerStreamReceiver, Logger.EventCode.FirstFrameNotOne, memberId,
                         "FATAL: first frame observed has globalSeqNo=%d, expected 1 — this node's recording "
                             + "does not reach the start of the log",
                         globalSeqNo);
            throw new IllegalStateException(
                "first frame observed has globalSeqNo=" + globalSeqNo
                    + ", expected 1 — this node's recording does not reach the start of the log");
        }
        dispatchFrame(buffer, offset, length, globalSeqNo, framePosition, receiveNs, fromReplay);
        drainRetained();
    }

    /**
     * Everything past the contiguity check: advance the baseline, then decode and hand the frame to the
     * caller. Split out so a frame drained from the retained FIFO (which lives in its own storage, not the
     * subscription's) runs the identical path.
     */
    private void dispatchFrame(final DirectBuffer buffer, final int offset, final int length,
                               final long globalSeqNo, final long framePosition, final long receiveNs,
                               final boolean fromReplay) {
        // The one funnel every in-order frame passes through, replayed or live — so recovery advancing its
        // globalSeqNo is exactly this being reached. onProgress returns the falling edge only, so the gauge
        // is written once per episode rather than once per frame.
        if (recoveryProgress.onProgress() && recoveryStalledCounter != null) {
            recoveryStalledCounter.set(0);
        }
        messageHeader.wrap(buffer, offset);
        final int templateId = messageHeader.templateId();
        final int blockLength = messageHeader.blockLength();
        final int version = messageHeader.version();
        final int bodyOffset = offset + MessageHeaderDecoder.ENCODED_LENGTH;
        header.wrap(buffer, bodyOffset);

        lastGlobalSeqNo = globalSeqNo;
        replayGapLogged = false;
        lastFramePosition = framePosition;
        if (!fromReplay && !caughtUp && !retainOverflowed) {
            // First in-order frame straight off the live tap ⇒ we are following the live tip. Unless
            // retainFrame dropped frames: this one may well be a retained frame draining over a closed hole
            // with the dropped ones still missing above it, and contiguity here says nothing about them.
            notifyCaughtUp();
        }

        if (templateId == LEADERSHIP_CHANGED_TEMPLATE_ID) {
            leadershipChanged.wrap(buffer, bodyOffset, blockLength, version);
            currentLeaderMemberId = leadershipChanged.newLeaderMemberId();
            if (onLeadershipChanged != null) {
                onLeadershipChanged.onLeadershipChanged(currentLeaderMemberId, globalSeqNo);
            }
            return;
        }
        if (onSequenced != null) {
            event.set(globalSeqNo, header.sourceId(), header.connectionId(), header.sessionId(), header.timestamp(),
                      receiveNs, header.origin(), templateId, blockLength, version, buffer, offset, length,
                      framePosition);
            onSequenced.onSequenced(event);
        }
    }

    /**
     * Keeps a live tap frame that sits beyond the current hole. This is what actually closes the
     * replay-to-live seam: the tap must be drained every duty cycle (it is untethered, so an unpolled
     * subscription falls behind), which means a frame not kept here is GONE — and every frame published
     * while a walk replays history is such a frame.
     *
     * <p>Bounded, and deliberately lossy past the bound: a re-walk over a full trading day cannot buffer a
     * day of traffic, so on overflow this falls back to drop-and-re-walk rather than growing without limit.
     * Retained as a FIFO of pooled blocks, not sorted by globalSeqNo: a single Aeron image delivers strictly
     * increasing globalSeqNo with no reordering, so arrival order already is globalSeqNo order — only an
     * exact-duplicate redelivery needs an explicit check, not general sorting.
     */
    private void retainFrame(final long globalSeqNo, final DirectBuffer buffer, final int offset, final int length,
                             final long framePosition, final long receiveNs) {
        final int recordSize = RetainBlock.RECORD_HEADER_LENGTH + length;
        if (retainFrameCount >= MAX_RETAINED_FRAMES || (retainBytes + length) > MAX_RETAINED_BYTES
            || recordSize > RetainBlock.SIZE) {
            retainOverflowed = true;
            if (!retainOverflowLogged) {
                retainOverflowLogged = true;
                Logger.log(Logger.Component.ReplayerStreamReceiver, Logger.Severity.Warn, Logger.EventCode.TapGap,
                           memberId,
                           "retained-frame buffer full at globalSeqNo=%d (%d frames, %d bytes) — dropping "
                               + "ahead-of-hole frames; recovery falls back to re-walking",
                           globalSeqNo, retainFrameCount, retainBytes);
            }
            return;
        }
        if (retainFrameCount > 0 && globalSeqNo <= retainTailGlobalSeqNo) {
            return; // already retained (the tap redelivered it) — keep the first copy
        }
        if (retained.isEmpty() || retained.peekLast().used + recordSize > RetainBlock.SIZE) {
            retained.addLast(acquireBlock());
        }
        final RetainBlock tail = retained.peekLast();
        tail.buffer.putLong(tail.used, globalSeqNo);
        tail.buffer.putLong(tail.used + Long.BYTES, framePosition);
        tail.buffer.putLong(tail.used + 2 * Long.BYTES, receiveNs);
        tail.buffer.putInt(tail.used + 3 * Long.BYTES, length);
        tail.buffer.putBytes(tail.used + RetainBlock.RECORD_HEADER_LENGTH, buffer, offset, length);
        tail.used += recordSize;
        ++retainFrameCount;
        retainBytes += length;
        retainTailGlobalSeqNo = globalSeqNo;
    }

    /**
     * Hands over every retained frame that has become contiguous, discarding any the replay has since
     * covered. Called after each dispatch, so the seam closes the instant the replay reaches it. A block is
     * returned to the pool the moment it is fully consumed, so the next recovery episode reuses
     * already-resident memory instead of paying a fresh allocation.
     */
    private void drainRetained() {
        while (true) {
            final RetainBlock front = retained.peekFirst();
            if (front == null) {
                return;
            }
            if (retainReadOffset >= front.used) {
                retained.pollFirst();
                releaseBlock(front);
                retainReadOffset = 0;
                continue;
            }
            final long globalSeqNo = front.buffer.getLong(retainReadOffset);
            if (globalSeqNo > lastGlobalSeqNo + 1) {
                return; // still a hole below the oldest retained frame
            }
            final long position = front.buffer.getLong(retainReadOffset + Long.BYTES);
            final long receiveNs = front.buffer.getLong(retainReadOffset + 2 * Long.BYTES);
            final int length = front.buffer.getInt(retainReadOffset + 3 * Long.BYTES);
            final int payloadOffset = retainReadOffset + RetainBlock.RECORD_HEADER_LENGTH;
            retainReadOffset += RetainBlock.RECORD_HEADER_LENGTH + length;
            --retainFrameCount;
            retainBytes -= length;
            if (globalSeqNo <= lastGlobalSeqNo) {
                continue; // the replay already covered it
            }
            dispatchFrame(front.buffer, payloadOffset, length, globalSeqNo, position, receiveNs, false);
        }
    }

    /**
     * The re-walk about to be requested is what covers the frames {@link #retainFrame} dropped, so the
     * overflow ends HERE, where it is acted on — not in {@link #drainRetained}, where clearing it would
     * forget the drop at exactly the wrong moment: the frame whose dispatch cleared it would then declare us
     * caught up at the seam, over a frontier the drops had already invalidated.
     */
    private void endOverflowEpisode() {
        retainOverflowed = false;
        retainOverflowLogged = false;
    }

    /**
     * Decides what a closed replay image means, from the position it closed at.
     *
     * <p>A stopped historical segment's bounded replay closes on its own exactly at its stopPosition — which
     * is the bound we were handed, so that IS completion. Every other close is not: the Replayer stopped
     * this replay (superseded by a resend, or its slot reclaimed by the idle TTL), the archive faulted, or
     * the Replayer shut down — and the image then closes SHORT of the bound. Treating those as completion
     * advances the walk over a segment that was never fully replayed, silently leaving a hole in history
     * that only the next tap gap would ever expose.
     */
    private void onReplayImageClosed(final long finalPosition) {
        if (finalPosition >= catchUpPosition) {
            onReplaySegmentComplete();
            return;
        }
        Logger.log(Logger.Component.ReplayerStreamReceiver, Logger.Severity.Warn, Logger.EventCode.TapGap, memberId,
                   "replay image closed at position %d, short of catchUpPosition %d — the replay was stopped "
                       + "under us; re-requesting the same segment (index %d)",
                   finalPosition, catchUpPosition, walkSegmentIndex);
        if (walkSegmentIndex < 0) {
            // A resume, not a walk step: re-anchor via requestResume() rather than re-sending the stale
            // requestFromPosition verbatim, which would carry no anchor for onFrame to validate against.
            requestResume();
        } else {
            requestReplay(walkSegmentIndex, requestFromPosition); // same request verbatim, new requestId
        }
    }

    /**
     * Recovery has run without dispatching a frame for {@link #RECOVERY_PROGRESS_TIMEOUT_MS}. Evaluated
     * every duty cycle; {@code nowMs} is a parameter only so the unit suite can supply a clock.
     * @return whether it reported
     */
    boolean checkRecoveryProgress(final long nowMs) {
        if (caughtUp || !recoveryProgress.onNoProgress(nowMs)) {
            return false;
        }
        // Reported, not acted on: holding IS the correct response to a baseline this node cannot establish,
        // so the only thing missing was someone saying so. The state printed is what tells the causes apart —
        // a chain that cannot cover the hole, a Replayer that never answers, a refusal.
        Logger.fault(Logger.Component.ReplayerStreamReceiver, Logger.EventCode.RecoveryStalled, memberId,
                     "recovery has dispatched nothing for >%dms: lastGlobalSeqNo=%d segment=%d awaitingReplay=%b "
                         + "replaySession=%d replayerUnavailable=%b — holding; check this node's Replayer and "
                         + "its recording chain",
                     RECOVERY_PROGRESS_TIMEOUT_MS, lastGlobalSeqNo, walkSegmentIndex, awaitingReplay,
                     replaySessionId, replayerUnavailable);
        if (recoveryStalledCounter != null) {
            recoveryStalledCounter.set(1);
        }
        return true;
    }

    /** An attached (or expected) replay stopped delivering — see the watchdog check in {@link #poll()}. */
    private void onReplayStalled() {
        Logger.log(Logger.Component.ReplayerStreamReceiver, Logger.Severity.Warn, Logger.EventCode.TapGap, memberId,
                   "replay session %d made no progress for %dms at position %d of catchUpPosition %d "
                       + "(image %s) — re-requesting segment %d",
                   replaySessionId, REPLAY_STALL_TIMEOUT_MS, lastReplayPosition, catchUpPosition,
                   replayImage != null ? "attached" : "never attached", walkSegmentIndex);
        if (walkSegmentIndex < 0) {
            requestResume(); // a resume, not a walk step: re-anchor — see onReplayImageClosed
        } else {
            requestReplay(walkSegmentIndex, requestFromPosition); // same request verbatim, new requestId
        }
    }

    /** A replay segment finished (reached its bounded tip, or its image closed for a stopped segment). */
    private void onReplaySegmentComplete() {
        closeReplaySubscription();
        replaySessionId = -1;
        if (walkSegmentIndex < 0) {
            // A resume, not a walk step: there is no next segment. We hold every frame the recording had when
            // the request was served, so we are back at the tip — anything published since is on the tap,
            // retained ahead of the hole we just closed. Drain it before trusting that: a retained-ahead frame
            // the replay didn't cover may itself sit behind a hole, and an overflow silently dropped tap
            // frames outright, past the bound this replay was even asked to cover.
            drainRetained();
            if (!retained.isEmpty() || retainOverflowed) {
                endOverflowEpisode();
                requestReplay(0, 0);
                return;
            }
            sendReplayComplete();
            notifyCaughtUp();
            return;
        }
        requestReplay(walkSegmentIndex + 1, 0); // advance the walk to the next segment
    }

    private void notifyCaughtUp() {
        if (caughtUp) {
            return; // idempotent — reached from the replay-tip, no-replay, and first-live-frame paths
        }
        // Fires again after a gap cleared caughtUp, so consumers can re-arm on re-convergence rather than
        // only on first catch-up.
        caughtUp = true;
        if (onCaughtUp != null) {
            onCaughtUp.onCaughtUp();
        }
    }

    private RetainBlock acquireBlock() {
        if (retainPool.isEmpty()) {
            return new RetainBlock();
        }
        final RetainBlock block = retainPool.remove(retainPool.size() - 1);
        block.used = 0;
        return block;
    }

    private void releaseBlock(final RetainBlock block) {
        retainPool.add(block);
    }

    /**
     * Stream position of the first byte of the frame {@code header} describes. Deliberately not
     * {@code header.position() - frameLength}: {@code position()} is the NEXT frame's position (this
     * frame's end rounded up to the 32-byte frame alignment), so subtracting an unaligned SBE length lands
     * short of the true start and is itself unaligned — and a replay position must sit on a frame boundary.
     * Under a FragmentAssembler it is further off. Term offsets are always frame-aligned, so this form is
     * exact in both cases.
     */
    private static long frameStartPosition(final io.aeron.logbuffer.Header header) {
        return LogBufferDescriptor.computePosition(header.termId(), header.termOffset(),
                                                   header.positionBitsToShift(), header.initialTermId());
    }

    private static long nowMs() {
        return System.currentTimeMillis();
    }

    private static long nowNs() {
        return System.currentTimeMillis() * 1_000_000L;
    }

    /**
     * Fixed-size block backing the retained-ahead FIFO. Records are appended length-prefixed and never split
     * across a block boundary — every message here is well under 512 bytes, so the wasted tail per boundary
     * is bounded and negligible against {@link #SIZE}.
     */
    private static final class RetainBlock {
        static final int SIZE = 4096;

        /** globalSeqNo, position, receiveNs, length — the per-record framing within a block. */
        static final int RECORD_HEADER_LENGTH = 3 * Long.BYTES + Integer.BYTES;

        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[SIZE]);
        int used;
    }
}
