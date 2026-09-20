package org.limitless.seqeron.replayer.client;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.limitless.seqeron.protocol.ReplayProtocol;
import org.limitless.seqeron.protocol.SequencedFrameDecoder;
import org.limitless.seqeron.protocol.SystemFrame;
import org.limitless.seqeron.replayer.client.ReplayerStreamReceiver.CaughtUpHandler;
import org.limitless.seqeron.replayer.client.ReplayerStreamReceiver.LeadershipHandler;
import org.limitless.seqeron.replayer.client.ReplayerStreamReceiver.SequencedHandler;
import org.limitless.seqeron.sbe.frame.LeadershipChangedDecoder;
import org.limitless.seqeron.sbe.frame.MessageHeaderDecoder;
import org.limitless.seqeron.sbe.replay.ReplayPendingDecoder;
import org.limitless.seqeron.sbe.replay.ReplayUnavailableDecoder;
import org.limitless.seqeron.sbe.replay.ReplayingDecoder;
import org.limitless.seqeron.util.Logger;

/**
 * The state machine of {@link ReplayerStreamReceiver}. Catch-up is detected by position
 * ({@code Replaying.catchUpPosition}); a gap by globalSeqNo, repaired by resuming the active recording at
 * the last dispatched frame.
 *
 * <p>The replay-to-live seam is closed by the tap itself: the untethered tap is polled every duty cycle, so
 * frames beyond the current hole are retained and drained once contiguous. While {@link #isRecovering()} a
 * non-contiguous tap frame is expected rather than a new gap, and it may not set the globalSeqNo baseline
 * before the walk has — a cold start would otherwise adopt a mid-stream baseline.
 *
 * <p>{@link #isCaughtUp()} is cleared on a live-tap gap and re-established once contiguous: consumers gate
 * real decisions on it. Single-threaded: every method runs on the one duty-cycle thread.
 */
final class ReplayerRecovery {
    private static final long RESEND_INTERVAL_MS = 500;

    /**
     * How long an established replay may deliver nothing before it is re-requested. The archive reads local
     * disk, so a replay with anything left is never quiet this long; a spurious fire re-replays a segment.
     */
    private static final long REPLAY_STALL_TIMEOUT_MS = 5_000;

    /**
     * How long recovery may dispatch nothing before it is reported unconvergent: a re-walk loop that keeps
     * finishing healthy replays yet dispatches nothing, which {@link #REPLAY_STALL_TIMEOUT_MS} cannot see.
     */
    private static final long RECOVERY_PROGRESS_TIMEOUT_MS = 30_000;

    /** {@code ReplayRequest.segmentIndex} meaning "resume the active recording at fromPosition". */
    private static final int RESUME_SEGMENT_INDEX = -1;

    /** {@code LeadershipChanged}, synthesized onto the sequenced stream; intercepted, never dispatched. */
    private static final int LEADERSHIP_CHANGED = SystemFrame.LEADERSHIP_CHANGED;

    /** Caps on frames retained ahead of a hole; past either, recovery falls back to re-walking. */
    private static final int MAX_RETAINED_FRAMES = 65536;

    private static final long MAX_RETAINED_BYTES = 16L * 1024 * 1024;

    private final int clientId;
    private final ReplayerRecoveryActions actions;
    private final SequencedHandler onSequenced;
    private final LeadershipHandler onLeadershipChanged;
    private final CaughtUpHandler onCaughtUp;

    private final SequencedFrameDecoder view = new SequencedFrameDecoder();
    private final LeadershipChangedDecoder leadershipChanged = new LeadershipChangedDecoder();
    private final org.limitless.seqeron.sbe.replay.MessageHeaderDecoder controlHeader =
        new org.limitless.seqeron.sbe.replay.MessageHeaderDecoder();
    private final ReplayingDecoder replaying = new ReplayingDecoder();
    private final ReplayPendingDecoder replayPending = new ReplayPendingDecoder();
    private final ReplayUnavailableDecoder replayUnavailable = new ReplayUnavailableDecoder();

    private final SequencedEvent event = new SequencedEvent();

    /** When the current no-progress episode started; 0 = none timed. */
    private long noProgressSinceMs;
    private boolean recoveryStallReported;

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
     * A {@code ReplayComplete} that did not land (transient back-pressure), retried from {@link #doTimers}.
     * Nothing supersedes a release, unlike a request.
     */
    private boolean completePending;

    private long lastReplayPosition = -1;
    private long lastReplayProgressMs;

    /** The Replayer is refusing to serve us: reported once per episode, and named in the stall report. */
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
     * Frames were dropped ahead of the hole because the FIFO was full, so this client's frontier is short:
     * it must re-walk rather than declare itself caught up. Cleared by {@link #endOverflowEpisode}.
     */
    private boolean retainOverflowed;

    private boolean retainOverflowLogged;

    /**
     * @param clientId            this replica's stable id, unique among the Replayer's co-located apps
     *                            ({@code SEQERON_REPLAYER_CLIENT_ID}); two apps sharing one supersede
     *                            each other's replays and neither ever catches up
     * @param actions             performs the sends and the replay subscription this class decides on, and
     *                            supplies the clock
     * @param onSequenced         receives every in-order frame
     * @param onLeadershipChanged receives each leadership change, or null
     * @param onCaughtUp          fires on every transition to caught-up, or null
     */
    public ReplayerRecovery(final int clientId, final ReplayerRecoveryActions actions,
                            final SequencedHandler onSequenced, final LeadershipHandler onLeadershipChanged,
                            final CaughtUpHandler onCaughtUp) {
        this.clientId = clientId;
        this.actions = actions;
        this.onSequenced = onSequenced;
        this.onLeadershipChanged = onLeadershipChanged;
        this.onCaughtUp = onCaughtUp;
    }

    /** Cold start: walk the recording chain from segment 0. */
    public void start() {
        requestReplay(0, 0);
    }

    /**
     * Decodes one sequenced frame and applies the contiguity rules.
     * @param framePosition where this frame starts in the recording — the tap, a replay image and the
     *                      recording itself all count positions in the same space
     * @param receiveNs     when the frame arrived, so a consumer's delivery-latency stats measure the tap
     *                      rather than the drain of the retained FIFO
     * @param fromReplay    whether the frame arrived on a replay image rather than the live tap
     */
    public void onFrame(final DirectBuffer buffer, final int offset, final int length, final long framePosition,
                        final long receiveNs, final boolean fromReplay) {
        if (!view.wrap(buffer, offset, length)) {
            return;
        }
        final long globalSeqNo = view.globalSeqNo();

        // First frame off a resume replay: it must be the frame whose position requested.
        if (fromReplay && resumeAnchorGlobalSeqNo != 0) {
            final long anchor = resumeAnchorGlobalSeqNo;
            resumeAnchorGlobalSeqNo = 0;
            if (globalSeqNo != anchor) {
                Logger.log(Logger.CoreComponent.ReplayerStreamReceiver, Logger.Severity.Warn,
                           Logger.CoreEventCode.TapGap,
                           actions.memberId(),
                           "resume replay opened at globalSeqNo=%d, expected %d — the active recording rotated "
                               + "under us; re-walking the recording chain from segment 0",
                           globalSeqNo, anchor);
                requestReplay(0, 0);
                return;
            }
        }
        if (lastGlobalSeqNo != 0) {
            if (globalSeqNo <= lastGlobalSeqNo) {
                return;
            }
            if (globalSeqNo > lastGlobalSeqNo + 1) {
                if (!fromReplay && !isRecovering()) {
                    Logger.log(Logger.CoreComponent.ReplayerStreamReceiver, Logger.Severity.Warn,
                               Logger.CoreEventCode.TapGap, actions.memberId(),
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
                    replayGapLogged = true;
                    Logger.log(Logger.CoreComponent.ReplayerStreamReceiver, Logger.Severity.Warn,
                               Logger.CoreEventCode.TapGap, actions.memberId(),
                               "gap in REPLAYED history: expected globalSeqNo=%d got %d — this node's recording "
                                   + "chain does not cover the hole; recovery cannot converge until it does",
                               lastGlobalSeqNo + 1, globalSeqNo);
                }
                return;
            }
        } else if (globalSeqNo != 1) {
            if (!fromReplay && isRecovering()) {
                retainFrame(globalSeqNo, buffer, offset, length, framePosition, receiveNs);
                return;
            }
            Logger.fault(Logger.CoreComponent.ReplayerStreamReceiver, Logger.CoreEventCode.FirstFrameNotOne,
                         actions.memberId(),
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

    /** Decodes one Replayer control reply ({@code Replaying}/{@code ReplayPending}/{@code ReplayUnavailable}). */
    public void onControl(final DirectBuffer buffer, final int offset, final int length) {
        if (length < org.limitless.seqeron.sbe.replay.MessageHeaderDecoder.ENCODED_LENGTH) {
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
                return;
            }
            onReplaying(replaying.replaySessionId(), replaying.catchUpPosition(), replaying.recordingId());
        } else if (controlHeader.templateId() == ReplayPendingDecoder.TEMPLATE_ID) {
            replayPending.wrap(buffer, bodyOffset, blockLength, version);
            if (replayPending.clientId() == clientId && replayPending.requestId() == requestId) {
                lastRequestMs = actions.nowMs();
                replayerUnavailable = false; // queued, not refused — the episode ended
            }
        } else if (controlHeader.templateId() == ReplayUnavailableDecoder.TEMPLATE_ID) {
            replayUnavailable.wrap(buffer, bodyOffset, blockLength, version);
            if (replayUnavailable.clientId() == clientId && replayUnavailable.requestId() == requestId) {
                onReplayUnavailable();
            }
        }
    }

    /**
     * Where the attached replay image has reached. Completion is by position, not by the image closing: a
     * bounded replay of an ACTIVE (still-recording) recording does NOT close its image at the bound.
     */
    public void onReplayPosition(final long position) {
        if (position >= catchUpPosition) {
            onReplaySegmentComplete();
            return;
        }
        if (position != lastReplayPosition) {
            lastReplayPosition = position;
            lastReplayProgressMs = actions.nowMs();
        }
    }

    /**
     * Decides what a closed replay image means. A stopped segment's bounded replay closes exactly at its
     * bound, which is completion; any close short of it (superseded, TTL-reclaimed, faulted) is not, and
     * advancing the walk over it would leave a silent hole.
     */
    public void onReplayImageClosed(final long finalPosition) {
        if (finalPosition >= catchUpPosition) {
            onReplaySegmentComplete();
            return;
        }
        Logger.log(Logger.CoreComponent.ReplayerStreamReceiver, Logger.Severity.Warn, Logger.CoreEventCode.TapGap,
                   actions.memberId(),
                   "replay image closed at position %d, short of catchUpPosition %d — the replay was stopped "
                       + "under us; re-requesting the same segment (index %d)",
                   finalPosition, catchUpPosition, walkSegmentIndex);
        reRequestCurrent();
    }

    /**
     * Timer-driven work, once per duty cycle: the request resend, an unsent {@code ReplayComplete}, the
     * replay-slot heartbeat, and the replay stall watchdog.
     * @param requestPublicationPending the request publication has not connected yet — retried every cycle
     *                                  rather than eating a full resend interval for a race much shorter
     *                                  than that
     */
    public void doTimers(final boolean requestPublicationPending) {
        final long nowMs = actions.nowMs();
        if (awaitingReplay && (requestPublicationPending || (nowMs - lastRequestMs) > RESEND_INTERVAL_MS)) {
            requestReplay(walkSegmentIndex, requestFromPosition); // re-send the same request verbatim
        }
        if (completePending) {
            sendReplayComplete();
        }

        if (replaySessionId < 0) {
            return;
        }
        if ((nowMs - lastHeartbeatMs) > RESEND_INTERVAL_MS) {
            sendHeartbeat();
        }
        if ((nowMs - lastReplayProgressMs) > REPLAY_STALL_TIMEOUT_MS) {
            onReplayStalled();
        }
    }

    /**
     * Recovery has run without dispatching a frame for {@link #RECOVERY_PROGRESS_TIMEOUT_MS}. Evaluated
     * every duty cycle.
     * @return whether it reported
     */
    public boolean checkRecoveryProgress() {
        if (caughtUp) {
            return false;
        }
        final long nowMs = actions.nowMs();
        if (noProgressSinceMs == 0) {
            noProgressSinceMs = nowMs; // the first observation only anchors the clock
            return false;
        }
        if (recoveryStallReported || nowMs - noProgressSinceMs < RECOVERY_PROGRESS_TIMEOUT_MS) {
            return false;
        }
        recoveryStallReported = true;
        Logger.fault(Logger.CoreComponent.ReplayerStreamReceiver, Logger.CoreEventCode.RecoveryStalled,
                     actions.memberId(),
                     "recovery has dispatched nothing for >%dms: lastGlobalSeqNo=%d segment=%d awaitingReplay=%b "
                         + "replaySession=%d replayerUnavailable=%b — holding; check this node's Replayer and "
                         + "its recording chain",
                     RECOVERY_PROGRESS_TIMEOUT_MS, lastGlobalSeqNo, walkSegmentIndex, awaitingReplay,
                     replaySessionId, replayerUnavailable);
        actions.recoveryStalled(true);
        return true;
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

    /** A request is out and the Replayer has not answered it yet. */
    public boolean isAwaitingReplay() {
        return awaitingReplay;
    }

    /**
     * Mid-walk (cold start or gap re-walk) or awaiting the Replayer's answer: a non-contiguous live-tap
     * frame is expected while this holds (the tap runs ahead of the replay), so {@link #onFrame} drops it
     * without treating it as a new gap.
     */
    public boolean isRecovering() {
        return replaySessionId >= 0 || awaitingReplay;
    }

    /** The replay currently being ridden, or -1. The receiver attaches its image by this id. */
    public long replaySessionId() {
        return replaySessionId;
    }

    /** The position the current replay is bounded to; the segment is done once the image reaches it. */
    public long catchUpPosition() {
        return catchUpPosition;
    }

    /** Cold-start walk cursor into the recording chain; -1 once caught up (steady/resume mode). */
    public int walkSegmentIndex() {
        return walkSegmentIndex;
    }

    /** The recordingId last served for {@link #walkSegmentIndex()}, or -1 — see the check in onReplaying. */
    public long walkRecordingId() {
        return walkRecordingId;
    }

    /** The id the next reply must carry to be acted on — see the correlation check in {@link #onControl}. */
    public long requestId() {
        return requestId;
    }

    /** The fromPosition of the current request: a gap asks to RESUME rather than re-walk from 0. */
    public long requestFromPosition() {
        return requestFromPosition;
    }

    /** A {@code ReplayComplete} encoded but not yet out on the wire. */
    public boolean completePending() {
        return completePending;
    }

    /** Frames held ahead of the current hole. */
    public int retainedFrameCount() {
        return retainFrameCount;
    }

    /**
     * Sends {@code ReplayRequest} and marks us awaiting the reply. The Replayer supersedes any in-flight
     * replay for this clientId, so a resend is safe. {@code requestId} advances on every send, resends
     * included: only a per-send id tells a stale reply from the live one on the shared control stream.
     */
    private void requestReplay(final int segmentIndex, final long fromPosition) {
        if (segmentIndex >= 0) {
            resumeAnchorGlobalSeqNo = 0; // a walk supersedes any resume in flight
            if (segmentIndex != walkSegmentIndex) {
                walkRecordingId = -1;
            }
        }
        walkSegmentIndex = segmentIndex;
        requestFromPosition = fromPosition;
        awaitingReplay = true;
        replaySessionId = -1;
        completePending = false;
        actions.closeReplay();
        lastRequestMs = actions.nowMs();
        ++requestId;
        actions.sendReplayRequest(requestId, segmentIndex, fromPosition);
    }

    /**
     * Steady-state gap recovery: resume the active recording at the last dispatched frame instead of
     * re-walking the chain, so a one-frame drop costs a one-frame replay. The recording may have rotated,
     * so the resumed replay's first frame is checked against the anchor, falling back to a walk.
     */
    private void requestResume() {
        requestReplay(RESUME_SEGMENT_INDEX, lastFramePosition);
        resumeAnchorGlobalSeqNo = lastGlobalSeqNo;
    }

    /** Re-asks for whatever is in flight; a resume goes back through {@link #requestResume()} for a fresh anchor. */
    private void reRequestCurrent() {
        if (walkSegmentIndex < 0) {
            requestResume();
        } else {
            requestReplay(walkSegmentIndex, requestFromPosition); // same request verbatim, new requestId
        }
    }

    private void onReplaying(final long session, final long replayCatchUpPosition, final long recordingId) {
        awaitingReplay = false;
        replayerUnavailable = false;
        if (session == ReplayProtocol.NO_REPLAY_NEEDED) {
            if (walkSegmentIndex < 0) {
                Logger.log(Logger.CoreComponent.ReplayerStreamReceiver, Logger.Severity.Warn,
                           Logger.CoreEventCode.TapGap,
                           actions.memberId(),
                           "resume at position %d answered 'nothing to replay' while a hole is open above "
                               + "globalSeqNo=%d — the active recording rotated under us; re-walking the "
                               + "recording chain from segment 0",
                           requestFromPosition, lastGlobalSeqNo);
                requestReplay(0, 0);
                return;
            }
            if (recordingId >= 0) {
                requestReplay(walkSegmentIndex + 1, 0);
                return;
            }
            replaySessionId = -1; // already at the tip — follow the live tap
            walkSegmentIndex = -1; // chain exhausted (or never a walk) → steady/resume mode
            if (reachedTip()) {
                notifyCaughtUp();
            }
            return;
        }

        if (walkSegmentIndex >= 0) {
            if (walkRecordingId >= 0 && recordingId != walkRecordingId) {
                Logger.log(Logger.CoreComponent.ReplayerStreamReceiver, Logger.Severity.Warn,
                           Logger.CoreEventCode.TapGap,
                           actions.memberId(),
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
        actions.openReplay(session);
        lastReplayPosition = -1;
        lastReplayProgressMs = actions.nowMs();
    }

    /**
     * The Replayer's archive failed its globalSeqNo-1 integrity check. Not fatal here: an operator repairs
     * the archive and restarts the Replayer, and the resend timer resumes; until then we never catch up.
     */
    private void onReplayUnavailable() {
        if (!replayerUnavailable) {
            replayerUnavailable = true;
            Logger.fault(Logger.CoreComponent.ReplayerStreamReceiver, Logger.CoreEventCode.ReplayUnavailable,
                         actions.memberId(),
                         "this node's Replayer has no valid history to serve (its archive failed the "
                             + "globalSeqNo-1 integrity check) — holding, not dispatching; repair the node's "
                             + "archive and restart its Replayer");
        }
        lastRequestMs = actions.nowMs();
    }

    /**
     * Releases our replay slot at the end of a resume (a walk's last request frees it via NO_REPLAY_NEEDED).
     * Retried via {@link #completePending} until it lands.
     */
    private void sendReplayComplete() {
        completePending = !actions.sendReplayComplete();
    }

    /**
     * Refreshes the replay slot while riding an image, so the Replayer's idle TTL measures an abandoned slot
     * rather than a long replay. A lost heartbeat is recovered by the truncated-close path.
     */
    private void sendHeartbeat() {
        if (actions.sendReplayHeartbeat()) {
            lastHeartbeatMs = actions.nowMs();
        }
    }

    /** An attached (or expected) replay stopped delivering — see the watchdog in {@link #doTimers}. */
    private void onReplayStalled() {
        Logger.log(Logger.CoreComponent.ReplayerStreamReceiver, Logger.Severity.Warn, Logger.CoreEventCode.TapGap,
                   actions.memberId(),
                   "replay session %d made no progress for %dms at position %d of catchUpPosition %d — "
                       + "re-requesting segment %d",
                   replaySessionId, REPLAY_STALL_TIMEOUT_MS, lastReplayPosition, catchUpPosition, walkSegmentIndex);
        reRequestCurrent();
    }

    /** A replay segment finished (reached its bounded tip, or its image closed for a stopped segment). */
    private void onReplaySegmentComplete() {
        actions.closeReplay();
        replaySessionId = -1;
        if (walkSegmentIndex < 0) {
            if (reachedTip()) {
                sendReplayComplete();
                notifyCaughtUp();
            }
            return;
        }
        requestReplay(walkSegmentIndex + 1, 0); // advance the walk to the next segment
    }

    /** Everything past the contiguity check; also the path a drained retained frame takes. */
    private void dispatchFrame(final DirectBuffer buffer, final int offset, final int length,
                               final long globalSeqNo, final long framePosition, final long receiveNs,
                               final boolean fromReplay) {
        noProgressSinceMs = 0;
        if (recoveryStallReported) {
            recoveryStallReported = false;
            actions.recoveryStalled(false);
        }
        view.wrap(buffer, offset, length);

        lastGlobalSeqNo = globalSeqNo;
        replayGapLogged = false;
        lastFramePosition = framePosition;
        if (!fromReplay && !caughtUp && !retainOverflowed) {
            notifyCaughtUp();
        }
        if (view.isSystem() && view.systemEventType() == LEADERSHIP_CHANGED) {
            leadershipChanged.wrap(buffer, view.payloadOffset(), view.blockLength(), view.version());
            currentLeaderMemberId = leadershipChanged.newLeaderMemberId();
            if (onLeadershipChanged != null) {
                onLeadershipChanged.onLeadershipChanged(currentLeaderMemberId, leadershipChanged.leadershipTermId(),
                                                        globalSeqNo);
            }
            return;
        }
        if (onSequenced != null) {
            event.set(globalSeqNo, view.sourceId(), view.connectionId(), view.sessionId(), view.timestamp(),
                      receiveNs, view.isSystem(), view.payloadId(), view.systemEventType(), view.templateId(),
                      view.blockLength(), view.version(), buffer, view.payloadOffset(), view.payloadLength(),
                      framePosition);
            onSequenced.onSequenced(event);
        }
    }

    /**
     * Keeps a live tap frame from beyond the current hole; one not kept is gone. Bounded and lossy past the
     * bound (overflow falls back to a re-walk). A plain FIFO: one image delivers globalSeqNo in order, so
     * only an exact redelivery needs checking.
     */
    private void retainFrame(final long globalSeqNo, final DirectBuffer buffer, final int offset, final int length,
                             final long framePosition, final long receiveNs) {
        final int recordSize = RetainBlock.RECORD_HEADER_LENGTH + length;
        if (retainFrameCount >= MAX_RETAINED_FRAMES || (retainBytes + length) > MAX_RETAINED_BYTES
            || recordSize > RetainBlock.SIZE) {
            retainOverflowed = true;
            if (!retainOverflowLogged) {
                retainOverflowLogged = true;
                Logger.log(Logger.CoreComponent.ReplayerStreamReceiver, Logger.Severity.Warn,
                           Logger.CoreEventCode.TapGap,
                           actions.memberId(),
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

    /** Dispatches every retained frame now contiguous, dropping any the replay covered, and recycles blocks. */
    private void drainRetained() {
        // Exits when the FIFO runs dry, or on a hole below the oldest retained frame.
        RetainBlock front = frontRetained();
        while (front != null && front.buffer.getLong(retainReadOffset) <= lastGlobalSeqNo + 1) {
            final long globalSeqNo = front.buffer.getLong(retainReadOffset);
            final long position = front.buffer.getLong(retainReadOffset + Long.BYTES);
            final long receiveNs = front.buffer.getLong(retainReadOffset + 2 * Long.BYTES);
            final int length = front.buffer.getInt(retainReadOffset + 3 * Long.BYTES);
            final int payloadOffset = retainReadOffset + RetainBlock.RECORD_HEADER_LENGTH;
            retainReadOffset += RetainBlock.RECORD_HEADER_LENGTH + length;
            --retainFrameCount;
            retainBytes -= length;
            if (globalSeqNo > lastGlobalSeqNo) { // otherwise the replay already covered it
                dispatchFrame(front.buffer, payloadOffset, length, globalSeqNo, position, receiveNs, false);
            }
            front = frontRetained();
        }
    }

    /** Block holding the oldest retained record, recycling any fully-read blocks first. Null when none. */
    private RetainBlock frontRetained() {
        RetainBlock front = retained.peekFirst();
        while (front != null && retainReadOffset >= front.used) {
            retained.pollFirst();
            releaseBlock(front);
            retainReadOffset = 0;
            front = retained.peekFirst();
        }
        return front;
    }

    /**
     * The replay side says we are at the tip. Whether we are is the retained FIFO's call: a retained frame
     * may sit behind a hole the replay never reached, or an overflow dropped frames. Either way this re-walks
     * now rather than waiting for a later tap frame to rediscover the hole.
     * @return true only once nothing is left waiting and no overflow is latched
     */
    private boolean reachedTip() {
        drainRetained();
        if (retained.isEmpty() && !retainOverflowed) {
            return true;
        }
        endOverflowEpisode();
        requestReplay(0, 0);
        return false;
    }

    /**
     * Ends an overflow episode where the covering re-walk is requested, not in {@link #drainRetained}, where
     * the dispatch that cleared it would declare us caught up over dropped frames.
     */
    private void endOverflowEpisode() {
        retainOverflowed = false;
        retainOverflowLogged = false;
    }

    private void notifyCaughtUp() {
        if (caughtUp) {
            return; // idempotent — reached from the replay-tip, no-replay, and first-live-frame paths
        }
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

    /** Fixed-size block of the retained FIFO; a record never spans two blocks. */
    private static final class RetainBlock {
        static final int SIZE = 4096;

        /** globalSeqNo, position, receiveNs, length — the per-record framing within a block. */
        static final int RECORD_HEADER_LENGTH = 3 * Long.BYTES + Integer.BYTES;

        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[SIZE]);
        int used;
    }
}
