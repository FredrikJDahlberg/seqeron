package org.limitless.seqeron.replayer.client;

import io.aeron.Aeron;
import io.aeron.Counter;
import io.aeron.FragmentAssembler;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.LogBufferDescriptor;
import java.util.concurrent.atomic.AtomicInteger;
import org.agrona.concurrent.SystemEpochNanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.limitless.seqeron.metrics.SeqeronCounters;
import org.limitless.seqeron.replayer.server.ReplayerService;
import org.limitless.seqeron.sbe.replay.MessageHeaderEncoder;
import org.limitless.seqeron.sbe.replay.ReplayCompleteEncoder;
import org.limitless.seqeron.sbe.replay.ReplayHeartbeatEncoder;
import org.limitless.seqeron.sbe.replay.ReplayRequestEncoder;
import org.limitless.seqeron.sequencer.SequencerService;

/**
 * App-replica side of the per-node {@link ReplayerService} — the Java twin of the C++
 * {@code replayer/client/ReplayerStreamReceiver.hpp}.
 *
 * <p>It reads the co-located {@code SequencerService} IPC tap ({@link SequencerService#FEEDER_STREAM_ID})
 * LIVE and only touches the archive indirectly — by asking the node-local Replayer to replay when it
 * detects a gap. That is the whole point of the Replayer: one process per node reads the archive, every
 * replica reads the cheap local tap for live and asks the Replayer for history and gaps.
 *
 * <p>This is the Aeron adapter only: subscriptions, the request publication, the replay image and the
 * clocks. Every decision it makes about them lives in {@link ReplayerRecovery}, which holds none of them.
 *
 * <p><b>Single-threaded.</b> Every method must be called from the one duty-cycle thread.
 */
public final class ReplayerStreamReceiver implements AutoCloseable, ReplayerRecoveryActions {
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

    /** Scratch for the three control messages this client sends; each is well under 64 bytes. */
    private static final int REQUEST_BUFFER_LENGTH = 64;

    private final int clientId;
    private final ReplayerRecovery recovery;

    private final MessageHeaderEncoder requestHeader = new MessageHeaderEncoder();
    private final ReplayRequestEncoder replayRequest = new ReplayRequestEncoder();
    private final ReplayCompleteEncoder replayComplete = new ReplayCompleteEncoder();
    private final ReplayHeartbeatEncoder replayHeartbeat = new ReplayHeartbeatEncoder();
    private final UnsafeBuffer requestBuffer = new UnsafeBuffer(new byte[REQUEST_BUFFER_LENGTH]);

    private final FragmentHandler tapHandler;
    private final FragmentHandler replayHandler;
    private final FragmentHandler controlFragmentHandler;

    private final AtomicInteger faultDropPending = new AtomicInteger();
    private boolean faultInjection;

    private Aeron aeron;
    private Integer memberId;
    private Subscription tapSubscription;
    private Subscription replaySubscription;
    private Subscription controlSubscription;
    private Publication requestPublication;
    private Image replayImage;
    private Counter recoveryStalledCounter;

    /**
     * @param clientId            this replica's stable id, unique among the Replayer's co-located apps
     *                            ({@code SEQERON_REPLAYER_CLIENT_ID}); two apps sharing one supersede
     *                            each other's replays and neither ever catches up
     * @param onSequenced         receives every in-order frame
     * @param onLeadershipChanged receives each leadership change, or null
     * @param onCaughtUp          fires on every transition to caught-up, or null
     */
    public ReplayerStreamReceiver(final int clientId, final ReplayerRecovery.SequencedHandler onSequenced,
                                  final ReplayerRecovery.LeadershipHandler onLeadershipChanged,
                                  final ReplayerRecovery.CaughtUpHandler onCaughtUp) {
        this.clientId = clientId;
        this.recovery = new ReplayerRecovery(clientId, this, onSequenced, onLeadershipChanged, onCaughtUp);
        this.tapHandler = new FragmentAssembler(this::onTapFragment);
        this.replayHandler = new FragmentAssembler(
            (buffer, offset, length, hdr) ->
                recovery.onFrame(buffer, offset, length, frameStartPosition(hdr), nowNs(), true));
        this.controlFragmentHandler =
            new FragmentAssembler((buffer, offset, length, hdr) -> recovery.onControl(buffer, offset, length));
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
        recoveryStalledCounter = SeqeronCounters.addAppCounter(
            aeron, SeqeronCounters.APP_RECOVERY_STALLED_TYPE_ID,
            "seqeron.app.recoveryStalled member=" + memberId + " client=" + clientId, memberId, clientId);
        tapSubscription = aeron.addSubscription(FEEDER_CONSUMER_CHANNEL, SequencerService.FEEDER_STREAM_ID);
        controlSubscription = aeron.addSubscription(CONTROL_CHANNEL, ReplayerService.CONTROL_STREAM_ID);
        requestPublication = aeron.addPublication(ReplayerService.IPC_CHANNEL, ReplayerService.REQUEST_STREAM_ID);
        recovery.start();
    }

    /**
     * Test-only (see {@code ClusterProbe}'s {@code SEQERON_PROBE_FAULT_INJECTION} hook): enable dropping
     * live tap frames on demand, to synthesize a consumer-side globalSeqNo gap so a test can drive gap
     * recovery deterministically ({@code cluster/src/test/scripts/gap-recovery-test.sh}). A no-op in
     * production (never enabled). The C++ twin is {@code enableFaultInjection} in
     * {@code replayer/client/ReplayerStreamReceiver.hpp}.
     */
    public void enableFaultInjection() {
        faultInjection = true;
    }

    /**
     * Arms a drop of the next {@code n} live tap frames. Called on the poll thread (deferred from a signal
     * handler); a no-op unless fault injection was enabled.
     * @param n frames to drop
     */
    public void injectTapDrop(final int n) {
        if (faultInjection) {
            faultDropPending.addAndGet(n);
        }
    }

    /**
     * One duty-cycle iteration. Poll ordering: always drain control (to learn Replaying/ReplayPending),
     * ride an attached replay image, and always drain AND dispatch the tap — the contiguity check in
     * {@link ReplayerRecovery}, not the poll routing, decides what a tap frame is worth mid-walk, which is
     * what lets the tap itself close the replay-to-live seam.
     * @return fragments consumed
     */
    public int poll() {
        int work = 0;
        if (controlSubscription != null) {
            work += controlSubscription.poll(controlFragmentHandler, FRAGMENT_LIMIT);
        }

        final boolean requestPubPending = requestPublication != null && !requestPublication.isConnected();
        recovery.doTimers(requestPubPending);

        if (recovery.replaySessionId() >= 0) {
            if (replayImage == null && replaySubscription != null) {
                replayImage = replaySubscription.imageBySessionId((int)recovery.replaySessionId());
            }
            if (replayImage != null) {
                final Image image = replayImage;
                if (!image.isClosed()) {
                    work += image.poll(replayHandler, FRAGMENT_LIMIT);
                    if (replayImage == image) {
                        recovery.onReplayPosition(image.position());
                    }
                } else {
                    recovery.onReplayImageClosed(image.position());
                }
            }
        }
        if (tapSubscription != null) {
            work += tapSubscription.poll(tapHandler, FRAGMENT_LIMIT);
        }

        recovery.checkRecoveryProgress();
        return work;
    }

    /** Whether this client is following the live tail; revoked on a tap gap, re-established at the seam. */
    public boolean isCaughtUp() {
        return recovery.isCaughtUp();
    }

    /**
     * Highest globalSeqNo dispatched in order, 0 before the first. The frontier a consumer measures its own
     * recovery progress by.
     */
    public long lastGlobalSeqNo() {
        return recovery.lastGlobalSeqNo();
    }

    /**
     * memberId of the current leader per the last {@code LeadershipChanged} processed, or -1 until one is
     * seen. A replica emits iff its own node is this leader.
     */
    public int currentLeaderMemberId() {
        return recovery.currentLeaderMemberId();
    }

    @Override
    public void close() {
        closeReplay();
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

    @Override
    public void sendReplayRequest(final long requestId, final int segmentIndex, final long fromPosition) {
        if (requestPublication == null || !requestPublication.isConnected()) {
            return; // Replayer not up yet; the resend timer retries
        }
        replayRequest.wrapAndApplyHeader(requestBuffer, 0, requestHeader)
                     .clientId(clientId)
                     .requestId(requestId)
                     .fromPosition(fromPosition)
                     .segmentIndex(segmentIndex);
        requestPublication.offer(requestBuffer, 0,
                                 MessageHeaderEncoder.ENCODED_LENGTH + replayRequest.encodedLength());
    }

    @Override
    public boolean sendReplayComplete() {
        if (requestPublication == null || !requestPublication.isConnected()) {
            return false;
        }
        replayComplete.wrapAndApplyHeader(requestBuffer, 0, requestHeader).clientId(clientId);
        return requestPublication.offer(requestBuffer, 0,
                                        MessageHeaderEncoder.ENCODED_LENGTH + replayComplete.encodedLength()) >= 0;
    }

    @Override
    public boolean sendReplayHeartbeat() {
        if (requestPublication == null || !requestPublication.isConnected()) {
            return false;
        }
        replayHeartbeat.wrapAndApplyHeader(requestBuffer, 0, requestHeader).clientId(clientId);
        return requestPublication.offer(requestBuffer, 0,
                                        MessageHeaderEncoder.ENCODED_LENGTH + replayHeartbeat.encodedLength()) >= 0;
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
    @Override
    public void openReplay(final long replaySessionId) {
        closeReplay();
        if (aeron == null) {
            return;
        }

        final String channel = ReplayerService.IPC_CHANNEL + "?session-id=" + (int)replaySessionId;
        replaySubscription = aeron.addSubscription(channel, ReplayerService.REPLAY_STREAM_ID);
    }

    @Override
    public void closeReplay() {
        replayImage = null;
        if (replaySubscription != null) {
            replaySubscription.close();
            replaySubscription = null;
        }
    }

    @Override
    public void recoveryStalled(final boolean stalled) {
        if (recoveryStalledCounter != null) {
            recoveryStalledCounter.set(stalled ? 1 : 0);
        }
    }

    @Override
    public Integer memberId() {
        return memberId;
    }

    @Override
    public long nowMs() {
        return System.currentTimeMillis();
    }

    /**
     * Stream position of the first byte of the frame {@code header} describes. Deliberately not
     * {@code header.position() - frameLength}: {@code position()} is the NEXT frame's position (this
     * frame's end rounded up to the 32-byte frame alignment), so subtracting an unaligned SBE length lands
     * short of the true start and is itself unaligned — and a replay position must sit on a frame boundary.
     * Under a FragmentAssembler it is further off. Term offsets are always frame-aligned, so this form is
     * exact in both cases.
     */
    private void onTapFragment(final org.agrona.DirectBuffer buffer, final int offset, final int length,
                               final io.aeron.logbuffer.Header header) {
        if (faultInjection && faultDropPending.get() > 0) {
            faultDropPending.decrementAndGet();
            return;
        }
        recovery.onFrame(buffer, offset, length, frameStartPosition(header), nowNs(), false);
    }

    private static long frameStartPosition(final io.aeron.logbuffer.Header header) {
        return LogBufferDescriptor.computePosition(header.termId(), header.termOffset(),
                                                   header.positionBitsToShift(), header.initialTermId());
    }

    /**
     * The wall-clock stamp every frame is delivered with, on the same epoch as the cluster consensus
     * timestamp a consumer measures it against. {@code currentTimeMillis() * 1_000_000} quantised every
     * sample to a whole millisecond — coarser than the latency it is there to measure — so this is
     * {@code Instant.now()} in nanoseconds, the C++ twin's {@code system_clock::now()}.
     */
    static long nowNs() {
        return SystemEpochNanoClock.INSTANCE.nanoTime();
    }
}
