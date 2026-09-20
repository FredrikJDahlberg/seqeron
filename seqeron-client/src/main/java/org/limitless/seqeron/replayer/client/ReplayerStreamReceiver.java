package org.limitless.seqeron.replayer.client;

import io.aeron.Aeron;
import io.aeron.Counter;
import io.aeron.FragmentAssembler;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.LogBufferDescriptor;
import org.agrona.concurrent.SystemEpochNanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.limitless.seqeron.protocol.FrameLayer;
import org.limitless.seqeron.protocol.ReplayProtocol;
import org.limitless.seqeron.protocol.SeqeronCounters;
import org.limitless.seqeron.sbe.replay.MessageHeaderEncoder;
import org.limitless.seqeron.sbe.replay.ReplayCompleteEncoder;
import org.limitless.seqeron.sbe.replay.ReplayHeartbeatEncoder;
import org.limitless.seqeron.sbe.replay.ReplayRequestEncoder;

/**
 * App-replica side of the per-node {@code ReplayerService} — the Java twin of
 * {@code replayer/client/ReplayerStreamReceiver.hpp}. Reads the co-located tap live and asks the Replayer
 * for history and gaps. The Aeron adapter only: every decision lives in {@link ReplayerRecovery}.
 *
 * <p>Single-threaded: every method runs on the one duty-cycle thread.
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
        void onLeadershipChanged(int newLeaderMemberId, long leadershipTermId, long globalSeqNo);
    }

    /** Fires on every transition to caught-up, including re-convergence after a gap. */
    @FunctionalInterface
    public interface CaughtUpHandler {
        void onCaughtUp();
    }

    /** The tap as a consumer addresses it: untethered, so a slow app is dropped and heals via replay. */
    public static final String FEEDER_CONSUMER_CHANNEL = FrameLayer.FEEDER_CHANNEL + "?tether=false";

    /**
     * Untethered like the tap: the Replayer answers every app from one thread, so an app that stops polling
     * must not back-pressure the others' replies. A dropped reply costs one resend interval.
     */
    public static final String CONTROL_CHANNEL = ReplayProtocol.IPC_CHANNEL + "?tether=false";

    private static final int FRAGMENT_LIMIT = 16;

    /** Scratch for the three control messages this client sends; each is well under 64 bytes. */
    private static final int REQUEST_BUFFER_LENGTH = 64;

    private final int clientId;
    private final Actions actions = new Actions();
    private final ReplayerRecovery recovery;
    private final TapFaultInjector tapFaults;

    private final MessageHeaderEncoder requestHeader = new MessageHeaderEncoder();
    private final ReplayRequestEncoder replayRequest = new ReplayRequestEncoder();
    private final ReplayCompleteEncoder replayComplete = new ReplayCompleteEncoder();
    private final ReplayHeartbeatEncoder replayHeartbeat = new ReplayHeartbeatEncoder();
    private final UnsafeBuffer requestBuffer = new UnsafeBuffer(new byte[REQUEST_BUFFER_LENGTH]);

    private final FragmentHandler tapHandler;
    private final FragmentHandler replayHandler;
    private final FragmentHandler controlFragmentHandler;

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
    public ReplayerStreamReceiver(final int clientId, final SequencedHandler onSequenced,
                                  final LeadershipHandler onLeadershipChanged, final CaughtUpHandler onCaughtUp) {
        this(clientId, onSequenced, onLeadershipChanged, onCaughtUp, null);
    }

    /**
     * As above, dropping live tap frames when {@code tapFaults} is armed. Test harnesses only.
     * @param tapFaults drops live tap frames on demand, or null
     */
    public ReplayerStreamReceiver(final int clientId, final SequencedHandler onSequenced,
                                  final LeadershipHandler onLeadershipChanged, final CaughtUpHandler onCaughtUp,
                                  final TapFaultInjector tapFaults) {
        this.clientId = clientId;
        this.tapFaults = tapFaults;
        this.recovery = new ReplayerRecovery(clientId, actions, onSequenced, onLeadershipChanged, onCaughtUp);
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
     * @param memberId this app's node, to label the counter
     */
    public void start(final Aeron aeron, final int memberId) {
        this.aeron = aeron;
        this.memberId = memberId;
        recoveryStalledCounter = SeqeronCounters.addAppCounter(
            aeron, SeqeronCounters.APP_RECOVERY_STALLED_TYPE_ID,
            "seqeron.app.recoveryStalled member=" + memberId + " client=" + clientId, memberId, clientId);
        tapSubscription = aeron.addSubscription(FEEDER_CONSUMER_CHANNEL, FrameLayer.FEEDER_STREAM_ID);
        controlSubscription = aeron.addSubscription(CONTROL_CHANNEL, ReplayProtocol.CONTROL_STREAM_ID);
        requestPublication = aeron.addPublication(ReplayProtocol.IPC_CHANNEL, ReplayProtocol.REQUEST_STREAM_ID);
        recovery.start();
    }

    /**
     * One duty-cycle iteration: drain control, ride an attached replay image, and always drain and dispatch
     * the tap — {@link ReplayerRecovery}'s contiguity check, not the poll routing, decides what a tap frame is
     * worth mid-walk.
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
        actions.closeReplay();
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
     * Stream position of the first byte of the frame {@code header} describes. Not {@code header.position()
     * - frameLength}: {@code position()} is the next frame's, aligned to 32 bytes, and a replay position must
     * sit on a frame boundary.
     */
    private void onTapFragment(final org.agrona.DirectBuffer buffer, final int offset, final int length,
                               final io.aeron.logbuffer.Header header) {
        if (tapFaults != null && tapFaults.dropNext()) {
            return;
        }
        recovery.onFrame(buffer, offset, length, frameStartPosition(header), nowNs(), false);
    }

    private static long frameStartPosition(final io.aeron.logbuffer.Header header) {
        return LogBufferDescriptor.computePosition(header.termId(), header.termOffset(),
                                                   header.positionBitsToShift(), header.initialTermId());
    }

    /**
     * The wall-clock stamp every frame is delivered with, in epoch nanoseconds like the consensus timestamp
     * it is measured against; millisecond resolution would be coarser than the latency measured.
     */
    static long nowNs() {
        return SystemEpochNanoClock.INSTANCE.nanoTime();
    }

    /** The recovery state machine's transport. Private, so a consumer cannot drive the replay protocol. */
    private final class Actions implements ReplayerRecoveryActions {
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
         * Subscribes to exactly one replay, this one, and to nothing on the replay stream otherwise. A standing
         * subscription would make every idle app a tethered, never-polled subscriber of every other app's
         * replay on the shared stream, and wedge the archive's replay one window in.
         */
        @Override
        public void openReplay(final long replaySessionId) {
            closeReplay();
            if (aeron == null) {
                return;
            }

            final String channel = ReplayProtocol.IPC_CHANNEL + "?session-id=" + (int)replaySessionId;
            replaySubscription = aeron.addSubscription(channel, ReplayProtocol.REPLAY_STREAM_ID);
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
    }
}
