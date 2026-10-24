package org.limitless.seqeron.replayer.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.LongStream;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.NoOpIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.limitless.seqeron.protocol.ReplayProtocol;
import org.limitless.seqeron.protocol.SeqeronCounters;
import org.limitless.seqeron.protocol.SystemFrame;
import org.limitless.seqeron.replayer.client.ReplayerRecoveryBridge;
import org.limitless.seqeron.sbe.frame.ClusterHeartbeatEncoder;
import org.limitless.seqeron.sbe.replay.MessageHeaderEncoder;
import org.limitless.seqeron.sbe.replay.ReplayRequestEncoder;
import org.limitless.seqeron.util.Logger;

/**
 * The replay protocol from both ends, with no Aeron runtime: a client's {@code ReplayerRecovery} against {@link
 * ReplayerService} on a {@link FakeReplayer}. Every answer the client acts on is the service's, so what {@code
 * ReplayerRecoveryPropertyTest}'s model assumes of the Replayer is held to the Replayer here. Requests and replies wait
 * in the harness until a case releases them, so a resend can cross the reply to the request it replaces.
 *
 * <p>Frame {@code g} of the queue starts at {@code (g − 1) × FRAME_LENGTH} in every recording that holds it, and a
 * replay delivers the frames of the range the service started it over.
 */
class ReplaySeamTest {
    private static final long CLIENT = 7;
    private static final int GROUP_ID = FakeReplayer.GROUP_ID;
    private static final long RECORDING = 6;
    private static final int FRAME_LENGTH = 1024;
    private static final int FRAME_BYTES =
        org.limitless.seqeron.sbe.frame.MessageHeaderEncoder.ENCODED_LENGTH + ClusterHeartbeatEncoder.BLOCK_LENGTH;

    /** Past the client's resend interval, 500 ms. */
    private static final long PAST_RESEND_MS = 600;

    private static final long NO_SESSION = -1;

    private final List<Logger.LoggerEvent> events = new ArrayList<>();
    private final MutableDirectBuffer encodeBuffer = new ExpandableArrayBuffer(128);
    private final List<Long> dispatched = new ArrayList<>();
    private final List<Long> opened = new ArrayList<>();
    private final Deque<byte[]> requests = new ArrayDeque<>();
    private int repliesForwarded;
    private long openSession = NO_SESSION;
    private long clockMs = 1_000;

    private FakeReplayer fakeReplayer;
    private ReplayerService replayerService;
    private Transport client;

    @BeforeEach
    void setUp() {
        Logger.install(events::add);
        fakeReplayer = new FakeReplayer();
        replayerService = new ReplayerService(
            fakeReplayer, 0, NoOpIdleStrategy.INSTANCE,
            new ConcurrentLinkedQueue<>(new TopologyLoad().groups(GROUP_ID, "group-1").rows()),
            QueueResolver.EVERY_GROUP, () -> { });
        client = new Transport();
    }

    @AfterEach
    void tearDown() {
        client.close();
        Logger.reset();
    }

    @Test
    void aColdStartWalksTheRecordingItIsServedThenFollowsTheQueue() {
        caughtUpAt(5);
        assertEquals(0, activeSlots(), "the walk's slot is freed once the client lets it go");

        recordTo(6);
        deliverLive(6);

        assertEquals(range(1, 6), dispatched);
        assertTrue(client.isCaughtUp());
    }

    @Test
    void aColdStartOnAnEmptyRecordingFollowsTheQueueFromItsFirstFrame() {
        record(1, 0);

        client.start();
        exchange();

        assertTrue(client.isCaughtUp(), "R-2: from the start, nothing to replay means the client is at the tip");
        assertTrue(fakeReplayer.startedReplays().isEmpty());
        recordTo(1);
        deliverLive(1);
        assertEquals(List.of(1L), dispatched);
        assertNull(client.fault());
    }

    @Test
    void aLiveGapIsHealedByAResumeOfTheRecording() {
        caughtUpAt(3);
        recordTo(5);

        deliverLive(5); // 4 lost on the live queue
        exchange();

        assertEquals(position(3), lastStartedReplay().position(), "resumed at the last frame dispatched");
        deliverReplay();
        assertEquals(range(1, 5), dispatched);
        assertTrue(client.isCaughtUp());
        assertNull(client.fault());
    }

    @Test
    void aResumeTheRecordingHasNotReachedIsHeldUntilItHas() {
        caughtUpAt(3);
        deliverLive(4); // the queue runs ahead of its recording, which holds up to 3
        deliverLive(6); // 5 lost

        exchange();

        assertNull(client.fault(), "R-2: a position the recording has not reached is held, not refused");
        assertTrue(client.isAwaitingReplay());
        assertEquals(range(1, 4), dispatched);

        recordTo(6);
        advance(PAST_RESEND_MS);
        exchange();
        deliverReplay();

        assertEquals(range(1, 6), dispatched);
        assertTrue(client.isCaughtUp());
    }

    @Test
    void aReplyCrossedByAResendIsIgnoredAndOnlyTheResendIsServed() {
        caughtUpAt(3);
        recordTo(5);
        deliverLive(5); // 4 lost

        serve(); // the Replayer answers the resume; the answer is still on its way
        final long first = lastStartedReplay().replaySessionId();
        advance(PAST_RESEND_MS); // the client, having heard nothing, resends
        serve();
        final long second = lastStartedReplay().replaySessionId();
        deliverReplies(); // both answers, in the order they were sent

        assertFalse(opened.contains(first), "R-1: the answer to a superseded request is not acted on");
        assertEquals(second, client.replaySessionId());
        assertTrue(fakeReplayer.stoppedReplays().contains(first), "the Replayer stopped the superseded replay");
        assertEquals(1, activeSlots());
        deliverReplay();
        assertEquals(range(1, 5), dispatched);
        assertEquals(0, activeSlots());
    }

    @Test
    void aClientHeldForASlotIsServedOnItsCurrentRequest() {
        caughtUpAt(3);
        for (long other = 101; other <= 104; other++) {
            requests.add(replayRequest(other, 1, ReplayProtocol.FROM_START));
        }
        serve();
        assertEquals(ReplayerService.MAX_CONCURRENT_REPLAYS, activeSlots());
        final long otherSession = fakeReplayer.startedReplays().get(1).replaySessionId();
        recordTo(5);
        deliverLive(5); // 4 lost

        exchange();
        assertTrue(client.isAwaitingReplay(), "held: every slot is taken");
        advance(PAST_RESEND_MS);
        exchange(); // the resend takes the held request's place in line, and is held again
        assertTrue(client.isAwaitingReplay());

        fakeReplayer.endReplay(otherSession);
        advance(PAST_RESEND_MS); // the next resend leaves before the Replayer frees the slot
        replayerService.poll();  // frees it and serves the request it holds, which the client has since replaced
        final long drained = lastStartedReplay().replaySessionId();
        deliverReplies();

        assertFalse(opened.contains(drained), "R-1: the answer to a replaced request is not acted on");
        assertTrue(client.isAwaitingReplay());

        exchange(); // the current request supersedes the drained replay
        assertTrue(fakeReplayer.stoppedReplays().contains(drained));
        deliverReplay();
        assertEquals(range(1, 5), dispatched);
        assertEquals(ReplayerService.MAX_CONCURRENT_REPLAYS - 1, activeSlots(), "the client left no slot behind");
    }

    @Test
    void aResumeBelowTheRecordingsStartIsRefusedAndStopsTheClient() {
        caughtUpAt(3);
        // The recording now starts past the client's position, as a restart at a cut leaves it.
        fakeReplayer.stopRecording(RECORDING);
        fakeReplayer.addRecording(RECORDING + 1, position(8), true, end(10));

        deliverLive(5); // 4 lost
        exchange();

        assertNotNull(client.fault(), "R-2: NO_REPLAY_NEEDED for a resume is fatal");
        assertTrue(client.fault().contains("refused a resume"), client.fault());
        assertEquals(range(1, 3), dispatched);
    }

    @Test
    void aWalkOfARecordingThatStartsAtACutStopsTheClient() {
        record(10, 12);

        client.start();
        exchange();
        deliverReplay();

        assertNotNull(client.fault());
        assertTrue(client.fault().contains("expected 1"), client.fault());
        assertTrue(dispatched.isEmpty());
    }

    // ── The node ────────────────────────────────────────────────────────────────

    /** Makes the active recording start at frame {@code first} and hold frames up to {@code last}, and the node ready. */
    private void record(final long first, final long last) {
        fakeReplayer.addRecording(RECORDING, position(first), true, end(last));
        for (int cycle = 0; cycle < 20 && fakeReplayer.counter(SeqeronCounters.REPLAYER_READY_TYPE_ID) == 0; ++cycle) {
            replayerService.poll();
        }
        assertEquals(1, fakeReplayer.counter(SeqeronCounters.REPLAYER_READY_TYPE_ID), "the node is not ready");
    }

    /** The active recording holds frames up to {@code last}. */
    private void recordTo(final long last) {
        fakeReplayer.recordTo(RECORDING, end(last));
    }

    private long activeSlots() {
        return fakeReplayer.counter(SeqeronCounters.REPLAYER_ACTIVE_SLOTS_TYPE_ID);
    }

    private FakeReplayer.StartedReplay lastStartedReplay() {
        return fakeReplayer.startedReplays().getLast();
    }

    // ── The client ──────────────────────────────────────────────────────────────

    /** A client that walked frames 1 to {@code last} and follows the queue live. */
    private void caughtUpAt(final long last) {
        record(1, last);
        client.start();
        exchange();
        deliverReplay();
        assertEquals(range(1, last), dispatched);
        assertTrue(client.isCaughtUp());
    }

    /** Hands the Replayer the requests sent so far and runs one of its duty cycles; its replies wait. */
    private void serve() {
        while (!requests.isEmpty()) {
            fakeReplayer.offerRequest(requests.poll());
        }
        replayerService.poll();
    }

    /** Hands the client every reply sent since the last call, other clients' included, as the shared stream does. */
    private void deliverReplies() {
        final List<byte[]> replies = fakeReplayer.controlReplies();
        while (repliesForwarded < replies.size()) {
            final byte[] reply = replies.get(repliesForwarded++);
            client.onControl(new UnsafeBuffer(reply), 0, reply.length);
        }
    }

    private void exchange() {
        serve();
        deliverReplies();
    }

    /**
     * Delivers the replay the client is attached to over the range the Replayer started it on, then its bound, which a
     * replay of an active recording reaches without closing; then lets the Replayer reclaim what the client let go of.
     */
    private void deliverReplay() {
        final long session = client.replaySessionId();
        assertEquals(session, openSession, "the client subscribes to the replay it was told of");
        final FakeReplayer.StartedReplay replay = fakeReplayer.startedReplays().stream()
            .filter(started -> started.replaySessionId() == session)
            .findFirst()
            .orElseThrow();
        final long bound = replay.position() + replay.length();
        for (long groupSeqNo = replay.position() / FRAME_LENGTH + 1; position(groupSeqNo) < bound; groupSeqNo++) {
            client.onFrame(frame(groupSeqNo), 0, FRAME_BYTES, position(groupSeqNo), 0, true);
        }
        client.onReplayPosition(bound);
        replayerService.poll();
    }

    private void deliverLive(final long groupSeqNo) {
        client.onFrame(frame(groupSeqNo), 0, FRAME_BYTES, position(groupSeqNo), 0, false);
    }

    /** Moves both clocks on and runs the client's timers, as its duty cycle does. */
    private void advance(final long millis) {
        clockMs += millis;
        fakeReplayer.advanceMillis(millis);
        client.doTimers(false);
    }

    /** The client, and its transport: requests wait in {@link #requests}, and letting a replay go ends it. */
    private final class Transport extends ReplayerRecoveryBridge {
        Transport() {
            super(event -> dispatched.add(event.groupSeqNo()));
        }

        @Override
        public void sendReplayRequest(final long requestId, final long fromPosition, final long cutClusterSeqNo) {
            requests.add(replayRequest(CLIENT, requestId, fromPosition));
        }

        @Override
        public void openReplay(final long replaySessionId) {
            openSession = replaySessionId;
            opened.add(replaySessionId);
        }

        @Override
        public void closeReplay() {
            if (openSession != NO_SESSION) {
                fakeReplayer.endReplay(openSession);
                openSession = NO_SESSION;
            }
        }

        @Override
        public void recoveryStalled(final boolean stalled) {
        }

        @Override
        public Integer memberId() {
            return 0;
        }

        @Override
        public long clientId() {
            return CLIENT;
        }

        @Override
        public long nowMs() {
            return clockMs;
        }
    }

    // ── Frames and encoders ─────────────────────────────────────────────────────

    private static long position(final long groupSeqNo) {
        return (groupSeqNo - 1) * FRAME_LENGTH;
    }

    private static long end(final long groupSeqNo) {
        return groupSeqNo * FRAME_LENGTH;
    }

    private static List<Long> range(final long first, final long last) {
        return LongStream.rangeClosed(first, last).boxed().toList();
    }

    /** Frame {@code groupSeqNo} of a queue other groups' frames fall between, so its clusterSeqNo jumps. */
    private static UnsafeBuffer frame(final long groupSeqNo) {
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[FRAME_BYTES]);
        new ClusterHeartbeatEncoder()
            .wrapAndApplyHeader(buffer, 0, new org.limitless.seqeron.sbe.frame.MessageHeaderEncoder())
            .header()
            .sourceId(-1)
            .connectionId(-1)
            .sessionId(-1)
            .systemEventType(SystemFrame.CLUSTER_HEARTBEAT)
            .clusterSeqNo(2 * groupSeqNo)
            .timestamp(groupSeqNo * 1000)
            .groupSeqNo(groupSeqNo);
        return buffer;
    }

    private byte[] replayRequest(final long clientId, final long requestId, final long fromPosition) {
        final ReplayRequestEncoder encoder = new ReplayRequestEncoder();
        encoder.wrapAndApplyHeader(encodeBuffer, 0, new MessageHeaderEncoder())
            .clientId(clientId)
            .requestId(requestId)
            .fromPosition(fromPosition)
            .queueId(GROUP_ID)
            .cutClusterSeqNo(ReplayProtocol.NO_CUT);
        final byte[] message = new byte[MessageHeaderEncoder.ENCODED_LENGTH + encoder.encodedLength()];
        encodeBuffer.getBytes(0, message);
        return message;
    }
}
