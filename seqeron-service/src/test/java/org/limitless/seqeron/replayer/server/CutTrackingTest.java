package org.limitless.seqeron.replayer.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.NoOpIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.limitless.seqeron.protocol.SeqeronCounters;
import org.limitless.seqeron.sbe.replay.MessageHeaderDecoder;
import org.limitless.seqeron.sbe.replay.MessageHeaderEncoder;
import org.limitless.seqeron.sbe.replay.ReplayRequestEncoder;
import org.limitless.seqeron.sbe.replay.SnapshotConfirmedEncoder;
import org.limitless.seqeron.sbe.replay.SnapshotJoinedEncoder;
import org.limitless.seqeron.sbe.replay.SnapshotRoundDecoder;
import org.limitless.seqeron.util.Logger;

/**
 * A member's Replayer tracking its cluster snapshots (doc/snapshot.md §10.8): it tells the applications of each round,
 * hears which join and which confirm, and writes the cut to the member's log once it may be restarted from.
 */
class CutTrackingTest {
    private static final int GROUP_ID = FakeReplayer.GROUP_ID;
    private static final long JOIN_WINDOW_MS = 1_000;
    private static final long LOG_POSITION = 8_192;

    @TempDir
    Path clusterDir;

    private final Queue<CutHandover> cuts = new ConcurrentLinkedQueue<>();
    private final MutableDirectBuffer encodeBuffer = new ExpandableArrayBuffer(64);
    private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
    private final List<Logger.LoggerEvent> events = new ArrayList<>();
    private FakeReplayer fakeReplayer;
    private ReplayerService replayerService;
    private RestartSafeLog log;

    @BeforeEach
    void setUp() {
        Logger.install(events::add);
        fakeReplayer = new FakeReplayer();
        log = RestartSafeLog.open(clusterDir);
        final Queue<byte[]> topologyRows = new ConcurrentLinkedQueue<>(new TopologyLoad().groups(GROUP_ID, "group-1").rows());
        replayerService = new ReplayerService(fakeReplayer, 0, NoOpIdleStrategy.INSTANCE, topologyRows,
                                              QueueResolver.EVERY_GROUP, () -> { },
                                              new CutIntake(cuts, log, JOIN_WINDOW_MS));
        fakeReplayer.addRecording(6, 4096, true, 8192);
        fakeReplayer.addFrames(6, 4096, 40, 5120, 45, 6144, 50, 7168, 55);
        for (int cycle = 0; cycle < 20 && fakeReplayer.counter(SeqeronCounters.REPLAYER_READY_TYPE_ID) == 0; cycle++) {
            replayerService.poll();
        }
    }

    @AfterEach
    void tearDown() {
        log.close();
        Logger.reset();
    }

    private void handOver(final long round) {
        final long[] positions = new long[16];
        java.util.Arrays.fill(positions, -1);
        positions[GROUP_ID - 1] = 6144;
        cuts.add(new CutHandover(round, 50, LOG_POSITION, positions));
        replayerService.poll();
    }

    private void join(final long round, final int sourceId) {
        join(round, sourceId, 7);
    }

    private void join(final long round, final int sourceId, final long clientId) {
        final SnapshotJoinedEncoder encoder = new SnapshotJoinedEncoder();
        encoder.wrapAndApplyHeader(encodeBuffer, 0, headerEncoder).clientId(clientId).sourceId(sourceId).round(round);
        send(encoder.encodedLength());
    }

    private void confirm(final long round, final int sourceId) {
        confirm(round, sourceId, 7);
    }

    private void confirm(final long round, final int sourceId, final long clientId) {
        final SnapshotConfirmedEncoder encoder = new SnapshotConfirmedEncoder();
        encoder.wrapAndApplyHeader(encodeBuffer, 0, headerEncoder).clientId(clientId).sourceId(sourceId).round(round);
        send(encoder.encodedLength());
    }

    private void send(final int bodyLength) {
        final byte[] message = new byte[MessageHeaderEncoder.ENCODED_LENGTH + bodyLength];
        encodeBuffer.getBytes(0, message);
        fakeReplayer.offerRequest(message);
        replayerService.poll();
    }

    @Test
    @DisplayName("a cut is told to every application as a SnapshotRound, and each queue keeps where its marker was")
    void aCutIsBroadcastAndKept() {
        handOver(3);

        final byte[] reply = fakeReplayer.controlReplies().getLast();
        final MessageHeaderDecoder header = new MessageHeaderDecoder().wrap(new UnsafeBuffer(reply), 0);
        assertEquals(SnapshotRoundDecoder.TEMPLATE_ID, header.templateId());
        final SnapshotRoundDecoder round = new SnapshotRoundDecoder().wrap(
            new UnsafeBuffer(reply), MessageHeaderDecoder.ENCODED_LENGTH, header.blockLength(), header.version());
        assertEquals(3, round.round());
        assertEquals(50, round.clusterSeqNo());

        final ReplayRequestEncoder request = new ReplayRequestEncoder();
        request.wrapAndApplyHeader(encodeBuffer, 0, headerEncoder)
            .clientId(7).requestId(1).fromPosition(0).queueId(GROUP_ID).cutClusterSeqNo(50);
        send(request.encodedLength());
        assertTrue(fakeReplayer.scans().isEmpty(), "found without reading the recording");
        assertEquals(6144, fakeReplayer.startedReplays().getLast().position());
    }

    @Test
    @DisplayName("with no application joining, the cut is written as safe once the window has passed")
    void aCutNobodyJoinedIsSafeAfterTheWindow() {
        handOver(1);
        fakeReplayer.advanceMillis(JOIN_WINDOW_MS - 1);
        replayerService.poll();
        assertFalse(log.isSafe(LOG_POSITION));

        fakeReplayer.advanceMillis(1);
        replayerService.poll();
        assertTrue(log.isSafe(LOG_POSITION));
        assertTrue(RestartSafeLog.read(clusterDir.resolve(RestartSafeLog.FILE_NAME)).safe().contains(LOG_POSITION),
                   "and it is on disk");
    }

    @Test
    @DisplayName("an application that joined holds the cut back until it confirms")
    void aJoinedApplicationMustConfirm() {
        handOver(1);
        join(1, 16);
        fakeReplayer.advanceMillis(JOIN_WINDOW_MS + 10_000);
        replayerService.poll();
        assertFalse(log.isSafe(LOG_POSITION), "joined and not confirmed");

        confirm(1, 16);
        replayerService.poll();
        assertTrue(log.isSafe(LOG_POSITION));
    }

    @Test
    @DisplayName("a confirmation for another round says nothing about this one")
    void aConfirmationForAnotherRoundIsIgnored() {
        handOver(2);
        join(2, 16);
        confirm(1, 16);
        fakeReplayer.advanceMillis(JOIN_WINDOW_MS);
        replayerService.poll();
        assertFalse(log.isSafe(LOG_POSITION));
    }

    @Test
    @DisplayName("a gateway's two instances on one node leave the round unsafe, and the Replayer says so once")
    void aPairOnOneNodeLeavesTheRoundUnsafe() {
        handOver(1);
        join(1, 16, 7);
        join(1, 16, 8);
        confirm(1, 16, 7);
        confirm(1, 16, 8);
        fakeReplayer.advanceMillis(JOIN_WINDOW_MS + 10_000);
        replayerService.poll();
        assertFalse(log.isSafe(LOG_POSITION), "both confirmed, and the round is still unsafe");
        assertEquals(1, events.stream().filter(event -> event.toString().contains("second client")).count());

        handOver(2);
        join(2, 16, 8);
        confirm(2, 16, 8);
        fakeReplayer.advanceMillis(JOIN_WINDOW_MS);
        replayerService.poll();
        assertTrue(log.isSafe(LOG_POSITION), "the next round is judged afresh");
    }

    @Test
    @DisplayName("an application that confirmed before the Replayer saw the cut still lets it be safe")
    void aConfirmationBeforeTheCutCounts() {
        confirm(1, 16); // the Replayer's cycle was late: the instance confirmed first
        handOver(1);
        join(1, 16);
        fakeReplayer.advanceMillis(JOIN_WINDOW_MS);
        replayerService.poll();

        assertTrue(log.isSafe(LOG_POSITION));
    }
}
