package org.limitless.seqeron.replayer.server;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.NoOpIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.limitless.seqeron.protocol.SeqeronCounters;
import org.limitless.seqeron.sbe.replay.MessageHeaderDecoder;
import org.limitless.seqeron.sbe.replay.MessageHeaderEncoder;
import org.limitless.seqeron.sbe.replay.SnapshotConfirmedEncoder;
import org.limitless.seqeron.sbe.replay.SnapshotJoinedEncoder;
import org.limitless.seqeron.sbe.replay.SnapshotRoundDecoder;

/**
 * A gateway host's Replayer tracking the markers its relays report (doc/snapshot-gateway-purge-plan.md): the first
 * live report of a cut opens its round, every report is kept for its own queue, and a safe cut raises each reported
 * queue's purge floor.
 */
class HostCutTrackingTest {
    private static final long JOIN_WINDOW_MS = 1_000;
    private static final long SEGMENT = FakeReplayer.SEGMENT_FILE_LENGTH;
    private static final long RECORDING_1 = 6;
    private static final long RECORDING_2 = 7;
    private static final int SOURCE_ID = 40;

    private final Queue<RelayedCut> reports = new ConcurrentLinkedQueue<>();
    private final MutableDirectBuffer encodeBuffer = new ExpandableArrayBuffer(64);
    private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
    private FakeReplayer fakeReplayer;
    private ReplayerService replayerService;

    @BeforeEach
    void setUp() {
        fakeReplayer = new FakeReplayer();
        final Queue<byte[]> topologyRows =
            new ConcurrentLinkedQueue<>(new TopologyLoad().groups(1, "group-1", 2, "group-2").rows());
        replayerService = new ReplayerService(fakeReplayer, 0, NoOpIdleStrategy.INSTANCE, topologyRows,
                                              QueueResolver.EVERY_GROUP, () -> { },
                                              CutIntake.ofHost(reports, JOIN_WINDOW_MS));
        fakeReplayer.addRecording(1, RECORDING_1, 0, true, 5 * SEGMENT);
        fakeReplayer.addRecording(2, RECORDING_2, 0, true, 5 * SEGMENT);
        for (int cycle = 0; cycle < 20 && fakeReplayer.counter(SeqeronCounters.REPLAYER_READY_TYPE_ID) == 0; cycle++) {
            replayerService.poll();
        }
    }

    private void report(final long round, final long clusterSeqNo, final int groupId, final long position,
                        final boolean live) {
        reports.add(new RelayedCut(round, clusterSeqNo, groupId, position, live));
        replayerService.poll();
    }

    private void join(final long round) {
        final SnapshotJoinedEncoder encoder = new SnapshotJoinedEncoder();
        encoder.wrapAndApplyHeader(encodeBuffer, 0, headerEncoder).clientId(7).sourceId(SOURCE_ID).round(round);
        send(encoder.encodedLength());
    }

    private void confirm(final long round) {
        final SnapshotConfirmedEncoder encoder = new SnapshotConfirmedEncoder();
        encoder.wrapAndApplyHeader(encodeBuffer, 0, headerEncoder).clientId(7).sourceId(SOURCE_ID).round(round);
        send(encoder.encodedLength());
    }

    private void send(final int bodyLength) {
        final byte[] message = new byte[MessageHeaderEncoder.ENCODED_LENGTH + bodyLength];
        encodeBuffer.getBytes(0, message);
        fakeReplayer.offerRequest(message);
        replayerService.poll();
    }

    private void passTheWindow() {
        fakeReplayer.advanceMillis(JOIN_WINDOW_MS);
        replayerService.poll();
    }

    private long rounds() {
        return fakeReplayer.controlReplies().stream()
            .filter(reply -> new MessageHeaderDecoder().wrap(new UnsafeBuffer(reply), 0).templateId() ==
                             SnapshotRoundDecoder.TEMPLATE_ID)
            .count();
    }

    @Test
    @DisplayName("a joined and confirmed cut raises the floor of every queue that reported it")
    void aConfirmedCutRaisesEveryReportedFloor() {
        report(1, 50, 1, 2 * SEGMENT + 100, true);
        report(1, 50, 2, 3 * SEGMENT + 100, true);
        join(1);
        confirm(1);
        passTheWindow();

        assertEquals(2, fakeReplayer.purgeAttempts().size());
        assertArrayEquals(new long[] {RECORDING_1, 2 * SEGMENT}, fakeReplayer.purgeAttempts().get(0));
        assertArrayEquals(new long[] {RECORDING_2, 3 * SEGMENT}, fakeReplayer.purgeAttempts().get(1));
    }

    @Test
    @DisplayName("an instance that joined and never confirmed holds every floor")
    void anUnconfirmedCutRaisesNoFloor() {
        report(1, 50, 1, 2 * SEGMENT + 100, true);
        report(1, 50, 2, 3 * SEGMENT + 100, true);
        join(1);
        fakeReplayer.advanceMillis(10 * JOIN_WINDOW_MS);
        replayerService.poll();

        assertTrue(fakeReplayer.purgeAttempts().isEmpty());
    }

    @Test
    @DisplayName("the second queue's report of a cut opens no second round and offers no second SnapshotRound")
    void oneRoundPerCut() {
        report(1, 50, 1, 2 * SEGMENT + 100, true);
        report(1, 50, 2, 3 * SEGMENT + 100, true);

        assertEquals(1, rounds());
    }

    @Test
    @DisplayName("a queue whose relay has not reached the marker gets no floor from the round")
    void aQueueThatReportedNothingGetsNoFloor() {
        report(1, 50, 1, 2 * SEGMENT + 100, true);
        passTheWindow();

        assertEquals(1, fakeReplayer.purgeAttempts().size());
        assertEquals(RECORDING_1, fakeReplayer.purgeAttempts().getFirst()[0]);
    }

    @Test
    @DisplayName("historical reports open no round and raise no floor")
    void historicalReportsOpenNoRound() {
        report(1, 50, 1, 2 * SEGMENT + 100, false);
        report(2, 60, 1, 3 * SEGMENT + 100, false);
        report(2, 60, 2, 3 * SEGMENT + 100, false);
        passTheWindow();

        assertEquals(0, rounds());
        assertTrue(fakeReplayer.purgeAttempts().isEmpty());
    }

    @Test
    @DisplayName("a live report after historical ones opens the round, and the historical cuts are still kept")
    void aLiveReportFollowsHistory() {
        report(1, 50, 1, 2 * SEGMENT + 100, false);
        report(2, 60, 1, 3 * SEGMENT + 100, true);
        passTheWindow();

        assertEquals(1, rounds());
        assertArrayEquals(new long[] {RECORDING_1, 3 * SEGMENT}, fakeReplayer.purgeAttempts().getFirst());
    }

    @Test
    @DisplayName("a cut already opened is not opened again by a late live report")
    void aCutOpensOnce() {
        report(2, 60, 1, 3 * SEGMENT + 100, true);
        report(1, 50, 2, 2 * SEGMENT + 100, true);

        assertEquals(1, rounds());
    }
}
