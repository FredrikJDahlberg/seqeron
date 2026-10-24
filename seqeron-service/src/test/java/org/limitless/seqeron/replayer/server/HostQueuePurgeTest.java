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
import org.limitless.seqeron.protocol.ReplayProtocol;
import org.limitless.seqeron.protocol.SeqeronCounters;
import org.limitless.seqeron.sbe.replay.MessageHeaderDecoder;
import org.limitless.seqeron.sbe.replay.MessageHeaderEncoder;
import org.limitless.seqeron.sbe.replay.ReplayRequestEncoder;
import org.limitless.seqeron.sbe.replay.ReplayingDecoder;

/**
 * A gateway host's Replayer purging its queue recording below the floor its relay's reported cuts give
 * (doc/snapshot-gateway-purge-plan.md); {@link QueuePurgeTest} is a member's.
 */
class HostQueuePurgeTest {
    private static final int GROUP_ID = FakeReplayer.GROUP_ID;
    private static final long JOIN_WINDOW_MS = 1_000;
    private static final long SEGMENT = FakeReplayer.SEGMENT_FILE_LENGTH;
    private static final long RECORDING_ID = 6;

    private final Queue<RelayedCut> reports = new ConcurrentLinkedQueue<>();
    private final MutableDirectBuffer encodeBuffer = new ExpandableArrayBuffer(64);
    private FakeReplayer fakeReplayer;
    private ReplayerService replayerService;

    @BeforeEach
    void setUp() {
        fakeReplayer = new FakeReplayer();
        final Queue<byte[]> topologyRows =
            new ConcurrentLinkedQueue<>(new TopologyLoad().groups(GROUP_ID, "group-1").rows());
        replayerService = new ReplayerService(fakeReplayer, 0, NoOpIdleStrategy.INSTANCE, topologyRows,
                                              QueueResolver.EVERY_GROUP, () -> { },
                                              CutIntake.ofHost(reports, JOIN_WINDOW_MS));
        fakeReplayer.addRecording(RECORDING_ID, 0, true, 5 * SEGMENT);
        for (int cycle = 0; cycle < 20 && fakeReplayer.counter(SeqeronCounters.REPLAYER_READY_TYPE_ID) == 0; cycle++) {
            replayerService.poll();
        }
    }

    /** Reports a live cut whose marker is at {@code position} and lets its window pass. */
    private void cutAt(final long round, final long clusterSeqNo, final long position) {
        reports.add(new RelayedCut(round, clusterSeqNo, GROUP_ID, position, true));
        replayerService.poll();
        fakeReplayer.advanceMillis(JOIN_WINDOW_MS);
        replayerService.poll();
    }

    @Test
    @DisplayName("the recording is purged to the segment boundary below the reported cut, once its window has passed")
    void purgesBelowTheReportedCut() {
        reports.add(new RelayedCut(1, 50, GROUP_ID, 2 * SEGMENT + 1_000, true));
        replayerService.poll();
        assertTrue(fakeReplayer.purgeAttempts().isEmpty(), "not safe yet");

        fakeReplayer.advanceMillis(JOIN_WINDOW_MS);
        replayerService.poll();

        assertEquals(1, fakeReplayer.purgeAttempts().size());
        assertArrayEquals(new long[] {RECORDING_ID, 2 * SEGMENT}, fakeReplayer.purgeAttempts().getFirst());
    }

    @Test
    @DisplayName("a refusal is retried after an interval, and is no archive error")
    void aRefusalIsRetried() {
        fakeReplayer.refusePurges(true);
        cutAt(1, 50, 2 * SEGMENT);
        assertEquals(1, fakeReplayer.purgeAttempts().size());

        fakeReplayer.advanceMillis(4_999);
        replayerService.poll();
        assertEquals(1, fakeReplayer.purgeAttempts().size(), "not before the interval");

        fakeReplayer.refusePurges(false);
        fakeReplayer.advanceMillis(1);
        replayerService.poll();
        assertEquals(2, fakeReplayer.purgeAttempts().size());

        replayerService.poll();
        assertEquals(2, fakeReplayer.purgeAttempts().size(), "done once it is purged");
        assertEquals(0, fakeReplayer.counter(SeqeronCounters.REPLAYER_ARCHIVE_ERRORS_COUNT_TYPE_ID));
    }

    @Test
    @DisplayName("after a purge, a cut kept below the new start is refused, as a position below it is")
    void aKeptCutBelowThePurgeIsRefused() {
        reports.add(new RelayedCut(1, 20, GROUP_ID, 1_024, false));
        cutAt(2, 50, 2 * SEGMENT);

        assertEquals(ReplayProtocol.NO_REPLAY_NEEDED, cutRequest(20));
        assertTrue(fakeReplayer.startedReplays().isEmpty());
    }

    @Test
    @DisplayName("a floor inside the first segment asks the archive for nothing")
    void nothingInsideTheFirstSegment() {
        cutAt(1, 50, SEGMENT - 1);

        assertTrue(fakeReplayer.purgeAttempts().isEmpty());
    }

    private long cutRequest(final long cutClusterSeqNo) {
        final ReplayRequestEncoder request = new ReplayRequestEncoder();
        request.wrapAndApplyHeader(encodeBuffer, 0, new MessageHeaderEncoder())
            .clientId(7).requestId(1).fromPosition(1_024).queueId(GROUP_ID).cutClusterSeqNo(cutClusterSeqNo);
        final byte[] message = new byte[MessageHeaderEncoder.ENCODED_LENGTH + request.encodedLength()];
        encodeBuffer.getBytes(0, message);
        fakeReplayer.offerRequest(message);
        replayerService.poll();

        final byte[] reply = fakeReplayer.controlReplies().getLast();
        final MessageHeaderDecoder header = new MessageHeaderDecoder().wrap(new UnsafeBuffer(reply), 0);
        return new ReplayingDecoder().wrap(new UnsafeBuffer(reply), MessageHeaderDecoder.ENCODED_LENGTH,
                                           header.blockLength(), header.version()).replaySessionId();
    }
}
