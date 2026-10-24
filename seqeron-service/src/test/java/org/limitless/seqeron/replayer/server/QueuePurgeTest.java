package org.limitless.seqeron.replayer.server;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.Arrays;
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
import org.limitless.seqeron.protocol.ReplayProtocol;
import org.limitless.seqeron.protocol.SeqeronCounters;
import org.limitless.seqeron.sbe.replay.MessageHeaderDecoder;
import org.limitless.seqeron.sbe.replay.MessageHeaderEncoder;
import org.limitless.seqeron.sbe.replay.ReplayRequestEncoder;
import org.limitless.seqeron.sbe.replay.ReplayingDecoder;

/**
 * A member's Replayer purging its queue recording below the newest restart-safe cut (doc/snapshot.md §10.6).
 */
class QueuePurgeTest {
    private static final int GROUP_ID = FakeReplayer.GROUP_ID;
    private static final long JOIN_WINDOW_MS = 1_000;
    private static final long SEGMENT = FakeReplayer.SEGMENT_FILE_LENGTH;
    private static final long RECORDING_ID = 6;
    private static final long TIP = 5 * SEGMENT;

    @TempDir
    Path clusterDir;

    private final Queue<CutHandover> cuts = new ConcurrentLinkedQueue<>();
    private final MutableDirectBuffer encodeBuffer = new ExpandableArrayBuffer(64);
    private FakeReplayer fakeReplayer;
    private ReplayerService replayerService;
    private RestartSafeLog log;

    private void start(final long startPosition) {
        fakeReplayer = new FakeReplayer();
        log = RestartSafeLog.open(clusterDir);
        final Queue<byte[]> topologyRows = new ConcurrentLinkedQueue<>(new TopologyLoad().groups(GROUP_ID, "group-1").rows());
        replayerService = new ReplayerService(fakeReplayer, 0, NoOpIdleStrategy.INSTANCE, topologyRows,
                                              QueueResolver.EVERY_GROUP, () -> { },
                                              new CutIntake(cuts, log, JOIN_WINDOW_MS));
        fakeReplayer.addRecording(RECORDING_ID, startPosition, true, TIP);
        for (int cycle = 0; cycle < 20 && fakeReplayer.counter(SeqeronCounters.REPLAYER_READY_TYPE_ID) == 0; cycle++) {
            replayerService.poll();
        }
    }

    @AfterEach
    void tearDown() {
        log.close();
    }

    /** Hands over a cut whose marker is at {@code markerPosition} and lets it become restart-safe. */
    private void cutAt(final long clusterSeqNo, final long logPosition, final long markerPosition) {
        final long[] positions = new long[16];
        Arrays.fill(positions, -1);
        positions[GROUP_ID - 1] = markerPosition;
        cuts.add(new CutHandover(1, clusterSeqNo, logPosition, positions));
        replayerService.poll();
        fakeReplayer.advanceMillis(JOIN_WINDOW_MS);
        replayerService.poll();
    }

    @Test
    @DisplayName("the recording is purged to the segment boundary below the safe cut's marker, once it is safe")
    void purgesBelowTheSafeCut() {
        start(0);
        final long[] positions = new long[16];
        Arrays.fill(positions, -1);
        positions[GROUP_ID - 1] = 2 * SEGMENT + 1_000;
        cuts.add(new CutHandover(1, 50, 8_192, positions));
        replayerService.poll();
        assertTrue(fakeReplayer.purgeAttempts().isEmpty(), "not safe yet");

        fakeReplayer.advanceMillis(JOIN_WINDOW_MS);
        replayerService.poll();

        assertEquals(1, fakeReplayer.purgeAttempts().size());
        assertArrayEquals(new long[] {RECORDING_ID, 2 * SEGMENT}, fakeReplayer.purgeAttempts().getFirst());
    }

    @Test
    @DisplayName("a recording that starts mid-term is purged to a boundary of its own segments")
    void roundsFromAStartMidTerm() {
        start(SEGMENT / 2);
        cutAt(50, 8_192, 3 * SEGMENT - 1);

        assertArrayEquals(new long[] {RECORDING_ID, 2 * SEGMENT}, fakeReplayer.purgeAttempts().getFirst());
    }

    @Test
    @DisplayName("a floor inside the first segment asks the archive for nothing")
    void nothingInsideTheFirstSegment() {
        start(0);
        cutAt(50, 8_192, SEGMENT - 1);

        assertTrue(fakeReplayer.purgeAttempts().isEmpty());
    }

    @Test
    @DisplayName("a refusal is retried after an interval, and is no archive error")
    void aRefusalIsRetried() {
        start(0);
        fakeReplayer.refusePurges(true);
        cutAt(50, 8_192, 2 * SEGMENT);
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
    @DisplayName("a purge that throws is counted as an archive error and retried after the interval")
    void aFailedPurgeIsCountedAndRetried() {
        start(0);
        fakeReplayer.failPurges(new IllegalStateException("archive gone"));
        cutAt(50, 8_192, 2 * SEGMENT);
        assertEquals(1, fakeReplayer.purgeAttempts().size());
        assertEquals(1, fakeReplayer.counter(SeqeronCounters.REPLAYER_ARCHIVE_ERRORS_COUNT_TYPE_ID));

        fakeReplayer.advanceMillis(4_999);
        replayerService.poll();
        assertEquals(1, fakeReplayer.purgeAttempts().size(), "not before the interval");

        fakeReplayer.failPurges(null);
        fakeReplayer.advanceMillis(1);
        replayerService.poll();
        assertEquals(2, fakeReplayer.purgeAttempts().size());
    }

    @Test
    @DisplayName("a cut handed over while the archive cannot list is counted, purged below nowhere, and found by reading")
    void aCutTheArchiveCannotListIsFoundByReading() {
        start(0);
        fakeReplayer.addFrames(RECORDING_ID, 0, 49, 2 * SEGMENT, 50);
        fakeReplayer.failArchive(new IllegalStateException("archive gone"));
        cutAt(50, 8_192, 2 * SEGMENT);

        assertEquals(2, fakeReplayer.counter(SeqeronCounters.REPLAYER_ARCHIVE_ERRORS_COUNT_TYPE_ID),
                     "keeping the cut, and the purge floor once it was safe");
        assertTrue(fakeReplayer.purgeAttempts().isEmpty());

        fakeReplayer.answerArchiveAgain();
        assertTrue(cutRequest(50) >= 0);
        assertFalse(fakeReplayer.scans().isEmpty(), "it was not kept");
        assertEquals(2 * SEGMENT, fakeReplayer.startedReplays().getLast().position());
    }

    @Test
    @DisplayName("after a purge, a cut kept below the new start is refused, as a position below it is")
    void aKeptCutBelowThePurgeIsRefused() {
        start(0);
        replayerService.keepCut(GROUP_ID, 20, 1_024);
        cutAt(50, 8_192, 2 * SEGMENT);

        assertEquals(ReplayProtocol.NO_REPLAY_NEEDED, cutRequest(20));
        assertTrue(fakeReplayer.startedReplays().isEmpty());
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
