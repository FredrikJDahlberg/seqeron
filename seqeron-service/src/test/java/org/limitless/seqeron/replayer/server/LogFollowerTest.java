package org.limitless.seqeron.replayer.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.codecs.NewLeadershipTermEventEncoder;
import io.aeron.cluster.codecs.SessionMessageHeaderEncoder;
import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.protocol.HeaderFlyweight;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import org.agrona.BitUtil;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.limitless.seqeron.protocol.FrameLayer;
import org.limitless.seqeron.protocol.SystemFrame;
import org.limitless.seqeron.sbe.frame.GroupsRegisteredEncoder;
import org.limitless.seqeron.sbe.frame.MessageHeaderEncoder;
import org.limitless.seqeron.sbe.frame.UnsequencedEncoder;
import org.limitless.seqeron.sbe.frame.UnsequencedSystemEncoder;
import org.limitless.seqeron.sequencer.LogDeriver;

/** The checkpoints and runs a derivation keeps over a Raft log, written here as an archive writes its segment files. */
class LogFollowerTest {
    private static final long RECORDING_ID = 3;
    private static final int TERM_LENGTH = 64 * 1024;
    private static final int SEGMENT_LENGTH = 2 * TERM_LENGTH;
    private static final int MTU = 1408;
    private static final long SESSION = 0x70_0001L;
    private static final int SOURCE_ID = 99;
    private static final int GROUP = 1;
    /** Large enough that no checkpoint, thinning or bound on the runs is reached. */
    private static final long NONE = Long.MAX_VALUE / 4;

    @TempDir Path archiveDir;

    private final MutableDirectBuffer entry = new ExpandableArrayBuffer(1024);
    private final MutableDirectBuffer body = new ExpandableArrayBuffer(GroupsRegisteredEncoder.BLOCK_LENGTH);
    /** Where each entry written ends, and where the next is written. */
    private final List<Long> ends = new ArrayList<>();
    private long tail;
    private long commitPosition = Long.MAX_VALUE;

    @Test
    @DisplayName("an entry the predictor does not derive keeps a run of its frames; one it derives keeps none")
    void runsWherePredictorDiffers() {
        groups();
        payload();
        payload();
        final long leadershipAt = tail;
        leadership();
        payload();
        final LogFollower follower = drained(NONE, Integer.MAX_VALUE, NONE);

        assertEquals(3, follower.runCount()); // the start, GroupsRegistered and LeadershipChanged
        final LogFollower.Run groupsRun = follower.run(1);
        assertEquals(0, groupsRun.logPosition());
        assertEquals(ends.getFirst(), groupsRun.entriesFrom());
        assertEquals(List.of(GROUP), groupsRun.lead().stream().map(LogFollower.Frame::groupId).toList());
        assertEquals(0, groupsRun.queuePositions()[GROUP - 1]);

        final LogFollower.Run leadershipRun = follower.run(2);
        assertEquals(leadershipAt, leadershipRun.logPosition());
        assertEquals(ends.get(3), leadershipRun.entriesFrom());
        assertEquals(1, leadershipRun.lead().size());
        final long beforeLeadership = leadershipRun.queuePositions()[GROUP - 1];
        assertTrue(beforeLeadership > 0);
        assertTrue(follower.queuePosition(GROUP) > beforeLeadership);

        assertEquals(1, follower.runAt(GROUP, 0));
        assertEquals(1, follower.runAt(GROUP, beforeLeadership - 1));
        assertEquals(2, follower.runAt(GROUP, beforeLeadership));
        assertNull(follower.run(3));
    }

    @Test
    @DisplayName("nothing past the commit position is read, and the queues are a tip once it reaches its start's")
    void boundedByTheCommitPosition() {
        groups();
        payload();
        payload();
        payload();
        commitPosition = ends.get(1);
        final LogFollower follower = follower(NONE, Integer.MAX_VALUE, NONE);
        assertFalse(follower.isValid());

        drain(follower);
        assertEquals(ends.get(1), follower.entryEnd());
        assertTrue(follower.isValid());
        final long queuePosition = follower.queuePosition(GROUP);

        commitPosition = tail;
        drain(follower);
        assertEquals(tail, follower.entryEnd());
        assertTrue(follower.queuePosition(GROUP) > queuePosition);
    }

    @Test
    @DisplayName("a checkpoint and a run are kept at the first entry's end the spacing past the last")
    void checkpointsAtTheSpacing() {
        groups();
        for (int i = 0; i < 40; i++) {
            payload();
        }
        final long spacing = 1000;
        final LogFollower follower = drained(spacing, Integer.MAX_VALUE, NONE);

        final List<Long> expected = new ArrayList<>(List.of(0L));
        for (final long end : ends) {
            if (end - expected.getLast() >= spacing) {
                expected.add(end);
            }
        }
        assertTrue(expected.size() > 3);
        assertEquals(expected, checkpointPositions(follower));
        for (final LogFollower.Checkpoint checkpoint : follower.checkpoints().subList(1, expected.size())) {
            final LogFollower.Run run = follower.run(follower.runAt(GROUP, checkpoint.queuePositions()[GROUP - 1]));
            assertEquals(checkpoint.logPosition(), run.logPosition());
            assertEquals(checkpoint.logPosition(), run.entriesFrom());
            assertTrue(run.lead().isEmpty());
        }
    }

    @Test
    @DisplayName("past the most checkpoints every other is released, the first kept, and the spacing doubles")
    void thinning() {
        for (int i = 0; i < 16; i++) {
            leadership(); // no group is active, so these publish nothing and are all one length
        }
        final long length = ends.getFirst();
        final LogFollower follower = drained(length, 4, NONE);

        // One a length at first: 0..4 is five, thinned to 0,2,4; then one every 2: 6, 8 thinned to 0,4,8; then every
        // 4: 12, 16 thinned to 0,8,16.
        assertEquals(List.of(0L, 8 * length, 16 * length), checkpointPositions(follower));
    }

    @Test
    @DisplayName("a purge releases what is below every queue's start but the last, and run numbers go on")
    void purge() {
        groups();
        for (int i = 0; i < 40; i++) {
            payload();
            if (i % 10 == 9) {
                leadership();
            }
        }
        final LogFollower follower = drained(1000, Integer.MAX_VALUE, NONE);
        final List<LogFollower.Checkpoint> before = List.copyOf(follower.checkpoints());
        final int runs = follower.runCount();
        final LogFollower.Checkpoint kept = before.get(3);
        final long newStart = kept.queuePositions()[GROUP - 1];
        final long firstKeptRun = follower.runAt(GROUP, newStart);
        final LogFollower.Run run = follower.run(firstKeptRun);

        follower.purge(GROUP, newStart);
        assertEquals(newStart, follower.queueStart(GROUP));
        assertEquals(before.subList(3, before.size()), follower.checkpoints());
        assertNull(follower.run(firstKeptRun - 1));
        assertSame(run, follower.run(firstKeptRun));
        assertEquals(firstKeptRun, follower.runAt(GROUP, newStart));
        assertEquals(-1, follower.runAt(GROUP, newStart - 1));
        assertEquals(runs - firstKeptRun, follower.runCount());

        follower.purge(GROUP, Long.MAX_VALUE);
        assertEquals(List.of(before.getLast()), follower.checkpoints());
        assertEquals(1, follower.runCount());
        assertNotNull(follower.run(runs - 1));
    }

    @Test
    @DisplayName("past the runs' bound, a run up to the next checkpoint is derived by a sequencer")
    void sequencedRuns() {
        groups();
        leadership();
        leadership();
        final long checkpointAt = tail;
        final long spacing = checkpointAt;
        payload();
        leadership();
        final LogFollower follower = drained(spacing, Integer.MAX_VALUE, 0);

        // The start; GroupsRegistered, which the deriver derives up to the checkpoint; the checkpoint; the leadership.
        assertEquals(4, follower.runCount());
        assertEquals(2, follower.sequencedRunCount());
        assertNotNull(follower.run(1).state());
        assertEquals(checkpointAt, follower.run(2).logPosition());
        assertNull(follower.run(2).state());
        assertNotNull(follower.run(3).state());
    }

    private static List<Long> checkpointPositions(final LogFollower follower) {
        return follower.checkpoints().stream().map(LogFollower.Checkpoint::logPosition).toList();
    }

    private LogFollower drained(final long checkpointLogBytes, final int maxCheckpoints, final long maxRunBytes) {
        final LogFollower follower = follower(checkpointLogBytes, maxCheckpoints, maxRunBytes);
        drain(follower);
        assertEquals(tail, follower.entryEnd());
        assertTrue(follower.isValid());
        return follower;
    }

    private LogFollower follower(final long checkpointLogBytes, final int maxCheckpoints, final long maxRunBytes) {
        final DerivationStart start = new DerivationStart(0, new LogDeriver.State(List.of(), new long[] { SESSION }),
                                                          false, new long[FrameLayer.MAX_GROUPS], TERM_LENGTH, MTU);
        final ArchiveRecording log =
            new ArchiveRecording(RECORDING_ID, 0, AeronArchive.NULL_POSITION, true, TERM_LENGTH, SEGMENT_LENGTH);
        return new LogFollower(0, start, archiveDir.toFile(), log,
                               () -> Math.min(commitPosition, tail), checkpointLogBytes, maxCheckpoints, maxRunBytes);
    }

    private static void drain(final LogFollower follower) {
        long last;
        do {
            last = follower.entryEnd();
            follower.doWork();
        } while (follower.entryEnd() != last);
    }

    /** The topology's GroupsRegistered, which activates group 1. */
    private void groups() {
        body.setMemory(0, GroupsRegisteredEncoder.BLOCK_LENGTH, (byte)0);
        body.putStringWithoutLengthAscii(0, "group-1");
        final UnsequencedSystemEncoder encoder = new UnsequencedSystemEncoder();
        encoder.wrapAndApplyHeader(entry, AeronCluster.SESSION_HEADER_LENGTH, new MessageHeaderEncoder());
        encoder.header()
            .sourceId(SOURCE_ID)
            .connectionId(-1)
            .sessionId(-1)
            .systemEventType(SystemFrame.GROUPS_REGISTERED)
            .groupMask(0);
        encoder.putBody(body, 0, GroupsRegisteredEncoder.BLOCK_LENGTH);
        sessionMessage(MessageHeaderEncoder.ENCODED_LENGTH + encoder.encodedLength());
    }

    /** An application frame to group 1. */
    private void payload() {
        final UnsequencedEncoder encoder = new UnsequencedEncoder();
        encoder.wrapAndApplyHeader(entry, AeronCluster.SESSION_HEADER_LENGTH, new MessageHeaderEncoder());
        encoder.header().sourceId(SOURCE_ID).connectionId(1).sessionId(-1).payloadId(5).groupMask(1 << (GROUP - 1));
        encoder.putPayload(new byte[24], 0, 24);
        sessionMessage(MessageHeaderEncoder.ENCODED_LENGTH + encoder.encodedLength());
    }

    private void sessionMessage(final int ingressLength) {
        new SessionMessageHeaderEncoder()
            .wrapAndApplyHeader(entry, 0, new io.aeron.cluster.codecs.MessageHeaderEncoder())
            .leadershipTermId(0)
            .clusterSessionId(SESSION)
            .timestamp(1_000_000_000L + ends.size());
        write(AeronCluster.SESSION_HEADER_LENGTH + ingressLength);
    }

    /** A new leadership term, whose LeadershipChanged no predictor derives. */
    private void leadership() {
        final NewLeadershipTermEventEncoder encoder = new NewLeadershipTermEventEncoder().wrapAndApplyHeader(
            entry, 0, new io.aeron.cluster.codecs.MessageHeaderEncoder());
        encoder.leadershipTermId(ends.size()).leaderMemberId(0).timestamp(1_000_000_000L + ends.size());
        write(io.aeron.cluster.codecs.MessageHeaderEncoder.ENCODED_LENGTH + encoder.encodedLength());
    }

    /** Writes {@link #entry} as one unfragmented frame, after a padding frame if the term cannot hold it. */
    private void write(final int length) {
        final int frameLength = DataHeaderFlyweight.HEADER_LENGTH + length;
        final int aligned = BitUtil.align(frameLength, FrameDescriptor.FRAME_ALIGNMENT);
        final int termRemaining = TERM_LENGTH - (int)(tail & (TERM_LENGTH - 1));
        if (aligned > termRemaining) {
            writeFrame(header(termRemaining, HeaderFlyweight.HDR_TYPE_PAD).limit(DataHeaderFlyweight.HEADER_LENGTH),
                       termRemaining);
        }
        final ByteBuffer frame = header(frameLength, HeaderFlyweight.HDR_TYPE_DATA);
        entry.getBytes(0, frame, DataHeaderFlyweight.HEADER_LENGTH, length);
        writeFrame(frame, aligned);
        ends.add(tail);
    }

    private static ByteBuffer header(final int frameLength, final int type) {
        final ByteBuffer frame = ByteBuffer.allocate(BitUtil.align(frameLength, FrameDescriptor.FRAME_ALIGNMENT))
                                     .order(ByteOrder.LITTLE_ENDIAN);
        frame.putInt(HeaderFlyweight.FRAME_LENGTH_FIELD_OFFSET, frameLength);
        frame.put(HeaderFlyweight.FLAGS_FIELD_OFFSET, (byte)FrameDescriptor.UNFRAGMENTED);
        frame.putShort(HeaderFlyweight.TYPE_FIELD_OFFSET, (short)type);
        return frame;
    }

    private void writeFrame(final ByteBuffer frame, final int alignedLength) {
        final long base = AeronArchive.segmentFileBasePosition(0, tail, TERM_LENGTH, SEGMENT_LENGTH);
        try (FileChannel file = FileChannel.open(archiveDir.resolve(RECORDING_ID + "-" + base + ".rec"),
                                                 StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
            file.write(frame, tail - base);
        } catch (final IOException ex) {
            throw new UncheckedIOException(ex);
        }
        tail += alignedLength;
    }
}
