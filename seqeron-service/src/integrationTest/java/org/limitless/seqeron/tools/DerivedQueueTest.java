package org.limitless.seqeron.tools;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.aeron.Aeron;
import io.aeron.ChannelUri;
import io.aeron.FragmentAssembler;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ConsensusModule;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.limitless.seqeron.protocol.FrameLayer;
import org.limitless.seqeron.protocol.PortLayout;
import org.limitless.seqeron.protocol.SequencedFrameDecoder;
import org.limitless.seqeron.sequencer.LogDeriver;
import org.limitless.seqeron.sequencer.QueuePositions;
import org.limitless.seqeron.sequencer.Sequencer;

/**
 * A member's queues derived from its Raft log alone (doc/disk-load-plan.md, step 2a): {@link LogDeriver} over the log's
 * recording, read up to the commit position, must give the frames the member recorded on each queue, byte for byte and
 * at the same positions. Each run also reports how fast the derivation is with the archive out of the way.
 */
@ExtendWith(InterruptingTestCallback.class)
class DerivedQueueTest {
    private static final String IPC_TERM_PROPERTY = "aeron.ipc.term.buffer.length";
    private static final int RESPONSE_STREAM_ID = 125; // beside QueueFrames' and NodeDriver's
    private static final String REPLAY_CHANNEL = "aeron:ipc";
    private static final int QUEUE_REPLAY_STREAM_ID = 1207;
    private static final int LOG_REPLAY_STREAM_ID = 1208;
    private static final long TIMEOUT_NS = TimeUnit.SECONDS.toNanos(30);
    private static final long PACING_NS = TimeUnit.MICROSECONDS.toNanos(100);
    private static final long MEASURE_NS = TimeUnit.SECONDS.toNanos(2);

    @RegisterExtension final SystemTestWatcher watcher = new SystemTestWatcher();

    @TempDir Path baseDir;

    private InProcessCluster cluster;
    private String ipcTerm;

    /** One frame of a queue: where it starts, and its bytes. */
    private record Frame(long position, byte[] bytes) { }

    /** The active recording of a group's queue, read from its start. */
    private record Recorded(long startPosition, int termLength, int mtu, List<Frame> frames) { }

    private record Span(long recordingId, long startPosition, boolean active, int termLength, int mtu) { }

    @BeforeEach
    void remember() {
        ipcTerm = System.getProperty(IPC_TERM_PROPERTY);
        watcher.dataCollector().add(baseDir);
    }

    @AfterEach
    void stopCluster() {
        if (cluster != null) {
            cluster.close();
        }
        if (ipcTerm == null) {
            System.clearProperty(IPC_TERM_PROPERTY);
        } else {
            System.setProperty(IPC_TERM_PROPERTY, ipcTerm);
        }
    }

    /** Frames above the MTU on 256 KiB terms: fragments, term-end padding, a leader stop and a full-log restart. */
    @Test
    @InterruptAfter(180)
    void theLogDerivesEachQueueAcrossALeaderStop() throws Exception {
        System.setProperty(IPC_TERM_PROPERTY, "256k");
        cluster = new InProcessCluster(baseDir);
        cluster.startAll();
        final int leader = cluster.awaitLeader();
        final int member = (leader + 1) % InProcessCluster.MEMBERS;
        final int count = 3_000;
        final long until;
        try (ConfirmProducer producer =
                 new ConfirmProducer(cluster.aeronDir(member), member, count, PACING_NS, 3_000)) {
            CompletableFuture<Void> stopping = null;
            while (!producer.isDrained() && !producer.isStalled()) {
                producer.doWork();
                if (stopping == null && producer.sent() > count / 2) {
                    stopping = CompletableFuture.runAsync(() -> cluster.stop(leader));
                }
                Tests.yield();
            }
            stopping.get(30, TimeUnit.SECONDS);
            assertTrue(producer.isDrained(), "the producer still held " + producer.held() + " frame(s)");
            until = producer.lastClusterSeqNo();
        }
        cluster.start(leader);
        assertEachMemberDerivesItsQueues(until);
    }

    /** Small frames through snapshot rounds, whose markers come from the log's snapshot actions. */
    @Test
    @InterruptAfter(180)
    void theLogDerivesEachQueueAcrossSnapshotRounds() {
        cluster = new InProcessCluster(baseDir);
        cluster.startAll(1, 0);
        final int member = (cluster.awaitLeader() + 1) % InProcessCluster.MEMBERS;
        final long until;
        try (ConfirmProducer producer = new ConfirmProducer(cluster.aeronDir(member), member, 30_000, PACING_NS)) {
            while (!producer.isDrained() && !producer.isStalled()) {
                producer.doWork();
                Tests.yield();
            }
            assertTrue(producer.isDrained(), "the producer still held " + producer.held() + " frame(s)");
            until = producer.lastClusterSeqNo();
        }
        final int markers = assertEachMemberDerivesItsQueues(until);
        assertTrue(markers >= 2, "the run crossed " + markers + " snapshot round(s), not two");
    }

    /** @return the snapshot markers in the default queue up to {@code untilClusterSeqNo} */
    private int assertEachMemberDerivesItsQueues(final long untilClusterSeqNo) {
        int markers = 0;
        for (int m = 0; m < InProcessCluster.MEMBERS; m++) {
            markers = assertDerivesItsQueues(m, untilClusterSeqNo);
        }
        assertFalse(cluster.fatal(), "a member raised a fatal");
        assertFalse(cluster.terminated(), "a member terminated");
        return markers;
    }

    private int assertDerivesItsQueues(final int member, final long untilClusterSeqNo) {
        final Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(cluster.aeronDir(member)));
        final AeronArchive archive =
            AeronArchive.connect(new AeronArchive.Context()
                                     .aeron(aeron)
                                     .ownsAeronClient(false)
                                     .controlRequestChannel(PortLayout.ARCHIVE_CONTROL_CHANNEL)
                                     .controlRequestStreamId(PortLayout.ARCHIVE_CONTROL_STREAM_ID)
                                     .controlResponseChannel(PortLayout.ARCHIVE_CONTROL_CHANNEL)
                                     .controlResponseStreamId(RESPONSE_STREAM_ID));
        try {
            final Recorded[] recorded = new Recorded[FrameLayer.MAX_GROUPS];
            for (final int groupId : InProcessCluster.GROUP_IDS) {
                recorded[groupId - 1] = readQueue(aeron, archive, member, groupId, untilClusterSeqNo);
            }
            final Derived derived = new Derived(recorded);
            final ExpandableArrayBuffer log = new ExpandableArrayBuffer(1 << 20);
            final int logLength = deriveLog(aeron, archive, member, untilClusterSeqNo, derived, log);

            int markers = 0;
            final SequencedFrameDecoder decoder = new SequencedFrameDecoder();
            for (final int groupId : InProcessCluster.GROUP_IDS) {
                final List<Frame> expected = recorded[groupId - 1].frames();
                final List<Frame> actual = derived.frames.get(groupId - 1);
                final String queue = "member " + member + "'s queue of group " + groupId;
                assertTrue(actual.size() >= expected.size(),
                           queue + ": the log derives " + actual.size() + " frame(s), the recording holds " +
                               expected.size());
                for (int i = 0; i < expected.size(); i++) {
                    assertArrayEquals(expected.get(i).bytes(), actual.get(i).bytes(), queue + ": frame " + (i + 1));
                    assertEquals(expected.get(i).position(), actual.get(i).position(),
                                 queue + ": the position of frame " + (i + 1));
                    final byte[] bytes = expected.get(i).bytes();
                    if (groupId == InProcessCluster.GROUP_ID &&
                        decoder.wrap(new UnsafeBuffer(bytes), 0, bytes.length) && decoder.isSystem() &&
                        decoder.systemEventType() == org.limitless.seqeron.protocol.SystemFrame.SNAPSHOT_STARTED) {
                        markers++;
                    }
                }
                System.out.printf("%s: %d frame(s) derived byte for byte, positions to %d%n", queue, expected.size(),
                                  expected.getLast().position());
            }
            if (member == 0) {
                reportThroughput(log, logLength);
            }
            return markers;
        } finally {
            CloseHelper.closeAll(archive, aeron);
        }
    }

    /** The active recording of a group's queue, to the first frame at or past {@code untilClusterSeqNo}. */
    private static Recorded readQueue(final Aeron aeron, final AeronArchive archive, final int member,
                                      final int groupId, final long untilClusterSeqNo) {
        final Span span = awaitActive(archive, FrameLayer.queueStreamId(groupId), "member " + member + "'s queue");
        assertEquals(0, span.startPosition(), "member " + member + "'s recording of group " + groupId + " is whole");
        final List<Frame> frames = new ArrayList<>();
        final ExpandableArrayBuffer assembled = new ExpandableArrayBuffer(4096);
        final long[] state = { 0, 0, 0 }; // the frame's start, its length so far, the last clusterSeqNo
        final SequencedFrameDecoder decoder = new SequencedFrameDecoder();
        final FragmentHandler handler = (buffer, offset, length, header) -> {
            if ((header.flags() & FrameDescriptor.BEGIN_FRAG_FLAG) != 0) {
                state[0] = LogBufferDescriptor.computePosition(header.termId(), header.termOffset(),
                                                               header.positionBitsToShift(), header.initialTermId());
                state[1] = 0;
            }
            assembled.putBytes((int)state[1], buffer, offset, length);
            state[1] += length;
            if ((header.flags() & FrameDescriptor.END_FRAG_FLAG) != 0 && state[2] < untilClusterSeqNo) {
                final byte[] bytes = new byte[(int)state[1]];
                assembled.getBytes(0, bytes);
                frames.add(new Frame(state[0], bytes));
                if (decoder.wrap(assembled, 0, bytes.length)) {
                    state[2] = decoder.clusterSeqNo();
                }
            }
        };
        final long session = archive.startReplay(span.recordingId(), span.startPosition(), AeronArchive.NULL_LENGTH,
                                                 REPLAY_CHANNEL, QUEUE_REPLAY_STREAM_ID);
        final long deadline = System.nanoTime() + TIMEOUT_NS;
        try (Subscription subscription =
                 aeron.addSubscription(ChannelUri.addSessionId(REPLAY_CHANNEL, (int)session), QUEUE_REPLAY_STREAM_ID)) {
            while (state[2] < untilClusterSeqNo) {
                if (subscription.poll(handler, 100) == 0) {
                    if (System.nanoTime() > deadline) {
                        fail("member " + member + "'s recording of group " + groupId + " reached clusterSeqNo " +
                             state[2] + " of " + untilClusterSeqNo);
                    }
                    Tests.yield();
                }
            }
        } finally {
            archive.stopReplay(session);
        }
        return new Recorded(span.startPosition(), span.termLength(), span.mtu(), frames);
    }

    /**
     * Runs a {@link LogDeriver} over the member's log recording, replayed up to its commit position, until it reaches
     * {@code untilClusterSeqNo}, and keeps a copy of each entry in {@code log}, length-prefixed.
     * @return the bytes of {@code log} used
     */
    private static int deriveLog(final Aeron aeron, final AeronArchive archive, final int member,
                                 final long untilClusterSeqNo, final Derived derived, final ExpandableArrayBuffer log) {
        final Span span =
            awaitActive(archive, ConsensusModule.Configuration.LOG_STREAM_ID_DEFAULT, "member " + member + "'s log");
        final LogDeriver deriver = new LogDeriver(member, derived);
        final MutableInteger logLength = new MutableInteger();
        final FragmentAssembler handler = new FragmentAssembler((buffer, offset, length, header) -> {
            deriver.onLogEntry(buffer, offset, length);
            log.putInt(logLength.get(), length);
            log.putBytes(logLength.get() + Integer.BYTES, buffer, offset, length);
            logLength.addAndGet(Integer.BYTES + length);
        });
        final long session =
            archive.startBoundedReplay(span.recordingId(), span.startPosition(), AeronArchive.NULL_LENGTH,
                                       commitPositionCounterId(aeron), REPLAY_CHANNEL, LOG_REPLAY_STREAM_ID);
        final long deadline = System.nanoTime() + TIMEOUT_NS;
        try (Subscription subscription =
                 aeron.addSubscription(ChannelUri.addSessionId(REPLAY_CHANNEL, (int)session), LOG_REPLAY_STREAM_ID)) {
            while (deriver.clusterSeqNo() < untilClusterSeqNo) {
                if (subscription.poll(handler, 100) == 0) {
                    if (System.nanoTime() > deadline) {
                        fail("member " + member + "'s log derived clusterSeqNo " + deriver.clusterSeqNo() + " of " +
                             untilClusterSeqNo);
                    }
                    Tests.yield();
                }
            }
        } finally {
            archive.stopReplay(session);
        }
        return logLength.get();
    }

    /** Derives the captured log again and again with nothing to read or keep, and prints the rate. */
    private static void reportThroughput(final DirectBuffer log, final int logLength) {
        final Sequencer.FrameSink nowhere = new Sequencer.FrameSink() {
            @Override
            public void activate(final int[] groupIds) {
            }

            @Override
            public void publish(final int groupId, final DirectBuffer buffer, final int length) {
            }
        };
        long entries = 0;
        long bytes = 0;
        long measuredFrom = 0;
        final long warmUntil = System.nanoTime() + MEASURE_NS / 2;
        long now;
        do {
            final LogDeriver deriver = new LogDeriver(0, nowhere);
            for (int offset = 0; offset < logLength;) {
                final int length = log.getInt(offset);
                deriver.onLogEntry(log, offset + Integer.BYTES, length);
                offset += Integer.BYTES + length;
                entries++;
                bytes += length;
            }
            now = System.nanoTime();
            if (measuredFrom == 0 && now > warmUntil) {
                measuredFrom = now;
                entries = 0;
                bytes = 0;
            }
        } while (measuredFrom == 0 || now - measuredFrom < MEASURE_NS);
        final double seconds = (now - measuredFrom) / 1e9;
        System.out.printf("derivation alone: %.0f log entries/s, %.0f MB/s of log (%d-byte log)%n", entries / seconds,
                          bytes / seconds / 1e6, logLength);
    }

    private static int commitPositionCounterId(final Aeron aeron) {
        final MutableInteger id = new MutableInteger(CountersReader.NULL_COUNTER_ID);
        aeron.countersReader().forEach((counterId, typeId, keyBuffer, label) -> {
            if (typeId == ConsensusModule.Configuration.COMMIT_POSITION_TYPE_ID) {
                id.set(counterId);
            }
        });
        assertTrue(id.get() != CountersReader.NULL_COUNTER_ID, "the member has a commit position counter");
        return id.get();
    }

    /** The one active recording of a stream, once there is exactly one. */
    private static Span awaitActive(final AeronArchive archive, final int streamId, final String what) {
        final long deadline = System.nanoTime() + TIMEOUT_NS;
        while (true) {
            final List<Span> active = new ArrayList<>();
            archive.listRecordingsForUri(
                0, Integer.MAX_VALUE, "", streamId,
                (controlSessionId, correlationId, recordingId, startTimestamp, stopTimestamp, startPosition,
                 stopPosition, initialTermId, segmentFileLength, termBufferLength, mtuLength, sessionId, stream,
                 strippedChannel, originalChannel, sourceIdentity) -> {
                    if (stopTimestamp == AeronArchive.NULL_TIMESTAMP) {
                        active.add(new Span(recordingId, startPosition, true, termBufferLength, mtuLength));
                    }
                });
            if (active.size() == 1) {
                return active.getFirst();
            }
            if (System.nanoTime() > deadline) {
                fail(what + " has " + active.size() + " active recording(s), not one");
            }
            Tests.sleep(50);
        }
    }

    /** Keeps what a {@link LogDeriver} publishes on each queue, at the position {@link QueuePositions} gives it. */
    private static final class Derived implements Sequencer.FrameSink {
        final List<List<Frame>> frames = new ArrayList<>();
        private final long[] positions = new long[FrameLayer.MAX_GROUPS];
        private final Recorded[] recorded;

        Derived(final Recorded[] recorded) {
            this.recorded = recorded;
            for (int i = 0; i < FrameLayer.MAX_GROUPS; i++) {
                frames.add(new ArrayList<>());
                positions[i] = recorded[i] == null ? 0 : recorded[i].startPosition();
            }
        }

        @Override
        public void activate(final int[] groupIds) {
        }

        @Override
        public void publish(final int groupId, final DirectBuffer buffer, final int length) {
            final Recorded queue = recorded[groupId - 1];
            final long start =
                QueuePositions.frameStart(positions[groupId - 1], length, queue.termLength(), queue.mtu());
            positions[groupId - 1] = start + QueuePositions.framedLength(length, queue.mtu());
            final byte[] bytes = new byte[length];
            buffer.getBytes(0, bytes);
            frames.get(groupId - 1).add(new Frame(start, bytes));
        }
    }
}
