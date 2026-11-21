package org.limitless.seqeron.tools;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.aeron.Aeron;
import io.aeron.ChannelUri;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.logbuffer.Header;
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
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.limitless.seqeron.protocol.FrameLayer;
import org.limitless.seqeron.protocol.PortLayout;
import org.limitless.seqeron.protocol.ReplayProtocol;
import org.limitless.seqeron.protocol.SequencedFrameDecoder;
import org.limitless.seqeron.protocol.SystemFrame;
import org.limitless.seqeron.sequencer.DerivationStart;
import org.limitless.seqeron.sequencer.RestartSafeLog;

/**
 * A member's queues, which its queue agent publishes from its Raft log, against the history its Replayer derives from
 * the same log: each member's queue, as its archive records it, must be what its Replayer serves, frame for frame and
 * position for position, and each member's history must be the reference member's from where it starts. A member
 * restarted from a cluster snapshot starts its queues at the cut its snapshot recorded, with the round's marker, and
 * that must be where the marker lies on the reference's. A restore's replay from each marker's cut, which the Replayer
 * answers from the marker positions the service recorded, must open on that marker where the queue holds it.
 */
@ExtendWith(InterruptingTestCallback.class)
class DerivedQueueTest {
    private static final String IPC_TERM_PROPERTY = "aeron.ipc.term.buffer.length";
    private static final String JOIN_PROPERTY = "replayer.snapshotJoinMs";
    private static final int RESPONSE_STREAM_ID = 125; // beside QueueFrames' and NodeDriver's
    private static final String REPLAY_CHANNEL = "aeron:ipc";
    private static final int QUEUE_REPLAY_STREAM_ID = 1207;
    private static final long TIMEOUT_NS = TimeUnit.SECONDS.toNanos(30);
    private static final long PACING_NS = TimeUnit.MICROSECONDS.toNanos(100);
    private static final long SAFE_CHECK_NS = TimeUnit.MILLISECONDS.toNanos(50);

    @RegisterExtension final SystemTestWatcher watcher = new SystemTestWatcher();

    @TempDir Path baseDir;

    private InProcessCluster cluster;
    private String ipcTerm;
    private String joinMs;

    /** One frame of a queue: where it starts, and its bytes. */
    private record Frame(long position, byte[] bytes) { }

    /** The active recording of a group's queue: where it starts, and its frames from the first whole one. */
    private record Recorded(long startPosition, List<Frame> frames) { }

    private record Span(long recordingId, long startPosition) { }

    @BeforeEach
    void remember() {
        ipcTerm = System.getProperty(IPC_TERM_PROPERTY);
        joinMs = System.getProperty(JOIN_PROPERTY);
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
        if (joinMs == null) {
            System.clearProperty(JOIN_PROPERTY);
        } else {
            System.setProperty(JOIN_PROPERTY, joinMs);
        }
    }

    /** Frames above the MTU on 256 KiB terms: fragments, term-end padding, a leader stop and a full-log restart. */
    @Test
    @InterruptAfter(180)
    void eachQueueIsItsDerivedHistoryAcrossALeaderStop() throws Exception {
        System.setProperty(IPC_TERM_PROPERTY, "256k");
        cluster = new InProcessCluster(baseDir);
        cluster.withQueueRecordings();
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
        assertEachMembersQueues(member, until);
    }

    /**
     * Small frames through snapshot rounds, whose markers come from the log's snapshot actions, and a follower
     * restarted from a cluster snapshot, whose queues start at its cut.
     */
    @Test
    @InterruptAfter(180)
    void eachQueueIsItsDerivedHistoryAcrossSnapshotRounds() throws Exception {
        System.setProperty(JOIN_PROPERTY, "200");
        cluster = new InProcessCluster(baseDir);
        cluster.withQueueRecordings();
        cluster.startAll(1, 0);
        final int leader = cluster.awaitLeader();
        final int member = (leader + 1) % InProcessCluster.MEMBERS;
        final int restarted = (leader + 2) % InProcessCluster.MEMBERS;
        final int count = 60_000;
        final long until;
        try (ConfirmProducer producer = new ConfirmProducer(cluster.aeronDir(member), member, count, PACING_NS)) {
            CompletableFuture<Void> restarting = null;
            long nextSafeCheckNs = 0;
            while (!producer.isDrained() && !producer.isStalled()) {
                producer.doWork();
                if (restarting == null && producer.sent() > count / 2 && System.nanoTime() > nextSafeCheckNs) {
                    nextSafeCheckNs = System.nanoTime() + SAFE_CHECK_NS;
                    if (!safeLog(restarted).safe().isEmpty()) {
                        restarting = CompletableFuture.runAsync(() -> {
                            cluster.stop(restarted);
                            cluster.start(restarted);
                        });
                    }
                }
                Tests.yield();
            }
            restarting.get(30, TimeUnit.SECONDS);
            assertTrue(producer.isDrained(), "the producer still held " + producer.held() + " frame(s)");
            until = producer.lastClusterSeqNo();
        }
        assertTrue(cluster.derivationStart(restarted).restored(),
                   "member " + restarted + " restarted from a cluster snapshot");
        final int markers = assertEachMembersQueues(member, until);
        assertTrue(markers >= 2, "the run crossed " + markers + " snapshot round(s), not two");
    }

    private RestartSafeLog.State safeLog(final int member) {
        return RestartSafeLog.read(baseDir.resolve("cluster-" + member).resolve(RestartSafeLog.FILE_NAME));
    }

    /**
     * @param reference the member whose recordings are whole: one the test did not restart
     * @return the snapshot markers in the reference's default queue up to {@code untilClusterSeqNo}
     */
    private int assertEachMembersQueues(final int reference, final long untilClusterSeqNo) {
        final List<List<Frame>> references = new ArrayList<>();
        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(cluster.aeronDir(reference)));
             AeronArchive archive = connectArchive(aeron)) {
            for (final int groupId : InProcessCluster.GROUP_IDS) {
                final Recorded recorded = readQueue(aeron, archive, "member " + reference + "'s queue of group " +
                                                    groupId, groupId, untilClusterSeqNo);
                assertEquals(0, recorded.startPosition(),
                             "member " + reference + "'s recording of group " + groupId + " is whole");
                references.add(recorded.frames());
            }
        }
        for (int m = 0; m < InProcessCluster.MEMBERS; m++) {
            assertMembersQueues(m, references, untilClusterSeqNo);
        }
        assertFalse(cluster.fatal(), "a member raised a fatal");
        assertFalse(cluster.terminated(), "a member terminated");
        int markers = 0;
        for (final Frame frame : references.get(InProcessCluster.GROUP_IDS.indexOf(InProcessCluster.GROUP_ID))) {
            if (isMarker(frame)) {
                markers++;
            }
        }
        return markers;
    }

    private void assertMembersQueues(final int member, final List<List<Frame>> references,
                                     final long untilClusterSeqNo) {
        final DerivationStart start = cluster.derivationStart(member);
        final Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(cluster.aeronDir(member)));
        final AeronArchive archive = connectArchive(aeron);
        try {
            for (int i = 0; i < InProcessCluster.GROUP_IDS.size(); i++) {
                final int groupId = InProcessCluster.GROUP_IDS.get(i);
                final String history = "member " + member + "'s Replayer history of group " + groupId;
                final List<Frame> served =
                    served(aeron, history, groupId, start.queueStarts()[groupId - 1], untilClusterSeqNo);
                if (start.restored()) {
                    assertTrue(isMarker(served.getFirst()), history + " opens with the restored round's marker");
                }
                assertFrom(references.get(i), served, history);
                assertCutsServed(aeron, history, groupId, served);

                final String queue = "member " + member + "'s queue of group " + groupId;
                assertFrom(served, readQueue(aeron, archive, queue, groupId, untilClusterSeqNo).frames(), queue);
            }
        } finally {
            CloseHelper.closeAll(archive, aeron);
        }
    }

    /**
     * Each marker in {@code history}, asked for as a restore's cut, must be where a replay of it opens: the position a
     * member's service recorded for its round, as its Replayer holds it, against the frame its queue agent offered.
     */
    private static void assertCutsServed(final Aeron aeron, final String what, final int groupId,
                                         final List<Frame> history) {
        final SequencedFrameDecoder decoder = new SequencedFrameDecoder();
        int cuts = 0;
        for (final Frame marker : history) {
            if (!isMarker(marker)) {
                continue;
            }
            decoder.wrap(new UnsafeBuffer(marker.bytes()), 0, marker.bytes().length);
            final long cut = decoder.clusterSeqNo();
            final Frame opened = cutServed(aeron, what + "'s replay from cut " + cut, groupId, cut);
            assertEquals(marker.position(), opened.position(), what + ": where the replay from cut " + cut + " opens");
            assertArrayEquals(marker.bytes(), opened.bytes(),
                              what + ": the frame the replay from cut " + cut + " opens on");
            cuts++;
        }
        System.out.printf("%s: %d cut(s) served at their markers%n", what, cuts);
    }

    /** The first frame of a restore's replay of a group's queue from {@code cutClusterSeqNo}. */
    private static Frame cutServed(final Aeron aeron, final String what, final int groupId,
                                   final long cutClusterSeqNo) {
        final Frames frames = new Frames(cutClusterSeqNo);
        final long deadline = System.nanoTime() + TIMEOUT_NS;
        try (Subscription control =
                 aeron.addSubscription(ReplayProtocol.IPC_CHANNEL + "|tether=false", ReplayProtocol.CONTROL_STREAM_ID);
             Publication requests =
                 aeron.addPublication(ReplayProtocol.IPC_CHANNEL, ReplayProtocol.REQUEST_STREAM_ID)) {
            final long[] replay = QueueFrames.ask(control, requests, aeron.nextCorrelationId(), 1, groupId,
                                                  ReplayProtocol.FROM_START, cutClusterSeqNo, deadline, what);
            assertTrue(replay[0] != ReplayProtocol.NO_REPLAY_NEEDED, what + " was refused");
            try (Subscription subscription = aeron.addSubscription(
                     ReplayProtocol.IPC_CHANNEL + "|session-id=" + (int)replay[0], ReplayProtocol.REPLAY_STREAM_ID)) {
                while (frames.list.isEmpty()) {
                    if (subscription.poll(frames, 1) == 0) {
                        assertTrue(System.nanoTime() < deadline, what + " delivered no frame");
                        Tests.yield();
                    }
                }
            }
        }
        return frames.list.getFirst();
    }

    /** {@code actual} must be {@code expected} from {@code actual}'s first frame on, frame for frame and position. */
    private static void assertFrom(final List<Frame> expected, final List<Frame> actual, final String what) {
        int from = 0;
        while (from < expected.size() && expected.get(from).position() < actual.getFirst().position()) {
            from++;
        }
        assertTrue(from < expected.size() && expected.get(from).position() == actual.getFirst().position(),
                   what + " starts at " + actual.getFirst().position() + ", where no frame of its reference starts");
        assertEquals(expected.size() - from, actual.size(),
                     what + " holds " + actual.size() + " frame(s), its reference " + (expected.size() - from));
        for (int i = from; i < expected.size(); i++) {
            assertEquals(expected.get(i).position(), actual.get(i - from).position(),
                         what + ": the position of frame " + (i - from + 1));
            assertArrayEquals(expected.get(i).bytes(), actual.get(i - from).bytes(),
                              what + ": frame " + (i - from + 1));
        }
        System.out.printf("%s: %d frame(s) byte for byte from position %d%n", what, actual.size(),
                          actual.getFirst().position());
    }

    private static boolean isMarker(final Frame frame) {
        final SequencedFrameDecoder decoder = new SequencedFrameDecoder();
        return decoder.wrap(new UnsafeBuffer(frame.bytes()), 0, frame.bytes().length) && decoder.isSystem() &&
            decoder.systemEventType() == SystemFrame.SNAPSHOT_STARTED;
    }

    private static AeronArchive connectArchive(final Aeron aeron) {
        return AeronArchive.connect(new AeronArchive.Context()
                                        .aeron(aeron)
                                        .ownsAeronClient(false)
                                        .controlRequestChannel(PortLayout.ARCHIVE_CONTROL_CHANNEL)
                                        .controlRequestStreamId(PortLayout.ARCHIVE_CONTROL_STREAM_ID)
                                        .controlResponseChannel(PortLayout.ARCHIVE_CONTROL_CHANNEL)
                                        .controlResponseStreamId(RESPONSE_STREAM_ID));
    }

    /** A group's queue as its active recording holds it, to the first frame at or past {@code untilClusterSeqNo}. */
    private static Recorded readQueue(final Aeron aeron, final AeronArchive archive, final String what,
                                      final int groupId, final long untilClusterSeqNo) {
        final Span span = awaitActive(archive, FrameLayer.queueStreamId(groupId), what);
        final Frames frames = new Frames(untilClusterSeqNo);
        final long session = archive.startReplay(span.recordingId(), span.startPosition(), AeronArchive.NULL_LENGTH,
                                                 REPLAY_CHANNEL, QUEUE_REPLAY_STREAM_ID);
        final long deadline = System.nanoTime() + TIMEOUT_NS;
        try (Subscription subscription =
                 aeron.addSubscription(ChannelUri.addSessionId(REPLAY_CHANNEL, (int)session), QUEUE_REPLAY_STREAM_ID)) {
            while (!frames.isDone()) {
                if (subscription.poll(frames, 100) == 0) {
                    if (System.nanoTime() > deadline) {
                        fail(what + "'s recording reached clusterSeqNo " + frames.clusterSeqNo + " of " +
                             untilClusterSeqNo);
                    }
                    Tests.yield();
                }
            }
        } finally {
            archive.stopReplay(session);
        }
        return new Recorded(span.startPosition(), frames.list);
    }

    /**
     * A group's queue as the member's Replayer serves it, from {@code from}, a frame's start, to the first frame at or
     * past {@code untilClusterSeqNo}: a replay to its catch-up position, then another from the last frame read, which
     * opens with that frame again.
     */
    private static List<Frame> served(final Aeron aeron, final String what, final int groupId, final long from,
                                      final long untilClusterSeqNo) {
        final Frames frames = new Frames(untilClusterSeqNo);
        final long clientId = aeron.nextCorrelationId();
        final long deadline = System.nanoTime() + TIMEOUT_NS;
        long requestId = 0;
        try (Subscription control =
                 aeron.addSubscription(ReplayProtocol.IPC_CHANNEL + "|tether=false", ReplayProtocol.CONTROL_STREAM_ID);
             Publication requests =
                 aeron.addPublication(ReplayProtocol.IPC_CHANNEL, ReplayProtocol.REQUEST_STREAM_ID)) {
            while (!frames.isDone()) {
                final String reached =
                    what + " reached clusterSeqNo " + frames.clusterSeqNo + " of " + untilClusterSeqNo;
                final long position = frames.list.isEmpty() ? from : frames.list.getLast().position();
                final long[] replay =
                    QueueFrames.ask(control, requests, clientId, ++requestId, groupId, position, deadline, reached);
                if (replay[0] == ReplayProtocol.NO_REPLAY_NEEDED) {
                    assertTrue(System.nanoTime() < deadline, reached);
                    Tests.sleep(50);
                    continue;
                }
                try (Subscription subscription =
                         aeron.addSubscription(ReplayProtocol.IPC_CHANNEL + "|session-id=" + (int)replay[0],
                                               ReplayProtocol.REPLAY_STREAM_ID)) {
                    Image image = null;
                    while (!frames.isDone() && (image == null || image.position() < replay[1])) {
                        image = image == null ? subscription.imageBySessionId((int)replay[0]) : image;
                        if ((image == null ? 0 : image.poll(frames, 100)) == 0) {
                            assertTrue(System.nanoTime() < deadline, reached);
                            Tests.yield();
                        }
                    }
                }
            }
        }
        return frames.list;
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
                        active.add(new Span(recordingId, startPosition));
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

    /**
     * A queue's frames from a stream's fragments: from the first whole frame, each once, in position order, to the
     * first frame at or past {@code untilClusterSeqNo}.
     */
    private static final class Frames implements FragmentHandler {
        final List<Frame> list = new ArrayList<>();
        long clusterSeqNo;
        private final long untilClusterSeqNo;
        private final ExpandableArrayBuffer assembled = new ExpandableArrayBuffer(4096);
        private final SequencedFrameDecoder decoder = new SequencedFrameDecoder();
        private long start;
        private int length = -1; // -1 before a frame begins

        Frames(final long untilClusterSeqNo) {
            this.untilClusterSeqNo = untilClusterSeqNo;
        }

        boolean isDone() {
            return clusterSeqNo >= untilClusterSeqNo;
        }

        @Override
        public void onFragment(final DirectBuffer buffer, final int offset, final int fragmentLength,
                               final Header header) {
            if ((header.flags() & FrameDescriptor.BEGIN_FRAG_FLAG) != 0) {
                start = LogBufferDescriptor.computePosition(header.termId(), header.termOffset(),
                                                            header.positionBitsToShift(), header.initialTermId());
                length = 0;
            }
            if (length < 0) {
                return; // the stream started inside a frame
            }
            assembled.putBytes(length, buffer, offset, fragmentLength);
            length += fragmentLength;
            if ((header.flags() & FrameDescriptor.END_FRAG_FLAG) != 0) {
                if (!isDone() && (list.isEmpty() || start > list.getLast().position())) {
                    final byte[] bytes = new byte[length];
                    assembled.getBytes(0, bytes);
                    list.add(new Frame(start, bytes));
                    if (decoder.wrap(assembled, 0, length)) {
                        clusterSeqNo = decoder.clusterSeqNo();
                    }
                }
                length = -1;
            }
        }
    }
}
