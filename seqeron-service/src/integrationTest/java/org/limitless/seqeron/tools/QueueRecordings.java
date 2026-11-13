package org.limitless.seqeron.tools;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import io.aeron.Aeron;
import io.aeron.ChannelUri;
import io.aeron.FragmentAssembler;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ConsensusModule;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.test.Tests;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32C;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.LongArrayList;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.status.CountersReader;
import org.limitless.seqeron.protocol.FrameLayer;
import org.limitless.seqeron.protocol.PortLayout;
import org.limitless.seqeron.protocol.ReplayProtocol;
import org.limitless.seqeron.protocol.SequencedFrameDecoder;
import org.limitless.seqeron.sbe.replay.MessageHeaderDecoder;
import org.limitless.seqeron.sbe.replay.MessageHeaderEncoder;
import org.limitless.seqeron.sbe.replay.ReplayRequestEncoder;
import org.limitless.seqeron.sbe.replay.ReplayingDecoder;
import org.limitless.seqeron.sequencer.LogDeriver;
import org.limitless.seqeron.sequencer.Sequencer;

/**
 * The recordings of the in-process cluster's queues on each archive, read straight off the archive rather than through
 * {@code ReplayerRecovery}, which drops a repeated frame and replays over a gap: what a client sees can be whole while
 * a recording is not. A member that records no queue ({@code replayer.deriveFromLog}) is read twice, each as one
 * active recording per queue: what its Raft log derives, bounded by its commit position, and what its Replayer
 * replays.
 */
final class QueueRecordings {
    private static final int RESPONSE_STREAM_ID = 123; // beside NodeDriver's archive response streams
    private static final String REPLAY_CHANNEL = "aeron:ipc";
    private static final int REPLAY_STREAM_ID = 1205;
    private static final int LOG_REPLAY_STREAM_ID = 1206;
    private static final long TIMEOUT_NS = TimeUnit.SECONDS.toNanos(30);

    /** One recording of a group's queue: a digest of each frame it holds, groupSeqNo 1 first. */
    record Recording(String archive, int groupId, long recordingId, boolean active, LongArrayList digests) { }

    private record Span(long recordingId, long startPosition, boolean active) { }

    private QueueRecordings() {
    }

    /**
     * Every recording of each group's queue on every archive is one log: each holds groupSeqNo 1 onwards, each once
     * and in order, each archive's active recording holds every frame up to {@code untilClusterSeqNo}, and every
     * recording of a queue holding a groupSeqNo holds the same frame for it.
     * @param aeronDirs each archive's driver directory, by the name a failure gives it
     * @param untilClusterSeqNo the clusterSeqNo each active recording must reach, waited for
     */
    static void assertOneLog(final Map<String, String> aeronDirs, final long untilClusterSeqNo) {
        final List<Recording> recordings = new ArrayList<>();
        aeronDirs.forEach((name, aeronDir) -> recordings.addAll(read(name, aeronDir, untilClusterSeqNo)));
        assertEachQueueOneLog(recordings);
    }

    /**
     * As {@link #assertOneLog(Map, long)}, over the active recordings alone: a member restarted after the others
     * discarded entries it applied holds one log again only from its restart.
     */
    static void assertActiveOneLog(final Map<String, String> aeronDirs, final long untilClusterSeqNo) {
        final List<Recording> recordings = new ArrayList<>();
        aeronDirs.forEach((name, aeronDir) -> recordings.addAll(read(name, aeronDir, untilClusterSeqNo)));
        recordings.removeIf(recording -> !recording.active());
        assertEachQueueOneLog(recordings);
    }

    private static void assertEachQueueOneLog(final List<Recording> recordings) {
        for (final int groupId : InProcessCluster.GROUP_IDS) {
            assertOneLog(recordings.stream().filter(recording -> recording.groupId() == groupId).toList());
        }
    }

    private static void assertOneLog(final List<Recording> recordings) {
        Recording longest = recordings.get(0);
        for (final Recording recording : recordings) {
            if (recording.digests.size() > longest.digests.size()) {
                longest = recording;
            }
        }
        for (final Recording recording : recordings) {
            for (int i = 0; i < recording.digests.size(); i++) {
                if (recording.digests.getLong(i) != longest.digests.getLong(i)) {
                    fail(describe(recording) + " and " + describe(longest) + " hold different frames at groupSeqNo " +
                         (i + 1));
                }
            }
        }
    }

    /** Each queue's recordings on the archive behind {@code aeronDir}: stopped ones whole, active ones to the given. */
    private static List<Recording> read(final String name, final String aeronDir, final long untilClusterSeqNo) {
        final Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDir));
        final AeronArchive archive =
            AeronArchive.connect(new AeronArchive.Context()
                                     .aeron(aeron)
                                     .ownsAeronClient(false)
                                     .controlRequestChannel(PortLayout.ARCHIVE_CONTROL_CHANNEL)
                                     .controlRequestStreamId(PortLayout.ARCHIVE_CONTROL_STREAM_ID)
                                     .controlResponseChannel(PortLayout.ARCHIVE_CONTROL_CHANNEL)
                                     .controlResponseStreamId(RESPONSE_STREAM_ID));
        try {
            final List<Recording> recordings = new ArrayList<>();
            if (commitPositionCounterId(aeron) >= 0) {
                recordings.addAll(derive(name, aeron, archive, untilClusterSeqNo));
                for (final int groupId : InProcessCluster.GROUP_IDS) {
                    recordings.add(serve(name, aeron, groupId, untilClusterSeqNo));
                }
                return recordings;
            }
            for (final int groupId : InProcessCluster.GROUP_IDS) {
                for (final Span span : awaitSpans(name, archive, groupId)) {
                    recordings.add(readOne(name, aeron, archive, groupId, span, untilClusterSeqNo));
                }
            }
            return recordings;
        } finally {
            CloseHelper.closeAll(archive, aeron);
        }
    }

    /**
     * Each recording of a group's queue, once exactly one is active: a restarted member's
     * service starts its recording after {@code launch} returns.
     */
    private static List<Span> awaitSpans(final String name, final AeronArchive archive, final int groupId) {
        final long deadline = System.nanoTime() + TIMEOUT_NS;
        while (true) {
            final List<Span> spans = new ArrayList<>();
            archive.listRecordingsForUri(
                0, Integer.MAX_VALUE, "", FrameLayer.queueStreamId(groupId),
                (controlSessionId, correlationId, recordingId, startTimestamp, stopTimestamp, startPosition,
                 stopPosition, initialTermId, segmentFileLength, termBufferLength, mtuLength, sessionId, streamId,
                 strippedChannel, originalChannel, sourceIdentity)
                    -> spans.add(new Span(recordingId, startPosition, stopTimestamp == AeronArchive.NULL_TIMESTAMP)));
            final long active = spans.stream().filter(Span::active).count();
            if (active == 1) {
                return spans;
            }
            if (System.nanoTime() > deadline) {
                assertEquals(1, active, name + "'s archive holds no single active recording of group " + groupId);
            }
            Tests.sleep(50);
        }
    }

    private static Recording readOne(final String name, final Aeron aeron, final AeronArchive archive,
                                     final int groupId, final Span span, final long untilClusterSeqNo) {
        final long recordingId = span.recordingId();
        final long startPosition = span.startPosition();
        final boolean active = span.active();
        final Reader reader = new Reader(new Recording(name, groupId, recordingId, active, new LongArrayList()));
        final long stopPosition = active ? AeronArchive.NULL_POSITION : archive.getStopPosition(recordingId);
        if (!active && stopPosition == startPosition) {
            return reader.recording;
        }
        final long length = active ? AeronArchive.NULL_LENGTH : stopPosition - startPosition;
        final long replaySessionId =
            archive.startReplay(recordingId, startPosition, length, REPLAY_CHANNEL, REPLAY_STREAM_ID);
        final FragmentAssembler assembler = new FragmentAssembler(reader);
        final long deadline = System.nanoTime() + TIMEOUT_NS;
        try (Subscription subscription = aeron.addSubscription(
                 ChannelUri.addSessionId(REPLAY_CHANNEL, (int)replaySessionId), REPLAY_STREAM_ID)) {
            while (active ? reader.lastClusterSeqNo < untilClusterSeqNo : !isReplayed(subscription, stopPosition)) {
                final int fragments = subscription.poll(assembler, 100);
                if (reader.error != null) {
                    fail(reader.error);
                }
                if (fragments == 0) {
                    if (System.nanoTime() > deadline) {
                        fail(describe(reader.recording) + " reached clusterSeqNo " + reader.lastClusterSeqNo + " of " +
                             (active ? untilClusterSeqNo : "its stop position"));
                    }
                    Tests.yield();
                }
            }
        } finally {
            if (active) {
                archive.stopReplay(replaySessionId); // a bounded replay stops itself
            }
        }
        return reader.recording;
    }

    /** Each queue as a member's log derives it, from the log's start to {@code untilClusterSeqNo}. */
    private static List<Recording> derive(final String name, final Aeron aeron, final AeronArchive archive,
                                          final long untilClusterSeqNo) {
        final Reader[] readers = new Reader[FrameLayer.MAX_GROUPS];
        for (final int groupId : InProcessCluster.GROUP_IDS) {
            readers[groupId - 1] = new Reader(new Recording(name, groupId, -1, true, new LongArrayList()));
        }
        final LogDeriver deriver = new LogDeriver(0, new Sequencer.FrameSink() {
            @Override
            public void activate(final int[] groupIds) {
            }

            @Override
            public void publish(final int groupId, final DirectBuffer buffer, final int length) {
                readers[groupId - 1].onFragment(buffer, 0, length, null);
            }
        });
        final FragmentAssembler entries =
            new FragmentAssembler((buffer, offset, length, header) -> deriver.onLogEntry(buffer, offset, length));
        final long deadline = System.nanoTime() + TIMEOUT_NS;
        final long[] log = awaitActiveLog(name, archive, deadline);
        assertEquals(0, log[1], name + "'s log does not start at its first entry");
        final long replaySessionId = archive.startBoundedReplay(
            log[0], 0, AeronArchive.NULL_LENGTH, commitPositionCounterId(aeron), REPLAY_CHANNEL, LOG_REPLAY_STREAM_ID);
        try (Subscription subscription = aeron.addSubscription(
                 ChannelUri.addSessionId(REPLAY_CHANNEL, (int)replaySessionId), LOG_REPLAY_STREAM_ID)) {
            while (deriver.clusterSeqNo() < untilClusterSeqNo) {
                final int fragments = subscription.poll(entries, 100);
                for (final int groupId : InProcessCluster.GROUP_IDS) {
                    if (readers[groupId - 1].error != null) {
                        fail(readers[groupId - 1].error);
                    }
                }
                if (fragments == 0) {
                    if (System.nanoTime() > deadline) {
                        fail(name + "'s log derived clusterSeqNo " + deriver.clusterSeqNo() + " of " +
                             untilClusterSeqNo);
                    }
                    Tests.yield();
                }
            }
        } finally {
            archive.stopReplay(replaySessionId);
        }
        final List<Recording> recordings = new ArrayList<>();
        for (final int groupId : InProcessCluster.GROUP_IDS) {
            recordings.add(readers[groupId - 1].recording);
        }
        return recordings;
    }

    /**
     * A group's queue as a member's Replayer serves it, from its start to {@code untilClusterSeqNo}: a replay to its
     * catch-up position, then another from the last frame read, until the queue has reached it.
     */
    private static Recording serve(final String name, final Aeron aeron, final int groupId,
                                   final long untilClusterSeqNo) {
        final String replayer = name + "'s Replayer";
        final Reader reader = new Reader(new Recording(replayer, groupId, -1, true, new LongArrayList()));
        final long[] lastPosition = { ReplayProtocol.FROM_START };
        final boolean[] repeated = { false };
        final FragmentAssembler frames = new FragmentAssembler((buffer, offset, length, header) -> {
            if (repeated[0]) {
                repeated[0] = false; // a resumed replay opens with the frame it resumes from
                return;
            }
            lastPosition[0] = LogBufferDescriptor.computePosition(header.termId(), header.termOffset(),
                                                                  header.positionBitsToShift(), header.initialTermId());
            reader.onFragment(buffer, offset, length, header);
        });
        final long clientId = aeron.nextCorrelationId();
        final long deadline = System.nanoTime() + TIMEOUT_NS;
        long requestId = 0;
        try (Subscription control =
                 aeron.addSubscription(ReplayProtocol.IPC_CHANNEL + "|tether=false", ReplayProtocol.CONTROL_STREAM_ID);
             Publication requests =
                 aeron.addPublication(ReplayProtocol.IPC_CHANNEL, ReplayProtocol.REQUEST_STREAM_ID)) {
            while (reader.lastClusterSeqNo < untilClusterSeqNo) {
                final String reached = describe(reader.recording) + " reached clusterSeqNo " + reader.lastClusterSeqNo +
                    " of " + untilClusterSeqNo;
                final long[] replay =
                    ask(control, requests, clientId, ++requestId, groupId, lastPosition[0], deadline, reached);
                if (replay[0] == ReplayProtocol.NO_REPLAY_NEEDED) {
                    Tests.sleep(50);
                    continue;
                }
                repeated[0] = lastPosition[0] != ReplayProtocol.FROM_START;
                try (Subscription subscription =
                         aeron.addSubscription(ReplayProtocol.IPC_CHANNEL + "|session-id=" + (int)replay[0],
                                               ReplayProtocol.REPLAY_STREAM_ID)) {
                    Image image = null;
                    while (image == null || image.position() < replay[1]) {
                        image = image == null ? subscription.imageBySessionId((int)replay[0]) : image;
                        final int fragments = image == null ? 0 : image.poll(frames, 100);
                        if (reader.error != null) {
                            fail(reader.error);
                        }
                        if (fragments == 0) {
                            if (System.nanoTime() > deadline) {
                                fail(reached);
                            }
                            Tests.yield();
                        }
                    }
                }
            }
        }
        return reader.recording;
    }

    /** Sends a replay request, and again every 500 ms, until it is answered with a replay: its session and end. */
    private static long[] ask(final Subscription control, final Publication requests, final long clientId,
                              final long requestId, final int groupId, final long fromPosition, final long deadline,
                              final String reached) {
        final MutableDirectBuffer buffer = new ExpandableArrayBuffer(64);
        final ReplayRequestEncoder request = new ReplayRequestEncoder();
        request.wrapAndApplyHeader(buffer, 0, new MessageHeaderEncoder())
            .clientId(clientId)
            .requestId(requestId)
            .fromPosition(fromPosition)
            .queueId(groupId)
            .cutClusterSeqNo(ReplayProtocol.NO_CUT);
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + request.encodedLength();
        final MessageHeaderDecoder header = new MessageHeaderDecoder();
        final ReplayingDecoder replaying = new ReplayingDecoder();
        final long[] replay = { 0, 0 };
        final boolean[] answered = { false };
        final FragmentHandler onReply = (buf, offset, len, unused) -> {
            header.wrap(buf, offset);
            if (header.templateId() == ReplayingDecoder.TEMPLATE_ID) {
                replaying.wrap(buf, offset + MessageHeaderDecoder.ENCODED_LENGTH, header.blockLength(),
                               header.version());
                if (replaying.clientId() == clientId && replaying.requestId() == requestId) {
                    replay[0] = replaying.replaySessionId();
                    replay[1] = replaying.catchUpPosition();
                    answered[0] = true;
                }
            }
        };
        long resendAt = 0;
        while (!answered[0]) {
            if (System.nanoTime() > deadline) {
                fail(reached + ": no replay answered");
            }
            if (System.nanoTime() >= resendAt) {
                requests.offer(buffer, 0, length);
                resendAt = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(500);
            }
            if (control.poll(onReply, 10) == 0) {
                Tests.sleep(1);
            }
        }
        return replay;
    }

    /** The id and start of the log's active recording: a restarted member's consensus module extends it. */
    private static long[] awaitActiveLog(final String name, final AeronArchive archive, final long deadline) {
        final long[][] active = { null };
        while (true) {
            archive.listRecordingsForUri(0, Integer.MAX_VALUE, "", ConsensusModule.Configuration.LOG_STREAM_ID_DEFAULT,
                                         (controlSessionId, correlationId, recordingId, startTimestamp, stopTimestamp,
                                          startPosition, stopPosition, initialTermId, segmentFileLength,
                                          termBufferLength, mtuLength, sessionId, streamId, strippedChannel,
                                          originalChannel, sourceIdentity) -> {
                                             if (stopTimestamp == AeronArchive.NULL_TIMESTAMP) {
                                                 active[0] = new long[] { recordingId, startPosition };
                                             }
                                         });
            if (active[0] != null) {
                return active[0];
            }
            if (System.nanoTime() > deadline) {
                fail(name + "'s archive is not recording a log");
            }
            Tests.sleep(50);
        }
    }

    /** The driver's commit position counter, or -1 on a gateway host, which has no consensus module. */
    private static int commitPositionCounterId(final Aeron aeron) {
        final MutableInteger id = new MutableInteger(CountersReader.NULL_COUNTER_ID);
        aeron.countersReader().forEach((counterId, typeId, keyBuffer, label) -> {
            if (typeId == ConsensusModule.Configuration.COMMIT_POSITION_TYPE_ID) {
                id.set(counterId);
            }
        });
        return id.get();
    }

    private static boolean isReplayed(final Subscription subscription, final long stopPosition) {
        final Image image = subscription.imageCount() == 0 ? null : subscription.imageAtIndex(0);
        return image != null && image.position() >= stopPosition;
    }

    private static String describe(final Recording recording) {
        return recording.archive + "'s " + (recording.active ? "active" : "stopped") + " recording " +
            recording.recordingId + " of group " + recording.groupId;
    }

    /** Checks each frame is the next groupSeqNo and digests it; records a failure, since a poll swallows a throw. */
    private static final class Reader implements FragmentHandler {
        final Recording recording;
        final SequencedFrameDecoder frame = new SequencedFrameDecoder();
        final CRC32C crc = new CRC32C();
        byte[] bytes = new byte[0];
        long next = 1;
        long lastClusterSeqNo;
        String error;

        Reader(final Recording recording) {
            this.recording = recording;
        }

        @Override
        public void onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header) {
            if (error != null) {
                return;
            }
            if (!frame.wrap(buffer, offset, length)) {
                error = describe(recording) + " holds a frame that is not sequenced after groupSeqNo " + (next - 1);
                return;
            }
            if (frame.groupSeqNo() != next) {
                error = describe(recording) + " holds groupSeqNo " + frame.groupSeqNo() + " where " + next + " belongs";
                return;
            }
            if (bytes.length < length) {
                bytes = new byte[length];
            }
            buffer.getBytes(offset, bytes, 0, length);
            crc.reset();
            crc.update(bytes, 0, length);
            recording.digests.addLong(crc.getValue() << 32 | length); // the frame itself would not fit a long run
            lastClusterSeqNo = frame.clusterSeqNo();
            next++;
        }
    }
}
