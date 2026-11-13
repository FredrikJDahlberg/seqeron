package org.limitless.seqeron.tools;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.aeron.Aeron;
import io.aeron.ChannelUri;
import io.aeron.FragmentAssembler;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.test.Tests;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.agrona.CloseHelper;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.limitless.seqeron.protocol.FrameLayer;
import org.limitless.seqeron.protocol.PortLayout;
import org.limitless.seqeron.protocol.ReplayProtocol;
import org.limitless.seqeron.protocol.SequencedFrameDecoder;
import org.limitless.seqeron.sbe.replay.MessageHeaderDecoder;
import org.limitless.seqeron.sbe.replay.MessageHeaderEncoder;
import org.limitless.seqeron.sbe.replay.ReplayRequestEncoder;
import org.limitless.seqeron.sbe.replay.ReplayingDecoder;

/**
 * A few frames of a queue read off an archive's recording of it, and the recordings it holds. A member records no
 * queue, so on a member's archive the frames are read through its Replayer, as a client reads them.
 */
final class QueueFrames {
    private static final int RESPONSE_STREAM_ID = 124; // beside QueueRecordings' and NodeDriver's
    private static final String REPLAY_CHANNEL = "aeron:ipc";
    private static final int REPLAY_STREAM_ID = 1206;
    private static final long TIMEOUT_NS = TimeUnit.SECONDS.toNanos(30);

    /** One recording of a queue. */
    record Span(long recordingId, long startPosition, boolean active) { }

    /** One frame: where it starts, and what it carries. */
    record Frame(long position, long clusterSeqNo, long groupSeqNo, boolean system, int systemEventType,
                 long timestampNs) { }

    private QueueFrames() {
    }

    /** Every recording of a group's queue on the archive behind {@code aeronDir}. */
    static List<Span> recordings(final String aeronDir, final int groupId) {
        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDir));
             AeronArchive archive = connect(aeron)) {
            return spans(archive, groupId);
        }
    }

    /**
     * The first {@code count} frames of a group's queue from {@code position}, a frame's start, waited for: off the
     * archive's active recording of it, or through the Replayer where the archive records none.
     */
    static List<Frame> readFrom(final String aeronDir, final int groupId, final long position, final int count) {
        final Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDir));
        final AeronArchive archive = connect(aeron);
        try {
            final Span active = spans(archive, groupId).stream().filter(Span::active).findFirst().orElse(null);
            if (active == null) {
                return served(aeron, groupId, position, count);
            }
            final long session = archive.startReplay(active.recordingId(), position, AeronArchive.NULL_LENGTH,
                                                     REPLAY_CHANNEL, REPLAY_STREAM_ID);
            final List<Frame> frames = new ArrayList<>();
            final SequencedFrameDecoder decoder = new SequencedFrameDecoder();
            final FragmentAssembler handler = new FragmentAssembler((buffer, offset, length, header) -> {
                if (frames.size() < count && decoder.wrap(buffer, offset, length)) {
                    frames.add(new Frame(LogBufferDescriptor.computePosition(header.termId(), header.termOffset(),
                                                                             header.positionBitsToShift(),
                                                                             header.initialTermId()),
                                         decoder.clusterSeqNo(), decoder.groupSeqNo(), decoder.isSystem(),
                                         decoder.isSystem() ? decoder.systemEventType() : -1,
                                         decoder.clusterTimestampNs()));
                }
            });
            final long deadline = System.nanoTime() + TIMEOUT_NS;
            try (Subscription subscription =
                     aeron.addSubscription(ChannelUri.addSessionId(REPLAY_CHANNEL, (int)session), REPLAY_STREAM_ID)) {
                while (frames.size() < count) {
                    assertTrue(System.nanoTime() < deadline, "the recording held " + frames.size() + " frame(s)");
                    if (subscription.poll(handler, 10) == 0) {
                        Tests.yield();
                    }
                }
            } finally {
                archive.stopReplay(session);
            }
            return frames;
        } finally {
            CloseHelper.closeAll(archive, aeron);
        }
    }

    /**
     * As {@link #readFrom}, through the Replayer on {@code aeron}'s driver: each replay is read to its end, and asked
     * again from the last frame read until {@code count} have come. A replay from a frame opens with that frame.
     */
    private static List<Frame> served(final Aeron aeron, final int groupId, final long position, final int count) {
        final List<Frame> frames = new ArrayList<>();
        final SequencedFrameDecoder decoder = new SequencedFrameDecoder();
        final long[] from = { position };
        final boolean[] repeated = { false };
        final FragmentAssembler handler = new FragmentAssembler((buffer, offset, length, header) -> {
            if (repeated[0]) {
                repeated[0] = false;
                return;
            }
            if (frames.size() < count && decoder.wrap(buffer, offset, length)) {
                from[0] = LogBufferDescriptor.computePosition(header.termId(), header.termOffset(),
                                                              header.positionBitsToShift(), header.initialTermId());
                frames.add(new Frame(from[0], decoder.clusterSeqNo(), decoder.groupSeqNo(), decoder.isSystem(),
                                     decoder.isSystem() ? decoder.systemEventType() : -1,
                                     decoder.clusterTimestampNs()));
            }
        });
        final long clientId = aeron.nextCorrelationId();
        final long deadline = System.nanoTime() + TIMEOUT_NS;
        long requestId = 0;
        try (Subscription control =
                 aeron.addSubscription(ReplayProtocol.IPC_CHANNEL + "|tether=false", ReplayProtocol.CONTROL_STREAM_ID);
             Publication requests =
                 aeron.addPublication(ReplayProtocol.IPC_CHANNEL, ReplayProtocol.REQUEST_STREAM_ID)) {
            while (frames.size() < count) {
                final String reached = "the Replayer served " + frames.size() + " of " + count + " frame(s) of group " +
                                       groupId + " from " + position;
                final long[] replay =
                    ask(control, requests, clientId, ++requestId, groupId, from[0], deadline, reached);
                if (replay[0] == ReplayProtocol.NO_REPLAY_NEEDED) {
                    assertTrue(System.nanoTime() < deadline, reached);
                    Tests.sleep(50);
                    continue;
                }
                repeated[0] = !frames.isEmpty();
                try (Subscription subscription =
                         aeron.addSubscription(ReplayProtocol.IPC_CHANNEL + "|session-id=" + (int)replay[0],
                                               ReplayProtocol.REPLAY_STREAM_ID)) {
                    Image image = null;
                    while (image == null || image.position() < replay[1]) {
                        image = image == null ? subscription.imageBySessionId((int)replay[0]) : image;
                        if ((image == null ? 0 : image.poll(handler, 100)) == 0) {
                            assertTrue(System.nanoTime() < deadline, reached);
                            Tests.yield();
                        }
                    }
                }
            }
        }
        return frames;
    }

    /** Sends a replay request, and again every 500 ms, until it is answered with a replay: its session and end. */
    static long[] ask(final Subscription control, final Publication requests, final long clientId, final long requestId,
                      final int groupId, final long fromPosition, final long deadline, final String reached) {
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

    private static List<Span> spans(final AeronArchive archive, final int groupId) {
        final List<Span> spans = new ArrayList<>();
        archive.listRecordingsForUri(
            0, Integer.MAX_VALUE, "", FrameLayer.queueStreamId(groupId),
            (controlSessionId, correlationId, recordingId, startTimestamp, stopTimestamp, startPosition, stopPosition,
             initialTermId, segmentFileLength, termBufferLength, mtuLength, sessionId, streamId, strippedChannel,
             originalChannel, sourceIdentity)
                -> spans.add(new Span(recordingId, startPosition, stopTimestamp == AeronArchive.NULL_TIMESTAMP)));
        return spans;
    }

    private static AeronArchive connect(final Aeron aeron) {
        return AeronArchive.connect(new AeronArchive.Context()
                                        .aeron(aeron)
                                        .ownsAeronClient(false)
                                        .controlRequestChannel(PortLayout.ARCHIVE_CONTROL_CHANNEL)
                                        .controlRequestStreamId(PortLayout.ARCHIVE_CONTROL_STREAM_ID)
                                        .controlResponseChannel(PortLayout.ARCHIVE_CONTROL_CHANNEL)
                                        .controlResponseStreamId(RESPONSE_STREAM_ID));
    }
}
