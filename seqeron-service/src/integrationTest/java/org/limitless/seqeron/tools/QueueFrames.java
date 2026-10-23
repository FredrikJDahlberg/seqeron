package org.limitless.seqeron.tools;

import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.Aeron;
import io.aeron.ChannelUri;
import io.aeron.FragmentAssembler;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.test.Tests;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.agrona.CloseHelper;
import org.limitless.seqeron.protocol.FrameLayer;
import org.limitless.seqeron.protocol.PortLayout;
import org.limitless.seqeron.protocol.SequencedFrameDecoder;

/** A few frames of a queue recording read off an archive, and the recordings it holds. */
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
     * The first {@code count} frames of the active recording of a group's queue from {@code position}, a frame boundary,
     * waited for.
     */
    static List<Frame> readFrom(final String aeronDir, final int groupId, final long position, final int count) {
        final Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDir));
        final AeronArchive archive = connect(aeron);
        try {
            final Span active = spans(archive, groupId).stream().filter(Span::active).findFirst().orElseThrow();
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
