package org.limitless.seqeron.tools;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import io.aeron.Aeron;
import io.aeron.ChannelUri;
import io.aeron.FragmentAssembler;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import io.aeron.test.Tests;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32C;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.collections.LongArrayList;
import org.limitless.seqeron.protocol.FrameLayer;
import org.limitless.seqeron.protocol.PortLayout;
import org.limitless.seqeron.protocol.SequencedFrameDecoder;

/**
 * The queue recordings on each archive, read straight off the archive rather than through {@code ReplayerRecovery},
 * which drops a repeated frame and replays over a gap: what a client sees can be whole while a recording is not.
 */
final class QueueRecordings {
    private static final int RESPONSE_STREAM_ID = 123; // beside NodeDriver's archive response streams
    private static final String REPLAY_CHANNEL = "aeron:ipc";
    private static final int REPLAY_STREAM_ID = 1205;
    private static final long TIMEOUT_NS = TimeUnit.SECONDS.toNanos(30);

    /** One recording: a digest of each frame it holds, clusterSeqNo 1 first. */
    record Recording(String archive, long recordingId, boolean active, LongArrayList digests) { }

    private record Span(long recordingId, long startPosition, boolean active) { }

    private QueueRecordings() {
    }

    /**
     * Every queue recording on every archive is one log: each holds clusterSeqNo 1 onwards, each once and in order,
     * each archive's active recording holds at least 1..{@code untilClusterSeqNo}, and every recording holding a
     * clusterSeqNo holds the same frame for it.
     * @param aeronDirs each archive's driver directory, by the name a failure gives it
     * @param untilClusterSeqNo the clusterSeqNo each active recording must reach, waited for
     */
    static void assertOneLog(final Map<String, String> aeronDirs, final long untilClusterSeqNo) {
        final List<Recording> recordings = new ArrayList<>();
        aeronDirs.forEach((name, aeronDir) -> recordings.addAll(read(name, aeronDir, untilClusterSeqNo)));
        assertOneLog(recordings);
    }

    /**
     * As {@link #assertOneLog(Map, long)}, over the active recordings alone: a member restarted after the others
     * discarded entries it applied holds one log again only from its restart.
     */
    static void assertActiveOneLog(final Map<String, String> aeronDirs, final long untilClusterSeqNo) {
        final List<Recording> recordings = new ArrayList<>();
        aeronDirs.forEach((name, aeronDir) -> recordings.addAll(read(name, aeronDir, untilClusterSeqNo)));
        recordings.removeIf(recording -> !recording.active());
        assertOneLog(recordings);
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
                    fail(describe(recording) + " and " + describe(longest) + " hold different frames at clusterSeqNo " +
                         (i + 1));
                }
            }
        }
    }

    /** The queue recordings on the archive behind {@code aeronDir}: stopped ones whole, the active one to the given. */
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
            for (final Span span : awaitSpans(name, archive)) {
                recordings.add(readOne(name, aeron, archive, span, untilClusterSeqNo));
            }
            return recordings;
        } finally {
            CloseHelper.closeAll(archive, aeron);
        }
    }

    /**
     * Each queue recording, once exactly one is active: a restarted member's
     * service starts its recording after {@code launch} returns.
     */
    private static List<Span> awaitSpans(final String name, final AeronArchive archive) {
        final long deadline = System.nanoTime() + TIMEOUT_NS;
        while (true) {
            final List<Span> spans = new ArrayList<>();
            archive.listRecordingsForUri(
                0, Integer.MAX_VALUE, "", FrameLayer.QUEUE_STREAM_ID,
                (controlSessionId, correlationId, recordingId, startTimestamp, stopTimestamp, startPosition,
                 stopPosition, initialTermId, segmentFileLength, termBufferLength, mtuLength, sessionId, streamId,
                 strippedChannel, originalChannel, sourceIdentity)
                    -> spans.add(new Span(recordingId, startPosition, stopTimestamp == AeronArchive.NULL_TIMESTAMP)));
            final long active = spans.stream().filter(Span::active).count();
            if (active == 1) {
                return spans;
            }
            if (System.nanoTime() > deadline) {
                assertEquals(1, active, name + "'s archive holds no single active queue recording");
            }
            Tests.sleep(50);
        }
    }

    private static Recording readOne(final String name, final Aeron aeron, final AeronArchive archive, final Span span,
                                     final long untilClusterSeqNo) {
        final long recordingId = span.recordingId();
        final long startPosition = span.startPosition();
        final boolean active = span.active();
        final Reader reader = new Reader(new Recording(name, recordingId, active, new LongArrayList()));
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
            while (active ? reader.next <= untilClusterSeqNo : !isReplayed(subscription, stopPosition)) {
                final int fragments = subscription.poll(assembler, 100);
                if (reader.error != null) {
                    fail(reader.error);
                }
                if (fragments == 0) {
                    if (System.nanoTime() > deadline) {
                        fail(describe(reader.recording) + " reached clusterSeqNo " + (reader.next - 1) + " of " +
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

    private static boolean isReplayed(final Subscription subscription, final long stopPosition) {
        final Image image = subscription.imageCount() == 0 ? null : subscription.imageAtIndex(0);
        return image != null && image.position() >= stopPosition;
    }

    private static String describe(final Recording recording) {
        return recording.archive + "'s " + (recording.active ? "active" : "stopped") + " recording " +
            recording.recordingId;
    }

    /** Checks each frame is the next clusterSeqNo and digests it; records a failure, since a poll swallows a throw. */
    private static final class Reader implements FragmentHandler {
        final Recording recording;
        final SequencedFrameDecoder frame = new SequencedFrameDecoder();
        final CRC32C crc = new CRC32C();
        byte[] bytes = new byte[0];
        long next = 1;
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
                error = describe(recording) + " holds a frame that is not sequenced after clusterSeqNo " + (next - 1);
                return;
            }
            if (frame.clusterSeqNo() != next) {
                error =
                    describe(recording) + " holds clusterSeqNo " + frame.clusterSeqNo() + " where " + next + " belongs";
                return;
            }
            if (bytes.length < length) {
                bytes = new byte[length];
            }
            buffer.getBytes(offset, bytes, 0, length);
            crc.reset();
            crc.update(bytes, 0, length);
            recording.digests.addLong(crc.getValue() << 32 | length); // the frame itself would not fit a long run
            next++;
        }
    }
}
