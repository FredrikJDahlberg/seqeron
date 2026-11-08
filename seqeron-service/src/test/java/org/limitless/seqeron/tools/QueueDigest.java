package org.limitless.seqeron.tools;

import io.aeron.Aeron;
import io.aeron.ChannelUri;
import io.aeron.FragmentAssembler;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.logbuffer.FragmentHandler;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32C;
import org.agrona.DirectBuffer;
import org.limitless.seqeron.protocol.FrameLayer;
import org.limitless.seqeron.protocol.PortLayout;
import org.limitless.seqeron.protocol.SequencedFrameDecoder;

/**
 * Digests each recording of a group's queue on one member's archive, for {@code hop-order-check.py} to compare across
 * members. Harness code in the test source set, run by {@code node.sh queue-digest}; it reads the archive as
 * {@code QueueRecordings} does, and fails on a frame that is not sequenced or on a gap in {@code groupSeqNo}.
 *
 * <p>One line per recording on stdout: {@code recording=<id> active=<bool> first=<groupSeqNo> last=<groupSeqNo>
 * frames=<n> crc=<hex> ckpt=<groupSeqNo>:<hex>,…}. The CRC-32C runs over every frame's bytes from the first, and a
 * checkpoint is its value at each multiple of {@value #CHECKPOINT} in {@code groupSeqNo}, so two members that stopped at
 * different frames compare at the last checkpoint both hold. An active recording is read to its position when the
 * tool starts.
 *
 * <p>System properties:
 * <pre>
 *   digest.aeronDir  — the member's Aeron directory (required)
 *   digest.group     — the group whose queue is read; default 1
 *   digest.upTo      — digest no frame after this groupSeqNo, so that members compare to one last frame; default none
 * </pre>
 */
public final class QueueDigest {
    private static final int CHECKPOINT = 1 << 16;
    private static final long UP_TO = Long.getLong("digest.upTo", Long.MAX_VALUE);
    private static final int RESPONSE_STREAM_ID = 126; // beside NodeDriver's and QueueRecordings'
    private static final String REPLAY_CHANNEL = "aeron:ipc";
    private static final int REPLAY_STREAM_ID = 1207;
    private static final long TIMEOUT_NS = TimeUnit.MINUTES.toNanos(5);

    private QueueDigest() {}

    /**
     * Prints a line for each recording of the queue.
     *
     * @param args unused
     */
    public static void main(final String[] args) {
        final int groupId = Integer.getInteger("digest.group", 1);
        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(System.getProperty("digest.aeronDir")));
             AeronArchive archive = AeronArchive.connect(new AeronArchive.Context()
                                                             .aeron(aeron)
                                                             .ownsAeronClient(false)
                                                             .controlRequestChannel(PortLayout.ARCHIVE_CONTROL_CHANNEL)
                                                             .controlRequestStreamId(
                                                                 PortLayout.ARCHIVE_CONTROL_STREAM_ID)
                                                             .controlResponseChannel(
                                                                 PortLayout.ARCHIVE_CONTROL_CHANNEL)
                                                             .controlResponseStreamId(RESPONSE_STREAM_ID))) {
            final List<long[]> spans = new ArrayList<>(); // recording id, start, stop or -1 when active
            archive.listRecordingsForUri(
                0, Integer.MAX_VALUE, "", FrameLayer.queueStreamId(groupId),
                (controlSessionId, correlationId, recordingId, startTimestamp, stopTimestamp, startPosition,
                 stopPosition, initialTermId, segmentFileLength, termBufferLength, mtuLength, sessionId, streamId,
                 strippedChannel, originalChannel, sourceIdentity)
                    -> spans.add(new long[] {recordingId, startPosition,
                                             stopTimestamp == AeronArchive.NULL_TIMESTAMP ? -1 : stopPosition}));
            for (final long[] span : spans) {
                System.out.println(digest(aeron, archive, span[0], span[1], span[2]));
            }
        }
    }

    private static String digest(final Aeron aeron, final AeronArchive archive, final long recordingId,
                                 final long start, final long stop) {
        final boolean active = stop < 0;
        final long end = active ? archive.getRecordingPosition(recordingId) : stop;
        final Reader reader = new Reader();
        if (end > start) {
            final long session =
                archive.startReplay(recordingId, start, end - start, REPLAY_CHANNEL, REPLAY_STREAM_ID);
            final FragmentAssembler assembler = new FragmentAssembler(reader);
            final long deadline = System.nanoTime() + TIMEOUT_NS;
            try (Subscription subscription = aeron.addSubscription(
                     ChannelUri.addSessionId(REPLAY_CHANNEL, (int)session), REPLAY_STREAM_ID)) {
                while (reader.error == null && !(subscription.imageCount() > 0 &&
                                                 subscription.imageAtIndex(0).position() >= end)) {
                    if (subscription.poll(assembler, 100) == 0 && System.nanoTime() > deadline) {
                        reader.error = "recording " + recordingId + " read to groupSeqNo " + reader.last + " of " + end;
                    }
                }
            }
        }
        if (reader.error != null) {
            System.out.println("ERROR " + reader.error);
            System.exit(1);
        }
        return "recording=" + recordingId + " active=" + active + " first=" + reader.first + " last=" + reader.last +
            " frames=" + reader.frames + " crc=" + Long.toHexString(reader.crc.getValue()) + " ckpt=" +
            String.join(",", reader.checkpoints);
    }

    /** Checks each frame is the next groupSeqNo and digests it; records a failure, since a poll swallows a throw. */
    private static final class Reader implements FragmentHandler {
        final SequencedFrameDecoder frame = new SequencedFrameDecoder();
        final CRC32C crc = new CRC32C();
        final List<String> checkpoints = new ArrayList<>();
        byte[] bytes = new byte[0];
        long first;
        long last;
        long frames;
        String error;

        @Override
        public void onFragment(final DirectBuffer buffer, final int offset, final int length,
                               final io.aeron.logbuffer.Header header) {
            if (error != null) {
                return;
            }
            if (frames > 0 && last >= UP_TO) {
                return;
            }
            if (!frame.wrap(buffer, offset, length)) {
                error = "a frame that is not sequenced after groupSeqNo " + last;
                return;
            }
            final long groupSeqNo = frame.groupSeqNo();
            if (frames == 0) {
                first = groupSeqNo;
            } else if (groupSeqNo != last + 1) {
                error = "groupSeqNo " + groupSeqNo + " where " + (last + 1) + " belongs";
                return;
            }
            if (bytes.length < length) {
                bytes = new byte[length];
            }
            buffer.getBytes(offset, bytes, 0, length);
            crc.update(bytes, 0, length);
            last = groupSeqNo;
            frames++;
            if (groupSeqNo % CHECKPOINT == 0) {
                checkpoints.add(groupSeqNo + ":" + Long.toHexString(crc.getValue()));
            }
        }
    }
}
