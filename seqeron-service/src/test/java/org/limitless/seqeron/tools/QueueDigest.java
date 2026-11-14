package org.limitless.seqeron.tools;

import io.aeron.Aeron;
import io.aeron.ChannelUri;
import io.aeron.FragmentAssembler;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.logbuffer.FragmentHandler;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32C;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.limitless.seqeron.protocol.FrameLayer;
import org.limitless.seqeron.protocol.PortLayout;
import org.limitless.seqeron.protocol.ReplayProtocol;
import org.limitless.seqeron.protocol.SequencedFrameDecoder;
import org.limitless.seqeron.sbe.replay.MessageHeaderDecoder;
import org.limitless.seqeron.sbe.replay.MessageHeaderEncoder;
import org.limitless.seqeron.sbe.replay.ReplayRequestEncoder;
import org.limitless.seqeron.sbe.replay.ReplayingDecoder;

/**
 * Digests each recording of a group's queue on one member's archive, for {@code hop-order-check.py} to compare across
 * members. Harness code in the test source set, run by {@code node.sh queue-digest}; it reads the archive as
 * {@code QueueRecordings} does, and fails on a frame that is not sequenced or on a gap in {@code groupSeqNo}.
 *
 * <p>One line per recording on stdout: {@code recording=<id> active=<bool> first=<groupSeqNo> last=<groupSeqNo>
 * frames=<n> crc=<hex> ckpt=<groupSeqNo>:<hex>,…}. The CRC-32C runs over every frame's bytes from the first, and a
 * checkpoint is its value at each multiple of {@value #CHECKPOINT} in {@code groupSeqNo}, so two members that stopped at
 * different frames compare at the last checkpoint both hold. An active recording is read to its position when the
 * tool starts. A member records no queue, so its queue is read through its Replayer, as a client reads it, from the
 * queue's start to its tip when the replay is answered, and printed as {@code recording=replayer active=true …}.
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
            if (spans.isEmpty()) {
                System.out.println(serve(aeron, groupId));
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

    /** Digests the queue as the member's Replayer serves it: one replay from its start to its tip. */
    private static String serve(final Aeron aeron, final int groupId) {
        final Reader reader = new Reader();
        final long clientId = aeron.nextCorrelationId();
        final long deadline = System.nanoTime() + TIMEOUT_NS;
        try (Subscription control =
                 aeron.addSubscription(ReplayProtocol.IPC_CHANNEL + "|tether=false", ReplayProtocol.CONTROL_STREAM_ID);
             Publication requests =
                 aeron.addPublication(ReplayProtocol.IPC_CHANNEL, ReplayProtocol.REQUEST_STREAM_ID)) {
            final long[] replay = ask(control, requests, clientId, groupId, deadline);
            final FragmentAssembler assembler = new FragmentAssembler(reader);
            try (Subscription subscription = aeron.addSubscription(
                     ReplayProtocol.IPC_CHANNEL + "|session-id=" + (int)replay[0], ReplayProtocol.REPLAY_STREAM_ID)) {
                Image image = null;
                while (reader.error == null && (image == null || image.position() < replay[1])) {
                    image = image == null ? subscription.imageBySessionId((int)replay[0]) : image;
                    if ((image == null ? 0 : image.poll(assembler, 100)) == 0 && System.nanoTime() > deadline) {
                        reader.error = "the Replayer's replay read to groupSeqNo " + reader.last + " of " + replay[1];
                    }
                }
            }
        }
        if (reader.error != null) {
            System.out.println("ERROR " + reader.error);
            System.exit(1);
        }
        return "recording=replayer active=true first=" + reader.first + " last=" + reader.last +
            " frames=" + reader.frames + " crc=" + Long.toHexString(reader.crc.getValue()) +
            " ckpt=" + String.join(",", reader.checkpoints);
    }

    /** Asks for the queue from its start, again every 500 ms, until a replay is answered: its session and end. */
    private static long[] ask(final Subscription control, final Publication requests, final long clientId,
                              final int groupId, final long deadline) {
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(64);
        final ReplayRequestEncoder request = new ReplayRequestEncoder();
        request.wrapAndApplyHeader(buffer, 0, new MessageHeaderEncoder())
            .clientId(clientId)
            .requestId(1)
            .fromPosition(ReplayProtocol.FROM_START)
            .queueId(groupId)
            .cutClusterSeqNo(ReplayProtocol.NO_CUT);
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + request.encodedLength();
        final MessageHeaderDecoder header = new MessageHeaderDecoder();
        final ReplayingDecoder replaying = new ReplayingDecoder();
        final long[] replay = { 0, -1 };
        final FragmentHandler onReply = (buf, offset, len, unused) -> {
            header.wrap(buf, offset);
            if (header.templateId() == ReplayingDecoder.TEMPLATE_ID) {
                replaying.wrap(buf, offset + MessageHeaderDecoder.ENCODED_LENGTH, header.blockLength(),
                               header.version());
                if (replaying.clientId() == clientId) {
                    replay[0] = replaying.replaySessionId();
                    replay[1] = replaying.catchUpPosition();
                }
            }
        };
        long resendAt = 0;
        while (replay[1] < 0) {
            if (System.nanoTime() > deadline) {
                System.out.println("ERROR the Replayer answered no replay of group " + groupId);
                System.exit(1);
            }
            if (System.nanoTime() >= resendAt) {
                requests.offer(buffer, 0, length);
                resendAt = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(500);
            }
            control.poll(onReply, 10);
            Thread.onSpinWait();
        }
        return replay;
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
