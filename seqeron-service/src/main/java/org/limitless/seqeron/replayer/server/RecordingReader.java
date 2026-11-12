package org.limitless.seqeron.replayer.server;

import io.aeron.archive.client.AeronArchive;
import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.protocol.HeaderFlyweight;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import org.agrona.BitUtil;
import org.agrona.BufferUtil;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Reads the messages of a recording from the archive's segment files, in order, up to a position the caller gives:
 * what a bounded replay of it delivers, without the archive or a publication between.
 *
 * <p>A segment file is the recorded stream's term buffers: data frames under Aeron's header, a padding frame at a
 * term's end. The archive truncates a segment file when it truncates the recording, so this reads with positional
 * reads, which come back short, and maps nothing. The caller's limit must be at or below what the recorder has
 * written, and below anything a truncation can reach: for a Raft log, the commit position.
 *
 * <p>Not thread-safe.
 */
final class RecordingReader implements AutoCloseable {
    /** Takes one whole message. */
    @FunctionalInterface
    interface MessageHandler {
        /**
         * @param endPosition where the message's last frame ends in the stream
         * @return whether to go on to the next message in this poll
         */
        boolean onMessage(DirectBuffer buffer, int offset, int length, long endPosition);
    }

    private static final int READ_LENGTH = 1024 * 1024;

    private final File archiveDir;
    private final long recordingId;
    private final long startPosition;
    private final int termLength;
    private final int segmentLength;
    private final ByteBuffer bytes = BufferUtil.allocateDirectAligned(READ_LENGTH, BitUtil.CACHE_LINE_LENGTH);
    private final UnsafeBuffer buffer = new UnsafeBuffer(bytes);
    private final ExpandableArrayBuffer message = new ExpandableArrayBuffer();
    /** The length assembled of a fragmented message, or -1 outside one. */
    private int messageLength = -1;
    /** The stream position of the buffer's first byte, and how much of it is read. */
    private long bufferPosition;
    private int bufferLength;
    private FileChannel segment;
    private long segmentBase;
    private long position;

    /**
     * @param startPosition the recording's
     * @param termLength    the recorded stream's
     * @param segmentLength the recording's segment file length
     * @param position      of the frame to read first
     */
    RecordingReader(final File archiveDir, final long recordingId, final long startPosition, final int termLength,
                    final int segmentLength, final long position) {
        this.archiveDir = archiveDir;
        this.recordingId = recordingId;
        this.startPosition = startPosition;
        this.termLength = termLength;
        this.segmentLength = segmentLength;
        this.position = position;
        bytes.order(ByteOrder.LITTLE_ENDIAN);
    }

    /** Where the next frame to read starts. */
    long position() {
        return position;
    }

    /**
     * Hands over the messages whose frames all end at or below {@code limit}.
     *
     * @param messageLimit the most to hand over
     * @return how many were handed over
     * @throws UncheckedIOException if a segment file is gone or cannot be read
     */
    int poll(final MessageHandler handler, final long limit, final int messageLimit) {
        int messages = 0;
        while (messages < messageLimit && position < limit && load(DataHeaderFlyweight.HEADER_LENGTH, limit)) {
            final int offset = (int)(position - bufferPosition);
            final int frameLength = buffer.getInt(offset + HeaderFlyweight.FRAME_LENGTH_FIELD_OFFSET);
            final int alignedLength = BitUtil.align(frameLength, FrameDescriptor.FRAME_ALIGNMENT);
            if (frameLength < DataHeaderFlyweight.HEADER_LENGTH || position + alignedLength > limit) {
                break;
            }
            if ((buffer.getShort(offset + HeaderFlyweight.TYPE_FIELD_OFFSET) & 0xFFFF) ==
                HeaderFlyweight.HDR_TYPE_PAD) {
                position += alignedLength;
                continue;
            }
            if (!load(frameLength, limit)) {
                break;
            }
            final int frameOffset = (int)(position - bufferPosition);
            final int flags = buffer.getByte(frameOffset + HeaderFlyweight.FLAGS_FIELD_OFFSET) & 0xFF;
            final int dataOffset = frameOffset + DataHeaderFlyweight.HEADER_LENGTH;
            final int dataLength = frameLength - DataHeaderFlyweight.HEADER_LENGTH;
            position += alignedLength;
            if ((flags & FrameDescriptor.UNFRAGMENTED) == FrameDescriptor.UNFRAGMENTED) {
                messages++;
                if (!handler.onMessage(buffer, dataOffset, dataLength, position)) {
                    break;
                }
                continue;
            }
            if ((flags & FrameDescriptor.BEGIN_FRAG_FLAG) != 0) {
                messageLength = 0;
            }
            if (messageLength < 0) {
                continue; // started inside a message
            }
            message.putBytes(messageLength, buffer, dataOffset, dataLength);
            messageLength += dataLength;
            if ((flags & FrameDescriptor.END_FRAG_FLAG) != 0) {
                final int length = messageLength;
                messageLength = -1;
                messages++;
                if (!handler.onMessage(message, 0, length, position)) {
                    break;
                }
            }
        }
        return messages;
    }

    /** @return whether {@code length} bytes at {@link #position} are in the buffer, read from the file if need be */
    private boolean load(final int length, final long limit) {
        if (position >= bufferPosition && position + length <= bufferPosition + bufferLength) {
            return true;
        }
        try {
            final long base = AeronArchive.segmentFileBasePosition(startPosition, position, termLength, segmentLength);
            if (segment == null || base != segmentBase) {
                CloseHelper.close(segment);
                segment = null;
                segment = FileChannel.open(new File(archiveDir, recordingId + "-" + base + ".rec").toPath(),
                                           StandardOpenOption.READ);
                segmentBase = base;
            }
            final long end = Math.min(limit, base + segmentLength);
            bytes.clear().limit((int)Math.min(READ_LENGTH, end - position));
            bufferPosition = position;
            while (bytes.hasRemaining() && segment.read(bytes, position - base + bytes.position()) > 0) {
                // a read may come back short of what the file holds
            }
            bufferLength = bytes.position();
        } catch (final IOException ex) {
            throw new UncheckedIOException(ex);
        }
        return length <= bufferLength;
    }

    @Override
    public void close() {
        CloseHelper.quietClose(segment);
    }
}
