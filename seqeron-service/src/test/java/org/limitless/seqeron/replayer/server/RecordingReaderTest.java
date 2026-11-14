package org.limitless.seqeron.replayer.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.aeron.archive.client.AeronArchive;
import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.protocol.HeaderFlyweight;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import org.agrona.BitUtil;
import org.agrona.DirectBuffer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** A recording read from its segment files, written here as an archive writes them. */
class RecordingReaderTest {
    private static final long RECORDING_ID = 7;
    private static final int TERM_LENGTH = 64 * 1024;
    private static final int SEGMENT_LENGTH = 2 * TERM_LENGTH;
    private static final int HEADER_LENGTH = DataHeaderFlyweight.HEADER_LENGTH;
    private static final int UNFRAGMENTED = FrameDescriptor.UNFRAGMENTED;

    @TempDir Path archiveDir;

    private final List<String> messages = new ArrayList<>();
    private final List<Long> ends = new ArrayList<>();
    private long startPosition;
    /** Where the next frame is written. */
    private long tail;

    @Test
    @DisplayName("messages come in order, each with where it ends, and none that ends past the limit")
    void messagesUpToTheLimit() {
        final long first = data(UNFRAGMENTED, "one");
        final long second = data(UNFRAGMENTED, "two");
        data(UNFRAGMENTED, "three");
        try (RecordingReader reader = reader(0)) {
            assertEquals(0, poll(reader, first - 1));
            assertEquals(2, poll(reader, second));
            assertEquals(List.of("one", "two"), messages);
            assertEquals(List.of(first, second), ends);
            assertEquals(second, reader.position());
            assertEquals(1, poll(reader, tail));
            assertEquals(List.of("one", "two", "three"), messages);
        }
    }

    @Test
    @DisplayName("a fragmented message is handed over whole, once its last fragment is under the limit")
    void fragmentsAreAssembled() {
        data(FrameDescriptor.BEGIN_FRAG_FLAG, "be");
        final long middle = data(0, "twe");
        final long end = data(FrameDescriptor.END_FRAG_FLAG, "en");
        data(UNFRAGMENTED, "after");
        try (RecordingReader reader = reader(0)) {
            assertEquals(0, poll(reader, middle));
            assertEquals(middle, reader.position());
            assertEquals(2, poll(reader, tail));
            assertEquals(List.of("between", "after"), messages);
            assertEquals(end, ends.get(0));
        }
    }

    @Test
    @DisplayName("the padding at a term's end is skipped, and the next segment file is read after the last")
    void paddingAndSegments() {
        data(UNFRAGMENTED, "term 0");
        pad();
        data(UNFRAGMENTED, "term 1");
        pad();
        final long inNextSegment = data(UNFRAGMENTED, "term 2");
        assertEquals(SEGMENT_LENGTH + BitUtil.align(HEADER_LENGTH + 6, FrameDescriptor.FRAME_ALIGNMENT), inNextSegment);
        try (RecordingReader reader = reader(0)) {
            assertEquals(3, poll(reader, tail));
            assertEquals(List.of("term 0", "term 1", "term 2"), messages);
        }
    }

    @Test
    @DisplayName("a recording that starts inside a term is read from the segment file that term is in")
    void aRecordingStartedInsideATerm() {
        startPosition = tail = 5L * TERM_LENGTH + 96;
        data(UNFRAGMENTED, "first");
        final long second = data(UNFRAGMENTED, "second");
        try (RecordingReader reader = reader(startPosition)) {
            assertEquals(2, poll(reader, tail));
        }
        try (RecordingReader reader = reader(second - BitUtil.align(HEADER_LENGTH + 6, 32))) {
            assertEquals(1, poll(reader, tail));
        }
        assertEquals(List.of("first", "second", "second"), messages);
    }

    @Test
    @DisplayName("a handler that returns false ends the poll after its message, and a message limit after that many")
    void aPollEndsEarly() {
        data(UNFRAGMENTED, "one");
        data(UNFRAGMENTED, "two");
        data(UNFRAGMENTED, "three");
        try (RecordingReader reader = reader(0)) {
            assertEquals(1, reader.poll((buffer, offset, length, end) -> false, tail, 10));
            assertEquals(1, reader.poll(this::onMessage, tail, 1));
            assertEquals(1, poll(reader, tail));
            assertEquals(List.of("two", "three"), messages);
        }
    }

    @Test
    @DisplayName("a segment file cut short of the limit throws after what it holds, and so does one that is gone")
    void truncatedAndMissingFiles() throws IOException {
        final long first = data(UNFRAGMENTED, "kept");
        data(UNFRAGMENTED, "cut");
        try (FileChannel file = FileChannel.open(segmentFile(0), StandardOpenOption.WRITE)) {
            file.truncate(first + 8);
        }
        try (RecordingReader reader = reader(0)) {
            assertThrows(UncheckedIOException.class, () -> poll(reader, tail));
            assertEquals(List.of("kept"), messages);
            assertEquals(first, reader.position());
        }
        try (RecordingReader reader = reader(SEGMENT_LENGTH)) {
            assertThrows(UncheckedIOException.class, () -> poll(reader, SEGMENT_LENGTH + 64));
        }
    }

    @Test
    @DisplayName("a frame header below the limit too short for a frame throws, where a damaged segment holds zeros")
    void aDamagedFrameHeader() {
        final long first = data(UNFRAGMENTED, "kept");
        write(ByteBuffer.allocate(64), 64);
        try (RecordingReader reader = reader(0)) {
            assertThrows(UncheckedIOException.class, () -> poll(reader, tail));
            assertEquals(List.of("kept"), messages);
            assertEquals(first, reader.position());
        }
    }

    private RecordingReader reader(final long position) {
        return new RecordingReader(archiveDir.toFile(), RECORDING_ID, startPosition, TERM_LENGTH, SEGMENT_LENGTH,
                                   position);
    }

    private int poll(final RecordingReader reader, final long limit) {
        return reader.poll(this::onMessage, limit, 10);
    }

    private boolean onMessage(final DirectBuffer buffer, final int offset, final int length, final long endPosition) {
        messages.add(buffer.getStringWithoutLengthAscii(offset, length));
        ends.add(endPosition);
        return true;
    }

    /** @return where the frame ends */
    private long data(final int flags, final String payload) {
        final byte[] bytes = payload.getBytes(StandardCharsets.US_ASCII);
        final ByteBuffer frame = header(HEADER_LENGTH + bytes.length, flags, HeaderFlyweight.HDR_TYPE_DATA);
        frame.put(HEADER_LENGTH, bytes);
        return write(frame, BitUtil.align(HEADER_LENGTH + bytes.length, FrameDescriptor.FRAME_ALIGNMENT));
    }

    /** Pads to the term's end, as a publication does when a frame does not fit: the archive writes its header alone. */
    private void pad() {
        final int length = TERM_LENGTH - (int)(tail & (TERM_LENGTH - 1));
        write(header(length, UNFRAGMENTED, HeaderFlyweight.HDR_TYPE_PAD).limit(HEADER_LENGTH), length);
    }

    private static ByteBuffer header(final int frameLength, final int flags, final int type) {
        final ByteBuffer frame = ByteBuffer.allocate(BitUtil.align(frameLength, FrameDescriptor.FRAME_ALIGNMENT))
                                     .order(ByteOrder.LITTLE_ENDIAN);
        frame.putInt(HeaderFlyweight.FRAME_LENGTH_FIELD_OFFSET, frameLength);
        frame.put(HeaderFlyweight.FLAGS_FIELD_OFFSET, (byte)flags);
        frame.putShort(HeaderFlyweight.TYPE_FIELD_OFFSET, (short)type);
        return frame;
    }

    private long write(final ByteBuffer frame, final int alignedLength) {
        final long base = AeronArchive.segmentFileBasePosition(startPosition, tail, TERM_LENGTH, SEGMENT_LENGTH);
        try (FileChannel file =
                 FileChannel.open(segmentFile(base), StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
            file.write(frame, tail - base);
        } catch (final IOException ex) {
            throw new UncheckedIOException(ex);
        }
        tail += alignedLength;
        return tail;
    }

    private Path segmentFile(final long base) {
        return archiveDir.resolve(RECORDING_ID + "-" + base + ".rec");
    }
}
