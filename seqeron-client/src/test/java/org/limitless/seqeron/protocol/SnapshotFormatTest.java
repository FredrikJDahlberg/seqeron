package org.limitless.seqeron.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.nio.charset.StandardCharsets;
import java.util.HexFormat;
import java.util.List;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.limitless.seqeron.protocol.SnapshotValidator.State;
import org.limitless.seqeron.protocol.SnapshotHeader.GatewayRow;
import org.limitless.seqeron.protocol.SnapshotHeader.GatewayState;

/**
 * The snapshot byte format (doc/snapshot.md §2, §5, §6). The golden vectors are shared with the C++
 * {@code SnapshotFormatTest} and were computed by a third implementation, so the two languages agree with
 * each other and not merely with themselves.
 */
class SnapshotFormatTest {
    /** An application's header: term 7, leader 2. */
    static final String APPLICATION_HEADER = "010012000000070000000000000002000000";

    /** A gateway's header: term 7, leader 2, source 9, active 10, highest connection 42, rows GW-A and GW-B. */
    static final String GATEWAY_HEADER = "01006a000000070000000000000002000000090000000a0000002a0000000200"
        + "0a0000000047572d41" + "00".repeat(28) + "0b0000000147572d42" + "00".repeat(28);

    /** CRC-32C of {@link #APPLICATION_HEADER} followed by {@link #body}. */
    static final long APPLICATION_SNAPSHOT_CRC = 0x6c24f56bL;

    /** A 3000-byte body; any split of it into records gives the same CRC. */
    static byte[] body() {
        final byte[] body = new byte[3000];
        for (int i = 0; i < body.length; i++) {
            body[i] = (byte)(i * 31 + 7);
        }
        return body;
    }

    static SnapshotHeader gatewayHeader() {
        return new SnapshotHeader(7, 2, new GatewayState(9, 10, 42, List.of(new GatewayRow(10, 0, "GW-A"),
                                                                            new GatewayRow(11, 1, "GW-B"))));
    }

    @Test
    @DisplayName("CRC-32C is the Castagnoli check value, and a record is at most 1302 bytes")
    void crcAndRecordSize() {
        final byte[] check = "123456789".getBytes(StandardCharsets.US_ASCII);
        assertEquals(0xE3069283L, SnapshotFormat.crc32c(new UnsafeBuffer(check), 0, check.length));
        assertEquals(1302, SnapshotFormat.MAX_RECORD_LENGTH);
    }

    @Test
    @DisplayName("both headers encode to the shared golden bytes and decode back")
    void headersEncodeToTheGoldenBytes() {
        final SnapshotHeader application = new SnapshotHeader(7, 2, null);
        assertArrayEquals(HexFormat.of().parseHex(APPLICATION_HEADER), encode(application));
        assertArrayEquals(HexFormat.of().parseHex(GATEWAY_HEADER), encode(gatewayHeader()));

        for (final SnapshotHeader header : List.of(application, gatewayHeader())) {
            final byte[] bytes = encode(header);
            assertEquals(header, SnapshotHeader.decode(new UnsafeBuffer(bytes), 0, bytes.length));
        }
    }

    @Test
    @DisplayName("a header of another version, or one its bytes cannot hold, does not decode")
    void foreignOrShortHeadersDoNotDecode() {
        final byte[] gateway = encode(gatewayHeader());
        assertNull(SnapshotHeader.decode(new UnsafeBuffer(gateway), 0, gateway.length - 1), "truncated");
        assertNull(SnapshotHeader.decode(new UnsafeBuffer(gateway), 0, 17), "shorter than any header");

        gateway[0] = 2;
        assertEquals(2, SnapshotHeader.version(new UnsafeBuffer(gateway), 0, gateway.length));
        assertNull(SnapshotHeader.decode(new UnsafeBuffer(gateway), 0, gateway.length), "a later headerVersion");

        final byte[] rows = encode(gatewayHeader());
        rows[30] = 3;
        assertNull(SnapshotHeader.decode(new UnsafeBuffer(rows), 0, rows.length), "rowCount disagrees with length");
    }

    @Test
    @DisplayName("a snapshot's chunks, in order and then its end, are valid")
    void chunksInOrderAreValid() {
        final byte[] snapshot = snapshot();
        assertEquals(APPLICATION_SNAPSHOT_CRC, SnapshotFormat.crc32c(new UnsafeBuffer(snapshot), 0, snapshot.length));

        final SnapshotValidator validator = new SnapshotValidator();
        assertEquals(State.COMPLETE, feed(validator, snapshot, 4, new int[] {0, 1, 2}));
        assertEquals(snapshot.length, validator.length());
    }

    @Test
    @DisplayName("a chunk out of order, repeated, missing or oversized makes the snapshot invalid, and it stays so")
    void misorderedChunksInvalidate() {
        final byte[] snapshot = snapshot();
        assertEquals(State.INVALID, feed(new SnapshotValidator(), snapshot, 4, new int[] {0, 2, 1}));
        assertEquals(State.INVALID, feed(new SnapshotValidator(), snapshot, 4, new int[] {0, 1, 1, 2}));
        assertEquals(State.INVALID, feed(new SnapshotValidator(), snapshot, 4, new int[] {0, 1}));

        final SnapshotValidator oversized = new SnapshotValidator();
        oversized.reset(4);
        assertEquals(State.INVALID, oversized.onChunk(4, 0, new UnsafeBuffer(snapshot), 0, 1303));

        final SnapshotValidator validator = new SnapshotValidator();
        feed(validator, snapshot, 4, new int[] {1});
        assertEquals(State.INVALID, validator.onChunk(4, 0, new UnsafeBuffer(snapshot), 0, 1302));
    }

    @Test
    @DisplayName("a SnapshotEnd disagreeing on count, length or CRC invalidates; another round's frames are ignored")
    void endMustAgreeWithTheChunks() {
        final byte[] snapshot = snapshot();
        final long crc = SnapshotFormat.crc32c(new UnsafeBuffer(snapshot), 0, snapshot.length);
        assertEquals(State.INVALID, chunksThenEnd(snapshot, 4, snapshot.length, crc));
        assertEquals(State.INVALID, chunksThenEnd(snapshot, 3, snapshot.length + 1, crc));
        assertEquals(State.INVALID, chunksThenEnd(snapshot, 3, snapshot.length, crc ^ 1));
        assertEquals(State.COMPLETE, chunksThenEnd(snapshot, 3, snapshot.length, crc));

        final SnapshotValidator validator = new SnapshotValidator();
        validator.reset(4);
        assertEquals(State.COLLECTING, validator.onChunk(3, 5, new UnsafeBuffer(snapshot), 0, 10), "a late chunk");
        assertEquals(State.COLLECTING, validator.onEnd(5, 0, 0, 0), "a later round's end");
        assertEquals(0, validator.length());
    }

    private static byte[] snapshot() {
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
        final int headerLength = new SnapshotHeader(7, 2, null).encode(buffer, 0);
        buffer.putBytes(headerLength, body());
        final byte[] snapshot = new byte[headerLength + 3000];
        buffer.getBytes(0, snapshot);
        return snapshot;
    }

    private static byte[] encode(final SnapshotHeader header) {
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
        final byte[] bytes = new byte[header.encode(buffer, 0)];
        buffer.getBytes(0, bytes);
        return bytes;
    }

    /** Feeds the 1302-byte records named by {@code order}, the last one short, then the matching end. */
    private static State feed(final SnapshotValidator validator, final byte[] snapshot, final long round,
                              final int[] order) {
        validator.reset(round);
        final UnsafeBuffer bytes = new UnsafeBuffer(snapshot);
        for (final int index : order) {
            final int offset = index * SnapshotFormat.MAX_RECORD_LENGTH;
            validator.onChunk(round, index, bytes, offset,
                              Math.min(SnapshotFormat.MAX_RECORD_LENGTH, snapshot.length - offset));
        }
        return validator.onEnd(round, 3, snapshot.length, SnapshotFormat.crc32c(bytes, 0, snapshot.length));
    }

    private static State chunksThenEnd(final byte[] snapshot, final int chunkCount, final long length,
                                       final long crc) {
        final SnapshotValidator validator = new SnapshotValidator();
        validator.reset(4);
        final UnsafeBuffer bytes = new UnsafeBuffer(snapshot);
        for (int index = 0; index < 3; index++) {
            final int offset = index * SnapshotFormat.MAX_RECORD_LENGTH;
            validator.onChunk(4, index, bytes, offset,
                              Math.min(SnapshotFormat.MAX_RECORD_LENGTH, snapshot.length - offset));
        }
        return validator.onEnd(4, chunkCount, length, crc);
    }
}
