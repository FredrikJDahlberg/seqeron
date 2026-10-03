package org.limitless.seqeron.protocol;

import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

/**
 * The header a façade writes ahead of the application's body: what it derives from frames before the
 * round's cut, which a restore never replays (doc/snapshot.md §6). Little-endian, packed:
 *
 * <pre>
 *  0  headerVersion       uint16   {@link #VERSION}
 *  2  headerLength        uint32   bytes up to the body
 *  6  leadershipTermId    int64    from the last LeadershipChanged before the cut
 * 14  leaderMemberId      int32    that frame's newLeaderMemberId
 *     — a gateway's header continues —
 * 18  gatewaySourceId     int32
 * 22  activeGatewayId     int32    the current GatewayActive's, or {@link #NO_GATEWAY}
 * 26  highestConnectionId int32    the highest in the source's history
 * 30  rowCount            uint16
 * 32  rows, in list order: gatewayId int32, preferenceRank uint8, gatewayName char[32]
 * </pre>
 *
 * Every instance of a source writes the same header (A-6), so it carries the rows of every instance of a
 * gateway and an instance finds its own by name, as it does in the list. The C++ twin is {@code
 * protocol/Snapshot.hpp}; keep the two in step.
 *
 * @param leadershipTermId the term of the last {@code LeadershipChanged} before the cut
 * @param leaderMemberId   that term's leader
 * @param gateway          a gateway's continuation, or null for an application's header
 */
public record SnapshotHeader(long leadershipTermId, int leaderMemberId, GatewayState gateway) {
    /** The one {@code headerVersion} this build writes and reads. */
    public static final int VERSION = 1;

    /** An application's header, and the start of a gateway's continuation. */
    public static final int APPLICATION_LENGTH = 18;

    /** No {@code GatewayActive} yet. */
    public static final int NO_GATEWAY = -1;

    private static final int GATEWAY_FIXED_LENGTH = 14;
    private static final int GATEWAY_NAME_LENGTH = 32;
    private static final int GATEWAY_ROW_LENGTH = 5 + GATEWAY_NAME_LENGTH;
    private static final ByteOrder LE = ByteOrder.LITTLE_ENDIAN;

    /**
     * A gateway's continuation.
     * @param gatewaySourceId     the logical gateway
     * @param activeGatewayId     the instance the current {@code GatewayActive} names, or {@link #NO_GATEWAY}
     * @param highestConnectionId the highest {@code connectionId} in the source's history
     * @param rows                the list rows of every instance of the gateway, in list order
     */
    public record GatewayState(int gatewaySourceId, int activeGatewayId, int highestConnectionId,
                               List<GatewayRow> rows) {
    }

    /**
     * One instance's list row.
     * @param gatewayId      the instance
     * @param preferenceRank its rank
     * @param gatewayName    its name, at most 32 US-ASCII characters
     */
    public record GatewayRow(int gatewayId, int preferenceRank, String gatewayName) {
    }

    /** Bytes {@link #encode} writes; the value of {@code headerLength}. */
    public int encodedLength() {
        return gateway == null ? APPLICATION_LENGTH
                               : APPLICATION_LENGTH + GATEWAY_FIXED_LENGTH + GATEWAY_ROW_LENGTH * gateway.rows().size();
    }

    /**
     * Writes the header.
     * @param buffer where to write
     * @param offset of the header's first byte
     * @return the bytes written, {@link #encodedLength()}
     */
    public int encode(final MutableDirectBuffer buffer, final int offset) {
        final int length = encodedLength();
        buffer.putShort(offset, (short)VERSION, LE);
        buffer.putInt(offset + 2, length, LE);
        buffer.putLong(offset + 6, leadershipTermId, LE);
        buffer.putInt(offset + 14, leaderMemberId, LE);
        if (gateway != null) {
            buffer.putInt(offset + 18, gateway.gatewaySourceId(), LE);
            buffer.putInt(offset + 22, gateway.activeGatewayId(), LE);
            buffer.putInt(offset + 26, gateway.highestConnectionId(), LE);
            buffer.putShort(offset + 30, (short)gateway.rows().size(), LE);
            int row = offset + APPLICATION_LENGTH + GATEWAY_FIXED_LENGTH;
            for (final GatewayRow each : gateway.rows()) {
                buffer.putInt(row, each.gatewayId(), LE);
                buffer.putByte(row + 4, (byte)each.preferenceRank());
                final byte[] name = each.gatewayName().getBytes(StandardCharsets.US_ASCII);
                final int nameLength = Math.min(name.length, GATEWAY_NAME_LENGTH);
                buffer.putBytes(row + 5, name, 0, nameLength);
                buffer.setMemory(row + 5 + nameLength, GATEWAY_NAME_LENGTH - nameLength, (byte)0);
                row += GATEWAY_ROW_LENGTH;
            }
        }
        return length;
    }

    /**
     * The {@code headerVersion} at {@code offset}, or -1 if {@code length} cannot hold one.
     * @param buffer holding the snapshot
     * @param offset of its first byte
     * @param length the snapshot's bytes from {@code offset}
     */
    public static int version(final DirectBuffer buffer, final int offset, final int length) {
        return length < 2 ? -1 : buffer.getShort(offset, LE) & 0xFFFF;
    }

    /**
     * Reads a header written by {@link #encode}.
     * @param buffer holding the snapshot
     * @param offset of its first byte
     * @param length the snapshot's bytes from {@code offset}
     * @return the header, or null if it is not a {@link #VERSION} header or does not fit {@code length}
     */
    public static SnapshotHeader decode(final DirectBuffer buffer, final int offset, final int length) {
        if (length < APPLICATION_LENGTH || version(buffer, offset, length) != VERSION) {
            return null;
        }
        final long headerLength = buffer.getInt(offset + 2, LE) & 0xFFFF_FFFFL;
        final long leadershipTermId = buffer.getLong(offset + 6, LE);
        final int leaderMemberId = buffer.getInt(offset + 14, LE);
        if (headerLength == APPLICATION_LENGTH) {
            return new SnapshotHeader(leadershipTermId, leaderMemberId, null);
        }
        if (headerLength > length || headerLength < APPLICATION_LENGTH + GATEWAY_FIXED_LENGTH) {
            return null;
        }
        final int rowCount = buffer.getShort(offset + 30, LE) & 0xFFFF;
        if (headerLength != APPLICATION_LENGTH + GATEWAY_FIXED_LENGTH + (long)GATEWAY_ROW_LENGTH * rowCount) {
            return null;
        }
        final List<GatewayRow> rows = new ArrayList<>(rowCount);
        int row = offset + APPLICATION_LENGTH + GATEWAY_FIXED_LENGTH;
        for (int i = 0; i < rowCount; i++) {
            int nameLength = 0;
            while (nameLength < GATEWAY_NAME_LENGTH && buffer.getByte(row + 5 + nameLength) != 0) {
                nameLength++;
            }
            rows.add(new GatewayRow(buffer.getInt(row, LE), buffer.getByte(row + 4) & 0xFF,
                                    buffer.getStringWithoutLengthAscii(row + 5, nameLength)));
            row += GATEWAY_ROW_LENGTH;
        }
        return new SnapshotHeader(leadershipTermId, leaderMemberId,
                                  new GatewayState(buffer.getInt(offset + 18, LE), buffer.getInt(offset + 22, LE),
                                                   buffer.getInt(offset + 26, LE), List.copyOf(rows)));
    }
}
