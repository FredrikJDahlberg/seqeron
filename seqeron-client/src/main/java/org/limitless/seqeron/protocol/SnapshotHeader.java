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
 * 18  groupCount          uint8
 * 19  groups, in ascending groupId: groupId uint8, groupName char[32]
 *     — a gateway's header continues at G = 19 + 33 × groupCount —
 * G       gatewaySourceId     int32
 * G + 4   activeGatewayId     int32    the current GatewayActive's, or {@link #NO_GATEWAY}
 * G + 8   highestConnectionId int32    the highest in the source's history
 * G + 12  rowCount            uint16
 * G + 14  rows, in list order: gatewayId int32, preferenceRank uint8, gatewayName char[32]
 * </pre>
 *
 * Every instance of a source writes the same header (A-6), so it carries the rows of every instance of a
 * gateway and an instance finds its own by name, as it does in the list. The C++ twin is {@code
 * protocol/Snapshot.hpp}; keep the two in step.
 *
 * @param leadershipTermId the term of the last {@code LeadershipChanged} before the cut
 * @param leaderMemberId   that term's leader
 * @param groups           the active groups before the cut, in ascending {@code groupId}
 * @param gateway          a gateway's continuation, or null for an application's header
 */
public record SnapshotHeader(long leadershipTermId, int leaderMemberId, List<Group> groups, GatewayState gateway) {
    /** The one {@code headerVersion} this build writes and reads. */
    public static final int VERSION = 2;

    /** No {@code GatewayActive} yet. */
    public static final int NO_GATEWAY = -1;

    private static final int FIXED_LENGTH = 19;
    private static final int NAME_LENGTH = 32;
    private static final int GROUP_LENGTH = 1 + NAME_LENGTH;
    private static final int GATEWAY_FIXED_LENGTH = 14;
    private static final int GATEWAY_ROW_LENGTH = 5 + NAME_LENGTH;
    private static final ByteOrder LE = ByteOrder.LITTLE_ENDIAN;

    /**
     * One active group.
     * @param groupId   1 to {@link FrameLayer#MAX_GROUPS}
     * @param groupName its name, at most 32 US-ASCII characters
     */
    public record Group(int groupId, String groupName) { }

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
        final int applicationLength = FIXED_LENGTH + GROUP_LENGTH * groups.size();
        return gateway == null ? applicationLength
                               : applicationLength + GATEWAY_FIXED_LENGTH + GATEWAY_ROW_LENGTH * gateway.rows().size();
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
        buffer.putByte(offset + 18, (byte)groups.size());
        int group = offset + FIXED_LENGTH;
        for (final Group each : groups) {
            buffer.putByte(group, (byte)each.groupId());
            putName(buffer, group + 1, each.groupName());
            group += GROUP_LENGTH;
        }
        if (gateway != null) {
            buffer.putInt(group, gateway.gatewaySourceId(), LE);
            buffer.putInt(group + 4, gateway.activeGatewayId(), LE);
            buffer.putInt(group + 8, gateway.highestConnectionId(), LE);
            buffer.putShort(group + 12, (short)gateway.rows().size(), LE);
            int row = group + GATEWAY_FIXED_LENGTH;
            for (final GatewayRow each : gateway.rows()) {
                buffer.putInt(row, each.gatewayId(), LE);
                buffer.putByte(row + 4, (byte)each.preferenceRank());
                putName(buffer, row + 5, each.gatewayName());
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
        if (length < FIXED_LENGTH || version(buffer, offset, length) != VERSION) {
            return null;
        }
        final long headerLength = buffer.getInt(offset + 2, LE) & 0xFFFF_FFFFL;
        final long leadershipTermId = buffer.getLong(offset + 6, LE);
        final int leaderMemberId = buffer.getInt(offset + 14, LE);
        final int groupCount = buffer.getByte(offset + 18) & 0xFF;
        final int applicationLength = FIXED_LENGTH + GROUP_LENGTH * groupCount;
        if (headerLength > length || headerLength < applicationLength) {
            return null;
        }
        final List<Group> groups = new ArrayList<>(groupCount);
        int group = offset + FIXED_LENGTH;
        for (int i = 0; i < groupCount; i++) {
            groups.add(new Group(buffer.getByte(group) & 0xFF, getName(buffer, group + 1)));
            group += GROUP_LENGTH;
        }
        if (headerLength == applicationLength) {
            return new SnapshotHeader(leadershipTermId, leaderMemberId, List.copyOf(groups), null);
        }
        if (headerLength < applicationLength + GATEWAY_FIXED_LENGTH) {
            return null;
        }
        final int rowCount = buffer.getShort(group + 12, LE) & 0xFFFF;
        if (headerLength != applicationLength + GATEWAY_FIXED_LENGTH + (long)GATEWAY_ROW_LENGTH * rowCount) {
            return null;
        }
        final List<GatewayRow> rows = new ArrayList<>(rowCount);
        int row = group + GATEWAY_FIXED_LENGTH;
        for (int i = 0; i < rowCount; i++) {
            rows.add(new GatewayRow(buffer.getInt(row, LE), buffer.getByte(row + 4) & 0xFF, getName(buffer, row + 5)));
            row += GATEWAY_ROW_LENGTH;
        }
        return new SnapshotHeader(leadershipTermId, leaderMemberId, List.copyOf(groups),
                                  new GatewayState(buffer.getInt(group, LE), buffer.getInt(group + 4, LE),
                                                   buffer.getInt(group + 8, LE), List.copyOf(rows)));
    }

    private static void putName(final MutableDirectBuffer buffer, final int offset, final String name) {
        final byte[] bytes = name.getBytes(StandardCharsets.US_ASCII);
        final int nameLength = Math.min(bytes.length, NAME_LENGTH);
        buffer.putBytes(offset, bytes, 0, nameLength);
        buffer.setMemory(offset + nameLength, NAME_LENGTH - nameLength, (byte)0);
    }

    private static String getName(final DirectBuffer buffer, final int offset) {
        int nameLength = 0;
        while (nameLength < NAME_LENGTH && buffer.getByte(offset + nameLength) != 0) {
            nameLength++;
        }
        return buffer.getStringWithoutLengthAscii(offset, nameLength);
    }
}
