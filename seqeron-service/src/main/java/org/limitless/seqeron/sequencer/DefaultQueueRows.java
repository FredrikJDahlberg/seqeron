package org.limitless.seqeron.sequencer;

import java.nio.ByteOrder;
import java.util.function.Consumer;
import org.agrona.DirectBuffer;
import org.limitless.seqeron.protocol.SystemFrame;
import org.limitless.seqeron.sbe.frame.MessageHeaderDecoder;
import org.limitless.seqeron.sbe.frame.SequencedSystemDecoder;
import org.limitless.seqeron.sbe.frame.SequencedSystemHeaderDecoder;

/**
 * What a member's sequencer hands its replayer (spec R-6): a copy of each topology row on the default queue, which
 * carries every one of them. Fed the sequencer's activation and each frame it publishes.
 */
public final class DefaultQueueRows {
    private final Consumer<byte[]> rows;
    private int defaultGroupId;

    /** @param rows receives each row's copy, in log order */
    public DefaultQueueRows(final Consumer<byte[]> rows) {
        this.rows = rows;
    }

    /** @param groupIds the groups activated, ascending: the first is the default queue's */
    public void onActivate(final int[] groupIds) {
        defaultGroupId = groupIds[0];
    }

    /**
     * Copies the frame if it is a topology row on the default queue.
     * @param groupId the queue it was published on
     * @param buffer  holding the frame from offset 0
     * @param length  of the frame
     */
    public void onPublish(final int groupId, final DirectBuffer buffer, final int length) {
        if (groupId == defaultGroupId && isTopologyRow(buffer, 0, length)) {
            final byte[] row = new byte[length];
            buffer.getBytes(0, row);
            rows.accept(row);
        }
    }

    /**
     * Whether a sequenced frame is a topology row. On the default queue's publish path, so it reads two fields and
     * allocates nothing.
     * @param buffer holding the frame
     * @param offset where it starts
     * @param length its length
     * @return true for a {@code GroupsRegistered}, {@code GatewayRegistered} or {@code ApplicationRegistered}
     */
    public static boolean isTopologyRow(final DirectBuffer buffer, final int offset, final int length) {
        if (length < MessageHeaderDecoder.ENCODED_LENGTH + SequencedSystemHeaderDecoder.ENCODED_LENGTH ||
            buffer.getShort(offset + MessageHeaderDecoder.templateIdEncodingOffset(), ByteOrder.LITTLE_ENDIAN) !=
                SequencedSystemDecoder.TEMPLATE_ID) {
            return false;
        }
        final int typeOffset =
            offset + MessageHeaderDecoder.ENCODED_LENGTH + SequencedSystemHeaderDecoder.systemEventTypeEncodingOffset();
        final int systemEventType = buffer.getShort(typeOffset, ByteOrder.LITTLE_ENDIAN) & 0xFFFF;
        return systemEventType == SystemFrame.GROUPS_REGISTERED || systemEventType == SystemFrame.GATEWAY_REGISTERED ||
            systemEventType == SystemFrame.APPLICATION_REGISTERED;
    }
}
