package org.limitless.seqeron.replayer.server;

import java.util.function.Consumer;
import org.agrona.DirectBuffer;

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
        if (groupId == defaultGroupId && QueueResolver.isTopologyRow(buffer, 0, length)) {
            final byte[] row = new byte[length];
            buffer.getBytes(0, row);
            rows.accept(row);
        }
    }
}
