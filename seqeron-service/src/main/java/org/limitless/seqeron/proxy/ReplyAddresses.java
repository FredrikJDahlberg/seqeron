package org.limitless.seqeron.proxy;

import org.limitless.seqeron.app.ReplyAddress;

/**
 * The {@link ReplyAddress}s of the replies a proxy places, kept so that a steady stream of replies to the same few
 * requesters allocates none. Direct-mapped and fixed in size: an address that finds its slot taken by another
 * replaces it, so a thrashing set costs an allocation per reply, as there would be without it.
 */
final class ReplyAddresses {
    private static final int SLOTS = 256;

    private final ReplyAddress[] table = new ReplyAddress[SLOTS];

    /**
     * @param sourceId     the requester's source
     * @param connectionId the requester's connection
     * @param groupMask    the groups that saw the request
     * @return@return an address equal to {@code new ReplyAddress(sourceId, connectionId, groupMask)}
     */
    ReplyAddress of(final int sourceId, final int connectionId, final int groupMask) {
        final int slot = ((sourceId * 31 + connectionId) * 31 + groupMask) & (SLOTS - 1);
        final ReplyAddress cached = table[slot];
        if (cached != null && cached.sourceId() == sourceId && cached.connectionId() == connectionId &&
            cached.groupMask() == groupMask) {
            return cached;
        }
        return table[slot] = new ReplyAddress(sourceId, connectionId, groupMask);
    }
}
