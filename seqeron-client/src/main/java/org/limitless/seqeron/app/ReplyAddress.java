package org.limitless.seqeron.app;

/**
 * What a reply needs of the request it answers: the requester's {@code sourceId}, {@code connectionId} and {@code
 * groupMask}. Unlike the {@link Payload} it is set from, it outlives the callback, so keep it, in the request's
 * {@link OutstandingWork} slot, for a reply dispatched later. Mutable, so that slot reuses it.
 */
public final class ReplyAddress {
    private int sourceId;
    private int connectionId;
    private int groupMask;

    /** An address to {@link #set} later. */
    public ReplyAddress() {
    }

    /** An address set to these. */
    public ReplyAddress(final int sourceId, final int connectionId, final int groupMask) {
        set(sourceId, connectionId, groupMask);
    }

    /**
     * Sets this to the request's address.
     * @return this
     */
    public ReplyAddress set(final Payload request) {
        return set(request.sourceId(), request.connectionId(), request.groupMask());
    }

    /**
     * Sets this to these.
     * @return this
     */
    public ReplyAddress set(final int sourceId, final int connectionId, final int groupMask) {
        this.sourceId = sourceId;
        this.connectionId = connectionId;
        this.groupMask = groupMask;
        return this;
    }

    /** The requester's source. */
    public int sourceId() {
        return sourceId;
    }

    /** The requester's connection, or -1 for a producer-scoped request. */
    public int connectionId() {
        return connectionId;
    }

    /** The groups that saw the request. */
    public int groupMask() {
        return groupMask;
    }
}
