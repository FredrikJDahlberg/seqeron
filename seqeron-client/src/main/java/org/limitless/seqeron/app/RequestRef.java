package org.limitless.seqeron.app;

/**
 * What a reply needs of the request it answers: the requester's {@code sourceId}, {@code connectionId} and {@code
 * groupMask}. Unlike the {@link Payload} it came from, it outlives the callback, so keep it for a reply dispatched
 * later.
 */
public record RequestRef(int sourceId, int connectionId, int groupMask) {
}
