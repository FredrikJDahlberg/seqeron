package org.limitless.seqeron.replayer.client;

import org.agrona.DirectBuffer;

/** Builds a {@link SequencedEvent} from outside its package, for the tests of what consumes one. */
public final class SequencedEvents {
    private SequencedEvents() {
    }

    /**
     * @param id a {@code payloadId}, or a {@code systemEventType} when {@code system}
     */
    public static SequencedEvent of(final long sourceSessionId, final boolean system, final int id,
                                    final DirectBuffer body, final int offset, final int length) {
        final SequencedEvent event = new SequencedEvent();
        event.set(0, 0, 0, sourceSessionId, 0, 0, system, system ? 0 : id, system ? id : 0, 0, 0, 0, body, offset,
                  length, 0);
        return event;
    }
}
