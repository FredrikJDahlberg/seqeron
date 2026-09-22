package org.limitless.seqeron.protocol;

/**
 * What a publish did. {@code Refused} is local and permanent — the body is too long, or the frame breaks
 * §9.2 conditions 6 to 9 — so retrying it cannot succeed. {@code Declined} (transport back-pressure, a
 * lost session, or the tracker holding or full) is the one a caller may retry.
 *
 * <p>Here rather than in {@code sequencer.client} because it is what the {@code app} façades return, and a
 * consumer that takes one should not have to reach into the layer below it. The C++ twin is
 * {@code protocol/Publish.hpp}.
 */
public enum Publish {
    /** The frame was offered, and tracked if a tracker was given. */
    Published,
    /** Nothing was offered and retrying cannot help. */
    Refused,
    /** Nothing was offered; the same frame may be offered again. */
    Declined
}
