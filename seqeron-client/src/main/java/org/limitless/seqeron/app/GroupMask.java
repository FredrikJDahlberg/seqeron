package org.limitless.seqeron.app;

import org.limitless.seqeron.protocol.FrameLayer;

/**
 * Groups a publish addresses besides the publisher's own, as bits of {@code groupMask} (spec A-9). Taken from the
 * {@link GroupDirectory} and combined with {@link #or}; a type of its own so that a mask and a {@code payloadId} cannot
 * trade places in a publish. Immutable and interned: neither {@link #of} nor {@link #or} allocates once a mask has been
 * seen.
 */
public final class GroupMask {
    private static final GroupMask[] INTERNED = new GroupMask[1 << FrameLayer.MAX_GROUPS];

    /** No group besides the own one. */
    public static final GroupMask NONE = of(0);

    private final int bits;

    private GroupMask(final int bits) {
        this.bits = bits;
    }

    /**
     * The mask with these bits, for one that arrives as bits rather than from a {@link GroupDirectory}.
     * @param bits 0 to 0xFFFF
     * @throws IllegalArgumentException outside that range
     */
    public static GroupMask of(final int bits) {
        if (bits < 0 || bits >= INTERNED.length) {
            throw new IllegalArgumentException("groupMask " + bits + " is not 16 bits");
        }
        GroupMask mask = INTERNED[bits];
        if (mask == null) {
            // A racing thread may intern its own copy; equality is by bits, so either serves.
            mask = new GroupMask(bits);
            INTERNED[bits] = mask;
        }
        return mask;
    }

    /** The groups of both. */
    public GroupMask or(final GroupMask other) {
        return of(bits | other.bits);
    }

    /** The {@code groupMask} bits. */
    public int bits() {
        return bits;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof GroupMask mask && mask.bits == bits;
    }

    @Override
    public int hashCode() {
        return bits;
    }

    @Override
    public String toString() {
        return "GroupMask[0x" + Integer.toHexString(bits) + "]";
    }
}
