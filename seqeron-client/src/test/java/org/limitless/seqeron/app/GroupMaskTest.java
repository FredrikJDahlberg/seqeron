package org.limitless.seqeron.app;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

/** {@link GroupMask}: its bits, its combination, its range, and that a mask once seen is never allocated again. */
class GroupMaskTest {
    @Test
    void combinesTheBitsOfBoth() {
        final GroupMask orders = GroupMask.of(1);
        final GroupMask risk = GroupMask.of(4);
        assertEquals(5, orders.or(risk).bits());
        assertEquals(1, orders.or(GroupMask.NONE).bits());
        assertEquals(0, GroupMask.NONE.bits());
    }

    @Test
    void isInternedSoCombiningAllocatesNothingOnceSeen() {
        assertSame(GroupMask.of(5), GroupMask.of(1).or(GroupMask.of(4)));
        assertSame(GroupMask.NONE, GroupMask.of(0));
        assertSame(GroupMask.of(0xFFFF), GroupMask.of(0x8000).or(GroupMask.of(0x7FFF)));
    }

    @Test
    void holdsSixteenBitsAndNoMore() {
        assertThrows(IllegalArgumentException.class, () -> GroupMask.of(-1));
        assertThrows(IllegalArgumentException.class, () -> GroupMask.of(0x10000));
    }

    @Test
    void isEqualByItsBits() {
        assertEquals(GroupMask.of(3), GroupMask.of(3));
        assertEquals(3, GroupMask.of(3).hashCode());
        assertNotEquals(GroupMask.of(3), GroupMask.of(2));
    }
}
