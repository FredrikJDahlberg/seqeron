package org.limitless.seqeron.proxy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

import org.junit.jupiter.api.Test;
import org.limitless.seqeron.app.ReplyAddress;

class ReplyAddressesTest {
    private final ReplyAddresses refs = new ReplyAddresses();

    @Test
    void theSameRequesterGetsTheSameInstance() {
        assertSame(refs.of(16, 7, 1), refs.of(16, 7, 1));
    }

    @Test
    void aReferenceIsEqualToANewOne() {
        assertEquals(new ReplyAddress(16, 7, 3), refs.of(16, 7, 3));
    }

    @Test
    void requestersThatShareASlotStillGetTheirOwnValues() {
        final int slots = 256;
        final ReplyAddress first = refs.of(1, 0, 1);
        final ReplyAddress second = refs.of(1, slots * 31, 1);
        assertEquals(new ReplyAddress(1, 0, 1), first);
        assertEquals(new ReplyAddress(1, slots * 31, 1), second);
        assertNotSame(first, second);
        assertEquals(new ReplyAddress(1, 0, 1), refs.of(1, 0, 1));
    }
}
