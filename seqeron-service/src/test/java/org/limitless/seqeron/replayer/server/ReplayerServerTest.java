package org.limitless.seqeron.replayer.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.limitless.seqeron.protocol.PortLayout;

class ReplayerServerTest {
    @Test
    void archiveEndpointsFollowTheHostList() {
        assertEquals("m0:" + PortLayout.archivePort(0) + ",m1:" + PortLayout.archivePort(1),
                     ReplayerServer.archiveEndpoints(List.of("m0", "m1")));
        assertNull(ReplayerServer.archiveEndpoints(List.of()));
    }

    @Test
    void aGatewayHostRelaysTheGroupsItNamesAscendingAndMustNameOne() {
        assertEquals(List.of(2, 5), ReplayerServer.parseGroups(" 5, 2 ,"));
        assertEquals(List.of(1, 16), ReplayerServer.parseGroups("16,1"));
        assertThrows(IllegalArgumentException.class, () -> ReplayerServer.parseGroups(""));
        assertThrows(IllegalArgumentException.class, () -> ReplayerServer.parseGroups("1,1"));
        assertThrows(IllegalArgumentException.class, () -> ReplayerServer.parseGroups("0"));
        assertThrows(IllegalArgumentException.class, () -> ReplayerServer.parseGroups("17"));
        assertThrows(IllegalArgumentException.class, () -> ReplayerServer.parseGroups("one"));
    }
}
