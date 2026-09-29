package org.limitless.seqeron.replayer.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

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
}
