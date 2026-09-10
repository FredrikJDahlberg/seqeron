package org.limitless.seqeron.sequencer;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the part of the cluster session client that is a decision rather than an Aeron call.
 * The session state machine itself is {@code AeronCluster}'s here — the C++ twin owns it and tests it
 * through fake transports, and there is no Java equivalent of that seam to drive.
 */
class ClusterStreamSenderTest {
    @Test
    @DisplayName("the endpoint set is built off the port formula, not a restatement of the numbers")
    void endpointsComeFromTheSequencerServerFormula() {
        final StringBuilder expected = new StringBuilder();
        for (int id = 0; id < 3; id++) {
            if (id > 0) {
                expected.append(',');
            }
            expected.append(id).append('=').append(SequencerServer.ingressEndpoint(id));
        }
        assertEquals(expected.toString(), ClusterStreamSender.ingressEndpoints(3));
    }

    @Test
    @DisplayName("a single-node cluster names one member")
    void singleNodeEndpointSet() {
        assertEquals("0=" + SequencerServer.ingressEndpoint(0), ClusterStreamSender.ingressEndpoints(1));
    }

    @Test
    @DisplayName("every endpoint the default set names is inside core's reserved block")
    void everyEndpointIsACoreClusterPort() {
        for (final String entry : ClusterStreamSender.ingressEndpoints(ClusterStreamSender.DEFAULT_NODE_COUNT)
                                      .split(",")) {
            final int port = Integer.parseInt(entry.substring(entry.lastIndexOf(':') + 1));
            assertEquals(true, SequencerServer.isClusterPort(port), entry);
        }
    }
}
