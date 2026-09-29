package org.limitless.seqeron.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The port base is a deployment knob ({@code SEQERON_PORT_BASE}), and this pins the rules that decide
 * it. Driven through the pure seam rather than the environment: the base is read once into a static
 * field, so a test that set the variable would be testing whichever test ran first. The C++ twin is
 * {@code PortLayoutTest.cpp}'s {@code ParseClusterPortBase*} cases — same rules, same boundaries.
 *
 * <p>The (memberId -&gt; port) pairs the formula produces stay in {@code SequencerServerTest}. The endpoint
 * set below is the Java mirror of {@code ports.sh}'s {@code ingress_endpoints_string}.
 */
class PortLayoutTest {
    @Test
    void unsetOrBlankFallsBackToTheRegisteredBlock() {
        assertEquals(9300, PortLayout.resolveClusterPortBase(null));
        assertEquals(9300, PortLayout.resolveClusterPortBase(""));
        assertEquals(9300, PortLayout.resolveClusterPortBase("   "));
        assertEquals(PortLayout.DEFAULT_CLUSTER_PORT_BASE, PortLayout.resolveClusterPortBase(null));
    }

    @Test
    void aValidBaseIsTakenAsGiven() {
        assertEquals(20000, PortLayout.resolveClusterPortBase("20000"));
        assertEquals(20000, PortLayout.resolveClusterPortBase("  20000  "));
    }

    @Test
    void nonNumericIsRejectedRatherThanTreatedAsUnset() {
        assertThrows(IllegalArgumentException.class, () -> PortLayout.resolveClusterPortBase("9300x"));
        assertThrows(IllegalArgumentException.class, () -> PortLayout.resolveClusterPortBase("nine"));
    }

    // 1024 is the first unprivileged port, and the block is 30 wide, so 65506 is the last base that
    // fits. Both boundaries are pinned from either side.
    @Test
    void theBaseMustLeaveRoomForTheWholeBlockAndAvoidPrivilegedPorts() {
        assertThrows(IllegalArgumentException.class, () -> PortLayout.resolveClusterPortBase("1023"));
        assertEquals(1024, PortLayout.resolveClusterPortBase("1024"));

        assertEquals(65506, PortLayout.resolveClusterPortBase("65506"));
        assertThrows(IllegalArgumentException.class, () -> PortLayout.resolveClusterPortBase("65507"));
    }

    @Test
    @DisplayName("the endpoint set is built off the port formula, not a restatement of the numbers")
    void endpointsComeFromTheSequencerServerFormula() {
        final StringBuilder expected = new StringBuilder();
        for (int id = 0; id < 3; id++) {
            if (id > 0) {
                expected.append(',');
            }
            expected.append(id).append('=').append(PortLayout.ingressEndpoint(id));
        }
        assertEquals(expected.toString(), PortLayout.ingressEndpoints(3));
    }

    @Test
    @DisplayName("a single-node cluster names one member")
    void singleNodeEndpointSet() {
        assertEquals("0=" + PortLayout.ingressEndpoint(0), PortLayout.ingressEndpoints(1));
    }

    @Test
    @DisplayName("with SEQERON_HOSTS unset, the default set names every member the cluster is bounded at")
    void theDefaultSetIsTheWholeCluster() {
        assertEquals(List.of(), PortLayout.HOSTS);
        assertEquals(PortLayout.ingressEndpoints(PortLayout.CLUSTER_MEMBER_COUNT), PortLayout.ingressEndpoints());
    }

    @Test
    @DisplayName("every endpoint the default set names is inside core's reserved block")
    void everyEndpointIsACoreClusterPort() {
        for (final String entry : PortLayout.ingressEndpoints().split(",")) {
            final int port = Integer.parseInt(entry.substring(entry.lastIndexOf(':') + 1));
            assertTrue(PortLayout.isClusterPort(port), entry);
        }
    }

    @Test
    @DisplayName("member i of a host list runs on entry i")
    void hostListEndpointSet() {
        assertEquals("0=h0:" + PortLayout.ingressPort(0) + ",1=h1:" + PortLayout.ingressPort(1) + ",2=h2:"
                         + PortLayout.ingressPort(2),
                     PortLayout.ingressEndpoints(PortLayout.parseHosts("h0, h1 ,h2")));
        assertEquals(PortLayout.ingressEndpoints(1), PortLayout.ingressEndpoints(List.of(PortLayout.DEFAULT_HOST)));
    }

    @Test
    @DisplayName("SEQERON_HOSTS unset or blank names no host; set, it is parsed like any host list")
    void hostsFromTheEnvironment() {
        assertEquals(List.of(), PortLayout.resolveHosts(null));
        assertEquals(List.of(), PortLayout.resolveHosts("  "));
        assertEquals(List.of("h0", "h1"), PortLayout.resolveHosts("h0,h1"));
        assertThrows(IllegalArgumentException.class, () -> PortLayout.resolveHosts("h0,,h1"));
    }

    @Test
    @DisplayName("a host list with a blank entry, or more members than the block holds, is refused")
    void hostListRules() {
        assertEquals(List.of("h0"), PortLayout.parseHosts("h0"));
        assertThrows(IllegalArgumentException.class, () -> PortLayout.parseHosts(""));
        assertThrows(IllegalArgumentException.class, () -> PortLayout.parseHosts("h0,,h2"));
        assertThrows(IllegalArgumentException.class, () -> PortLayout.parseHosts("h0,h1,"));
        assertThrows(IllegalArgumentException.class, () -> PortLayout.parseHosts("h0,h1,h2,h3"));
    }
}
