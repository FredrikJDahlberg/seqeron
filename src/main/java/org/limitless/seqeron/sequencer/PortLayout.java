package org.limitless.seqeron.sequencer;

/**
 * Canonical cluster port-layout formula — the Java twin of {@code sequencer/PortLayout.hpp}, which
 * carried it alone until the jar was split by audience (doc/future-arch.md §6). Cluster member ports
 * only: an application's own bases are its own, because a reusable sequencer must not name the
 * processes that talk to it. Which block each product owns is {@code doc/registries.md} §2's; the
 * only thing core states in code is its own reservation, below.
 *
 * <p><b>Client tier.</b> A producer derives a member's ingress endpoint from here and a product
 * asserts its own bases sit outside the block from here, so neither has to depend on the node —
 * {@link SequencerServer} is the only thing that binds these ports, and it reads them from here too.
 *
 * <p>Every port constant used to be an independently hand-typed literal restating the formula (or a
 * satellite base "known" to sit outside it) — that drift is how two co-located egress ports ended up
 * both defaulting to 9340+memberId. {@code SequencerServerTest} (Java) and {@code PortLayoutTest}
 * (C++) pin the same (memberId -&gt; port) pairs, so a change to one side without the other fails a
 * build.
 */
public final class PortLayout {
    /** Every default endpoint below is on one host; a real deployment overrides them wholesale. */
    public static final String DEFAULT_HOST = "localhost";

    public static final int CLUSTER_PORT_BASE = 9300;
    public static final int CLUSTER_PORT_STRIDE = 10;

    /**
     * Core's reserved port block (doc/registries.md §2) — three members wide, one stride each.
     * Deliberately <i>not</i> the 9301-9325 a three-node cluster actually binds: that is the number
     * every restatement of this boundary used to carry, and the gap between the two is where an
     * application port ended up squatting on 9320.
     */
    public static final int CLUSTER_PORT_BLOCK_FIRST = CLUSTER_PORT_BASE;

    /** Last port of core's reserved block. See {@link #CLUSTER_PORT_BLOCK_FIRST}. */
    public static final int CLUSTER_PORT_BLOCK_LAST = CLUSTER_PORT_BASE + 3 * CLUSTER_PORT_STRIDE - 1;

    private PortLayout() {
    }

    /**
     * Whether a port falls inside core's reservation. For a product asserting its own bases sit
     * outside it, so the boundary is read from here rather than copied.
     */
    public static boolean isClusterPort(final int port) {
        return port >= CLUSTER_PORT_BLOCK_FIRST && port <= CLUSTER_PORT_BLOCK_LAST;
    }

    public static int memberPortBase(final int memberId) {
        return CLUSTER_PORT_BASE + memberId * CLUSTER_PORT_STRIDE;
    }

    public static int archivePort(final int memberId) {
        return memberPortBase(memberId) + 1;
    }

    public static int ingressPort(final int memberId) {
        return memberPortBase(memberId) + 2;
    }

    public static int consensusPort(final int memberId) {
        return memberPortBase(memberId) + 3;
    }

    public static int logPort(final int memberId) {
        return memberPortBase(memberId) + 4;
    }

    public static int transferPort(final int memberId) {
        return memberPortBase(memberId) + 5;
    }

    /**
     * A member's cluster-ingress endpoint ("host:port"). Exposed so a caller co-located with the
     * default cluster (a producer, {@code ClusterCtl}) derives its default from here instead of
     * restating the port number.
     */
    public static String ingressEndpoint(final int memberId) {
        return DEFAULT_HOST + ":" + ingressPort(memberId);
    }
}
