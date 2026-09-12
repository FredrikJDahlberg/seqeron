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

    /**
     * Deployment-wide port-base override, read by all three mirrors — this class,
     * {@code PortLayout.hpp} and {@code ports.sh}. It must be set identically for <i>every</i> seqeron
     * process on every host: a node that disagrees with a client about the base binds and dials
     * different ports, and the symptom is a connection that never completes rather than an error
     * naming the cause. An environment variable rather than a system property because it is the one
     * knob all three languages have to read.
     */
    public static final String ENV_PORT_BASE = "SEQERON_PORT_BASE";

    /** The base when {@link #ENV_PORT_BASE} is unset — core's registered block, doc/registries.md §2. */
    public static final int DEFAULT_CLUSTER_PORT_BASE = 9300;

    public static final int CLUSTER_PORT_STRIDE = 10;

    /** Three members, one stride each. See {@link #CLUSTER_PORT_BLOCK_FIRST}. */
    private static final int CLUSTER_PORT_BLOCK_WIDTH = 3 * CLUSTER_PORT_STRIDE;

    private static final int MIN_PORT_BASE = 1024;
    private static final int MAX_PORT = 65535;

    public static final int CLUSTER_PORT_BASE = resolveClusterPortBase(System.getenv(ENV_PORT_BASE));

    /**
     * Core's reserved port block (doc/registries.md §2) — three members wide, one stride each.
     * Deliberately <i>not</i> the base+1..base+25 a three-node cluster actually binds: that is the
     * number every restatement of this boundary used to carry, and the gap between the two is where an
     * application port ended up squatting on the third member's base.
     */
    public static final int CLUSTER_PORT_BLOCK_FIRST = CLUSTER_PORT_BASE;

    /** Last port of core's reserved block. See {@link #CLUSTER_PORT_BLOCK_FIRST}. */
    public static final int CLUSTER_PORT_BLOCK_LAST = CLUSTER_PORT_BASE + CLUSTER_PORT_BLOCK_WIDTH - 1;

    private PortLayout() {
    }

    /**
     * Parses {@link #ENV_PORT_BASE}. The environment read is the caller's, so this stays a pure
     * function and the rules are unit-testable without touching the process environment — the same
     * seam {@code parseClusterPortBase} is on the C++ side.
     *
     * <p>A bad value fails here, loudly, rather than surfacing later as a bind error on a port nobody
     * chose.
     *
     * @param raw the raw environment value, or {@code null} when unset
     * @return the configured base, or {@link #DEFAULT_CLUSTER_PORT_BASE}
     */
    static int resolveClusterPortBase(final String raw) {
        if (null == raw || raw.isBlank()) {
            return DEFAULT_CLUSTER_PORT_BASE;
        }

        final int base;
        try {
            base = Integer.parseInt(raw.trim());
        } catch (final NumberFormatException ex) {
            throw new IllegalArgumentException(ENV_PORT_BASE + " is not a number: '" + raw + "'", ex);
        }

        if (base < MIN_PORT_BASE) {
            throw new IllegalArgumentException(
                ENV_PORT_BASE + "=" + base + " is below " + MIN_PORT_BASE + " (privileged ports)");
        }
        if (base + CLUSTER_PORT_BLOCK_WIDTH - 1 > MAX_PORT) {
            throw new IllegalArgumentException(
                ENV_PORT_BASE + "=" + base + " leaves no room for the " + CLUSTER_PORT_BLOCK_WIDTH
                    + "-port cluster block below " + MAX_PORT);
        }
        return base;
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
