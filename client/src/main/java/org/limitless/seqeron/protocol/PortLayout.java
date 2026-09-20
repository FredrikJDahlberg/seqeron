package org.limitless.seqeron.protocol;

/**
 * The cluster port-layout formula — the Java twin of {@code protocol/PortLayout.hpp} and {@code
 * ports.sh}; {@code SequencerServerTest} and {@code PortLayoutTest} pin the same pairs. Cluster member ports
 * only: which block each product owns is doc/registries.md §2's. Client tier, so a producer derives
 * endpoints from here without depending on the node.
 */
public final class PortLayout {
    /** Every default endpoint below is on one host; a real deployment overrides them wholesale. */
    public static final String DEFAULT_HOST = "localhost";

    /**
     * Deployment-wide port-base override, read by all three mirrors. Set it identically for every seqeron
     * process: a disagreement shows as a connection that never completes, not as an error.
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
     * Core's reserved block (doc/registries.md §2): three members wide, one stride each — wider than the
     * base+1..base+25 three members bind.
     */
    public static final int CLUSTER_PORT_BLOCK_FIRST = CLUSTER_PORT_BASE;

    /** Last port of core's reserved block. See {@link #CLUSTER_PORT_BLOCK_FIRST}. */
    public static final int CLUSTER_PORT_BLOCK_LAST = CLUSTER_PORT_BASE + CLUSTER_PORT_BLOCK_WIDTH - 1;

    private PortLayout() {
    }

    /**
     * Parses {@link #ENV_PORT_BASE}; a pure function so it is testable without the environment. A bad value
     * fails here rather than as a bind error later.
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

    /** Whether a port falls inside core's reservation, for a product checking its own bases. */
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

    /** A member's cluster-ingress endpoint ("host:port"). */
    public static String ingressEndpoint(final int memberId) {
        return DEFAULT_HOST + ":" + ingressPort(memberId);
    }
}
