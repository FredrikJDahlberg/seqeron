package org.limitless.seqeron.protocol;

/**
 * The cluster port-layout formula — the Java twin of {@code protocol/PortLayout.hpp} and {@code
 * ports.sh}; {@code SequencerServerTest} and {@code PortLayoutTest} pin the same pairs. Cluster member ports
 * only (doc/ops.md, "Ports"). Client tier, so a producer derives
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

    /** The base when {@link #ENV_PORT_BASE} is unset: core's registered block. */
    public static final int DEFAULT_CLUSTER_PORT_BASE = 9300;

    /** Ports one member takes, so member {@code m}'s block starts at {@code base + m * stride}. */
    public static final int CLUSTER_PORT_STRIDE = 10;

    /** Members a cluster is bounded at, since the reserved block is this many strides wide. */
    public static final int CLUSTER_MEMBER_COUNT = 3;

    /** One stride per member. See {@link #CLUSTER_PORT_BLOCK_FIRST}. */
    private static final int CLUSTER_PORT_BLOCK_WIDTH = CLUSTER_MEMBER_COUNT * CLUSTER_PORT_STRIDE;

    private static final int MIN_PORT_BASE = 1024;
    private static final int MAX_PORT = 65535;

    /** The base this process runs on, {@link #ENV_PORT_BASE}'s value or {@link #DEFAULT_CLUSTER_PORT_BASE}. */
    public static final int CLUSTER_PORT_BASE = resolveClusterPortBase(System.getenv(ENV_PORT_BASE));

    /**
     * Core's reserved block (doc/ops.md, "Ports"): three members wide, one stride each — wider than the
     * base+1..base+25 three members bind.
     */
    public static final int CLUSTER_PORT_BLOCK_FIRST = CLUSTER_PORT_BASE;

    /** Last port of core's reserved block. See {@link #CLUSTER_PORT_BLOCK_FIRST}. */
    public static final int CLUSTER_PORT_BLOCK_LAST = CLUSTER_PORT_BASE + CLUSTER_PORT_BLOCK_WIDTH - 1;

    /** How a process reaches the archive in its own Aeron directory: no endpoint, so no port to allocate. */
    public static final String ARCHIVE_CONTROL_CHANNEL = "aeron:ipc";

    /**
     * Control stream of that link. The archive's own local control, every Java archive client and the C++
     * one must name the same id, so it is stated here once rather than per process.
     */
    public static final int ARCHIVE_CONTROL_STREAM_ID = 100;

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

    /** First port of one member's stride; the five below sit at fixed offsets from it. */
    public static int memberPortBase(final int memberId) {
        return CLUSTER_PORT_BASE + memberId * CLUSTER_PORT_STRIDE;
    }

    /** That member's Aeron Archive control port. */
    public static int archivePort(final int memberId) {
        return memberPortBase(memberId) + 1;
    }

    /** That member's cluster-ingress port, where a producer submits over UDP. */
    public static int ingressPort(final int memberId) {
        return memberPortBase(memberId) + 2;
    }

    /** That member's consensus port, which the cluster's members use among themselves. */
    public static int consensusPort(final int memberId) {
        return memberPortBase(memberId) + 3;
    }

    /** That member's log port, where it replicates the Raft log. */
    public static int logPort(final int memberId) {
        return memberPortBase(memberId) + 4;
    }

    /** That member's log-transfer port, used to catch a member up. */
    public static int transferPort(final int memberId) {
        return memberPortBase(memberId) + 5;
    }

    /** A member's cluster-ingress endpoint ("host:port"). */
    public static String ingressEndpoint(final int memberId) {
        return DEFAULT_HOST + ":" + ingressPort(memberId);
    }

    /**
     * The ingress endpoint set of a {@code nodeCount}-member cluster, {@code "0=host:9302,1=host:9312,…"} —
     * the form {@code AeronCluster} and {@code clusterctl} take, and the Java mirror of {@code ports.sh}'s
     * {@code ingress_endpoints_string}.
     */
    public static String ingressEndpoints(final int nodeCount) {
        final StringBuilder endpoints = new StringBuilder();
        for (int id = 0; id < nodeCount; id++) {
            if (id > 0) {
                endpoints.append(',');
            }
            endpoints.append(id).append('=').append(ingressEndpoint(id));
        }
        return endpoints.toString();
    }

    /** The set naming every member of a full {@link #CLUSTER_MEMBER_COUNT}-member cluster. */
    public static String ingressEndpoints() {
        return ingressEndpoints(CLUSTER_MEMBER_COUNT);
    }
}
