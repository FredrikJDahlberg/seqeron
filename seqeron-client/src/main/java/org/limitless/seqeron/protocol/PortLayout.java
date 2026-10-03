package org.limitless.seqeron.protocol;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * The cluster port-layout formula — the Java twin of {@code protocol/PortLayout.hpp} and {@code
 * ports.sh}; {@code SequencerServerTest} and {@code PortLayoutTest} pin the same pairs. Cluster member ports
 * only (doc/ops.md, "Ports"). Client tier, so a producer derives
 * endpoints from here without depending on the node.
 */
public final class PortLayout {
    /** Every default endpoint below is on one host; a real deployment overrides them wholesale. */
    public static final String DEFAULT_HOST = "localhost";

    /** Members of the default cluster on {@link #DEFAULT_HOST}, what the default endpoint sets name. */
    public static final int DEFAULT_MEMBER_COUNT = 3;

    /**
     * Deployment-wide port-base override, read by all three mirrors. Set it identically for every seqeron
     * process: a disagreement shows as a connection that never completes, not as an error.
     */
    public static final String ENV_PORT_BASE = "SEQERON_PORT_BASE";

    /**
     * Deployment-wide host list, {@code "h0,h1,h2"}: member {@code i} runs on entry {@code i}. Read by every seqeron
     * process in both languages, it names the members wherever a default would otherwise say {@link #DEFAULT_HOST}.
     */
    public static final String ENV_HOSTS = "SEQERON_HOSTS";

    /** The base when {@link #ENV_PORT_BASE} is unset: core's registered block. */
    public static final int DEFAULT_CLUSTER_PORT_BASE = 9300;

    /** Ports one member takes, so member {@code m}'s block starts at {@code base + m * stride}. */
    public static final int CLUSTER_PORT_STRIDE = 10;

    /** Members a cluster is bounded at, since the reserved block is this many strides wide: Raft's 3, 5 or 7. */
    public static final int CLUSTER_MEMBER_COUNT = 7;

    /** One stride per member. See {@link #CLUSTER_PORT_BLOCK_FIRST}. */
    private static final int CLUSTER_PORT_BLOCK_WIDTH = CLUSTER_MEMBER_COUNT * CLUSTER_PORT_STRIDE;

    private static final int MIN_PORT_BASE = 1024;
    private static final int MAX_PORT = 65535;

    /** The base this process runs on, {@link #ENV_PORT_BASE}'s value or {@link #DEFAULT_CLUSTER_PORT_BASE}. */
    public static final int CLUSTER_PORT_BASE = resolveClusterPortBase(System.getenv(ENV_PORT_BASE));

    /** The members' hosts from {@link #ENV_HOSTS}, empty when it is unset. */
    public static final List<String> HOSTS = resolveHosts(System.getenv(ENV_HOSTS));

    /**
     * Core's reserved block (doc/ops.md, "Ports"): seven members wide, one stride each — wider than the
     * base+1..base+65 seven members bind.
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

    /**
     * Parses {@link #ENV_HOSTS}; a pure function so it is testable without the environment.
     *
     * @param raw the raw environment value, or {@code null} when unset
     * @return the host list, empty when {@code raw} is unset or blank
     */
    static List<String> resolveHosts(final String raw) {
        return null == raw || raw.isBlank() ? List.of() : parseHosts(raw);
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
     * Parses a cluster's host list, {@code "h0,h1,h2"}: member {@code i} runs on entry {@code i}.
     *
     * @param csv the members' host names, comma-separated
     * @return the host names, trimmed
     * @throws IllegalArgumentException if an entry is blank, or the list names no member or more than
     *                                  {@link #CLUSTER_MEMBER_COUNT}
     */
    public static List<String> parseHosts(final String csv) {
        final List<String> hosts = Arrays.stream(csv.split(",", -1)).map(String::trim).toList();
        if (hosts.contains("")) {
            throw new IllegalArgumentException("host list has a blank entry: '" + csv + "'");
        }
        if (hosts.size() > CLUSTER_MEMBER_COUNT) {
            throw new IllegalArgumentException(
                "host list names " + hosts.size() + " members, more than the " + CLUSTER_MEMBER_COUNT
                    + " the port block holds: '" + csv + "'");
        }
        return hosts;
    }

    /**
     * The ingress endpoint set of a {@code nodeCount}-member cluster on {@link #DEFAULT_HOST}, {@code
     * "0=host:9302,1=host:9312,…"} — the form {@code AeronCluster} and {@code clusterctl} take, and the Java
     * mirror of {@code ports.sh}'s {@code ingress_endpoints_string}.
     */
    public static String ingressEndpoints(final int nodeCount) {
        return ingressEndpoints(Collections.nCopies(nodeCount, DEFAULT_HOST));
    }

    /**
     * The ingress endpoint set of a cluster whose member {@code i} runs on {@code hosts.get(i)}.
     *
     * @param hosts the members' host names, as {@link #parseHosts} returns them
     */
    public static String ingressEndpoints(final List<String> hosts) {
        final StringBuilder endpoints = new StringBuilder();
        for (int id = 0; id < hosts.size(); id++) {
            if (id > 0) {
                endpoints.append(',');
            }
            endpoints.append(id).append('=').append(hosts.get(id)).append(':').append(ingressPort(id));
        }
        return endpoints.toString();
    }

    /**
     * The default endpoint set: the members {@link #HOSTS} names, else a {@link #DEFAULT_MEMBER_COUNT}-member
     * cluster on {@link #DEFAULT_HOST}.
     */
    public static String ingressEndpoints() {
        return HOSTS.isEmpty() ? ingressEndpoints(DEFAULT_MEMBER_COUNT) : ingressEndpoints(HOSTS);
    }
}
