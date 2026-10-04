# ports.sh — canonical port-layout formula shared by every seqeron launch/test script, the
# bash mirror of SequencerServer's PORT_BASE + memberId*10 + offset scheme (see
# SequencerServer.java's class Javadoc, PortLayout.hpp on the C++ side, and the
# "Port layout" section — the source of truth all three cite). Meant to be sourced, not
# executed.

# The stride is fixed; the base is a deployment knob read by all three mirrors (this file,
# PortLayout.java, PortLayout.hpp). Set it identically for every seqeron process on every host, or a
# node and a client will disagree about which ports to bind and dial — a connection that never
# completes rather than an error naming the cause.
CLUSTER_PORT_BASE="${SEQERON_PORT_BASE:-9300}"
CLUSTER_PORT_STRIDE=10
# Members a cluster is bounded at, one stride each; PortLayout's CLUSTER_MEMBER_COUNT in both languages.
CLUSTER_MEMBER_COUNT=7

# Sourced, so this exits the caller — which is the point: a bad base is better caught here than as a
# bind error on a port nobody chose.
if ! [[ "${CLUSTER_PORT_BASE}" =~ ^[0-9]+$ ]] ||
   (( CLUSTER_PORT_BASE < 1024 ||
      CLUSTER_PORT_BASE + CLUSTER_MEMBER_COUNT * CLUSTER_PORT_STRIDE - 1 > 65535 )); then
    echo "ERROR: SEQERON_PORT_BASE='${CLUSTER_PORT_BASE}' must be an integer in 1024..65466" >&2
    exit 1
fi

cluster_member_port_base() { echo $(( CLUSTER_PORT_BASE + $1 * CLUSTER_PORT_STRIDE )); }
archive_port()  { echo $(( $(cluster_member_port_base "$1") + 1 )); }
ingress_port()  { echo $(( $(cluster_member_port_base "$1") + 2 )); }

# Builds the sequencer.hosts list for a nodeCount-member cluster: "localhost,localhost,…". The host argument
# may carry a literal {id}, replaced by each member's id. Usage: cluster_hosts_string 3 [host]
cluster_hosts_string() {
    local node_count="$1" host_template="${2:-localhost}" out="" id
    for (( id = 0; id < node_count; id++ )); do
        [[ -n "${out}" ]] && out+=","
        out+="${host_template//\{id\}/${id}}"
    done
    echo "${out}"
}

# Builds the ingressEndpoints string clusterctl takes (CLUSTERCTL_INGRESS_ENDPOINTS) for a
# nodeCount-member cluster: "0=host:9302,1=host:9312,…". The host argument may carry a literal {id},
# replaced by each member's id. Usage: ingress_endpoints_string 3 [host]   e.g. 'node-{id}'
ingress_endpoints_string() {
    local node_count="$1" host_template="${2:-localhost}" out="" id host
    for (( id = 0; id < node_count; id++ )); do
        host="${host_template//\{id\}/${id}}"
        [[ -n "${out}" ]] && out+=","
        out+="${id}=${host}:$(ingress_port "${id}")"
    done
    echo "${out}"
}

# ── Core's own satellite port block (doc/ops.md, "Ports"). An application's ports are its own. ──────────────────
TEST_GATEWAY_PORT_BASE=9200          # TestGateway TCP listen (9200 GW-T-A, 9201 GW-T-B) — the cluster
                                     # tier's OWN harness block, 9200-9209. Not in
                                     # the 9300 block: that is seven members of stride 10 with nothing spare.

test_gateway_port()       { echo $(( TEST_GATEWAY_PORT_BASE + ${1:-0} )); }
