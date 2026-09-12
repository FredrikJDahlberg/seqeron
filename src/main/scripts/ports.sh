# ports.sh — canonical port-layout formula shared by every seqeron launch/test script, the
# bash mirror of SequencerServer's PORT_BASE + memberId*10 + offset scheme (see
# SequencerServer.java's class Javadoc, PortLayout.hpp on the C++ side, and the
# "Port layout" section — the source of truth all three cite). Meant to be sourced, not
# executed: every script that built a CLUSTER_MEMBERS string used to hand-type the same
# three-line block, which is how BasicDataServer's and FixGateway's egress-port defaults
# once drifted onto the same value (9340+memberId) without anyone noticing.

CLUSTER_PORT_BASE=9300
CLUSTER_PORT_STRIDE=10

cluster_member_port_base() { echo $(( CLUSTER_PORT_BASE + $1 * CLUSTER_PORT_STRIDE )); }
archive_port()  { echo $(( $(cluster_member_port_base "$1") + 1 )); }
ingress_port()  { echo $(( $(cluster_member_port_base "$1") + 2 )); }
member_port()   { echo $(( $(cluster_member_port_base "$1") + 3 )); }
log_port()      { echo $(( $(cluster_member_port_base "$1") + 4 )); }
transfer_port() { echo $(( $(cluster_member_port_base "$1") + 5 )); }

# Builds the Aeron clusterMembers string for a nodeCount-member cluster — the bash mirror of
# SequencerServer.buildClusterMembers. The host argument may carry a literal {id}, replaced by each
# member's id, for a topology giving every member its own host (docker/compose.yml does).
# Usage: cluster_members_string 3 [host]   e.g. cluster_members_string 3 'node-{id}'
cluster_members_string() {
    local node_count="$1" host_template="${2:-localhost}" out="" id host
    for (( id = 0; id < node_count; id++ )); do
        host="${host_template//\{id\}/${id}}"
        out+="${id},${host}:$(ingress_port "${id}"),${host}:$(member_port "${id}"),"
        out+="${host}:$(log_port "${id}"),${host}:$(transfer_port "${id}"),${host}:$(archive_port "${id}")|"
    done
    echo "${out}"
}

# Builds the ingressEndpoints string clusterctl takes (CLUSTERCTL_INGRESS_ENDPOINTS) for a
# nodeCount-member cluster: "0=host:9302,1=host:9312,…". The host argument takes {id} the same way
# cluster_members_string's does. Usage: ingress_endpoints_string 3 [host]
ingress_endpoints_string() {
    local node_count="$1" host_template="${2:-localhost}" out="" id host
    for (( id = 0; id < node_count; id++ )); do
        host="${host_template//\{id\}/${id}}"
        [[ -n "${out}" ]] && out+=","
        out+="${id}=${host}:$(ingress_port "${id}")"
    done
    echo "${out}"
}

# ── Core's own satellite port block. A consumer's bases are the consumer's own, in the consumer's
#    own file (the C++ mirror of that split is AppPorts.hpp beside PortLayout.hpp); which block each
#    repo draws from is doc/registries.md §2's table, and core states only its own here. ──────────
TEST_GATEWAY_PORT_BASE=9200          # TestGateway TCP listen (9200 GW-T-A, 9201 GW-T-B) — the cluster
                                     # tier's OWN harness block, 9200-9209 (doc/registries.md §2). Not in
                                     # the 9300 block: that is three members of stride 10 with nothing spare.
# 9348 and 9349 were the cluster-tier harnesses' own test-consumer egress ports, and are now free:
# those harnesses run ClusterProbe follow, which opens no cluster session at all. Left unallocated rather than reused, since doc/registries.md §2 records the block.

test_gateway_port()       { echo $(( TEST_GATEWAY_PORT_BASE + ${1:-0} )); }
