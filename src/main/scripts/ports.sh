# ports.sh — canonical port-layout formula shared by every phixeron launch/test script, the
# bash mirror of SequencerServer's PORT_BASE + memberId*10 + offset scheme (see
# SequencerServer.java's class Javadoc, PortLayout.hpp on the C++ side, and doc/design.md's
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

# ── Satellite ports — one dedicated base per client role, deliberately outside the cluster's
#    own reserved block (9300-9329, three members of stride 10); offset by memberId for a role
#    with one co-located replica per node. Must match AppPorts.hpp (C++); the cluster block above
#    is PortLayout.hpp's, and the block table is doc/registries.md §2's. ────────────────────────
FIX_TCP_PORT_BASE=9000               # FixGateway TCP listen port
FIX_TEST_CLIENT_EGRESS_PORT=9403     # fix_test_server's own (non-colocated) cluster egress
ORDER_EXEC_EGRESS_PORT_BASE=9330     # OrderExecServer co-located egress
FIX_GATEWAY_EGRESS_PORT_BASE=9340    # FixGateway co-located egress
BASICDATA_EGRESS_PORT_BASE=9350      # BasicDataServer co-located egress
RISK_TEST_REPLAY_PORT_DEFAULT=9400   # fix_test_server risk-test replay
RESEND_REPLAY_PORT_DEFAULT=9401      # FixGateway resend-recovery replay
TEST_GATEWAY_PORT_BASE=9200          # TestGateway TCP listen (9200 GW-T-A, 9201 GW-T-B) — the cluster
                                     # tier's OWN harness block, 9200-9209 (doc/registries.md §2). Not in
                                     # the 9300 block: that is three members of stride 10 with nothing spare.
# 9348 and 9349 were the cluster-tier harnesses' own test-consumer egress ports, and are now free:
# those harnesses run ClusterProbe follow, which opens no cluster session at all (doc/future-arch.md
# §11 step 5). Left unallocated rather than reused, since doc/registries.md §2 records the block.

test_gateway_port()       { echo $(( TEST_GATEWAY_PORT_BASE + ${1:-0} )); }
fix_tcp_port()            { echo $(( FIX_TCP_PORT_BASE + ${1:-0} )); }
order_exec_egress_port()  { echo $(( ORDER_EXEC_EGRESS_PORT_BASE + $1 )); }
fix_gateway_egress_port() { echo $(( FIX_GATEWAY_EGRESS_PORT_BASE + $1 )); }
basicdata_egress_port()   { echo $(( BASICDATA_EGRESS_PORT_BASE + $1 )); }
