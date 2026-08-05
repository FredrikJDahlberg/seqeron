# ports.sh — canonical port-layout formula shared by every phixeron launch/test script, the
# bash mirror of SequencerNode's PORT_BASE + memberId*10 + offset scheme (see
# SequencerNode.java's class Javadoc, PortLayout.hpp on the C++ side, and doc/design.md's
# "Port layout" section — the source of truth all three cite). Meant to be sourced, not
# executed: every script that built a CLUSTER_MEMBERS string used to hand-type the same
# three-line block, which is how BasicDataClient's and FixGateway's egress-port defaults
# once drifted onto the same value (9340+memberId) without anyone noticing.

CLUSTER_PORT_BASE=9300
CLUSTER_PORT_STRIDE=10

cluster_member_port_base() { echo $(( CLUSTER_PORT_BASE + $1 * CLUSTER_PORT_STRIDE )); }
archive_port()  { echo $(( $(cluster_member_port_base "$1") + 1 )); }
ingress_port()  { echo $(( $(cluster_member_port_base "$1") + 2 )); }
member_port()   { echo $(( $(cluster_member_port_base "$1") + 3 )); }
log_port()      { echo $(( $(cluster_member_port_base "$1") + 4 )); }
transfer_port() { echo $(( $(cluster_member_port_base "$1") + 5 )); }

# Builds the Aeron clusterMembers string for a nodeCount-member cluster, all on one host — the
# bash mirror of SequencerNode.buildClusterMembers. Usage: cluster_members_string 3 [host]
cluster_members_string() {
    local node_count="$1" host="${2:-localhost}" out="" id
    for (( id = 0; id < node_count; id++ )); do
        out+="${id},${host}:$(ingress_port "${id}"),${host}:$(member_port "${id}"),"
        out+="${host}:$(log_port "${id}"),${host}:$(transfer_port "${id}"),${host}:$(archive_port "${id}")|"
    done
    echo "${out}"
}

# ── Satellite ports — one dedicated base per client role, deliberately outside the cluster's
#    own 9300-9325 (3-node) block; offset by memberId for a role with one co-located replica
#    per node. Must match PortLayout.hpp (C++) / SequencerNode.java. ─────────────────────────
FIX_TCP_PORT_BASE=9000               # FixGateway TCP listen port
FIX_TEST_CLIENT_EGRESS_PORT=9320     # fix_test_server's own (non-colocated) cluster egress
ORDER_EXEC_EGRESS_PORT_BASE=9330     # OrderExecClient co-located egress
FIX_GATEWAY_EGRESS_PORT_BASE=9340    # FixGateway co-located egress
BASICDATA_EGRESS_PORT_BASE=9350      # BasicDataClient co-located egress
RISK_TEST_REPLAY_PORT_DEFAULT=9400   # fix_test_server risk-test replay
RESEND_REPLAY_PORT_DEFAULT=9401      # FixGateway resend-recovery replay
TEST_CONSUMER_EGRESS_PORT=9349       # test-only: gap-recovery-test.sh/chaos-runner.sh's own
                                      # observation-consumer OrderExecClient (replayerClientId=9),
                                      # distinct from ORDER_EXEC_EGRESS_PORT_BASE+id

fix_tcp_port()            { echo $(( FIX_TCP_PORT_BASE + ${1:-0} )); }
order_exec_egress_port()  { echo $(( ORDER_EXEC_EGRESS_PORT_BASE + $1 )); }
fix_gateway_egress_port() { echo $(( FIX_GATEWAY_EGRESS_PORT_BASE + $1 )); }
basicdata_egress_port()   { echo $(( BASICDATA_EGRESS_PORT_BASE + $1 )); }
