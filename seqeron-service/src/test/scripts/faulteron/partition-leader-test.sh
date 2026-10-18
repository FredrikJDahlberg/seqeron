#!/usr/bin/env bash
# Partitions the leader of seqeron's three-node docker cluster from its peers while a producer streams through it,
# then heals the partition.
#
# THE FAULT: every member's archive, consensus, log and transfer port is blocked on the leader's interface, in both
# directions. Its ingress port and the clients' egress stay open, so the leader can still accept ingress it can no
# longer replicate. A process kill never produces that: the old leader takes frames that the new term discards.
#
# WHAT IT ASSERTS:
#   1. The fault fired: the leader's interface dropped packets on the blocked ports.
#   2. The majority elects a new leader while the old one is cut off.
#   3. Healed, the old leader rejoins as a follower of the new term.
#   4. A ClusterProbe confirm producer on a majority member, streaming throughout, sees every frame on its queue exactly
#      once, in order — PendingSends resent what the old leader took and lost. A second, untracked producer is the
#      control: its loss is reported, not asserted, and is the evidence the partition had frames in flight.
set -uo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
LOG_DIR="${REPO_ROOT}/logs/faulteron/partition-leader"

PARTITION_SECS="${PARTITION_SECS:-10}"

start_cluster
OLD="$(await_leader "$(deadline 60)")" || {
    echo "no initial leader emerged"
    exit 1
}
echo "leader = member ${OLD}"

# ── producers on a majority member ─────────────────────────────────────────────────────────────────
P=$(((OLD + 1) % NODE_COUNT))
confirm "${P}" 11 > "${LOG_DIR}/confirm.log" 2>&1 &
CONFIRM_PID=$!
confirm "${P}" 12 -Dprobe.pendingSends=false > "${LOG_DIR}/control.log" 2>&1 &
CONTROL_PID=$!
for f in confirm control; do
    wait_for_log "${LOG_DIR}/${f}.log" "confirm: sending" "$(deadline 60)" || {
        echo "${f} producer never started; see ${LOG_DIR}/${f}.log"
        exit 1
    }
done
echo "confirm + control producers streaming on member ${P}"
sleep 2

# ── partition ──────────────────────────────────────────────────────────────────────────────────────
# shellcheck disable=SC2046
if ! faulteron "${OLD}" attach || ! faulteron "${OLD}" block $(internal_ports); then
    echo "faulteron could not partition node-${OLD}"
    exit 1
fi
echo "partitioned node-${OLD} from its peers: $(internal_ports | tr '\n' ' ')"

NEW="$(await_leader "$(deadline 30)" "${OLD}")"
[[ -n "${NEW}" ]] && echo "new leader = member ${NEW}" || echo "no new leader emerged while node-${OLD} was cut off"
sleep "${PARTITION_SECS}"

faulteron "${OLD}" stats > "${LOG_DIR}/rules.txt"
DROPPED="$(counter dropped < "${LOG_DIR}/rules.txt")"
faulteron "${OLD}" detach
echo "healed node-${OLD} after ${PARTITION_SECS}s; it dropped ${DROPPED} packet(s)"

REJOINED=0
[[ -n "${NEW}" ]] && await_follower "${OLD}" "${NEW}" "$(deadline 60)" && REJOINED=1
[[ "$(docker inspect -f '{{.State.Running}}' "node-${OLD}")" == true ]] || echo "node-${OLD} exited"

wait "${CONFIRM_PID}"
CONFIRM_RC=$?
wait "${CONTROL_PID}"
CONTROL_RC=$?

echo ""
echo "=== RESULT ==="
echo "rules on node-${OLD}:"
sed 's/^/  /' "${LOG_DIR}/rules.txt"
echo "new leader                  : ${NEW:-none}"
echo "old leader rejoined         : ${REJOINED}"
echo "PendingSends producer       : $(grep -h 'confirm:' "${LOG_DIR}/confirm.log" | tail -1)"
echo "untracked control           : $(grep -h 'confirm:' "${LOG_DIR}/control.log" | tail -1)"
((CONTROL_RC != 0)) \
    || echo "  (the control lost nothing: the partition had no frame in flight, so this run proved no recovery)"

PASS=1
((DROPPED > 0)) || {
    echo "FAIL: the partition dropped nothing"
    PASS=0
}
[[ -n "${NEW}" ]] || {
    echo "FAIL: no new leader"
    PASS=0
}
((REJOINED == 1)) || {
    echo "FAIL: node-${OLD} did not rejoin as a follower of member ${NEW:-?}"
    PASS=0
}
((CONFIRM_RC == 0)) || {
    echo "FAIL: the PendingSends producer was not exact"
    PASS=0
}

echo ""
if ((PASS == 1)); then
    echo "PARTITION LEADER TEST: PASS"
    exit 0
fi
echo "PARTITION LEADER TEST: FAIL"
exit 1
