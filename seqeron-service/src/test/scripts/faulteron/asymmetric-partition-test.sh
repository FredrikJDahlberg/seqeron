#!/usr/bin/env bash
# Cuts the link between the leader and one follower, and only that link, while the cluster streams; then heals it.
#
# THE FAULT: on the leader's interface, all UDP to and from the follower's host is dropped (--peer, every port).
# The other follower still reaches both. The cut-off follower hears no leader, so it times out and stands for
# election, and the only member it can ask is one that still has a leader. A Raft without pre-vote and leader
# stickiness lets it depose a healthy leader, over and over; one with them keeps the leader.
#
# WHAT IT ASSERTS:
#   1. The fault fired: the leader's interface dropped packets to and from the follower.
#   2. The leader keeps leading: no election completes while the follower is cut off.
#   3. A ClusterProbe confirm producer on the other follower, streaming throughout, sees every frame on its queue
#      exactly once, in order.
#   4. Healed, the cut-off follower catches up: a ping through its queue passes.
set -uo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
LOG_DIR="${REPO_ROOT}/logs/faulteron/asymmetric-partition"

FAULT_SECS="${FAULT_SECS:-15}"
CONFIRM_COUNT="${CONFIRM_COUNT:-60000}"

start_cluster
LEADER="$(await_leader "$(deadline 60)")" || {
    echo "no initial leader emerged"
    exit 1
}
F=$(((LEADER + 1) % NODE_COUNT))
O=$(((LEADER + 2) % NODE_COUNT))
echo "leader = member ${LEADER}; cutting it off from follower ${F}, producing on follower ${O}"

confirm "${O}" 11 > "${LOG_DIR}/confirm.log" 2>&1 &
CONFIRM_PID=$!
wait_for_log "${LOG_DIR}/confirm.log" "confirm: sending" "$(deadline 60)" || {
    echo "confirm producer never started; see ${LOG_DIR}/confirm.log"
    exit 1
}
echo "confirm producer streaming on member ${O}"
CHANGES_BEFORE="$(leadership_changes)"
TERM_BEFORE="$(term_of "${LEADER}")"

# ── fault ──────────────────────────────────────────────────────────────────────────────────────────
if ! faulteron "${LEADER}" attach || ! faulteron "${LEADER}" block --peer "node-${F}"; then
    echo "faulteron could not cut node-${LEADER} off from node-${F}"
    exit 1
fi
echo "node-${LEADER}: dropping all UDP to and from node-${F}"
FAULT_START=${SECONDS}
sleep 3

DURING=0
ping_queue "${F}" > "${LOG_DIR}/ping-during.log" 2>&1 && DURING=1
echo "ping through node-${F}'s queue under the fault: ${DURING}"

REMAINING=$((FAULT_SECS - (SECONDS - FAULT_START)))
((REMAINING > 0)) && sleep "${REMAINING}"
CHANGES_DURING="$(leadership_changes)"
LEADER_DURING="$(await_leader 5)"
heal "${LEADER}" > "${LOG_DIR}/rules.txt"
HEALED=${SECONDS}
echo "healed node-${LEADER} after $((HEALED - FAULT_START))s"

AFTER=0
for ((attempt = 1; attempt <= 5; attempt++)); do
    if ping_queue "${F}" >> "${LOG_DIR}/ping-after.log" 2>&1; then
        AFTER=1
        break
    fi
done
echo "ping through node-${F}'s queue after the heal: ${AFTER} (attempt ${attempt}, $((SECONDS - HEALED))s after the heal)"

wait "${CONFIRM_PID}"
CONFIRM_RC=$?
DROPPED="$(counter dropped < "${LOG_DIR}/rules.txt")"

echo ""
echo "=== RESULT ==="
echo "rules on node-${LEADER}:"
sed 's/^/  /' "${LOG_DIR}/rules.txt"
echo "leadership lines before/during : ${CHANGES_BEFORE} / ${CHANGES_DURING}"
echo "leader before / during         : ${LEADER} / ${LEADER_DURING:-none}"
echo "leader's term before / after   : ${TERM_BEFORE} / $(term_of "${LEADER_DURING:-${LEADER}}")"
echo "ping through node-${F}, during   : ${DURING}"
echo "PendingSends producer          : $(grep -h 'confirm:' "${LOG_DIR}/confirm.log" | tail -1)"

PASS=1
((DROPPED > 0)) || {
    echo "FAIL: nothing between node-${LEADER} and node-${F} was dropped"
    PASS=0
}
((CHANGES_DURING == CHANGES_BEFORE)) && [[ "${LEADER_DURING}" == "${LEADER}" ]] || {
    echo "FAIL: the cut-off follower deposed the leader"
    PASS=0
}
((CONFIRM_RC == 0)) || {
    echo "FAIL: the PendingSends producer was not exact"
    PASS=0
}
((AFTER == 1)) || {
    echo "FAIL: node-${F} did not catch up after the heal"
    PASS=0
}

echo ""
if ((PASS == 1)); then
    echo "ASYMMETRIC PARTITION TEST: PASS"
    exit 0
fi
echo "ASYMMETRIC PARTITION TEST: FAIL"
exit 1
