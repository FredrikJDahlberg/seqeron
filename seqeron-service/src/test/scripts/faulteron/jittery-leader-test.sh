#!/usr/bin/env bash
# Makes the leader's links slow, jittery and duplicating while the cluster streams, then heals them.
#
# THE FAULT: on the leader's interface, everything to or from a member's internal ports or the leader's ingress port
# leaves DELAY_MS plus up to JITTER_MS late, which reorders it, and DUPLICATE_PCT% of it, in either direction, is
# delivered twice. The worst case, DELAY_MS + JITTER_MS, stays under the cluster's 200 ms leader heartbeat timeout,
# and the 20 ms heartbeat interval is shorter than the jitter: heartbeats arrive late, out of order and twice, but
# never too far apart.
#
# WHAT IT ASSERTS:
#   1. The fault fired: the leader's interface delayed and duplicated packets.
#   2. No election: a slow link under the timeout is not a dead one.
#   3. Every member's queue stayed current: a ping through each passes under the fault.
#   4. A ClusterProbe confirm producer on a follower, streaming throughout, sees every frame on its queue exactly once,
#      in order — Aeron discards the duplicates and reorders what jitter shuffled.
set -uo pipefail

source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
LOG_DIR="${REPO_ROOT}/logs/faulteron/jittery-leader"

DELAY_MS="${DELAY_MS:-20}"
JITTER_MS="${JITTER_MS:-30}"
DUPLICATE_PCT="${DUPLICATE_PCT:-10}"
FAULT_SECS="${FAULT_SECS:-20}"

start_cluster
LEADER="$(await_leader "$(deadline 60)")" || {
    echo "no initial leader emerged"
    exit 1
}
P=$(((LEADER + 1) % NODE_COUNT))
echo "leader = member ${LEADER}; producing on follower ${P}"

confirm "${P}" 11 > "${LOG_DIR}/confirm.log" 2>&1 &
CONFIRM_PID=$!
wait_for_log "${LOG_DIR}/confirm.log" "confirm: sending" "$(deadline 60)" || {
    echo "confirm producer never started; see ${LOG_DIR}/confirm.log"
    exit 1
}
echo "confirm producer streaming on member ${P}"
CHANGES_BEFORE="$(leadership_changes)"

# ── fault ──────────────────────────────────────────────────────────────────────────────────────────
# shellcheck disable=SC2046
if ! faulteron "${LEADER}" attach || ! faulteron "${LEADER}" rule --delay "${DELAY_MS}" --jitter "${JITTER_MS}" \
    --duplicate "${DUPLICATE_PCT}" $(internal_ports) "$(ingress_port "${LEADER}")"; then
    echo "faulteron could not impair node-${LEADER}"
    exit 1
fi
echo "node-${LEADER}: delaying ${DELAY_MS}+0..${JITTER_MS} ms and duplicating ${DUPLICATE_PCT}%"
FAULT_START=${SECONDS}
sleep 3

PINGS=""
for m in 0 1 2; do
    ping_queue "${m}" >> "${LOG_DIR}/ping-during.log" 2>&1 && PINGS+="${m} "
done
echo "queues answering a ping under the fault: ${PINGS:-none}"

REMAINING=$((FAULT_SECS - (SECONDS - FAULT_START)))
((REMAINING > 0)) && sleep "${REMAINING}"
faulteron "${LEADER}" stats > "${LOG_DIR}/rules.txt"
faulteron "${LEADER}" detach
echo "healed node-${LEADER} after $((SECONDS - FAULT_START))s"
CHANGES_AFTER="$(leadership_changes)"

wait "${CONFIRM_PID}"
CONFIRM_RC=$?

DELAYED="$(counter delayed < "${LOG_DIR}/rules.txt")"
DUPLICATED="$(counter duplicated < "${LOG_DIR}/rules.txt")"

echo ""
echo "=== RESULT ==="
echo "delayed / duplicated         : ${DELAYED} / ${DUPLICATED}"
echo "leadership lines before/after: ${CHANGES_BEFORE} / ${CHANGES_AFTER}"
echo "queues answering under fault : ${PINGS:-none}"
echo "PendingSends producer        : $(grep -h 'confirm:' "${LOG_DIR}/confirm.log" | tail -1)"

PASS=1
((DELAYED > 0 && DUPLICATED > 0)) || {
    echo "FAIL: the fault did not both delay and duplicate"
    PASS=0
}
((CHANGES_AFTER == CHANGES_BEFORE)) || {
    echo "FAIL: an election ran under the fault"
    PASS=0
}
[[ "${PINGS}" == "0 1 2 " ]] || {
    echo "FAIL: not every queue answered a ping under the fault"
    PASS=0
}
((CONFIRM_RC == 0)) || {
    echo "FAIL: the PendingSends producer was not exact"
    PASS=0
}

echo ""
if ((PASS == 1)); then
    echo "JITTERY LEADER TEST: PASS"
    exit 0
fi
echo "JITTERY LEADER TEST: FAIL"
exit 1
